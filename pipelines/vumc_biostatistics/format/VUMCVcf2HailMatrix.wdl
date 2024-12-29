version 1.0

workflow VUMCVcf2HailMatrix {
  #modified based on 
  #https://github.com/broadinstitute/long-read-pipelines/blob/7d36a93964998f513a132b86ca9ace6c663d3327/wdl/tasks/Utility/Hail.wdl
 
  input {
    File input_vcf
    File input_vcf_index

    File? id_map_file
    Boolean pass_only=true

    String reference_genome = "GRCh38"

    String output_prefix

    String? project_id
    String? target_gcp_folder
  }

  call Vcf2HailMatrix {
    input:
      input_vcf = input_vcf,
      input_vcf_index = input_vcf_index,
      id_map_file = id_map_file,
      pass_only = pass_only,
      reference_genome = reference_genome,
      output_prefix = output_prefix,
      project_id = project_id,
      target_gcp_folder = target_gcp_folder
  }

  output {
    String hail_gcs_path = Vcf2HailMatrix.hail_gcs_path
    Int num_samples = Vcf2HailMatrix.num_samples
    Int num_variants = Vcf2HailMatrix.num_variants
    Int num_invalid_samples = Vcf2HailMatrix.num_invalid_samples
  }
}

task Vcf2HailMatrix {
  meta {
    description: "Convert a .vcf.bgz file to a Hail MatrixTable and copy it to a final gs:// URL."
  }

  input {
    File input_vcf
    File input_vcf_index

    File? id_map_file

    String reference_genome
    String output_prefix

    Boolean pass_only=true

    String? project_id
    String? target_gcp_folder

    String docker = "shengqh/hail_gcp:20240211"
    Int disk_size_factor = 3
    Int memory_gb = 64
    Int preemptible = 0
    Int cpu = 4
    Int? disk_size_override
    Int boot_disk_gb = 25
  }

  Int disk_size = select_first([disk_size_override, disk_size_factor * ceil(size(input_vcf, "GB")) + 100])
  Int total_memory_gb = memory_gb + 2

  Boolean output_to_gcp = defined(target_gcp_folder)
  String gcs_output_dir = sub("~{target_gcp_folder}", "/+$", "")
  String gcs_output_path = if output_to_gcp then gcs_output_dir + "/" + output_prefix else ""

  String local_output_file = "~{output_prefix}/metadata.json.gz"

  command <<<

#https://discuss.hail.is/t/i-get-a-negativearraysizeexception-when-loading-a-plink-file/899
export PYSPARK_SUBMIT_ARGS="--driver-java-options '-XX:hashCode=0' --conf 'spark.executor.extraJavaOptions=-XX:hashCode=0' pyspark-shell"

mkdir -p tmp

cat <<CODE > vcf2hail.py
import logging
import hail as hl
import pandas as pd

logger = logging.getLogger('v2h')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)-8s - %(message)s')

logger.info("Calling hl.init ...")
hl.init(tmp_dir='./tmp',
        master='local[*]',  # Use all available cores
        min_block_size=128,  # Minimum block size in MB
        quiet=True,
        spark_conf={
            'spark.driver.memory': '~{memory_gb}g',
            'spark.executor.memory': '~{memory_gb}g',
            'spark.network.timeout': '800s',
            'spark.executor.heartbeatInterval': '400s'
        })

logger.info("Reading from ~{input_vcf} ...")
callset = hl.import_vcf("~{input_vcf}",
                        array_elements_required=False,
                        force_bgz=True,
                        reference_genome='~{reference_genome}')

if "~{pass_only}" == "true":
  logger.info("Filtering out variants that do not pass all filters...")
  nsnp_pre = callset.count_rows()
  callset = callset.filter_rows(hl.len(callset.filters) == 0)
  nsnp_post = callset.count_rows()
  logger.info(f"Filtered out {nsnp_pre - nsnp_post} variants.")

if "~{id_map_file}" != "":
  logger.info("Loading ID map file...")
  df = pd.read_csv("~{id_map_file}", sep="\t") 

  logger.info("Replacing PRIMARY_GRID=='-' with ICA_ID + '_INVALID'...")
  df['PRIMARY_GRID'] = df.apply(
      lambda row: f"{row['ICA_ID']}_INVALID" if row['PRIMARY_GRID'] == "-" else row['PRIMARY_GRID'],
      axis=1
  )

  logger.info("Converting pandas data frame to hail table...")
  ht = hl.Table.from_pandas(df)
  ht = ht.key_by("ICA_ID")
  ht.describe()
  
  logger.info("Annotate the MatrixTable with the mapping ...")
  callset = callset.annotate_cols(PRIMARY_GRID=ht[callset.s].PRIMARY_GRID)

  logger.info("Assign new sample names...")
  callset = callset.key_cols_by(s=callset.PRIMARY_GRID)

  logger.info("Drop the temporary field...")
  callset = callset.drop('PRIMARY_GRID')

nsample = callset.count_cols()
logger.info(f"Number of samples: {nsample}")
with open("num_samples.txt", "w") as f:
  f.write(str(nsample))

n_invalid = callset.aggregate_cols(hl.agg.count_where(callset.s.endswith('_INVALID')))
logger.info(f"Number of invalid samples: {n_invalid}")
with open("num_invalid_samples.txt", "w") as f:
  f.write(str(n_invalid))

nsnp = callset.count_rows()
logger.info(f"Number of variants: {nsnp}")
with open("num_variants.txt", "w") as f:
  f.write(str(nsnp))

logger.info("Writing MatrixTable to ~{output_prefix} ...")
callset.write("~{output_prefix}", 
              overwrite=True, 
              stage_locally=False)

CODE

set -o pipefail

python3 vcf2hail.py

if [[ -f "~{local_output_file}" ]]; then
  echo "Writing completed successfully."

  if [[ "~{output_to_gcp}" == "true" ]]; then
    echo "Copying MatrixTable to GCS..."
    gsutil ~{"-u " + project_id} -m rsync -Cr ~{output_prefix} ~{gcs_output_path}

    res=$?
    if [[ $res -ne 0 ]]; then
      echo "Copying to GCS failed."
      exit $res
    fi

    echo "Copying to GCS succeed."
    touch hail_copied_to_gcp.txt
    exit 0
  else
    echo "Compressing hail matrix folder ..."
    tar czf ~{output_prefix}.tar.gz ~{output_prefix}
  fi

else
  echo "Writing failed."
  exit 1
fi

>>>

  runtime {
    cpu: cpu
    docker: "~{docker}"
    preemptible: preemptible
    disks: "local-disk ~{disk_size} SSD"
    memory: "~{total_memory_gb} GiB"
    bootDiskSizeGb: boot_disk_gb
  }
  output {
    String hail_gcs_path = "~{gcs_output_path}"
    File hail_local_path = if output_to_gcp then "hail_copied_to_gcp.txt" else "~{output_prefix}.tar.gz"

    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")  
    Int num_invalid_samples = read_int("num_invalid_samples.txt")
  }
}
