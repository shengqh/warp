version 1.0

workflow VUMCVcf2HailMatrix {
  #modified based on https://dockstore.org/workflows/github.com/broadinstitute/long-read-pipelines/ConvertToHailMTT2T:hangsu_phasing?tab=files
 
  input {
    File source_vcf
    File source_vcf_index

    File? id_map_file

    String reference_genome = "GRCh38"

    String target_prefix

    String? project_id
    String target_gcp_folder
  }

  call Vcf2HailMatrix {
    input:
      source_vcf = source_vcf,
      source_vcf_index = source_vcf_index,
      id_map_file = id_map_file,
      reference_genome = reference_genome,
      target_prefix = target_prefix,
      project_id = project_id,
      target_gcp_folder = target_gcp_folder,
  }

  output {
    String hail_gcs_path = Vcf2HailMatrix.hail_gcs_path
  }
}

task Vcf2HailMatrix {
  meta {
    description: "Convert a .vcf.bgz file to a Hail MatrixTable and copy it to a final gs:// URL."
  }

  parameter_meta {
    source_vcf: "The input .vcf.bgz file."
    source_vcf_index: "The input .vcf.bgz.tbi file."

    id_map_file: "Optional. A tab-delimited file with at least two columns: ICA_ID and PRIMARY_GRID."

    reference_genome: "The reference genome to use.  Currently only GRCh38 is supported."
    target_prefix: "The prefix to use for the output MatrixTable."

    project_id: "The GCP project to use for gsutil."
    target_gcp_folder: "The output GCP directory to copy the MatrixTable to."

    docker: "The docker image to use for this task."
    memory_gb: "The amount of memory to use for this task."
    preemptible: "Number of preemptible tries to use for this task."
  }  

  input {
    File source_vcf
    File source_vcf_index

    File? id_map_file

    String reference_genome
    String target_prefix

    Boolean pass_only=true

    String? project_id
    String target_gcp_folder

    String docker = "shengqh/hail_gcp:20240211"
    Int memory_gb = 64
    Int preemptible = 0
    Int cpu = 4
    Int boot_disk_gb = 10
  }

  Int disk_size = 100 + 3*ceil(size(source_vcf, "GB"))
  Int total_memory_gb = memory_gb + 2

  String bucket=sub(target_gcp_folder, "^gs://", "")
  Boolean is_gcs = bucket != target_gcp_folder

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")
  String gcs_output_path = gcs_output_dir + "/" + target_prefix

  String final_url = if is_gcs then gcs_output_path else target_prefix

  command <<<

#https://discuss.hail.is/t/i-get-a-negativearraysizeexception-when-loading-a-plink-file/899
export PYSPARK_SUBMIT_ARGS="--driver-java-options '-XX:hashCode=0' --conf 'spark.executor.extraJavaOptions=-XX:hashCode=0' pyspark-shell"

cat <<CODE > vcf2hail.py

import hail as hl
import pandas as pd

hl.init(master='local[*]',  # Use all available cores
        min_block_size=128,  # Minimum block size in MB
        quiet=True,
        spark_conf={
            'spark.driver.memory': '~{memory_gb}g',
            'spark.executor.memory': '~{memory_gb}g',
            'spark.network.timeout': '800s',
            'spark.executor.heartbeatInterval': '400s'
        })

hl.utils.warning("Reading vcf from ~{source_vcf} ...")
callset = hl.import_vcf("~{source_vcf}",
                        array_elements_required=False,
                        force_bgz=True,
                        reference_genome='~{reference_genome}')

if "~{pass_only}" == "true":
  hl.utils.warning("Filtering out variants that do not pass all filters...")
  nsnp_pre = callset.count_rows()
  callset = callset.filter_rows(hl.len(callset.filters) == 0)
  nsnp_post = callset.count_rows()
  hl.utils.warning(f"Filtered out {nsnp_pre - nsnp_post} variants.")

if "~{id_map_file}" != "":
  hl.utils.warning("Loading ID map file...")
  df = pd.read_csv("~{id_map_file}", sep="\t") 

  hl.utils.warning("Replacing PRIMARY_GRID=='-' with ICA_ID + '_INVALID'...")
  df['PRIMARY_GRID'] = df.apply(
      lambda row: f"{row['ICA_ID']}_INVALID" if row['PRIMARY_GRID'] == "-" else row['PRIMARY_GRID'],
      axis=1
  )

  hl.utils.warning("Converting pandas data frame to hail table...")
  ht = hl.Table.from_pandas(df)
  ht = ht.key_by("ICA_ID")
  ht.describe()
  
  hl.utils.warning("Annotate the MatrixTable with the mapping ...")
  callset = callset.annotate_cols(PRIMARY_GRID=ht[callset.s].PRIMARY_GRID)

  hl.utils.warning("Assign new sample names...")
  callset = callset.key_cols_by(s=callset.PRIMARY_GRID)

  hl.utils.warning("Drop the temporary field...")
  callset = callset.drop('PRIMARY_GRID')

nsample = callset.count_cols()
with open("num_samples.txt", "w") as f:
    f.write(str(nsample))

nsnp = callset.count_rows()
with open("num_variants.txt", "w") as f:
    f.write(str(nsnp))

n_invalid = callset.aggregate_cols(hl.agg.count_where(callset.s.endswith('_INVALID')))
with open("num_invalid_samples.txt", "w") as f:
    f.write(str(n_invalid))

hl.utils.warning("Writing MatrixTable to disk...")
callset.write("~{target_prefix}", 
              overwrite=True, 
              stage_locally=True)

CODE

set -o pipefail

python3 vcf2hail.py

status=$?
if [[ "$status" == "0" ]]; then
  if [[ "~{is_gcs}" == "true" ]]; then
    echo "Copying MatrixTable to GCS..."
    gsutil ~{"-u " + project_id} -m rsync -Cr ~{target_prefix} ~{gcs_output_path}
  fi
fi

>>>

  runtime {
    cpu: cpu
    docker: "~{docker}"
    preemptible: preemptible
    disks: "local-disk ~{disk_size} HDD"
    memory: "~{total_memory_gb} GiB"
    bootDiskSizeGb: boot_disk_gb
  }
  output {
    String hail_gcs_path = "~{final_url}"
    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")  
    Int num_invalid_samples = read_int("num_invalid_samples.txt")
  }
}
