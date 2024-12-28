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

    Boolean output_to_gcp=true
    String? project_id
    String target_gcp_folder
  }

  call Vcf2HailMatrix {
    input:
      input_vcf = input_vcf,
      input_vcf_index = input_vcf_index,
      id_map_file = id_map_file,
      pass_only = pass_only,
      reference_genome = reference_genome,
      output_prefix = output_prefix,
      output_to_gcp = output_to_gcp,
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

  parameter_meta {
    input_vcf: "The input .vcf.bgz file."
    input_vcf_index: "The input .vcf.bgz.tbi file."

    id_map_file: "Optional. A tab-delimited file with at least two columns: ICA_ID and PRIMARY_GRID."

    reference_genome: "The reference genome to use.  Currently only GRCh38 is supported."
    output_prefix: "The prefix to use for the output MatrixTable."

    project_id: "The GCP project to use for gsutil."
    target_gcp_folder: "The output GCP directory to copy the MatrixTable to."

    docker: "The docker image to use for this task."
    memory_gb: "The amount of memory to use for this task."
    preemptible: "Number of preemptible tries to use for this task."
  }  

  input {
    File input_vcf
    File input_vcf_index

    File? id_map_file

    String reference_genome
    String output_prefix

    Boolean pass_only=true

    Boolean output_to_gcp=true
    String? project_id
    String target_gcp_folder

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

  String gcs_output_dir = sub(target_gcp_folder, "/+$", "")
  String gcs_output_path = gcs_output_dir + "/" + output_prefix

  String local_output_file = "~{output_prefix}/metadata.json.gz"

  command <<<

#https://discuss.hail.is/t/i-get-a-negativearraysizeexception-when-loading-a-plink-file/899
export PYSPARK_SUBMIT_ARGS="--driver-java-options '-XX:hashCode=0' --conf 'spark.executor.extraJavaOptions=-XX:hashCode=0' pyspark-shell"

cat <<CODE > vcf2hail.py

import hail as hl
import pandas as pd

print("Calling hl.init ...", flush=True)
hl.init(master='local[*]',  # Use all available cores
        min_block_size=128,  # Minimum block size in MB
        quiet=True,
        spark_conf={
            'spark.driver.memory': '~{memory_gb}g',
            'spark.executor.memory': '~{memory_gb}g',
            'spark.network.timeout': '800s',
            'spark.executor.heartbeatInterval': '400s'
        })

print("Reading vcf from ~{input_vcf} ...", flush=True)
callset = hl.import_vcf("~{input_vcf}",
                        array_elements_required=False,
                        force_bgz=True,
                        reference_genome='~{reference_genome}')

if "~{pass_only}" == "true":
  print("Filtering out variants that do not pass all filters...", flush=True)
  nsnp_pre = callset.count_rows()
  callset = callset.filter_rows(hl.len(callset.filters) == 0)
  nsnp_post = callset.count_rows()
  print(f"Filtered out {nsnp_pre - nsnp_post} variants.", flush=True)

if "~{id_map_file}" != "":
  print("Loading ID map file...", flush=True)
  df = pd.read_csv("~{id_map_file}", sep="\t") 

  print("Replacing PRIMARY_GRID=='-' with ICA_ID + '_INVALID'...", flush=True)
  df['PRIMARY_GRID'] = df.apply(
      lambda row: f"{row['ICA_ID']}_INVALID" if row['PRIMARY_GRID'] == "-" else row['PRIMARY_GRID'],
      axis=1
  )

  print("Converting pandas data frame to hail table...", flush=True)
  ht = hl.Table.from_pandas(df)
  ht = ht.key_by("ICA_ID")
  ht.describe()
  
  print("Annotate the MatrixTable with the mapping ...", flush=True)
  callset = callset.annotate_cols(PRIMARY_GRID=ht[callset.s].PRIMARY_GRID)

  print("Assign new sample names...", flush=True)
  callset = callset.key_cols_by(s=callset.PRIMARY_GRID)

  print("Drop the temporary field...", flush=True)
  callset = callset.drop('PRIMARY_GRID')

nsample = callset.count_cols()
print(f"Number of samples: {nsample}", flush=True)
with open("num_samples.txt", "w") as f:
  f.write(str(nsample))

n_invalid = callset.aggregate_cols(hl.agg.count_where(callset.s.endswith('_INVALID')))
print(f"Number of invalid samples: {n_invalid}", flush=True)
with open("num_invalid_samples.txt", "w") as f:
  f.write(str(n_invalid))

nsnp = callset.count_rows()
print(f"Number of variants: {nsnp}", flush=True)
with open("num_variants.txt", "w") as f:
  f.write(str(nsnp))

print("Writing MatrixTable to ~{output_prefix} ...", flush=True)
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
  fi

  touch convert_complete.txt
  exit 0
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
    String hail_local_path = "~{output_prefix}"
    String hail_gcs_path = "~{gcs_output_path}"
    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")  
    Int num_invalid_samples = read_int("num_invalid_samples.txt")
    File completion_file = "convert_complete.txt" # This is a dummy file to indicate the task has completed
  }
}
