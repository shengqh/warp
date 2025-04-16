version 1.0

workflow VUMCBgenUrl2HailMatrix {
  #modified based on 
  #https://github.com/broadinstitute/long-read-pipelines/blob/7d36a93964998f513a132b86ca9ace6c663d3327/wdl/tasks/Utility/Hail.wdl
 
  input {
    String input_bgen
    String input_bgen_sample

    String reference_genome = "GRCh38"

    String output_prefix

    String billing_project_id
    String? target_gcp_folder
  }

  call BgenUrl2HailMatrix {
    input:
      input_bgen = input_bgen,
      input_bgen_sample = input_bgen_sample,
      reference_genome = reference_genome,
      output_prefix = output_prefix,
      billing_project_id = billing_project_id,
      target_gcp_folder = target_gcp_folder
  }

  output {
    String hail_gcs_path = BgenUrl2HailMatrix.hail_gcs_path
    Int num_samples = BgenUrl2HailMatrix.num_samples
    Int num_variants = BgenUrl2HailMatrix.num_variants
  }
}

task BgenUrl2HailMatrix {
  input {
    String input_bgen
    String input_bgen_sample

    String reference_genome
    String output_prefix

    String billing_project_id
    String? target_gcp_folder

    String docker = "shengqh/hail_gcp:20240211"
    Int disk_size_factor = 3
    Int memory_gb = 64
    Int preemptible = 3
    Int cpu = 4
    Int? disk_size_override
    Int boot_disk_gb = 25
  }

  Int disk_size = select_first([disk_size_override, disk_size_factor * ceil(size(input_bgen, "GB")) + 10])
  Int total_memory_gb = memory_gb + 2

  Boolean output_to_gcp = defined(target_gcp_folder)
  String gcs_output_dir = sub("~{target_gcp_folder}", "/+$", "")
  String gcs_output_path = if output_to_gcp then gcs_output_dir + "/" + output_prefix else ""

  String index_meta_file = "~{input_bgen}.idx2/metadata.json.gz"

  String local_output_file = "~{output_prefix}/metadata.json.gz"

  command <<<

#https://discuss.hail.is/t/i-get-a-negativearraysizeexception-when-loading-a-plink-file/899
export PYSPARK_SUBMIT_ARGS="--driver-java-options '-XX:hashCode=0' --conf 'spark.executor.extraJavaOptions=-XX:hashCode=0' pyspark-shell"

mkdir -p tmp

cat <<CODE > bgen2hail.py

import logging
import hail as hl

has_index=sys.argv[1]

logger = logging.getLogger('bgen2hail')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)-8s - %(message)s')

logger.info("Calling hl.init ...")

def parse_gcs_url(gcs_url):
  if not gcs_url.startswith('gs://'):
    raise ValueError("URL must start with 'gs://'")

  # Remove the 'gs://' prefix
  gcs_url = gcs_url[5:]

  # Split the remaining URL into bucket name and object key
  parts = gcs_url.split('/', 1)
  if len(parts) != 2:
    raise ValueError("Invalid GCS URL format")

  bucket_name = parts[0]

  return bucket_name

bucket_name = parse_gcs_url("~{input_bgen}")
print(f"hail_bucket_name={bucket_name}")

hl.init(tmp_dir='./tmp',
        master='local[*]',  # Use all available cores
        min_block_size=128,  # Minimum block size in MB
        quiet=True,
        spark_conf={
            'spark.driver.memory': '~{memory_gb}g',
            'spark.executor.memory': '~{memory_gb}g',
            'spark.network.timeout': '800s',
            'spark.executor.heartbeatInterval': '400s',
            'spark.hadoop.fs.gs.requester.pays.mode': 'CUSTOM',
            'spark.hadoop.fs.gs.requester.pays.buckets': bucket_name,
            'spark.hadoop.fs.gs.requester.pays.project.id': "~{billing_project_id}"
        }, 
        idempotent=True)

if has_index == "0":
  logger.info("Index bgen file ...")
  hl.index_bgen("~{input_bgen}", 
                reference_genome="~{reference_genome}",
                contig_recoding={ '1': 'chr1',
                                  '2': 'chr2',
                                  '3': 'chr3',
                                  '4': 'chr4',
                                  '5': 'chr5',
                                  '6': 'chr6',
                                  '7': 'chr7',
                                  '8': 'chr8',
                                  '9': 'chr9',
                                  '10': 'chr10',
                                  '11': 'chr11',
                                  '12': 'chr12',
                                  '13': 'chr13',
                                  '14': 'chr14',
                                  '15': 'chr15',
                                  '16': 'chr16',
                                  '17': 'chr17',
                                  '18': 'chr18',
                                  '19': 'chr19',
                                  '20': 'chr20',
                                  '21': 'chr21',
                                  '22': 'chr22',
                                  'X': 'chrX',
                                  'Y': 'chrY',
                                  'MT': 'chrM',
                                  'PAR1': 'chrX',
                                  'PAR2': 'chrX'})

logger.info("Reading bgen from ~{input_bgen} ...")
callset = hl.import_bgen("~{input_bgen}",
                           entry_fields=['GT'],
                           sample_file="~{input_bgen_sample}")

nsnp = callset.count_rows()
logger.info(f"Number of variants: {nsnp}")
with open("num_variants.txt", "w") as f:
  f.write(str(nsnp))

nsample = callset.count_cols()
logger.info(f"Number of samples: {nsample}")
with open("num_samples.txt", "w") as f:
  f.write(str(nsample))

logger.info("Writing MatrixTable to ~{output_prefix} ...")
callset.write("~{output_prefix}", 
              overwrite=True, 
              stage_locally=False)

CODE

set -o pipefail

if gsutil -u ~{billing_project_id} stat ~{index_meta_file} 2>/dev/null; then
  has_index="1"
else
  has_index="0"
fi

python3 bgen2hail.py $has_index

if [[ -f "~{local_output_file}" ]]; then
  echo "Writing completed successfully."

  if [[ "~{output_to_gcp}" == "true" ]]; then
    echo "Copying MatrixTable to GCS..."
    gsutil ~{"-u " + billing_project_id} -m rsync -Cr ~{output_prefix} ~{gcs_output_path}

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
  }
}