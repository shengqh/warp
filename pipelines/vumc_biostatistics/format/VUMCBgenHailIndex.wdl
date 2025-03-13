version 1.0

# VUMCBgenHailIndex Workflow
#
# This workflow creates a Hail index for BGEN format files. 
#
# Original Author: VUMC/VANGARD
# Modified from: Broad Institute's long-read-pipelines repository
#
# WORKFLOW OVERVIEW:
# The workflow processes genetic data in BGEN format to create an index for use with Hail.
#
# WORKFLOW INPUTS:
# - input_bgen: File            # BGEN format file containing genetic data
# - input_bgen_sample: File     # Sample file accompanying the BGEN file
# - reference_genome: String    # Reference genome (default: "GRCh38")
# - project_id: String?         # Optional Google Cloud project ID
# - target_gcp_folder: String?  # Optional Google Cloud storage target folder
#
# WORKFLOW OUTPUTS:
# - hail_gcs_path: String       # Google Cloud Storage path to the Hail index
# - hail_local_path: File       # Local path to the Hail index
#
workflow VUMCBgenHailIndex {
  #modified based on 
  #https://github.com/broadinstitute/long-read-pipelines/blob/7d36a93964998f513a132b86ca9ace6c663d3327/wdl/tasks/Utility/Hail.wdl
 
  input {
    File input_bgen
    File input_bgen_sample

    String reference_genome = "GRCh38"

    String? project_id
    String? target_gcp_folder
  }

  call BgenHailIndex {
    input:
      input_bgen = input_bgen,
      input_bgen_sample = input_bgen_sample,
      reference_genome = reference_genome,
      project_id = project_id,
      target_gcp_folder = target_gcp_folder
  }

  output {
    String hail_gcs_path = BgenHailIndex.hail_gcs_path
    File hail_local_path = BgenHailIndex.hail_local_path
  }
}

task BgenHailIndex {
  input {
    File input_bgen
    File input_bgen_sample

    String reference_genome

    String? project_id
    String? target_gcp_folder

    String docker = "shengqh/hail_gcp:20240211"
    Int memory_gb = 64
    Int preemptible = 0
    Int cpu = 4
    Int? disk_size_override
    Int boot_disk_gb = 25
  }

  Int disk_size = select_first([disk_size_override, ceil(size(input_bgen, "GB")) + 10])
  Int total_memory_gb = memory_gb + 2

  Boolean output_to_gcp = defined(target_gcp_folder)
  String gcs_output_dir = sub("~{target_gcp_folder}", "/+$", "") + "/"
  String gcs_output_path = if output_to_gcp then gcs_output_dir else ""

  String basename_input_bgen = basename(input_bgen)
  String basename_input_bgen_sample = basename(input_bgen_sample)

  String local_output_file = "~{basename_input_bgen}.idx2/metadata.json.gz"

  command <<<

#https://discuss.hail.is/t/i-get-a-negativearraysizeexception-when-loading-a-plink-file/899
export PYSPARK_SUBMIT_ARGS="--driver-java-options '-XX:hashCode=0' --conf 'spark.executor.extraJavaOptions=-XX:hashCode=0' pyspark-shell"

mkdir -p tmp

ln -s ~{input_bgen} ~{basename_input_bgen}
ln -s ~{input_bgen_sample} ~{basename_input_bgen_sample}

cat <<CODE > bgen_hail_index.py
import logging
import hail as hl

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

logger.info("Index bgen file ...")
hl.index_bgen("~{basename_input_bgen}", 
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
                                'MT': 'chrM'})
CODE

set -o pipefail

python3 bgen_hail_index.py

if [[ -f "~{local_output_file}" ]]; then
  echo "Writing completed successfully."

  if [[ "~{output_to_gcp}" == "true" ]]; then
    echo "Copying MatrixTable to GCS..."
    gsutil ~{"-u " + project_id} -m rsync -Cr ~{basename_input_bgen}.idx2 ~{gcs_output_path}

    res=$?
    if [[ $res -ne 0 ]]; then
      echo "Copying to GCS failed."
      exit $res
    fi

    echo "Copying to GCS succeed."
    touch hail_copied_to_gcp.txt
    exit 0
  else
    echo "Compressing hail matrix index ..."
    tar czf ~{basename_input_bgen}.idx2.tar.gz ~{basename_input_bgen}.idx2
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
    String hail_gcs_path = if output_to_gcp then "~{gcs_output_path}~{basename_input_bgen}.idx2" else ""
    File hail_local_path = if output_to_gcp then "hail_copied_to_gcp.txt" else "~{basename_input_bgen}.idx2.tar.gz"
  }
}