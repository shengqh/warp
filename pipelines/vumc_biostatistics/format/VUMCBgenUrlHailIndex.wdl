version 1.0

## VUMC BGEN Hail Indexing Workflow
##
## This workflow creates Hail index files for a BGEN format genetic data file.
## Developed by VUMC/VANGARD team for efficient processing of population genetic data.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## BGEN is a file format for storing large genetic datasets used in population genetics.
## Creating Hail index files allows for efficient querying and analysis using the Hail framework.
##
## ### Workflow Steps:
## 1. Uses Hail to create index files for the input BGEN file
## 2. Optionally copies the resulting index files to a specified GCP folder
##
## ### Inputs:
## - input_bgen: Input BGEN file to be indexed
## - input_bgen_sample: Sample file associated with the BGEN file
## - reference_genome: Reference genome version (default: "GRCh38")
## - project_id: Optional GCP project ID for file copy operations
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - bgen_hail_index_gcp: Path to the Hail index in GCS (if uploaded)
## - bgen_hail_index_local: Local path to the Hail index file or confirmation of GCP copy
##
## ### Notes:
## - Modified from Broad Institute's long-read-pipelines
## - Uses Hail for efficient indexing and preparation for downstream analyses
## - File copy operation to GCP is optional and only executed if a target folder is provided


workflow VUMCBgenUrlHailIndex {
  #modified based on 
  #https://github.com/broadinstitute/long-read-pipelines/blob/7d36a93964998f513a132b86ca9ace6c663d3327/wdl/tasks/Utility/Hail.wdl
 
  input {
    String input_bgen
    String input_bgen_sample

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
    String bgen_hail_index_gcp = BgenHailIndex.bgen_hail_index_gcp
    File bgen_hail_index_local = BgenHailIndex.bgen_hail_index_local
  }
}

task BgenHailIndex {
  input {
    String input_bgen
    String input_bgen_sample

    String reference_genome

    String? project_id
    String? target_gcp_folder

    String docker = "shengqh/hail_gcp:20240211"
    Int memory_gb = 20
    Int preemptible = 1
    Int cpu = 4
    Int disk_size = 20
  }

  Int total_memory_gb = memory_gb + 2

  Boolean output_to_gcp = defined(target_gcp_folder)
  String gcs_output_dir = sub("~{target_gcp_folder}", "/+$", "")
  String gcs_output_path = if output_to_gcp then gcs_output_dir else ""

  String basename_input_bgen = basename(input_bgen)
  String local_output_file = "~{basename_input_bgen}.idx2/metadata.json.gz"

  command <<<

#https://discuss.hail.is/t/i-get-a-negativearraysizeexception-when-loading-a-plink-file/899
export PYSPARK_SUBMIT_ARGS="--driver-java-options '-XX:hashCode=0' --conf 'spark.executor.extraJavaOptions=-XX:hashCode=0' pyspark-shell"

mkdir -p tmp

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
CODE

set -o pipefail

python3 bgen_hail_index.py

if [[ -f "~{local_output_file}" ]]; then
  echo "Writing completed successfully."

  if [[ "~{output_to_gcp}" == "true" ]]; then
    echo "Copying MatrixTable to GCS..."
    gsutil ~{"-u " + project_id} -m rsync -Cr ~{basename_input_bgen}.idx2 ~{gcs_output_path}/~{basename_input_bgen}.idx2

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
  }
  output {
    String bgen_hail_index_gcp = if output_to_gcp then "~{gcs_output_path}~{basename_input_bgen}.idx2" else ""
    File bgen_hail_index_local = if output_to_gcp then "hail_copied_to_gcp.txt" else "~{basename_input_bgen}.idx2.tar.gz"
  }
}