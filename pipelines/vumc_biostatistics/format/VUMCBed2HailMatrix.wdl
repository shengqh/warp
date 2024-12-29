version 1.0

workflow VUMCBed2HailMatrix {
  input {
    File input_bed
    File input_bim
    File input_fam

    String reference_genome = "GRCh38"

    String output_prefix

    String? project_id
    String? target_gcp_folder
  }

  call Bed2HailMatrix {
    input:
      input_bed = input_bed,
      input_bim = input_bim,
      input_fam = input_fam,

      reference_genome = reference_genome,
      output_prefix = output_prefix,

      project_id = project_id,
      target_gcp_folder = target_gcp_folder
  }

  output {
    String hail_gcs_path = Bed2HailMatrix.hail_gcs_path
    File hail_local_path = Bed2HailMatrix.hail_local_path
  }
}

task Bed2HailMatrix {
  input {
    File input_bed
    File input_bim
    File input_fam

    String reference_genome
    String output_prefix

    String? project_id
    String? target_gcp_folder

    String docker = "hailgenetics/hail:0.2.127-py3.11"
    Int memory_gb = 20
  }

  Int disk_size = ceil(size([input_bed, input_bim, input_fam], "GB")  * 3) + 20
  Int total_memory_gb = memory_gb + 2

  Boolean output_to_gcp = defined(target_gcp_folder)
  String gcs_output_dir = sub("~{target_gcp_folder}", "/+$", "")
  String gcs_output_path = if output_to_gcp then gcs_output_dir + "/" + output_prefix else ""

  String local_output_file = "~{output_prefix}/metadata.json.gz"

  command <<<

#https://discuss.hail.is/t/i-get-a-negativearraysizeexception-when-loading-a-plink-file/899

export PYSPARK_SUBMIT_ARGS="--driver-java-options '-XX:hashCode=0' --conf 'spark.executor.extraJavaOptions=-XX:hashCode=0' pyspark-shell"

mkdir -p tmp

cat <<CODE > bed2hail.py

import logging
import hail as hl

logger = logging.getLogger('b2h')
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

#contig_recoding is hard coded for human only
logger.info("Reading from ~{input_bed} ...")
mt = hl.import_plink( bed="~{input_bed}",
                      bim="~{input_bim}",
                      fam="~{input_fam}",
                      reference_genome="~{reference_genome}",
                      contig_recoding={
                        '1': 'chr1',
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

logger.info("Writing MatrixTable to ~{output_prefix} ...")
mt.write("~{output_prefix}", overwrite=True)

CODE

python3 bed2hail.py

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
    rm -rf ~{output_prefix}
  fi

else
  echo "Writing failed."
  exit 1
fi

>>>

  runtime {
    docker: "~{docker}"
    preemptible: 1
    disks: "local-disk ~{disk_size} HDD"
    memory: "~{total_memory_gb} GiB"
  }
  output {
    String hail_gcs_path = "~{gcs_output_path}"
    File hail_local_path = if output_to_gcp then "hail_copied_to_gcp.txt" else "~{output_prefix}.tar.gz"
  }
}
