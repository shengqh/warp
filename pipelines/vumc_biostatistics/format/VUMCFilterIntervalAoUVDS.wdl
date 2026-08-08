version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC AoU VDS Interval Filter to Hail MatrixTable Workflow
##
## This workflow filters an AoU short-read WGS Hail VDS by a genomic interval
## and converts the filtered result to a dense Hail MatrixTable stored in GCS.
## Developed by VUMC Biostatistics for population genetics analysis.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## AoU short-read WGS variants in Hail VDS format are filtered to a target genomic interval
## and converted to a dense Hail MatrixTable for downstream Hail-based analyses.
##
## ### Workflow Steps:
## 1. **FilterVDS**: Reads the input AoU VDS and filters to the specified interval.
## 2. **LGT to GT Conversion**: Converts local genotype calls (LGT/LA) to GT.
## 3. **Dense Matrix Export**: Converts the filtered VDS to a dense MatrixTable
##    and writes it to the specified output path.
##
## ### Inputs:
## - vds_srwgs_path: Path to the input AoU short-read WGS VDS.
## - reference_genome: Reference genome version for Hail. Default is "GRCh38".
## - interval: Genomic interval used to filter the VDS.
## - output_prefix: Prefix for the output Hail MatrixTable path.
## - target_gcp_folder: GCP folder path to copy output files.
##
## ### Outputs:
## - hail_gcs_path: GCS path to the output Hail MatrixTable.

workflow VUMCFilterIntervalAoUVDS {
  input {
    String google_project_id = "wb-quick-okra-799"

    String vds_srwgs_path = "gs://vwb-aou-datasets-controlled/v9/wgs/short_read/snpindel/vds/hail.vds"
    
    String reference_genome = "GRCh38"

    String interval = "chr11:108217482-108374103"

    String output_prefix = "ATM_AoU"

    String target_gcp_folder = "gs://workspace-bucket-wb-quick-okra-799/00_ATM_variants/data/"
  }

  parameter_meta {
    vds_srwgs_path: "Path to the input VDS file"
    reference_genome: "Reference genome version for Hail. Default is 'GRCh38'."
    interval: "Genomic interval to filter the VDS"
    output_prefix: "Prefix for the output Hail MatrixTable path"
    target_gcp_folder: "GCP folder path to save output files"
  }

  call FilterVDS {
    input:
      google_project_id = google_project_id,
      vds_srwgs_path = vds_srwgs_path,
      reference_genome = reference_genome,
      interval = interval,
      output_prefix = output_prefix,
      target_gcp_folder = target_gcp_folder
  }

  output {
    String hail_gcs_path = FilterVDS.hail_gcs_path
    File hail_local_path = FilterVDS.hail_local_path
  }
}

task FilterVDS {
  input {
    String google_project_id
    String vds_srwgs_path
    String reference_genome
    String interval
    String output_prefix
    String target_gcp_folder

    String docker = "hailgenetics/hail:0.2.138-py3.13"
    Int memory_gb = 20
    Int disk_size = 20
  }

  Int total_memory_gb = memory_gb + 2

  String gcs_output_dir = sub("~{target_gcp_folder}", "/+$", "")
  String gcs_output_path = gcs_output_dir + "/" + output_prefix

  String local_output_file = "~{output_prefix}/metadata.json.gz"

  command <<<

#https://discuss.hail.is/t/i-get-a-negativearraysizeexception-when-loading-a-plink-file/899

export PYSPARK_SUBMIT_ARGS="pyspark-shell"

mkdir -p tmp

cat <<CODE > filter_vds.py

import logging
import hail as hl

logger = logging.getLogger('vds_interval')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)-8s - %(message)s')

logger.info("Calling hl.init ...")
hl.init(tmp_dir='./tmp',
        master='local[*]',  # Use all available cores
        min_block_size=128,  # Minimum block size in MB
        gcs_requester_pays_configuration='~{google_project_id}',
        quiet=True,
        spark_conf={
            'spark.driver.memory': '~{memory_gb}g',
            'spark.network.timeout': '800s',
            'spark.executor.heartbeatInterval': '400s'
        })

hl.default_reference(new_default_reference = "~{reference_genome}")

logger.info("Read vds from ~{vds_srwgs_path} ...")
vds = hl.vds.read_vds("~{vds_srwgs_path}")

parsed = [hl.parse_locus_interval("~{interval}")]

logger.info("Filtering vds by interval ~{interval} ...")
vds_interval = hl.vds.filter_intervals(vds, parsed)

logger.info(
    "Filtered variants: %d",
    vds_interval.variant_data.count_rows()
)

mt = vds_interval.variant_data

logger.info("Converting LGT to GT ...")
mt = mt.annotate_entries(GT=hl.vds.lgt_to_gt(mt.LGT, mt.LA))

logger.info("Converting VDS to dense MatrixTable ...")
vds_transformed = hl.vds.VariantDataset(vds.reference_data, mt)
mt_dense = hl.vds.to_dense_mt(vds_transformed)

logger.info("Writing MatrixTable to ~{output_prefix} ...")
mt_dense.write("~{gcs_output_path}", overwrite=True)

CODE

python3 filter_vds.py

>>>

  runtime {
    docker: "~{docker}"
    preemptible: 1
    disks: "local-disk ~{disk_size} SSD"
    memory: "~{total_memory_gb} GiB"
  }
  output {
    String hail_gcs_path = "~{gcs_output_path}"
  }
}
