version 1.0
## VUMC AGD RSID to PVAR Workflow
##
## This workflow converts a list of RSIDs to a PVAR file by querying the AGD BigQuery database.
## Developed by VUMC Biostatistics for genetic analysis projects.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## This pipeline queries a BigQuery Annovar table to extract variant information from AGD based on input RSIDs,
## and produces a PVAR file as output.
##
## ### Workflow Steps:
## 1. AgdRsidToPvar: Query BigQuery Annovar table using input RSIDs to generate a PVAR file.
## 2. Optionally copy the output PVAR file to a specified GCP folder.
##
## ### Inputs:
## - annovar_url: BigQuery table URL for Annovar data (default: working-set-385118.agd250k.cb_avsnp_variant)
## - input_rsid_url: GCS path to file containing RSIDs to query, with column name "RSID" or without column name
## - output_prefix: Prefix for output files
## - target_gcp_folder: Optional target GCP folder for the output file
##
## ### Outputs:
## - output_pvar: Path to the output PVAR file containing variant information
##
## ### Notes:
## - Uses GcpUtils for file copy operations.
## - File copy operation to GCP is optional and only executed if a target folder is provided.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAgdVaridToRsid {
  input {
    String annovar_url='working-set-385118.agd250k.cb_avsnp_variant'

    String input_varid_url
    String output_prefix

    String? target_gcp_folder
  }

  call AgdVaridToRsid {
    input:
      annovar_url = annovar_url,
      input_varid_url = input_varid_url,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = AgdVaridToRsid.output_rsid_csv,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_rsid_csv = select_first([CopyFile.output_file , AgdVaridToRsid.output_rsid_csv])
  }
}

task AgdVaridToRsid {
  input {
    String annovar_url

    String input_varid_url
    String output_prefix

    String docker = "us.gcr.io/broad-dsp-gcr-public/terra-jupyter-hail:1.1.14"
  }

  # using `~{annovar_url}` will fail. "`" would cause problem.
  command <<<

cat <<EOF > query_table.py

import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

# Configure the external data source and query job.
external_config = bigquery.ExternalConfig("CSV")
external_config.source_uris = [
    "~{input_varid_url}"
]
external_config.schema = [
    bigquery.SchemaField("VARIANT_ID", "STRING"),
]
assert external_config.csv_options is not None

# Even if there is header, using it in query will get identical result as removing it, so just pretend it is a unmatchable rsid.
external_config.csv_options.skip_leading_rows = 0

print(f"annovar_url: ~{annovar_url}")

table_id = "variant_id_tbl"
job_config = bigquery.QueryJobConfig(table_definitions={table_id: external_config})

query = f"""SELECT * from {table_id}"""
print(query)

res = client.query(query, job_config=job_config).result().to_dataframe()  # Make an API request.
print(res.shape)
res.head()

query = f"""SELECT
  DISTINCT anno.ID, anno.avsnp151
FROM
  \`~{annovar_url}\` as anno,
  {table_id} as g
WHERE
  anno.ID = g.VARIANT_ID
"""
print(query)

anno_res = client.query(query, job_config=job_config).result().to_dataframe()  # Make an API request.
print(anno_res.shape)
anno_res.to_csv("~{output_prefix}.variantid_rsid.csv", sep="\t", index=False, header=True)

EOF

python3 query_table.py

  >>>

  runtime {
    docker: docker
    memory: 10 + " GiB"
    disks: "local-disk " + 10 + " HDD"
    cpu: 1
    preemptible: 1
  }

  output {
    File output_rsid_csv = "~{output_prefix}.variantid_rsid.csv"
  }
}
