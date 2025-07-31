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
## - annovar_url: BigQuery table URL for Annovar data (default: working-set-385118.agd250k.annovar_pvar)
## - input_rsid_url: GCS path to file containing RSIDs to query, with column name "RSID"
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

workflow VUMCAgdRsidToPvar {
  input {
    String annovar_url='working-set-385118.agd250k.annovar_pvar'

    String input_rsid_url
    String output_prefix

    String? target_gcp_folder
  }

  call AgdRsidToPvar {
    input:
      annovar_url = annovar_url,
      input_rsid_url = input_rsid_url,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = AgdRsidToPvar.output_pvar,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_pvar = select_first([CopyFile.output_file , AgdRsidToPvar.output_pvar])
  }
}

task AgdRsidToPvar {
  input {
    String annovar_url

    String input_rsid_url
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
    "~{input_rsid_url}"
]
external_config.schema = [
    bigquery.SchemaField("RSID", "STRING"),
]
assert external_config.csv_options is not None
external_config.csv_options.skip_leading_rows = 1

print(f"annovar_url: ~{annovar_url}")

table_id = "rsid_tbl"
job_config = bigquery.QueryJobConfig(table_definitions={table_id: external_config})

query = f"""SELECT * from {table_id}"""
print(query)

res = client.query(query, job_config=job_config).result().to_dataframe()  # Make an API request.
print(res.shape)
res.head()

query = f"""SELECT
  anno._CHROM, anno.POS, anno.ID, anno.REF, anno.ALT, anno.avsnp150 as FILTER, anno.INFO
FROM
  `~{annovar_url}` as anno,
  {table_id} as g
WHERE
  anno.avsnp150 = g.RSID
"""
print(query)

anno_res = client.query(query, job_config=job_config).result().to_dataframe()  # Make an API request.
print(anno_res.shape)
anno_res= anno_res.sort_values(by=['_CHROM', 'POS'])
anno_res.head()

anno_res['_CHROM'] = anno_res['_CHROM'].str.replace('chr', '')
anno_res.rename(columns={'_CHROM': '#CHROM'}, inplace=True)
anno_res.to_csv("~{output_prefix}.pvar", sep="\t", index=False, header=True)

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
    File output_pvar = "~{output_prefix}.pvar"
  }
}
