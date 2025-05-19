version 1.0

## VUMC Extract Variant BigQuery Workflow
##
## This workflow handles the extraction of variant information from BigQuery using RSID inputs.
## Developed by VUMC Biostatistics for genetic analysis projects.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## This pipeline queries a BigQuery Annovar table to extract variant information based on input RSIDs,
## and produces a BED format output file.
##
## ### Workflow Steps:
## 1. ExtractVariantBigQuery: Query BigQuery Annovar table using input RSIDs
## 2. Optionally copy output files to a specified GCP folder
##
## ### Inputs:
## - annovar_url: BigQuery table URL for Annovar data
## - input_rsid_url: File containing RSIDs to query, with column name "rsid"
## - output_prefix: Prefix for output files
## - billing_gcp_project_id: Optional GCP project ID for file copy operations
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - output_variant_bed: Path to the output BED file containing extracted variant information
##
## ### Notes:
## - Uses GcpUtils for file copy operations
## - Produces a BED file sorted by genomic coordinates
## - File copy operation to GCP is optional and only executed if a target folder is provided

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCExtractVariantBigQuery {
  input {
    String annovar_url='vangard-workflow-data.agd250k.annovar'

    String input_rsid_url
    String output_prefix

    String? billing_gcp_project_id
    String? target_gcp_folder
  }

  call ExtractVariantBigQuery {
    input:
      annovar_url = annovar_url,
      input_rsid_url = input_rsid_url,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = ExtractVariantBigQuery.output_variant_bed,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_variant_bed = select_first([CopyFile.output_file , ExtractVariantBigQuery.output_variant_bed])
  }
}

task ExtractVariantBigQuery {
  input {
    String annovar_url

    File input_rsid_url
    String output_prefix

    String docker = "shengqh/hail_gcp:20241120"
  }

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

table_id = "rsid_tbl"
job_config = bigquery.QueryJobConfig(table_definitions={table_id: external_config})

query = f"""SELECT * from {table_id}"""
print(query)

res = client.query(query, job_config=job_config).result().to_dataframe()  # Make an API request.
print(res.shape)
res.head()

query = f"""
SELECT DISTINCT anno.Chr, anno.Start, anno.End, anno.avsnp150, anno.Ref, anno.Alt
FROM `~{annovar_url}` as anno,
    {table_id} as g
WHERE anno.avsnp150 = g.RSID
"""
print(query)

anno_res = client.query(query, job_config=job_config).result().to_dataframe()  # Make an API request.
print(anno_res.shape)
anno_res= anno_res.sort_values(by=['Chr', 'Start'])
anno_res

anno_res.to_csv("~{output_prefix}.bed", sep="\t", index=False, header=False)

anno_res.Start=anno_res.Start-1
anno_res['Name'] = anno_res['avsnp150'] + ":" + anno_res['Ref'] + ":" + anno_res['Alt']
anno_res=anno_res[['Chr', 'Start', 'End', 'Name']]
anno_res

anno_res.to_csv("~{output_prefix}.bed", sep="\t", index=False, header=False)

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
    File output_variant_bed = "~{output_prefix}.bed"
  }
}
