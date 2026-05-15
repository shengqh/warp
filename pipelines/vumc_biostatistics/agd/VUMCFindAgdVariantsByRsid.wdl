version 1.0

## VUMC Extract AGD Variant Annotation By RSID Workflow
##
## This workflow handles the extraction of variant information from AGD BigQuery database using RSID inputs.
## Developed by VUMC Biostatistics for genetic analysis projects.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## This pipeline queries a BigQuery Annovar table to extract AGD variant information based on input RSIDs,
## and produces both a BED format and detailed variant annotation output files.
##
## ### Workflow Steps:
## 1. FindAgdVariantsByRsid: Query BigQuery Annovar table using input RSIDs
## 2. Optionally copy output files to a specified GCP folder
##
## ### Inputs:
## - annovar_url: BigQuery table URL for Annovar data (default: working-set-385118.agd250k.annovar_pvar_dnsnp157_clinvar20251109)
## - dbsnp_column: Column name for the dbSNP identifier (default: dbsnp157)
## - input_rsid_url: GCS path to file containing RSIDs to query, with/without column name "RSID" or without column name
## - output_prefix: Prefix for output files
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - output_variant_bed: Path to the output BED file containing extracted variant information
## - output_variant_txt: Path to the output text file containing complete Annovar annotations
##
## ### Notes:
## - Uses GcpUtils for file copy operations
## - Produces a BED file sorted by genomic coordinates
## - File copy operation to GCP is optional and only executed if a target folder is provided

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCFindAgdVariantsByRsid {
  input {
    String annovar_url='working-set-385118.agd250k.annovar_pvar_dnsnp157_clinvar20251109'
    String dbsnp_column='dbsnp157'

    String input_rsid_url
    String output_prefix

    String? target_gcp_folder
  }

  call FindAgdVariantsByRsid {
    input:
      annovar_url = annovar_url,
      dbsnp_column = dbsnp_column,
      input_rsid_url = input_rsid_url,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = FindAgdVariantsByRsid.output_variant_bed,
        source_file2 = FindAgdVariantsByRsid.output_variant_txt,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_variant_bed = select_first([CopyFile.output_file1 , FindAgdVariantsByRsid.output_variant_bed])
    File output_variant_txt = select_first([CopyFile.output_file2 , FindAgdVariantsByRsid.output_variant_txt])
  }
}

task FindAgdVariantsByRsid {
  input {
    String annovar_url
    String dbsnp_column

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

# Even if there is header, using it in query will get identical result as removing it, so just pretend it is a unmatchable rsid.
external_config.csv_options.skip_leading_rows = 0

annovar_url = "~{annovar_url}"
dbsnp_column = "~{dbsnp_column}"
print(f"annovar_url: {annovar_url}")

table_id = "rsid_tbl"
job_config = bigquery.QueryJobConfig(table_definitions={table_id: external_config})

query = f"""SELECT * from {table_id}"""
print(query)

res = client.query(query, job_config=job_config).result().to_dataframe()  # Make an API request.
print(res.shape)
res.head()

query = f"""SELECT
  anno.*
FROM
  {annovar_url} as anno,
  {table_id} as g
WHERE
  anno.{dbsnp_column} = g.RSID
"""
print(query)

anno_res = client.query(query, job_config=job_config).result().to_dataframe()  # Make an API request.
print(anno_res.shape)
anno_res= anno_res.sort_values(by=['Chr', 'Start'])
anno_res.head()

anno_res.to_csv("~{output_prefix}.annovar.txt", sep="\t", index=False, header=True)

anno_res.Start=anno_res.Start-1
anno_res['Name'] = anno_res[dbsnp_column] + ":" + anno_res['Ref'] + ":" + anno_res['Alt']
anno_res=anno_res[['Chr', 'Start', 'End', 'Name']]
anno_res.head()

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
    File output_variant_txt = "~{output_prefix}.annovar.txt"
    File output_variant_bed = "~{output_prefix}.bed"
  }
}
