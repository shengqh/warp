version 1.0
## VUMC AGD Genes to PVAR Workflow
##
## This workflow converts a list of Genes to a PVAR file by querying the AGD BigQuery database.
## Developed by VUMC Biostatistics for genetic analysis projects.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## This pipeline queries a BigQuery Annovar table to extract variant information from AGD based on input Genes,
## and produces a PVAR file as output.
##
## ### Workflow Steps:
## 1. AgdGenesToPvar: Query BigQuery Annovar table using input Genes to generate a PVAR file.
## 2. Optionally copy the output PVAR file to a specified GCP folder.
##
## ### Inputs:
## - annovar_url: BigQuery table URL for Annovar data (default: working-set-385118.agd250k.annovar_pvar)
## - input_genes_url: GCS path to file containing Genes to query, with column name "GENE"
## - output_prefix: Prefix for output files
## - target_gcp_folder: Optional target GCP folder for the output file
##
## ### Outputs:
## - output_pvar: Path to the output PVAR file containing variant information
##
## ### Notes:
## - Uses GcpUtils for file copy operations.
## - File copy operation to GCP is optional and only executed if a target folder is provided.
## 
## Clinvar Significance Codes and count
## NULL 943035419
## Uncertain_significance 825362
## Likely_benign 511049
## Benign 188512
## Conflicting_classifications_of_pathogenicity 103251
## Benign/Likely_benign 47407
## Pathogenic 24788
## Likely_pathogenic 12262
## Pathogenic/Likely_pathogenic 11743
## not_provided 1834

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAgdPathogenicVariantInGeneToPvar {
  input {
    String annovar_url='working-set-385118.agd250k.annovar_pvar_clinvar20250721'

    String input_genes_url
    String output_prefix

    String? target_gcp_folder
  }

  call AgdPathogenicVariantInGeneToPvar {
    input:
      annovar_url = annovar_url,
      input_genes_url = input_genes_url,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = AgdPathogenicVariantInGeneToPvar.output_pvar,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String keep_pvar = select_first([CopyFile.output_file , AgdPathogenicVariantInGeneToPvar.output_pvar])
  }
}

task AgdPathogenicVariantInGeneToPvar {
  input {
    String annovar_url

    String input_genes_url
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
    "~{input_genes_url}"
]
external_config.schema = [
    bigquery.SchemaField("GENE", "STRING"),
]
assert external_config.csv_options is not None
external_config.csv_options.skip_leading_rows = 1

print(f"annovar_url: ~{annovar_url}")

table_id = "gene_tbl"
job_config = bigquery.QueryJobConfig(table_definitions={table_id: external_config})

query = f"""SELECT * from {table_id}"""
print(query)

res = client.query(query, job_config=job_config).result().to_dataframe()
print(res.shape)
res.head()

query = f"""SELECT
  DISTINCT anno._CHROM, anno.POS, anno.ID, anno.REF, anno.ALT, anno.avsnp150 as FILTER, anno.INFO
FROM
  \`~{annovar_url}\` as anno,
  {table_id} as g
WHERE
  anno.avsnp150 = g.RSID
"""


query = f"""SELECT anno._CHROM, anno.POS, anno.ID, anno.Ref, anno.Alt, anno.avsnp150, anno.Gene_refGene, anno.CLNSIG, anno.CLNREVSTAT 
FROM
  \`~{annovar_url}\` as anno,
  {table_id} as g
WHERE 
    anno.Gene_refGene = g.GENE 
    and 
    (anno.CLNSIG = 'Pathogenic/Likely_pathogenic' or 
     anno.CLNSIG = 'Pathogenic' or 
     anno.CLNSIG = 'Likely_pathogenic') 
    and
    (anno.CLNREVSTAT = 'criteria_provided,_multiple_submitters,_no_conflicts' or 
     anno.CLNREVSTAT = 'reviewed_by_expert_panel' or 
     anno.CLNREVSTAT = 'practice_guideline')
ORDER BY
    anno._CHROM, anno.POS
"""

print(query)

anno_res = client.query(query, job_config=job_config).result().to_dataframe()  # Make an API request.
print(anno_res.shape)
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
