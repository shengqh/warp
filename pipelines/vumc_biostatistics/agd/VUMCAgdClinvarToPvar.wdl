version 1.0
## VUMC AGD Genes to PVAR Workflow
##
## This workflow converts a list of Genes to a PVAR file by querying the AGD BigQuery database.
## Developed by VUMC Biostatistics for genetic analysis projects.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## This pipeline queries a BigQuery Annovar table to extract variant information from AGD based on input Genes,
## and produces a PVAR file as output. Only the variants classified as "Pathogenic" or "Likely_pathogenic" 
## in clinvar database with star 2+ confidence are included in the output.
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
## Clinvar Significance Distribution Table, clinvar20250721
## | Value                                        | Count     |
## |----------------------------------------------|-----------|
## | NULL                                         | 942693261 |
## | Uncertain_significance                       | 1091641   |
## | Likely_benign                                | 563757    |
## | Benign                                       | 192243    |
## | Conflicting_classifications_of_pathogenicity | 116445    |
## | Benign/Likely_benign                         | 48915     |
## | Pathogenic                                   | 25598     |
## | Likely_pathogenic                            | 14292     |
## | Pathogenic/Likely_pathogenic                 | 13657     |
## | not_provided                                 | 1769      |
##
## Clinvar Review Status Distribution Table
## Germline classification and oncogenicity
## | Gold Stars | Review Status                                        | Count     | Description                                                                                                                                                                                                                                                                                          |
## |------------|------------------------------------------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
## | four       | practice_guideline                                   | 44        | There is a submitted record with a classification from a practice guideline                                                                                                                                                                                                                         |
## | three      | reviewed_by_expert_panel                             | 7279      | There is a submitted record with a classification from an expert panel                                                                                                                                                                                                                              |
## | two        | criteria_provided,_multiple_submitters,_no_conflicts | 422645    | There are multiple submitted records with a classification. Assertion criteria and evidence for the classification (or a public contact) were provided and the classifications agree.                                                                                                               |
## | one        | criteria_provided,_conflicting_classifications       | 116351    | There are multiple submitted records with a classification, where assertion criteria and evidence for the classification (or a public contact) were provided. However there are conflicting classifications. The conflicting values for the classification are enumerated.                        |
## | one        | criteria_provided,_single_submitter                  | 1479408   | There is a single submitted record with a classification, where assertion criteria and evidence for the classification (or a public contact) were provided.                                                                                                                                        |
## | none       | no_assertion_criteria_provided                       | 42565     | There are one or more submitted records with a classification but without assertion criteria and evidence for the classification (or a public contact).                                                                                                                                             |
## | none       | no_classification_provided                           | 1768      | There are one or more submitted records without a classification.                                                                                                                                                                                                                                   |
## | none       | no_classification_for_the_single_variant             | 170       | The variant was not classified directly in any submitted record; it was submitted to ClinVar only as part of a haplotype or a genotype.                                                                                                                                                            |
## | none       | NULL                                                 | 942693261 | No review status information available.                                                                                                                                                                                                                                                             |
## | none       | .                                                    | 45        | Review status marked with a dot.                                                                                                                                                                                                                                                                    |
##

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAgdClinvarToPvar {
  input {
    String annovar_url='working-set-385118.agd250k.annovar_pvar_clinvar20250721'

    String output_prefix

    String? target_gcp_folder
  }

  call AgdClinvarToPvar {
    input:
      annovar_url = annovar_url,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = AgdClinvarToPvar.clinvar_pvar,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String clinvar_pvar = select_first([CopyFile.output_file , AgdClinvarToPvar.clinvar_pvar])
    Int clinvar_num_variants = AgdClinvarToPvar.clinvar_num_variants
  }
}

task AgdClinvarToPvar {
  input {
    String annovar_url
    String output_prefix
    String docker = "us.gcr.io/broad-dsp-gcr-public/terra-jupyter-hail:1.1.14"
  }

  # using `~{annovar_url}` will fail. "`" would cause problem.
  command <<<

cat <<EOF > query_table.py

import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

job_config = bigquery.QueryJobConfig(
    use_query_cache=True
)

query = f"""SELECT anno._CHROM, anno.POS, anno.ID, anno.Ref, anno.Alt, anno.avsnp151 as avsnp, anno.Gene_refGene, anno.CLNSIG, anno.CLNREVSTAT 
FROM
  \`~{annovar_url}\` as anno
WHERE 
    anno.CLNSIG is not NULL
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

grep -v "^#" "~{output_prefix}.pvar" | wc -l | cut -d ' ' -f 1 > num_variants.txt

  >>>

  runtime {
    docker: docker
    memory: 10 + " GiB"
    disks: "local-disk " + 10 + " HDD"
    cpu: 1
    preemptible: 1
  }

  output {
    File clinvar_pvar = "~{output_prefix}.pvar"
    Int clinvar_num_variants = read_int("num_variants.txt")
  }
}
