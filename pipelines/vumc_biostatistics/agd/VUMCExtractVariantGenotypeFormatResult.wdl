version 1.0


import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow formats genotype results from BED, VCF, and Annovar files
# into a CSV file with the desired format.
#
# The workflow performs the following steps:
# 1. Process input BED, VCF, and Annovar files to extract genotype information
# 2. Remove HG samples and invalid samples
# 3. Filter out samples with all 0/0 genotypes
# 4. Transpose the data to create the final CSV format
# 5. Optionally copy the output files to a specified GCP folder
#
# Input parameters:
# - input_bed_file: A BED file containing variant coordinates
# - input_vcf_file: A VCF file containing genotype information
# - input_annovar_file: An Annovar annotation file
# - output_prefix: The prefix for the output files
# - billing_gcp_project_id: (Optional) The GCP project ID for billing
# - target_gcp_folder: (Optional) The GCP folder to copy the output files to
#
# Output files:
# - output_genotype_csv: The final CSV file containing formatted genotype information

workflow VUMCExtractVariantGenotypeFormatResult {
  input {
    File input_bed_file
    File input_vcf_file
    File input_annovar_file
    String output_prefix

    String? billing_gcp_project_id
    String? target_gcp_folder    
  }

  call FormatResult {
    input:
      input_bed_file = input_bed_file,
      input_vcf_file = input_vcf_file,
      input_annovar_file = input_annovar_file,
      output_prefix = output_prefix,
  }

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = FormatResult.output_genotype_csv,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_genotype_csv = select_first([CopyFile.output_file, FormatResult.output_genotype_csv])
  }
}

task FormatResult {
  input {
    File input_bed_file
    File input_vcf_file
    File input_annovar_file

    String output_prefix
    String docker = "shengqh/report:20241120"

    Int memory_gb = 20
  }

  Int disk_size = ceil(size([input_bed_file, input_vcf_file, input_annovar_file], "GB")) + 10

  command <<<

zcat ~{input_vcf_file} | grep -v "^##" | cut -f7- > request.clean
zcat ~{input_annovar_file} > annovar.clean

paste annovar.clean request.clean > request.annovar.final.tsv

cat <<EOF > transpose.r 

bed_file="~{input_bed_file}"
annovar_file="request.annovar.final.tsv"
output_file="~{output_prefix}.csv"

library(data.table)
library(dplyr)

fbed=fread(bed_file, sep="\t", data.table=FALSE) |>
  dplyr::rename( Name=V4 ) |>
  dplyr::select(-V1,-V2,-V3)

fdat=fread(annovar_file, sep="\t", data.table=FALSE)
samples=colnames(fdat)[14:ncol(fdat)]

fdat = fdat |>
  dplyr::mutate(Chr=paste0("chr", Chr),
                Position=Start-1,
                Name=paste0(Chr, ":", Position, ":", Ref, ":", Alt)) |>
  dplyr::select(-Position)

fcomb = merge(fbed, fdat, by="Name") |> 
  dplyr::select(-GeneDetail.refGene, -ExonicFunc.refGene, -AAChange.refGene, -FILTER, -INFO, -FORMAT) |>
  dplyr::select(Name, everything())  

fcomb = fcomb[,!grepl("HG", colnames(fcomb))] #remove all HG samples
fcomb = fcomb[,!grepl("_INVALID", colnames(fcomb))] #remove all invalid samples

samples=samples[!grepl("HG", samples) & !grepl("_INVALID", samples)]

fdata=fcomb[,samples] #remove all non-genotype columns

#remove all samples with all 0/0 genotypes
snv_count=nrow(fcomb) - apply(fdata, 2, function(x) sum(x=="0/0" | x=="./." | x=="0|0" | x==".|." | x=="0"))
table(snv_count)

ffiltered_cols=colnames(fdata)[snv_count == 0]

ffiltered=fcomb[,!colnames(fcomb) %in% ffiltered_cols]

mdat=t(ffiltered)

write.table(mdat, output_file, sep=",", row.names=TRUE, col.names=FALSE, quote=F)

EOF

R -f transpose.r

if [[ ! -f "~{output_prefix}.csv" ]]; then
  echo "Error: Output CSV file not found."
  exit 1
fi

>>>

  runtime {
    docker: docker
    preemptible: 3
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }

  output {
    File output_genotype_csv = "~{output_prefix}.csv"
  }
}