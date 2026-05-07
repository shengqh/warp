version 1.0


import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow extracts variant genotypes from PVAR and VCF files
# and formats the results into CSV files.
#
# The workflow performs the following steps:
# 1. Process input PVAR and VCF files to extract genotype information
# 2. Remove HG samples and invalid samples
# 3. Filter out samples with all 0/0 genotypes
# 4. Generate both variant-centric and sample-centric CSV outputs
# 5. Optionally copy the output files to a specified GCP folder
#
# Input parameters:
# - input_pvar_file: A PVAR file containing variant information
# - input_vcf_file: A VCF file containing genotype information
# - output_prefix: The prefix for the output files
# - target_gcp_folder: (Optional) The GCP folder to copy the output files to
#
# Output files:
# - output_variant_csv: CSV file with variant-centric information
# - output_sample_csv: CSV file with sample-centric information
# - num_samples: The number of samples with valid genotypes

workflow VUMCExtractVariantGenotypeByPvarFormatResult {
  input {
    File input_pvar_file
    File input_psam_file
    File input_vcf_file
    String output_prefix

    String? target_gcp_folder    
  }

  call FormatResult {
    input:
      input_pvar_file = input_pvar_file,
      input_psam_file = input_psam_file,
      input_vcf_file = input_vcf_file,
      output_prefix = output_prefix,
  }

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = FormatResult.output_variant_csv,
        source_file2 = FormatResult.output_sample_csv,
        source_file3 = FormatResult.output_psam,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_variant_csv = select_first([CopyFile.output_file1, FormatResult.output_variant_csv])
    File output_sample_csv = select_first([CopyFile.output_file2, FormatResult.output_sample_csv])
    File output_psam = select_first([CopyFile.output_file3, FormatResult.output_psam])
    Int num_samples = FormatResult.num_samples
  }
}

task FormatResult {
  input {
    File input_pvar_file
    File input_psam_file

    File input_vcf_file

    String output_prefix
    String docker = "shengqh/report:20250415"

    Int additional_disk_gb = 20
    Int memory_gb = 20
  }

  Int disk_size = ceil(size([input_pvar_file, input_vcf_file], "GB")) + additional_disk_gb

  command <<<

cat <<EOF> transpose.r 

pvar_file="~{input_pvar_file}"
psam_file="~{input_psam_file}"
vcf_file="~{input_vcf_file}"
output_prefix="~{output_prefix}"

library(data.table)
library(dplyr)

fpvar=fread(pvar_file, sep="\t", data.table=FALSE) |>
  dplyr::rename("CHROM"=1)

fpsam=fread(psam_file, sep="\t", data.table=FALSE) 

fvcf=fread(vcf_file, sep="\t", data.table=FALSE) |>
  dplyr::rename("CHROM"=1)

fvcf = fvcf[,!grepl("HG", colnames(fvcf))] #remove all HG samples
fvcf = fvcf[,!grepl("_INVALID", colnames(fvcf))] #remove all invalid samples

samples=colnames(fvcf)[10:ncol(fvcf)]

fdata=fvcf[,samples] #remove all non-genotype columns

#remove all samples with all 0/0 genotypes
snv_count=nrow(fdata) - apply(fdata, 2, function(x) sum(x=="0/0" | x=="./." | x=="0|0" | x==".|." | x=="0"))

print(table(snv_count))

ffiltered_samples=colnames(fdata)[snv_count > 0]

ffiltered_psam=fpsam |>
  dplyr::filter(IID %in% ffiltered_samples) 
write.table(ffiltered_psam, paste0(output_prefix, ".psam"), sep="\t", row.names=FALSE, col.names=TRUE, quote=F)

fvcf_filtered=fvcf[,c('ID', ffiltered_samples)]

fcomb = merge(fpvar, fvcf_filtered, by="ID")
write.table(fcomb, paste0(output_prefix, ".variant.csv"), sep=",", row.names=FALSE, col.names=TRUE, quote=F)

mdat=t(fcomb)
write.table(mdat, paste0(output_prefix, ".sample.csv"), sep=",", row.names=TRUE, col.names=FALSE, quote=F)

n_samples=sum(snv_count > 0)
writeLines(as.character(n_samples), "num_samples.txt")

EOF

R -f transpose.r

>>>

  runtime {
    docker: docker
    preemptible: 3
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }

  output {
    Int num_samples = read_int("num_samples.txt")
    File output_psam = "~{output_prefix}.psam"
    File output_variant_csv = "~{output_prefix}.variant.csv"
    File output_sample_csv = "~{output_prefix}.sample.csv"
  }
}