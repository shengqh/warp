version 1.0


import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow processes PVAR, PSAM, and VCF files to format variant and sample data
# and generates CSV outputs for downstream analysis.
#
# The workflow performs the following steps:
# 1. Reads PVAR file for variant information and PSAM file for sample metadata
# 2. Processes VCF file to extract genotype data
# 3. Removes HG samples and samples marked as invalid
# 4. Merges variant and genotype information
# 5. Generates both variant-centric and sample-centric CSV outputs
# 6. Optionally copies the output files to a specified GCP folder
#
# Input parameters:
# - keep_pvar: PVAR file containing variant information to keep
# - input_sample_with_variant_psam: PSAM file with sample metadata
# - input_sample_with_variant_vcf: VCF file containing genotype data
# - output_prefix: The prefix for the output files
# - target_gcp_folder: (Optional) The GCP folder to copy the output files to
#
# Output files:
# - output_by_variant_csv: CSV file with variant-centric view of the data
# - output_by_sample_csv: CSV file with sample-centric (transposed) view of the data


workflow VUMCAgdPgenExtractSampleWithVariantByPvarFormatResult {
  input {
    File keep_pvar
    File input_sample_with_variant_psam
    File input_sample_with_variant_vcf
    String output_prefix

    String? target_gcp_folder    
  }

  call FormatResult {
    input:
      keep_pvar = keep_pvar,
      input_sample_with_variant_psam = input_sample_with_variant_psam,
      input_sample_with_variant_vcf = input_sample_with_variant_vcf,
      output_prefix = output_prefix,
  }

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = FormatResult.output_by_variant_tsv,
        source_file2 = FormatResult.output_by_sample_tsv,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_by_variant_tsv = select_first([CopyFile.output_file1, FormatResult.output_by_variant_tsv])
    String output_by_sample_tsv = select_first([CopyFile.output_file2, FormatResult.output_by_sample_tsv])
  }
}

task FormatResult {
  input {
    File keep_pvar
    File input_sample_with_variant_psam

    File input_sample_with_variant_vcf

    String output_prefix
    String docker = "shengqh/report:20250415"

    Int memory_gb = 20
  }

  Int disk_size = ceil(size([keep_pvar, input_sample_with_variant_psam, input_sample_with_variant_vcf], "GB")) + 10

  command <<<

cat <<EOF> transpose.r 

pvar_file="~{keep_pvar}"
psam_file="~{input_sample_with_variant_psam}"
vcf_file="~{input_sample_with_variant_vcf}"
output_prefix="~{output_prefix}"

library(data.table)
library(dplyr)

fpvar=fread(pvar_file, sep="\t", data.table=FALSE) |>
  dplyr::rename("CHROM"=1)

fpsam=fread(psam_file, sep="\t", data.table=FALSE) 

fvcf=fread(vcf_file, sep="\t", data.table=FALSE) |>
  dplyr::rename("CHROM"=1) |>
  dplyr::select(-c("CHROM", "POS", "REF", "ALT", "FORMAT", "QUAL", "FILTER", "INFO"))

samples=colnames(fvcf)[2:ncol(fvcf)]

fcomb=merge(fpvar, fvcf, by="ID")

header_cols=colnames(fcomb)[!colnames(fcomb) %in% samples]

fheader=fcomb[, header_cols]

fdata=fcomb[, samples] 

convert_genotype <- function(geno_matrix) {
  # Check if input is a matrix
  if (!is.matrix(geno_matrix)) {
    stop("Input must be a matrix")
  }
  
  # Define genotype mapping
  genotype_map <- c("0" = 0,
                    "0/0" = 0, 
                    "0|0" = 0,
                    "0/1" = 1,
                    "0|1" = 1,
                    "1|0" = 1, 
                    "1/0" = 1, 
                    "1/1" = 2,
                    "1|1" = 2, 
                    "." = NA_integer_,
                    "./." = NA_integer_)
  
  # Convert string matrix to integer matrix
  integer_matrix <- matrix(NA_integer_, nrow = nrow(geno_matrix), ncol = ncol(geno_matrix))
  
  # Iterate over unique values for efficiency
  unique_vals <- unique(as.vector(geno_matrix))
  for (val in unique_vals) {
    if (val %in% names(genotype_map)) {
      integer_matrix[geno_matrix == val] <- genotype_map[val]
    } else {
      warning(paste("Unknown genotype value:", val, "- treated as NA"))
      integer_matrix[geno_matrix == val] <- NA_integer_
    }
  }
  
  # Preserve row and column names
  dimnames(integer_matrix) <- dimnames(geno_matrix)
  
  return(integer_matrix)
}

fmat = convert_genotype(as.matrix(fdata))
fmat_col_sums = colSums(fmat > 0, na.rm=TRUE)

# Order columns by the number of non-zero values and then by column name
order_idx <- order(fmat_col_sums, decreasing = TRUE)
fmat_ordered <- fmat[, order_idx]

final=cbind(fheader, fmat_ordered)
write.table(final, gzfile(paste0(output_prefix, ".by_variant.tsv.gz")), sep="\t", row.names=FALSE, col.names=TRUE, quote=F)

mdat=t(final |> tibble::column_to_rownames(var="ID")) |> as.data.frame() |> tibble::rownames_to_column(var="SampleID")
mdat[mdat == " 0"] <- "0"
mdat[mdat == " 1"] <- "1"
mdat[mdat == " 2"] <- "2"
write.table(mdat, gzfile(paste0(output_prefix, ".by_sample.tsv.gz")), sep="\t", row.names=FALSE, col.names=TRUE, quote=F)

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
    File output_by_variant_tsv = "~{output_prefix}.by_variant.tsv.gz"
    File output_by_sample_tsv = "~{output_prefix}.by_sample.tsv.gz"
  }
}