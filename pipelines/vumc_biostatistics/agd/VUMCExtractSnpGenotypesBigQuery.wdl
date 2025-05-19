version 1.0

import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../annotation/VUMCAnnovar.wdl" as VUMCAnnovar

# This workflow extracts SNP genotypes from VUMC pgen files based on a list of rsids
# and generates a CSV file with the genotype information.
# The workflow performs the following steps:
# 1. Convert the input rsid file or rsids to a BED file.
# 2. Get the chromosome indices for the input chromosomes.
# 3. Filter the pgen files based on the BED file.
# 4. Merge the filtered pgen files if there are multiple chromosomes.
# 5. Filter samples without SNV.
# 6. Convert the filtered pgen files to VCF format.
# 7. Annotate the VCF file using Annovar.
# 8. Format the results into a CSV file.
# 9. Optionally move the output files to a specified GCP folder.
# 10. Outputs the final files including the BED, VCF, and CSV files.
#
# Input parameters:
# - input_rsid_file: A file containing rsids, one per line.
# - input_rsids: A string containing rsids separated by spaces.
# - output_prefix: The prefix for the output files.
# - chromosomes: An array of chromosomes to process.
# - input_pgen_files: An array of input pgen files.
# - input_psam_files: An array of input psam files.
# - input_pvar_files: An array of input pvar files.
# - billing_gcp_project_id: The GCP project ID for billing.
# - target_gcp_folder: The GCP folder to move the output files to.
# Output files:
# - output_bed: The output BED file.
# - output_pgen: The output PGEN file.
# - output_pvar: The output PVAR file.
# - output_psam: The output PSAM file.
# - output_genotype_csv: The output CSV file containing genotype information.
# - output_num_variants: The number of variants in the output PGEN file.
# - output_num_samples: The number of samples in the output PGEN file.
# Note: The workflow uses the Plink2Utils and BioUtils tasks for filtering and converting pgen files.
# The Annovar task is used for annotating the VCF file.
# The GcpUtils task is used for moving files to GCP.

import "./VUMCExtractVariantBigQuery.wdl" as WdlList

workflow VUMCExtractSnpGenotypesBigQuery {
  input {
    # Input rsid file with column name rsid, each line contains one rsid
    String input_rsid_url

    String annovar_url="working-set-385118.agd250k.annovar"

    String output_prefix

    Array[String] chromosomes
    Array[File] input_pgen_files
    Array[File] input_psam_files
    Array[File] input_pvar_files

    String? billing_gcp_project_id
    String? target_gcp_folder    
  }

  # Find rdis from ANNOVAR table.
  call WdlList.ExtractVariantBigQuery as ExtractVariantBigQuery {
    input:
      annovar_url = annovar_url,
      input_rsid_url = input_rsid_url,
      output_prefix = output_prefix
  }

  # Before extract the SNVs, we need to find the chromosomes containing required SNVs.
  call BioUtils.GetChromosomeIndecies {
    input:
      input_chromosomes = chromosomes,
      input_bed_file = ExtractVariantBigQuery.output_variant_bed
  }

  Int num_valid_chromsome = length(GetChromosomeIndecies.chromosome_indecies)

  # Extract SNVs from the input pgen files.
  scatter(chrom_ind in range(num_valid_chromsome)){
    Int old_ind = GetChromosomeIndecies.chromosome_indecies[chrom_ind]
    File pgen_file = input_pgen_files[old_ind]
    File pvar_file = input_pvar_files[old_ind]
    File psam_file = input_psam_files[old_ind]
    String chromosome = chromosomes[old_ind]

    call Plink2Utils.Plink2FilterPgen as Plink2FilterPgen {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = psam_file,
        keep_bed = ExtractVariantBigQuery.output_variant_bed,
        plink2_filter_option = "",
        output_prefix = output_prefix + "." + chromosome + ".snp"
    }
  }

  # Combine them together
  if (num_valid_chromsome > 1){
    call Plink2Utils.MergePgenFiles as MergePgenFiles {
      input:
        input_pgen_files = Plink2FilterPgen.output_pgen,
        input_pvar_files = Plink2FilterPgen.output_pvar,
        input_psam_files = Plink2FilterPgen.output_psam,
        output_prefix = output_prefix
    }
  }

  # Remove the samples without any SNVs
  call Plink2Utils.FilterSamplesWithoutSNV {
    input:
      input_pgen = select_first([MergePgenFiles.output_pgen, Plink2FilterPgen.output_pgen[0]]),
      input_pvar = select_first([MergePgenFiles.output_pvar, Plink2FilterPgen.output_pvar[0]]),
      input_psam = select_first([MergePgenFiles.output_psam, Plink2FilterPgen.output_psam[0]]),
      output_prefix = output_prefix
  }

  # Convert the pgen files to VCF format
  call Plink2Utils.Pgen2Vcf {
    input:
      input_pgen = FilterSamplesWithoutSNV.output_pgen,
      input_pvar = FilterSamplesWithoutSNV.output_pvar,
      input_psam = FilterSamplesWithoutSNV.output_psam,
      output_prefix = output_prefix,
  }

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyFiveFiles as CopyFle {
      input:
        source_file1 = ExtractVariantBigQuery.output_variant_txt,
        source_file2 = ExtractVariantBigQuery.output_variant_bed,
        source_file3 = FilterSamplesWithoutSNV.output_pgen,
        source_file4 = FilterSamplesWithoutSNV.output_pvar,
        source_file5 = FilterSamplesWithoutSNV.output_psam,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  # call FormatResult {
  #   input:
  #     input_bed_file = ExtractVariantBigQuery.output_variant_txt,
  #     input_vcf_file = Pgen2Vcf.output_vcf,
  #     output_prefix = output_prefix,
  # }

  # if (defined(target_gcp_folder)) {
  #   call GcpUtils.MoveOrCopySixFiles as CopyFle {
  #     input:
  #       source_file1 = ExtractVariantBigQuery.output_variant_txt,
  #       source_file2 = ExtractVariantBigQuery.output_variant_bed,
  #       source_file3 = FilterSamplesWithoutSNV.output_pgen,
  #       source_file4 = FilterSamplesWithoutSNV.output_pvar,
  #       source_file5 = FilterSamplesWithoutSNV.output_psam,
  #       source_file6 = FormatResult.output_genotype_csv,
  #       is_move_file = false,
  #       project_id = billing_gcp_project_id,
  #       target_gcp_folder = select_first([target_gcp_folder])
  #   }
  # }

  output {
    File output_variant_txt = select_first([CopyFle.output_file1, ExtractVariantBigQuery.output_variant_txt])
    File output_variant_bed = select_first([CopyFle.output_file2, ExtractVariantBigQuery.output_variant_bed])
    File output_pgen = select_first([CopyFle.output_file3, FilterSamplesWithoutSNV.output_pgen])
    File output_pvar = select_first([CopyFle.output_file4, FilterSamplesWithoutSNV.output_pvar])
    File output_psam = select_first([CopyFle.output_file5, FilterSamplesWithoutSNV.output_psam])
    #File output_genotype_csv = select_first([CopyFle.output_file6, FormatResult.output_genotype_csv])
    Int output_num_variants = FilterSamplesWithoutSNV.output_num_variants
    Int output_num_samples = FilterSamplesWithoutSNV.output_num_samples
  }
}

task FormatResult {
  input {
    File input_bed_file
    File input_vcf_file
    File input_annovar_file
    String output_prefix
    String docker = "shengqh/report:20241120"
  }

  Int disk_size = ceil(size([input_bed_file, input_vcf_file, input_annovar_file], "GB")) + 10

  command <<<

zcat ~{input_vcf_file} | grep -v "^##" | cut -f7- > request.clean
zcat ~{input_annovar_file} > annovar.clean

paste annovar.clean request.clean > request.annovar.final.tsv

cat > transpose.r << 'EOF'

bed_file="~{input_bed_file}"
annovar_file="request.annovar.final.tsv"
output_file="~{output_prefix}.csv"

library(data.table)
library(dplyr)

fbed=fread(bed_file, sep="\t", data.table=FALSE) |>
  dplyr::rename(
    Chr=V1,
    Start=V2,
    End=V3,
    Rsid=V4
  ) |>
  dplyr::mutate(
    Start=Start + 1,
    Locus= paste0(Chr, ":", Start)
  ) |>
  dplyr::select(Locus, Rsid)

fdat=fread(annovar_file, sep="\t", data.table=FALSE) |>
  dplyr::mutate(Chr=paste0("chr", Chr),
                Locus= paste0(Chr, ":", Start)) 

fcomb = merge(fdat, fbed, by="Locus", all.x=TRUE) |> 
  dplyr::select(-Locus, -GeneDetail.refGene, -ExonicFunc.refGene, -AAChange.refGene, -FILTER, -INFO, -FORMAT) |>
  tibble::column_to_rownames("Rsid") 

#remove all samples with all 0/0 genotypes
refcount=apply(fcomb, 2, function(x) sum(x=="0/0" | x=="./." | x=="0|0" | x==".|."))
ffiltered=fcomb[,refcount<nrow(fcomb)]

mdat=t(ffiltered)

write.csv(mdat, output_file, row.names=TRUE)

EOF

R -f transpose.r

  >>>

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: "10 GiB"
  }

  output {
    File output_genotype_csv = "~{output_prefix}.csv"
  }
}