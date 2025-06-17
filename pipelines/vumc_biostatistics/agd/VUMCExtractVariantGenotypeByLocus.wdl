version 1.0


import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WdlUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../annotation/VUMCAnnovar.wdl" as VUMCAnnovar

# This workflow extracts SNP genotypes from VUMC pgen files based on a ucsc bed file
# and generates a CSV file with the genotype information.
# The workflow performs the following steps:
# 1. Get the chromosome indices for the input chromosomes.
# 2. Filter the pgen files based on the BED file.
# 3. Merge the filtered pgen files if there are multiple chromosomes.
# 4. Filter samples without SNV.
# 5. Convert the filtered pgen files to VCF format.
# 6. Annotate the VCF file using Annovar.
# 7. Format the results into a CSV file.
# 8. Optionally move the output files to a specified GCP folder.
# 9. Outputs the final files including the BED, VCF, and CSV files.
#
# Input parameters:
# - input_bed: A file containing snv coordinates in ucsc bed format
# - output_prefix: The prefix for the output files.
# - chromosomes: An array of chromosomes to process.
# - input_pgen_files: An array of input pgen files.
# - input_psam_files: An array of input psam files.
# - input_pvar_files: An array of input pvar files.
# - billing_gcp_project_id: The GCP project ID for billing.
# - target_gcp_folder: The GCP folder to move the output files to.
# Output files:
# - output_pgen: The output PGEN file.
# - output_pvar: The output PVAR file.
# - output_psam: The output PSAM file.
# - output_genotype_csv: The output CSV file containing genotype information.
# - output_num_variants: The number of variants in the output PGEN file.
# - output_num_samples: The number of samples in the output PGEN file.
# Note: The workflow uses the Plink2Utils and BioUtils tasks for filtering and converting pgen files.
# The Annovar task is used for annotating the VCF file.
# The GcpUtils task is used for moving files to GCP.

workflow VUMCExtractVariantGenotypeByLocus {
  input {
    File input_bed

    String output_prefix

    Array[String] chromosomes
    Array[File] input_pgen_files
    Array[File] input_psam_files
    Array[File] input_pvar_files

    String? billing_gcp_project_id
    String? target_gcp_folder    
  }

  Int num_all_chromsome = length(chromosomes)

  scatter(all_chrom_ind in range(num_all_chromsome)){
    call BioUtils.CheckOverlapVariants as CheckOverlapVariants {
      input:
        chromosome = chromosomes[all_chrom_ind],
        input_pgen_pvar = input_pvar_files[all_chrom_ind],
        input_ucsc_bed = input_bed
    }
  }

  call WdlUtils.get_index_of_true {
    input:
      values = CheckOverlapVariants.has_variant
  }

  Int num_valid_chromsome = length(get_index_of_true.indices)

  scatter(chrom_ind in range(num_valid_chromsome)){
    Int old_ind = get_index_of_true.indices[chrom_ind]
    File pgen_file = input_pgen_files[old_ind]
    File pvar_file = input_pvar_files[old_ind]
    File psam_file = input_psam_files[old_ind]
    String chromosome = chromosomes[old_ind]

    call Plink2Utils.Plink2FilterPgen as Plink2FilterPgen {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = psam_file,
        keep_bed = input_bed,
        plink2_filter_option = "",
        output_prefix = output_prefix + "." + chromosome + ".snp"
    }
  }

  if (num_valid_chromsome > 1){
    call Plink2Utils.MergePgenFiles as MergePgenFiles {
      input:
        input_pgen_files = Plink2FilterPgen.output_pgen,
        input_pvar_files = Plink2FilterPgen.output_pvar,
        input_psam_files = Plink2FilterPgen.output_psam,
        output_prefix = output_prefix
    }
  }

  call Plink2Utils.FilterSamplesWithoutSNV {
    input:
      input_pgen = select_first([MergePgenFiles.output_pgen, Plink2FilterPgen.output_pgen[0]]),
      input_pvar = select_first([MergePgenFiles.output_pvar, Plink2FilterPgen.output_pvar[0]]),
      input_psam = select_first([MergePgenFiles.output_psam, Plink2FilterPgen.output_psam[0]]),
      output_prefix = output_prefix
  }

  call Plink2Utils.Pgen2Vcf {
    input:
      input_pgen = FilterSamplesWithoutSNV.output_pgen,
      input_pvar = FilterSamplesWithoutSNV.output_pvar,
      input_psam = FilterSamplesWithoutSNV.output_psam,
      output_prefix = output_prefix,
  }

  call VUMCAnnovar.Annovar {
    input:
      input_vcf = Pgen2Vcf.output_vcf,
      target_prefix = output_prefix,
  }

  call FormatResult {
    input:
      input_bed_file = input_bed,
      input_vcf_file = Pgen2Vcf.output_vcf,
      input_annovar_file = Annovar.annovar_file,
      output_prefix = output_prefix,
  }

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyFiveFiles as CopyFile {
      input:
        source_file1 = FilterSamplesWithoutSNV.output_pgen,
        source_file2 = FilterSamplesWithoutSNV.output_pvar,
        source_file3 = FilterSamplesWithoutSNV.output_psam,
        source_file4 = FormatResult.output_genotype_csv,
        source_file5 = Annovar.annovar_file,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_pgen = select_first([CopyFile.output_file1, FilterSamplesWithoutSNV.output_pgen])
    File output_pvar = select_first([CopyFile.output_file2, FilterSamplesWithoutSNV.output_pvar])
    File output_psam = select_first([CopyFile.output_file3, FilterSamplesWithoutSNV.output_psam])
    File output_genotype_csv = select_first([CopyFile.output_file4, FormatResult.output_genotype_csv])
    File output_annovar_file = select_first([CopyFile.output_file5, Annovar.annovar_file])
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