version 1.0

# This workflow implements polygenic risk score calculation based on pre-defined variants and effect sizes.
# Developed by VUMC Biostatistics for polygenic risk score analyses.
# Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
#
# Workflow steps:
# 1. Checks for overlapping variants between input genotypes and effect file
# 2. Filters input genotypes to keep only variants present in the effect file
# 3. Merges filtered chromosome-specific files if multiple chromosomes are present
# 4. Calculates polygenic risk scores using the filtered genotypes and effect file
# 5. Optionally copies result files to a GCP storage location
#
# Inputs:
# - chromosomes: List of chromosomes to analyze
# - input_pvar/pgen/psam_files: PLINK2 format genotype files for each chromosome
# - input_effort_file: File containing variant IDs and effect sizes for PRS calculation
# - input_effort_pvar_file: File containing variants to be used in PRS calculation
# - input_effort_file_columns: Column specification for the effort file
# - output_prefix: Prefix for output files
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_sscore_file: Path to the calculated PRS score file

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WdlUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils

workflow VUMCPrsStep3Score {
  input {
    Array[String] chromosomes
    Array[File] input_pvar_files
    Array[File] input_pgen_files
    Array[File] input_psam_files

    File input_effort_file
    File input_effort_pvar_file

    # for example, "2 4 6", 2:Variant IDs, 4:allele codes, 6:coefficients
    # for AGD dataset, the variant id should be: chr:pos:ref:alt
    String input_effort_file_columns 

    String output_prefix

    String? target_gcp_folder
  }

  Int num_all_chromsome = length(chromosomes)

  scatter(all_chrom_ind in range(num_all_chromsome)){
    call BioUtils.CheckOverlapVariants as CheckOverlapVariants {
      input:
        chromosome = chromosomes[all_chrom_ind],
        input_pgen_pvar = input_pvar_files[all_chrom_ind],
        input_ucsc_bed = input_effort_pvar_file,
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

    call Plink2Utils.Plink2FilterPgenByPvar as Plink2FilterPgen_variants {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = psam_file,
        keep_pvar = input_effort_pvar_file,
        plink2_filter_option = "",
        output_prefix = output_prefix + "." + chromosome + ".snp"
    }
  }

  if (num_valid_chromsome > 1){
    call Plink2Utils.MergePgenFiles as MergePgenFiles {
      input:
        input_pgen_files = Plink2FilterPgen_variants.output_pgen,
        input_pvar_files = Plink2FilterPgen_variants.output_pvar,
        input_psam_files = Plink2FilterPgen_variants.output_psam,
        output_prefix = output_prefix + '.allchroms'
    }
  }

  File all_chroms_pgen = select_first([MergePgenFiles.output_pgen, Plink2FilterPgen_variants.output_pgen[0]])
  File all_chroms_pvar = select_first([MergePgenFiles.output_pvar, Plink2FilterPgen_variants.output_pvar[0]])
  File all_chroms_psam = select_first([MergePgenFiles.output_psam, Plink2FilterPgen_variants.output_psam[0]])

  call Plink2PolygenicRiskScore {
    input:
      input_pgen = all_chroms_pgen,
      input_pvar = all_chroms_pvar,
      input_psam = all_chroms_psam,
      input_effort_file = input_effort_file,
      input_effort_file_columns = input_effort_file_columns,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = Plink2PolygenicRiskScore.output_sscore_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_sscore_file = select_first([CopyFile.output_file, Plink2PolygenicRiskScore.output_sscore_file])
  }
}

task Plink2PolygenicRiskScore {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    File input_effort_file
    String input_effort_file_columns

    String output_prefix

    String docker = "shengqh/plink_1.9_2.0:20250304"

    Int preemptible=3
    Int memory_gb = 10
    Int addtional_disk_space_gb = 10
  }

  Int disk_size = ceil(size([input_pgen, input_psam, input_pvar], "GB")) + addtional_disk_space_gb

  command <<<

  plink2 \
    --pgen ~{input_pgen} \
    --pvar ~{input_pvar} \
    --psam ~{input_psam} \
    --score ~{input_effort_file} ~{input_effort_file_columns} \
    --out ~{output_prefix}

  >>>

  runtime{
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
   }

  output {
    File output_sscore_file = "~{output_prefix}.sscore"
  }
}
