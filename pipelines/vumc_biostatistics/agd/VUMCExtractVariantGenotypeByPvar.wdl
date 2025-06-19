version 1.0

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WdlUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow extracts variant genotypes from PLINK2 files based on genomic loci defined in a BED file
# and performs variant annotation using Annovar.
#
# Workflow steps:
# 1. Identify chromosomes with variants overlapping the input BED regions
# 2. Extract variants within those regions from PGEN files 
# 3. Merge filtered PGEN files across valid chromosomes if needed
# 4. Remove samples without any variants
# 5. Convert PGEN to VCF format
# 6. Annotate variants using Annovar
# 7. Optionally copy results to a GCP storage location
#
# Inputs:
# - input_bed: BED file specifying target genomic regions
# - output_prefix: Prefix for all output files
# - chromosomes: List of chromosomes to process
# - input_pgen_files: PGEN files (one per chromosome)
# - input_psam_files: PSAM files (one per chromosome)
# - input_pvar_files: PVAR files (one per chromosome)
# - billing_gcp_project_id: Optional GCP billing project
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_pgen: Final filtered PGEN file path
# - output_pvar: Final filtered PVAR file path  
# - output_psam: Final filtered PSAM file path
# - output_annovar_file: Path to Annovar annotation results
# - output_num_variants: Number of variants in final result
# - output_num_samples: Number of samples in final result

workflow VUMCExtractVariantGenotypeByPvar {
  input {
    File keep_pvar

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
        input_ucsc_bed = keep_pvar
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

    call Plink2Utils.Plink2FilterPgenByPvar as Plink2FilterPgen {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = psam_file,
        keep_pvar = keep_pvar,
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
  
  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyFourFiles as CopyFile {
      input:
        source_file1 = FilterSamplesWithoutSNV.output_pgen,
        source_file2 = FilterSamplesWithoutSNV.output_pvar,
        source_file3 = FilterSamplesWithoutSNV.output_psam,
        source_file4 = Pgen2Vcf.output_vcf,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_pgen = select_first([CopyFile.output_file1, FilterSamplesWithoutSNV.output_pgen])
    String output_pvar = select_first([CopyFile.output_file2, FilterSamplesWithoutSNV.output_pvar])
    String output_psam = select_first([CopyFile.output_file3, FilterSamplesWithoutSNV.output_psam])
    String output_vcf = select_first([CopyFile.output_file4, Pgen2Vcf.output_vcf])
    Int output_num_variants = FilterSamplesWithoutSNV.output_num_variants
    Int output_num_samples = FilterSamplesWithoutSNV.output_num_samples
  }
}
