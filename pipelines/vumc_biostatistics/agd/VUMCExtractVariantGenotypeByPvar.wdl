version 1.0

# This workflow extracts variant genotypes from PLINK2 files based on variant IDs defined in a plink2 pvar file
# and formats the results for further analysis.
#
# Workflow steps:
# 1. Check for overlapping variants between input pvar files and the keep_pvar file
# 2. Extract variants from PGEN files for chromosomes with overlapping variants
# 3. Merge filtered PGEN files across valid chromosomes if more than one exists
# 4. Convert merged PGEN to VCF format
# 5. Format results to generate sample and variant CSV files
# 6. Filter to keep only samples with variants
# 7. Convert final filtered data to VCF format
# 8. Optionally copy results to a GCP storage location
#
# Inputs:
# - keep_pvar: PVAR file containing variant IDs to extract
# - keep_psam: Optional PSAM file containing sample GRIDs to extract
# - output_prefix: Prefix for all output files
# - chromosomes: List of chromosomes to process
# - input_pgen_files: PGEN files (one per chromosome)
# - input_psam_files: PSAM files (one per chromosome)
# - input_pvar_files: PVAR files (one per chromosome)
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_pgen: Final filtered PGEN file path
# - output_pvar: Final filtered PVAR file path
# - output_psam: Final filtered PSAM file path
# - output_vcf: Final VCF file path
# - output_variant_csv: CSV file with variant in row
# - output_sample_csv: CSV file with sample in row
# - output_num_variants: Number of variants in final result
# - output_num_samples: Number of samples in final result

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WdlUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "./VUMCExtractVariantGenotypeByPvarFormatResult.wdl" as VUMCFormatResult

workflow VUMCExtractVariantGenotypeByPvar {
  input {
    File keep_pvar
    File? keep_psam

    String output_prefix

    Array[String] chromosomes
    Array[File] input_pgen_files
    Array[File] input_psam_files
    Array[File] input_pvar_files

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

    call Plink2Utils.PgenFilter as PgenFilter_variants {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = psam_file,
        keep_pvar = keep_pvar,
        keep_psam = keep_psam,
        plink2_filter_option = "",
        output_prefix = output_prefix + "." + chromosome + ".snp"
    }
  }

  if (num_valid_chromsome > 1){
    call Plink2Utils.MergePgenFiles as MergePgenFiles {
      input:
        input_pgen_files = PgenFilter_variants.output_pgen,
        input_pvar_files = PgenFilter_variants.output_pvar,
        input_psam_files = PgenFilter_variants.output_psam,
        output_prefix = output_prefix + '.allsamples'
    }
  }

  File all_samples_pgen = select_first([MergePgenFiles.output_pgen, PgenFilter_variants.output_pgen[0]])
  File all_samples_pvar = select_first([MergePgenFiles.output_pvar, PgenFilter_variants.output_pvar[0]])
  File all_samples_psam = select_first([MergePgenFiles.output_psam, PgenFilter_variants.output_psam[0]])

  call Plink2Utils.Pgen2Vcf as Pgen2Vcf_variants {
    input:
      input_pgen = all_samples_pgen,
      input_pvar = all_samples_pvar,
      input_psam = all_samples_psam,
      output_prefix = output_prefix + '.allsamples'
  }

  call VUMCFormatResult.FormatResult as FormatResult {
    input:
      input_pvar_file = all_samples_pvar,
      input_psam_file = all_samples_psam,
      input_vcf_file = Pgen2Vcf_variants.output_vcf,
      output_prefix = output_prefix
  }

  call Plink2Utils.PgenFilter as PgenFilter_samples {
    input:
      input_pgen = all_samples_pgen,
      input_pvar = all_samples_pvar,
      input_psam = all_samples_psam,
      keep_psam = FormatResult.output_psam,
      plink2_filter_option = "",
      output_prefix = output_prefix
  }
  
  call Plink2Utils.Pgen2Vcf as Pgen2Vcf_samples {
    input:
      input_pgen = PgenFilter_samples.output_pgen,
      input_pvar = PgenFilter_samples.output_pvar,
      input_psam = PgenFilter_samples.output_psam,
      output_prefix = output_prefix
  }

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopySixFiles as CopyFile {
      input:
        source_file1 = PgenFilter_samples.output_pgen,
        source_file2 = PgenFilter_samples.output_pvar,
        source_file3 = PgenFilter_samples.output_psam,
        source_file4 = Pgen2Vcf_samples.output_vcf,
        source_file5 = FormatResult.output_variant_csv,
        source_file6 = FormatResult.output_sample_csv,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_pgen = select_first([CopyFile.output_file1, PgenFilter_samples.output_pgen])
    File output_pvar = select_first([CopyFile.output_file2, PgenFilter_samples.output_pvar])
    File output_psam = select_first([CopyFile.output_file3, PgenFilter_samples.output_psam])
    File output_vcf = select_first([CopyFile.output_file4, Pgen2Vcf_samples.output_vcf])
    File output_variant_csv = select_first([CopyFile.output_file5, FormatResult.output_variant_csv])
    File output_sample_csv = select_first([CopyFile.output_file6, FormatResult.output_sample_csv])
    Int output_num_variants = PgenFilter_samples.num_variants
    Int output_num_samples = PgenFilter_samples.num_samples
  }
}
