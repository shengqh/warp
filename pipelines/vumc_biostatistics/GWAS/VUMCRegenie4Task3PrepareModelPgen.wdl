version 1.0

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/order_files_by_strings.wdl" as order_files_by_strings

/**
 * Workflow: VUMCRegenie4Task3PrepareModelPgen
 * 
 * Description:
 * This workflow prepares the model PGEN files for Regenie step 1. 
 * It performs QC filtering on the input PGEN files using specified PLINK2 options.
 * The filtered files are then optionally copied to a specified GCP folder.
 *
 * Author:
 * Quanhu Sheng, quanhu.sheng.1@vumc.org
 */

workflow VUMCRegenie4Task3PrepareModelPgen {
  input {
    Array[String] chromosomes

    Boolean use_autosomal_chromosome_only = true

    Array[File] test_pgen_files
    Array[File] test_pvar_files
    Array[File] test_psam_files

    String output_prefix

    #option of variants for model fitting
    String step1_plink2_option="--mac 100 --geno 0.01 --maf 0.1 --max-maf 0.9 --hwe 1e-15 --snps-only --not-chr 23-27"
    Int step1_max_variants=500000

    #https://www.nature.com/articles/s41588-021-00870-7
    #LD pruning using a R2 threshold of 0.9 with a window size of 1,000 markers and a step size of 100 markers.
    Boolean step1_prune = true
    String step1_prune_option="--indep-pairwise 1000 100 0.9"

    String? billing_gcp_project_id
    String? target_gcp_folder
  }

  if (use_autosomal_chromosome_only){
    call BioUtils.GetAutosomalChromosomeIndecies {
      input:
        input_chromosomes = chromosomes
    }
    Array[Int] autosomal_chromosome_indecies = GetAutosomalChromosomeIndecies.autosomal_chromosome_indecies
  }
  
  if(!use_autosomal_chromosome_only){
    Array[Int] all_indecies = range(length(chromosomes))
  }

  Array[Int] cur_chromosome_indecies = select_first([autosomal_chromosome_indecies, all_indecies])

  Int num_valid_chromsome = length(cur_chromosome_indecies)

  scatter(chrom_ind in range(num_valid_chromsome)){
    Int old_ind = cur_chromosome_indecies[chrom_ind]
    File pgen_file = test_pgen_files[old_ind]
    File pvar_file = test_pvar_files[old_ind]
    File psam_file = test_psam_files[old_ind]
    String chromosome = chromosomes[old_ind]

    if(step1_prune){
      call BioUtils.QCFilterAndPrunePgen as Step1FilterPrune {
        input:
          input_pgen = pgen_file,
          input_pvar = pvar_file,
          input_psam = psam_file,
          qc_filter_option = step1_plink2_option,
          indep_pairwise_option = step1_prune_option,
          output_prefix = output_prefix + "." + chromosome + ".step1"
      }
    }
    
    if(!step1_prune){
      call BioUtils.QCFilterPgen as Step1Filter {
        input:
          input_pgen = pgen_file,
          input_pvar = pvar_file,
          input_psam = psam_file,
          qc_filter_option = step1_plink2_option,
          output_prefix = output_prefix + "." + chromosome + ".step1"
      }
    }

    File step1_chrom_pgen = select_first([Step1FilterPrune.output_pgen, Step1Filter.output_pgen])
    File step1_chrom_pvar = select_first([Step1FilterPrune.output_pvar, Step1Filter.output_pvar])
    File step1_chrom_psam = select_first([Step1FilterPrune.output_psam, Step1Filter.output_psam])
    Int step1_chrom_num_variants = select_first([Step1FilterPrune.num_variants, Step1Filter.num_variants])
  }

  if (num_valid_chromsome > 1){
    call Plink2Utils.MergePgenFiles as MergeStep1Pgen {
      input:
        input_pgen_files = step1_chrom_pgen,
        input_pvar_files = step1_chrom_pvar,
        input_psam_files = step1_chrom_psam,
        output_prefix = output_prefix + ".step1"
    }
  }

  if (num_valid_chromsome <= 1){
    File only_pgen = step1_chrom_pgen[0]
    File only_pvar = step1_chrom_pvar[0]
    File only_psam = step1_chrom_psam[0]
    Int only_num_variants = step1_chrom_num_variants[0]
  }

  Int step1_cur_num_variants = select_first([MergeStep1Pgen.num_variants, only_num_variants])

  if (step1_cur_num_variants > step1_max_variants){
    call Plink2Utils.SamplingVariantsInPgen {
      input:
        input_pgen = select_first([MergeStep1Pgen.output_pgen, only_pgen]),
        input_pvar = select_first([MergeStep1Pgen.output_pvar, only_pvar]),
        input_psam = select_first([MergeStep1Pgen.output_psam, only_psam]),
        output_prefix = output_prefix + ".step1.sampled",
        max_num_variants = step1_max_variants
    }
  }
  
  File model_pgen = select_first([SamplingVariantsInPgen.output_pgen, MergeStep1Pgen.output_pgen, only_pgen])
  File model_pvar = select_first([SamplingVariantsInPgen.output_pvar, MergeStep1Pgen.output_pvar, only_pvar])
  File model_psam = select_first([SamplingVariantsInPgen.output_psam, MergeStep1Pgen.output_psam, only_psam])

  if(defined(target_gcp_folder)){
    String gcs_output_dir = select_first([target_gcp_folder])

    call GcpUtils.MoveOrCopyThreeFiles as CopyFile6 {
      input:
        source_file1 = model_pgen,
        source_file2 = model_pvar,
        source_file3 = model_psam,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = gcs_output_dir
    }
  }

  output {
    String model_plink2_option = step1_plink2_option
    String model_prune_option = if step1_prune then step1_prune_option else ""
    
    File model_pgen_file = select_first([CopyFile6.output_file1, model_pgen])
    File model_pvar_file = select_first([CopyFile6.output_file2, model_pvar])
    File model_psam_file = select_first([CopyFile6.output_file3, model_psam])

    Int model_num_variants = select_first([SamplingVariantsInPgen.num_variants, MergeStep1Pgen.num_variants, only_num_variants])
  }
}

