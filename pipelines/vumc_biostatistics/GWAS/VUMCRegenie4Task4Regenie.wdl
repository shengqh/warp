version 1.0

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/order_files_by_strings.wdl" as order_files_by_strings

import "./GWASUtils.wdl" as GWASUtils

/**
 * Workflow: VUMCRegenie4Task4Regenie
 * 
 * Description:
 * This workflow performs the both step1 and step2 the Regenie since both of them require phenotype and covariate files. 
 * It fits the model using the training data and then tests the model using the testing data.
 * The results are then merged and plotted and are then optionally copied to a specified GCP folder.
 *
 * Author:
 * Quanhu Sheng, quanhu.sheng.1@vumc.org
 */
workflow VUMCRegenie4Task4Regenie {
  input {
    Array[String] chromosomes

    Array[File] test_pgen_files
    Array[File] test_pvar_files
    Array[File] test_psam_files
    
    File model_pgen_file
    File model_pvar_file
    File model_psam_file

    File phenoFile
    String phenoColList
    Boolean is_binary_traits

    File covarFile
    String covarColList

    String output_prefix

    #option of regenie for model fitting
    String step1_regenie_option="--loocv --bsize 1000 --lowmem"
    Int step1_block_size=1000
    Int step1_max_variants=500000

    #option of regenie for testing
    String step2_regenie_option="--firth --approx --pThresh 0.01 --bsize 400"

    String? billing_gcp_project_id
    String? target_gcp_folder
  }

  Int num_chromosomes = length(chromosomes)

  Array[Int] chrom_indecies = range(num_chromosomes)

  call WDLUtils.count_lines as model_psam_file_lines {
    input:
      input_file = model_psam_file,
      ignore_comments = true
  }
  Int num_samples = model_psam_file_lines.num_lines

  call WDLUtils.count_lines as model_pvar_file_lines {
    input:
      input_file = model_pvar_file,
      ignore_comments = true
  }
  Int num_variants = model_pvar_file_lines.num_lines

  call WDLUtils.string_to_array as pheco_list {
    input:
      str = phenoColList,
      delimiter = ","
  }
  Array[String] phenotype_names = pheco_list.arr
  Int num_phenotypes = length(phenotype_names)

  call WDLUtils.string_to_array as covar_list{
    input:
      str = covarColList,
      delimiter = ","
  }
  Array[String] covar_names = covar_list.arr
  Int num_covariates = length(covar_names)

  call GWASUtils.Regenie4MemoryEstimation {
    input:
      num_samples = num_samples,
      num_variants = num_variants,
      num_phenotypes = num_phenotypes,
      num_chromosomes = num_chromosomes,
      num_covariates = num_covariates,
      num_ridge_l0 = 5,
      block_size = step1_block_size
  }

  Int step1_memory_gb = Regenie4MemoryEstimation.step1_memory_gb

  call GWASUtils.Regenie4Step1FitModel as RegenieStep1FitModel {
    input:
      input_pgen = model_pgen_file,
      input_pvar = model_pvar_file,
      input_psam = model_psam_file,
      phenoFile = phenoFile,
      phenoColList = phenoColList,
      is_binary_traits = is_binary_traits,
      covarFile = covarFile,
      covarColList = covarColList,
      output_prefix = output_prefix,
      step1_option = step1_regenie_option,
      memory_gb = step1_memory_gb * 2 #Level 1 ridge and making predictions need much more memory than Level 0 ridge.
  }

  scatter(chrom_ind in chrom_indecies){
    File step2_pgen = test_pgen_files[chrom_ind]
    File step2_pvar = test_pvar_files[chrom_ind]
    File step2_psam = test_psam_files[chrom_ind]
    String step2_chromosome = chromosomes[chrom_ind]

    call GWASUtils.Regenie4Step2AssociationTest as RegenieStep2AssociationTest {
      input:
        pred_list_file = RegenieStep1FitModel.pred_list_file,
        pred_loco_files = RegenieStep1FitModel.pred_loco_files,
        input_pgen = step2_pgen,
        input_pvar = step2_pvar,
        input_psam = step2_psam,
        phenoFile = phenoFile,
        phenoColList = phenoColList,
        is_binary_traits = is_binary_traits,
        covarFile = covarFile,
        covarColList = covarColList,
        output_prefix = "~{output_prefix}.~{step2_chromosome}",
        step2_option = step2_regenie_option,
        memory_gb = step1_memory_gb #chromosome level memory cost would be less than step1, use step1 memory here.
    }

    scatter(cur_pheno in phenotype_names){
      String expect_regenie_file = "~{output_prefix}.~{step2_chromosome}_~{cur_pheno}.regenie"
    }

    call order_files_by_strings.order_files_by_strings as OrderFiles {
      input:
        input_files = RegenieStep2AssociationTest.regenie_files,
        expect_files = expect_regenie_file
    }
  }

  scatter(pheno_idx in range(num_phenotypes)){
    String phenotype_name = phenotype_names[pheno_idx]
    scatter(chrom_idx in range(num_chromosomes)){
      File regenie_file = OrderFiles.ordered_files[chrom_idx][pheno_idx]
    }

    call GWASUtils.MergeRegenieChromosomeResultsOnePhenotype as MergeRegenieChromosomeResults {
      input:
        regenie_chromosome_files = regenie_file,
        output_prefix = output_prefix + "." + phenotype_name
    }

    call GWASUtils.RegeniePlots {
      input:
        regenie_file = MergeRegenieChromosomeResults.phenotype_regenie_file,
        output_prefix = "~{output_prefix}.~{phenotype_name}"
    }
  }

  if(defined(target_gcp_folder)){
    String gcs_output_dir = select_first([target_gcp_folder])

    call GcpUtils.MoveOrCopyOneFile as CopyFile1 {
      input:
        source_file = RegenieStep1FitModel.pred_list_file,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = gcs_output_dir
    }

    call GcpUtils.MoveOrCopyFileArray as CopyFile2 {
      input:
        source_files = RegenieStep1FitModel.pred_loco_files,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = gcs_output_dir
    }
    scatter(output_loco_file in CopyFile2.outputFiles) {
      String pred_loco_file = output_loco_file
    }

    call GcpUtils.MoveOrCopyFileArray as CopyFile3 {
      input:
        source_files = MergeRegenieChromosomeResults.phenotype_regenie_file,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = gcs_output_dir
    }
    scatter(afile in CopyFile3.outputFiles) {
      String phenotype_regenie_file = afile
    }

    call GcpUtils.MoveOrCopyFileArray as CopyFile4 {
      input:
        source_files = RegeniePlots.qqplot_png,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = gcs_output_dir
    }
    scatter(qpng in CopyFile4.outputFiles) {
      String pheno_qqplot_png = qpng
    }

    call GcpUtils.MoveOrCopyFileArray as CopyFile5 {
      input:
        source_files = RegeniePlots.manhattan_png,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = gcs_output_dir
    }
    scatter(mpng in CopyFile5.outputFiles) {
      String pheno_manhattan_png = mpng
    }
  }

  output {
    File pred_list_file = select_first([CopyFile1.output_file, RegenieStep1FitModel.pred_list_file])
    Array[File] pred_loco_files = select_first([pred_loco_file, RegenieStep1FitModel.pred_loco_files])

    Array[File] phenotype_regenie_files = select_first([phenotype_regenie_file, MergeRegenieChromosomeResults.phenotype_regenie_file])

    Array[File] phenotype_qqplot_png = select_first([pheno_qqplot_png, RegeniePlots.qqplot_png])
    Array[File] phenotype_manhattan_png = select_first([pheno_manhattan_png, RegeniePlots.manhattan_png])
  }
}

