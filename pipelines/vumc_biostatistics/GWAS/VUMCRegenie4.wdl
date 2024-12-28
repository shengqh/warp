version 1.0

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/order_files_by_strings.wdl" as order_files_by_strings
import "./GWASUtils.wdl" as GWASUtils

workflow VUMCRegenie4 {
  input {
    File? qc_pgen
    File? qc_pvar
    File? qc_psam

    File input_pgen
    File input_pvar
    File input_psam

    File phenoFile
    String phenoColList
    Boolean is_binary_traits

    File covarFile
    String covarColList

    String output_prefix

    #option of variants for model fitting
    String step1_plink2_option="--mac 100 --geno 0.01 --maf 0.1 --max-maf 0.9 --hwe 1e-15 --snps-only --not-chr 23-27"
    String step1_regenie_option="--loocv --bsize 1000 --lowmem"
    Int step1_block_size=1000
    Int step1_max_variants=500000

    #https://www.nature.com/articles/s41588-021-00870-7
    #LD pruning using a R2 threshold of 0.9 with a window size of 1,000 markers and a step size of 100 markers.
    Boolean step1_prune = true
    String step1_prune_option="--indep-pairwise 1000 100 0.9"
    
    #option of variants for testing
    String? step2_plink2_option
    String step2_regenie_option="--firth --approx --pThresh 0.01 --bsize 400"

    Array[String] chromosome_list = ["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","X"]

    String? billing_gcp_project_id
    String? target_gcp_folder
  }

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

  call BioUtils.GetValidChromosomeList {
    input:
      input_pvar = input_pvar,
      input_chromosomes = chromosome_list
  }
  Array[String] valid_chromosomes = GetValidChromosomeList.valid_chromosomes
  Int num_chromosomes = length(valid_chromosomes)

  if(defined(step2_plink2_option)){
    if (step2_plink2_option != ""){
      call BioUtils.QCFilterPgen as Step2Filter {
        input:
          input_pgen = input_pgen,
          input_pvar = input_pvar,
          input_psam = input_psam,
          qc_filter_option = select_first([step2_plink2_option]),
          output_prefix = output_prefix + ".step2"
      }
    }
  }

  File step2_pgen = select_first([Step2Filter.output_pgen, input_pgen])
  File step2_pvar = select_first([Step2Filter.output_pvar, input_pvar])
  File step2_psam = select_first([Step2Filter.output_psam, input_psam])

  if(defined(qc_pgen)){
    call WDLUtils.count_lines as qc_pvar_count {
      input:
        input_file = select_first([qc_pvar]),
        ignore_comments = true
    }
    Int qc_num_variants = qc_pvar_count.num_lines

    call WDLUtils.count_lines as qc_psam_count {
      input:
        input_file = select_first([qc_psam]),
        ignore_comments = true
    }
    Int qc_num_samples = qc_psam_count.num_lines
  }

  if(!defined(qc_pgen)){
    if(step1_prune){
      call BioUtils.QCFilterAndPrunePgen as Step1FilterPrune {
        input:
          input_pgen = input_pgen,
          input_pvar = input_pvar,
          input_psam = input_psam,
          qc_filter_option = step1_plink2_option,
          indep_pairwise_option = step1_prune_option,
          output_prefix = output_prefix + ".step1"
      }
    }
    
    if(!step1_prune){
      call BioUtils.QCFilterPgen as Step1Filter {
        input:
          input_pgen = input_pgen,
          input_pvar = input_pvar,
          input_psam = input_psam,
          qc_filter_option = step1_plink2_option,
          output_prefix = output_prefix + ".step1"
      }
    }
  }

  File step1_pgen = select_first([qc_pgen, Step1FilterPrune.output_pgen, Step1Filter.output_pgen])
  File step1_pvar = select_first([qc_pvar, Step1FilterPrune.output_pvar, Step1Filter.output_pvar])
  File step1_psam = select_first([qc_psam, Step1FilterPrune.output_psam, Step1Filter.output_psam])
  Int step1_num_variants = select_first([qc_num_variants, Step1FilterPrune.num_variants, Step1Filter.num_variants])
  Int step1_num_samples = select_first([qc_num_samples, Step1FilterPrune.num_samples, Step1Filter.num_samples])

  if (step1_num_variants > step1_max_variants){
    call Plink2Utils.SamplingVariantsInPgen {
      input:
        input_pgen = step1_pgen,
        input_pvar = step1_pvar,
        input_psam = step1_psam,
        output_prefix = output_prefix + ".step1.sampled",
        max_num_variants = step1_max_variants
    }
  }
  
  File model_pgen = select_first([SamplingVariantsInPgen.output_pgen, step1_pgen])
  File model_pvar = select_first([SamplingVariantsInPgen.output_pvar, step1_pvar])
  File model_psam = select_first([SamplingVariantsInPgen.output_psam, step1_psam])
  Int model_num_samples = select_first([SamplingVariantsInPgen.num_samples, step1_num_samples])
  Int model_num_variants = select_first([SamplingVariantsInPgen.num_variants, step1_num_variants])

  call GWASUtils.Regenie4MemoryEstimation {
    input:
      num_samples = model_num_samples,
      num_variants = model_num_variants,
      num_phenotypes = num_phenotypes,
      num_chromosomes = num_chromosomes,
      num_covariates = num_covariates,
      num_ridge_l0 = 5,
      block_size = step1_block_size
  }

  Int step1_memory_gb = Regenie4MemoryEstimation.step1_memory_gb

  call GWASUtils.Regenie4Step1FitModel as RegenieStep1FitModel {
    input:
      input_pgen = model_pgen,
      input_pvar = model_pvar,
      input_psam = model_psam,
      phenoFile = phenoFile,
      phenoColList = phenoColList,
      is_binary_traits = is_binary_traits,
      covarFile = covarFile,
      covarColList = covarColList,
      output_prefix = output_prefix,
      step1_option = step1_regenie_option,
      memory_gb = step1_memory_gb * 2 #Level 1 ridge and making predictions need much more memory than Level 0 ridge.
  }

  scatter(chromosome in valid_chromosomes) {
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
        output_prefix = "~{output_prefix}.~{chromosome}",
        step2_option = step2_regenie_option,
        chromosome = chromosome,
        memory_gb = step1_memory_gb #chromosome level memory cost would be less than step1, use step1 memory here.
    }

    scatter(cur_pheno in phenotype_names){
      String expect_regenie_file = "~{output_prefix}.~{chromosome}_~{cur_pheno}.regenie"
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

