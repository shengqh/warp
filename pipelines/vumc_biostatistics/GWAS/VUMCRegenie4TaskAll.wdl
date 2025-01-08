version 1.0

import "./VUMCRegenie4Task1CreateCohortPsam.wdl" as Task1
import "./VUMCRegenie4Task2PrepareTestPgen.wdl" as Task2
import "./VUMCRegenie4Task3PrepareModelPgen.wdl" as Task3
import "./VUMCRegenie4Task4Regenie.wdl" as Task4

workflow VUMCRegenie4TaskAll {
  input {
    Array[String] chromosomes

    Array[File] input_pgen_files
    Array[File] input_pvar_files
    Array[File] input_psam_files

    File? input_grid
    Int input_grid_column = 0

    String? input_ancestry
    File? input_ancestry_file

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
    String step2_plink2_option="--geno 0.05 --maf 0.01"
    String step2_regenie_option="--firth --approx --pThresh 0.01 --bsize 400"

    String? billing_gcp_project_id
    String? target_gcp_folder
  }

  Boolean filter_sample = if (defined(input_grid)) then true else if (defined(input_ancestry)) then true else false
  if (filter_sample) {
    call Task1.VUMCRegenie4Task1CreateCohortPsam as CreateCohortPsam {
      input:
        input_psam = input_psam_files[1],
        input_grid = input_grid,
        input_grid_column = input_grid_column,
        input_ancestry = input_ancestry,
        input_ancestry_file = input_ancestry_file,
        output_prefix = output_prefix,
        billing_gcp_project_id = billing_gcp_project_id,
        target_gcp_folder = target_gcp_folder
    }
  }

  call Task2.VUMCRegenie4Task2PrepareTestPgen as PrepareTestPgen {
    input:
      chromosomes = chromosomes,
      input_pgen_files = input_pgen_files,
      input_pvar_files = input_pvar_files,
      input_psam_files = input_psam_files,
      filter_psam_file = CreateCohortPsam.output_psam,
      output_prefix = output_prefix,
      step2_plink2_option = step2_plink2_option,
      billing_gcp_project_id = billing_gcp_project_id,
      target_gcp_folder = target_gcp_folder
  }

  call Task3.VUMCRegenie4Task3PrepareModelPgen as PrepareModelPgen {
    input:
      chromosomes = chromosomes,
      test_pgen_files = PrepareTestPgen.test_pgen_files,
      test_pvar_files = PrepareTestPgen.test_pvar_files,
      test_psam_files = PrepareTestPgen.test_psam_files,
      output_prefix = output_prefix,
      step1_plink2_option = step1_plink2_option,
      step1_prune = step1_prune,
      step1_prune_option = step1_prune_option,
      step1_max_variants = step1_max_variants,
      billing_gcp_project_id = billing_gcp_project_id,
      target_gcp_folder = target_gcp_folder
  }

  call Task4.VUMCRegenie4Task4Regenie as Regenie {
    input:
      chromosomes = chromosomes,
      test_pgen_files = PrepareTestPgen.test_pgen_files,
      test_pvar_files = PrepareTestPgen.test_pvar_files,
      test_psam_files = PrepareTestPgen.test_psam_files,
      model_pgen_file = PrepareModelPgen.model_pgen_file,
      model_pvar_file = PrepareModelPgen.model_pvar_file,
      model_psam_file = PrepareModelPgen.model_psam_file,
      phenoFile = phenoFile,
      phenoColList = phenoColList,
      is_binary_traits = is_binary_traits,
      covarFile = covarFile,
      covarColList = covarColList,
      output_prefix = output_prefix,
      step1_regenie_option = step1_regenie_option,
      step1_block_size = step1_block_size,
      step1_max_variants = step1_max_variants,
      step2_regenie_option = step2_regenie_option,
      billing_gcp_project_id = billing_gcp_project_id,
      target_gcp_folder = target_gcp_folder
  }

  output {
    Array[File] test_pgen_files = PrepareTestPgen.test_pgen_files
    Array[File] test_pvar_files = PrepareTestPgen.test_pvar_files
    Array[File] test_psam_files = PrepareTestPgen.test_psam_files
    String test_plink2_option = PrepareTestPgen.test_plink2_option
    Int test_num_variants = PrepareTestPgen.test_num_variants

    File model_pgen_file = PrepareModelPgen.model_pgen_file
    File model_pvar_file = PrepareModelPgen.model_pvar_file
    File model_psam_file = PrepareModelPgen.model_psam_file
    String model_plink2_option = PrepareModelPgen.model_plink2_option
    String model_prune_option = PrepareModelPgen.model_prune_option
    Int model_num_variants = PrepareModelPgen.model_num_variants

    File pred_list_file = Regenie.pred_list_file
    Array[File] pred_loco_files = Regenie.pred_loco_files

    Array[File] phenotype_regenie_files = Regenie.phenotype_regenie_files

    Array[File] phenotype_qqplot_png = Regenie.phenotype_qqplot_png
    Array[File] phenotype_manhattan_png = Regenie.phenotype_manhattan_png
  }
}

