version 1.0

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils

# Description: This workflow prepares test PGEN files for Regenie step 2. 
# It performs QC filtering on the input PGEN files using specified PLINK2 options.
# The filtered files are then optionally copied to a specified GCP folder.
workflow VUMCRegenie4Task2PrepareTestPgen {
  input {
    Array[String] chromosomes

    Array[File] input_pgen_files
    Array[File] input_pvar_files
    Array[File] input_psam_files

    File? filter_psam_file

    String output_prefix
    
    String step2_plink2_option="--geno 0.05 --maf 0.01"

    String? billing_gcp_project_id
    String? target_gcp_folder
  }

  Int num_chromosomes = length(chromosomes)

  Array[Int] chrom_indecies = range(num_chromosomes)
  scatter(chrom_ind in chrom_indecies){
    File pgen_file = input_pgen_files[chrom_ind]
    File pvar_file = input_pvar_files[chrom_ind]
    File psam_file = input_psam_files[chrom_ind]
    String chromosome = chromosomes[chrom_ind]

    call BioUtils.QCFilterPgen as Step2Filter {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = psam_file,
        filter_psam_file = filter_psam_file,
        qc_filter_option = step2_plink2_option,
        output_prefix = output_prefix + "." + chromosome + ".step2"
    }
  }

  if(defined(target_gcp_folder)){
    String gcs_output_dir = select_first([target_gcp_folder])

    call GcpUtils.MoveOrCopyFileArray as CopyFile7 {
      input:
        source_files = Step2Filter.output_pgen,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = gcs_output_dir
    }
    scatter(pgen in CopyFile7.outputFiles) {
      String step2_pgen = pgen
    }

    call GcpUtils.MoveOrCopyFileArray as CopyFile8 {
      input:
        source_files = Step2Filter.output_psam,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = gcs_output_dir
    }
    scatter(psam in CopyFile8.outputFiles) {
      String step2_psam = psam
    }

    call GcpUtils.MoveOrCopyFileArray as CopyFile9 {
      input:
        source_files = Step2Filter.output_pvar,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = gcs_output_dir
    }
    scatter(pvar in CopyFile9.outputFiles) {
      String step2_pvar = pvar
    }
  }

  call WDLUtils.sum_integers {
    input:
      input_integers = Step2Filter.num_variants
  }

  output {
    Array[File] test_pgen_files = select_first([step2_pgen, Step2Filter.output_pgen])
    Array[File] test_psam_files = select_first([step2_psam, Step2Filter.output_psam])
    Array[File] test_pvar_files = select_first([step2_pvar, Step2Filter.output_pvar])

    String test_plink2_option = step2_plink2_option
    Int test_num_variants = sum_integers.sum
  }
}

