version 1.0

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "./GWASUtils.wdl" as GWASUtils

workflow VUMCRegenie4Step1FitModel {
  input {
    File pgen_file
    File pvar_file
    File psam_file

    File phenoFile
    String phenoColList
    Boolean is_binary_traits

    File covarFile
    String covarColList

    Boolean is_time_to_event=false
    String? eventColList

    String output_prefix

    String step1_option = "--loocv --bsize 1000 --lowmem"

    String? target_gcp_folder
  }

  if(is_time_to_event && !defined(eventColList)){
    call WDLUtils.FailWithMessage {
      input:
        message = "eventColList is required when is_time_to_event is true"
    }
  }

  call GWASUtils.Regenie4Step1FitModel as RegenieStep1FitModel {
    input:
      input_pgen = pgen_file,
      input_pvar = pvar_file,
      input_psam = psam_file,
      phenoFile = phenoFile,
      phenoColList = phenoColList,
      is_binary_traits = is_binary_traits,
      covarFile = covarFile,
      covarColList = covarColList,
      is_time_to_event = is_time_to_event,
      eventColList = eventColList,
      output_prefix = output_prefix,
      step1_option = step1_option
  }

  if(defined(target_gcp_folder)){
    String gcs_output_dir = sub(select_first([target_gcp_folder]), "/+$", "")
    call GcpUtils.MoveOrCopyOneFile as CopyFile1 {
      input:
        source_file = RegenieStep1FitModel.pred_list_file,
        is_move_file = false,
        target_gcp_folder = gcs_output_dir
    }
    call GcpUtils.MoveOrCopyFileArray as CopyFile2 {
      input:
        source_files = RegenieStep1FitModel.pred_loco_files,
        is_move_file = false,
        target_gcp_folder = gcs_output_dir
    }
    scatter(output_loco_file in CopyFile2.outputFiles) {
      String target_loco_file = output_loco_file
    }
  }

  output {
    File pred_list_file = select_first([CopyFile1.output_file, RegenieStep1FitModel.pred_list_file])
    Array[File] pred_loco_files = select_first([target_loco_file, RegenieStep1FitModel.pred_loco_files])
  }
}

