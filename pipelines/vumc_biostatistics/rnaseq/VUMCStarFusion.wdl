version 1.0

## VUMC STAR-Fusion Workflow
##
## This workflow processes RNA-Seq data to detect gene fusions using STAR-Fusion.
## Developed by VUMC/VANGARD team for comprehensive RNA-Seq fusion detection analysis.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## This pipeline handles RNA-Seq data processing from FASTQ files to fusion detection,
## enabling identification of gene fusions for cancer and other disease research.
##
## ### Workflow Steps:
## 1. STAR-Fusion: Detect gene fusions from paired-end FASTQ files using STAR alignment
## 2. Optionally copy output files to a specified GCP folder
##
## ### Inputs:
## - left_fq, right_fq, fastq_pair_tar_gz: Input FASTQ files (paired-end or tar.gz archive)
## - sample_name: Identifier for the sample
## - genome_plug_n_play_tar_gz: STAR-Fusion genome reference package
## - fusion_inspector: FusionInspector mode (inspect or validate)
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - fusion_coding_effect: Fusion predictions with coding effect annotations
## - fusion_predictions_abridged: Abridged fusion predictions
## - fusion_predictions: Complete fusion predictions
## - fusion_log_final: STAR-Fusion log file
## - fusion_inspector_web: Fusion Inspector web visualization files (optional)
## - fusion_inspector_fusions: Fusion Inspector fusion details (optional)
##
## ### Notes:
## - Utilizes STAR-Fusion for sensitive and accurate fusion detection
## - Multiple output formats available for downstream analysis and visualization
## - File copy operation to GCP is optional and only executed if a target folder is provided

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "./RNAseqUtils.wdl" as RNAseqUtils

workflow VUMCStarFusion {
  input {
    String sample_name

    # input data options
    File? left_fq
    File? right_fq
    File? fastq_pair_tar_gz

    File genome_plug_n_play_tar_gz

    String? target_gcp_folder
  }

  call RNAseqUtils.STARFusion as STARFusion {
    input:
      fastq_pair_tar_gz = fastq_pair_tar_gz,
      left_fq = left_fq,
      right_fq = right_fq,
      genome_plug_n_play_tar_gz = genome_plug_n_play_tar_gz,
      sample_name = sample_name
  }

  if (defined(target_gcp_folder)) {
    String gcs_output_dir = sub(select_first([target_gcp_folder]), "/+$", "") + "/" + sample_name + "/"
    call GcpUtils.MoveOrCopyFiles as CopyFile {
      input:
        source_file1 = STARFusion.fusion_coding_effect,
        source_file2 = STARFusion.fusion_predictions_abridged,
        source_file3 = STARFusion.fusion_predictions,
        source_file4 = STARFusion.fusion_inspector_validate_web,
        source_file5 = STARFusion.fusion_inspector_validate_fusions_abridged,
        source_file6 = STARFusion.fusion_inspector_inspect_web,
        source_file7 = STARFusion.fusion_inspector_inspect_fusions_abridged,
        is_move_file = false,
        target_gcp_folder = gcs_output_dir
    }
  }
  # Outputs that will be retained when execution is complete
  output {
    String fusion_coding_effect = select_first([CopyFile.output_file1, STARFusion.fusion_coding_effect])
    String fusion_predictions_abridged = select_first([CopyFile.output_file2, STARFusion.fusion_predictions_abridged])
    String fusion_predictions = select_first([CopyFile.output_file3, STARFusion.fusion_predictions])
    String? fusion_inspector_validate_web = select_first([CopyFile.output_file4, STARFusion.fusion_inspector_validate_web, ""])
    String? fusion_inspector_validate_fusions_abridged = select_first([CopyFile.output_file5, STARFusion.fusion_inspector_validate_fusions_abridged, ""])
    String? fusion_inspector_inspect_web = select_first([CopyFile.output_file6, STARFusion.fusion_inspector_inspect_web, ""])
    String? fusion_inspector_inspect_fusions_abridged = select_first([CopyFile.output_file7, STARFusion.fusion_inspector_inspect_fusions_abridged, ""])}
}
