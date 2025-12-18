version 1.0

## VUMC STAR-Fusion Workflow
##
## This workflow processes RNA-Seq data to detect gene fusions using STAR-Fusion.
## Developed by VUMC/VANGARD team for comprehensive RNA-Seq fusion detection analysis.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## This pipeline processes RNA-Seq data from FASTQ files to detect gene fusions,
## enabling identification of fusion transcripts in cancer and other disease research.
##
## ### Workflow Steps:
## 1. STAR Alignment: Aligns paired-end RNA-Seq reads and generates chimeric junction outputs
## 2. Optional GCP Transfer: Copies chimeric junction output to specified GCP folder
##
## ### Inputs:
## - sample_name: Unique identifier for the sample
## - left_fq: Left/R1 FASTQ file (optional if using tar.gz)
## - right_fq: Right/R2 FASTQ file (optional if using tar.gz)
## - fastq_pair_tar_gz: Compressed archive of paired FASTQ files (alternative input)
## - genome_plug_n_play_tar_gz: Pre-built STAR genome reference package
## - target_gcp_folder: Optional GCP destination folder for outputs
##
## ### Outputs:
## - fusion_chimeric_out_junction: STAR chimeric junction file for downstream fusion detection
##
## ### Notes:
## - Accepts either individual FASTQ files or tar.gz archive as input
## - Generates chimeric junction outputs suitable for STAR-Fusion or other fusion callers
## - GCP file transfer is conditional based on target_gcp_folder parameter

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "./RNAseqUtils.wdl" as RNAseqUtils

workflow VUMCStarForFusion {
  input {
    String sample_name

    # input data options
    File? left_fq
    File? right_fq
    File? fastq_pair_tar_gz

    File genome_plug_n_play_tar_gz

    String? target_gcp_folder
  }

  call RNAseqUtils.STARForFusion {
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
        source_file1 = STARForFusion.fusion_chimeric_out_junction,
        is_move_file = false,
        target_gcp_folder = gcs_output_dir
    }
  }
  # Outputs that will be retained when execution is complete
  output {
    String fusion_chimeric_out_junction = select_first([CopyFile.output_file1, STARForFusion.fusion_chimeric_out_junction])
  }
}
