version 1.0

## VUMC STAR-Fusion Workflow
##
## This workflow detects gene fusions from RNA-Seq data using STAR-Fusion.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Steps:
## 1. Untar input FASTQ files (optional, if provided as tar.gz archive)
## 2. Run STAR-Fusion to detect gene fusions from paired-end FASTQ files
## 3. Copy output files to GCP folder (optional)
##
## ### Key Inputs:
## - left_fq, right_fq: Paired-end FASTQ files (or fastq_pair_tar_gz as tar.gz archive)
## - sample_name: Sample identifier
## - genome_plug_n_play_tar_gz: STAR-Fusion genome reference package
## - fusion_inspector: FusionInspector mode ("inspect" or "validate", default: "validate")
## - examine_coding_effect: Enable coding effect prediction (default: true)
## - min_FFPM: Minimum fusion fragments per million threshold (default: 0.1)
## - target_gcp_folder: Optional GCP destination for output files
##
## ### Key Outputs:
## - fusion_predictions_abridged: Abridged fusion predictions table
## - fusion_predictions: Complete fusion predictions table
## - fusion_chimeric_out_junction: Chimeric junction information from STAR alignment
## - fusion_coding_effect: Coding effect annotations (if examine_coding_effect enabled)
## - fusion_inspector_validate_web: FusionInspector validation HTML report (if fusion_inspector="validate")
## - fusion_inspector_inspect_web: FusionInspector inspection HTML report (if fusion_inspector="inspect")
## - fusion_junction_file_size_mb: Size of the chimeric junction file in MB, usually it should be a few MBs. If it is too small, it may indicate an issue with the fusion detection.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../format/VUMCUntar.wdl" as UntarModule
import "./RNAseqUtils.wdl" as RNAseqUtils
import "https://github.com/STAR-Fusion/STAR-Fusion/raw/refs/heads/master/WDL/star_fusion_workflow.wdl" as STARFusionModule

workflow VUMCStarSnapShot {
  input {
    String sample_name

    # input data options
    File? left_fq
    File? right_fq

    File? fastq_pair_tar_gz
    File? fastq_pair_tar_gz_output_extension = ".fastq.gz"

    String reference_snapshot_url = "projects/vangard-workflow-data/global/snapshots/starfusion-db-20231029"
    String reference_snapshot_root = "/mnt/disks/star_fusion"
    String reference_snapshot_path = "/mnt/disks/star_fusion/ref_genome.fa.star.idx"

    String docker = "trinityctat/starfusion:1.15.1"
    Int cpu = 12
    Float fastq_disk_space_multiplier = 3.25
    Int memory_gb = 50
    Float genome_disk_space_multiplier = 2.5
    Int preemptible = 2
    Float extra_disk_space = 10
    Boolean use_ssd = true

    String? target_gcp_folder
  }

  if(defined(fastq_pair_tar_gz)) {
    call UntarModule.Untar {
      input:
        input_tar_gz = select_first([fastq_pair_tar_gz]),
        output_extension = select_first([fastq_pair_tar_gz_output_extension]),
    }
    File untar_fastq1 = Untar.output_files[0]
    File untar_fastq2 = Untar.output_files[1]
  }

  File final_left_fq = select_first([left_fq, untar_fastq1])
  File final_right_fq = select_first([right_fq, untar_fastq2])

  call RNAseqUtils.STARForCount_SnapShot as STAR_Unsorted {
    input:
      sample_name = sample_name,

      left_fq = final_left_fq,
      right_fq = final_right_fq,
      fastq_disk_space_multiplier = fastq_disk_space_multiplier,

      reference_snapshot_url = reference_snapshot_url,
      reference_snapshot_root = reference_snapshot_root,
      reference_snapshot_path = reference_snapshot_path,

      preemptible = preemptible,
      docker = docker,
      cpu = cpu,
      memory_gb = memory_gb,
      extra_disk_space = extra_disk_space,
      use_ssd = use_ssd
  }

  if (defined(target_gcp_folder)) {
    String gcs_output_dir = sub(select_first([target_gcp_folder]), "/+$", "") + "/" + sample_name + "/"
    call GcpUtils.MoveOrCopyFiles as CopyFile1 {
      input:
        source_file1 = STAR_Unsorted.output_bam,
        source_file2 = STAR_Unsorted.output_star_summary,
        is_move_file = false,
        target_gcp_folder = gcs_output_dir
    }
  }

  output {
    File output_bam = select_first([CopyFile1.output_file1, STAR_Unsorted.output_bam])
    File output_star_summary = select_first([CopyFile1.output_file2, STAR_Unsorted.output_star_summary])
  }
}
