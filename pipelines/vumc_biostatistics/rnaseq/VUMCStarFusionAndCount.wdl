version 1.0

## VUMC STAR-Fusion and Gene Expression Count Workflow
##
## This workflow processes RNA-Seq data to detect gene fusions using STAR-Fusion
## and quantify gene expression using featureCounts.
## Developed by VUMC/VANGARD team for comprehensive RNA-Seq fusion detection and expression analysis.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## This pipeline handles RNA-Seq data processing from FASTQ files to fusion detection
## and gene expression quantification, enabling identification of gene fusions and
## expression profiling for cancer and other disease research.
##
## ### Workflow Steps:
## 1. Optional: Untar input FASTQ files if provided as tar.gz archive
## 2. STAR-Fusion: Detect gene fusions from paired-end FASTQ files using STAR alignment
## 3. STAR alignment: Generate BAM file for gene counting
## 4. featureCounts: Quantify gene expression from aligned BAM file
## 5. Optionally copy output files to a specified GCP folder
##
## ### Inputs:
## - left_fq, right_fq, fastq_pair_tar_gz: Input FASTQ files (paired-end or tar.gz archive)
## - sample_name: Identifier for the sample
## - genome_plug_n_play_tar_gz: STAR-Fusion genome reference package
## - gtf: Gene annotation file for featureCounts
## - fusion_inspector: FusionInspector mode (inspect or validate)
## - examine_coding_effect: Enable coding effect prediction
## - min_FFPM: Minimum fusion fragments per million threshold
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## Fusion Detection:
## - fusion_predictions_abridged: Abridged fusion predictions
## - fusion_predictions: Complete fusion predictions
## - fusion_coding_effect: Fusion predictions with coding effect annotations
## - fusion_chimeric_out_junction: Chimeric junction information
## - fusion_inspector_validate_web: FusionInspector validation web visualization (optional)
## - fusion_inspector_validate_fusions_abridged: FusionInspector validation results (optional)
## - fusion_inspector_inspect_web: FusionInspector inspection web visualization (optional)
## - fusion_inspector_inspect_fusions_abridged: FusionInspector inspection results (optional)
##
## Gene Expression:
## - star_summary: STAR alignment summary statistics
## - featurecounts_count: Gene expression count matrix
## - featurecounts_count_summary: featureCounts summary statistics
##
## ### Notes:
## - Utilizes STAR-Fusion for sensitive and accurate fusion detection
## - Uses featureCounts for reliable gene expression quantification
## - Multiple output formats available for downstream analysis and visualization
## - File copy operation to GCP is optional and only executed if a target folder is provided

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../format/VUMCUntar.wdl" as UntarModule
import "./RNAseqUtils.wdl" as RNAseqUtils
import "https://github.com/STAR-Fusion/STAR-Fusion/raw/refs/heads/master/WDL/star_fusion_workflow.wdl" as STARFusionModule

workflow VUMCStarFusionAndCount {
  input {
    String sample_name

    # input data options
    File? left_fq
    File? right_fq

    File? fastq_pair_tar_gz
    File? fastq_pair_tar_gz_output_extension = ".fastq.gz"

    # use "gs://mdl-ctat-genome-libs/__genome_libs_StarFv1.10/GRCh38_gencode_v37_CTAT_lib_Mar012021.plug-n-play.tar.gz"
    # or
    # download from https://data.broadinstitute.org/Trinity/CTAT_RESOURCE_LIB/ and upload to your GCP bucket
    File genome_plug_n_play_tar_gz 

    # gtf extracted from genome_plug_n_play_tar_gz to keep consistent
    File gtf

    # STAR-Fusion parameters
    String fusion_inspector = "validate"  # inspect or validate
    Boolean examine_coding_effect = true
    Float min_FFPM = 0.1

    # runtime params
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

  call STARFusionModule.star_fusion as STARFusion {
    input:
      sample_id = sample_name,
      left_fq = final_left_fq,
      right_fq = final_right_fq,
      genome = genome_plug_n_play_tar_gz,

      examine_coding_effect = examine_coding_effect,
      coord_sort_bam = false,
      min_FFPM = min_FFPM,

      preemptible = preemptible,
      docker = docker,
      cpu = cpu,
      memory = memory_gb + " GiB",
      extra_disk_space = extra_disk_space,
      fastq_disk_space_multiplier = fastq_disk_space_multiplier,
      genome_disk_space_multiplier = genome_disk_space_multiplier,
      fusion_inspector = fusion_inspector,
      use_ssd = use_ssd
  }

  if (defined(target_gcp_folder)) {
    String gcs_output_dir_1 = sub(select_first([target_gcp_folder]), "/+$", "") + "/" + sample_name + "/"
    call GcpUtils.MoveOrCopyFiles as CopyFile1 {
      input:
        source_file1 = STARFusion.fusion_predictions_abridged,
        source_file2 = STARFusion.fusion_predictions,
        source_file3 = STARFusion.coding_effect,
        source_file4 = STARFusion.junction,
        source_file5 = STARFusion.fusion_inspector_validate_web,
        source_file6 = STARFusion.fusion_inspector_validate_fusions_abridged,
        source_file7 = STARFusion.fusion_inspector_inspect_web,
        source_file8 = STARFusion.fusion_inspector_inspect_fusions_abridged,
        is_move_file = false,
        target_gcp_folder = gcs_output_dir_1
    }
  }

  call RNAseqUtils.STARForCount as STAR_Unsorted {
    input:
      sample_name = sample_name,
      left_fq = final_left_fq,
      right_fq = final_right_fq,
      genome_plug_n_play_tar_gz = genome_plug_n_play_tar_gz,
      docker = docker,
      cpu = cpu,
      fastq_disk_space_multiplier = fastq_disk_space_multiplier,
      memory_gb = memory_gb,
      genome_disk_space_multiplier = genome_disk_space_multiplier
  }

  call RNAseqUtils.FeatureCounts {
    input:
      bam = STAR_Unsorted.output_bam,
      sample_name = sample_name,
      gtf = gtf
  }

  if (defined(target_gcp_folder)) {
    String gcs_output_dir_2 = sub(select_first([target_gcp_folder]), "/+$", "") + "/" + sample_name + "/"
    call GcpUtils.MoveOrCopyFiles as CopyFile2 {
      input:
        source_file1 = STAR_Unsorted.output_star_summary,
        source_file2 = FeatureCounts.output_count,
        source_file3 = FeatureCounts.output_count_summary,
        is_move_file = false,
        target_gcp_folder = gcs_output_dir_2
    }
  }

  # Outputs that will be retained when execution is complete
  output {
    String fusion_predictions_abridged = select_first([CopyFile1.output_file1, STARFusion.fusion_predictions_abridged])
    String fusion_predictions = select_first([CopyFile1.output_file2, STARFusion.fusion_predictions])
    String fusion_coding_effect = select_first([CopyFile1.output_file3, STARFusion.coding_effect])
    String fusion_chimeric_out_junction = select_first([CopyFile1.output_file4, STARFusion.junction])
    String fusion_inspector_validate_web = select_first([CopyFile1.output_file5, STARFusion.fusion_inspector_validate_web, ""])
    String fusion_inspector_validate_fusions_abridged = select_first([CopyFile1.output_file6, STARFusion.fusion_inspector_validate_fusions_abridged, ""])
    String fusion_inspector_inspect_web = select_first([CopyFile1.output_file7, STARFusion.fusion_inspector_inspect_web, ""])
    String fusion_inspector_inspect_fusions_abridged = select_first([CopyFile1.output_file8, STARFusion.fusion_inspector_inspect_fusions_abridged, ""])

    String star_summary = select_first([CopyFile2.output_file1, STAR_Unsorted.output_star_summary])
    String featurecounts_count = select_first([CopyFile2.output_file2, FeatureCounts.output_count])
    String featurecounts_count_summary = select_first([CopyFile2.output_file3, FeatureCounts.output_count_summary])
  }
}
