version 1.0

## VUMC STAR-Fusion and FeatureCounts Workflow
##
## This workflow processes RNA-Seq data to detect gene fusions using STAR-Fusion and quantify gene expression using FeatureCounts.
## Developed by VUMC/VANGARD team for comprehensive RNA-Seq analysis including fusion detection and gene counting.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## This pipeline handles RNA-Seq data processing from FASTQ files to fusion detection and gene expression quantification,
## enabling identification of gene fusions and expression profiles for cancer and other disease research.
##
## ### Workflow Steps:
## 1. STAR-Fusion: Detect gene fusions from paired-end FASTQ files using STAR alignment
## 2. SortSam: Sort the output BAM file by coordinates
## 3. FeatureCounts: Count reads mapped to genomic features using GTF annotation
## 4. Optionally copy output files to a specified GCP folder
##
## ### Inputs:
## - left_fq, right_fq, fastq_pair_tar_gz: Input FASTQ files (paired-end or tar.gz archive)
## - sample_name: Identifier for the sample
## - genome_plug_n_play_tar_gz: STAR-Fusion genome reference package
## - gtf: Gene annotation file for feature counting
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - fusion_coding_effect: Fusion predictions with coding effect annotations
## - fusion_predictions_abridged: Abridged fusion predictions
## - fusion_predictions: Complete fusion predictions
## - fusion_inspector_web: Fusion Inspector web visualization files
## - fusion_inspector_fusions: Fusion Inspector fusion details
## - fusion_log_final: STAR-Fusion log file
## - featurecounts_count: Gene expression count matrix
## - featurecounts_count_summary: FeatureCounts summary statistics
##
## ### Notes:
## - Utilizes STAR-Fusion for sensitive and accurate fusion detection
## - Provides gene expression quantification via FeatureCounts
## - Multiple output formats available for downstream analysis and visualization
## - File copy operation to GCP is optional and only executed if a target folder is provided

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/broad/BamProcessing.wdl" as Processing
import "./RNAseqUtils.wdl" as RNAseqUtils

workflow VUMCStarFusionAndCount {
  input {
    String sample_name

    File genome_plug_n_play_tar_gz
    File gtf
    
    # input data options
    File? left_fq
    File? right_fq
    File? fastq_pair_tar_gz
    
    # STAR-Fusion parameters
    String fusion_inspector = "validate"  # inspect or validate
    Boolean examine_coding_effect = true
    Float min_FFPM = 0.1

    # STAR-Fusion runtime params
    String docker = "trinityctat/starfusion:latest"
    Int cpu = 12
    Float fastq_disk_space_multiplier = 3.25
    Int memory_gb = 50
    Float genome_disk_space_multiplier = 2.5
    Int preemptible = 2
    Float extra_disk_space = 10
    Boolean use_ssd = true

    String? target_gcp_folder
  }

  call RNAseqUtils.STARFusion as STARFusion {
      input:
        fastq_pair_tar_gz = fastq_pair_tar_gz,
        left_fq = left_fq,
        right_fq = right_fq,
        genome_plug_n_play_tar_gz = genome_plug_n_play_tar_gz,
        sample_name = sample_name,
        fusion_inspector = fusion_inspector,
        min_FFPM = min_FFPM,

        preemptible = preemptible,
        docker = docker,
        cpu = cpu,
        memory_gb = memory_gb,
        extra_disk_space = extra_disk_space,
        fastq_disk_space_multiplier = fastq_disk_space_multiplier,
        genome_disk_space_multiplier = genome_disk_space_multiplier,
        use_ssd = use_ssd
  }

  call Processing.SortSam {
    input:
      input_bam = STARFusion.fusion_unsorted_bam,
      output_bam_basename = sample_name + "_Aligned.sortedByCoord.out",
      compression_level = 2,
      preemptible_tries = 3
  }

  call RNAseqUtils.FeatureCounts {
    input:
      bam = SortSam.output_bam,
      bam_index = SortSam.output_bam_index,
      sample_name = sample_name,
      gtf = gtf
  }

    File? fusion_inspector_validate_fusions_abridged = "~{sample_name}_validate_finspector.FusionInspector.fusions.abridged.tsv.gz"
    File? fusion_inspector_validate_web = "~{sample_name}_validate_finspector.fusion_inspector_web.html"

    File? fusion_inspector_inspect_fusions_abridged = "~{sample_name}_inspect_finspector.FusionInspector.fusions.abridged.tsv.gz"
    File? fusion_inspector_inspect_web = "~{sample_name}_inspect_finspector.fusion_inspector_web.html"

    File fusion_log_final = "~{sample_name}_star-fusion.Log.final.out"
    File fusion_unsorted_bam = "~{sample_name}.STAR.aligned.UNsorted.bam"


  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyFiles as CopyFile {
      input:
        source_file1 = STARFusion.fusion_coding_effect,
        source_file2 = STARFusion.fusion_predictions_abridged,
        source_file3 = STARFusion.fusion_predictions,
        source_file4 = select_first([STARFusion.fusion_inspector_validate_web, STARFusion.fusion_inspector_inspect_web]),
        source_file5 = select_first([STARFusion.fusion_inspector_validate_fusions_abridged, STARFusion.fusion_inspector_inspect_fusions_abridged]),
        source_file6 = STARFusion.fusion_log_final,
        source_file7 = FeatureCounts.output_count,
        source_file8 = FeatureCounts.output_count_summary,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }
  # Outputs that will be retained when execution is complete
  output {
    String fusion_coding_effect = select_first([CopyFile.output_file1, STARFusion.fusion_coding_effect])
    String fusion_predictions_abridged = select_first([CopyFile.output_file2, STARFusion.fusion_predictions_abridged])
    String fusion_predictions = select_first([CopyFile.output_file3, STARFusion.fusion_predictions])
    String fusion_inspector_web = select_first([CopyFile.output_file4, STARFusion.fusion_inspector_validate_web, STARFusion.fusion_inspector_inspect_web])
    String fusion_inspector_fusions = select_first([CopyFile.output_file5, STARFusion.fusion_inspector_validate_fusions_abridged, STARFusion.fusion_inspector_inspect_fusions_abridged])
    String fusion_log_final = select_first([CopyFile.output_file6, STARFusion.fusion_log_final])
    String featurecounts_count = select_first([CopyFile.output_file7, FeatureCounts.output_count])
    String featurecounts_count_summary = select_first([CopyFile.output_file8, FeatureCounts.output_count_summary])
  }
}
