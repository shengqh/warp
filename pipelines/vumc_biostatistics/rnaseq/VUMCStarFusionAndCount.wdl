version 1.0

## VUMC STAR-Fusion and Gene Expression Quantification Workflow
##
## This WDL workflow performs comprehensive RNA-Seq analysis including gene fusion
## detection via STAR-Fusion and transcript-level quantification using featureCounts.
## Developed by the VUMC/VANGARD Bioinformatics Core for translational genomics research.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Overview:
## This pipeline processes paired-end RNA-Seq FASTQ data through fusion detection
## and gene expression quantification workflows, providing comprehensive analysis
## for oncology research, rare disease studies, and general transcriptomics applications.
##
## ### Processing Steps:
## 1. Input preparation: Untar FASTQ files if provided as compressed archive
## 2. STAR-Fusion analysis: Identify gene fusions using STAR aligner with fusion detection
## 3. STAR alignment: Generate coordinate-sorted BAM file for downstream quantification
## 4. featureCounts: Calculate gene-level expression counts from aligned reads
## 5. Output management: Optionally transfer results to specified GCP bucket location
##
## ### Required Inputs:
## - sample_name: Unique sample identifier for file naming and tracking
## - left_fq, right_fq: Paired-end FASTQ files (R1/R2), or fastq_pair_tar_gz as alternative
## - genome_plug_n_play_tar_gz: STAR-Fusion reference genome library (CTAT resource)
## - gtf: Gene annotation file in GTF format for expression quantification
##
## ### Optional Parameters:
## - fusion_inspector: FusionInspector mode - "validate" (default) or "inspect"
## - examine_coding_effect: Predict coding consequences of detected fusions (default: true)
## - min_FFPM: Minimum fusion fragments per million mapped reads threshold (default: 0.1)
## - target_gcp_folder: Destination GCS path for automated output file transfer
##
## ### Output Files:
## Fusion Detection Results:
## - fusion_predictions_abridged: Summary table of detected fusions
## - fusion_predictions: Detailed fusion predictions with supporting evidence
## - fusion_coding_effect: Predicted protein-level effects of fusions
## - fusion_chimeric_out_junction: Chimeric junction reads from STAR alignment
## - fusion_inspector_validate_web: HTML report for validated fusions (if fusion_inspector = "validate")
## - fusion_inspector_validate_fusions_abridged: Validated fusion summary (if fusion_inspector = "validate")
## - fusion_inspector_inspect_web: HTML report for inspected fusions (if fusion_inspector = "inspect")
## - fusion_inspector_inspect_fusions_abridged: Inspected fusion summary (if fusion_inspector = "inspect")
## - fusion_junction_file_size_mb: Size of the chimeric junction file in MB, usually it should be a few MBs. If it is too small, it may indicate an issue with the fusion detection.
##
## Expression Quantification Results:
## - star_summary: STAR alignment metrics and mapping statistics
## - featurecounts_count: Gene-by-sample count matrix
## - featurecounts_count_summary: Feature assignment statistics and QC metrics
##
## ### Implementation Notes:
## - STAR-Fusion provides high-sensitivity fusion detection with low false-positive rates
## - featureCounts offers fast, accurate gene-level quantification from BAM alignments
## - All outputs compatible with standard downstream analysis tools and visualization platforms
## - GCP file transfer is conditional and only executes when target_gcp_folder is specified

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
    Int memory_gb = 60
    Float genome_disk_space_multiplier = 2.5
    Int preemptible = 3
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
      fastq_disk_space_multiplier = fastq_disk_space_multiplier + 1,
      genome_disk_space_multiplier = genome_disk_space_multiplier,
      fusion_inspector = fusion_inspector,
      use_ssd = use_ssd
  }

  Float junction_file_size_mb = size(STARFusion.junction, "MB")

  if (defined(target_gcp_folder)) {
    String gcs_output_dir_1 = sub(select_first([target_gcp_folder]), "/+$", "") + "/" + sample_name + "/"
    call GcpUtils.MoveOrCopyFiles as CopyFile1 {
      input:
        source_file1 = STARFusion.fusion_predictions_abridged,
        source_file2 = STARFusion.fusion_predictions,
        source_file3 = STARFusion.junction,
        source_file4 = STARFusion.coding_effect,
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

      preemptible = preemptible,
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
    File fusion_predictions_abridged = select_first([CopyFile1.output_file1, STARFusion.fusion_predictions_abridged])
    File fusion_predictions = select_first([CopyFile1.output_file2, STARFusion.fusion_predictions])
    File fusion_chimeric_out_junction = select_first([CopyFile1.output_file3, STARFusion.junction])

    File? fusion_coding_effect = if(defined(CopyFile1.output_file4)) then CopyFile1.output_file4 else STARFusion.coding_effect
    File? fusion_inspector_validate_web = if(defined(CopyFile1.output_file5)) then CopyFile1.output_file5 else STARFusion.fusion_inspector_validate_web
    File? fusion_inspector_validate_fusions_abridged = if(defined(CopyFile1.output_file6)) then CopyFile1.output_file6 else STARFusion.fusion_inspector_validate_fusions_abridged
    File? fusion_inspector_inspect_web = if(defined(CopyFile1.output_file7)) then CopyFile1.output_file7 else STARFusion.fusion_inspector_inspect_web
    File? fusion_inspector_inspect_fusions_abridged = if(defined(CopyFile1.output_file8)) then CopyFile1.output_file8 else STARFusion.fusion_inspector_inspect_fusions_abridged

    File star_summary = select_first([CopyFile2.output_file1, STAR_Unsorted.output_star_summary])
    File featurecounts_count = select_first([CopyFile2.output_file2, FeatureCounts.output_count])
    File featurecounts_count_summary = select_first([CopyFile2.output_file3, FeatureCounts.output_count_summary])

    Float fusion_junction_file_size_mb = junction_file_size_mb
  }
}
