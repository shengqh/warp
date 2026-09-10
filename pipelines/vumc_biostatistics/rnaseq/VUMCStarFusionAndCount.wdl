version 1.0

## VUMC STAR-Fusion and RNA-Seq Quantification Workflow
##
## This WDL workflow analyzes paired-end RNA-Seq data in parallel branches:
## STAR-Fusion detects candidate gene fusions, while STAR alignment and featureCounts
## generate gene-level expression estimates in a single unified workflow.
## Developed by the VUMC/VANGARD Bioinformatics Core for translational genomics research.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Overview:
## FASTQ files are optionally extracted from a tar.gz archive, then processed by
## STAR-Fusion for fusion detection and by STAR plus featureCounts for expression
## quantification. Results can optionally be copied to a specified GCP location.
##
## ### Processing Steps:
## 1. Input preparation: Untar FASTQ files if provided as a compressed archive
## 2. STAR-Fusion analysis: Identify gene fusions using STAR aligner with fusion detection
## 3. STAR alignment: Generate a BAM file suitable for downstream gene-level quantification
## 4. featureCounts: Calculate gene-level expression counts from the aligned reads
## 5. Output management: Optionally copy results to a specified GCP bucket location
##
## ### Required Inputs:
## - sample_name: Unique sample identifier for file naming and tracking
## - left_fq, right_fq: Paired-end FASTQ files (R1/R2), or fastq_pair_tar_gz as an alternative
## - genome_plug_n_play_tar_gz: STAR-Fusion reference genome library (CTAT resource)
## - reference: STAR reference used for alignment and expression quantification
## - gtf: Gene annotation file in GTF format for expression quantification
##
## ### Optional Parameters:
## - fastq_pair_tar_gz_output_extension: Extension assigned to FASTQ files extracted from the archive (default: .fastq.gz)
## - uncompressed_genome_size_gb: Uncompressed STAR-Fusion genome size in GB (default: 72)
## - fusion_inspector: FusionInspector mode - "validate" (default) or "inspect"
## - examine_coding_effect: Predict coding consequences of detected fusions (default: true)
## - min_FFPM: Minimum fusion fragments per million mapped reads threshold (default: 0.1)
## - docker: Docker image used for STAR-Fusion and RNA-Seq processing (default: trinityctat/starfusion:1.15.1)
## - cpu: Number of CPUs allocated to processing tasks (default: 12)
## - star_memory_gb: Memory allocated to STAR alignment in GB (default: 40)
## - star_fastq_disk_space_multiplier: STAR FASTQ disk space multiplier (default: 3.25)
## - star_extra_disk_space: Additional STAR disk space in GB (default: 10)
## - star_use_ssd: Whether to use SSD storage for STAR alignment (default: true)
## - fusion_memory_gb: Memory allocated to STAR-Fusion in GB (default: 50)
## - fusion_fastq_disk_space_multiplier: STAR-Fusion FASTQ disk space multiplier (default: 3.25)
## - fusion_extra_disk_space: Additional STAR-Fusion disk space in GB (default: 10)
## - fusion_use_ssd: Whether to use SSD storage for STAR-Fusion (default: true)
## - preemptible: Maximum number of preemptible VM retries (default: 3)
## - target_gcp_folder: Destination GCS path for optional output copying
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
## - Outputs are compatible with standard downstream analysis and visualization tools
## - GCP output copying is conditional on target_gcp_folder being specified

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../format/VUMCUntar.wdl" as UntarModule
import "./RNAseqUtils.wdl" as RNAseqUtils

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
    Int uncompressed_genome_size_gb = 72 # for GRCh38_gencode_v44_CTAT_lib_Oct292023.plug-n-play

    # STAR-Fusion parameters
    String fusion_inspector = "validate"  # inspect or validate
    Boolean examine_coding_effect = true
    Float min_FFPM = 0.1

    # based on test, the memory usually less than 40, adding 5 gb only add about 1 cent per hour, so we can set it to 45 gb to be safe
    Int fusion_memory_gb = 45

    # since in fusion, we will delete the genome tar.gz file after untar which will free up about 27 gb,
    # so we don't need to reserve extra disk space for fastq files
    Float fusion_fastq_disk_space_multiplier = 1
    Float fusion_extra_disk_space = 5
    Boolean fusion_use_ssd = true

    # for star+featureCounts, we don't need the whole database for star_fusion which requires about 100gb space
    # we can use small one with about 37 g
    StarReference reference
    
    # based on test, the memory usually less than 40, adding 5 gb only add about 1 cent per hour, so we can set it to 45 gb to be safe
    Int star_memory_gb = 45
    Float star_fastq_disk_space_multiplier = 3.25
    Float star_extra_disk_space = 5
    Boolean star_use_ssd = true

    # for featureCounts, use the gtf extracted from genome_plug_n_play_tar_gz to keep consistent
    File gtf

    # runtime params
    String docker = "trinityctat/starfusion:1.15.1"
    Int cpu = 12

    Int preemptible = 3

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

  call RNAseqUtils.STARForCount as STAR_Unsorted {
    input:
      sample_name = sample_name,

      left_fq = final_left_fq,
      right_fq = final_right_fq,
      fastq_disk_space_multiplier = star_fastq_disk_space_multiplier,

      reference = reference,

      preemptible = preemptible,
      docker = docker,
      cpu = cpu,
      memory_gb = star_memory_gb,
      extra_disk_space = star_extra_disk_space,
      use_ssd = star_use_ssd
  }

  call RNAseqUtils.FeatureCounts {
    input:
      bam = STAR_Unsorted.output_bam,
      sample_name = sample_name,
      gtf = gtf
  }

  if (defined(target_gcp_folder)) {
    String gcs_output_dir_1 = sub(select_first([target_gcp_folder]), "/+$", "") + "/" + sample_name + "/"
    call GcpUtils.MoveOrCopyFiles as CopyFile1 {
      input:
        source_file1 = STAR_Unsorted.output_star_summary,
        source_file2 = FeatureCounts.output_count,
        source_file3 = FeatureCounts.output_count_summary,
        is_move_file = false,
        target_gcp_folder = gcs_output_dir_1
    }
  }

  call RNAseqUtils.star_fusion as STARFusion {
    input:
      sample_name = sample_name,

      left_fq = final_left_fq,
      right_fq = final_right_fq,
      fastq_disk_space_multiplier = fusion_fastq_disk_space_multiplier,

      genome = genome_plug_n_play_tar_gz,
      uncompressed_genome_size_gb = uncompressed_genome_size_gb,

      examine_coding_effect = examine_coding_effect,
      coord_sort_bam = false,
      min_FFPM = min_FFPM,
      fusion_inspector = fusion_inspector,

      preemptible = preemptible,
      docker = docker,
      cpu = cpu,
      memory = fusion_memory_gb + " GiB",
      extra_disk_space = fusion_extra_disk_space,
      use_ssd = fusion_use_ssd
  }

  Float junction_file_size_mb = size(STARFusion.junction, "MB")

  if (defined(target_gcp_folder)) {
    String gcs_output_dir_2 = sub(select_first([target_gcp_folder]), "/+$", "") + "/" + sample_name + "/"
    call GcpUtils.MoveOrCopyFiles as CopyFile2 {
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
        target_gcp_folder = gcs_output_dir_2
    }
  }

  # Outputs that will be retained when execution is complete
  output {
    File star_summary = select_first([CopyFile1.output_file1, STAR_Unsorted.output_star_summary])
    File featurecounts_count = select_first([CopyFile1.output_file2, FeatureCounts.output_count])
    File featurecounts_count_summary = select_first([CopyFile1.output_file3, FeatureCounts.output_count_summary])

    File fusion_predictions_abridged = select_first([CopyFile2.output_file1, STARFusion.fusion_predictions_abridged])
    File fusion_predictions = select_first([CopyFile2.output_file2, STARFusion.fusion_predictions])
    File fusion_chimeric_out_junction = select_first([CopyFile2.output_file3, STARFusion.junction])

    File? fusion_coding_effect = if(defined(CopyFile2.output_file4)) then CopyFile2.output_file4 else STARFusion.coding_effect
    File? fusion_inspector_validate_web = if(defined(CopyFile2.output_file5)) then CopyFile2.output_file5 else STARFusion.fusion_inspector_validate_web
    File? fusion_inspector_validate_fusions_abridged = if(defined(CopyFile2.output_file6)) then CopyFile2.output_file6 else STARFusion.fusion_inspector_validate_fusions_abridged
    File? fusion_inspector_inspect_web = if(defined(CopyFile2.output_file7)) then CopyFile2.output_file7 else STARFusion.fusion_inspector_inspect_web
    File? fusion_inspector_inspect_fusions_abridged = if(defined(CopyFile2.output_file8)) then CopyFile2.output_file8 else STARFusion.fusion_inspector_inspect_fusions_abridged

    Float fusion_junction_file_size_mb = junction_file_size_mb
  }
}
