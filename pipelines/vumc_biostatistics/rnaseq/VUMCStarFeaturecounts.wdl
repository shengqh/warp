version 1.0

## VUMC RNA-Seq Analysis Workflow with STAR and FeatureCounts
##
## This workflow aligns RNA-Seq FASTQ files with STAR and quantifies gene-level counts with FeatureCounts.
## It supports paired-end FASTQ inputs, compressed FASTQ tar.gz archives, and optional coordinate-sorted BAM output.
## Developed by the VUMC/VANGARD team for efficient and reproducible RNA-Seq processing.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## This pipeline converts raw RNA-Seq reads into aligned BAM files and gene-count tables for downstream
## expression analysis, differential expression testing, and other transcriptomics workflows.
##
## ### Workflow Steps:
## 1. STAR: Align paired-end FASTQ files to the reference genome
## 2. FeatureCounts: Count reads assigned to genes using the provided GTF annotation
## 3. (Optional) Sort the aligned BAM by genomic coordinate when output_sorted_bam is true
## 4. (Optional) Copy selected output files to a target GCP folder
##
## ### Inputs:
## - left_fq, right_fq: Paired-end FASTQ files or alternative compressed FASTQ pair inputs
## - sample_name: Sample identifier used in output naming
## - Reference genome files (chrLength.txt, chrNameLength.txt, Genome, SA, SAindex, etc.)
## - gtf: Gene annotation file used for read counting
## - output_sorted_bam: Flag to generate coordinate-sorted BAM files (default: true)
## - target_gcp_folder: Optional destination for copied output files
##
## ### Outputs:
## - output_star_summary: STAR alignment summary statistics
## - output_count: FeatureCounts gene count matrix
## - output_count_summary: FeatureCounts read-assignment summary
## - output_bam: Coordinate-sorted BAM file (optional, controlled by output_sorted_bam)
## - output_bam_index: BAM index file (optional, controlled by output_sorted_bam)
##
## ### Notes:
## - Supports both individual FASTQ files and compressed tar.gz paired-input archives
## - STAR provides efficient spliced alignment for RNA-Seq reads
## - FeatureCounts offers accurate gene-level quantification
## - BAM output can be enabled or disabled to balance storage and downstream analysis needs
## - GCP transfer occurs only when target_gcp_folder is provided

import "../format/VUMCUntar.wdl" as UntarModule
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/wdl/BamProcessing.wdl" as Processing
import "./RNAseqUtils.wdl" as RNAseqUtils

workflow VUMCStarFeaturecounts {
  input {
    String sample_name

    File? left_fq
    File? right_fq

    File? fastq_pair_tar_gz
    File? fastq_pair_tar_gz_output_extension = ".fastq.gz"

    StarReference reference

    File gtf

    Boolean output_sorted_bam = true

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
      reference = reference,
  }

  call RNAseqUtils.FeatureCounts {
    input:
      bam = STAR_Unsorted.output_bam,
      sample_name = sample_name,
      gtf = gtf
  }

  if(output_sorted_bam){
    call Processing.SortSam {
      input:
        input_bam = STAR_Unsorted.output_bam,
        output_bam_basename = sample_name + "_Aligned.sortedByCoord.out",
        compression_level = 2,
        preemptible_tries = 3
    }
  }

  if (defined(target_gcp_folder)) {
    String gcs_output_dir = sub(select_first([target_gcp_folder]), "/+$", "") + "/" + sample_name + "/"
    call GcpUtils.MoveOrCopyFiles as CopyFile {
      input:
        source_file1 = STAR_Unsorted.output_star_summary,
        source_file2 = FeatureCounts.output_count,
        source_file3 = FeatureCounts.output_count_summary,
        source_file4 = SortSam.output_bam,
        source_file5 = SortSam.output_bam_index,
        is_move_file = false,
        target_gcp_folder = gcs_output_dir
    }
  }
  # Outputs that will be retained when execution is complete
  output {
    File output_star_summary = select_first([CopyFile.output_file1, STAR_Unsorted.output_star_summary])
    File output_count = select_first([CopyFile.output_file2, FeatureCounts.output_count])
    File output_count_summary = select_first([CopyFile.output_file3, FeatureCounts.output_count_summary])
    File? output_bam = if (output_sorted_bam) then select_first([CopyFile.output_file4, SortSam.output_bam]) else SortSam.output_bam
    File? output_bam_index = if (output_sorted_bam) then select_first([CopyFile.output_file5, SortSam.output_bam_index]) else SortSam.output_bam_index 
  }
}
