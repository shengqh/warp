version 1.0

## VUMC RNA-Seq Analysis Workflow with STAR and FeatureCounts
##
## This workflow processes RNA-Seq data using STAR alignment and FeatureCounts quantification.
## Optionally outputs sorted BAM files based on user configuration.
## Developed by VUMC/VANGARD team for efficient processing of RNA-Seq data.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## This pipeline handles RNA-Seq data processing from FASTQ files to gene counts,
## enabling gene expression analysis and downstream applications.
##
## ### Workflow Steps:
## 1. STAR: Align paired-end FASTQ files to reference genome
## 2. FeatureCounts: Quantify gene expression from aligned BAM files
## 3. (Optional) Sort BAM files by coordinate if output_sorted_bam is true
## 4. (Optional) Copy output files to a specified GCP folder
##
## ### Inputs:
## - left_fq, right_fq: Paired-end FASTQ files (or fastq_pair_tar_gz as alternative)
## - sample_name: Identifier for the sample
## - Reference genome files (chrLength_txt, chrNameLength_txt, Genome, SA, SAindex, etc.)
## - gtf: Gene annotation file for quantification
## - output_sorted_bam: Flag to generate coordinate-sorted BAM files (default: true)
## - target_gcp_folder: Optional GCP destination for output files
##
## ### Outputs:
## - output_star_summary: STAR alignment summary statistics
## - output_count: FeatureCounts gene count matrix
## - output_count_summary: FeatureCounts alignment summary
## - output_bam: Coordinate-sorted BAM file (optional, controlled by output_sorted_bam)
## - output_bam_index: BAM index file (optional, controlled by output_sorted_bam)
##
## ### Notes:
## - Supports flexible input: individual FASTQ files or compressed tar.gz pairs
## - STAR provides efficient spliced alignment for RNA-Seq data
## - FeatureCounts delivers accurate gene-level quantification
## - BAM output is configurable to balance storage and downstream analysis needs
## - GCP file transfer occurs only when target_gcp_folder is specified

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/broad/BamProcessing.wdl" as Processing
import "./RNAseqUtils.wdl" as RNAseqUtils

workflow VUMCStarFeaturecounts {
  input {
    # input data options
    File? left_fq
    File? right_fq
    File? fastq_pair_tar_gz

    String sample_name
    
    File chrLength_txt
    File chrNameLength_txt
    File chrName_txt
    File chrStart_txt
    File exonGeTrInfo_tab
    File exonInfo_tab
    File geneInfo_tab
    File Genome
    File genomeParameters_txt
    File SA
    File SAindex
    File sjdbInfo_txt
    File sjdbList_fromGTF_out_tab
    File sjdbList_out_tab
    File transcriptInfo_tab

    File gtf

    Boolean output_sorted_bam = true

    String? target_gcp_folder
  }

  call RNAseqUtils.STAR_Unsorted {
    input:
      fastq_pair_tar_gz = fastq_pair_tar_gz,
      left_fq = left_fq,
      right_fq = right_fq,
      sample_name = sample_name,
      chrLength_txt = chrLength_txt,
      chrNameLength_txt = chrNameLength_txt,
      chrName_txt = chrName_txt,
      chrStart_txt = chrStart_txt,
      exonGeTrInfo_tab = exonGeTrInfo_tab,
      exonInfo_tab = exonInfo_tab,
      geneInfo_tab = geneInfo_tab,
      Genome = Genome,
      genomeParameters_txt = genomeParameters_txt,
      SA = SA,
      SAindex = SAindex,
      sjdbInfo_txt = sjdbInfo_txt,
      sjdbList_fromGTF_out_tab = sjdbList_fromGTF_out_tab,
      sjdbList_out_tab = sjdbList_out_tab,
      transcriptInfo_tab = transcriptInfo_tab
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
