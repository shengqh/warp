version 1.0

## VUMC RNA-Seq Analysis Workflow with STAR and FeatureCounts (NoBam version)
##
## This workflow processes RNA-Seq data using STAR alignment and FeatureCounts quantification,
## without preserving BAM files in the final outputs to save storage space.
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
## 3. Optionally copy output files to a specified GCP folder
##
## ### Inputs:
## - fastq_1, fastq_2: Paired-end FASTQ files
## - sample_name: Identifier for the sample
## - Reference genome files (chrLength_txt, chrNameLength_txt, etc.)
## - gtf: Gene annotation file
## - billing_gcp_project_id: Optional GCP project ID for file copy operations
## - target_gcp_folder: Optional target GCP folder for the output files
##
## ### Outputs:
## - output_star_chromosome_count: STAR chromosome count
## - output_star_summary: STAR alignment summary
## - output_count: FeatureCounts gene count file
## - output_count_summary: FeatureCounts summary file
##
## ### Notes:
## - Utilizes STAR for efficient spliced alignments
## - FeatureCounts for accurate gene-level quantification
## - BAM files are used internally but not preserved in final outputs
## - File copy operation to GCP is optional and only executed if a target folder is provided

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "./RNAseqUtils.wdl" as RNAseqUtils

workflow VUMCStarFusion {
  input {
    File fastq_1
    File fastq_2
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

    String? billing_gcp_project_id
    String? target_gcp_folder
  }

  call RNAseqUtils.STARFusion {
    input:
      fastq_1 = fastq_1,
      fastq_2 = fastq_2,
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

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyFiveFiles as CopyFile {
      input:
        source_file1 = STARFusion.output_fusion_predictions_abridged_coding_effect,
        source_file2 = STARFusion.output_fusion_predictions_abridged,
        source_file3 = STARFusion.output_fusion_predictions,
        source_file4 = STARFusion.output_fusion_inspector_web,
        source_file5 = STARFusion.output_fusion_inspector_fusions,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }
  # Outputs that will be retained when execution is complete
  output {
    String output_fusion_predictions_abridged_coding_effect = select_first([CopyFile.output_file1, STARFusion.output_fusion_predictions_abridged_coding_effect])
    String output_fusion_predictions_abridged = select_first([CopyFile.output_file2, STARFusion.output_fusion_predictions_abridged])
    String output_fusion_predictions = select_first([CopyFile.output_file3, STARFusion.output_fusion_predictions])
    String output_fusion_inspector_web = select_first([CopyFile.output_file4, STARFusion.output_fusion_inspector_web])
    String output_fusion_inspector_fusions = select_first([CopyFile.output_file5, STARFusion.output_fusion_inspector_fusions])
  }
}
