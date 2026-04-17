version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC AutoGVP Get Gene Locus Sub-workflow
##
## This workflow retrieves genomic coordinates for specified gene symbols using biomaRt.
## Developed by VUMC Biostatistics as a standalone test wrapper for the GetGeneLocus task.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Given one or more gene symbols (comma-separated), this workflow queries Ensembl biomaRt
## to obtain chromosomal coordinates and produces a BED file of gene loci.
##
## ### Workflow Steps:
## 1. **GetGeneLocus**: Query Ensembl biomaRt for gene coordinates and produce a BED file.
## 2. **CopyFile (Optional)**: Copy output to GCP if target_gcp_folder is provided.
##
## ### Inputs:
## - gene_symbols: Comma-separated gene symbols (e.g., "BRCA1,BRCA2").
## - shift_bases: Number of bases to extend beyond gene boundaries. Default is 2000.
## - target_gcp_folder: Optional GCP folder path for copying output files.
##
## ### Outputs:
## - gene_bed: BED file with gene locus coordinates.

import "VUMCAutoGVP.wdl" as AutoGVP
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAutoGVPGetGeneLocus {
  input {
    String gene_symbols
    Int? shift_bases

    String? target_gcp_folder
  }

  call AutoGVP.GetGeneLocus {
    input:
      gene_symbols = gene_symbols,
      shift_bases = select_first([shift_bases, 2000])
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = GetGeneLocus.gene_bed,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File gene_bed = select_first([CopyFile.output_file, GetGeneLocus.gene_bed])
  }
}
