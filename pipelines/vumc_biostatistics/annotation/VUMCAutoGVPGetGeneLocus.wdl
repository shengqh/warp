version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC AutoGVP Get Gene Locus Sub-workflow
##
## This workflow retrieves genomic coordinates for specified gene symbols.
## If a GENCODE/Ensembl GTF file is provided, it uses local GTF parsing (stable, offline).
## Otherwise, it falls back to querying Ensembl biomaRt (network-dependent).
## Developed by VUMC Biostatistics as a standalone test wrapper for the GetGeneLocus tasks.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Given one or more gene symbols (comma-separated), this workflow produces a BED file
## of gene loci with optional flanking regions.
##
## ### Workflow Steps:
## 1. **GetGeneLocusGTF** or **GetGeneLocusBiomart**: Determine gene coordinates.
## 2. **CopyFile (Optional)**: Copy output to GCP if target_gcp_folder is provided.
##
## ### Inputs:
## - gene_symbols: Comma-separated gene symbols (e.g., "BRCA1,BRCA2").
## - gene_gtf: Optional GENCODE/Ensembl GTF file for local parsing (recommended).
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
    File? gene_gtf

    String? target_gcp_folder
  }

  if (defined(gene_gtf)) {
    call AutoGVP.GetGeneLocusGTF {
      input:
        gene_symbols = gene_symbols,
        gene_gtf = select_first([gene_gtf])
    }
  }

  if (!defined(gene_gtf)) {
    call AutoGVP.GetGeneLocusBiomart {
      input:
        gene_symbols = gene_symbols
    }
  }

  File gene_bed_result = select_first([GetGeneLocusGTF.gene_bed, GetGeneLocusBiomart.gene_bed])

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = gene_bed_result,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File gene_bed = select_first([CopyFile.output_file, gene_bed_result])
  }
}
