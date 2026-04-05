version 1.0

## Copyright Broad Institute, 2019
## 
## The haplotypecaller-gvcf-gatk4 workflow runs the HaplotypeCaller tool
## from GATK4 in GVCF mode on a single sample according to GATK Best Practices.
## When executed the workflow scatters the HaplotypeCaller tool over a sample
## using an intervals list file. The output file produced will be a
## single gvcf file which can be used by the joint-discovery workflow.
##
## Requirements/expectations :
## - One analysis-ready BAM file for a single sample (as identified in RG:SM)
## - Set of variant calling intervals lists for the scatter, provided in a file
##
## Outputs :
## - One GVCF file and its index
##
## Cromwell version support 
## - Successfully tested on v53
##
## Runtime parameters are optimized for Broad's Google Cloud Platform implementation.
##
## LICENSING : 
## This script is released under the WDL source code license (BSD-3) (see LICENSE in 
## https://github.com/broadinstitute/wdl). Note however that the programs it calls may 
## be subject to different licenses. Users are responsible for checking that they are
## authorized to run all programs before running this script. Please see the dockers
## for detailed licensing information pertaining to the included programs.

import "../../../../wdl/dna_seq/germline/joint_genotyping/reblocking/ReblockGVCF.wdl" as BroadReblock
import "../../../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCReblockGVCF {
  input {
    File input_gvcf
    File input_gvcf_index

    String gvcf_file_extension = ".g.vcf.gz"
    
    File ref_dict
    File ref_fasta
    File ref_fasta_index
    File scattered_calling_intervals_list

    String cloud_provider = "gcp"

    String? target_gcp_folder  
  }  

  call BroadReblock.ReblockGVCF as Reblock {
    input:
      gvcf = input_gvcf,
      gvcf_index = input_gvcf_index,
      gvcf_file_extension = gvcf_file_extension,
      ref_fasta = ref_fasta,
      ref_fasta_index = ref_fasta_index,
      ref_dict = ref_dict,
      calling_interval_list = scattered_calling_intervals_list,
      cloud_provider = cloud_provider
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = Reblock.reblocked_gvcf,
        source_file2 = Reblock.reblocked_gvcf_index,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder]),
    }
  }

  output {
    File output_vcf = select_first([CopyFile.output_file1, Reblock.reblocked_gvcf])
    File output_vcf_index = select_first([CopyFile.output_file2, Reblock.reblocked_gvcf_index])
  }
}
