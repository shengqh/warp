version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC AutoGVP Run AutoGVP Sub-workflow
##
## This workflow runs AutoGVP to combine VEP, InterVar, ANNOVAR gnomAD, AutoPVS1,
## and ClinVar annotations into a final pathogenicity assessment.
## Developed by VUMC Biostatistics as a standalone test wrapper for the RunAutoGVP task.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Given outputs from VEP, InterVar, ANNOVAR gnomAD, AutoPVS1, and ClinVar references,
## this workflow produces the final combined AutoGVP variant pathogenicity classification.
##
## ### Workflow Steps:
## 1. **RunAutoGVP**: Combine all annotations into a final pathogenicity assessment.
## 2. **CopyFile (Optional)**: Copy output to GCP if target_gcp_folder is provided.
##
## ### Inputs:
## - vep_vcf: VEP-annotated VCF file.
## - intervar_file: InterVar interpretation results file.
## - annovar_file: ANNOVAR gnomAD annotation results file.
## - autopvs1_file: AutoPVS1 PVS1 criterion results file.
## - clinvar_vcf: ClinVar VCF file.
## - selected_clinvar_submissions: Selected ClinVar submissions file.
## - variant_summary: ClinVar variant summary file.
## - submission_summary: ClinVar submission summary file.
## - target_prefix: Prefix for the output AutoGVP results.
## - target_gcp_folder: Optional GCP folder path for copying output files.
##
## ### Outputs:
## - autogvp_file: Final AutoGVP combined pathogenicity assessment file.

import "VUMCAutoGVP.wdl" as AutoGVP
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCAutoGVPRunAutoGVP {
  input {
    File vep_vcf
    File intervar_file
    File annovar_file
    File autopvs1_file

    File clinvar_vcf
    File selected_clinvar_submissions
    File variant_summary
    File submission_summary

    String target_prefix

    String? target_gcp_folder
  }

  call AutoGVP.RunAutoGVP {
    input:
      vep_vcf = vep_vcf,
      intervar_file = intervar_file,
      annovar_file = annovar_file,
      autopvs1_file = autopvs1_file,
      clinvar_vcf = clinvar_vcf,
      selected_clinvar_submissions = selected_clinvar_submissions,
      variant_summary = variant_summary,
      submission_summary = submission_summary,
      target_prefix = target_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = RunAutoGVP.autogvp_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File autogvp_file = select_first([CopyFile.output_file, RunAutoGVP.autogvp_file])
  }
}
