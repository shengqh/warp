version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils

import "./AgdUtils.wdl" as AgdUtils

#This workflow will do prepare the AGD PGEN file for release
#1) Replace the GRID used in the AGD with the primary GRID based on ID map file
#2) Keep PASS variants only
workflow VUMCPrepareAgdPgen {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    File id_map_file

    String output_prefix

    String? project_id
    String? target_gcp_folder
  }
  
  String replaced_sample_name = "~{output_prefix}.id_mapped.psam"

  call AgdUtils.ReplaceICAIdWithGrid {
    input:
      input_psam = input_psam,
      id_map_file = id_map_file,
      target_psam = replaced_sample_name
  }

  call Plink2Utils.FilterPassVariantsInPgen {
    input: 
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = ReplaceICAIdWithGrid.output_psam,
      output_prefix = output_prefix + ".primary_pass"
  }

  if(defined(target_gcp_folder)){
    String filtered_pgen = "~{FilterPassVariantsInPgen.output_pgen}"
    String filtered_pvar = "~{FilterPassVariantsInPgen.output_pvar}"
    String filtered_psam = "~{FilterPassVariantsInPgen.output_psam}"

    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = filtered_pgen,
        source_file2 = filtered_pvar,
        source_file3 = filtered_psam,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_pgen = select_first([CopyFile.output_file1, FilterPassVariantsInPgen.output_pgen])
    File output_pvar = select_first([CopyFile.output_file2, FilterPassVariantsInPgen.output_pvar])
    File output_psam = select_first([CopyFile.output_file3, FilterPassVariantsInPgen.output_psam])

    Int num_samples = FilterPassVariantsInPgen.num_samples
    Int num_variants = FilterPassVariantsInPgen.num_variants
  }
}
