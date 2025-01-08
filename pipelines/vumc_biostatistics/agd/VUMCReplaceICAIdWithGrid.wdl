version 1.0

import "./AgdUtils.wdl" as AgdUtils

workflow VUMCReplaceICAIdWithGrid {
  input {
    File input_psam

    File id_map_file

    String output_prefix
  }
  
  String replaced_sample_name = "~{output_prefix}.id_mapped.psam"

  call AgdUtils.ReplaceICAIdWithGrid {
    input:
      input_psam = input_psam,
      id_map_file = id_map_file,
      target_psam = replaced_sample_name
  }

  output {
    File output_psam = ReplaceICAIdWithGrid.output_psam
  }
}
