version 1.0

import "../../tasks/vumc_biostatistics/WDLUtils.wdl" as WdlUtils

workflow test_optional_output {
  input {
    String? input_string_optional
  }

  call append_optional_string {
    input:
      input_string_optional = input_string_optional
  }
  
  output {
    String? result_string = append_optional_string.result_string
  }
}

task append_optional_string {
  input {
    String? input_string_optional
  }

  command <<<
    if [ "~{input_string_optional}" ne "" ]; then
      echo "~{input_string_optional} defined" 
    fi
  >>>

  output {
    String? result_string = if(defined(input_string_optional)) then read_string(stdout()) else input_string_optional
  }
}
