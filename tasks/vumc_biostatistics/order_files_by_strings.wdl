version 1.0

import "./WDLUtils.wdl" as WDLUtils

workflow order_files_by_strings {
  input {
    Array[String] input_files
    Array[String] expect_files
  }

  scatter(input_file in input_files) {
    String actual_file = basename(input_file)
  }

  call WDLUtils.array_to_map {
    input: 
        input_strings = actual_file
  }

  scatter(expect_file in expect_files) {
    Int index = array_to_map.index_map[expect_file]
    String result_file = input_files[index]
  }

  output {
    Array[String] ordered_files = result_file
  }
}
