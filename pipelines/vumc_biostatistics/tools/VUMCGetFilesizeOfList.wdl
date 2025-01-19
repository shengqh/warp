version 1.0

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils

/**
 * This workflow, VUMCGetFilesizeOfList, is designed to calculate the file sizes of a list of files.
 * It takes a list of file paths as input and returns the sizes of these files.
 * This can be useful for managing storage and ensuring that file sizes are within expected limits.
 */
workflow VUMCGetFilesizeOfList {
  input {
    /*
    This WDL script calculates the file sizes of a list of files provided in an input list file.

    Parameters:
    - File input_list_csv: The input file containing a list of file paths whose sizes need to be calculated. The first column is name and the second column is URL.
    - String unit: The unit in which the file sizes should be reported. Default is "KB".
    - Boolean has_header: Indicates whether the input list file has a header row. Default is true.
    - String output_prefix: The prefix to be used for the output files.
    */
    File input_list_csv
    String unit = "KB"
    Boolean has_header = true
    String output_prefix
  }

  call WDLUtils.read_pair_from_csv {
    input:
      input_list_csv = input_list_csv,
      has_header = has_header
  }

  scatter(pair in read_pair_from_csv.output_pairs) {
    String file_name = pair.left
    Float file_size=size(pair.right, unit)
    String file_size_str = "~{file_name}\t~{file_size} ~{unit}"
  }

  call WDLUtils.write_lines_to_file {
    input:
      lines = file_size_str,
      output_file_name = "~{output_prefix}.size.txt"
  }

  output {
    File output_size_file = write_lines_to_file.output_file
  }
}
