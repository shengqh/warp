version 1.0

import "../../tasks/vumc_biostatistics/WDLUtils.wdl" as WdlUtils

workflow match_chromosome {
  input {
    Array[String] input_chromosomes = ["1", "2", "3"]
    Array[String] input_pgen_files = ["file1.pgen", "file2.pgen", "file3.pgen"]
    Array[String] input_psam_files = ["file1.psam", "file2.psam", "file3.psam"]
    Array[String] input_pvar_files = ["file1.pvar", "file2.pvar", "file3.pvar"] 

    Array[String] target_chromosomes  = ["1", "3"]
  }

  call WdlUtils.array_to_map {
    input:
      input_strings = input_chromosomes
  }
  
  Map[String, Int] chrom_to_index = array_to_map.index_map

  scatter(chrom in target_chromosomes) {
    Int chrom_index = chrom_to_index[chrom]
    String chromosome = chrom
    File pgen_file = input_pgen_files[chrom_index]
    File psam_file = input_psam_files[chrom_index]
    File pvar_file = input_pvar_files[chrom_index]
  }

  output {
    Array[String] matched_chromosomes = target_chromosomes
    Array[String] matched_pgen_files = pgen_file
    Array[String] matched_psam_files = psam_file
    Array[String] matched_pvar_files = pvar_file
  }
}
