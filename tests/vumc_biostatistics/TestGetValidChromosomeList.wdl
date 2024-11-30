version 1.0

import "../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils

workflow TestGetValidChromosomeList {
  input {
    File input_pvar
    Array[String] input_chromosomes
  }

  call BioUtils.GetValidChromosomeList {
    input:
      input_pvar = input_pvar,
      input_chromosomes = input_chromosomes
  }
  
  output {
    Array[String] valid_chromosomes = GetValidChromosomeList.valid_chromosomes
  }
}
