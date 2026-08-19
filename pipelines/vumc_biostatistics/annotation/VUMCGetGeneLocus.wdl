version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils

workflow VUMCGetGeneLocus {
  input {
    String gene_symbol

    Int frank_bases = 5000

    String? target_gcp_folder
  }

  call BioUtils.GetGeneLocus as GetGeneLocus {
    input:
      gene_symbol = gene_symbol,
      frank_bases = frank_bases
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = GetGeneLocus.gene_bed,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File gene_bed = select_first([CopyFile.output_file, GetGeneLocus.gene_bed])
    String gene_interval = GetGeneLocus.gene_interval
  }
}
