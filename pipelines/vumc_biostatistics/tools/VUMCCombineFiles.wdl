version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCCombineFiles {
  input {
    Array[String] input_names
    Array[String] input_files
    String sep="\\t"
    Boolean has_header = false
    String output_prefix
    String output_suffix = ".txt"
    String? billing_gcp_project_id
    String? target_gcp_folder  
  }

  call CombineFiles {
    input:
      input_names = input_names,
      input_files = input_files,
      sep = sep,
      has_header = has_header,
      output_file = output_prefix + output_suffix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = CombineFiles.combined_file,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_file = select_first([CopyFile.output_file, CombineFiles.combined_file])
  }
}

task CombineFiles {
  input {
    Array[String] input_names
    Array[String] input_files
    String sep
    Boolean has_header
    String output_file
    String docker = "shengqh/hail_gcp:20241127"
  }

  String header_read = if has_header then "'infer'" else "None"
  String header_write = if has_header then "True" else "False"

  command <<<

cat<<EOF > combine.py

import pandas as pd
import os

name_file="~{write_lines(input_names)}"
list_file="~{write_lines(input_files)}"

with open("~{output_file}", "wt") as fout, open(name_file, "rt") as fnamein, open(list_file, "rt") as fdatain:
    for fname in fnamein:
        fname = fname.strip()
        fdata = fdatain.readline().strip()
        df = pd.read_csv(fdata, sep="~{sep}", header=~{header_read})
        df.insert(0, 'Sample', fname)
        df.to_csv(fout, sep="~{sep}", index=False, header=~{header_write})
EOF

python3 combine.py

>>>

  runtime {
    docker: docker
    memory: 10 + " GiB"
    disks: "local-disk " + 10 + " HDD"
    cpu: 1
    preemptible: 1
  }

  output {
    File combined_file = "~{output_file}"
  }
}
