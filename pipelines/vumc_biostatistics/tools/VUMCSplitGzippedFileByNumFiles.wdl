version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCSplitGzippedFileByNumFiles {
  input {
    File input_file

    String output_prefix
    Boolean skip_first_line = false

    String? target_gcp_folder
  }
  
  call SplitGzippedFileByNumFiles as SplitGzippedFile {
    input: 
      input_file = input_file,
      skip_first_line = skip_first_line,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyFileArray as CopyFile {
      input:
        source_files = SplitGzippedFile.output_files,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    Array[String] output_files = select_first([CopyFile.outputFiles, SplitGzippedFile.output_files])
  }
}

task SplitGzippedFileByNumFiles {
  input {
    File input_file
    Boolean skip_first_line = false
    String output_prefix

    Int machine_mem_gb = 10

    Int? n_files_override
    Int expect_gb_per_file = 2
    Float size_multiplier = 4
    Int addtional_disk_space_gb = 10
    Int cpu = 3
  }

  Int file_size=ceil(size(input_file, "GB"))

  Int n_files=select_first([n_files_override, ceil(file_size  / expect_gb_per_file)])

  Int disk_size = ceil(file_size * size_multiplier + addtional_disk_space_gb)

  command <<<

  if [[ "~{skip_first_line}" == "true" ]]; then
    zcat ~{input_file} | tail -n +2 | split - -n ~{n_files} --verbose --filter='gzip > $FILE.gz' ~{output_prefix}.
  else
    zcat ~{input_file} | split - -n ~{n_files} --verbose --filter='gzip > $FILE.gz' ~{output_prefix}.
  fi
  
  >>>

  output {
    Array[File] output_files = glob("~{output_prefix}.*.gz")
  }

  runtime {
    docker: "ubuntu:25.10"
    memory: "~{machine_mem_gb} GB"
    disks: "local-disk ~{disk_size} HDD"
    cpu: cpu
  }

}
