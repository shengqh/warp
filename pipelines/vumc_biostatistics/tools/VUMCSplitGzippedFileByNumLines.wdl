version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCSplitGzippedFileByNumLines {
  input {
    File input_file
    Boolean skip_first_line = false

    Int n_lines_per_file = 750000000 # For pgen txt file, this is about 2 GB per file

    String output_prefix

    String? project_id
    String? target_gcp_folder
  }
  
  call SplitGzippedFileByNumLines as SplitGzippedFile {
    input: 
      input_file = input_file,
      skip_first_line = skip_first_line,
      n_lines_per_file = n_lines_per_file,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyFileArray as CopyFile {
      input:
        source_files = SplitGzippedFile.output_files,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    Array[String] output_files = select_first([CopyFile.outputFiles, SplitGzippedFile.output_files])
  }
}

task SplitGzippedFileByNumLines {
  input {
    File input_file
    Boolean skip_first_line = false
    String output_prefix

    Int machine_mem_gb = 10
    Int n_lines_per_file

    Float size_multiplier = 4.5
    Int addtional_disk_space_gb = 10
    Int cpu = 3
  }

  Int disk_size = ceil(size(input_file, "GB") * size_multiplier + addtional_disk_space_gb)

  command <<<

  if [[ ~{skip_first_line} == "true" ]]; then
    zcat ~{input_file} | tail -n +2 | split - -l ~{n_lines_per_file} --verbose --filter='gzip > $FILE.gz' ~{output_prefix}.
  else
    zcat ~{input_file} | split - -l ~{n_lines_per_file} --verbose --filter='gzip > $FILE.gz' ~{output_prefix}.
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
