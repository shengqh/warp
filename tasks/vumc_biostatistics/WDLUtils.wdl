version 1.0

task string_to_array {
  input {
    String str
    String delimiter = ","
  }
  command {
    echo ~{str} | tr '~{delimiter}' '\n'
  }
  runtime {
    cpu: 1
    docker: "ubuntu:20.04"
    preemptible: 1
    disks: "local-disk 5 HDD"
    memory: "1 GiB"
  }
  output {
    Array[String] arr = read_lines(stdout())
  }
}

task array_to_map {
  input {
    Array[String] input_strings
  }

  command <<<
    cat <<EOF> script.py

input_str = "~{sep=',' input_strings}"
input_array = input_str.split(',')

with open("output.txt", "w") as f:
  for i in range(len(input_array)):
    f.write(f"{input_array[i]}\t{i}\n")

EOF

python script.py
  
>>>

  runtime {
    docker: "python:3.9-slim"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }

  output {
    Map[String, Int] index_map = read_map("output.txt")
  }
}

task count_lines {
  input {
    File input_file
    Boolean ignore_comments = false
  }

  Int disk_size = ceil(size(input_file, "GB")) + 2

  command <<<
  
# Count the number of lines in the file
if [[ "~{ignore_comments}" == "true" ]]; then
  grep -v '^#' ~{input_file} | wc -l | cut -d ' ' -f 1 > line_count.txt
else
  wc -l ~{input_file} | cut -d ' ' -f 1 > line_count.txt
fi

  >>>

  output {
    Int num_lines = read_int("line_count.txt")
  }

  runtime {
    cpu: 1
    docker: "ubuntu:20.04"
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: "1 GiB"
  }
}

task sum_integers {
  input {
    Array[Int] input_integers
  }

  command <<<
    echo ~{sep=' ' input_integers} | tr ' ' '\n' | awk '{s+=$1} END {print s}' > sum.txt
  >>>

  output {
    Int sum = read_int("sum.txt")
  }

  runtime {
    cpu: 1
    docker: "ubuntu:20.04"
    preemptible: 1
    disks: "local-disk 5 HDD"
    memory: "1 GiB"
  }
}

workflow read_pair_from_csv {
  input {
    File input_list_csv
    Boolean has_header = true
  }
  
  Array[String] my_lines = read_lines(input_list_csv)
  scatter (line in my_lines) {
    String filtered_line = sub(line, '"', '')
    String cur_file_name = sub(filtered_line, ",.+$", "")
    String cur_file_path = sub(filtered_line, ".+,", "")
    Pair[String, String] cur_file = (cur_file_name, cur_file_path)
  }

  if (has_header) {
    Int len = length(cur_file)
    scatter(idx in range(len-1)){
      Int next_idx = idx + 1
      Pair[String, String] cur_file2 = cur_file[next_idx]
    }
  } 

  output {
    Array[Pair[String, String]] output_pairs = select_first([cur_file2, cur_file])
  }
}

task write_lines_to_file {
  input {
    Array[String] lines
    String output_file_name
  }

  File tmp_file = write_lines(lines)

  command <<<
mv ~{tmp_file} ~{output_file_name}
>>>

  output {
    File output_file = "~{output_file_name}"
  }

  runtime {
    cpu: 1
    docker: "ubuntu:20.04"
    preemptible: 1
    disks: "local-disk 5 HDD"
    memory: "1 GiB"
  }
}
