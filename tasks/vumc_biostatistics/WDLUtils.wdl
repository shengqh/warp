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
