version 1.0

workflow order_files_by_strings {
    input {
        Array[String] input_files
        Array[String] expect_files
    }

    scatter(input_file in input_files) {
        String actual_file = basename(input_file)
    }

    call array_to_map {
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