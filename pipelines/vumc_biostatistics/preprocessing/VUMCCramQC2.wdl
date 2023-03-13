version 1.0

#WORKFLOW DEFINITION   
workflow VUMCCramQC2 {
  input {
    Array[File] input_crams
    String sample_name
    String samtools_docker = "registry.hub.docker.com/mgibio/samtools-cwl"
    String samtools_path = "/samtools-cwl/"
    File reference_file
  }

  output {
    Int unmapped_reads = CountCRAM.NumberUnmappedReads
    Int mapped_reads = CountCRAM.NumberMappedReads
  }

  call CountCRAM {
    input:
      input_crams = input_crams,
      sample_name = sample_name,
      docker = samtools_docker,
      samtools_path = samtools_path,
      reference_file = reference_file,
      
  }
}

# TASK DEFINITIONS
# Counting the number of mapped and unmapped reads in CRAM file using samtools
task CountCRAM {
  input {
    # Command parameters
    Array[File] input_crams
    String sample_name
    String samtools_path
    File reference_file

  
    # Runtime parameters
    String docker
    Int machine_mem_gb = 4
    Int addtional_disk_space_gb = 50
  }
    
  Int disk_size = ceil(size(input_crams, "GB")) + addtional_disk_space_gb
  String NumUnmapped = "${sample_name}_Unmapped.txt"
  String NumMapped = "${sample_name}_Mapped.txt"

  command <<<
    echo "0" > ~{NumUnmapped}
    echo "0" > ~{NumMapped}

    for input_cram in ~{sep=" " input_crams}
    do
    
    ~{samtools_path} \
        samtools flagstat $input_cram |cut -f1 -d' '|head -n3|tail -n1 > ~{NumMapped}

    ~{samtools_path} \
        samtools view -c -T $reference_file $input_cram > ~{NumUnmapped}

    done
  >>>

  runtime {
    docker: docker
    memory: machine_mem_gb + " GB"
    disks: "local-disk " + disk_size + " HDD"
  }
  output {
    Int NumberUnmappedReads = read_int("~{NumMapped}")
    Int NumberMappedReads = read_int("~{NumUnmapped}")
  }
}