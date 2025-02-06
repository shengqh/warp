version 1.0

#WORKFLOW DEFINITION   
workflow VUMCVirus_hhv6_recon {
  input {
    String sample_name

    File input_cram
    File input_cram_index

    File ref_fasta
    File ref_fasta_index

    File? ref_virus_fasta_bwa_fa
    File? ref_virus_fasta_bwa_amb
    File? ref_virus_fasta_bwa_ann
    File? ref_virus_fasta_bwa_bwt
    File? ref_virus_fasta_bwa_pac
    File? ref_virus_fasta_bwa_sa
  }

  call Virus_hhv6_recon {
    input:
      sample_name = sample_name,
      input_cram = input_cram,
      input_cram_index = input_cram_index,
      ref_fasta = ref_fasta,
      ref_fasta_index = ref_fasta_index,
      ref_virus_fasta_bwa_fa = ref_virus_fasta_bwa_fa,
      ref_virus_fasta_bwa_amb = ref_virus_fasta_bwa_amb,
      ref_virus_fasta_bwa_ann = ref_virus_fasta_bwa_ann,
      ref_virus_fasta_bwa_bwt = ref_virus_fasta_bwa_bwt,
      ref_virus_fasta_bwa_pac = ref_virus_fasta_bwa_pac,
      ref_virus_fasta_bwa_sa = ref_virus_fasta_bwa_sa
  }

  output {
    File output_bam = Virus_hhv6_recon.output_bam
    File output_log = Virus_hhv6_recon.output_log
    File output_bedgraph = Virus_hhv6_recon.output_bedgraph
    File output_markduplicate_metrics = Virus_hhv6_recon.output_markduplicate_metrics
    File output_summary = Virus_hhv6_recon.output_summary
  }
}

task Virus_hhv6_recon {
  input {
    String sample_name

    File input_cram
    File input_cram_index

    File ref_fasta
    File ref_fasta_index

    File? ref_virus_fasta_bwa_fa
    File? ref_virus_fasta_bwa_amb
    File? ref_virus_fasta_bwa_ann
    File? ref_virus_fasta_bwa_bwt
    File? ref_virus_fasta_bwa_pac
    File? ref_virus_fasta_bwa_sa

    Int machine_mem_gb = 4
    Int additional_disk_size = 10
  }
  Int disk_size = ceil(size(input_cram, "GB") + additional_disk_size)

  command <<<

HHV6_recon \
  -alignmentin \
  -c ~{input_cram} \
  -fa ~{ref_fasta} \
  -p 4 ~{"-bwa -vref " + ref_virus_fasta_bwa_fa + " -vrefindex " + ref_virus_fasta_bwa_fa} \
  -outdir ~{sample_name}

mv ~{sample_name}/mapped_to_virus_dedup.bam ~{sample_name}_mapped_to_virus_dedup.bam
mv ~{sample_name}/for_debug.log ~{sample_name}_for_debug.log
mv ~{sample_name}/mapped_to_virus.bedgraph ~{sample_name}_mapped_to_virus.bedgraph
mv ~{sample_name}/mark_duplicate_metrix.txt ~{sample_name}_mark_duplicate_metrix.txt
mv ~{sample_name}/virus_detection_summary.txt ~{sample_name}_virus_detection_summary.txt

grep "virus_exist=True" ~{sample_name}_virus_detection_summary.txt > ~{sample_name}_virus_detection_summary_exist.txt

  >>>

  runtime {
    cpu: 4
    docker: "shoheikojima/integrated_hhv6_recon:v201202"
    preemptible: 3
    memory: machine_mem_gb + " GB"
    disks: "local-disk " + disk_size + " HDD"
  }

  output {
    File output_bam = "~{sample_name}_mapped_to_virus_dedup.bam"
    File output_log = "~{sample_name}_for_debug.log"
    File output_bedgraph = "~{sample_name}_mapped_to_virus.bedgraph"
    File output_markduplicate_metrics = "~{sample_name}_mark_duplicate_metrix.txt"
    File output_summary = "~{sample_name}_virus_detection_summary.txt"
    File output_summary_exist = "~{sample_name}_virus_detection_summary_exist.txt"
  }
}
