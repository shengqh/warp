version 1.0

task STAR {
  input {
    File fastq_1
    File fastq_2
    String sample_name

    String star_option = "--twopassMode Basic --outSAMmapqUnique 60 --outSAMprimaryFlag AllBestScore"
    
    File chrLength_txt
    File chrNameLength_txt
    File chrName_txt
    File chrStart_txt
    File exonGeTrInfo_tab
    File exonInfo_tab
    File geneInfo_tab
    File Genome
    File genomeParameters_txt
    File SA
    File SAindex
    File sjdbInfo_txt
    File sjdbList_fromGTF_out_tab
    File sjdbList_out_tab
    File transcriptInfo_tab

    Int threads = 8
  }
  command <<<

set -euo pipefail

star_index_folder_name=$(dirname ~{Genome})
echo "star_index_folder_name: $star_index_folder_name"

STAR ~{star_option} \
  --outSAMattrRGline ID:~{sample_name} SM:~{sample_name} LB:~{sample_name} PL:ILLUMINA PU:ILLUMINA \
  --runThreadN ~{threads} \
  --genomeDir $star_index_folder_name \
  --readFilesIn ~{fastq_1} ~{fastq_2} \
  --readFilesCommand zcat \
  --outFileNamePrefix ~{sample_name}_ \
  --outSAMtype BAM SortedByCoordinate

samtools index ~{sample_name}_Aligned.sortedByCoord.out.bam
samtools idxstats ~{sample_name}_Aligned.sortedByCoord.out.bam > ~{sample_name}_Aligned.sortedByCoord.out.bam.chromosome.count

  >>>

  runtime {
    docker: "shengqh/cqs_rnaseq:20240813"
    memory: 40 + " GiB"
    disks: "local-disk " + 50 + " HDD"
    cpu: threads
    preemptible: 1
  }

  output {
    File output_bam = "~{sample_name}_Aligned.sortedByCoord.out.bam"
    File output_bam_index = "~{sample_name}_Aligned.sortedByCoord.out.bam.bai"
    File output_star_summary = "~{sample_name}_Log.final.out"
    File output_star_chromosome_count = "~{sample_name}_Aligned.sortedByCoord.out.bam.chromosome.count"
  }
}

task FeatureCounts {
  input {
    String featureCounts_option = "-g gene_id -t exon -p --countReadPairs"
    File bam
    File bam_index
    File gtf
    String sample_name
    Int threads = 8
  }
  Int disk_size_gb = ceil(size(bam, "GB")) + 4
  command <<<

set -euo pipefail

featureCounts ~{featureCounts_option} \
  -g gene_id \
  -t exon \
  -p \
  --countReadPairs \
  -T ~{threads} \
  -a ~{gtf} \
  -o ~{sample_name}.count \
  ~{bam}

  >>>
  runtime {
    docker: "shengqh/cqs_rnaseq:20240813"
    memory: 40 + " GiB"
    disks: "local-disk " + disk_size_gb + " HDD"
    cpu: threads
    preemptible: 1
  }
  output {
    File output_count = "~{sample_name}.count"
    File output_count_summary = "~{sample_name}.count.summary"
  }
}
