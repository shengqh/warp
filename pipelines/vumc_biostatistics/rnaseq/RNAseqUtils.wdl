version 1.0

task STAR_Unsorted {
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

    Int memory_gb = 40
    Float disk_size_factor = 2.5
    Int additional_disk_size_gb = 10
    Int threads = 8
  }
  Int disk_size_gb = ceil(size([Genome, SA, SAindex], "GB") + size([fastq_1, fastq_2], "GB") * disk_size_factor + additional_disk_size_gb)

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
  --outSAMtype BAM Unsorted

  >>>

  runtime {
    docker: "shengqh/cqs_rnaseq:20240813"
    memory: memory_gb + " GiB"
    disks: "local-disk " + disk_size_gb + " HDD"
    cpu: threads
    preemptible: 3
  }

  output {
    File output_bam = "~{sample_name}_Aligned.out.bam"
    File output_star_summary = "~{sample_name}_Log.final.out"
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
  Int disk_size_gb = ceil(size([bam, gtf], "GB")) + 4
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
    preemptible: 3
  }
  output {
    File output_count = "~{sample_name}.count"
    File output_count_summary = "~{sample_name}.count.summary"
  }
}

task STARFusion {
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

    Int memory_gb = 40
    Float disk_size_factor = 2.5
    Int additional_disk_size_gb = 10
    Int threads = 8
  }
  Int disk_size_gb = ceil(size([Genome, SA, SAindex], "GB") + size([fastq_1, fastq_2], "GB") * disk_size_factor + additional_disk_size_gb)

  command <<<

set -euo pipefail

star_index_folder_name=$(dirname ~{Genome})
echo "star_index_folder_name: $star_index_folder_name"

STAR-Fusion --genome_lib_dir $star_index_folder_name \
  --left_fq ~{fastq_1} \
  --right_fq ~{fastq_2} \
  --output_dir . \
  --CPU ~{threads} \
  --STAR_limitBAMsortRAM ~{memory_gb}G \
  --FusionInspector validate \
  --examine_coding_effect \
  --STAR_SortedByCoordinate \
  --denovo_reconstruct

mv star-fusion.fusion_predictions.abridged.coding_effect.tsv ~{sample_name}_star-fusion.fusion_predictions.abridged.coding_effect.tsv
mv star-fusion.fusion_predictions.abridged.tsv ~{sample_name}_star-fusion.fusion_predictions.abridged.tsv
mv star-fusion.fusion_predictions.tsv ~{sample_name}_star-fusion.fusion_predictions.tsv
mv FusionInspector-validate/finspector.fusion_inspector_web.html ~{sample_name}_finspector.fusion_inspector_web.html
mv FusionInspector-validate/finspector.FusionInspector.fusions.tsv ~{sample_name}_finspector.FusionInspector.fusions.tsv

  >>>

  runtime {
    docker: "trinityctat/starfusion:1.10.0"
    memory: memory_gb + " GiB"
    disks: "local-disk " + disk_size_gb + " HDD"
    cpu: threads
    preemptible: 3
  }

  output {
    File output_fusion_predictions_abridged_coding_effect = "~{sample_name}_star-fusion.fusion_predictions.abridged.coding_effect.tsv"
    File output_fusion_predictions_abridged = "~{sample_name}_star-fusion.fusion_predictions.abridged.tsv"
    File output_fusion_predictions = "~{sample_name}_star-fusion.fusion_predictions.tsv"
    File output_fusion_inspector_web = "~{sample_name}_finspector.fusion_inspector_web.html"
    File output_fusion_inspector_fusions = "~{sample_name}_finspector.FusionInspector.fusions.tsv"
  }
}
