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

# Modified based on https://github.com/STAR-Fusion/STAR-Fusion/blob/master/WDL/star_fusion_workflow.wdl

task STARFusion {
  input {
    String sample_name

    File left_fq
    File right_fq

    File genome_plug_n_play_tar_gz
    
    String star_fusion_option = ""
    
    Float min_FFPM = 0.1

    # runtime params
    String docker = "trinityctat/starfusion:latest"
    Int cpu = 12
    Float fastq_disk_space_multiplier = 3.25
    String memory_gb = "50G"
    Float genome_disk_space_multiplier = 2.5
    Int preemptible = 2
    Float extra_disk_space = 10
    Boolean use_ssd = true
  }
  
  Int disk_size_gb = ceil((fastq_disk_space_multiplier * (size(left_fq, "GB") + size(right_fq, "GB"))) + size(genome_plug_n_play_tar_gz, "GB") * genome_disk_space_multiplier + extra_disk_space)

  command <<<

set -euo pipefail

mkdir -p ~{sample_name}

mkdir -p genome_dir

tar xf ~{genome_plug_n_play_tar_gz} -C genome_dir --strip-components 1

STAR-Fusion ~{star_fusion_option} \
  --genome_lib_dir `pwd`/genome_dir/ctat_genome_lib_build_dir \
  --left_fq ~{left_fq} \
  --right_fq ~{right_fq} \
  --output_dir ~{sample_name} \
  --CPU ~{cpu} \
  --FusionInspector inspect \
  --min_FFPM ~{min_FFPM}

# rename outputs to include the sample ID
mv ~{sample_name}/star-fusion.fusion_predictions.abridged.coding_effect.tsv ~{sample_name}_star-fusion.fusion_predictions.abridged.coding_effect.tsv && gzip ~{sample_name}_star-fusion.fusion_predictions.abridged.coding_effect.tsv
mv ~{sample_name}/star-fusion.fusion_predictions.abridged.tsv ~{sample_name}.star-fusion.fusion_predictions.abridged.tsv && gzip ~{sample_name}.star-fusion.fusion_predictions.abridged.tsv
mv ~{sample_name}/star-fusion.fusion_predictions.tsv ~{sample_name}.star-fusion.fusion_predictions.tsv && gzip ~{sample_name}.star-fusion.fusion_predictions.tsv

mv ~{sample_name}/FusionInspector-inspect/finspector.FusionInspector.fusions.tsv ~{sample_name}_finspector.FusionInspector.fusions.tsv && gzip ~{sample_name}_finspector.FusionInspector.fusions.tsv
mv ~{sample_name}/FusionInspector-inspect/finspector.fusion_inspector_web.html ~{sample_name}_finspector.fusion_inspector_web.html

mv ~{sample_name}/Log.final.out ~{sample_name}_star-fusion.Log.final.out
mv ~{sample_name}/Aligned.out.bam ~{sample_name}.STAR.aligned.UNsorted.bam

  >>>

  runtime {
    docker: docker
    memory: memory_gb + " GiB"
    disks: "local-disk " + disk_size_gb + (if use_ssd then "SSD" else "HDD")
    cpu: cpu
    preemptible: 3
  }

  output {
    File fusion_coding_effect = "~{sample_name}_star-fusion.fusion_predictions.abridged.coding_effect.tsv.gz"
    File fusion_predictions_abridged = "~{sample_name}_star-fusion.fusion_predictions.abridged.tsv.gz"
    File fusion_predictions = "~{sample_name}_star-fusion.fusion_predictions.tsv.gz"
    File fusion_inspector_fusions = "~{sample_name}_finspector.FusionInspector.fusions.tsv.gz"
    File fusion_inspector_web = "~{sample_name}_finspector.fusion_inspector_web.html"
    File fusion_log_final = "~{sample_name}_star-fusion.Log.final.out"
    File fusion_unsorted_bam = "~{sample_name}.STAR.aligned.UNsorted.bam"
  }
}
