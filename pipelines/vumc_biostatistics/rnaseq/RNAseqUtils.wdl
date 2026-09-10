version 1.0

struct StarReference {
  File Genome
  File SA
  File SAindex
  File chrLength_txt
  File chrName_txt
  File chrNameLength_txt
  File chrStart_txt
  File exonGeTrInfo_tab
  File exonInfo_tab
  File geneInfo_tab
  File genomeParameters_txt
  File sjdbInfo_txt
  File sjdbList_fromGTF_out_tab
  File sjdbList_out_tab
  File transcriptInfo_tab
}

task FeatureCounts {
  input {
    String featureCounts_option = "-g gene_id -t exon -p --countReadPairs"
    File bam
    File? bam_index
    File gtf
    String sample_name
    Int threads = 8
  }
  Int disk_size_gb = ceil(size([bam, gtf], "GB")) + 4
  command <<<

set -euo pipefail

featureCounts -v

featureCounts ~{featureCounts_option} \
  -T ~{threads} \
  -a ~{gtf} \
  -o ~{sample_name}.count \
  ~{bam}

gzip ~{sample_name}.count
mv ~{sample_name}.count.summary ~{sample_name}.count.summary.txt

  >>>
  runtime {
    docker: "shengqh/cqs_rnaseq:20240813"
    memory: 40 + " GiB"
    disks: "local-disk " + disk_size_gb + " HDD"
    cpu: threads
    preemptible: 3
  }
  output {
    File output_count = "~{sample_name}.count.gz"
    File output_count_summary = "~{sample_name}.count.summary.txt"
  }
}

task STARForCount {
  input {
    String sample_name

    File left_fq
    File right_fq

    StarReference reference

    String star_option = "--twopassMode Basic --outSAMmapqUnique 60 --outSAMprimaryFlag AllBestScore"
    
    # runtime params
    String docker = "trinityctat/starfusion:1.15.1"
    Int preemptible = 3
    String star_path = "/usr/local/bin/STAR"
    Int cpu = 8
    Float fastq_disk_space_multiplier = 3.25
    Int memory_gb = 50
    Float extra_disk_space = 10
    Boolean use_ssd = true
  }
  
  Int disk_size_gb = ceil(size([reference.Genome, reference.SA, reference.SAindex], "GB") + size([left_fq, right_fq], "GB") * fastq_disk_space_multiplier + extra_disk_space)

  command <<<

set -ex
shopt -s nullglob

star_index_folder_name=$(dirname ~{reference.Genome})
echo "star_index_folder_name: $star_index_folder_name"

~{star_path} --version

~{star_path} ~{star_option} \
  --outSAMattrRGline ID:~{sample_name} SM:~{sample_name} LB:~{sample_name} PL:ILLUMINA PU:ILLUMINA \
  --runThreadN ~{cpu} \
  --genomeDir `pwd`/genome_dir/ctat_genome_lib_build_dir/ref_genome.fa.star.idx \
  --readFilesIn ~{left_fq} ~{right_fq} \
  --readFilesCommand "gunzip -c" \
  --outFileNamePrefix ~{sample_name}_ \
  --outSAMtype BAM Unsorted

  >>>

  runtime {
    docker: docker
    memory: memory_gb + " GiB"
    disks: "local-disk " + disk_size_gb + " " + (if use_ssd then "SSD" else "HDD")
    cpu: cpu
    preemptible: preemptible
  }

  output {
    File output_bam = "~{sample_name}_Aligned.out.bam"
    File output_star_summary = "~{sample_name}_Log.final.out"  
  }
}

# Modified from https://github.com/STAR-Fusion/STAR-Fusion/blob/master/WDL/star_fusion_workflow.wdl
task star_fusion {
  input {
    String sample_name

    File left_fq
    File right_fq

    File genome
    Float uncompressed_genome_size_gb
    
    String? fusion_inspector
    Boolean examine_coding_effect
    Boolean coord_sort_bam
    Float min_FFPM

    Int preemptible
    String docker
    Int cpu
    String memory
    Float extra_disk_space
    Float fastq_disk_space_multiplier
    Boolean use_ssd
  }

  command <<<

    set -ex
    shopt -s nullglob

    mkdir -p ~{sample_name}

    mkdir -p genome_dir

    tar xf ~{genome} -C genome_dir --strip-components 1

    # delete the genome to save space
    rm -f ~{genome}

    /usr/local/src/STAR-Fusion/STAR-Fusion \
      --genome_lib_dir `pwd`/genome_dir/ctat_genome_lib_build_dir \
      --left_fq ~{left_fq} \
      --right_fq ~{right_fq} \
      --output_dir ~{sample_name} \
      --CPU ~{cpu} \
      ~{"--FusionInspector " + fusion_inspector} \
      ~{true='--examine_coding_effect' false='' examine_coding_effect} \
      ~{"--min_FFPM " + min_FFPM}
    
    # rename outputs to include the sample ID
    mv ~{sample_name}/star-fusion.fusion_predictions.tsv ~{sample_name}.star-fusion.fusion_predictions.tsv && gzip ~{sample_name}.star-fusion.fusion_predictions.tsv
    mv ~{sample_name}/star-fusion.fusion_predictions.abridged.tsv ~{sample_name}.star-fusion.fusion_predictions.abridged.tsv && gzip ~{sample_name}.star-fusion.fusion_predictions.abridged.tsv
    mv ~{sample_name}/Chimeric.out.junction ~{sample_name}.Chimeric.out.junction && gzip ~{sample_name}.Chimeric.out.junction
    mv ~{sample_name}/SJ.out.tab ~{sample_name}.SJ.out.tab && gzip ~{sample_name}.SJ.out.tab 
    mv ~{sample_name}/Log.final.out ~{sample_name}.Log.final.out

    gzip -c ~{sample_name}/star-fusion.preliminary/star-fusion.fusion_candidates.preliminary > ~{sample_name}.star-fusion.fusion_candidates.preliminary.tsv.gz

  >>>

  output {
    
    File fusion_predictions = "~{sample_name}.star-fusion.fusion_predictions.tsv.gz"
    File fusion_predictions_abridged = "~{sample_name}.star-fusion.fusion_predictions.abridged.tsv.gz"

    File preliminary_fusion_predictions = "~{sample_name}.star-fusion.fusion_candidates.preliminary.tsv.gz"

    File junction = "~{sample_name}.Chimeric.out.junction.gz"
    File sj = "~{sample_name}.SJ.out.tab.gz"

    File? coding_effect = "~{sample_name}/star-fusion.fusion_predictions.abridged.coding_effect.tsv"
    
    Array[File] extract_fusion_reads = glob("~{sample_name}/star-fusion.fusion_evidence_*.fq")

    File star_log_final = "~{sample_name}.Log.final.out"
    
    File? fusion_inspector_validate_fusions_abridged = "~{sample_name}/FusionInspector-validate/finspector.FusionInspector.fusions.abridged.tsv"
    File? fusion_inspector_validate_web = "~{sample_name}/FusionInspector-validate/finspector.fusion_inspector_web.html"

    File? fusion_inspector_inspect_fusions_abridged = "~{sample_name}/FusionInspector-inspect/finspector.FusionInspector.fusions.abridged.tsv"
    File? fusion_inspector_inspect_web = "~{sample_name}/FusionInspector-inspect/finspector.fusion_inspector_web.html"
  }

  runtime {
    preemptible: preemptible
    disks: "local-disk " + ceil((fastq_disk_space_multiplier * (size(left_fq, "GB") + size(right_fq, "GB"))) + size(genome, "GB") + uncompressed_genome_size_gb + extra_disk_space) + " " + (if use_ssd then "SSD" else "HDD")
    docker: docker
    cpu: cpu
    memory: memory
  }
}