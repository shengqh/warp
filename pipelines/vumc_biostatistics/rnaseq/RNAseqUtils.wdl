version 1.0

task STAR_Unsorted {
  input {
    File? fastq_pair_tar_gz
    File? left_fq
    File? right_fq

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

    Int memory_gb = 50
    Float disk_size_factor = 3.25
    Int additional_disk_size_gb = 10
    Int threads = 8
  }
  Int disk_size_gb = ceil(size([Genome, SA, SAindex], "GB") + size([fastq_pair_tar_gz, left_fq, right_fq], "GB") * disk_size_factor + additional_disk_size_gb)

  command <<<

set -euo pipefail

if [[ ! -z "~{fastq_pair_tar_gz}" ]]; then
  # untar the fq pair
  mv ~{fastq_pair_tar_gz} reads.tar.gz
  tar xzvf reads.tar.gz
  rm reads.tar.gz

  # left reads
  if compgen -G "*_1.fastq*" > /dev/null; then
    left_fq=(*_1.fastq*)
  elif compgen -G "*_1.fq*" > /dev/null; then
    left_fq=(*_1.fq*)
  fi

  # right reads (_2 preferred, fallback to _3)
  if compgen -G "*_2.fastq*" > /dev/null; then
    right_fq=(*_2.fastq*)
  elif compgen -G "*_2.fq*" > /dev/null; then
    right_fq=(*_2.fq*)
  elif compgen -G "*_3.fastq*" > /dev/null; then
    right_fq=(*_3.fastq*)
  elif compgen -G "*_3.fq*" > /dev/null; then
    right_fq=(*_3.fq*)
  fi
else
  left_fq="~{left_fq}"
  right_fq="~{right_fq}"
fi

# sanity check
if [[ -z "${left_fq[0]:-}" || -z "${right_fq[0]:-}" ]]; then
  echo "Error: fastq files not found"
  ls -ltr
  exit 1
fi

echo "left_fq:  ${left_fq[@]}"
echo "right_fq: ${right_fq[@]}"

left_fqs=$(IFS=, ; echo "${left_fq[*]}")

read_params="--readFilesIn ${left_fqs}"
if [[ "${right_fq[0]}" != "" ]]; then
  right_fqs=$(IFS=, ; echo "${right_fq[*]}")   
  read_params="${read_params} ${right_fqs}"
fi

echo "read_params: ${read_params}"

star_index_folder_name=$(dirname ~{Genome})
echo "star_index_folder_name: $star_index_folder_name"

STAR --version

STAR ~{star_option} \
  --outSAMattrRGline ID:~{sample_name} SM:~{sample_name} LB:~{sample_name} PL:ILLUMINA PU:ILLUMINA \
  --runThreadN ~{threads} \
  --genomeDir $star_index_folder_name \
  ${read_params} \
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
    File? bam_index
    File gtf
    String sample_name
    Int threads = 8
  }
  Int disk_size_gb = ceil(size([bam, gtf], "GB")) + 4
  command <<<

set -euo pipefail

featureCounts --version

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
# Since the STAR-Fusion bam file cannot be used in FeatureCounts directly, we will not output the bam file here.

task STARFusion {
  input {
    String sample_name

    File? fastq_pair_tar_gz
    File? left_fq
    File? right_fq

    File genome_plug_n_play_tar_gz
    
    String star_fusion_option = ""
    
    String fusion_inspector = "validate" # inspect or validate

    # runtime params
    String docker = "trinityctat/starfusion:1.15.1"
    Int cpu = 12
    Float fastq_disk_space_multiplier = 3.25
    String memory_gb = "50G"
    Float genome_disk_space_multiplier = 2.5
    Int preemptible = 2
    Float extra_disk_space = 10
    Boolean use_ssd = true
  }
  
  Int disk_size_gb = ceil((fastq_disk_space_multiplier * (size(left_fq, "GB") + size(right_fq, "GB"))) + size(genome_plug_n_play_tar_gz, "GB") * genome_disk_space_multiplier + extra_disk_space)

  String finspect_tsv=if (fusion_inspector == "validate") then sample_name + "_finspector_validate.fusions.abridged.tsv.gz" else sample_name + "_finspector_inspect.fusions.abridged.tsv.gz"
  String finspect_html=if (fusion_inspector == "validate") then sample_name + "_finspector_validate.fusion_inspector_web.html" else sample_name + "_finspector_inspect.fusion_inspector_web.html"

  command <<<

set -ex
shopt -s nullglob

if [[ "~{fusion_inspector}" != "validate" && "~{fusion_inspector}" != "inspect" ]]; then
  echo "Error: fusion_inspector ~{fusion_inspector} is not valid. It should be either 'validate' or 'inspect'."
  exit 1
fi

if [[ ! -z "~{fastq_pair_tar_gz}" ]]; then
  # untar the fq pair
  mv ~{fastq_pair_tar_gz} reads.tar.gz
  tar xzvf reads.tar.gz
  rm reads.tar.gz

  # left reads
  if compgen -G "*_1.fastq*" > /dev/null; then
    left_fq=(*_1.fastq*)
  elif compgen -G "*_1.fq*" > /dev/null; then
    left_fq=(*_1.fq*)
  fi

  # right reads (_2 preferred, fallback to _3)
  if compgen -G "*_2.fastq*" > /dev/null; then
    right_fq=(*_2.fastq*)
  elif compgen -G "*_2.fq*" > /dev/null; then
    right_fq=(*_2.fq*)
  elif compgen -G "*_3.fastq*" > /dev/null; then
    right_fq=(*_3.fastq*)
  elif compgen -G "*_3.fq*" > /dev/null; then
    right_fq=(*_3.fq*)
  fi
else
  left_fq="~{left_fq}"
  right_fq="~{right_fq}"
fi

# sanity check
if [[ -z "${left_fq[0]:-}" || -z "${right_fq[0]:-}" ]]; then
  echo "Error: fastq files not found"
  ls -ltr
  exit 1
fi

echo "left_fq:  ${left_fq[@]}"
echo "right_fq: ${right_fq[@]}"

left_fqs=$(IFS=, ; echo "${left_fq[*]}")

read_params="--left_fq ${left_fqs}"
if [[ "${right_fq[0]}" != "" ]]; then
  right_fqs=$(IFS=, ; echo "${right_fq[*]}")   
  read_params="${read_params} --right_fq ${right_fqs}"
fi

echo "read_params: ${read_params}"

mkdir -p genome_dir

tar xzvf ~{genome_plug_n_play_tar_gz} -C genome_dir --strip-components 1

STAR --version

STAR-Fusion --version

STAR-Fusion ~{star_fusion_option} \
  --genome_lib_dir `pwd`/genome_dir/ctat_genome_lib_build_dir \
  ${read_params} \
  --output_dir . \
  --CPU ~{cpu} \
  --FusionInspector ~{fusion_inspector} \
  --examine_coding_effect \
  --denovo_reconstruct

# rename outputs to include the sample ID
mv star-fusion.fusion_predictions.abridged.coding_effect.tsv ~{sample_name}_star-fusion.fusion_predictions.abridged.coding_effect.tsv && gzip ~{sample_name}_star-fusion.fusion_predictions.abridged.coding_effect.tsv
mv star-fusion.fusion_predictions.abridged.tsv ~{sample_name}_star-fusion.fusion_predictions.abridged.tsv && gzip ~{sample_name}_star-fusion.fusion_predictions.abridged.tsv
mv star-fusion.fusion_predictions.tsv ~{sample_name}_star-fusion.fusion_predictions.tsv && gzip ~{sample_name}_star-fusion.fusion_predictions.tsv

if [[ -s FusionInspector-validate/finspector.FusionInspector.fusions.abridged.tsv ]]; then
  mv FusionInspector-validate/finspector.FusionInspector.fusions.abridged.tsv ~{sample_name}_finspector_validate.fusions.abridged.tsv && gzip ~{sample_name}_finspector_validate.fusions.abridged.tsv
  mv FusionInspector-validate/finspector.fusion_inspector_web.html ~{sample_name}_finspector_validate.fusion_inspector_web.html
fi

if [[ -s FusionInspector-inspect/finspector.FusionInspector.fusions.abridged.tsv ]]; then
  mv FusionInspector-inspect/finspector.FusionInspector.fusions.abridged.tsv ~{sample_name}_finspector_inspect.fusions.abridged.tsv && gzip ~{sample_name}_finspector_inspect.fusions.abridged.tsv
  mv FusionInspector-inspect/finspector.fusion_inspector_web.html ~{sample_name}_finspector_inspect.fusion_inspector_web.html
fi

mv star-fusion.Log.final.out ~{sample_name}_star-fusion.Log.final.out

rm -rf genome_dir

  >>>

  runtime {
    docker: docker
    memory: memory_gb + " GiB"
    disks: "local-disk " + disk_size_gb + " " + (if use_ssd then "SSD" else "HDD")
    cpu: cpu
    preemptible: 3
  }

  output {
    File fusion_coding_effect = "~{sample_name}_star-fusion.fusion_predictions.abridged.coding_effect.tsv.gz"
    File fusion_predictions_abridged = "~{sample_name}_star-fusion.fusion_predictions.abridged.tsv.gz"
    File fusion_predictions = "~{sample_name}_star-fusion.fusion_predictions.tsv.gz"
    File fusion_log_final = "~{sample_name}_star-fusion.Log.final.out"
    
    # Those file might not be generated if no fusions are found
    File? fusion_inspector_fusions_abridged = finspect_tsv
    File? fusion_inspector_web = finspect_html
  }
}
