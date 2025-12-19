version 1.0

## VUMC STAR-Fusion Workflow
##
## This workflow processes RNA-Seq data to detect gene fusions using STAR-Fusion.
## Developed by VUMC/VANGARD team for comprehensive RNA-Seq fusion detection analysis.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
## 
## ### Workflow Purpose:
## This pipeline processes RNA-Seq data from FASTQ files to detect gene fusions,
## enabling identification of fusion transcripts in cancer and other disease research.
##
## ### Workflow Steps:
## 1. STAR Alignment: Aligns paired-end RNA-Seq reads and generates chimeric junction outputs
## 2. Optional GCP Transfer: Copies chimeric junction output to specified GCP folder
##
## ### Inputs:
## - sample_name: Unique identifier for the sample
## - left_fq: Left/R1 FASTQ file (optional if using tar.gz)
## - right_fq: Right/R2 FASTQ file (optional if using tar.gz)
## - fastq_pair_tar_gz: Compressed archive of paired FASTQ files (alternative input)
## - genome_plug_n_play_tar_gz: Pre-built STAR genome reference package
## - target_gcp_folder: Optional GCP destination folder for outputs
##
## ### Outputs:
## - fusion_chimeric_out_junction: STAR chimeric junction file for downstream fusion detection
##
## ### Notes:
## - Accepts either individual FASTQ files or tar.gz archive as input
## - Generates chimeric junction outputs suitable for STAR-Fusion or other fusion callers
## - GCP file transfer is conditional based on target_gcp_folder parameter

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCStarForFusion {
  input {
    String sample_name

    # input data options
    File? left_fq
    File? right_fq
    File? fastq_pair_tar_gz

    File genome_plug_n_play_tar_gz

    String? target_gcp_folder
  }

  call STARForFusion {
    input:
      fastq_pair_tar_gz = fastq_pair_tar_gz,
      left_fq = left_fq,
      right_fq = right_fq,
      genome_plug_n_play_tar_gz = genome_plug_n_play_tar_gz,
      sample_name = sample_name
  }

  if (defined(target_gcp_folder)) {
    String gcs_output_dir = sub(select_first([target_gcp_folder]), "/+$", "") + "/" + sample_name + "/"
    call GcpUtils.MoveOrCopyFiles as CopyFile {
      input:
        source_file1 = STARForFusion.fusion_chimeric_out_junction,
        is_move_file = false,
        target_gcp_folder = gcs_output_dir
    }
  }
  # Outputs that will be retained when execution is complete
  output {
    File fusion_chimeric_out_junction = select_first([CopyFile.output_file1, STARForFusion.fusion_chimeric_out_junction])
  }
}

task STARForFusion {
  input {
    String sample_name

    File? fastq_pair_tar_gz
    File? left_fq
    File? right_fq

    File genome_plug_n_play_tar_gz
    
    # runtime params
    String docker = "trinityctat/starfusion:1.15.1"
    Int cpu = 12
    Float fastq_disk_space_multiplier = 3.25
    Int memory_gb = 50
    Float genome_disk_space_multiplier = 2.5
    Int preemptible = 2
    Float extra_disk_space = 10
    Boolean use_ssd = true
  }
  
  Int disk_size_gb = ceil((fastq_disk_space_multiplier * (size(left_fq, "GB") + size(right_fq, "GB"))) + size(genome_plug_n_play_tar_gz, "GB") * genome_disk_space_multiplier + extra_disk_space)

  command <<<

set -ex
shopt -s nullglob

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

read_params="${left_fqs}"
if [[ "${right_fq[0]}" != "" ]]; then
  right_fqs=$(IFS=, ; echo "${right_fq[*]}")   
  read_params="${read_params} ${right_fqs}"
fi

echo "read_params: ${read_params}"

mkdir -p genome_dir

tar xzvf ~{genome_plug_n_play_tar_gz} -C genome_dir --strip-components 1

/usr/local/bin/STAR --version

/usr/local/bin/STAR --genomeDir `pwd`/genome_dir/ctat_genome_lib_build_dir/ref_genome.fa.star.idx \
  --outReadsUnmapped None \
  --chimSegmentMin 12 \
  --chimJunctionOverhangMin 8 \
  --chimOutJunctionFormat 1 \
  --alignSJDBoverhangMin 10 \
  --alignMatesGapMax 100000 \
  --alignIntronMax 100000 \
  --alignSJstitchMismatchNmax 5 -1 5 5 \
  --runThreadN ~{cpu} \
  --outSAMstrandField intronMotif \
  --outSAMunmapped Within \
  --alignInsertionFlush Right \
  --alignSplicedMateMapLminOverLmate 0 \
  --alignSplicedMateMapLmin 30 \
  --outSAMtype BAM Unsorted \
  --readFilesIn ${read_params} \
  --outSAMattrRGline ID:GRPundef \
  --chimMultimapScoreRange 3 \
  --chimScoreJunctionNonGTAG -4 \
  --chimMultimapNmax 20 \
  --chimOutType Junctions WithinBAM \
  --chimNonchimScoreDropMin 10 \
  --peOverlapNbasesMin 12 \
  --peOverlapMMp 0.1 \
  --genomeLoad NoSharedMemory \
  --twopassMode None \
  --readFilesCommand "gunzip -c" \
  --quantMode GeneCounts

mv Chimeric.out.junction ~{sample_name}_Chimeric.out.junction && gzip ~{sample_name}_Chimeric.out.junction

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
    File fusion_chimeric_out_junction = "~{sample_name}_Chimeric.out.junction.gz"
  }
}
