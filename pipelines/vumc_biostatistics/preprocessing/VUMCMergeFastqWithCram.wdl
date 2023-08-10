version 1.0

## Copyright Broad Institute, 2018
##
## This WDL defines tasks used for alignment of human whole-genome or exome sequencing data.
##
## Runtime parameters are often optimized for Broad's Google Cloud Platform implementation.
## For program versions, see docker containers.
##
## LICENSING :
## This script is released under the WDL source code license (BSD-3) (see LICENSE in
## https://github.com/broadinstitute/wdl). Note however that the programs it calls may
## be subject to different licenses. Users are responsible for checking that they are
## authorized to run all programs before running this script. Please see the docker
## page at https://hub.docker.com/r/broadinstitute/genomes-in-the-cloud/ for detailed
## licensing information pertaining to the included programs.

import "../../../tasks/broad/Utilities.wdl" as Utilities

workflow VUMCMergeFastqWithCram {
  input {
    File fastq_1
    File fastq_2

    String sample_name

    String readgroup_name
    String library_name
    String platform_unit
    String run_date
    String platform_name
    String sequencing_center

    String ref_fasta
    String ref_fasta_index
    String ref_dict

    File mapped_cram
    File mapped_cram_index

    Boolean hard_clip_reads = false
    Boolean unmap_contaminant_reads = true
    Boolean allow_empty_ref_alt = false
  }

  call PairedFastQsToUnmappedBAM as FastqToUnmappedBam {
    input:
      fastq_1 = fastq_1,
      fastq_2 = fastq_2,
      sample_name = sample_name,
      readgroup_name = readgroup_name,
      library_name = library_name,
      platform_unit = platform_unit,
      run_date = run_date,
      platform_name = platform_name,
      sequencing_center = sequencing_center,
  }

  call Utilities.ConvertToBam as CramToBam {
    input:
      input_cram = mapped_cram,
      ref_fasta = ref_fasta,
      ref_fasta_index = ref_fasta_index,
      output_basename = sample_name
  }

  call MergeBamAlignment {
    input:
      input_bam = CramToBam.output_bam,
      input_bam_index = CramToBam.output_bam_index,

      unmapped_bam = FastqToUnmappedBam.output_unmapped_bam,

      sample_name = sample_name,

      ref_fasta = ref_fasta,
      ref_fasta_index = ref_fasta_index,
      ref_dict = ref_dict
  }

  call Utilities.ConvertToCram as MergedBamToCram {
    input:
      input_bam = MergeBamAlignment.output_bam,
      ref_fasta = ref_fasta,
      ref_fasta_index = ref_fasta_index,
      output_basename = sample_name
  }

  output {
    File output_cram = MergedBamToCram.output_cram
    File output_cram_index = MergedBamToCram.output_cram_index
    File output_cram_md5 = MergedBamToCram.output_cram_md5
  }
}

# Convert a pair of FASTQs to uBAM
task PairedFastQsToUnmappedBAM {
  input {
    # Command parameters
    String sample_name
    File fastq_1
    File fastq_2
    String readgroup_name
    String library_name
    String platform_unit
    String run_date
    String platform_name
    String sequencing_center

    # Runtime parameters
    Int addtional_disk_space_gb = 10
    Int machine_mem_gb = 7
    Int preemptible_attempts = 3
    String gatk_docker = "us.gcr.io/broad-gatk/gatk:4.4.0.0"
  }

  Int command_mem_gb = machine_mem_gb - 1
  Int disk_space_gb = ceil((size(fastq_1, "GB") + size(fastq_2, "GB")) * 2 ) + addtional_disk_space_gb

  command <<<
    gatk --java-options "-Xms~{command_mem_gb}g -Xmx~{command_mem_gb}g" \
    FastqToSam \
    --FASTQ ~{fastq_1} \
    --FASTQ2 ~{fastq_2} \
    --OUTPUT ~{readgroup_name}.unmapped.bam \
    --READ_GROUP_NAME ~{readgroup_name} \
    --SAMPLE_NAME ~{sample_name} \
    --LIBRARY_NAME ~{library_name} \
    --PLATFORM_UNIT ~{platform_unit} \
    --RUN_DATE ~{run_date} \
    --PLATFORM ~{platform_name} \
    --SEQUENCING_CENTER ~{sequencing_center} 
  >>>
  
  runtime {
    docker: gatk_docker
    memory: machine_mem_gb + " GB"
    disks: "local-disk " + disk_space_gb + " HDD"
    preemptible: preemptible_attempts
  }
  output {
    File output_unmapped_bam = "~{readgroup_name}.unmapped.bam"
  }
}

task MergeBamAlignment {
  input {
    File input_bam
    File input_bam_index

    File unmapped_bam

    String sample_name

    String ref_fasta
    String ref_fasta_index
    String ref_dict

    Boolean hard_clip_reads = false
    Boolean unmap_contaminant_reads = true

    Float memory_multiplier = 1.0
    Float disk_multiplier = 2.5
    Int preemptible_tries = 3
  }

  Float unmapped_bam_size = size(unmapped_bam, "GiB")
  Float input_bam_size = size(input_bam, "GiB")
  Float ref_size = size(ref_fasta, "GiB") + size(ref_fasta_index, "GiB") + size(ref_dict, "GiB")

  Int disk_size = ceil(ref_size + input_bam_size + disk_multiplier * unmapped_bam_size + 20)

  Int memory_size_gb = ceil(14 * memory_multiplier)

  command <<<
    set -o pipefail
    set -e

    # copied the following code from HaplotypeCaller_GATK4_VCF in GermlineVariantDiscovery
    # Quanhu Sheng, 20230809 
    #
    # We need at least 1 GB of available memory outside of the Java heap in order to execute native code, thus, limit
    # Java's memory by the total memory minus 1 GB. We need to compute the total memory as it might differ from
    # memory_size because of Cromwell's retry with more memory feature.
    # Note: In the future this should be done using Cromwell's ${MEM_SIZE} and ${MEM_UNIT} environment variables,
    #       which do not rely on the output format of the `free` command.
    available_memory_mb=$(free -m | awk '/^Mem/ {print $2}')
    let java_memory_size_mb=available_memory_mb-1024
    echo Total available memory: ${available_memory_mb} MB >&2
    echo Memory reserved for Java: ${java_memory_size_mb} MB >&2

    gatk --java-options "-Xms${java_memory_size_mb}m -Xmx${java_memory_size_mb}m -XX:GCTimeLimit=50 -XX:GCHeapFreeLimit=10" \
      MergeBamAlignment \
      VALIDATION_STRINGENCY=SILENT \
      EXPECTED_ORIENTATIONS=FR \
      ATTRIBUTES_TO_RETAIN=X0 \
      ATTRIBUTES_TO_REMOVE=NM \
      ATTRIBUTES_TO_REMOVE=MD \
      ALIGNED_BAM=~{input_bam} \
      UNMAPPED_BAM=~{unmapped_bam} \
      OUTPUT=~{sample_name}.bam \
      REFERENCE_SEQUENCE=~{ref_fasta} \
      SORT_ORDER="coordinate" \
      IS_BISULFITE_SEQUENCE=false \
      ALIGNED_READS_ONLY=false \
      CLIP_ADAPTERS=false \
      ~{true='CLIP_OVERLAPPING_READS=true' false="" hard_clip_reads} \
      ~{true='CLIP_OVERLAPPING_READS_OPERATOR=H' false="" hard_clip_reads} \
      MAX_RECORDS_IN_RAM=2000000 \
      ADD_MATE_CIGAR=true \
      MAX_INSERTIONS_OR_DELETIONS=-1 \
      PRIMARY_ALIGNMENT_STRATEGY=MostDistant \
      UNMAPPED_READ_STRATEGY=COPY_TO_TAG \
      ALIGNER_PROPER_PAIR_FLAGS=true \
      UNMAP_CONTAMINANT_READS=~{unmap_contaminant_reads} \
      ADD_PG_TAG_TO_READS=false

  >>>
  runtime {
    docker: "us.gcr.io/broad-gotc-prod/samtools-picard-bwa:1.0.2-0.7.15-2.26.10-1643840748"
    preemptible: preemptible_tries
    memory: memory_size_gb + " GiB"
    cpu: "16"
    disks: "local-disk " + disk_size + " HDD"
  }
  output {
    File output_bam = "~{sample_name}.bam"
  }
}
