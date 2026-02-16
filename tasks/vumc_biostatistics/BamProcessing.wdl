version 1.0

# Mark duplicate reads to avoid counting non-independent observations
task MarkDuplicates {
  input {
    Array[File] input_bams
    String output_bam_basename
    String metrics_filename
    Float total_input_size
    Int compression_level
    Int preemptible_tries

    # The program default for READ_NAME_REGEX is appropriate in nearly every case.
    # Sometimes we wish to supply "null" in order to turn off optical duplicate detection
    # This can be desirable if you don't mind the estimated library size being wrong and optical duplicate detection is taking >7 days and failing
    String? read_name_regex
    Int memory_multiplier = 1
    Int additional_disk = 20

    Float? sorting_collection_size_ratio
  }

  # The merged bam will be smaller than the sum of the parts so we need to account for the unmerged inputs and the merged output.
  # Mark Duplicates takes in as input readgroup bams and outputs a slightly smaller aggregated bam. Giving .25 as wiggleroom
  Float md_disk_multiplier = 3
  Int disk_size = ceil(md_disk_multiplier * total_input_size) + additional_disk

  Int java_memory_size = ceil(7.5 * memory_multiplier)
  Float memory_size = java_memory_size + 2 # add 2 g for system.

  # Task is assuming query-sorted input so that the Secondary and Supplementary reads get marked correctly
  # This works because the output of BWA is query-grouped and therefore, so is the output of MergeBamAlignment.
  # While query-grouped isn't actually query-sorted, it's good enough for MarkDuplicates with ASSUME_SORT_ORDER="queryname"

  command <<<
    java -Dsamjdk.compression_level=~{compression_level} \
      -Xmx~{java_memory_size}g \
      -jar /usr/picard/picard.jar \
      MarkDuplicates \
      INPUT=~{sep=' INPUT=' input_bams} \
      OUTPUT=~{output_bam_basename}.bam \
      METRICS_FILE=~{metrics_filename} \
      VALIDATION_STRINGENCY=SILENT \
      ~{"READ_NAME_REGEX=" + read_name_regex} \
      ~{"SORTING_COLLECTION_SIZE_RATIO=" + sorting_collection_size_ratio} \
      OPTICAL_DUPLICATE_PIXEL_DISTANCE=2500 \
      ASSUME_SORT_ORDER="queryname" \
      CLEAR_DT="false" \
      ADD_PG_TAG_TO_READS=false
  >>>
  runtime {
    docker: "us.gcr.io/broad-gotc-prod/picard-cloud:2.26.10"
    preemptible: preemptible_tries
    memory: "~{memory_size} GiB"
    disks: "local-disk " + disk_size + " HDD"
  }
  output {
    File output_bam = "~{output_bam_basename}.bam"
    File duplicate_metrics = "~{metrics_filename}"
  }
}

# Mark duplicate reads to avoid counting non-independent observations
task MarkDuplicates_old {
  input {
    Array[File] input_bams
    String output_bam_basename
    String metrics_filename
    Float total_input_size
    Int compression_level
    Int preemptible_tries

    # The program default for READ_NAME_REGEX is appropriate in nearly every case.
    # Sometimes we wish to supply "null" in order to turn off optical duplicate detection
    # This can be desirable if you don't mind the estimated library size being wrong and optical duplicate detection is taking >7 days and failing
    String? read_name_regex
    #SQH: don't increase memory_multiplier
    Int memory_multiplier = 1 
    Int additional_disk = 20

    # Default is 0.25, however, it causes memory issue in Terra. Setting it to 0.1 to avoid memory issue.
    Float? sorting_collection_size_ratio = 0.1
  }

  # The merged bam will be smaller than the sum of the parts so we need to account for the unmerged inputs and the merged output.
  # Mark Duplicates takes in as input readgroup bams and outputs a slightly smaller aggregated bam. Giving .25 as wiggleroom
  Float md_disk_multiplier = 3
  Int disk_size = ceil(md_disk_multiplier * total_input_size) + additional_disk

  Float memory_size = 7.5 * memory_multiplier
  #Int java_memory_size = (ceil(memory_size) - 2)

  # Task is assuming query-sorted input so that the Secondary and Supplementary reads get marked correctly
  # This works because the output of BWA is query-grouped and therefore, so is the output of MergeBamAlignment.
  # While query-grouped isn't actually query-sorted, it's good enough for MarkDuplicates with ASSUME_SORT_ORDER="queryname"

  command <<<
    set -e
    # copied the following code from HaplotypeCaller_GATK4_VCF in GermlineVariantDiscovery 
    # Quanhu Sheng, 20230719 
    #
    # We need at least 1 GB of available memory outside of the Java heap in order to execute native code, thus, limit
    # Java's memory by the total memory minus 1 GB. We need to compute the total memory as it might differ from
    # memory_size_gb because of Cromwell's retry with more memory feature.
    # Note: In the future this should be done using Cromwell's ${MEM_SIZE} and ${MEM_UNIT} environment variables,
    #       which do not rely on the output format of the `free` command.
    # In ACCRE, the available_memory_mb is much larger than the memory we requested, which would cause the out of memory issue.
    available_memory_mb=$(free -m | awk '/^Mem/ {print $2}')
    let java_memory_size_mb=available_memory_mb-2048
    echo Total available memory: ${available_memory_mb} MB >&2
    echo Memory reserved for Java: ${java_memory_size_mb} MB >&2

    # To avoid OOM error, cap java_memory_size_mb to 50000 MB
    # Quanhu Sheng, 20251208 
    if [[ $java_memory_size_mb -gt 50000 ]]; then
      let java_memory_size_mb=50000
    fi

    java -Dsamjdk.compression_level=~{compression_level} -Xms${java_memory_size_mb}m -Xmx${java_memory_size_mb}m  -XX:GCTimeLimit=50 -XX:GCHeapFreeLimit=10 -jar /usr/picard/picard.jar \
      MarkDuplicates \
      INPUT=~{sep=' INPUT=' input_bams} \
      OUTPUT=~{output_bam_basename}.bam \
      METRICS_FILE=~{metrics_filename} \
      VALIDATION_STRINGENCY=SILENT \
      ~{"READ_NAME_REGEX=" + read_name_regex} \
      ~{"SORTING_COLLECTION_SIZE_RATIO=" + sorting_collection_size_ratio} \
      OPTICAL_DUPLICATE_PIXEL_DISTANCE=2500 \
      ASSUME_SORT_ORDER="queryname" \
      CLEAR_DT="false" \
      ADD_PG_TAG_TO_READS=false
  >>>

  runtime {
    docker: "us.gcr.io/broad-gotc-prod/picard-cloud:2.26.10"
    preemptible: preemptible_tries
    memory: "~{memory_size} GiB"
    disks: "local-disk " + disk_size + " HDD"
  }
  output {
    File output_bam = "~{output_bam_basename}.bam"
    File duplicate_metrics = "~{metrics_filename}"
  }
}
