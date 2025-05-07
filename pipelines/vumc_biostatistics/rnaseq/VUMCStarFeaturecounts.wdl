version 1.0

## Copyright Broad Institute, 2018
##
## This WDL pipeline implements data processing according to the GATK Best Practices (June 2016)
## for human whole-genome and exome sequencing data.
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

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCStarFeaturecounts {
  input {
    File fastq_1
    File fastq_2
    String sample_name
    
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

    File gtf

    Boolean output_bam = true

    String? billing_gcp_project_id
    String? target_gcp_folder
  }

  call STAR {
    input:
      fastq_1 = fastq_1,
      fastq_2 = fastq_2,
      sample_name = sample_name,
      chrLength_txt = chrLength_txt,
      chrNameLength_txt = chrNameLength_txt,
      chrName_txt = chrName_txt,
      chrStart_txt = chrStart_txt,
      exonGeTrInfo_tab = exonGeTrInfo_tab,
      exonInfo_tab = exonInfo_tab,
      geneInfo_tab = geneInfo_tab,
      Genome = Genome,
      genomeParameters_txt = genomeParameters_txt,
      SA = SA,
      SAindex = SAindex,
      sjdbInfo_txt = sjdbInfo_txt,
      sjdbList_fromGTF_out_tab = sjdbList_fromGTF_out_tab,
      sjdbList_out_tab = sjdbList_out_tab,
      transcriptInfo_tab = transcriptInfo_tab
  }

  call FeatureCounts {
    input:
      bam = STAR.output_bam,
      bam_index = STAR.output_bam_index,
      sample_name = sample_name,
      gtf = gtf
  }

  if (defined(target_gcp_folder)) {
    if (output_bam){
      call GcpUtils.MoveOrCopySixFiles as CopyFile6 {
        input:
          source_file1 = FeatureCounts.output_count,
          source_file2 = FeatureCounts.output_count_summary,
          source_file3 = STAR.output_star_chromosome_count,
          source_file4 = STAR.output_star_summary,
          source_file5 = STAR.output_bam,
          source_file6 = STAR.output_bam_index,
          is_move_file = false,
          project_id = billing_gcp_project_id,
          target_gcp_folder = select_first([target_gcp_folder])
      }
      String output_file11 = CopyFile6.output_file1
      String output_file12 = CopyFile6.output_file2
      String output_file13 = CopyFile6.output_file3
      String output_file14 = CopyFile6.output_file4
      String output_file15 = CopyFile6.output_file5
      String output_file16 = CopyFile6.output_file6
    }

    if (!output_bam){
      call GcpUtils.MoveOrCopyFourFiles as CopyFile4 {
        input:
          source_file1 = FeatureCounts.output_count,
          source_file2 = FeatureCounts.output_count_summary,
          source_file3 = STAR.output_star_chromosome_count,
          source_file4 = STAR.output_star_summary,
          is_move_file = false,
          project_id = billing_gcp_project_id,
          target_gcp_folder = select_first([target_gcp_folder])
      }
      String output_file21 = CopyFile4.output_file1
      String output_file22 = CopyFile4.output_file2
      String output_file23 = CopyFile4.output_file3
      String output_file24 = CopyFile4.output_file4
      String output_file25 = ""
      String output_file26 = ""
    }
  }
  # Outputs that will be retained when execution is complete
  output {
    File output_star_chromosome_count = select_first([output_file11, output_file21, STAR.output_star_chromosome_count])
    File output_star_summary = select_first([output_file12, output_file22, STAR.output_star_summary])
    File output_count = select_first([output_file13, output_file23, FeatureCounts.output_count])
    File output_count_summary = select_first([output_file14, output_file24, FeatureCounts.output_count_summary])}
    File output_bam = select_first([output_file15, output_file25, STAR.output_bam])
    File output_bam_index = select_first([output_file16, output_file26, STAR.output_bam_index])
}

task STAR {
  input {
    File fastq_1
    File fastq_2
    String sample_name
    
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

STAR \
  --twopassMode Basic \
  --outSAMmapqUnique 60 \
  --outSAMprimaryFlag AllBestScore \
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
    File bam
    File bam_index
    File gtf
    String sample_name
    Int threads = 8
  }
  Int disk_size_gb = ceil(size(bam, "GB")) + 4
  command <<<

set -euo pipefail

featureCounts \
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