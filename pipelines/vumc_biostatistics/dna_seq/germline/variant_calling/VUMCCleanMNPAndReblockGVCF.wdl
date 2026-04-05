version 1.0

# This workflow cleans GVCF files by removing multi-nucleotide polymorphisms (MNPs).
# Developed by VUMC Biostatistics for variant calling quality control.
# Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
#
# Workflow steps:
# 1. Filters GVCF to remove variants without <NON_REF> in info field (otherwise cause Reblock failure )
# 2. Removes MNP variants (such like AC => TT)
# 3. Removes duplicate variants at the same position
# 4. Compresses cleaned GVCF with bgzip
# 5. Indexes cleaned GVCF with tabix
# 6. Reblocks GVCF for joint genotyping
# 7. Optionally copies results to a GCP storage location
#
# Inputs:
# - sample_name: Name of the sample for output file naming
# - input_gvcf: GVCF file to clean
# - input_gvcf_index: Index file for input GVCF
# - ref_fasta: Reference genome FASTA file
# - ref_fasta_index: Index file for reference FASTA
# - ref_dict: Dictionary file for reference genome
# - scattered_calling_intervals_list: Intervals list for reblocking
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - reblocked_gvcf: Cleaned and reblocked GVCF file (bgzip compressed)
# - reblocked_gvcf_index: Index file for cleaned GVCF (tabix format)


import "../../../../wdl/dna_seq/germline/joint_genotyping/reblocking/ReblockGVCF.wdl" as BroadReblock
import "../../../../../tasks/wdl/GermlineVariantDiscovery.wdl" as Calling
import "../../../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# WORKFLOW DEFINITION 
workflow VUMCCleanMNPAndReblockGVCF {
  input {
    String sample_name

    File input_gvcf
    File input_gvcf_index

    File ref_dict
    File ref_fasta
    File ref_fasta_index

    String gatk_docker = "us.gcr.io/broad-gatk/gatk:4.6.1.0"

    String? target_gcp_folder  
  }  

  call CleanMNP {
    input:
      sample_name = sample_name,
      input_gvcf = input_gvcf,
      input_gvcf_index = input_gvcf_index
  }

  call Calling.Reblock as Reblock {
    input:
      gvcf = CleanMNP.output_gvcf,
      gvcf_index = CleanMNP.output_gvcf_index,
      ref_fasta = ref_fasta,
      ref_fasta_index = ref_fasta_index,
      ref_dict = ref_dict,
      output_vcf_filename = sample_name + ".clean.rb.g.vcf.gz",
      docker_path = gatk_docker
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = Reblock.output_vcf,
        source_file2 = Reblock.output_vcf_index,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder]),
    }
  }

  output {
    File reblocked_gvcf = select_first([CopyFile.output_file1, Reblock.output_vcf])
    File reblocked_gvcf_index = select_first([CopyFile.output_file2, Reblock.output_vcf_index])
  }
}

task CleanMNP {
  input {
    String sample_name

    File input_gvcf
    File input_gvcf_index

    # Runtime parameters
    String docker = "shengqh/samtools_bcftools_tabix:v1.23"
    Int mem_gb = 10
    Int preemptible_attempts = 3
  }

  Int disk_size = ceil(size(input_gvcf, "GB") * 2) + 10

  command <<<

    set -e

    bcftools view "~{input_gvcf}" | awk -F"\t" 'BEGIN{OFS="\t"}
    /^#/ {print; next}
    {
        split($5, alt_arr, ",");
        n_alt = length(alt_arr);
        if (alt_arr[n_alt] == "<NON_REF>") {
            is_mnp = 0;
            if (length($4) > 1) {
                if (length($4) == length(alt_arr[1])) {
                    is_mnp = 1;
                }
            }

            if (is_mnp == 0 && $2 != last_pos) {
                print $0;
                last_pos = $2;
            }
        }
    }' | bgzip -c > ~{sample_name}.clean.g.vcf.gz

    tabix -p vcf "~{sample_name}.clean.g.vcf.gz"

  >>>
  runtime {
    docker: docker
    memory: mem_gb + " GiB"
    disks: "local-disk " + disk_size + " HDD"
    preemptible: preemptible_attempts
  }
  output {
    File output_gvcf = "~{sample_name}.clean.g.vcf.gz"
    File output_gvcf_index = "~{sample_name}.clean.g.vcf.gz.tbi"
  }
}
