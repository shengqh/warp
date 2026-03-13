version 1.0

# This workflow prepares AGD VCF files by processing input VCF.
# The workflow performs the following steps:
# 1. Processes input VCF file to keep PASS variants only and add index.
# 3. Optionally copies the processed VCF file and index to a GCP folder.
#
# Input parameters:
# - input_vcf: The input VCF file to be processed.
# - output_prefix: Prefix for the output processed VCF file.
# - target_gcp_folder: Optional GCP folder to copy processed files to.
#
# Output parameters:
# - output_vcf: The merged VCF file.
# - output_vcf_index: The index file for the merged VCF.
#

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPrepareAgdVcfAwk {
  input {
    File input_vcf
    File input_vcf_index

    String output_prefix

    String? target_gcp_folder
  }
  
  # Prepare VCF file is extremely time cost, although add index and get sample/variant info might be fast, 
  # we still want to run them in serial to avoid any potential issue of running them in parallel. 
  call PrepareAgdVcfAwk {
    input: 
      input_vcf = input_vcf,
      input_vcf_index = input_vcf_index,
      output_prefix = output_prefix + ".pass"
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = PrepareAgdVcfAwk.output_vcf,
        source_file2 = PrepareAgdVcfAwk.output_vcf_index,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_vcf = select_first([CopyFile.output_file1, PrepareAgdVcfAwk.output_vcf])
    File output_vcf_index = select_first([CopyFile.output_file2, PrepareAgdVcfAwk.output_vcf_index])
    Int num_samples = PrepareAgdVcfAwk.num_samples
    Int num_variants = PrepareAgdVcfAwk.num_variants
  }
}

task PrepareAgdVcfAwk {
  input{
    File input_vcf
    File input_vcf_index

    String output_prefix

    Int cpu = 3
    Int machine_mem_gb = 10
    Int addtional_disk_space_gb = 10
    Int preemptible = 0 # for shard vcf, we can use preemptible node, but for whole chromosome vcf, we should not use preemptible node
  }

  Int disk_size = ceil(size(input_vcf, "GB") * 2) + addtional_disk_space_gb

  String target_vcf = "~{output_prefix}.vcf.gz"
  String output_sample_file = "samples.txt"

  command <<<

echo `date`: filter PASS variants ...
zcat ~{input_vcf} | awk '$7 == "PASS" || $1 ~ /^#/' | bgzip > ~{target_vcf}

status=$?
if [[ $status -ne 0 ]]; then
  echo "Error: Failed to filter PASS variants using awk." >&2
  exit $status
fi

echo `date`: tabix ...
tabix --threads ~{cpu} -p vcf ~{target_vcf}

echo `date`: bcftools query number of samples ...
bcftools query -l ~{target_vcf} > ~{output_sample_file}

cat ~{output_sample_file} | wc -l > num_samples.txt

echo `date`: bcftools query number of variants ...
bcftools index -n ~{target_vcf} > num_variants.txt

echo `date`: done.

  >>>

  runtime{
    cpu: cpu
    docker: "shengqh/samtools_bcftools_tabix:v1.23"
    preemptible: preemptible
    memory: machine_mem_gb + " GB"
    disks: "local-disk " + disk_size + " HDD"
  }

  output{
    File output_vcf = "~{target_vcf}"
    File output_vcf_index = "~{target_vcf}.tbi"
    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")
  }
}
