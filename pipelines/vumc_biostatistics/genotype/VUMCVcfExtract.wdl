version 1.0

import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCVcfExtract {
  input {
    File input_vcf
    File input_vcf_index

    File? include_samples
    File? include_region_bed

    String bcftools_option = ""

    String target_prefix
    String target_suffix = ".vcf.gz"

    String? target_gcp_folder
  }

  if(!defined(include_samples) && !defined(include_region_bed)){
    call WDLUtils.FailWithMessage {
      input:
        message = "Either include_samples or include_region_bed must be defined"
    }
  }

  call BcftoolsExtract {
    input:
      input_vcf = input_vcf,
      input_vcf_index = input_vcf_index,
      include_samples = include_samples,
      include_region_bed = include_region_bed,
      bcftools_option = bcftools_option,
      target_prefix = target_prefix,
      target_suffix = target_suffix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = BcftoolsExtract.output_vcf,
        source_file2 = BcftoolsExtract.output_vcf_index,
        source_file3 = BcftoolsExtract.output_vcf_sample,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_vcf = select_first([CopyFile.output_file1, BcftoolsExtract.output_vcf])
    File output_vcf_index = select_first([CopyFile.output_file2, BcftoolsExtract.output_vcf_index])
    File output_vcf_sample = select_first([CopyFile.output_file3, BcftoolsExtract.output_vcf_sample])
    Int output_vcf_num_samples = BcftoolsExtract.output_vcf_num_samples
    Int output_vcf_num_variants = BcftoolsExtract.output_vcf_num_variants
  }
}

task BcftoolsExtract {
  input {
    File input_vcf
    File input_vcf_index

    File? include_samples
    File? include_region_bed

    String bcftools_option = ""

    String target_prefix
    String target_suffix
    
    String docker = "shengqh/hail_gcp:20240213"
    Float disk_factor = 2.2
    Int preemptible = 1
    Int cpu = 8
  }

  Int disk_size = ceil(size(input_vcf, "GB") * disk_factor) + 2
  Int memory_gb = 2 * cpu

  String target_vcf = target_prefix + target_suffix
  String target_vcf_index = target_vcf + ".tbi"
  String target_sample_file = target_vcf + ".samples.txt"

  command <<<

sample_query=""
if [[ "~{include_samples}" != "" ]]; then
  echo "get all samples in original VCF"
  bcftools query -l ~{input_vcf} > all.id.txt

  echo "get included samples"
  tr -d '\r' < ~{include_samples} > filter.id.txt
  grep -Fxf all.id.txt filter.id.txt | sort | uniq > keep.id.txt

  if [[ ! -s keep.id.txt ]]; then
    echo "ERROR: no samples to keep"
    exit 1
  fi

  sample_query="-S keep.id.txt"
fi

region_query=""
if [[ "~{include_region_bed}" != "" ]]; then
  region_query="-R ~{include_region_bed}"
fi

echo bcftools view ~{bcftools_option} $sample_query $region_query --threads ~{cpu} -o ~{target_vcf} ~{input_vcf}
bcftools view ~{bcftools_option} $sample_query $region_query --threads ~{cpu} -o ~{target_vcf} ~{input_vcf}

echo "build index"
bcftools index -t --threads ~{cpu} ~{target_vcf}

bcftools query -l ~{target_vcf} > ~{target_sample_file}

cat ~{target_sample_file} | wc -l > num_samples.txt

bcftools index -n ~{target_vcf} > num_variants.txt

>>>

  runtime {
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    # The output has to be defined as File, otherwise the file would not be delocalized
    File output_vcf = "~{target_vcf}"
    File output_vcf_index = "~{target_vcf_index}"
    File output_vcf_sample = "~{target_sample_file}"
    Int output_vcf_num_samples = read_int("num_samples.txt")
    Int output_vcf_num_variants = read_int("num_variants.txt")
  }
}
