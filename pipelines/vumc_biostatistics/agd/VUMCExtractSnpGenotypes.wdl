version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils

workflow VUMCExtractSnpGenotypes {
  input {
    File input_rsid_file
    String output_prefix

    Array[String] chromosomes
    Array[File] input_pgen_files
    Array[File] input_pvar_files
    Array[File] input_psam_files

    String? billing_gcp_project_id
    String? target_gcp_folder    
  }

  call ConvertRsidToBed {
    input:
      input_rsid_file = input_rsid_file,
      output_prefix = output_prefix
  }

  call GetChromosomeIndecies {
    input:
      input_chromosomes = chromosomes,
      input_bed_file = ConvertRsidToBed.output_bed
  }

  Int num_valid_chromsome = length(GetChromosomeIndecies.chromosome_indecies)

  scatter(chrom_ind in range(num_valid_chromsome)){
    Int old_ind = GetChromosomeIndecies.chromosome_indecies[chrom_ind]
    File pgen_file = input_pgen_files[old_ind]
    File pvar_file = input_pvar_files[old_ind]
    File psam_file = input_psam_files[old_ind]
    String chromosome = chromosomes[old_ind]

    call Plink2Utils.Plink2FilterPgen as Plink2FilterPgen {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = psam_file,
        keep_bed = ConvertRsidToBed.output_bed,
        plink2_filter_option = "",
        output_prefix = output_prefix + "." + chromosome + ".snp"
    }
  }

  if (num_valid_chromsome > 1){
    call Plink2Utils.MergePgenFiles as MergePgenFiles {
      input:
        input_pgen_files = Plink2FilterPgen.output_pgen,
        input_pvar_files = Plink2FilterPgen.output_pvar,
        input_psam_files = Plink2FilterPgen.output_psam,
        output_prefix = output_prefix + ".snp"
    }
  }

  if (num_valid_chromsome <= 1){
    File only_pgen = Plink2FilterPgen.output_pgen[0]
    File only_pvar = Plink2FilterPgen.output_pvar[0]
    File only_psam = Plink2FilterPgen.output_psam[0]
    Int only_num_variants = Plink2FilterPgen.num_variants[0]
  }

  Int cur_num_variants = select_first([MergePgenFiles.num_variants, only_num_variants])

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyFourFiles {
      input:
        source_file1 = ConvertRsidToBed.output_bed,
        source_file2 = select_first([MergePgenFiles.output_pgen, only_pgen]),
        source_file3 = select_first([MergePgenFiles.output_pvar, only_pvar]),
        source_file4 = select_first([MergePgenFiles.output_psam, only_psam]),
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_bed = select_first([MoveOrCopyFourFiles.output_file1, ConvertRsidToBed.output_bed])
    File output_pgen = select_first([MoveOrCopyFourFiles.output_file2, MergePgenFiles.output_pgen, only_pgen])
    File output_pvar = select_first([MoveOrCopyFourFiles.output_file3, MergePgenFiles.output_pvar, only_pvar])
    File output_psam = select_first([MoveOrCopyFourFiles.output_file4, MergePgenFiles.output_psam, only_psam])
    Int output_num_variants = select_first([MergePgenFiles.num_variants, only_num_variants])
  }
}

task ConvertRsidToBed {
  input {
    File input_rsid_file
    String dbSnp155_bb_file = "http://hgdownload.soe.ucsc.edu/gbdb/hg38/snp/dbSnp155.bb"
    String output_prefix
  }

  command {
    bigBedNamedItems -nameFile ~{dbSnp155_bb_file} ~{input_rsid_file} request.tmp.bed
    grep -v "_alt" request.tmp.bed > ~{output_prefix}.bed
    rm -f request.tmp.bed dbSnp155.bb
  }
  runtime {
    docker: "shengqh/ucsctools:latest"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "5 GiB"
  }
  output {
    File output_bed = "~{output_prefix}.bed"
  }
}

task GetChromosomeIndecies {
  input {
    Array[String] input_chromosomes
    File input_bed_file
    String docker = "shengqh/hail_gcp:20241127"
  }

  command <<<

#!/bin/bash

set -e

# Create Python script
cat > get_chrom_indices.py << 'EOF'
import pandas as pd
import sys

def get_chrom_indices(chrom_list, bed_file):
  # Read the bed file (assuming standard BED format: chrom start end ...)
  bed_df = pd.read_csv(bed_file, sep='\t', header=None)
  
  # Extract unique chromosomes from the bed file
  bed_chroms = set(bed_df[0].astype(str))
  
  # Find indices of chromosomes that are in the bed file
  indices = []
  for i, chrom in enumerate(chrom_list):
    if chrom in bed_chroms:
      indices.append(i)
  
  return indices

if __name__ == "__main__":
  # Read chromosomes from environment variable
  chrom_list = sys.argv[1].split(",")
  bed_file = sys.argv[2]
  
  indices = get_chrom_indices(chrom_list, bed_file)
  
  # Write indices to output file
  with open("chromosomes.txt", "w") as f:
    for idx in indices:
      f.write(f"{idx}\n")
EOF

# Run the Python script
python3 get_chrom_indices.py ~{sep="," input_chromosomes} ~{input_bed_file}

>>>

  runtime {
    cpu: 1
    docker: docker
    preemptible: 1
    disks: "local-disk 5 HDD"
    memory: "1 GiB"
  }

  output {
    Array[Int] chromosome_indecies = read_lines("chromosomes.txt")
  }
}