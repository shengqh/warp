version 1.0


# This workflow implements PRScs for polygenic risk score calculation using GWAS summary statistics.
# Developed by VUMC Biostatistics for population-specific PRS studies.
# Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
#
# Workflow steps:
# 1. Performs chromosome-specific PRS calculations using PRScs
# 2. Concatenates chromosome-level effect files into a combined output
# 3. Optionally converts rsID to variantID format
# 4. Generates PLINK pvar format file from effect estimates
# 5. Optionally copies results to a GCP storage location
#
# Inputs:
# - chromosomes: List of chromosomes to analyze (default: 1-22)
# - n_gwas: Sample size of GWAS study
# - input_sst: Summary statistics file in PRScs-SST format
#              In order to convert rsID to variantID, the SST file must contain both SNP (rsID) and VARIANT_ID columns.
# - convert_rsID_to_variantID: Whether to convert rsID to variantID (default: true)
# - ld_files: Reference LD files (1000 Genomes or UK Biobank)
# - ld_folder_name: Name of the LD reference panel folder (must contain "1kg" or "ukbb")
# - output_prefix: Prefix for output files
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_effect_file: Path to the combined effect file (with variantID if converted)
# - output_effect_pvar_file: Effect file in PLINK pvar format


import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPrsStep2PRScs {
  input {
    Array[Int] chromosomes = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]

    Int n_gwas
    File input_sst

    Boolean convert_rsID_to_variantID = true

    # use files instead of .tar.gz to avoid unzipping the file, save time and disk space
    Array[File] ld_files
    
    # the folder name should contains either 1kg or ukbb, for example: "ldblk_ukbb_eur"
    String ld_folder_name

    String output_prefix

    String? target_gcp_folder

    # Since the lkg and ukbb all use hg18, using locus for agd data is not correct.
    # We will use "snplist" for all data.
    String ld_snpname = "snplist"
  }

  Int num_all_chromsome = length(chromosomes)

  scatter(all_chrom_ind in range(num_all_chromsome)){
    Int chromosome = chromosomes[all_chrom_ind]

    String output_prefix_chromosome = output_prefix + ".chr" + chromosome

    call PRScs as PRScs {
      input:
        n_gwas = n_gwas,
        input_sst = input_sst,
        ld_files = ld_files,
        ld_folder_name = ld_folder_name,
        ld_snpname = ld_snpname,
        chromosome = chromosome,
        output_prefix = output_prefix_chromosome
    }
  }

  call WDLUtils.concat_files as concat_files {
    input:
      input_files = PRScs.output_effect_file,
      output_file = output_prefix + ".effect.txt"
  }

  if(convert_rsID_to_variantID){
    call rsID_to_variantID as idconvert {
      input:
        input_effect_file = concat_files.concat_file,
        input_sst = input_sst,
        output_prefix = output_prefix
    }
  }

  call EffectToPvar {
    input:
      input_effect_file = select_first([idconvert.output_effect_file, concat_files.concat_file]),
      output_prefix = output_prefix + ".effect"
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = select_first([idconvert.output_effect_file, concat_files.concat_file]),
        source_file2 = EffectToPvar.output_pvar_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    String output_effect_file = select_first([CopyFile.output_file1, idconvert.output_effect_file, concat_files.concat_file])
    File output_effect_pvar_file = select_first([CopyFile.output_file2, EffectToPvar.output_pvar_file])
  }
}

task PRScs {
  input {
    Int n_gwas
    File input_sst

    Int seed = 20250610
    String chromosome

    Array[File] ld_files
    String ld_folder_name
    String ld_snpname

    String output_prefix

    String docker = "shengqh/prs:20251106"
    String PRSsc_script = "/opt/PRScs/PRScs.py"

    Int preemptible=3
    Int addtional_memory_gb = 10
    Int addtional_disk_space_gb = 10
  }

  Int disk_size = ceil(size(ld_files, "GB") + size(input_sst, "GB")) + addtional_disk_space_gb

  Int memory_gb = ceil(size(ld_files, "GB") + size(input_sst, "GB")) + addtional_memory_gb # 10 GB is the minimum memory for PRScs

  String suffix = ".pvar"

  command <<<
    ld_cur_folder=$(dirname ~{ld_files[1]})
    ln -s ${ld_cur_folder} ~{ld_folder_name}
    echo "ld_folder: ~{ld_folder_name} : $ld_cur_folder"

    echo "Running PRScs ..."
    python3 ~{PRSsc_script} \
      --ref_dir=~{ld_folder_name} \
      --ref_snpname=~{ld_snpname} \
      --sst_file=~{input_sst} \
      --chrom=~{chromosome} \
      --n_gwas=~{n_gwas} \
      --seed=~{seed} \
      --out=~{output_prefix}

    echo "Done ..."
    
  >>>

  runtime{
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
   }

  output {
    File output_effect_file = "~{output_prefix}_pst_eff_a1_b0.5_phiauto_chr~{chromosome}.txt"
  }
}

task rsID_to_variantID {
  input {
    File input_effect_file
    File input_sst

    String output_prefix

    Int addtional_disk_space_gb = 5

    Int preemptible=3
    Int memory_gb=2
  }

  Int disk_size = ceil(size([input_effect_file], "GB") * 2) + addtional_disk_space_gb

  command <<<

cat <<CODE> rsid_variantid_map.R

library(data.table)

cat("Reading sst file with both rsID and variantID: ~{input_sst} ...\n")
rsmap=fread("~{input_sst}",header=T) |>
  dplyr::select(SNP, VARIANT_ID) 

cat("Reading effect file: ~{input_effect_file} ...\n")
old_effect=fread("~{input_effect_file}",header=F) |>
  dplyr::rename(CHROM=1,
                SNP=2,
                POS=3,
                A1=4,
                A2=5,
                EFFECT=6)

cat("Merge sst and effect file ...\n")
new_effect=merge(old_effect,rsmap,by.x="SNP",by.y="SNP",all.x=TRUE)

new_effect=new_effect |>
  dplyr::select(CHROM,VARIANT_ID,POS,A1,A2,EFFECT,SNP)

cat("Save effect file ...\n")
fwrite(new_effect,
       file="~{output_prefix}.variantID.effect.txt",
       sep="\t",
       col.names=FALSE,
       quote=FALSE)

cat("Done ...\n")

CODE

R --vanilla -f rsid_variantid_map.R

  >>>

  runtime{
    docker: "shengqh/report:20250415"
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
   }

  output {
    File output_effect_file = "~{output_prefix}.variantID.effect.txt"
  }
}

task EffectToPvar {
  input {
    File input_effect_file

    String output_prefix

    Int addtional_disk_space_gb = 5

    Int preemptible=3
    Int memory_gb=2
  }

  Int disk_size = ceil(size([input_effect_file], "GB") * 2) + addtional_disk_space_gb
  String suffix = ".pvar"

  command <<<

  awk 'NR==1 {print "#CHROM\tPOS\tID\tREF\tALT"}; NR>1 {print $1"\t"$3"\t"$2"\t"$5"\t"$4}' ~{input_effect_file} > ~{output_prefix}.pvar

  >>>

  runtime{
    docker: "ubuntu:20.04"
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
   }

  output {
    File output_pvar_file = "~{output_prefix}.pvar"
  }
}
