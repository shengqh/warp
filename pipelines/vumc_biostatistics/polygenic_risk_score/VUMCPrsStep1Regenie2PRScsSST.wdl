version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

# This workflow converts Regenie output to PRScs-SST format for polygenic risk score calculation.
# Developed by VUMC Biostatistics for population-specific GWAS studies.
# Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
#
# Workflow steps:
# 1. Transform Regenie output file into PRScs-SST compatible summary statistics format
# 2. Format columns to match required PRScs-SST input format (SNP, A1, A2, BETA, P)
# 3. Optionally map variant IDs to rsIDs using provided mapping file (ID,RSID columns)
# 4. Optionally copy results to a GCP storage location
#
# Inputs:
# - input_regenie: Regenie output file containing summary statistics
# - rsid_variantid_map_file: Optional CSV file mapping variant IDs to rsIDs (ID,RSID columns)
# - output_prefix: Prefix for the output SST file
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_sst_file: Path to the formatted PRScs-SST summary statistics file


workflow VUMCPrsStep1Regenie2PRScsSST {
  input {
    File input_regenie
    File? rsid_variantid_map_file # ID,RSID map file

    String output_prefix

    String? target_gcp_folder
  }

  call Regenie2PRScsSST {
    input:
      input_regenie = input_regenie,
      output_prefix = output_prefix
  }

  if(defined(rsid_variantid_map_file)){
    call variantID2rsID {
      input:
        input_sst = Regenie2PRScsSST.output_sst_file,
        rsid_variantid_map_file = select_first([rsid_variantid_map_file]),
        output_prefix = output_prefix
    }
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = select_first([variantID2rsID.output_sst_file, Regenie2PRScsSST.output_sst_file]),
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_sst_file = select_first([CopyFile.output_file, variantID2rsID.output_sst_file, Regenie2PRScsSST.output_sst_file])
  }
}

task Regenie2PRScsSST {
  input {
    File input_regenie

    String output_prefix

    Int preemptible=3
    Int memory_gb = 5
    Int additional_disk_size_gb = 2
  }

  Int disk_size = ceil(size([input_regenie], "GB") * 3) + additional_disk_size_gb

  command <<<

  zcat ~{input_regenie} | awk 'NR==1 {print "SNP\tA1\tA2\tBETA\tP"}; NR>1 {print $3"\t"$5"\t"$4"\t"$9"\t"10^-$12}' > ~{output_prefix}.sst

  >>>

  runtime {
    docker: "ubuntu:20.04"
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }

  output {
    File output_sst_file = "~{output_prefix}.sst"
  }
}

task variantID2rsID {
  input {
    File input_sst
    File rsid_variantid_map_file

    String output_prefix

    Int preemptible=3
    Int memory_gb=200
    Int additional_disk_size_gb = 2
  }

  Int disk_size = ceil(size(rsid_variantid_map_file, "GB")) + ceil(size([input_sst], "GB") * 3) + additional_disk_size_gb

  command <<<

cat <<CODE> rsid_variantid_map.R

library(data.table)

cat("Reading VariantID to rsID map file: ~{rsid_variantid_map_file} ...\n")
rsmap=fread("~{rsid_variantid_map_file}",header=T,sep=",",colClasses=c("character","character")) |>
  dplyr::rename(ID=1,RSID=2)

cat("Reading sst file: ~{input_sst} ...\n")
old_sst=fread("~{input_sst}",header=T,sep="\t",colClasses=c("character","character","character","numeric","numeric"))

cat("Merge sst and map file ...\n")
new_sst=merge(old_sst,rsmap,by.x="SNP",by.y="ID",all.x=TRUE)

new_sst=new_sst |>
  dplyr::rename(VARIANT_ID=SNP,
                SNP=RSID)

new_sst=new_sst |>
  dplyr::filter(!is.na(SNP)) |>
  dplyr::select(SNP,A1,A2,BETA,P,VARIANT_ID)

cat("Save sst file ...\n")
fwrite(new_sst,
       file="~{output_prefix}.rsid.sst",
       sep="\t",
       col.names=TRUE,
       quote=FALSE)

cat("Done ...\n")

CODE

R --vanilla -f rsid_variantid_map.R
  >>>

  runtime {
    docker: "shengqh/report:20250415"
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }

  output {
    File output_sst_file = "~{output_prefix}.rsid.sst"
  }
}
