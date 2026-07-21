version 1.0

task rsID2variantID {
  input {
    File input_sst
    File input_bim

    File rsid_variantid_map_file

    String output_prefix

    Int preemptible=3
    Int memory_gb=20
    Int additional_disk_size_gb = 2
  }

  Int disk_size = ceil(size(rsid_variantid_map_file, "GB")) + ceil(size([input_sst, input_bim], "GB") * 2) + additional_disk_size_gb

  command <<<

cat <<CODE> rsid_variantid_map.R

library(data.table)
library(dplyr)

cat("Reading VariantID to rsID map file: ~{rsid_variantid_map_file} ...\n")
rsmap=fread("~{rsid_variantid_map_file}",header=T,sep=",") |>
  dplyr::rename(ID=1,RSID=2)

cat("Reading sst file: ~{input_sst} ...\n")
old_sst=fread("~{input_sst}",header=T,sep="\t",colClasses=c("character","character","character","numeric","numeric"))

cat("Merge sst and map file ...\n")
new_sst=merge(old_sst,rsmap,by.x="SNP",by.y="RSID",all=FALSE)

cat("Make sure the A1/A2 match the variant REF/ALT (oder doesn't matter), otherwise remove the variant ...\n")
new_sst=new_sst |>
  dplyr::filter((REF==A2 & ALT==A1) | (REF==A1 & ALT==A2)) |>
  dplyr::rename(VARIANT_ID=ID) 
  
cat("Flip alleles to A1=ALT and A2=REF ...\n")
new_sst <- new_sst |>
  dplyr::mutate(
    is_flipped = A1 == REF,
    A1 = ALT,
    A2 = REF,
    BETA = if_else(is_flipped, -BETA, BETA)
  ) |>
  dplyr::select(SNP,A1,A2,BETA,P,VARIANT_ID,is_flipped)

cat("Save sst file ...\n")
fwrite(new_sst,
       file="~{output_prefix}.rsid_variantid_flipped.sst",
       sep="\t",
       col.names=TRUE,
       quote=FALSE)

cat("Reading bim file: ~{input_bim} ...\n")
old_bim=fread("~{input_bim}",header=FALSE,sep="\t") |>
  dplyr::rename(BIM_CHROM=1,SNP=2,CM_POS=3,BIM_POS=4,A1=5,A2=6)

cat("Merge bim and map file ...\n")
new_bim=merge(old_bim,rsmap,by.x="SNP",by.y="RSID",all=FALSE)

cat("Make sure the A1/A2 match the variant REF/ALT (oder doesn't matter), otherwise remove the variant ...\n")
new_bim=new_bim |>
  dplyr::filter((REF==A2 & ALT==A1) | (REF==A1 & ALT==A2)) |>
  dplyr::select(BIM_CHROM,SNP,CM_POS,POS,ALT,REF) # replace the BIM_POS, A1 and A2 by POS, ALT and REF from map file

cat("Save bim file ...\n")
fwrite(new_bim,
       file="~{output_prefix}.rsid_variantid_flipped.bim",
       sep="\t",
       col.names=FALSE,
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
    File output_sst_file = "~{output_prefix}.rsid_variantid_flipped.sst"
    File output_bim_file_for_PRScs = "~{output_prefix}.rsid_variantid_flipped.bim"
  }
}

task variantID2rsID {
  input {
    File input_sst
    File input_bim

    File rsid_variantid_map_file

    String output_prefix

    Int preemptible=3
    Int memory_gb=20
    Int additional_disk_size_gb = 2
  }

  Int disk_size = ceil(size(rsid_variantid_map_file, "GB")) + ceil(size([input_sst, input_bim], "GB") * 3) + additional_disk_size_gb

  command <<<

cat <<CODE> rsid_variantid_map.R

library(data.table)
library(dplyr)
library(stringr)
library(tidyr)

cat("Reading VariantID to rsID map file: ~{rsid_variantid_map_file} ...\n")
rsmap=fread("~{rsid_variantid_map_file}",header=T,sep=",") |>
  dplyr::rename(ID=1,RSID=2)

rsmap <- rsmap |>
  mutate(
    CHROM_POS = paste0(CHROM, ":", POS)
  )

cat("Reading sst file: ~{input_sst} ...\n")
old_sst=fread("~{input_sst}",header=T,sep="\t",colClasses=c("character","character","character","numeric","numeric"))

cat("Make sure the A1 and A2 match the variant ID, otherwise remove the variant ...\n")
old_sst <- old_sst |>
  mutate(
    CHROM_POS = sub(":[^:]+:[^:]+$", "", SNP)
  )

cat("Merge sst and map file by chrom/position...\n")
new_sst=merge(old_sst,rsmap,by="CHROM_POS",all=FALSE)

cat("Make sure that A1/A2 match the variant REF/ALT (order doesn't matter) ...\n")
new_sst = new_sst |>
  dplyr::filter((A1 == REF & A2 == ALT) | (A1 == ALT & A2 == REF))

new_sst=new_sst |>
  dplyr::rename(SST_ID=SNP,
                VARIANT_ID=ID,
                SNP=RSID)

cat("Flip alleles to A1=ALT and A2=REF ...\n")
new_sst <- new_sst |>
  dplyr::mutate(
    is_flipped = A1 == REF,
    A1 = ALT,
    A2 = REF,
    BETA = if_else(is_flipped, -BETA, BETA)
  ) |>
  dplyr::select(SNP,A1,A2,BETA,P,VARIANT_ID,is_flipped)

cat("Save sst file ...\n")
fwrite(new_sst,
       file="~{output_prefix}.rsid_variantid_flipped.sst",
       sep="\t",
       col.names=TRUE,
       quote=FALSE)

cat("Reading bim file: ~{input_bim} ...\n")
old_bim=fread("~{input_bim}",header=F,sep="\t") |>
  dplyr::rename(BIM_CHROM=1,SNP=2,CM_POS=3,BIM_POS=4,A1=5,A2=6)

old_bim <- old_bim |>
  mutate(
    CHROM_POS = sub(":[^:]+:[^:]+$", "", SNP)
  )

cat("Merge bim and map file ...\n")
new_bim=merge(old_bim,rsmap,by="CHROM_POS",all=FALSE)

new_bim=new_bim |>
  dplyr::rename(SST_ID=SNP,
                VARIANT_ID=ID,
                SNP=RSID)

cat("Make sure that A1/A2 match the variant REF/ALT (order doesn't matter) ...\n")
new_bim = new_bim |>
  dplyr::filter((A1 == REF & A2 == ALT) | (A1 == ALT & A2 == REF))

new_bim=new_bim |>
  dplyr::select(BIM_CHROM,SNP,CM_POS,POS,ALT,REF)

cat("Save bim file ...\n")
fwrite(new_bim,
       file="~{output_prefix}.rsid_variantid_flipped.bim",
       sep="\t",
       col.names=FALSE,
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
    File output_sst_file = "~{output_prefix}.rsid_variantid_flipped.sst"
    File output_bim_file_for_PRScs = "~{output_prefix}.rsid_variantid_flipped.bim"
  }
}
