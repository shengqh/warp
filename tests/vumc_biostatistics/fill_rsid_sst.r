library(data.table)

rsmap=fread('/nobackup/h_cqs/shengq2/biovu/agd250k/rsid/rsid-variantid-map.txt.gz',header=T,sep=",",colClasses=c("character","character"))

old_sst=fread('/nobackup/h_cqs/shengq2/biovu/prs/gwas_eur.499.sst',header=T,sep="\t",colClasses=c("character","character","character","numeric","numeric"))

new_sst=merge(old_sst,rsmap,by.x="SNP",by.y="ID",all.x=TRUE)

new_sst=new_sst |>
  dplyr::rename(VARIANT_ID=SNP,
                SNP=avsnp151)

new_sst=new_sst |>
  dplyr::filter(!is.na(SNP)) |>
  dplyr::select(SNP,A1,A2,BETA,P,VARIANT_ID)

fwrite(new_sst,
       file='/nobackup/h_cqs/shengq2/biovu/prs/gwas_eur.499.rsid.sst',
       sep="\t",col.names=TRUE,quote=FALSE)
