version 1.0

task ReplaceICAIdWithGrid {
  input {
    File input_psam
    File id_map_file
    Int ICA_ID_Column = 0
    Int PRIMARY_GRID_Column = 1
    String target_psam
  }

  command <<<

cat <<CODE> script.py

import io

with open("~{id_map_file}", "rt") as fin:
  id_map = {}
  for line in fin:
    parts = line.strip().split('\t')
    
    ica_id = parts[~{ICA_ID_Column}]
    grid = parts[~{PRIMARY_GRID_Column}]

    # if GRID is "-", which means invalid, create a fake GRID
    if grid == "-":
      id_map[ica_id] = ica_id + "_INVALID"
    else:
      id_map[ica_id] = grid

with open("~{input_psam}", "rt") as fin:
  with open("~{target_psam}", "wt") as fout:
    for line in fin:
      parts = line.strip().split('\t')
      if parts[1] in id_map:
        parts[1] = id_map[parts[1]]

      newline = '\t'.join(parts)
      fout.write(f"{newline}\n")

CODE

python3 script.py 

>>>

  runtime {
    docker: "us.gcr.io/broad-dsp-gcr-public/base/python:3.9-debian"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    File output_psam = "~{target_psam}"
  }
}

/**
 * Task: CreateCohortPsam
 * 
 * Description:
 * This task is responsible for creating a cohort PSAM (Phenotype and Sample 
 * Attribute Matrix) file. The PSAM file typically contains metadata about 
 * samples and phenotypes, which can be used for downstream analysis in 
 * genomic studies.
 */
task CreateCohortPsam {
  input {
    File input_psam

    File? input_grid
    Int input_grid_column = 0

    String? input_ancestry
    String input_ancestry_column="ANCESTRY" #"supervised_ancestry_cluster" for original ancestry file
    File? input_ancestry_file

    String output_prefix
  }

  command <<<

cat <<CODE> script.py

import os

# Ensure required inputs are provided
if "~{input_grid}" == "" and ("~{input_ancestry}" == "" or "~{input_ancestry_file}" == ""):
    raise ValueError("Either input_grid must be defined, or both input_ancestry and input_ancestry_file must be defined.")

# Read the grid file and store the GRID in a set.
# It is possible to put the value in the header into GRID list but it should not matter.
grids = set()
has_grid_file = False
if "~{input_grid}" != "":
    has_grid_file = True
    with open("~{input_grid}", "rt") as fin:
        for line in fin:
            columns = line.rstrip().split('\t')
            if len(columns) > ~{input_grid_column}:
                grids.add(columns[~{input_grid_column}])
print(f"Grids from grid file: {len(grids)}")

# Read the ancenstry file and store the GRID in a set 
ancestry_grids = set()
has_ancestry_file = False
if "~{input_ancestry}" != "":
    if "~{input_ancestry_file}" != "":
        has_ancestry_file = True
        with open("~{input_ancestry_file}", "rt") as fin:
            header = fin.readline().rstrip().split('\t')
            if "~{input_ancestry_column}" in header:
                ancestry_index = header.index("~{input_ancestry_column}")
                for line in fin:
                    columns = line.rstrip().split('\t')
                    if len(columns) > ancestry_index and columns[ancestry_index] == "~{input_ancestry}":
                        ancestry_grids.add(columns[1])
print(f"Grids from ancestry file: {len(ancestry_grids)}")

# generate final grids
if has_grid_file and has_ancestry_file:
    grids = grids.intersection(ancestry_grids)
elif has_ancestry_file:
    grids = ancestry_grids

if len(grids) == 0:
    raise ValueError("No grid found.")

# Open the input PSAM file and the output file
output_file = "~{output_prefix}.psam"
with open("~{input_psam}", "rt") as fin, open(output_file, "wt") as fout:
    for line in fin:
        if line.startswith("#"):
            # Write the header line to the output file
            fout.write(line)
        else:
            # Split the line and check if the second column matches any grid value
            columns = line.split('\t')
            if columns[1] in grids:
                fout.write(line)

CODE

python3 script.py

grep -v "^#" "~{output_prefix}.psam" | wc -l > psam.count

>>>

  runtime {
    docker: "us.gcr.io/broad-dsp-gcr-public/base/python:3.9-debian"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    File output_psam = "~{output_prefix}.psam"
    Int output_sample_count = read_int("psam.count")
  }
}

task HailMatrixExtractRegions {
  input {
    File input_hail_mt_path_file
    File input_bed
    Float expect_output_vcf_bgz_size_gb
    String target_prefix

    String billing_project_id
    String? target_gcp_folder

    #don't change it to new version, as it might cause problem of hail
    String docker = "shengqh/hail_gcp:20241120"
    Int memory_gb = 10
    Int preemptible = 1
    Int cpu = 4
    Int boot_disk_gb = 10  
  }

  Int disk_size = ceil(expect_output_vcf_bgz_size_gb * 2) + 20
  Int total_memory_gb = memory_gb + 2

  String target_file = if defined(target_gcp_folder) then sub(select_first([target_gcp_folder]), "/+$", "") + "/" + target_prefix + ".vcf.bgz" else target_prefix + ".vcf.bgz"

  command <<<

#https://discuss.hail.is/t/i-get-a-negativearraysizeexception-when-loading-a-plink-file/899
export PYSPARK_SUBMIT_ARGS="--driver-java-options '-XX:+UnlockExperimentalVMOptions -XX:hashCode=0' --conf 'spark.executor.extraJavaOptions=\"-XX:+UnlockExperimentalVMOptions -XX:hashCode=0\"' pyspark-shell"

mkdir tmp

python3 <<CODE

import hail as hl
import pandas as pd

def parse_gcs_url(gcs_url):
    if not gcs_url.startswith('gs://'):
        raise ValueError("URL must start with 'gs://'")

    # Remove the 'gs://' prefix
    gcs_url = gcs_url[5:]

    # Split the remaining URL into bucket name and object key
    parts = gcs_url.split('/', 1)
    if len(parts) != 2:
        raise ValueError("Invalid GCS URL format")

    bucket_name = parts[0]

    return bucket_name

regions = pd.read_csv("~{input_bed}", sep='\t', header=None)
regions=regions.iloc[:, :3]
regions.columns = ["chr", "start", "end"]
regions['chr']=regions['chr'].astype(str)
regions['locus']=regions.chr + ":" + (regions.start + 1).astype(str) + "-" + (regions.end + 1).astype(str)
regions.head()

new_tbl = pd.read_csv("~{input_hail_mt_path_file}", sep='\t')
new_tbl.head()

hail_url = new_tbl['hail'][0]
if hail_url.startswith('gs://'):
  bucket_name = parse_gcs_url(new_tbl['hail'][0])
  print(f"hail_bucket_name={bucket_name}")

  hl.init(tmp_dir='tmp',
          spark_conf={"spark.driver.memory": "~{memory_gb}g",
                      "spark.local.dir": "tmp",
                      'spark.hadoop.fs.gs.requester.pays.mode': 'CUSTOM',
                      'spark.hadoop.fs.gs.requester.pays.buckets': bucket_name,
                      'spark.hadoop.fs.gs.requester.pays.project.id': "~{billing_project_id}"}, idempotent=True)
else:
  hl.init(tmp_dir='tmp',
          spark_conf={"spark.driver.memory": "~{memory_gb}g",
                      "spark.local.dir": "tmp"}, idempotent=True)

hl.default_reference("GRCh38")

hail_col="hail"

all_tbl=None
for ind in new_tbl.index:
    chr=new_tbl['chromosome'][ind]
    chr_regions=regions[regions.chr==chr]
    if chr_regions.shape[0] == 0:
        print(f"{chr}: no snps")
    else:
        print(f"{chr}: {chr_regions.shape[0]} snps")
        print(chr_regions)
        hail_url=new_tbl[hail_col][ind]
        mt = hl.read_matrix_table(hail_url)

        mt_filter = hl.filter_intervals(
            mt,
            [hl.parse_locus_interval(x,) for x in chr_regions.locus])
        
        print(f"  {chr} found {mt_filter.count_rows()} snps from hailmatrix")

        if mt_filter.count_rows() > 0:
            #keep GT only
            mt_filter = mt_filter.select_entries(mt_filter.GT)

            # Remove INFO annotations by setting them to empty
            mt_filter = mt_filter.annotate_rows(info=hl.struct())

            if all_tbl == None:
                all_tbl = mt_filter
            else:
                all_tbl = all_tbl.union_rows(mt_filter)

print(f"SNPs={all_tbl.count_rows()}, Samples={all_tbl.count_cols()}")

print(f"Writing to ~{target_file}")

all_tbl = all_tbl.naive_coalesce(2)

hl.export_vcf(all_tbl, "~{target_file}")

CODE

# #keep GT only
# bcftools annotate -x INFO,^FORMAT/GT ~{target_file} -Ov -o tmp.vcf.bgz
# mv tmp.vcf.bgz ~{target_file}

rm -rf tmp

>>>

  runtime {
    cpu: cpu
    docker: "~{docker}"
    preemptible: preemptible
    disks: "local-disk ~{disk_size} HDD"
    memory: "~{total_memory_gb} GiB"
    bootDiskSizeGb: boot_disk_gb
  }
  output {
    File output_vcf = "~{target_file}"
  }
}

task PrepareGeneGenotype {
  input {
    String gene_symbol
    File agd_primary_grid_file
    File annovar_file
    File vcf_file

    String docker = "shengqh/report:20241120"
    
    Int preemptible = 1
    Int additional_disk_size = 10
    Int memory_gb = 20
  }

  Int disk_size = ceil(size([agd_primary_grid_file, annovar_file, vcf_file], "GB") * 5) + additional_disk_size

  command <<<

cat <<EOF > script.r

library(data.table)
library(dplyr)

gene='~{gene_symbol}'
agd_primary_grid_file='~{agd_primary_grid_file}'
annovar_file='~{annovar_file}'
vcf_file='~{vcf_file}'

cat("reading", agd_primary_grid_file, "..\n")
agd_df=fread(agd_primary_grid_file, data.table=FALSE)

cat("reading", annovar_file, "...\n")
annovar=fread(annovar_file)
cat("there are", nrow(annovar), "SNVs in annovar ...\n")

print(head(annovar))

cat("filtering by gene", gene, "...\n")
annovar = annovar |>
  dplyr::filter(Gene.refGene==gene)
cat("there are", nrow(annovar), "SNVs in annovar from", gene, "...\n")

cat("filtering snv ... \n")
lof_snv = rbind(annovar |> dplyr::filter(Func.refGene %in% c('splicing')),
            annovar |> dplyr::filter(Func.refGene %in% c('exonic')) |> dplyr::filter(ExonicFunc.refGene %in% c('stopgain', 'startloss'))
)
write.table(lof_snv, paste0(gene, ".lof.annovar.txt"), quote=FALSE, row.names=FALSE, sep="\t")

vus_snv = rbind(annovar |> dplyr::filter(Func.refGene %in% c('splicing')),
            annovar |> dplyr::filter(Func.refGene %in% c('exonic')) |> dplyr::filter(ExonicFunc.refGene %in% c('stopgain', 'startloss', 'nonsynonymous SNV'))
)

cat("there are", nrow(vus_snv), "valid variants of uncertain significance (VUS) and loss-of-function variants (LOF), including", nrow(lof_snv), "LOF variants.\n")

# Use a pipe to decompress with zcat and read the first 4000 lines
con <- pipe(paste("zcat", vcf_file, "| head -n 4000"), "rt")
first_lines <- readLines(con)

# Close the connection
close(con)

chrom_index=grep("^#CHROM", first_lines)
cat("data starts from line", chrom_index, "...\n")

cat("reading", vcf_file, "...\n")
vcf = fread(cmd=paste0("zcat ", vcf_file), skip=chrom_index-1, data.table=FALSE)
cat("there are total", nrow(vcf), "SNVs...\n")

to_genotype_file<-function(vcf, snv, genotype_file){
  cat("preparing", genotype_file, "...\n")
  snv_vcf=vcf |> dplyr::filter(POS %in% snv\$Start)
  snv_vcf_data = snv_vcf[,10:ncol(snv_vcf)]

  cat("  converting snv to genotype ... \n")
  snv_vcf_gt = data.frame(lapply(snv_vcf_data, function(x) { gsub(':.*', '', x)}), check.names=FALSE)
  snv_vcf_gt = data.frame(lapply(snv_vcf_data, function(x) { gsub('[|]', '/', x)}), check.names=FALSE)
  print(head(snv_vcf_gt[,1:5]))

  has_snv=apply(snv_vcf_gt, 2, function(x) { any(x %in% c('1/1', '0/1', '1/0', '0/2', '2/0'))})

  df=data.frame(GRID=colnames(snv_vcf_gt), Genotype=ifelse(has_snv, "1", "0")) |> 
    dplyr::filter(GRID %in% agd_df\$PRIMARY_GRID) |>
    dplyr::arrange(GRID)
    
  cat("  saving to", genotype_file, "...\n")
  write.csv(df, genotype_file, quote=FALSE, row.names=FALSE)

  freq_df=as.data.frame(table(df\$Genotype))
  write.csv(freq_df, gsub(".csv", ".freq.csv", genotype_file), quote=FALSE, row.names=FALSE)
}

to_genotype_file(vcf, lof_snv, paste0(gene, ".lof.genotype.csv"))
to_genotype_file(vcf, vus_snv, paste0(gene, ".vus.genotype.csv"))

cat("done\n")

EOF

R -f script.r

>>>

  runtime {
    cpu: 1
    docker: "~{docker}"
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    String lof_genotype_name = "~{gene_symbol}_lof"
    File lof_annovar_file = "~{gene_symbol}.lof.annovar.txt"
    File lof_genotype_file = "~{gene_symbol}.lof.genotype.csv"
    File lof_genotype_freq_file = "~{gene_symbol}.lof.genotype.freq.csv"
    String vus_genotype_name = "~{gene_symbol}_vus"
    File vus_genotype_file = "~{gene_symbol}.vus.genotype.csv"
    File vus_genotype_freq_file = "~{gene_symbol}.vus.genotype.freq.csv"
  }
}

task PreparePhenotype {
  input {
    String phename
    Float phecode

    File agd_primary_grid_file
    File phecode_data_file
    File phecode_map_file
    Int min_occurance = 2

    String docker = "shengqh/report:20241120"
    
    Int preemptible = 1

    Int memory_gb = 20
  }

  Int disk_size = ceil(size([agd_primary_grid_file, phecode_data_file, phecode_map_file], "GB")) + 10

  command <<<

wget https://raw.githubusercontent.com/shengqh/ngsperl/refs/heads/master/lib/CQS/reportFunctions.R

wget https://raw.githubusercontent.com/shengqh/ngsperl/refs/heads/master/lib/BioVU/prepare_phenotype_data.rmd

mv prepare_phenotype_data.rmd ~{phename}.phenotype.rmd

echo -e "~{phename}\tphename" > input_options.txt
echo -e "~{phecode}\tphecode" >> input_options.txt
echo -e "~{agd_primary_grid_file}\tagd_file" >> input_options.txt
echo -e "~{phecode_data_file}\tphecode_data_file" >> input_options.txt
echo -e "~{phecode_map_file}\tphecode_map_file" >> input_options.txt
echo -e "~{min_occurance}\tmin_occurance" >> input_options.txt

R -e "library(knitr);rmarkdown::render(input='~{phename}.phenotype.rmd');"   

>>>

  runtime {
    cpu: 1
    docker: "~{docker}"
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File phenotype_file = "~{phename}.phenotype.csv"
    File phenotype_report = "~{phename}.phenotype.html"
  }
}

task LinearAssociation {
  input {
    String phename
    Float phecode
    String genotype_name

    File phenotype_file

    File agd_primary_grid_file
    File demographics_file
    File genotype_file
    File pca_file
    File phecode_map_file
    File ancestry_file

    String docker = "shengqh/report:20241120"
    
    Int preemptible = 1

    Int memory_gb = 20
  }

  Int disk_size = ceil(size([phenotype_file, agd_primary_grid_file, demographics_file, genotype_file, pca_file, phecode_map_file, ancestry_file], "GB")) + 10

  command <<<

wget https://raw.githubusercontent.com/shengqh/ngsperl/refs/heads/master/lib/CQS/reportFunctions.R

wget https://raw.githubusercontent.com/shengqh/ngsperl/refs/heads/master/lib/BioVU/linear_association.rmd

mv linear_association.rmd ~{phename}.~{genotype_name}.glm.rmd

echo -e "~{phename}\tphename" > input_options.txt
echo -e "~{phecode}\tphecode" >> input_options.txt
echo -e "~{phenotype_file}\tphefile" >> input_options.txt
echo -e "~{genotype_name}\tgenotype_name" >> input_options.txt
echo -e "~{genotype_file}\tgenotype_file" >> input_options.txt
echo -e "~{agd_primary_grid_file}\tagd_file" >> input_options.txt
echo -e "~{demographics_file}\tdemographics_file" >> input_options.txt
echo -e "~{pca_file}\tpca_file" >> input_options.txt
echo -e "~{phecode_map_file}\tphecode_map_file" >> input_options.txt
echo -e "~{ancestry_file}\tancestry_file" >> input_options.txt

R -e "library(knitr);rmarkdown::render(input='~{phename}.~{genotype_name}.glm.rmd');"   

>>>

  runtime {
    cpu: 1
    docker: "~{docker}"
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File linear_association_file = "~{phename}.~{genotype_name}.glm.csv"
    File linear_association_report = "~{phename}.~{genotype_name}.glm.html"
  }
}

task LinearAssociationSummary {
  input {
    File phecode_map_file

    Array[String] phecodes
    Array[File] linear_association_files

    String genotype_name

    String docker = "shengqh/report:20241120"
    
    Int preemptible = 1

    Int memory_gb = 20
  }

  Int disk_size = ceil(size(linear_association_files, "GB")) + 10

  String output_file = "~{genotype_name}.linear_association.csv"

  command <<<

echo "~{sep='\n' phecodes}" > phecodes.txt
echo "~{sep='\n' linear_association_files}" > linear_association_files.txt
paste linear_association_files.txt phecodes.txt > phecode_files.txt
rm phecodes.txt linear_association_files.txt

cat <<EOF > script.r

library(data.table)
library(dplyr)

files=fread("phecode_files.txt", data.table=FALSE) |>
  dplyr::rename(file=1, phecode=2)

res_df=data.frame()
idx=1
for(idx in c(1:nrow(files))){
  file<-files\$file[idx]
  phecode<-files\$phecode[idx]

  #Load
  data <- fread(file, data.table=FALSE)
  data_res=data[2,,drop=FALSE]    
  data_res$V1[1]=phecode

  res_df=rbind(res_df, data_res)
}

res_df=res_df |>
  dplyr::rename(phecode=V1)

phecodes_def=fread("~{phecode_map_file}", data.table=FALSE) |>
  dplyr::select(phecode, phenotype) |>
  dplyr::distinct()

final_df=merge(res_df, phecodes_def, by="phecode", all.x=TRUE) |>
  dplyr::select(phecode, phenotype, everything()) |>
  dplyr::rename(pvalue=6)
final_df\$padj=p.adjust(final_df\$pvalue, method="fdr")
final_df=final_df[order(final_df\$pvalue),]

write.csv(final_df, "~{output_file}", row.names=FALSE)

EOF

R -f script.r

>>>

  runtime {
    cpu: 1
    docker: "~{docker}"
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File linear_association_summary_file = "~{output_file}"
  }
}
