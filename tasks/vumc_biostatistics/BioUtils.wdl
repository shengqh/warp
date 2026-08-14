version 1.0

task GetGeneLocus_hg38_AnnotationHub {
  input {
    String gene_symbol
    Int shift_bases = 2000

    String docker = "shengqh/annotationhub:20260814"
    Int preemptible = 1

    Int addChr = 1
  }

  String target_file = gene_symbol + ".bed"

  command <<<

mkdir -p AnnotationHub_cache

cat <<EOF > script.r

library(AnnotationHub)
library(ensembldb)
library(stringr)

setAnnotationHubOption("CACHE", "./AnnotationHub_cache")

gene_names_str="~{gene_symbol}"
gene_names <- trimws(strsplit(gene_names_str, ",")[[1]])

cat("gene_names: ", gene_names, "\n")

addChr=~{addChr}
shift_bases=~{shift_bases}

ah <- AnnotationHub()

edb = query(ah, c("EnsDb", "Homo sapiens", "113"))
edb <- edb[[1]]

geneLocus = genes(
    edb,
    filter = AnnotationFilterList(GeneNameFilter(gene_names),
                                  GeneBiotypeFilter("protein_coding")),
    return.type = "DataFrame"
)

#          gene_id   gene_name   gene_biotype gene_seq_start gene_seq_end
#       <character> <character>    <character>      <integer>    <integer>
# 1 ENSG00000149311         ATM protein_coding      108222804    108369102
#      seq_name seq_strand seq_coord_system            description
#   <character>  <integer>      <character>            <character>
# 1          11          1       chromosome ATM serine/threonine..
#      gene_id_version canonical_transcript      symbol entrezid
#          <character>          <character> <character>   <list>
# 1 ENSG00000149311.22      ENST00000675843         ATM      472

geneLocus<-geneLocus[nchar(geneLocus\$seq_name) < 6,]

geneLocus\$score<-1000

geneLocus<-geneLocus[,c("seq_name", "gene_seq_start", "gene_seq_end", "score", "symbol", "seq_strand", "gene_id")]
geneLocus<-geneLocus[order(geneLocus\$seq_name, geneLocus\$gene_seq_start),]

geneLocus\$seq_strand[geneLocus\$seq_strand == 1]<-"+"
geneLocus\$seq_strand[geneLocus\$seq_strand == -1]<-"-"

if(addChr & (!any(grepl("chr", geneLocus\$seq_name)))){
  geneLocus\$seq_name = paste0("chr", geneLocus\$seq_name)
}

geneLocus\$seq_name=gsub("chrMT", "chrM", geneLocus\$seq_name)

if(shift_bases > 0){
  geneLocus\$gene_seq_start = geneLocus\$gene_seq_start - shift_bases
  geneLocus\$gene_seq_end = geneLocus\$gene_seq_end + shift_bases
}

bedFile<-"~{target_file}"
write.table(geneLocus, file=bedFile, row.names=F, col.names = F, sep="\t", quote=F)

EOF

R -f script.r

rm -rf AnnotationHub_cache

>>>

  runtime {
    cpu: 1
    docker: "~{docker}"
    preemptible: preemptible
    disks: "local-disk 10 HDD"
    memory: "4 GiB"
  }
  output {
    File gene_bed = "~{target_file}"
  }
}

task GetGeneLocus {
  input {
    String gene_symbol
    Int shift_bases = 2000

    String docker = "shengqh/report:20241120"
    Int preemptible = 1

    String host = "https://www.ensembl.org"
    String dataset = "hsapiens_gene_ensembl"
    String symbolKey = "hgnc_symbol"
    Int addChr = 1
  }

  String target_file = gene_symbol + ".bed"

  command <<<

cat <<EOF > script.r

require(biomaRt)
require(stringr)

host="~{host}"
dataset="~{dataset}"
symbolKey="~{symbolKey}"
genes="~{gene_symbol}"
addChr=~{addChr}
shift_bases=~{shift_bases}

ensembl <- useMart("ensembl", host=host, dataset=dataset)

geneLocus<-getBM(attributes=c("chromosome_name", "start_position", "end_position", symbolKey, "strand", "ensembl_gene_id"),
                 filters=symbolKey, 
                 values=genes, 
                 mart=ensembl, 
                 uniqueRows=TRUE,
                 useCache=FALSE)

geneLocus<-geneLocus[nchar(geneLocus\$chromosome_name) < 6,]

geneLocus\$score<-1000

geneLocus<-geneLocus[,c("chromosome_name", "start_position", "end_position", "score", symbolKey, "strand", "ensembl_gene_id")]
geneLocus<-geneLocus[order(geneLocus\$chromosome_name, geneLocus\$start_position),]

geneLocus\$strand[geneLocus\$strand == 1]<-"+"
geneLocus\$strand[geneLocus\$strand == -1]<-"-"

if(addChr & (!any(grepl("chr", geneLocus\$chromosome_name)))){
  geneLocus\$chromosome_name = paste0("chr", geneLocus\$chromosome_name)
}

geneLocus\$chromosome_name=gsub("chrMT", "chrM", geneLocus\$chromosome_name)

if(shift_bases > 0){
  geneLocus\$start_position = geneLocus\$start_position - shift_bases
  geneLocus\$end_position = geneLocus\$end_position + shift_bases
}

bedFile<-"~{target_file}"
write.table(geneLocus, file=bedFile, row.names=F, col.names = F, sep="\t", quote=F)

EOF

R -f script.r

>>>

  runtime {
    cpu: 1
    docker: "~{docker}"
    preemptible: preemptible
    disks: "local-disk 10 HDD"
    memory: "4 GiB"
  }
  output {
    File gene_bed = "~{target_file}"
  }
}

# this task doesn't work for AGD since pgen format doesn't have variant name in pvar file. It would cause duplicated variant name issue.
task PgenQCFilterList {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    String output_prefix

    String qc_option

    Int memory_gb = 20
    Int cpu = 8

    String docker = "shengqh/plink_1.9_2.0:20260526"
  }

  Int disk_size = ceil(size([input_pgen, input_pvar, input_psam], "GB")) + 5

  command <<<

plink2 \
  --pgen ~{input_pgen} \
  --pvar ~{input_pvar} \
  --psam ~{input_psam} \
  ~{qc_option} \
  --threads ~{cpu} \
  --write-snplist --write-samples --no-id-header \
  --out ~{output_prefix}

>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_snplist = "~{output_prefix}.snplist"
    File output_samples = "~{output_prefix}.id"
  }
}

# This task check the overlap between the chromosomes of pgen_pvar file and user defined chromosomes.
# It is used in VUMCRegenie4.wdl workflow
task GetValidChromosomeList {
  input {
    File input_pvar
    Array[String] input_chromosomes
  }

  Int disk_size = ceil(size(input_pvar, "GB")) + 1

  command <<<

cut -f 1 ~{input_pvar} | tail -n +2 | uniq > pvar_chromosomes.txt

echo -e "~{sep='\n' input_chromosomes}" > input_chromosomes.txt

#using pvar_chromosomes.txt to filter input_chromosomes.txt, get the common chromosomes
grep -Fxf pvar_chromosomes.txt input_chromosomes.txt > common_chromosomes.txt

>>>

  runtime {
    cpu: 1
    docker: "ubuntu:20.04"
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: "1 GiB"
  }

  output {
    Array[String] valid_chromosomes = read_lines("common_chromosomes.txt")
  }
}

task GetAutosomalChromosomeIndecies {
  input {
    Array[String] input_chromosomes
  }

  command <<<
  echo -e "~{sep='\n' input_chromosomes}" | 
  awk '{
            # Remove "chr" prefix if present
            gsub(/^chr/, "", $1);
            # If the chromosome is a number between 1-22, print its line number
            if ($1 ~ /^[1-9]$/ || $1 ~ /^1[0-9]$/ || $1 ~ /^2[0-2]$/) {
                print NR - 1  # Subtract 1 for 0-based index
            }
        }' > autosomal_chromosomes.txt
>>>

  runtime {
    cpu: 1
    docker: "ubuntu:20.04"
    preemptible: 1
    disks: "local-disk 5 HDD"
    memory: "1 GiB"
  }

  output {
    Array[Int] autosomal_chromosome_indecies = read_lines("autosomal_chromosomes.txt")
  }
}

# This task converts a list of rsIDs to BED format using UCSC tools
# 
# The task uses the UCSC bigBedNamedItems tool to extract genomic position information
# for given rsIDs from the dbSnp155 database, resulting in a standard BED file
# that can be used for downstream genomic analyses.
#
# Inputs:
#   - File containing list of rsIDs, one per line
#   - Path to the dbSnp155 bigBed file
#
# Outputs:
#   - BED format file with positions for the input rsIDs
task ConvertRsidToBed {
  input {
    File? input_rsid_file
    String? input_rsids
    String dbSnp155_bb_file = "http://hgdownload.soe.ucsc.edu/gbdb/hg38/snp/dbSnp155.bb"
    String output_prefix
    String docker = "shengqh/ucsctools:20250516"
  }

  command <<<

if [[ "~{input_rsids}" != "" ]]; then
  echo "~{input_rsids}" >> rsid.tmp.txt
fi

if [[ "~{input_rsid_file}" != "" ]]; then
  cat "~{input_rsid_file}" >> rsid.tmp.txt
fi

#remove rsid in case it is used as header
grep -v "rsid" rsid.tmp.txt > rsid.txt 

bigBedNamedItems -nameFile ~{dbSnp155_bb_file} rsid.txt request.tmp.bed
grep -v "_alt" request.tmp.bed > ~{output_prefix}.bed
rm -f request.tmp.bed dbSnp155.bb

  >>>
  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "5 GiB"
  }
  output {
    File output_bed = "~{output_prefix}.bed"
  }
}

# This task finds the corresponding index of a chromosome
# in a given chromosome list from an input BED file.
#
# The task is useful for genomic analyses where chromosome identification and
# indexing are required, such as for alignment, variant calling, or other
# bioinformatics operations that need to reference chromosomes consistently.
#
# Inputs:
#   - BED file containing chromosome information
#   - List of chromosomes to use as reference
#
# Outputs:
#   - Index of chromosomes from the BED file in the reference chromosome list
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

task GetChromosomeIndeciesWithVariants {
  input {
    Array[String] input_chromosomes
    Array[File] input_pvar_files
    File input_bed_file
    String docker = "shengqh/hail_gcp:20241127"
  }
  Int disk_size = ceil(size(input_pvar_files, "GB")) + 5
  command <<<
#!/bin/bash

set -e

# Create Python script
cat > get_chrom_indices.py << 'EOF'
import pandas as pd
import sys

chrom_list = "~{sep=',' input_chromosomes}".split(",")
bed_file = "~{input_bed_file}"
pvar_files = "~{sep=',' input_pvar_files}".split(",")

# Read the bed file (assuming standard BED format: chrom start end ...)
bed_df = pd.read_csv(bed_file, sep='\t', header=None)

# Group bed file entries by chromosome
bed_by_chrom = {}
for _, row in bed_df.iterrows():
    chrom = str(row[0])
    if chrom.startswith("chr"):
        chrom = chrom[3:]

    pos = int(row[1])
    if chrom not in bed_by_chrom:
        bed_by_chrom[chrom] = set()
    bed_by_chrom[chrom].add(pos)

# Check which chromosomes have matching SNVs in both bed and pvar
indices = []
for i, (chrom, pvar_file) in enumerate(zip(chrom_list, pvar_files)):
    if chrom.startswith("chr"):
        chrom = chrom[3:]
    print(f"Checking chromosome: {chrom} with pvar file: {pvar_file}")
    if chrom in bed_by_chrom:
        pos_set = bed_by_chrom[chrom]
        # Check if any SNV in this chromosome's pvar file matches positions in bed
        found_match = False
        with open(pvar_file, 'r') as f:
            for line in f:
                if line.startswith('#'):
                    continue
                fields = line.strip().split('\t', 2)
                pos = int(fields[1])
                if pos in pos_set:
                    print(f"Found match at position: {line}")
                    found_match = True
                    break
        
        if found_match:
            indices.append(i)
  
# Write indices to output file
with open("chromosomes.txt", "w") as f:
  for idx in indices:
    f.write(f"{idx}\n")

print(f"Indices of matching chromosomes: {indices}")

EOF

# Run the Python script
python3 get_chrom_indices.py 

>>>

  runtime {
    cpu: 1
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: "10 GiB"
  }

  output {
    Array[Int] chromosome_indecies = read_lines("chromosomes.txt")
  }
}

task CheckOverlapVariants {
  input {
    String chromosome
    File input_pgen_pvar
    File input_ucsc_bed
    String docker = "shengqh/hail_gcp:20241127"
  }
  Int disk_size = ceil(size([input_pgen_pvar, input_ucsc_bed], "GB")) + 5
  command <<<
#!/bin/bash

set -e

# Create Python script
cat <<EOF> get_chrom_indices.py 
import pandas as pd
import sys

bed_file = "~{input_ucsc_bed}"

pvar_chrom = "~{chromosome}"
pvar_file = "~{input_pgen_pvar}"

# Read the bed file (assuming standard BED format: chrom start end ...)
bed_df = pd.read_csv(bed_file, sep='\t', header=None, comment='#')

# Group bed file entries by chromosome
bed_by_chrom = {}
for _, row in bed_df.iterrows():
    chrom = str(row[0])
    if chrom == "#CHROM": # input is a pvar file
        continue  # Skip header line

    if chrom.startswith("chr"):
        chrom = chrom[3:]

    pos = int(row[1])
    if chrom not in bed_by_chrom:
        bed_by_chrom[chrom] = set()
    bed_by_chrom[chrom].add(pos)

if pvar_chrom.startswith("chr"):
    pvar_chrom = pvar_chrom[3:]

found_match = False
if pvar_chrom in bed_by_chrom:
  print(f"Checking chromosome: {pvar_chrom} with pvar file: {pvar_file}")
  pos_set = bed_by_chrom[pvar_chrom]

  # Check if any SNV in this chromosome's pvar file matches positions in bed
  with open(pvar_file, 'r') as f:
      for line in f:
          if line.startswith('#'):
              continue
          fields = line.strip().split('\t', 2)
          pos = int(fields[1])
          if pos in pos_set:
              print(f"Found match at position: {line}")
              found_match = True
              break
else:
  print(f"Chromosome {pvar_chrom} not found in bed file.")
        
# Write indices to output file
with open("has_match.txt", "w") as f:
  if found_match:
    f.write("true\n")
  else:
    f.write("false\n")

EOF

# Run the Python script
python3 get_chrom_indices.py 

>>>

  runtime {
    cpu: 1
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: "10 GiB"
  }

  output {
    Boolean has_variant = read_boolean("has_match.txt")
  }
}


task CheckOverlapVariantsReturnIndex {
  input {
    String chromosome
    Int chrom_index
    Int? chrom_index_none # don't set value for it.
    File input_pgen_pvar
    File input_ucsc_bed
    String docker = "shengqh/hail_gcp:20241127"
  }
  Int disk_size = ceil(size([input_pgen_pvar, input_ucsc_bed], "GB")) + 5

  command <<<
#!/bin/bash

set -e

# Create Python script
cat <<EOF> get_chrom_indices.py 
import pandas as pd
import sys

bed_file = "~{input_ucsc_bed}"

pvar_chrom = "~{chromosome}"
pvar_file = "~{input_pgen_pvar}"

# Read the bed file (assuming standard BED format: chrom start end ...)
bed_df = pd.read_csv(bed_file, sep='\t', header=None, comment='#')

# Group bed file entries by chromosome
bed_by_chrom = {}
for _, row in bed_df.iterrows():
    chrom = str(row[0])
    if chrom == "#CHROM": # input is a pvar file
        continue  # Skip header line

    if chrom.startswith("chr"):
        chrom = chrom[3:]

    pos = int(row[1])
    if chrom not in bed_by_chrom:
        bed_by_chrom[chrom] = set()
    bed_by_chrom[chrom].add(pos)

if pvar_chrom.startswith("chr"):
    pvar_chrom = pvar_chrom[3:]

found_match = False
if pvar_chrom in bed_by_chrom:
  print(f"Checking chromosome: {pvar_chrom} with pvar file: {pvar_file}")
  pos_set = bed_by_chrom[pvar_chrom]

  # Check if any SNV in this chromosome's pvar file matches positions in bed
  with open(pvar_file, 'r') as f:
      for line in f:
          if line.startswith('#'):
              continue
          fields = line.strip().split('\t', 2)
          pos = int(fields[1])
          if pos in pos_set:
              print(f"Found match at position: {line}")
              found_match = True
              break
else:
  print(f"Chromosome {pvar_chrom} not found in bed file.")
        
# Write indices to output file
with open("has_match.txt", "w") as f:
  if found_match:
    f.write("true\n")
  else:
    f.write("false\n")

EOF

# Run the Python script
python3 get_chrom_indices.py 

>>>

  runtime {
    cpu: 1
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: "10 GiB"
  }

  output {
    Int? res_chrom_index = if read_boolean("has_match.txt") then chrom_index else chrom_index_none
  }
}

task CheckOverlapVariantsBetweenPvarFilesByIDAndReturnIndex {
  input {
    Int chrom_index
    Int? chrom_index_none # don't set value for it.

    File query_pgen_pvar
    File target_pgen_pvar

    String docker = "shengqh/hail_gcp:20241127"
  }
  Int disk_size = ceil(size([query_pgen_pvar, target_pgen_pvar], "GB")) + 5
  command <<<
#!/bin/bash

set -e

# Create Python script
cat <<EOF> check_overlap.py 
import pandas as pd
import sys

query_pvar_file = "~{query_pgen_pvar}"
target_pvar_file = "~{target_pgen_pvar}"

query_ids = set()
with open(query_pvar_file, 'r') as f:
    for line in f:
        if line.startswith('#'):
            continue
        fields = line.strip().split('\t', 3)
        id = fields[2]
        query_ids.add(id)

found_match = False
with open(target_pvar_file, 'r') as f:
    for line in f:
        if line.startswith('#'):
            continue
        fields = line.strip().split('\t', 3)
        id = fields[2]
        if id in query_ids:
            print(f"Found match at position: {line}")
            found_match = True
            break

with open("has_match.txt", "w") as f:
  if found_match:
    f.write("true\n")
  else:
    f.write("false\n")

EOF

# Run the Python script
python3 get_chrom_indices.py 

>>>

  runtime {
    cpu: 1
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: "10 GiB"
  }

  output {
    Int? res_chrom_index = if read_boolean("has_match.txt") then chrom_index else chrom_index_none
  }
}

task CheckOverlapVariantsByID {
  input {
    File input_pgen_pvar
    File input_id_file
    Int input_id_col # 3 for bed file and 2 for pvar file
    String docker = "shengqh/hail_gcp:20241127"
  }
  Int disk_size = ceil(size([input_pgen_pvar, input_id_file], "GB")) + 5
  command <<<
#!/bin/bash

set -e

# Create Python script
cat <<EOF> find_id.py 
import sys
import logging

logger = logging.getLogger("find_id")
logging.basicConfig(level=logging.INFO)

id_file = "~{input_id_file}"
id_col = ~{input_id_col}

pvar_file = "~{input_pgen_pvar}"

logger.info(f"Reading IDs from column {id_col} of file {id_file}")
id_set = set()
with open(id_file, 'r') as f:
    for line in f:
        if line.startswith('#'):
            continue
        fields = line.strip().split('\t', id_col + 1)
        id = fields[id_col]
        id_set.add(id)

logger.info(f"Total IDs read: {len(id_set)}")

logger.info(f"Checking for matches in pvar file: {pvar_file}")
found_match = False
with open(pvar_file, 'r') as f:
    for line in f:
        if line.startswith('#'):
            continue
        fields = line.strip().split('\t', 4)
        id = fields[2]
        if id in id_set:
            print(f"Found match: {line}")
            found_match = True
            break
        
logger.info(f"Match found: {found_match}")

with open("has_match.txt", "w") as f:
  if found_match:
    f.write("true\n")
  else:
    f.write("false\n")

logger.info("Finished checking for matches.")

EOF

# Run the Python script
python3 find_id.py 

>>>

  runtime {
    cpu: 1
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: "10 GiB"
  }

  output {
    Boolean has_variant = read_boolean("has_match.txt")
  }
}


task VcfIndexAndInfo {
  input{
    File input_vcf

    String bcftools_docker = "shengqh/samtools_bcftools_tabix:v1.21"

    Int cpu = 4
    Int machine_mem_gb = 4
    Int addtional_disk_space_gb = 5
  }

  Int disk_size = ceil(size(input_vcf, "GB")) + addtional_disk_space_gb

  String target_vcf = basename(input_vcf)
  String output_sample_file = "samples.txt"

  command <<<

ln -s ~{input_vcf} ~{target_vcf}

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
    docker: bcftools_docker
    preemptible: 1
    memory: machine_mem_gb + " GB"
    disks: "local-disk " + disk_size + " HDD"
  }

  output{
    File output_vcf_index = "~{target_vcf}.tbi"
    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")
  }
}

task FilterVariantsForModelling {
  input {
    File phenoFile
    String phenoColList
    File covarFile
    String covarColList
    String? catCovarColList

    String filter_model_plink2_option_no_mac

    File model_pgen_file
    File model_psam_file
    File model_pvar_file

    String output_prefix

    String docker = "shengqh/plink_1.9_2.0:20260526"
  }
  
  Int disk_size = ceil(size([model_pgen_file, model_psam_file, model_pvar_file, phenoFile, covarFile], "GB")) + 5

  command <<<
#!/bin/bash

set -e

cat <<EOF> get_variants.py 
#! /usr/bin/env python3
import re

phenoFile = "~{phenoFile}"
covarFile = "~{covarFile}"
phenoColList = "~{phenoColList}"
covarColList = "~{covarColList}"
catCovarColList = "~{catCovarColList}"
output_prefix = "~{output_prefix}"

fn_samlist_non_na = "samplelist.psam"

phenoColList_l = phenoColList.split(',')
covarColList_l = covarColList.split(',') if covarColList != "" else []
catCovarColList_l = catCovarColList.split(',') if catCovarColList != "" else []

if covarFile != phenoFile:
    id_selected = set()
    with open(covarFile) as f:
        header = f.readline().rstrip('\n\r').split('\t')
        idIdx = header.index('IID')
        covarColIdx = [header.index(x) for x in covarColList_l]
        catCovarColIdx = [header.index(x) for x in catCovarColList_l]
        all_covarColIdx = covarColIdx + catCovarColIdx
        for line in f:
            line = line.rstrip('\n\r').split('\t')
            if all(line[i].upper() not in {'', 'NA', 'NAN'} for i in all_covarColIdx):
                id_selected.add(line[idIdx])
else:
    id_selected = None

with open(phenoFile) as f, open(fn_samlist_non_na, 'w') as out:
    header = f.readline().rstrip('\n\r').split('\t')
    phenoColIdx = [header.index(x) for x in phenoColList_l]
    idIdx = [header.index(x) for x in ['FID', 'IID']]
    print('#FID\tIID', file=out)
    n_kept = 0
    n_total = 0
    for line in f:
        n_total += 1
        line = line.rstrip('\n\r').split('\t')
        if (id_selected is None or line[idIdx[1]] in id_selected) and all(line[i].upper() not in {'', 'NA', 'NAN'} for i in phenoColIdx):
            print('\t'.join([line[i] for i in idIdx]), file=out)
            n_kept += 1

print(f'total sample input = {n_total}, sample with non-missing phenotype and covariates = {n_kept}')
min_ac_in_use = 2 if n_kept < 200 else 5 if n_kept < 500 else 10

# Plink2 QC command before regenie step 1
plink_qc_cmd = f"""
plink2 \\
    --pgen ~{model_pgen_file} \\
    --psam ~{model_psam_file} \\
    --pvar ~{model_pvar_file} \\
    --keep {fn_samlist_non_na} \\
    ~{filter_model_plink2_option_no_mac} \\
    --mac {min_ac_in_use} \\
    --write-snplist  --no-id-header \\
    --out {output_prefix}.model
"""

with open("filter.sh", "w") as file:
    file.writelines(plink_qc_cmd)

EOF

python3 get_variants.py 

bash filter.sh

>>>

  runtime {
    cpu: 1
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: "10 GiB"
  }

  output {
    File output_snp_list = "~{output_prefix}.model.snplist"
  }
}
