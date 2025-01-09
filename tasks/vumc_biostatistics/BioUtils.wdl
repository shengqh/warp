version 1.0

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

task QCFilterPgen {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    String output_prefix

    File? filter_psam_file
    String qc_filter_option

    Int memory_gb = 20
    Int cpu = 8
    Float disk_size_factor = 1.5

    String docker = "shengqh/plink_1.9_2.0:20241129"
  }

  Int disk_size = ceil(size([input_pgen, input_pvar, input_psam], "GB")  * disk_size_factor) + 20

  command <<<

plink2 \
  --pgen ~{input_pgen} \
  --pvar ~{input_pvar} \
  --psam ~{input_psam} \
  ~{qc_filter_option} \
  --threads ~{cpu} "~{sep='--keep ' filter_psam_file}" \
  --make-pgen \
  --out ~{output_prefix}

grep -v "^#" ~{output_prefix}.psam | wc -l | cut -d ' ' -f 1 > num_samples.txt
grep -v "^#" ~{output_prefix}.pvar | wc -l | cut -d ' ' -f 1 > num_variants.txt

>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_pgen = "~{output_prefix}.pgen"
    File output_pvar = "~{output_prefix}.pvar"
    File output_psam = "~{output_prefix}.psam"
    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")
  }
}

task QCFilterAndPrunePgen {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    String output_prefix

    String qc_filter_option
    String indep_pairwise_option

    Int max_variants = 1000000

    Int memory_gb = 20
    Int cpu = 8
    Float disk_size_factor = 2

    String docker = "shengqh/plink_1.9_2.0:20241129"
  }

  Int disk_size = ceil(size([input_pgen, input_pvar, input_psam], "GB")  * disk_size_factor) + 20

  command <<<

awk 'BEGIN {FS=OFS="\t"} NR==1 {print; next} {if ($3 == ".") $3 = $1 ":" $2} 1' ~{input_pvar} > id.pvar

echo "Running QC on the input dataset"
plink2 \
  --pgen ~{input_pgen} \
  --pvar id.pvar \
  --psam ~{input_psam} \
  ~{qc_filter_option} \
  --threads ~{cpu} \
  --make-pgen \
  --out qc_pass

qc_variants=$(grep -v "^#" qc_pass.pvar | wc -l | cut -d ' ' -f 1)
echo "The number of variants after QC is $qc_variants"

echo "Running Prune on the qc-ed dataset"
plink2 \
  --pfile qc_pass \
  ~{indep_pairwise_option} \
  --threads ~{cpu} \
  --out qc_pass

prune_variants=$(wc -l qc_pass.prune.in | cut -d ' ' -f 1)
echo "The number of variants after Prune is $prune_variants"

if [[ $prune_variants -gt ~{max_variants} ]]; then
  echo "The number of variants after QC and Prune is $prune_variants, which is greater than the maximum allowed number of variants (~{max_variants}). We will generate pruned dataset first."

  cat <<EOF > filter.py
import pandas as pd

# Read the file into a DataFrame
df = pd.read_csv("qc_pass.prune.in", sep='\t', header=None)
print(df.head())

# Randomly select ~{max_variants} rows
sample_df = df.sample(n=~{max_variants}, random_state=20241129).sort_index()
print(sample_df.head())

sample_df.to_csv("sampled.snplist", sep='\t', index=False, header=False)

EOF

  python3 filter.py
else
  mv qc_pass.prune.in sampled.snplist
fi

echo "Writing Prune dataset"
plink2 \
  --pfile qc_pass \
  --extract sampled.snplist \
  --threads ~{cpu} \
  --make-pgen \
  --out ~{output_prefix}

grep -v "^#" ~{output_prefix}.psam | wc -l | cut -d ' ' -f 1 > num_samples.txt
grep -v "^#" ~{output_prefix}.pvar | wc -l | cut -d ' ' -f 1 > num_variants.txt

rm -f id.pvar qc_pass.* sampled.snplist

>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_pgen = "~{output_prefix}.pgen"
    File output_pvar = "~{output_prefix}.pvar"
    File output_psam = "~{output_prefix}.psam"

    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")    
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

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
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