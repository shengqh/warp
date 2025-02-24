version 1.0

import "./MyGetChromosomes.wdl" as MyGetChromosomes

workflow CombineGvsChromosome {

  String pipeline_version = "1.0.0"

  input {
    Array[File] intervals_files
    Array[String] vcf_files
  }

  call GetChromosomes {
    input:
      intervals_files = intervals_files,
      vcf_files = vcf_files
  }

  call MyGetChromosomes.MyGetChromosomes {
    input:
      unique_chromosomes = GetChromosomes.unique_chromosomes,
      chromosomes = GetChromosomes.chromosomes,
      ordered_vcf_files = GetChromosomes.ordered_vcf_files
  }

  Array[Pair[String,Array[File]]] ans = MyGetChromosomes.chrom_vcf_map

  scatter (cv in ans) {
    String chrom = cv.left
    Array[File] vcf_files = cv.right
    call CombineGvsChromosome {
      input:
        chromosome = chrom,
        vcf_files = vcf_files
    }
  }

  output {
    Array[File] chromosome_vcfs = CombineGvsChromosome.output_vcf
    Array[File] chromosome_vcfs_tbi = CombineGvsChromosome.output_vcf_tbi
    # Array[String] chromosomes = chrom
    # Array[File] chromosome_vcf_list = list_file
  }
}


task GetChromosomes {
  input {
    Array[File] intervals_files
    Array[String] vcf_files
    Int preemptible_tries=3
  }

  Int disk_size = ceil(size(intervals_files, "GB")) + 2

  String chrom_file = "chromosome.txt"
  String chrom_array_file = "chromosome_array.txt"
  String vcf_array_file = "vcf_array.txt"

  command <<<

    python3 <<CODE

import pandas as pd
import os
import itertools
import json

from collections import OrderedDict

interval_files = "~{sep=',' intervals_files}".split(',')
vcf_files = "~{sep=',' vcf_files}".split(',')

df = [[pd.read_csv(f, sep='\t', header=None, comment='@').iloc[0][0], os.path.basename(f)] for f in interval_files]

key_func = lambda x: x[0]
chrom_map = OrderedDict()

for key, group in itertools.groupby(df, key_func):
  chrom_map[key] = [g[1].replace(".interval_list","") for g in group]

unique_chromosomes = [chrom_map.keys()]
print(unique_chromosomes)

vcf_map = {os.path.basename(f):f for f in vcf_files}

with open("~{chrom_array_file}", 'wt') as fchrom:
  with open("~{vcf_array_file}", 'wt') as fvcf:
    for chrom in chrom_map:
      for vcf_name in chrom_map[chrom]:
        fchrom.write(chrom + "\n")
        vcf_file = vcf_map[vcf_name]
        fvcf.write(vcf_file + "\n")

chroms = sorted(chrom_map.keys())
with open("~{chrom_file}", 'wt') as fchrom:
  for chrom in chroms:
    fchrom.write(chrom + "\n")

CODE

>>>

  runtime {
    docker: "shengqh/cqs_scrnaseq:20230721"
    preemptible: preemptible_tries
    disks: "local-disk " + disk_size + " HDD"
    memory: "2 GiB"
  }
  output {
    Array[String] unique_chromosomes = read_lines(chrom_file)
    Array[String] chromosomes = read_lines(chrom_array_file)
    Array[String] ordered_vcf_files = read_lines(vcf_array_file)
  }
}


task GetChromosomeVcfMap {
  input {
    Array[File] intervals_files
    Array[String] vcf_files
    Int preemptible_tries=3
  }

  Int disk_size = ceil(size(intervals_files, "GB")) + 2

  command <<<

    python3 <<CODE

import pandas as pd
import os
import itertools
import json

from collections import OrderedDict

interval_files = "~{sep=',' intervals_files}".split(',')
vcf_files = "~{sep=',' vcf_files}".split(',')

df = [[pd.read_csv(f, sep='\t', header=None, comment='@').iloc[0][0], os.path.basename(f)] for f in interval_files]

key_func = lambda x: x[0]
chrom_map = OrderedDict()

for key, group in itertools.groupby(df, key_func):
  chrom_map[key] = [g[1].replace(".interval_list","") for g in group]

print(chrom_map.keys())

vcf_map = {os.path.basename(f):f for f in vcf_files}

result = {}
for chrom in chrom_map:
  list_file = chrom + ".txt"
  result[chrom] = list_file
  with open(list_file, 'wt') as flist:
    for vcf_name in chrom_map[chrom]:
      vcf_file = vcf_map[vcf_name]
      flist.write(vcf_file + "\n")

with open('vcf_map.json', 'w') as fp:
  json.dump(result, fp)

CODE

exit 0

>>>

  runtime {
    docker: "shengqh/cqs_scrnaseq:20230721"
    preemptible: preemptible_tries
    disks: "local-disk " + disk_size + " HDD"
    memory: "2 GiB"
  }
  output {
    Array[Pair[String, File]] chrom_vcf_array = read_json("vcf_map.json")
  }
}

task CombineGvsChromosome {
  input {
    String chromosome
    Array[File] vcf_files

    String docker = "shengqh/cqs_exomeseq:20220719"
    Int preemptible_tries=3
  }

  Int disk_size = ceil(size(vcf_files, "GB") * 2.5) + 2
  String chrom_vcf = chromosome + ".vcf.gz"
  String chrom_vcf_tbi = chromosome + ".vcf.gz.tbi"

  command <<<

bcftools concat -o ~{chrom_vcf} -O z ~{sep=' ' vcf_files}

tabix ~{chrom_vcf}

exit 0

>>>

  runtime {
    docker: docker
    preemptible: preemptible_tries
    disks: "local-disk " + disk_size + " HDD"
    memory: "2 GiB"
  }
  output {
    File output_vcf ="~{chrom_vcf}"
    File output_vcf_tbi = "~{chrom_vcf_tbi}"
  }
}