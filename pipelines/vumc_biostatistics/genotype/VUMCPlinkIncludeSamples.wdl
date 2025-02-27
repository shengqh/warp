version 1.0

import "./Utils.wdl" as Utils

workflow VUMCPlinkIncludeSamples {
  input {
    File source_bed
    File source_bim
    File source_fam

    #plink2 default is MT
    String output_chr = "MT"

    File include_samples
    String target_prefix

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"

    File? id_map_file

    String? project_id
    String? target_bucket
  }

  if(defined(id_map_file)){
    call ReplaceIdFam{
      input:
        source_fam = source_fam,
        id_map_file = select_first([id_map_file])
    }
  }

  call CreateIncludeFam {
    input:
      source_fam = select_first([ReplaceIdFam.replaced_fam, source_fam]),
      include_samples = include_samples,
  }

  call PlinkIncludeSamples {
    input:
      source_bed = source_bed,
      source_bim = source_bim,
      source_fam = select_first([ReplaceIdFam.replaced_fam, source_fam]),
      output_chr = output_chr,
      keep_id_fam = CreateIncludeFam.keep_id_fam,
      target_prefix = target_prefix,
      docker = docker
  }

  if(defined(target_bucket)){
    call Utils.MoveOrCopyPlinkFile as CopyFile {
      input:
        source_bed = PlinkIncludeSamples.output_bed,
        source_bim = PlinkIncludeSamples.output_bim,
        source_fam = PlinkIncludeSamples.output_fam,
        is_move_file = false,
        project_id = project_id,
        target_bucket = select_first([target_bucket])
    }
  }

  output {
    File output_bed = select_first([CopyFile.output_bed, PlinkIncludeSamples.output_bed])
    File output_bim = select_first([CopyFile.output_bim, PlinkIncludeSamples.output_bim])
    File output_fam = select_first([CopyFile.output_fam, PlinkIncludeSamples.output_fam])
  }
}

task ReplaceIdFam {
  input {
    File source_fam
    File id_map_file
  }

  command <<<

python3 <<CODE

import gzip
import io

if "~{id_map_file}".endswith(".gz"):
  fin = gzip.open("~{id_map_file}", "rt")
else:
  fin = open("~{id_map_file}", "rt")

with fin:
  id_map = {}
  for line in fin:
    parts = line.strip().split('\t')
    id_map[parts[1]] = parts[0]

with open("~{source_fam}", "rt") as fin:
  with open("replaced.fam", "wt") as fout:
    for line in fin:
      parts = line.strip().split(' ')
      if parts[1] in id_map:
        grid = id_map[parts[1]]
        parts[1] = grid

      newline = ' '.join(parts)
      fout.write(f"{newline}\n")

CODE

>>>

  runtime {
    docker: "us.gcr.io/broad-dsp-gcr-public/base/python:3.9-debian"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    File replaced_fam = "replaced.fam"
  }
}

task CreateIncludeFam {
  input {
    File source_fam
    File include_samples
  }

  command <<<

python3 <<CODE

import os

grids = set(line.strip() for line in open("~{include_samples}", "rt"))
with open("keep.id.fam", "wt") as fout:
  with open("~{source_fam}", "rt") as fin:
    for line in fin:
      if line.split()[1] in grids:
        fout.write(line)
CODE

echo "Number of samples to keep:"
wc -l keep.id.fam

>>>

  runtime {
    docker: "us.gcr.io/broad-dsp-gcr-public/base/python:3.9-debian"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    File keep_id_fam = "keep.id.fam"
  }
}

task PlinkIncludeSamples {
  input {
    File source_bed
    File source_bim
    File source_fam

    File keep_id_fam
    
    String output_chr = "MT"

    String target_prefix
    
    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
    Int memory_gb = 20
  }

  Int disk_size = ceil(size([source_bed, source_bim, source_fam], "GB")  * 2) + 20

  String new_bed = target_prefix + ".bed"
  String new_bim = target_prefix + ".bim"
  String new_fam = target_prefix + ".fam"

  command <<<

plink2 \
  --bed ~{source_bed} \
  --bim ~{source_bim} \
  --fam ~{source_fam} \
  --keep ~{keep_id_fam} \
  --make-bed \
  --output-chr ~{output_chr} \
  --out ~{target_prefix}

>>>

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_bed = new_bed
    File output_bim = new_bim
    File output_fam = new_fam
  }
}
