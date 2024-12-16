version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCReadsInBed {
  input {
    File cram
    File cram_index
    File reference_fasta
    File reference_fasta_fai
    File bed_file
  }

  call ReadsInBed {
    input:
      cram = cram,
      cram_index = cram_index,
      reference_fasta = reference_fasta,
      reference_fasta_fai = reference_fasta_fai,
      bed_file = bed_file
  }

  output {
    Int total_reads = ReadsInBed.total_reads
    Int interval_reads = ReadsInBed.interval_reads
  }
}

task ReadsInBed {
  input {
    File cram
    File cram_index
    File reference_fasta
    File reference_fasta_fai
    File bed_file
  }

  Int disk_size = ceil(size([cram, cram_index, bed_file, reference_fasta],"GB")) + 10

  command <<<

samtools view -T ~{reference_fasta} -c ~{cram} > total_reads.txt

cat <<EOF > get_reads_in_bed.py

import argparse
import logging
import pysam
import os

NOT_DEBUG=True

parser = argparse.ArgumentParser(description="get_reads_in_bed",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('-i', '--input', action='store', nargs='?', help='Input bed file', required=NOT_DEBUG)
parser.add_argument('-c', '--cram', action='store', nargs='?', help='Input cram file', required=NOT_DEBUG)
parser.add_argument('-r', '--reference_fasta', action='store', nargs='?', help="Input reference fasta file", required=NOT_DEBUG)

args = parser.parse_args()

bed_file=args.input
cram_file=args.cram
reference_file=args.reference_fasta

logger = logging.getLogger('get_reads_in_bed')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)-8s - %(message)s')

def read_bed_file(bed_file):
  intervals = []
  with open(bed_file, 'r') as f:
    for line in f:
      if line.startswith('track') or line.startswith('#'):
        continue
      parts = line.strip().split()
      chrom, start, end = parts[0], int(parts[1]), int(parts[2])
      intervals.append((chrom, start, end))
  return intervals

def count_reads_in_intervals(cram_file, bed_file):
  logger.info(f"reading {bed_file} ...")
  intervals = read_bed_file(bed_file)
  logger.info(f"total {len(intervals)} intervals ...")
  n_interval_count = 0
  with pysam.AlignmentFile(cram_file, "rc", reference_filename=reference_file) as cram:
    ninterval = 0
    for interval in intervals:
      ninterval = ninterval + 1
      if ninterval % 100 == 0:
        logger.info(f"processing interval {ninterval} / {len(intervals)} ...")
      chrom, start, end = interval
      for read in cram.fetch(chrom, start, end):
        n_interval_count += 1
  return n_interval_count

logger.info(f"Counting reads of {cram_file} in {bed_file} ...")
interval_reads = count_reads_in_intervals(cram_file, bed_file)
logger.info(f"Interval reads : {interval_reads}")

with open("interval_reads.txt", "wt") as fout:
  fout.write(str(interval_reads))

EOF

python3 get_reads_in_bed.py -i ~{bed_file} -c ~{cram} -r ~{reference_fasta}

>>>

  runtime {
    cpu: 1
    docker: "shengqh/cqs_rnaseq:20240813"
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: "10 GiB"
  }

  output {
    Int total_reads = read_int("total_reads.txt")
    Int interval_reads = read_int("interval_reads.txt")
  }
}

