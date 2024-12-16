version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCReadsInBed {
  input {
    File cram
    File cram_index
    File reference_fasta
    File reference_fasta_fai
    File bed_file
    String output_prefix

    String? billing_gcp_project_id
    String? target_gcp_folder    
  }

  call ReadsInBed {
    input:
      cram = cram,
      cram_index = cram_index,
      reference_fasta = reference_fasta,
      reference_fasta_fai = reference_fasta_fai,
      bed_file = bed_file,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    String gcs_output_dir = select_first([target_gcp_folder])

    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = ReadsInBed.reads_file,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = gcs_output_dir
    }
  }

  output {
    File reads_file = select_first([CopyFile.output_file, ReadsInBed.reads_file])
  }
}

task ReadsInBed {
  input {
    File cram
    File cram_index
    File reference_fasta
    File reference_fasta_fai
    File bed_file
    String output_prefix
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
parser.add_argument('-o', '--output', action='store', nargs='?', help="Output file prefix", required=NOT_DEBUG)

args = parser.parse_args()

bed_file=args.input
cram_file=args.cram
reference_file=args.reference_fasta
output_prefix=args.output

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

def total_reads(reads_file):
  with open(reads_file, "rt") as fin:
    n_total_count = int(fin.readline().strip())
  return n_total_count

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

logger.info(f"Counting total reads of {cram_file} ...")
total_reads = total_reads("total_reads.txt")
logger.info(f"Total reads : {total_reads}")

logger.info(f"Counting reads of {cram_file} in {bed_file} ...")
interval_reads = count_reads_in_intervals(cram_file, bed_file)
logger.info(f"Interval reads : {interval_reads}")

with open(f"{output_prefix}.reads.txt", "w") as f:
  f.write("File\tTotalReads\tReadInBed\n")
  f.write(f"{os.path.basename(cram_file)}\t{total_reads}\t{interval_reads}\n")

EOF

python3 get_reads_in_bed.py -i ~{bed_file} -c ~{cram} -r ~{reference_fasta} -o ~{output_prefix}

>>>

  runtime {
    cpu: 1
    docker: "shengqh/cqs_rnaseq:20240813"
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: "10 GiB"
  }

  output {
    File reads_file = "~{output_prefix}.reads.txt"
  }
}

