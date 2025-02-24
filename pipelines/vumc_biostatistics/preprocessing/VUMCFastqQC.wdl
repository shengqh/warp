version 1.0

import "../../../tasks/broad/Utilities.wdl" as Utils

# WORKFLOW DEFINITION
workflow VUMCFastqQC {
  input {
    Array[File] fastq_1
    Array[File] fastq_2
    String sample_name
    Int preemptible_tries = 3
  }

  scatter (idx in range(length(fastq_1))) {
    File new_fastq_1 = fastq_1[idx]
    File new_fastq_2 = fastq_2[idx]

    Float fastq_size = size(new_fastq_1, "GiB") + size(new_fastq_2, "GiB")
  }

  # Sum the read group bam sizes to approximate the aggregated bam size
  call Utils.SumFloats as SumFloats {
    input:
      sizes = fastq_size,
      preemptible_tries = preemptible_tries
  }

  call ValidatePairendFastq {
    input:
      fastq_1 = fastq_1,
      fastq_2 = fastq_2,
      total_size = SumFloats.total_size,
      sample_name = sample_name,
      preemptible_tries = preemptible_tries
  }

  # Outputs that will be retained when execution is complete
  output {
    File qc_file = ValidatePairendFastq.qc_file
    Int qc_failed = ValidatePairendFastq.qc_failed
    Int qc_gzip_failed = ValidatePairendFastq.qc_gzip_failed
  }
}

task ValidatePairendFastq {
  input {
    Array[File] fastq_1
    Array[File] fastq_2
    Int preemptible_tries
    String sample_name
    Float total_size
  }

  Int disk_size = ceil(total_size + 10)
  String qc_file = sample_name + ".qc.txt"
  String res_file = sample_name + ".res.txt"
  String gzip_file = sample_name + ".gzip.txt"

  command <<<

    echo "0" > ~{gzip_file}
    for f1 in ~{sep=" " fastq_1} 
    do
      gzip -t $f1
      status=$?
      if [[ $status != 0 ]]; then
        echo "$status" > ~{gzip_file}
        break
      fi
    done

    for f2 in ~{sep=" " fastq_2} 
    do
      gzip -t $f2
      status=$?
      if [[ $status != 0 ]]; then
        echo "$status" > ~{gzip_file}
        break
      fi
    done


    python3 <<CODE

import argparse
import logging
import gzip
import os
import sys

def validate2(logger, input1str, input2str, output):
  input1 = input1str.split(',')
  input2 = input2str.split(',')

  error_files = []
  error_msgs = []
  total_read_count = 0
  idx = 0
  while idx < len(input1):
    read1 = input1[idx]
    read2 = input2[idx]
    idx += 1

    logger.info("Validating %s ..." % read1)
    logger.info("Validating %s ..." % read2)
    read_count = 0
    error_msg = None
    try:
      with gzip.open(read1, "rt") as fin1, gzip.open(read2, "rt") as fin2:
        while(True):
          line1 = fin1.readline()
          line2 = fin2.readline()
            
          if not line1:
            if not line2:
              break
            else:
              error_msg = "%s end but %s not" % (read1, read2)
              break
          if not line2:
            error_msg = "%s end but %s not" % (read2, read1)
            break
          
          if not line1.startswith('@'):
            error_msg = "query %s not starts with @ in %s" % (line1, read1)
            break

          if not line2.startswith('@'):
            error_msg = "query %s not starts with @ in %s" % (line2, read2)
            break

          read_count += 1
          if read_count % 1000000 == 0:
            logger.info("%s" % read_count)
            #break

          line1 = line1.rstrip()
          line2 = line2.rstrip()

          qname1 = line1.split(' ', 1)[0]
          qname2 = line2.split(' ', 1)[0]
          if qname1 != qname2:
            if qname1.endswith("/1"):
              qname1 = qname1[0:len(qname1) - 2]
              qname2 = qname2[0:len(qname2) - 2]
              if qname1 != qname2:
                error_msg = "query name not equals: %s , %s in %s and %s" % (qname1, qname2, read1, read2)
                break
            else:
              error_msg = "query name not equals: %s , %s in %s and %s" % (qname1, qname2, read1, read2)
              break

          seq1 = fin1.readline()
          mid1 = fin1.readline()
          score1 = fin1.readline()

          if not seq1 or not mid1 or not score1:
            error_msg = "unexpected end for query %s in %s" % (qname1, read1)
            break

          if len(seq1) != len(score1):
            error_msg = "sequence length not equals to score length for query %s in %s\n  seq  :%s\n  score:%s\n" % (qname1, read1, seq1, score1)
            break

          seq2 = fin2.readline()
          mid2 = fin2.readline()
          score2 = fin2.readline()

          if not seq2 or not mid2 or not score2:
            error_msg = "unexpected end for query %s in %s" % (qname2, read2)
            break

          if len(seq2) != len(score2):
            error_msg = "sequence length not equals to score length for query %s in %s\n  seq  :%s\n  score:%s\n" % (qname2, read2, seq2, score2)
            break
    except OSError as e:
      error_msg = "I/O error({0}): {1}".format(e.errno, e.strerror)
    except Exception as e:
      error_msg = "Unexpected error: {0}".format(e)

    total_read_count += read_count

    if error_msg != None:
      logger.error(error_msg)
      error_msgs.append(error_msg)
      error_files.append(read1)
      error_files.append(read2)

  if os.path.exists(output):
    os.remove(output)

  if len(error_msgs) > 0:
    logger.error("failed : all error msgs:")
    with open(output, "wt") as fout:
      for error_msg in error_msgs:
        logger.error(error_msg)
        fout.write("ERROR: %s\n" % error_msg)

      fout.write("\n")
      for error_file in error_files:
        fout.write(error_file + "\n")
      
    return(1)
  else:
    logger.info("succeed")
    with open(output, "wt") as fout:
      fout.write("READ\t%d\n" % total_read_count)
    return(0)

logger = logging.getLogger('fastq_validator')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)-8s - %(message)s')
  
res = validate2(logger, "~{sep="," fastq_1}", "~{sep="," fastq_2}", "~{qc_file}")
with open("~{res_file}", "wt") as fout:
  fout.write(f"{res}")

CODE

exit 0

>>>

  runtime {
    docker: "us.gcr.io/broad-dsp-gcr-public/base/python:3.9-debian"
    preemptible: preemptible_tries
    disks: "local-disk " + disk_size + " HDD"
    memory: "2 GiB"
  }
  output {
    File qc_file = glob("~{qc_file}*")[0]
    Int qc_failed = read_int("~{res_file}")
    Int qc_gzip_failed = read_int("~{gzip_file}")
  }
}