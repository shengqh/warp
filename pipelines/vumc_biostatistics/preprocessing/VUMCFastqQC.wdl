version 1.0

## VUMC FASTQ QC
##
## Validates paired-end gzipped FASTQ files for a single sample.
## Developed by VUMC Biostatistics for sequencing data quality control.
##
## ### Workflow Purpose:
## Detects corrupted or malformed paired-end FASTQ files before they are used by downstream
## pipelines, and records the reason for any failure in the QC file.
##
## ### Workflow Steps:
## 1. **ValidatePairendFastq (gzip check)**: Tests the integrity of every fastq_1 and fastq_2 file with `gzip -t`,
##    and records each failure message in the QC file.
## 2. **ValidatePairendFastq (FASTQ content check)**: Runs only if all gzip tests pass. Checks that the
##    fastq_1 and fastq_2 files are equal in number and pair up, that each record starts with `@`,
##    that read names match between mates, that sequence and quality lengths agree, and that no
##    file ends early. Counts reads when validation succeeds.
## 3. **ValidatePairendFastq (MD5)**: Runs only if all checks pass. Calculates the md5sum of every
##    fastq_1 and fastq_2 file.
##
## ### Inputs:
## - fastq_1: Array of gzipped FASTQ files for read 1.
## - fastq_2: Array of gzipped FASTQ files for read 2, in the same order as fastq_1.
## - sample_name: Sample name, used as the prefix of the output files.
## - preemptible_tries: Number of preemptible attempts for the validation task (default 3).
##
## ### Outputs:
## - qc_file: Text file named `<sample_name>.qc.txt`. Contains `READ<TAB><total read count>` on
##   success, or `ERROR:` lines describing each failure (plus the affected files for content errors).
## - qc_failed: 0 if all checks passed, 1 if the gzip test or the content validation failed.
## - qc_gzip_failed: 0 if all files passed the gzip test, otherwise the exit code of the first failed test.
## - fastq1_md5: One `md5sum` output line (`<md5>  <file name>`) per fastq_1 file, in input order. Empty if any check failed.
## - fastq2_md5: One `md5sum` output line (`<md5>  <file name>`) per fastq_2 file, in input order. Empty if any check failed.
##
## ### Notes:
## - A failed validation does not fail the workflow; check qc_failed and qc_gzip_failed.
## - The content check is skipped when any gzip test fails; MD5 is calculated only when no error occurred.

workflow VUMCFastqQC {
  input {
    Array[File] fastq_1
    Array[File] fastq_2
    String sample_name
    Int preemptible_tries = 3
  }

  call ValidatePairendFastq {
    input:
      fastq_1 = fastq_1,
      fastq_2 = fastq_2,
      sample_name = sample_name,
      preemptible_tries = preemptible_tries
  }

  # Outputs that will be retained when execution is complete
  output {
    File qc_file = ValidatePairendFastq.qc_file
    Int qc_failed = ValidatePairendFastq.qc_failed
    Int qc_gzip_failed = ValidatePairendFastq.qc_gzip_failed
    Array[String] fastq1_md5 = ValidatePairendFastq.fastq1_md5
    Array[String] fastq2_md5 = ValidatePairendFastq.fastq2_md5
  }
}

task ValidatePairendFastq {
  input {
    Array[File] fastq_1
    Array[File] fastq_2
    String sample_name
    Int preemptible_tries
  }

  Float total_size = size(fastq_1, "GiB") + size(fastq_2, "GiB")
  Int disk_size = ceil(total_size + 10)
  String qc_file_name = sample_name + ".qc.txt"
  String res_file = sample_name + ".res.txt"
  String gzip_file = sample_name + ".gzip.txt"
  String fastq1_md5_file = sample_name + ".fastq1.md5"
  String fastq2_md5_file = sample_name + ".fastq2.md5"

  command <<<
    : > ~{fastq1_md5_file}
    : > ~{fastq2_md5_file}

    gzip_status=0
    : > ~{qc_file_name}

    for fastq in ~{sep=" " fastq_1} ~{sep=" " fastq_2}
    do
      gzip_msg=$(gzip -t "$fastq" 2>&1)
      status=$?
      if [[ $status != 0 ]]; then
        if [[ $gzip_status == 0 ]]; then
          gzip_status=$status
        fi
        echo "ERROR: gzip test failed for $fastq (exit code $status): $gzip_msg" >> ~{qc_file_name}
      fi
    done

    echo "$gzip_status" > ~{gzip_file}

    if [[ $gzip_status != 0 ]]; then
      echo "1" > ~{res_file}
    else
python3 <<CODE

import logging
import gzip
import os

def validate2(logger, input1str, input2str, output):
  input1 = input1str.split(',')
  input2 = input2str.split(',')

  if len(input1) != len(input2):
    msg = "number of fastq_1 files (%d) not equals to fastq_2 files (%d)" % (len(input1), len(input2))
    logger.error(msg)
    with open(output, "wt") as fout:
      fout.write("ERROR: %s\n" % msg)
    return(1)

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

    if error_msg is not None:
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
  
res = validate2(logger, "~{sep="," fastq_1}", "~{sep="," fastq_2}", "~{qc_file_name}")
with open("~{res_file}", "wt") as fout:
  fout.write(f"{res}")

CODE

      if [[ "$(cat ~{res_file})" == "0" ]]; then
        for fastq in ~{sep=" " fastq_1}
        do
          (cd "$(dirname "$fastq")" && md5sum "$(basename "$fastq")") >> ~{fastq1_md5_file}
        done

        for fastq in ~{sep=" " fastq_2}
        do
          (cd "$(dirname "$fastq")" && md5sum "$(basename "$fastq")") >> ~{fastq2_md5_file}
        done
      fi
    fi

    exit 0

>>>

  runtime {
    docker: "us.gcr.io/broad-dsp-gcr-public/base/python:3.9-debian"
    preemptible: preemptible_tries
    disks: "local-disk " + disk_size + " HDD"
    memory: "2 GiB"
  }
  output {
    File qc_file = "~{qc_file_name}"
    Int qc_failed = read_int("~{res_file}")
    Int qc_gzip_failed = read_int("~{gzip_file}")
    Array[String] fastq1_md5 = read_lines("~{fastq1_md5_file}")
    Array[String] fastq2_md5 = read_lines("~{fastq2_md5_file}")
  }
}