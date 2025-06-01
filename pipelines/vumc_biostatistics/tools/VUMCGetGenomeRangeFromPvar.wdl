version 1.0

# This workflow extracts genome range information from a PLINK pvar file.
# The workflow performs the following steps:
# 1. Reads the pvar file and identifies the unique chromosome.
# 2. Finds the minimum position (start) and maximum position (end) in the file.
# 3. Verifies that only one chromosome is present in the file.
# 4. Returns the chromosome as a string, and start and end positions as integers.
#
# Input parameters:
# - pvar_file: A PLINK pvar file containing variant information.
#
# Output parameters:
# - chromosome: The single chromosome identified in the pvar file (string).
# - start_position: The minimum base position found in the pvar file (integer).
# - end_position: The maximum base position found in the pvar file (integer).
#
# Note: The workflow will fail if multiple chromosomes are detected in the input pvar file.

workflow VUMCGetGenomeRangeFromPvar {
  input {
    File pvar_file
  }

  call ExtractGenomeRange {
    input:
      pvar_file = pvar_file
  }

  output {
    String chromosome = ExtractGenomeRange.chromosome
    Int chromosome_start = ExtractGenomeRange.start_position
    Int chromosome_end = ExtractGenomeRange.end_position
  }

  meta {
    author: "VUMC Biostatistics"
    description: "Extract genome range information from a PLINK pvar file"
  }
}

task ExtractGenomeRange {
  input {
    File pvar_file
    Int memory_gb = 4
    Int disk_space_gb = 10
    Int cpu = 1
  }

  command <<<
    # Skip header lines that start with # and extract chromosome, position information
    # Then find min and max positions and ensure only one chromosome exists
    awk '
      BEGIN {
        min_pos = -1;
      }
      !/^#/ {
        chroms[$1] = 1;

        if (min_pos==-1) {
          first_chrom = $1;
          min_pos = $2;
        }
        max_pos = $2;
        print("chr=", $1, "min_pos=", min_pos, "max_pos=", max_pos);
      }
      END {
        if (length(chroms) > 1) {
          print "Error: Multiple chromosomes found in pvar file. Expected only one chromosome." > "/dev/stderr";
          exit 1;
        }
        print first_chrom > "chromosome.txt";
        print min_pos > "start_position.txt";
        print max_pos > "end_position.txt";
      }
    ' ~{pvar_file}
  >>>

  output {
    String chromosome = read_string("chromosome.txt")
    Int start_position = read_int("start_position.txt")
    Int end_position = read_int("end_position.txt")
  }

  runtime {
    memory: "~{memory_gb} GB"
    disk: "~{disk_space_gb} GB"
    preemptible: 3
    cpu: cpu
    docker: "ubuntu:20.04"
  }

  meta {
    description: "Extracts the chromosome, first base position, and last base position from a PLINK pvar file, expecting only one chromosome"
  }
}
