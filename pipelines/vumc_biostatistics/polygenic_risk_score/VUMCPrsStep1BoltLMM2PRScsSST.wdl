version 1.0

# This workflow converts BoltLMM output to PRScs-SST format for polygenic risk score calculation.
# Developed by VUMC Biostatistics for population-specific GWAS studies.
# Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
#
# Workflow steps:
# 1. Convert BoltLMM summary statistics into PRScs-SST compatible output
# 2. Write PRScs-SST columns (SNP, A1, A2, BETA, P)
#    SNP: rsID from SNP column
#    A1: Effect allele from A1 column
#    A2: Non-effect allele from AX column
#    BETA: Log(OR) computed from the OR column
#    P: P-value from the P column
# 3. Generate a matching BIM file for PRScs
# 4. Optionally map rsIDs to variant IDs using a provided ID,RSID mapping file
# 5. Optionally copy results to a GCP storage location
#
# Inputs:
# - input_boltLMM: BoltLMM output file containing summary statistics
# - perform_rsid_to_variantid: Whether to map rsIDs to variant IDs
# - rsid_variantid_map_file: Optional ID,RSID mapping file used when mapping is enabled
# - output_prefix: Prefix for the output SST and BIM files
# - target_gcp_folder: Optional GCP destination for result files
#
# Outputs:
# - output_sst_file: Path to the formatted PRScs-SST summary statistics file
# - output_bim_file_for_PRScs: Path to the matching BIM file for PRScs
# 
# Both output files are in GWAS A1,A2 order, not flipped by rsid_variantid map file.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils
import "./PRSUtils.wdl" as PRSUtils

workflow VUMCPrsStep1BoltLMM2PRScsSST {
  input {
    File input_boltLMM

    Boolean perform_rsid_to_variantid = true
    
    File? rsid_variantid_map_file # ID,RSID map file

    String output_prefix

    String? target_gcp_folder
  }

  # Validate: at least one of vep_cache_folder or vep_cache_tar_gz must be provided
  if (perform_rsid_to_variantid && !defined(rsid_variantid_map_file)) {
    call WDLUtils.FailWithMessage as Validate_rsid_variantid_map_file {
      input:
        message = "rsid_variantid_map_file must be provided when perform_rsid_to_variantid=true."
    }
  }

  call BoltLMM2PRScsSST {
    input:
      input_boltLMM = input_boltLMM,
      output_prefix = output_prefix
  }

  if (perform_rsid_to_variantid) {
    call PRSUtils.rsID2variantID {
      input:
        input_sst = BoltLMM2PRScsSST.output_sst_file,
        input_bim = BoltLMM2PRScsSST.output_bim_file_for_PRScs,
        rsid_variantid_map_file = select_first([rsid_variantid_map_file]),
        output_prefix = output_prefix
    }
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile {
      input:
        source_file1 = select_first([rsID2variantID.output_sst_file, BoltLMM2PRScsSST.output_sst_file]),
        source_file2 = select_first([rsID2variantID.output_bim_file_for_PRScs, BoltLMM2PRScsSST.output_bim_file_for_PRScs]),
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_sst_file = select_first([CopyFile.output_file1, rsID2variantID.output_sst_file, BoltLMM2PRScsSST.output_sst_file])
    File output_bim_file_for_PRScs = select_first([CopyFile.output_file2, rsID2variantID.output_bim_file_for_PRScs, BoltLMM2PRScsSST.output_bim_file_for_PRScs])
  }
}

task BoltLMM2PRScsSST {
  input {
    File input_boltLMM

    String output_prefix

    Int preemptible=3
    Int memory_gb = 5
    Int additional_disk_size_gb = 2
  }

  Int disk_size = ceil(size([input_boltLMM], "GB") * 3) + additional_disk_size_gb

  command <<<

cat <<CODE> convert.py

import csv
import gzip
import math

input_file = "~{input_boltLMM}"
output_sst = "~{output_prefix}.sst"
output_bim = "~{output_prefix}.bim"

def open_maybe_gzip(path):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path, "rt")

chr_map = {"X": "23", "Y": "24", "MT": "25", "M": "25"}

#ID SNP CHROM POS REF ALT1 ALT A1 AX A1_FREQ A1_CASE_FREQ A1_CTRL_FREQ MACH_R2 FIRTH? TEST OBS_CT OR LOG(OR)_SE L95 U95 Z_STAT P
#SNP:rsid, 8:A1, 9:AX, 17:OR, 22:P

with open_maybe_gzip(input_file) as fin, \
     open(output_sst, "w") as sst_out, \
     open(output_bim, "w") as bim_out:

    reader = csv.DictReader(fin, delimiter=' ')
    required = ["SNP", "A1", "AX", "OR", "P", "CHROM", "POS"]
    missing = [c for c in required if c not in (reader.fieldnames or [])]
    if missing:
        raise ValueError("Missing required columns: " + ",".join(missing))

    sst_out.write("SNP\tA1\tA2\tBETA\tP\n")

    for row in reader:
        snpid = row["SNP"]
        a1 = row["A1"]
        a2 = row["AX"]
        beta = math.log(float(row["OR"]))
        pval = row["P"]

        chr_str = row["CHROM"].replace('chr', '')
        chr_val = chr_map.get(chr_str, chr_str)

        pos = row["POS"]

        sst_out.write(f"{snpid}\t{a1}\t{a2}\t{beta}\t{pval}\n")
        bim_out.write(f"{chr_val}\t{snpid}\t0\t{pos}\t{a1}\t{a2}\n")

CODE

python3 convert.py

  >>>

  runtime {
    docker: "python:3.9-slim"
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }

  output {
    File output_sst_file = "~{output_prefix}.sst"
    File output_bim_file_for_PRScs = "~{output_prefix}.bim"
  }
}
