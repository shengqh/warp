version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC PGEN to BigQuery Matrix Conversion Workflow
##
## This workflow converts PLINK2 PGEN format genetic data files to BigQuery-compatible flat text matrices.
## Developed by VUMC Biostatistics for population genetics data ingestion into BigQuery.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## BigQuery requires genetic data in flat text format. This workflow converts PGEN files to
## tab-separated text matrices for genotypes, variants, and samples for BigQuery import.
##
## ### Workflow Steps:
## 1. **Pgen2BigQueryMatrix**: Converts the PGEN files to three text files — genotype matrix,
##    variant list, and sample list — suitable for BigQuery import.
## 2. **CopyFile (Optional)**: If `target_bucket` is provided, copies all three output files
##    to the specified GCS bucket.
##
## ### Inputs:
## - input_pgen: PLINK2 PGEN file containing genotype data.
## - input_pvar: PLINK2 PVAR file containing variant information.
## - input_psam: PLINK2 PSAM file containing sample information.
## - output_prefix: Prefix for output filenames.
## - target_bucket: Optional GCS bucket path to copy output files to after completion.
##
## ### Outputs:
## - output_pgen_txt: Genotype matrix in tab-separated text format.
## - output_pvar_txt: Variant list in tab-separated text format.
## - output_psam_txt: Sample list in tab-separated text format.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPgen2BigQueryMatrix {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    String output_prefix

    String? target_bucket
  }

  meta {
    allowNestedInputs: true
  }

  parameter_meta {
    input_pgen: "PLINK2 PGEN file containing genotype data"
    input_pvar: "PLINK2 PVAR file containing variant information"
    input_psam: "PLINK2 PSAM file containing sample information"
    output_prefix: "Prefix for output filenames"
    target_bucket: "Optional GCS bucket path to copy output files to after completion"
  }

  call Pgen2BigQueryMatrix {
    input:
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,
      output_prefix = output_prefix
  }

  if(defined(target_bucket)){
    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = Pgen2BigQueryMatrix.output_pgen_txt,
        source_file2 = Pgen2BigQueryMatrix.output_pvar_txt,
        source_file3 = Pgen2BigQueryMatrix.output_psam_txt,
        is_move_file = false,
        target_gcp_folder = select_first([target_bucket])
    }
  }

  output {
    File output_pgen_txt = select_first([CopyFile.output_file1, Pgen2BigQueryMatrix.output_pgen_txt])
    File output_pvar_txt = select_first([CopyFile.output_file2, Pgen2BigQueryMatrix.output_pvar_txt])
    File output_psam_txt = select_first([CopyFile.output_file3, Pgen2BigQueryMatrix.output_psam_txt])
  }
}

task Pgen2BigQueryMatrix {
  input {
    File input_pgen
    File input_pvar
    File input_psam
    
    String output_prefix

    Int preemptible = 1

    Float size_multiplier = 5.5
    
    String docker = "shengqh/plink_1.9_2.0:20250620"
    Int? memory_gb_override
    Int? disk_size_override
  }

  Int disk_size = select_first([disk_size_override, ceil(size([input_pgen, input_pvar, input_psam], "GB") * size_multiplier) + 20])
  Int memory_gb = select_first([memory_gb_override, 10])

  command <<<

cat<<EOF>convert.py

import pgenlib
import pandas as pd
import numpy as np
import gzip
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Pgen2Matrix")

pgen_file = '~{input_pgen}'
pvar_file = '~{input_pvar}'
psam_file = '~{input_psam}'

# Read .pvar file (variant information)
logger.info(f"Processing pvar file: {pvar_file} ...")
pvar = pd.read_csv(pvar_file, sep='\t', comment='#', 
                   names=['CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO'], 
                   dtype={'CHROM': str, 'POS': int, 'ID': str, 'REF': str, 'ALT': str})

# Create a function to map chromosome to start_index
def get_start_index(chrom):
  if chrom == 'X':
    int_chrom = 23
  elif chrom == 'Y':
    int_chrom = 24
  elif chrom == 'MT':
    int_chrom = 26
  else:
    try:
      int_chrom = int(chrom)
    except ValueError:
      int_chrom = 30  # Default case for any other chromosome format
  return int_chrom * 100000000

logger.info("Creating VARIANT_ID based on chromosome and position ...")
start_index = get_start_index(pvar['CHROM'].iloc[0])
# Apply the function to create VAR_ID
pvar['VARIANT_ID'] = pvar.index.astype(int) + start_index

# Reorder columns to have VARIANT_ID as the first column
pvar = pvar[['VARIANT_ID', 'CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO']]

# Save pvar information to a compressed tab-delimited file
logger.info("Saving pvar information to a compressed file ...")
with gzip.open("~{output_prefix}_pvar.txt.gz", "wt") as f:
  pvar.to_csv(f, sep="\t", index=False)

logger.info(f"Processing pvar file: {psam_file} ...")
# Read .psam file (sample information)
psam = pd.read_csv(psam_file, sep='\t', header=0)

psam.rename(columns={'#FID': 'FID'}, inplace=True)

# Create sample IDs based on the row index of the psam DataFrame
psam['SAMPLE_ID'] = psam.index.astype(int)

# Reorder columns to have SAMPLE_ID as the first column
psam = psam[['SAMPLE_ID', 'FID', 'IID', 'SEX']]

# Save psam information to a compressed tab-delimited file
logger.info("Saving psam information to a compressed file ...")
with gzip.open("~{output_prefix}_psam.txt.gz", "wt") as f:
  psam.to_csv(f, sep="\t", index=False)

logger.info(f"Processing pgen file: {pgen_file} ...")

num_samples = len(psam)
num_variants = len(pvar)

logger.info(f"Number of samples: {num_samples}")
logger.info(f"Number of variants: {num_variants}")

with gzip.open("~{output_prefix}_pgen.txt.gz", "wt") as fout:
  fout.write("VARIANT_ID\tSAMPLE_ID\tGENOTYPE\n")
  with pgenlib.PgenReader(pgen_file.encode(), num_samples, num_variants) as reader:
    # Allocate buffer for reading genotypes
    genotypes = np.zeros(num_samples, dtype=np.int8)
    
    # Preallocate arrays for sample indices and rows to write
    sample_indices = np.arange(num_samples, dtype=np.int32)
    buffer_size = 100  # Process this many variants before writing
    output_buffer = []
    
    # Read genotypes for all variants in batches
    for variant_idx in range(num_variants):
      if variant_idx % 10000 == 0:
        # Time estimation
        if variant_idx == 0:
          start_time = time.time()
          elapsed = 0
          logger.info(f" {variant_idx + 1}/{num_variants}")
        else:
          # Calculate percentage complete and estimated time remaining
          percent_complete = (variant_idx / num_variants) * 100
          elapsed = time.time() - start_time
            
          time_per_variant = elapsed / variant_idx
          remaining_variants = num_variants - variant_idx
          est_time_remaining = time_per_variant * remaining_variants
          
          # Format time remaining in minutes/seconds
          minutes, seconds = divmod(est_time_remaining, 60)
          
          logger.info(f" {variant_idx + 1}/{num_variants} | {percent_complete:.1f}% | Elapsed: {elapsed:.1f}s | Est. remaining: {int(minutes)}m {int(seconds)}s")

      var_id = pvar.iloc[variant_idx]['VARIANT_ID']
      # Read genotypes for the current variant
      reader.read(variant_idx, genotypes)
      
      # Find non-zero genotypes (vectorized operation)
      nonzero_indices = np.nonzero(genotypes)[0]
  
      # If there are non-zero genotypes, add them to buffer
      if len(nonzero_indices) > 0:
        nonzero_genotypes = genotypes[nonzero_indices]
        nonzero_samples = sample_indices[nonzero_indices]
        
        # Add to buffer
        for i in range(len(nonzero_indices)):
          output_buffer.append(f"{var_id}\t{nonzero_samples[i]}\t{nonzero_genotypes[i]}")
      
      # Write buffer when it gets large enough
      if len(output_buffer) >= 100000 or variant_idx == num_variants - 1:
        fout.write("\n".join(output_buffer) + "\n")
        output_buffer = []

EOF

python3 convert.py

>>>

  runtime {
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_pgen_txt = "~{output_prefix}_pgen.txt.gz"
    File output_pvar_txt = "~{output_prefix}_pvar.txt.gz"
    File output_psam_txt = "~{output_prefix}_psam.txt.gz"
  }
}
