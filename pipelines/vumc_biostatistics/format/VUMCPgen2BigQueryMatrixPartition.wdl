version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPgen2BigQueryMatrixPartition {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    String output_prefix

    String? target_bucket
  }

  call Pgen2BigQueryMatrixPartition as Pgen2BigQueryMatrix {
    input:
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,
      output_prefix = output_prefix
  }

  if(defined(target_bucket)){
    call GcpUtils.MoveOrCopyTwoFiles as CopyFile1 {
      input:
        source_file1 = Pgen2BigQueryMatrix.output_pvar_txt,
        source_file2 = Pgen2BigQueryMatrix.output_psam_txt,
        is_move_file = false,
        target_gcp_folder = select_first([target_bucket])
    }
    call GcpUtils.MoveOrCopyFileArray as CopyFile2 {
      input:
        source_files = Pgen2BigQueryMatrix.output_pgen_txt_files,
        is_move_file = false,
        target_gcp_folder = select_first([target_bucket])
    }
  }

  output {
    String output_pvar_txt = select_first([CopyFile1.output_file1, Pgen2BigQueryMatrix.output_pvar_txt])
    String output_psam_txt = select_first([CopyFile1.output_file2, Pgen2BigQueryMatrix.output_psam_txt])
    Array[String] output_pgen_txt_files = select_first([CopyFile2.outputFiles, Pgen2BigQueryMatrix.output_pgen_txt_files])
  }
}

task Pgen2BigQueryMatrixPartition {
  input {
    File input_pgen
    File input_pvar
    File input_psam
    
    String output_prefix

    Int preemptible = 1

    Int n_lines_per_file = 750000000 # For pgen txt file, this is about 2 GB per file
    
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

# Initialize variables for file splitting
current_file_index = 1
current_line_count = 0
output_file = None
base_filename = f"~{output_prefix}_pgen"

output_pgen_files = []
# Process the genotypes
with pgenlib.PgenReader(pgen_file.encode(), num_samples, num_variants) as reader:
  # Allocate buffer for reading genotypes
  genotypes = np.zeros(num_samples, dtype=np.int8)
  
  # Preallocate arrays for sample indices and rows to write
  sample_indices = np.arange(num_samples, dtype=np.int32)
  buffer_size = 100  # Process this many variants before writing
  output_buffer = []
  
  # Open the first output file
  current_part_file = f"{base_filename}_{current_file_index:03d}.txt.gz"
  output_pgen_files.append(current_part_file)
  logger.info(f"Creating output file: {current_part_file}")
  output_file = gzip.open(current_part_file, "wt")
  output_file.write("VARIANT_ID\tSAMPLE_ID\tGENOTYPE\n")
  
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
      # Check if adding these lines would exceed the limit
      if current_line_count + len(output_buffer) > ~{n_lines_per_file}:
        # Close current file
        output_file.close()
        
        # Create new file
        current_file_index += 1
        current_line_count = 0
        current_part_file = f"{base_filename}_{current_file_index:03d}.txt.gz"
        output_pgen_files.append(current_part_file)
        logger.info(f"Creating new output file: {current_part_file}")
        output_file = gzip.open(current_part_file, "wt")
        output_file.write("VARIANT_ID\tSAMPLE_ID\tGENOTYPE\n")
      
    # Write data to current file
    output_file.write("\n".join(output_buffer) + "\n")
    current_line_count += len(output_buffer)
    output_buffer = []

  # Close the final output file
  if output_file:
    output_file.close()

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
    Array[File] output_pgen_txt_files = glob("~{output_prefix}_pgen_*.txt.gz")
    File output_pvar_txt = "~{output_prefix}_pvar.txt.gz"
    File output_psam_txt = "~{output_prefix}_psam.txt.gz"
  }
}
