version 1.0

## Copyright Vanderbilt Health, 2026
##
## VUMC AutoGVP Automated Germline Variant Pathogenicity Pipeline
##
## This workflow performs automated germline variant pathogenicity assessment
## by integrating VEP, InterVar, ANNOVAR gnomAD, AutoPVS1, and AutoGVP annotations.
## Developed by VUMC Biostatistics for clinical variant interpretation.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## Given input VCF files and gene symbols, this workflow extracts variants in gene regions,
## annotates them with multiple tools, and produces a combined pathogenicity assessment.
## Supports both local folder and cloud tar.gz inputs for VEP cache, ANNOVAR DB, and AutoPVS1 data.
##
## ### Workflow Steps:
## 1. **GetGeneLocusGTF** or **GetGeneLocusBiomart**: Get gene locus coordinates.
##    - If gene_gtf is provided, parses local GTF file (stable, offline, recommended).
##    - Otherwise, queries Ensembl biomaRt (network-dependent, can be unstable).
## 2. **ExtractVcfByRegion**: Extract variants within gene regions from input VCFs (scatter).
## 3. **MergeVcfs**: Merge extracted VCFs into a single file.
## 4. **RunVEP**: Annotate with VEP (Variant Effect Predictor).
## 5. **RunInterVar**: Run InterVar for clinical variant interpretation (parallel with 6, 7).
## 6. **RunAnnovarGnomad**: Run ANNOVAR for gnomAD allele frequency annotation (parallel with 5, 7).
## 7. **RunAutoPVS1**: Run AutoPVS1 for PVS1 criterion assessment (parallel with 5, 6).
## 8. **RunAutoGVP**: Combine all annotations with AutoGVP.
##
## ### Inputs:
## - input_vcfs: Array of input VCF files (one per chromosome or region).
## - input_vcf_indices: Array of VCF index files corresponding to input_vcfs.
## - gene_symbols: Comma-separated gene symbols (e.g., "BRCA1,BRCA2").
## - gene_gtf: Optional GENCODE/Ensembl GTF file for local gene lookup (recommended).
## - genome_fasta: Reference genome FASTA file for VEP.
## - genome_fasta_fai: Reference genome FASTA index file.
## - vep_cache_folder: Optional local VEP cache directory.
## - vep_cache_tar_gz: Optional VEP cache tar.gz archive (cloud).
## - vep_cache_uncompressed_gb: Estimated uncompressed size of the VEP cache in GB (required if using tar.gz).
## - annovar_db_folder: Optional local ANNOVAR database directory.
## - annovar_db_tar_gz: Optional ANNOVAR database tar.gz archive (cloud).
## - annovar_db_tar_folder_name: Optional folder name inside the ANNOVAR tar.gz archive (default "humandb").
## - annovar_db_uncompressed_gb: Estimated uncompressed size of the ANNOVAR database in GB (required if using tar.gz).
## - annovar_gnomAD_folder: Optional local gnomAD ANNOVAR database directory.
## - annovar_gnomAD_tar_gz: Optional gnomAD ANNOVAR database tar.gz archive (cloud).
## - annovar_gnomAD_tar_folder_name: Optional folder name inside the gnomAD ANNOVAR tar.gz archive (default "humandb_gnomad41").
## - annovar_gnomAD_uncompressed_gb: Estimated uncompressed size of the gnomAD ANNOVAR database in GB (required if using tar.gz).
## - autopvs_data_folder: Optional local AutoPVS1 data directory.
## - autopvs_data_tar_gz: Optional AutoPVS1 data tar.gz archive (cloud).
## - autopvs_data_uncompressed_gb: Estimated uncompressed size of the AutoPVS1 data in GB (required if using tar.gz).
## - clinvar_vcf: ClinVar VCF file for AutoGVP.
## - selected_clinvar_submissions: Selected ClinVar submissions file.
## - variant_summary: ClinVar variant summary file.
## - submission_summary: ClinVar submission summary file.
## - target_prefix: Prefix for all output files.
## - target_gcp_folder: Optional GCP folder path for copying output files.
##
## ### Outputs:
## - gene_bed: BED file with gene locus coordinates.
## - merged_vcf: Merged VCF file from all filtered VCFs.
## - vep_vcf: VEP-annotated VCF file.
## - intervar_file: InterVar interpretation results.
## - annovar_file: ANNOVAR gnomAD annotation results.
## - autopvs1_file: AutoPVS1 PVS1 criterion results.
## - autogvp_abridged_file: Final combined AutoGVP pathogenicity assessment.

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../../../tasks/vumc_biostatistics/WDLUtils.wdl" as WDLUtils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils

workflow VUMCAutoGVP {
  input {
    # Input VCFs (one per chromosome or region) with their indices
    Array[String] input_chromosomes
    Array[File] input_vcfs
    Array[File] input_vcf_indices

    # Gene symbols (comma-separated, e.g., "BRCA1,BRCA2")
    String gene_symbols

    # Gene annotation GTF file (e.g., GENCODE v38). If provided, uses local GTF parsing
    # instead of biomaRt (which can be unstable due to Ensembl server load).
    File? gene_gtf

    # VEP references
    File genome_fasta
    File genome_fasta_fai

    String? vep_cache_folder
    File? vep_cache_tar_gz
    Float? vep_cache_uncompressed_gb

    # Annovar database (tar.gz archive) for InterVar
    String? annovar_db_folder
    File? annovar_db_tar_gz
    String? annovar_db_tar_folder_name = "humandb"
    Float? annovar_db_uncompressed_gb

    # Annovar database (tar.gz archive) for gnomAD annotation
    String? annovar_gnomAD_folder
    File? annovar_gnomAD_tar_gz
    String? annovar_gnomAD_tar_folder_name = "humandb_gnomad41"
    Float? annovar_gnomAD_uncompressed_gb

    # AutoPVS1 data
    String? autopvs_data_folder
    File? autopvs_data_tar_gz
    Float? autopvs_data_uncompressed_gb

    # ClinVar references for AutoGVP
    File clinvar_vcf
    File selected_clinvar_submissions
    File variant_summary
    File submission_summary

    # Output prefix
    String target_prefix

    # Optional GCP folder to copy final output
    String? target_gcp_folder
  }

  # Validate: at least one of vep_cache_folder or vep_cache_tar_gz must be provided
  if (!defined(vep_cache_folder) && !defined(vep_cache_tar_gz)) {
    call WDLUtils.FailWithMessage as ValidateVepCache {
      input:
        message = "Either vep_cache_folder or vep_cache_tar_gz must be provided."
    }
  }

  if (defined(vep_cache_tar_gz) && !defined(vep_cache_uncompressed_gb)) {
    call WDLUtils.FailWithMessage as ValidateVepCacheSize {
      input:
        message = "vep_cache_uncompressed_gb must be provided when using vep_cache_tar_gz."
    }
  }

  # Validate: at least one of annovar_db_folder or annovar_db_tar_gz must be provided
  if (!defined(annovar_db_folder) && !defined(annovar_db_tar_gz)) {
    call WDLUtils.FailWithMessage as ValidateAnnovarDb {
      input:
        message = "Either annovar_db_folder or annovar_db_tar_gz must be provided."
    }
  }

  if(defined(annovar_db_tar_gz) && !defined(annovar_db_uncompressed_gb)) {
    call WDLUtils.FailWithMessage as ValidateAnnovarDbSize {
      input:
        message = "annovar_db_uncompressed_gb must be provided when using annovar_db_tar_gz."
    }
  }

  # Validate: at least one of annovar_gnomAD_folder or annovar_gnomAD_tar_gz must be provided
  if (!defined(annovar_gnomAD_folder) && !defined(annovar_gnomAD_tar_gz)) {
    call WDLUtils.FailWithMessage as ValidateAnnovarGnomAD {
      input:
        message = "Either annovar_gnomAD_folder or annovar_gnomAD_tar_gz must be provided."
    }
  }

  if(defined(annovar_gnomAD_tar_gz) && !defined(annovar_gnomAD_uncompressed_gb)) {
    call WDLUtils.FailWithMessage as ValidateAnnovarGnomADSize {
      input:
        message = "annovar_gnomAD_uncompressed_gb must be provided when using annovar_gnomAD_tar_gz."
    }
  }

  # Validate: at least one of autopvs_data_folder or autopvs_data_tar_gz must be provided
  if (!defined(autopvs_data_folder) && !defined(autopvs_data_tar_gz)) {
    call WDLUtils.FailWithMessage as ValidateAutoPVS1Data {
      input:
        message = "Either autopvs_data_folder or autopvs_data_tar_gz must be provided."
    }
  }

  if(defined(autopvs_data_tar_gz) && !defined(autopvs_data_uncompressed_gb)) {
    call WDLUtils.FailWithMessage as ValidateAutoPVS1DataSize {
      input:
        message = "autopvs_data_uncompressed_gb must be provided when using autopvs_data_tar_gz."
    }
  }

  # Step 1: Get gene locus BED file (prefer GTF if provided, fallback to biomaRt)
  if (defined(gene_gtf)) {
    call GetGeneLocusGTF {
      input:
        gene_symbols = gene_symbols,
        gene_gtf = select_first([gene_gtf])
    }
  }

  if (!defined(gene_gtf)) {
    call GetGeneLocusBiomart {
      input:
        gene_symbols = gene_symbols
    }
  }

  File gene_bed_result = select_first([GetGeneLocusGTF.gene_bed, GetGeneLocusBiomart.gene_bed])

  # Step 2: Extract VCFs by gene region (scatter over input VCFs)

  # In case there are input VCFs without any variants in the gene regions, we need to handle empty VCFs gracefully.
  call BioUtils.GetChromosomeIndecies as CheckOverlapVariants {
    input:
      input_chromosomes = input_chromosomes,
      input_bed_file = gene_bed_result
  }

  scatter(idx in CheckOverlapVariants.chromosome_indecies){
    call ExtractVcfByRegion {
      input:
        input_vcf = input_vcfs[idx],
        input_vcf_index = input_vcf_indices[idx],
        region_bed = gene_bed_result,
        output_prefix = basename(input_vcfs[idx], ".vcf.gz")
    }
  }

  # Step 3: Merge extracted VCFs
  call MergeVcfs {
    input:
      input_vcfs = ExtractVcfByRegion.filtered_vcf,
      target_prefix = target_prefix
  }

  # Step 4: Run VEP
  call RunVEP {
    input:
      input_vcf = MergeVcfs.merged_vcf,
      genome_fasta = genome_fasta,
      genome_fasta_fai = genome_fasta_fai,
      vep_cache_folder = vep_cache_folder,
      vep_cache_tar_gz = vep_cache_tar_gz,
      vep_cache_uncompressed_gb = vep_cache_uncompressed_gb,
      target_prefix = target_prefix
  }

  # Steps 5, 6, 7 run in parallel (all depend only on VEP output)

  # Step 5: Run InterVar
  call RunInterVar {
    input:
      input_vcf = RunVEP.vep_vcf,
      annovar_db_folder = annovar_db_folder,
      annovar_db_tar_gz = annovar_db_tar_gz,
      annovar_db_uncompressed_gb = annovar_db_uncompressed_gb,
      annovar_db_tar_folder_name = annovar_db_tar_folder_name,
      target_prefix = target_prefix
  }

  # Step 6: Run ANNOVAR with gnomAD
  call RunAnnovarGnomad {
    input:
      input_vcf = RunVEP.vep_vcf,
      annovar_gnomAD_folder = annovar_gnomAD_folder,
      annovar_gnomAD_tar_gz = annovar_gnomAD_tar_gz,
      annovar_gnomAD_tar_folder_name = annovar_gnomAD_tar_folder_name,
      annovar_gnomAD_uncompressed_gb = annovar_gnomAD_uncompressed_gb,
      target_prefix = target_prefix
  }

  # Step 7: Run AutoPVS1
  call RunAutoPVS1 {
    input:
      input_vcf = RunVEP.vep_vcf,
      autopvs_data_folder = autopvs_data_folder,
      autopvs_data_tar_gz = autopvs_data_tar_gz,
      autopvs_data_uncompressed_gb = autopvs_data_uncompressed_gb,
      target_prefix = target_prefix
  }

  # Step 8: Run AutoGVP (combines all annotations)
  call RunAutoGVP {
    input:
      vep_vcf = RunVEP.vep_vcf,
      intervar_file = RunInterVar.intervar_file,
      annovar_file = RunAnnovarGnomad.annovar_file,
      autopvs1_file = RunAutoPVS1.autopvs1_file,
      clinvar_vcf = clinvar_vcf,
      selected_clinvar_submissions = selected_clinvar_submissions,
      variant_summary = variant_summary,
      submission_summary = submission_summary,
      target_prefix = target_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyFiles as CopyFile {
      input:
        source_file1 = gene_bed_result,
        source_file2 = MergeVcfs.merged_vcf,
        source_file3 = RunVEP.vep_vcf,
        source_file4 = RunInterVar.intervar_file,
        source_file5 = RunAnnovarGnomad.annovar_file,
        source_file6 = RunAutoPVS1.autopvs1_file,
        source_file7 = RunAutoGVP.autogvp_abridged_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File gene_bed = select_first([CopyFile.output_file1, gene_bed_result])
    File merged_vcf = select_first([CopyFile.output_file2, MergeVcfs.merged_vcf])
    File vep_vcf = select_first([CopyFile.output_file3, RunVEP.vep_vcf])
    File intervar_file = select_first([CopyFile.output_file4, RunInterVar.intervar_file])
    File annovar_file = select_first([CopyFile.output_file5, RunAnnovarGnomad.annovar_file])
    File autopvs1_file = select_first([CopyFile.output_file6, RunAutoPVS1.autopvs1_file])
    File autogvp_abridged_file = select_first([CopyFile.output_file7, RunAutoGVP.autogvp_abridged_file])
  }
}


# Get gene locus coordinates by parsing a local GENCODE/Ensembl GTF file.
# This is a stable offline alternative to biomaRt (no network dependency).
# Supports comma-separated gene symbols (e.g., "BRCA1,BRCA2").
task GetGeneLocusGTF {
  input {
    String gene_symbols
    File gene_gtf
    Int shift_bases = 0

    String docker = "python:3.9-slim"
    Int memory_gb = 4
    Int cpu = 1
  }

  String target_file = sub(gene_symbols, ",", "_") + ".bed"
  Int disk_size = ceil(size(gene_gtf, "GB") * 2) + 5

  command <<<
set -e

python3 <<'PYEOF'
import sys
import re

gene_str = "~{gene_symbols}"
gtf_file = "~{gene_gtf}"
shift_bases = ~{shift_bases}
output_file = "~{target_file}"

genes = set(g.strip() for g in gene_str.split(","))
found_genes = set()
results = []

with open(gtf_file) as f:
    for line in f:
        if line.startswith("#"):
            continue
        fields = line.rstrip("\n").split("\t")
        if len(fields) < 9 or fields[2] != "gene":
            continue

        attrs = fields[8]
        m_name = re.search(r'gene_name "([^"]+)"', attrs)
        if not m_name or m_name.group(1) not in genes:
            continue

        gene_name = m_name.group(1)
        found_genes.add(gene_name)

        chrom = fields[0]
        start = max(0, int(fields[3]) - shift_bases - 1)  # GTF is 1-based inclusive, BED is 0-based half-open
        end = int(fields[4]) + shift_bases
        strand = fields[6]

        m_id = re.search(r'gene_id "([^"]+)"', attrs)
        gene_id = m_id.group(1).split(".")[0] if m_id else ""

        # Add chr prefix if missing
        if not chrom.startswith("chr"):
            chrom = "chr" + chrom
        chrom = chrom.replace("chrMT", "chrM")

        results.append((chrom, start, end, 1000, gene_name, strand, gene_id))

results.sort(key=lambda x: (x[0], x[1]))

with open(output_file, "w") as f:
    for r in results:
        f.write("\t".join(str(x) for x in r) + "\n")

missing = genes - found_genes
if missing:
    print(f"WARNING: Gene(s) not found in GTF: {', '.join(sorted(missing))}", file=sys.stderr)

if not results:
    print(f"ERROR: No gene entries found for: {gene_str}", file=sys.stderr)
    sys.exit(1)

print(f"Found {len(results)} gene entries for {len(found_genes)}/{len(genes)} genes")
PYEOF
  >>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File gene_bed = "~{target_file}"
  }
}


# Get gene locus coordinates from Ensembl using biomaRt.
# Supports comma-separated gene symbols (e.g., "BRCA1,BRCA2").
task GetGeneLocusBiomart {
  input {
    String gene_symbols
    Int shift_bases = 0

    String host = "https://www.ensembl.org"
    String dataset = "hsapiens_gene_ensembl"
    String symbolKey = "hgnc_symbol"
    Int addChr = 1

    String docker = "shengqh/report:20241120"
    Int memory_gb = 4
    Int cpu = 1
  }

  String target_file = sub(gene_symbols, ",", "_") + ".bed"

  command <<<

cat <<'RSCRIPT' > script.r

require(biomaRt)
require(stringr)

host <- "~{host}"
dataset <- "~{dataset}"
symbolKey <- "~{symbolKey}"
gene_str <- "~{gene_symbols}"
addChr <- ~{addChr}
shift_bases <- ~{shift_bases}

genes <- trimws(unlist(strsplit(gene_str, ",")))

ensembl <- useMart("ensembl", host=host, dataset=dataset)

geneLocus <- getBM(
  attributes=c("chromosome_name", "start_position", "end_position", symbolKey, "strand", "ensembl_gene_id"),
  filters=symbolKey,
  values=genes,
  mart=ensembl,
  uniqueRows=TRUE,
  useCache=FALSE
)

geneLocus <- geneLocus[nchar(geneLocus$chromosome_name) < 6,]

geneLocus$score <- 1000

geneLocus <- geneLocus[,c("chromosome_name", "start_position", "end_position", "score", symbolKey, "strand", "ensembl_gene_id")]
geneLocus <- geneLocus[order(geneLocus$chromosome_name, geneLocus$start_position),]

geneLocus$strand[geneLocus$strand == 1] <- "+"
geneLocus$strand[geneLocus$strand == -1] <- "-"

if(addChr & (!any(grepl("chr", geneLocus$chromosome_name)))){
  geneLocus$chromosome_name <- paste0("chr", geneLocus$chromosome_name)
}

geneLocus$chromosome_name <- gsub("chrMT", "chrM", geneLocus$chromosome_name)

if(shift_bases > 0){
  geneLocus$start_position <- geneLocus$start_position - shift_bases
  geneLocus$end_position <- geneLocus$end_position + shift_bases
}

bedFile <- "~{target_file}"
write.table(geneLocus, file=bedFile, row.names=FALSE, col.names=FALSE, sep="\t", quote=FALSE)

RSCRIPT

R -f script.r

>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk 10 HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File gene_bed = "~{target_file}"
  }
}


# Extract variants from a VCF file that overlap with regions in a BED file.
task ExtractVcfByRegion {
  input {
    File input_vcf
    File input_vcf_index
    File region_bed
    String output_prefix

    Int memory_gb = 10
    Int cpu = 1
    String docker = "shengqh/samtools_bcftools_tabix:v1.21"
  }

  Int disk_size = ceil(size([input_vcf, input_vcf_index], "GB") * 2) + 10

  command <<<
set -e

bcftools view -R ~{region_bed} ~{input_vcf} -Oz -o ~{output_prefix}.filtered.vcf.gz

tabix -f -p vcf ~{output_prefix}.filtered.vcf.gz
>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File filtered_vcf = "~{output_prefix}.filtered.vcf.gz"
    File filtered_vcf_index = "~{output_prefix}.filtered.vcf.gz.tbi"
  }
}


# Merge multiple VCF files into a single VCF.
# Handles the edge case of a single input VCF (copies instead of merge).
task MergeVcfs {
  input {
    Array[File] input_vcfs
    String target_prefix

    Int memory_gb = 20
    Int cpu = 1
    String docker = "shengqh/samtools_bcftools_tabix:v1.21"
  }

  Int disk_size = ceil(size(input_vcfs, "GB") * 3) + 10

  command <<<
set -e

# Index input VCFs (indices may not be co-localized by Cromwell)
for vcf in ~{sep=" " input_vcfs}; do
  tabix -f -p vcf "$vcf"
done

# Create file list for bcftools merge
for vcf in ~{sep=" " input_vcfs}; do
  echo "$vcf" >> vcf_list.txt
done

num_vcfs=$(cat vcf_list.txt | wc -l)

if [[ $num_vcfs -eq 1 ]]; then
  cp "$(cat vcf_list.txt)" ~{target_prefix}.vcf.gz
else
  bcftools merge -Oz -o ~{target_prefix}.vcf.gz -l vcf_list.txt
fi

tabix -f -p vcf ~{target_prefix}.vcf.gz
>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File merged_vcf = "~{target_prefix}.vcf.gz"
    File merged_vcf_index = "~{target_prefix}.vcf.gz.tbi"
  }
}


# Run Variant Effect Predictor (VEP) on a VCF file.
# Provide either vep_cache_folder (local path) or vep_cache_tar_gz (will be extracted).
task RunVEP {
  input {
    File input_vcf
    File genome_fasta
    File genome_fasta_fai
    String? vep_cache_folder
    File? vep_cache_tar_gz
    Float? vep_cache_uncompressed_gb

    String target_prefix

    String species = "homo_sapiens"
    String assembly = "GRCh38"

    Int memory_gb = 20
    Int cpu = 1
    String docker = "ensemblorg/ensembl-vep:release_104.3"
  }

  Boolean use_local_cache = defined(vep_cache_folder)
  Float tar_gz_gb = if defined(vep_cache_tar_gz) then size(select_first([vep_cache_tar_gz]), "GB") else 0
  Float true_cache_gb = if defined(vep_cache_uncompressed_gb) then select_first([vep_cache_uncompressed_gb]) else tar_gz_gb * 5
  Int disk_size = ceil(size([input_vcf, genome_fasta, genome_fasta_fai], "GB") * 3 + tar_gz_gb + true_cache_gb) + 20

  command <<<
set -e

if [[ "~{use_local_cache}" == "true" ]]; then
  VEP_CACHE_DIR="~{vep_cache_folder}"
elif [[ -n "~{vep_cache_tar_gz}" ]]; then
  echo "Extracting VEP cache..."
  mkdir -p vep_cache
  tar -xzf ~{vep_cache_tar_gz} -C vep_cache
  VEP_CACHE_DIR="vep_cache"
  echo "VEP cache extracted."
else
  echo "ERROR: Either vep_cache_folder or vep_cache_tar_gz must be provided." >&2
  exit 1
fi

echo "vep_start=$(date)"

vep --offline --cache --dir_cache $VEP_CACHE_DIR \
  --fasta ~{genome_fasta} \
  --use_given_ref \
  --species ~{species} --assembly ~{assembly} \
  --fork ~{cpu} --xref_refseq --hgvs --hgvsg --canonical --symbol \
  --distance 0 --exclude_predicted --flag_pick --lookup_ref --force \
  --format vcf --vcf --no_stats --numbers \
  --input_file ~{input_vcf} \
  --output_file ~{target_prefix}.VEP.vcf

echo "vep_end=$(date)"
>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File vep_vcf = "~{target_prefix}.VEP.vcf"
  }
}


# Run InterVar for clinical variant interpretation.
# Provide either annovar_db_folder (local path) or annovar_db_tar_gz (will be extracted).
task RunInterVar {
  input {
    File input_vcf
    String? annovar_db_folder
    File? annovar_db_tar_gz
    String? annovar_db_tar_folder_name
    Float? annovar_db_uncompressed_gb

    String target_prefix

    String buildver = "hg38"
    String intervar_path = "/opt/InterVar/Intervar.py"
    String intervar_db = "/opt/InterVar/intervardb"
    String table_annovar_path = "/opt/annovar_20250302/table_annovar.pl"
    String convert2annovar_path = "/opt/annovar_20250302/convert2annovar.pl"
    String annotate_variation_path = "/opt/annovar_20250302/annotate_variation.pl"

    Int memory_gb = 20
    Int cpu = 1
    String docker = "shengqh/intervar:20260331"
  }

  Boolean use_local_db = defined(annovar_db_folder)
  Float tar_gz_gb = if defined(annovar_db_tar_gz) then size(select_first([annovar_db_tar_gz]), "GB") else 0
  Float true_db_gb = if defined(annovar_db_uncompressed_gb) then select_first([annovar_db_uncompressed_gb]) else tar_gz_gb * 10
  Int disk_size = ceil(size(input_vcf, "GB") * 5 + tar_gz_gb + true_db_gb) + 20

  command <<<
set -e

if [[ "~{use_local_db}" == "true" ]]; then
  ANNOVAR_DB_DIR="~{annovar_db_folder}"
elif [[ -n "~{annovar_db_tar_gz}" ]]; then
  echo "Extracting annovar database..."
  tar -xzf ~{annovar_db_tar_gz}
  if [[ -n "~{annovar_db_tar_folder_name}" ]]; then
    ANNOVAR_DB_DIR="~{annovar_db_tar_folder_name}"
  else
    ANNOVAR_DB_DIR=$(basename "~{annovar_db_tar_gz}" .tar.gz)
  fi
  echo "Annovar database extracted."
else
  echo "ERROR: Either annovar_db_folder or annovar_db_tar_gz must be provided." >&2
  exit 1
fi

echo "intervar_start=$(date)"

python ~{intervar_path} \
  -i ~{input_vcf} \
  --input_type=VCF \
  -o ~{target_prefix} \
  -b ~{buildver} \
  -t ~{intervar_db} \
  --table_annovar=~{table_annovar_path} \
  --convert2annovar=~{convert2annovar_path} \
  --annotate_variation=~{annotate_variation_path} \
  -d $ANNOVAR_DB_DIR

# Clean up intermediate files
rm -f ~{target_prefix}.~{buildver}_multianno.txt.grl_p

if [[ "~{use_local_db}" != "true" ]]; then
  rm -rf $ANNOVAR_DB_DIR
fi

echo "intervar_end=$(date)"
>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File intervar_file = "~{target_prefix}.~{buildver}_multianno.txt.intervar"
  }
}


# Run ANNOVAR with gnomAD allele frequency annotation.
# Provide either annovar_db_folder (local path) or annovar_db_tar_gz (will be extracted).
task RunAnnovarGnomad {
  input {
    File input_vcf

    String? annovar_gnomAD_folder
    File? annovar_gnomAD_tar_gz
    String? annovar_gnomAD_tar_folder_name
    Float? annovar_gnomAD_uncompressed_gb

    String target_prefix

    String buildver = "hg38"
    String protocol = "gnomad41_genome"
    String operation = "f"

    Int memory_gb = 20
    Int cpu = 1
    String docker = "shengqh/intervar:20260331"
  }

  Boolean use_local_db = defined(annovar_gnomAD_folder)
  Float tar_gz_gb = if defined(annovar_gnomAD_tar_gz) then size(select_first([annovar_gnomAD_tar_gz]), "GB") else 0
  Float true_db_gb = if defined(annovar_gnomAD_uncompressed_gb) then select_first([annovar_gnomAD_uncompressed_gb]) else tar_gz_gb * 10
  Int disk_size = ceil(size(input_vcf, "GB") * 5 + tar_gz_gb + true_db_gb) + 20

  command <<<
set -e

if [[ "~{use_local_db}" == "true" ]]; then
  ANNOVAR_DB_DIR="~{annovar_gnomAD_folder}"
elif [[ -n "~{annovar_gnomAD_tar_gz}" ]]; then
  echo "Extracting annovar database..."
  tar -xzf ~{annovar_gnomAD_tar_gz}
  if [[ -n "~{annovar_gnomAD_tar_folder_name}" ]]; then
    ANNOVAR_DB_DIR="~{annovar_gnomAD_tar_folder_name}"
  else
    ANNOVAR_DB_DIR=$(basename "~{annovar_gnomAD_tar_gz}" .tar.gz)
  fi
  echo "Annovar database extracted."
else
  echo "ERROR: Either annovar_gnomAD_folder or annovar_gnomAD_tar_gz must be provided." >&2
  exit 1
fi

echo "ANNOVAR_DB_DIR=$ANNOVAR_DB_DIR"
echo "annovar_start=$(date)"
table_annovar.pl \
  ~{input_vcf} \
  $ANNOVAR_DB_DIR \
  --buildver ~{buildver} \
  --out ~{target_prefix} \
  --remove \
  --protocol ~{protocol} \
  --operation ~{operation} \
  --vcfinput

rm -f ~{target_prefix}.~{buildver}_multianno.vcf ~{target_prefix}.avinput

if [[ "~{use_local_db}" != "true" ]]; then
  rm -rf $ANNOVAR_DB_DIR
fi

echo "annovar_end=$(date)"
>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File annovar_file = "~{target_prefix}.~{buildver}_multianno.txt"
  }
}


# Run AutoPVS1 for PVS1 criterion assessment on VEP-annotated VCF.
# Provide either autopvs_data_folder (local path) or autopvs_data_tar_gz (will be extracted).
task RunAutoPVS1 {
  input {
    File input_vcf

    String? autopvs_data_folder
    File? autopvs_data_tar_gz
    Float? autopvs_data_uncompressed_gb

    String target_prefix

    String genome_version = "hg38"

    Int memory_gb = 20
    Int cpu = 1
    String docker = "shengqh/autopvs1:20260401"
  }

  Boolean use_local_data = defined(autopvs_data_folder)
  Float tar_gz_gb = if defined(autopvs_data_tar_gz) then size(select_first([autopvs_data_tar_gz]), "GB") else 0
  Float true_data_gb = if defined(autopvs_data_uncompressed_gb) then select_first([autopvs_data_uncompressed_gb]) else tar_gz_gb * 5
  Int disk_size = ceil(size(input_vcf, "GB") * 3 + tar_gz_gb + true_data_gb) + 20

  command <<<
set -e

if [[ "~{use_local_data}" == "true" ]]; then
  AUTOPVS1_DATA_DIR="~{autopvs_data_folder}"
elif [[ -n "~{autopvs_data_tar_gz}" ]]; then
  echo "Extracting AutoPVS1 data..."
  tar -xzf ~{autopvs_data_tar_gz}
  AUTOPVS1_DATA_DIR=$(basename "~{autopvs_data_tar_gz}" .tar.gz)
  echo "AutoPVS1 data extracted."
else
  echo "ERROR: Either autopvs_data_folder or autopvs_data_tar_gz must be provided." >&2
  exit 1
fi

# Generate config.ini dynamically with actual data folder path
cat > config.ini <<EOF
[DEFAULT]
pvs1levels = ${AUTOPVS1_DATA_DIR}/PVS1.level
gene_alias = ${AUTOPVS1_DATA_DIR}/hgnc.symbol.previous.tsv
gene_trans = ${AUTOPVS1_DATA_DIR}/clinvar_trans_stats.tsv

[HG38]
genome = ${AUTOPVS1_DATA_DIR}/hg38.fa
transcript = ${AUTOPVS1_DATA_DIR}/ncbiRefSeq_hg38.gpe
domain = ${AUTOPVS1_DATA_DIR}/functional_domains_hg38.bed
hotspot = ${AUTOPVS1_DATA_DIR}/mutational_hotspots_hg38.bed
curated_region = ${AUTOPVS1_DATA_DIR}/expert_curated_domains_hg38.bed
exon_lof_popmax = ${AUTOPVS1_DATA_DIR}/exon_lof_popmax_hg38.bed
pathogenic_site = ${AUTOPVS1_DATA_DIR}/clinvar_pathogenic_GRCh38.vcf
EOF

echo "autopvs1_start=$(date)"

python3 /opt/D3b-autoPVS1-2.0.0/autoPVS1_from_VEP_vcf.py \
  --genome_version ~{genome_version} \
  --vep_vcf ~{input_vcf} > ~{target_prefix}.autopvs1.txt

status=$?

if [[ "~{use_local_data}" != "true" ]]; then
  rm -rf $AUTOPVS1_DATA_DIR
fi

if [ $status -ne 0 ]; then
  rm -f ~{target_prefix}.autopvs1.txt
  echo "ERROR: AutoPVS1 script failed with exit code $status" >&2
  exit $status
fi

echo "autopvs1_end=$(date)"
>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File autopvs1_file = "~{target_prefix}.autopvs1.txt"
  }
}


# Run AutoGVP to combine VEP, InterVar, ANNOVAR, and AutoPVS1 annotations
# with ClinVar data for final pathogenicity assessment.
task RunAutoGVP {
  input {
    File vep_vcf
    File intervar_file
    File annovar_file
    File autopvs1_file

    File clinvar_vcf
    File selected_clinvar_submissions
    File variant_summary
    File submission_summary

    String target_prefix

    Int memory_gb = 20
    Int cpu = 1
    String docker = "shengqh/autogvp:v1.0.5"
  }

  Int disk_size = ceil(size([vep_vcf, intervar_file, annovar_file, autopvs1_file, clinvar_vcf, selected_clinvar_submissions, variant_summary, submission_summary], "GB") * 3) + 20

  command <<<
set -e

echo "autogvp_start=$(date)"

bash /rocker-build/AutoGVP/run_autogvp.sh \
  --workflow custom \
  --vcf ~{vep_vcf} \
  --clinvar ~{clinvar_vcf} \
  --intervar ~{intervar_file} \
  --multianno ~{annovar_file} \
  --autopvs1 ~{autopvs1_file} \
  --outdir . \
  --out ~{target_prefix} \
  --selected_clinvar_submissions ~{selected_clinvar_submissions} \
  --variant_summary ~{variant_summary} \
  --submission_summary ~{submission_summary}

status=$?

rm -rf ~{target_prefix}-autogvp-annotated-full.tsv

if [ $status -ne 0 ]; then
  rm -f ~{target_prefix}-autogvp-annotated-full.tsv ~{target_prefix}-autogvp-annotated-abridged.tsv
  echo "ERROR: run_autogvp.sh script failed with exit code $status" >&2
  exit $status
fi

echo "autogvp_end=$(date)"
>>>

  runtime {
    docker: docker
    preemptible: 1
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File autogvp_abridged_file = "~{target_prefix}-autogvp-annotated-abridged.tsv"
  }
}
