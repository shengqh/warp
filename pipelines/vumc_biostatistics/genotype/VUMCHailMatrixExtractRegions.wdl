version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCHailMatrixExtractRegions {
  input {
    File input_hail_mt_path_file
    File input_bed

    Float expect_output_vcf_bgz_size_gb

    String target_prefix

    String billing_project_id
    String? target_gcp_folder
  }

  call HailMatrixExtractRegions {
    input:
      input_hail_mt_path_file = input_hail_mt_path_file,
      expect_output_vcf_bgz_size_gb = expect_output_vcf_bgz_size_gb,
      input_bed = input_bed,
      target_prefix = target_prefix,
      billing_project_id = billing_project_id
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = HailMatrixExtractRegions.output_vcf,
        is_move_file = false,
        project_id = billing_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_vcf = select_first([CopyFile.output_file, HailMatrixExtractRegions.output_vcf])
  }
}

task HailMatrixExtractRegions {
  input {
    File input_hail_mt_path_file
    File input_bed
    Float expect_output_vcf_bgz_size_gb
    String target_prefix
    String billing_project_id

    String docker = "shengqh/hail_gcp:20240211"
    Int memory_gb = 10
    Int preemptible = 1
    Int cpu = 4
    Int boot_disk_gb = 10  
  }

  Int disk_size = ceil(expect_output_vcf_bgz_size_gb) + 20
  Int total_memory_gb = memory_gb + 2

  String target_file = target_prefix + ".vcf.bgz"

  command <<<

#https://discuss.hail.is/t/i-get-a-negativearraysizeexception-when-loading-a-plink-file/899
export PYSPARK_SUBMIT_ARGS="--driver-java-options '-XX:hashCode=0' --conf 'spark.executor.extraJavaOptions=-XX:hashCode=0' pyspark-shell"

python3 <<CODE

import hail as hl
import pandas as pd

def parse_gcs_url(gcs_url):
    if not gcs_url.startswith('gs://'):
        raise ValueError("URL must start with 'gs://'")

    # Remove the 'gs://' prefix
    gcs_url = gcs_url[5:]

    # Split the remaining URL into bucket name and object key
    parts = gcs_url.split('/', 1)
    if len(parts) != 2:
        raise ValueError("Invalid GCS URL format")

    bucket_name = parts[0]

    return bucket_name

new_tbl = pd.read_csv("~{input_hail_mt_path_file}", sep='\t')
new_tbl.head()

bucket_name = parse_gcs_url(new_tbl['hail'][0])
print(f"hail_bucket_name={bucket_name}")

regions = pd.read_csv("~{input_bed}", sep='\t', header=None)
regions=regions.iloc[:, :3]
regions.columns = ["chr", "start", "end"]
regions['chr']=regions['chr'].astype(str)
regions['locus']=regions.chr + ":" + (regions.start + 1).astype(str) + "-" + (regions.end + 1).astype(str)
regions.head()

hl.init(spark_conf={"spark.driver.memory": "~{memory_gb}g",
                    'spark.hadoop.fs.gs.requester.pays.mode': 'CUSTOM',
                    'spark.hadoop.fs.gs.requester.pays.buckets': bucket_name,
                    'spark.hadoop.fs.gs.requester.pays.project.id': "~{billing_project_id}"}, idempotent=True)

hl.default_reference("GRCh38")

hail_col="hail"

all_tbl=None
for ind in new_tbl.index:
    chr=new_tbl['chromosome'][ind]
    chr_regions=regions[regions.chr==chr]
    if chr_regions.shape[0] == 0:
        print(f"{chr}: no snps")
    else:
        print(f"{chr}: {chr_regions.shape[0]} snps")
        print(chr_regions)
        hail_url=new_tbl[hail_col][ind]
        mt = hl.read_matrix_table(hail_url)

        mt_filter = hl.filter_intervals(
            mt,
            [hl.parse_locus_interval(x,) for x in chr_regions.locus])
        
        print(f"  {chr} found {mt_filter.count_rows()} snps from hailmatrix")

        if mt_filter.count_rows() > 0:
            if all_tbl == None:
                all_tbl = mt_filter
            else:
                all_tbl = all_tbl.union_rows(mt_filter)

print(f"SNPs={all_tbl.count_rows()}, Samples={all_tbl.count_cols()}")

hl.export_vcf(all_tbl, "~{target_file}")

CODE

>>>

  runtime {
    cpu: cpu
    docker: "~{docker}"
    preemptible: preemptible
    disks: "local-disk ~{disk_size} HDD"
    memory: "~{total_memory_gb} GiB"
    bootDiskSizeGb: boot_disk_gb
  }
  output {
    File output_vcf = "~{target_file}"
  }
}
