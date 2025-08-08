version 1.0

task FilterPassVariantsInPgen {
  input{
    File input_pgen
    File input_pvar
    File input_psam

    String output_prefix

    String docker = "shengqh/plink_1.9_2.0:20250304"

    Int preemptible=1
    Int memory_gb = 15
    Int addtional_disk_space_gb = 10
  }

  Int disk_size = ceil(size([input_pgen, input_psam, input_pvar], "GB") * 2) + addtional_disk_space_gb

  String target_pgen = "~{output_prefix}.pgen"
  String target_pvar = "~{output_prefix}.pvar"
  String target_psam = "~{output_prefix}.psam"

  command <<<

awk '$7 == "PASS" || $1 ~ /^#/' ~{input_pvar} > filter.pvar
plink2  --pgen ~{input_pgen} \
        --pvar ~{input_pvar} \
        --psam ~{input_psam} \
        --extract filter.pvar \
        --make-pgen \
        --out ~{output_prefix}  

grep -v "^#" ~{target_psam} | wc -l | cut -d ' ' -f 1 > num_samples.txt
grep -v "^#" ~{target_pvar} | wc -l | cut -d ' ' -f 1 > num_variants.txt

>>>

  runtime{
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
   }

  output{
    File output_pgen = "~{target_pgen}"
    File output_pvar = "~{target_pvar}"
    File output_psam = "~{target_psam}"

    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")
  }
}

task SamplingVariantsInPgen {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    String output_prefix

    Int max_num_variants
    Int seed=20241227

    String docker = "shengqh/plink_1.9_2.0:20250304"

    Int preemptible=1
    Int memory_gb = 40
    Int cpu = 8
    Int addtional_disk_space_gb = 10
  }

  Int disk_size = ceil(size([input_pgen, input_psam, input_pvar], "GB") * 2) + addtional_disk_space_gb

  String target_pgen = "~{output_prefix}.pgen"
  String target_pvar = "~{output_prefix}.pvar"
  String target_psam = "~{output_prefix}.psam"

  command <<<

#Use the seed, thread, memory parameters for reproducibilty. 

plink2 \
--pgen ~{input_pgen} \
--pvar ~{input_pvar} \
--psam ~{input_psam} \
--thin-count ~{max_num_variants} \
--seed ~{seed} --threads ~{cpu} --memory 8000 require \
--make-pgen \
--out ~{output_prefix}

grep -v "^#" ~{target_psam} | wc -l | cut -d ' ' -f 1 > num_samples.txt
grep -v "^#" ~{target_pvar} | wc -l | cut -d ' ' -f 1 > num_variants.txt

>>>

  runtime {
    cpu: cpu
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
   }

  output {
    File output_pgen = "~{target_pgen}"
    File output_pvar = "~{target_pvar}"
    File output_psam = "~{target_psam}"

    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")
  }
}

task MergePgenFiles {
  input {
    Array[File] input_pgen_files
    Array[File] input_pvar_files
    Array[File] input_psam_files

    String output_prefix

    String? plink2_option

    Int memory_gb = 20
    Int cpu = 8

    String docker = "shengqh/plink_1.9_2.0:20250304"
  }

  Int disk_size = ceil((size(input_pgen_files, "GB") + size(input_pvar_files, "GB") + size(input_psam_files, "GB"))  * 3) + 20

  String target_pgen = output_prefix + ".pgen"
  String target_pvar = output_prefix + ".pvar"
  String target_psam = output_prefix + ".psam"

  String merged_pgen = output_prefix + "-merge.pgen"
  String merged_pvar = output_prefix + "-merge.pvar"
  String merged_psam = output_prefix + "-merge.psam"

  command <<<

cat ~{write_lines(input_pgen_files)} > pgen.list
cat ~{write_lines(input_pvar_files)} > pvar.list
cat ~{write_lines(input_psam_files)} > psam.list

paste pgen.list pvar.list psam.list > merge.list

plink2 ~{plink2_option} --pmerge-list merge.list --make-pgen --out ~{output_prefix} --threads ~{cpu}

rm -f ~{target_pgen} ~{target_pvar} ~{target_psam}

mv ~{merged_pgen} ~{target_pgen}
mv ~{merged_pvar} ~{target_pvar}
mv ~{merged_psam} ~{target_psam}

grep -v "^#" ~{target_psam} | wc -l | cut -d ' ' -f 1 > num_samples.txt
grep -v "^#" ~{target_pvar} | wc -l | cut -d ' ' -f 1 > num_variants.txt

>>>

  runtime {
    cpu: cpu
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_pgen = target_pgen
    File output_pvar = target_pvar
    File output_psam = target_psam

    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")
  }
}

task PgenFilter {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    File? keep_psam
    File? keep_pvar
    File? keep_bed
    File? keep_variant_ids
    
    String output_prefix

    String plink2_filter_option

    Int memory_gb = 15

    String docker = "shengqh/plink_1.9_2.0:20250304"
  }

  Int disk_size = ceil(size([input_pgen, input_pvar, input_psam], "GB")  * 2) + 5

  String target_pgen = output_prefix + ".pgen"
  String target_pvar = output_prefix + ".pvar"
  String target_psam = output_prefix + ".psam"

  command <<<

if [[ "~{keep_pvar}" != "" ]]; then
  grep -v "^#" ~{keep_pvar} | cut -f 3 > keep_variant_ids.txt

  plink2 \
    --pgen ~{input_pgen} \
    --pvar ~{input_pvar} \
    --psam ~{input_psam} \
    ~{plink2_filter_option} \
    ~{"--keep " + keep_psam} \
    ~{"--extract bed0 " + keep_bed} \
    --extract keep_variant_ids.txt \
    --make-pgen \
    --out ~{output_prefix}
else
  if [[ "~{keep_variant_ids}" != "" ]]; then
    plink2 \
      --pgen ~{input_pgen} \
      --pvar ~{input_pvar} \
      --psam ~{input_psam} \
      ~{plink2_filter_option} \
      ~{"--keep " + keep_psam} \
      ~{"--extract bed0 " + keep_bed} \
      --extract ~{keep_variant_ids} \
      --make-pgen \
      --out ~{output_prefix}
  else
    plink2 \
      --pgen ~{input_pgen} \
      --pvar ~{input_pvar} \
      --psam ~{input_psam} \
      ~{plink2_filter_option} \
      ~{"--keep " + keep_psam} \
      ~{"--extract bed0 " + keep_bed} \
      --make-pgen \
      --out ~{output_prefix}
  fi
fi

grep -v "^#" ~{target_psam} | wc -l | cut -d ' ' -f 1 > num_samples.txt
grep -v "^#" ~{target_pvar} | wc -l | cut -d ' ' -f 1 > num_variants.txt

>>>

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_pgen = target_pgen
    File output_pvar = target_pvar
    File output_psam = target_psam

    Int num_samples = read_int("num_samples.txt")
    Int num_variants = read_int("num_variants.txt")
  }
}

task Pgen2Vcf {
  input {
    File input_pgen
    File input_pvar
    File input_psam
    
    String? plink2_option

    String output_prefix
    
    String docker = "shengqh/plink_1.9_2.0:20250304"
    Int? memory_gb_override
    Int? disk_size_override
  }

  Int pgen_file_size = ceil(size([input_pgen, input_pvar, input_psam], "GB"))
  Int disk_size = select_first([disk_size_override, pgen_file_size * 3 + 20])
  Int memory_gb = select_first([memory_gb_override, pgen_file_size * 3])

  command <<<

plink2 ~{plink2_option} \
  --pgen ~{input_pgen} \
  --pvar ~{input_pvar} \
  --psam ~{input_psam} \
  --export vcf bgz id-paste=iid \
  --out ~{output_prefix} 

>>>

  runtime {
    docker: docker
    preemptible: 3
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_vcf = "~{output_prefix}.vcf.gz"
  }
}

task KeepSampleWithVariant {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    Boolean is_agd_data = true

    String output_prefix

    Int memory_gb = 13
    String docker = "shengqh/plink_1.9_2.0:20250304"
  }

  Int disk_size = ceil(size([input_pgen, input_pvar, input_psam], "GB")  * 2) + 2

  String grep_option = if is_agd_data then "| grep -v '^HG00' | grep -v '_INVALID'" else ""

  String target_pgen = output_prefix + ".pgen"
  String target_pvar = output_prefix + ".pvar"
  String target_psam = output_prefix + ".psam"

  command <<<

plink2  --pgen ~{input_pgen} \
        --pvar ~{input_pvar} \
        --psam ~{input_psam} \
        --sample-counts \
        --out sample_geno_counts
 
awk -F'\t' '$4 > 0 || $5 > 0 {print $1,$2}' sample_geno_counts.scount ~{grep_option} > samples_with_variants.pvar

plink2  --pgen ~{input_pgen} \
        --pvar ~{input_pvar} \
        --psam ~{input_psam} \
        --keep samples_with_variants.pvar \
        --make-pgen \
        --out ~{output_prefix}

grep -v "^#" ~{target_psam} | wc -l | cut -d ' ' -f 1 > num_samples.txt
grep -v "^#" ~{target_pvar} | wc -l | cut -d ' ' -f 1 > num_variants.txt

plink2 --pfile ~{output_prefix} --freq --out ~{output_prefix}
mv ~{output_prefix}.afreq ~{output_prefix}.afreq.txt

>>>

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_pgen = target_pgen
    File output_pvar = target_pvar
    File output_psam = target_psam
    Int output_num_samples = read_int("num_samples.txt")
    Int output_num_variants = read_int("num_variants.txt")
    File output_allele_freq = "~{output_prefix}.afreq.txt"
  }
}