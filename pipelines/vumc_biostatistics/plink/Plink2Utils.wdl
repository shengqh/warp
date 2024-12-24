version 1.0

task FilterPassVariantsInPgen {
  input{
    File input_pgen
    File input_pvar
    File input_psam

    String target_prefix

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"

    #when has_pass is false, this task will just rename pgen files
    Boolean has_pass=true

    Int preemptible=1
    Int memory_gb = 40
    Int addtional_disk_space_gb = 10
  }

  Int disk_size = ceil(size([input_pgen, input_psam, input_pvar], "GB") * 2) + addtional_disk_space_gb

  String target_pgen = "~{target_prefix}.pgen"
  String target_pvar = "~{target_prefix}.pvar"
  String target_psam = "~{target_prefix}.psam"

  command <<<

if [[ "~{has_pass}" == "true" ]]; then
  awk '$7 == "PASS" || $1 ~ /^#/' ~{input_pvar} > filter.pvar
  plink2  --pgen ~{input_pgen} \
          --pvar ~{input_pvar} \
          --psam ~{input_psam} \
          --extract filter.pvar \
          --make-pgen \
          --out ~{target_prefix}  
else
  mv ~{input_pgen} ~{target_pgen} 
  mv ~{input_pvar} ~{target_pvar}
  mv ~{input_psam} ~{target_psam}
fi

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

