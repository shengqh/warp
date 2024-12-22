version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCFilterPassVariantsInPgen {
  input {
    File input_pgen
    File input_pvar
    File input_psam

    String target_prefix

    String? project_id
    String? target_gcp_folder
  }
  
  call FilterPassVariantsInPgen {
    input: 
      input_pgen = input_pgen,
      input_pvar = input_pvar,
      input_psam = input_psam,

      target_prefix = target_prefix
  }

  if(defined(target_gcp_folder)){
    String filtered_pgen = "~{FilterPassVariantsInPgen.output_pgen}"
    String filtered_pvar = "~{FilterPassVariantsInPgen.output_pvar}"
    String filtered_psam = "~{FilterPassVariantsInPgen.output_psam}"

    call GcpUtils.MoveOrCopyThreeFiles as CopyFile {
      input:
        source_file1 = filtered_pgen,
        source_file2 = filtered_pvar,
        source_file3 = filtered_psam,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_pgen = select_first([CopyFile.output_file1, FilterPassVariantsInPgen.output_pgen])
    File output_pvar = select_first([CopyFile.output_file2, FilterPassVariantsInPgen.output_pvar])
    File output_psam = select_first([CopyFile.output_file3, FilterPassVariantsInPgen.output_psam])

    Int num_samples = FilterPassVariantsInPgen.num_samples
    Int num_variants = FilterPassVariantsInPgen.num_variants
  }
}

task FilterPassVariantsInPgen {
  input{
    File input_pgen
    File input_pvar
    File input_psam

    String target_prefix

    String docker = "hkim298/plink_1.9_2.0:20230116_20230707"

    Int preemptible=1
    Int memory_gb = 4
    Int addtional_disk_space_gb = 10
  }

  Int disk_size = ceil(size([input_pgen, input_psam, input_pvar], "GB") * 2) + addtional_disk_space_gb

  String target_pgen = "~{target_prefix}.PASS.pgen"
  String target_pvar = "~{target_prefix}.PASS.pvar"
  String target_psam = "~{target_prefix}.PASS.psam"

  command <<<

awk '$7 == "PASS" || $1 ~ /^#/' ~{input_pvar} > filter.pvar

plink2  --pgen ~{input_pgen} \
        --pvar ~{input_pvar} \
        --psam ~{input_psam} \
        --extract filter.pvar \
        --make-pgen \
        --out ~{target_prefix}.PASS

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

