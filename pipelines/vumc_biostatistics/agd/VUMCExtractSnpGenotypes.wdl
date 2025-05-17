version 1.0

import "../../../tasks/vumc_biostatistics/Plink2Utils.wdl" as Plink2Utils
import "../../../tasks/vumc_biostatistics/BioUtils.wdl" as BioUtils
import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "../annotation/VUMCAnnovar.wdl" as VUMCAnnovar

workflow VUMCExtractSnpGenotypes {
  input {
    File? input_rsid_file
    String? input_rsids

    String output_prefix

    Array[String] chromosomes
    Array[File] input_pgen_files
    Array[File] input_psam_files
    Array[File] input_pvar_files

    String? billing_gcp_project_id
    String? target_gcp_folder    
  }

  call BioUtils.ConvertRsidToBed {
    input:
      input_rsid_file = input_rsid_file,
      input_rsids = input_rsids,
      output_prefix = output_prefix
  }

  call BioUtils.GetChromosomeIndecies {
    input:
      input_chromosomes = chromosomes,
      input_bed_file = ConvertRsidToBed.output_bed
  }

  Int num_valid_chromsome = length(GetChromosomeIndecies.chromosome_indecies)

  scatter(chrom_ind in range(num_valid_chromsome)){
    Int old_ind = GetChromosomeIndecies.chromosome_indecies[chrom_ind]
    File pgen_file = input_pgen_files[old_ind]
    File pvar_file = input_pvar_files[old_ind]
    File psam_file = input_psam_files[old_ind]
    String chromosome = chromosomes[old_ind]

    call Plink2Utils.Plink2FilterPgen as Plink2FilterPgen {
      input:
        input_pgen = pgen_file,
        input_pvar = pvar_file,
        input_psam = psam_file,
        keep_bed = ConvertRsidToBed.output_bed,
        plink2_filter_option = "",
        output_prefix = output_prefix + "." + chromosome + ".snp"
    }
  }

  if (num_valid_chromsome > 1){
    call Plink2Utils.MergePgenFiles as MergePgenFiles {
      input:
        input_pgen_files = Plink2FilterPgen.output_pgen,
        input_pvar_files = Plink2FilterPgen.output_pvar,
        input_psam_files = Plink2FilterPgen.output_psam,
        output_prefix = output_prefix + ".snp"
    }
  }

  if (num_valid_chromsome <= 1){
    File only_pgen = Plink2FilterPgen.output_pgen[0]
    File only_pvar = Plink2FilterPgen.output_pvar[0]
    File only_psam = Plink2FilterPgen.output_psam[0]
    Int only_num_variants = Plink2FilterPgen.num_variants[0]
  }

  Int cur_num_variants = select_first([MergePgenFiles.num_variants, only_num_variants])

  call Plink2Utils.Pgen2Vcf {
    input:
      input_pgen = select_first([MergePgenFiles.output_pgen, only_pgen]),
      input_pvar = select_first([MergePgenFiles.output_pvar, only_pvar]),
      input_psam = select_first([MergePgenFiles.output_psam, only_psam]),
      output_prefix = output_prefix + ".snp",
  }

  call VUMCAnnovar.Annovar {
    input:
      input_vcf = Pgen2Vcf.output_vcf,
      target_prefix = output_prefix + ".snp",
  }

  call FormatResult {
    input:
      input_bed_file = ConvertRsidToBed.output_bed,
      input_vcf_file = Pgen2Vcf.output_vcf,
      input_annovar_file = Annovar.annovar_file,
      output_prefix = output_prefix + ".snp",
  }

  if (defined(target_gcp_folder)) {
    call GcpUtils.MoveOrCopyFiveFiles {
      input:
        source_file1 = ConvertRsidToBed.output_bed,
        source_file2 = select_first([MergePgenFiles.output_pgen, only_pgen]),
        source_file3 = select_first([MergePgenFiles.output_pvar, only_pvar]),
        source_file4 = select_first([MergePgenFiles.output_psam, only_psam]),
        source_file5 = FormatResult.output_genotype_csv,
        is_move_file = false,
        project_id = billing_gcp_project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_bed = select_first([MoveOrCopyFiveFiles.output_file1, ConvertRsidToBed.output_bed])
    File output_pgen = select_first([MoveOrCopyFiveFiles.output_file2, MergePgenFiles.output_pgen, only_pgen])
    File output_pvar = select_first([MoveOrCopyFiveFiles.output_file3, MergePgenFiles.output_pvar, only_pvar])
    File output_psam = select_first([MoveOrCopyFiveFiles.output_file4, MergePgenFiles.output_psam, only_psam])
    File output_genotype_csv = select_first([MoveOrCopyFiveFiles.output_file5, FormatResult.output_genotype_csv])
    Int output_num_variants = select_first([MergePgenFiles.num_variants, only_num_variants])
  }
}

task FormatResult {
  input {
    File input_bed_file
    File input_vcf_file
    File input_annovar_file
    String output_prefix
    String docker = "shengqh/report:20241120"
  }

  Int disk_size = ceil(size([input_bed_file, input_vcf_file, input_annovar_file], "GB")) + 10

  command <<<

zcat ~{input_vcf_file} | grep -v "^##" | cut -f7- > request.clean
zcat ~{input_annovar_file} > annovar.clean

paste annovar.clean request.clean > request.annovar.final.tsv

cat > transpose.r << 'EOF'

bed_file="~{input_bed_file}"
annovar_file="request.annovar.final.tsv"
output_file="~{output_prefix}.annovar.final.transposed.csv"

library(data.table)
library(dplyr)

fbed=fread(bed_file, sep="\t", data.table=FALSE) |>
  dplyr::rename(
    Chr=V1,
    Start=V2,
    End=V3,
    Rsid=V4
  ) |>
  dplyr::mutate(
    Start=Start + 1,
    Locus= paste0(Chr, ":", Start)
  ) |>
  dplyr::select(Locus, Rsid)

fdat=fread(annovar_file, sep="\t", data.table=FALSE) |>
  dplyr::mutate(Chr=paste0("chr", Chr),
                Locus= paste0(Chr, ":", Start)) 

fcomb = merge(fdat, fbed, by="Locus", all.x=TRUE) |> 
  dplyr::select(-Locus, -GeneDetail.refGene, -ExonicFunc.refGene, -AAChange.refGene, -FILTER, -INFO, -FORMAT) |>
  tibble::column_to_rownames("Rsid") 

mdat=t(fcomb)

write.csv(mdat, output_file, row.names=TRUE)

EOF

R -f transpose.r

  >>>

  runtime {
    docker: docker
    preemptible: 1
    disks: "local-disk " + disk_size + " HDD"
    memory: "10 GiB"
  }

  output {
    File output_genotype_csv = "~{output_prefix}.annovar.final.transposed.csv"
  }
}