version 1.0

## Copyright Broad Institute/VUMC, 2018/2022
##
## This WDL pipeline implements data pre-processing and initial variant calling (GVCF
## generation) according to the GATK Best Practices (June 2016) for germline SNP and
## Indel discovery in human whole-genome data.
##
## Requirements/expectations :
## - Human whole-genome pair-end sequencing data in FASTQ format
## - GVCF output names must end in ".g.vcf.gz"
## - Reference genome must be Hg38 with ALT contigs
##
## Runtime parameters are optimized for Broad's Google Cloud Platform implementation.
## For program versions, see docker containers.
##
## LICENSING :
## This script is released under the WDL source code license (BSD-3) (see LICENSE in
## https://github.com/broadinstitute/wdl). Note however that the programs it calls may
## be subject to different licenses. Users are responsible for checking that they are
## authorized to run all programs before running this script. Please see the docker
## page at https://hub.docker.com/r/broadinstitute/genomes-in-the-cloud/ for detailed
## licensing information pertaining to the included programs.

## By Quanhu Sheng
## Some samples always failed at QC converting unmapped bam to bam, so I remove the following tasks:
## CollectUnsortedReadgroupBamQualityMetrics
## CrossCheckFingerprints
## CheckContamination

import "../../../../../../tasks/vumc_biostatistics/PairedFastQsToUnmappedBAM.wdl" as ToUnmappedBam
import "./VUMCExomeGermlineSingleSample.wdl" as VUMCPipeline
import "../../../../../../structs/dna_seq/DNASeqStructs.wdl"
import "./VUMCMoveSingleSampleWESResultLessQC.wdl" as MoveResults

# WORKFLOW DEFINITION
workflow VUMCExomeGermlineSingleSampleFromFastqLessQC {

  #String pipeline_version = "3.1.10"

  input {
    # Optional for VUMC pipeline
    String? genoset

    String sample_name 
    File fastq_1 
    File fastq_2 
    String readgroup_name 
    String library_name 
    String platform_unit 
    String platform_name 
    String? run_date 
    String? sequencing_center 

    # Optional for BROAD pipeline
    PapiSettings papi_settings
    DNASeqSingleSampleReferences references
    VariantCallingScatterSettings scatter_settings

    File? fingerprint_genotypes_file
    File? fingerprint_genotypes_index

    File target_interval_list
    File bait_interval_list
    String bait_set_name

    String? project_id
    String? target_gcp_folder
  }

  # Convert pair of FASTQs to uBAM
  call ToUnmappedBam.PairedFastQsToUnmappedBAM {
    input:
      sample_name = sample_name,
      fastq_1 = fastq_1,
      fastq_2 = fastq_2,
      readgroup_name = readgroup_name,
      library_name = library_name,
      platform_unit = platform_unit,
      run_date = run_date,
      platform_name = platform_name,
      sequencing_center = sequencing_center,
  }

  SampleAndUnmappedBams sample_and_unmapped_bams = object {
    base_file_name: sample_name,
    final_gvcf_base_name: sample_name,
    flowcell_unmapped_bams: [ PairedFastQsToUnmappedBAM.output_unmapped_bam ],
    sample_name: sample_name,
    unmapped_bam_suffix: ".bam"
  }

  call VUMCPipeline.VUMCExomeGermlineSingleSampleNoQC as broad {
    input:
      cloud_provider = "gcp",
      papi_settings = papi_settings,
      sample_and_unmapped_bams = sample_and_unmapped_bams,
      references = references,
      scatter_settings = scatter_settings,
      fingerprint_genotypes_file = fingerprint_genotypes_file,
      fingerprint_genotypes_index = fingerprint_genotypes_index,
      target_interval_list = target_interval_list,
      bait_interval_list = bait_interval_list,
      bait_set_name = bait_set_name,
  }

  if(defined(target_gcp_folder)){
    call MoveResults.VUMCMoveSingleSampleWESResultLessQC as mf {
      input:
        target_bucket = select_first([target_gcp_folder]),
        project_id = project_id,
        genoset = select_first([genoset]),
        GRID = sample_name,

        quality_yield_metrics = broad.quality_yield_metrics,

        read_group_alignment_summary_metrics = broad.read_group_alignment_summary_metrics,

        calculate_read_group_checksum_md5 = broad.calculate_read_group_checksum_md5,

        agg_alignment_summary_metrics = broad.agg_alignment_summary_metrics,
        agg_bait_bias_detail_metrics = broad.agg_bait_bias_detail_metrics,
        agg_bait_bias_summary_metrics = broad.agg_bait_bias_summary_metrics,
        agg_insert_size_histogram_pdf = broad.agg_insert_size_histogram_pdf,
        agg_insert_size_metrics = broad.agg_insert_size_metrics,
        agg_pre_adapter_detail_metrics = broad.agg_pre_adapter_detail_metrics,
        agg_pre_adapter_summary_metrics = broad.agg_pre_adapter_summary_metrics,
        agg_quality_distribution_pdf = broad.agg_quality_distribution_pdf,
        agg_quality_distribution_metrics = broad.agg_quality_distribution_metrics,
        agg_error_summary_metrics = broad.agg_error_summary_metrics,

        duplicate_metrics = broad.duplicate_metrics,
        output_bqsr_reports = broad.output_bqsr_reports,

        gvcf_summary_metrics = broad.gvcf_summary_metrics,
        gvcf_detail_metrics = broad.gvcf_detail_metrics,

        hybrid_selection_metrics = broad.hybrid_selection_metrics,

        output_cram = broad.output_cram,
        output_cram_index = broad.output_cram_index,
        output_cram_md5 = broad.output_cram_md5,

        validate_cram_file_report = broad.validate_cram_file_report,

        output_vcf = broad.output_vcf,
        output_vcf_index = broad.output_vcf_index,
    }
  }

  # Outputs that will be retained when execution is complete
  output {
    Array[File] quality_yield_metrics = select_first([mf.target_quality_yield_metrics, broad.quality_yield_metrics])

    File read_group_alignment_summary_metrics = select_first([mf.target_read_group_alignment_summary_metrics, broad.read_group_alignment_summary_metrics])

    File calculate_read_group_checksum_md5 = select_first([mf.target_calculate_read_group_checksum_md5, broad.calculate_read_group_checksum_md5])

    File agg_alignment_summary_metrics = select_first([mf.target_agg_alignment_summary_metrics, broad.agg_alignment_summary_metrics])
    File agg_bait_bias_detail_metrics = select_first([mf.target_agg_bait_bias_detail_metrics, broad.agg_bait_bias_detail_metrics])
    File agg_bait_bias_summary_metrics = select_first([mf.target_agg_bait_bias_summary_metrics, broad.agg_bait_bias_summary_metrics])
    File agg_insert_size_histogram_pdf = select_first([mf.target_agg_insert_size_histogram_pdf, broad.agg_insert_size_histogram_pdf])
    File agg_insert_size_metrics = select_first([mf.target_agg_insert_size_metrics, broad.agg_insert_size_metrics])
    File agg_pre_adapter_detail_metrics = select_first([mf.target_agg_pre_adapter_detail_metrics, broad.agg_pre_adapter_detail_metrics])
    File agg_pre_adapter_summary_metrics = select_first([mf.target_agg_pre_adapter_summary_metrics, broad.agg_pre_adapter_summary_metrics])
    File agg_quality_distribution_pdf = select_first([mf.target_agg_quality_distribution_pdf, broad.agg_quality_distribution_pdf])
    File agg_quality_distribution_metrics = select_first([mf.target_agg_quality_distribution_metrics, broad.agg_quality_distribution_metrics])
    File agg_error_summary_metrics = select_first([mf.target_agg_error_summary_metrics, broad.agg_error_summary_metrics])

    File duplicate_metrics = select_first([mf.target_duplicate_metrics, broad.duplicate_metrics])
    File? output_bqsr_reports = select_first([mf.target_output_bqsr_reports, broad.output_bqsr_reports])

    File gvcf_summary_metrics = select_first([mf.target_gvcf_summary_metrics, broad.gvcf_summary_metrics])
    File gvcf_detail_metrics = select_first([mf.target_gvcf_detail_metrics, broad.gvcf_detail_metrics])

    File hybrid_selection_metrics = select_first([mf.target_hybrid_selection_metrics, broad.hybrid_selection_metrics])

    File output_cram = select_first([mf.target_output_cram, broad.output_cram])
    File output_cram_index = select_first([mf.target_output_cram_index, broad.output_cram_index])
    File output_cram_md5 = select_first([mf.target_output_cram_md5, broad.output_cram_md5])

    File validate_cram_file_report = select_first([mf.target_validate_cram_file_report, broad.validate_cram_file_report])

    File output_vcf = select_first([mf.target_output_vcf, broad.output_vcf])
    File output_vcf_index = select_first([mf.target_output_vcf_index, broad.output_vcf_index])
  }
  meta {
    allowNestedInputs: true
  }
}

