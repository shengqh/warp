version 1.0

import "../../../../broad/dna_seq/germline/joint_genotyping/JointGenotyping.wdl" as BroadJointGenotyping
import "../../../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils


workflow VUMCJointGenotyping {
  input {
    File unpadded_intervals_file

    String callset_name
    File sample_name_map

    File ref_fasta
    File ref_fasta_index
    File ref_dict

    File dbsnp_vcf
    File dbsnp_vcf_index

    Int small_disk
    Int medium_disk
    Int large_disk
    Int huge_disk

    Array[String]? snp_recalibration_tranche_values
    Array[String] snp_recalibration_annotation_values
    Array[String]? indel_recalibration_tranche_values
    Array[String]? indel_recalibration_annotation_values

    File haplotype_database

    File eval_interval_list
    File hapmap_resource_vcf
    File hapmap_resource_vcf_index
    File omni_resource_vcf
    File omni_resource_vcf_index
    File one_thousand_genomes_resource_vcf
    File one_thousand_genomes_resource_vcf_index
    File mills_resource_vcf
    File mills_resource_vcf_index
    File axiomPoly_resource_vcf
    File axiomPoly_resource_vcf_index
    File dbsnp_resource_vcf = dbsnp_vcf
    File dbsnp_resource_vcf_index = dbsnp_vcf_index

    # ExcessHet is a phred-scaled p-value. We want a cutoff of anything more extreme
    # than a z-score of -4.5 which is a p-value of 3.4e-06, which phred-scaled is 54.69
    Float excess_het_threshold = 54.69
    Float? vqsr_snp_filter_level
    Float? vqsr_indel_filter_level
    Int? snp_vqsr_downsampleFactor
    File? targets_interval_list

    Int? top_level_scatter_count
    Boolean? gather_vcfs
    Int snps_variant_recalibration_threshold = 500000
    Boolean rename_gvcf_samples = true
    Float unbounded_scatter_count_scale_factor = 0.15
    Int gnarly_scatter_count = 10
    Boolean use_gnarly_genotyper = false
    Boolean use_allele_specific_annotations = true # only applicabale to VQSR
    Boolean cross_check_fingerprints = true
    Boolean scatter_cross_check_fingerprints = false
    Boolean run_vets = false

    String? target_gcp_folder    
  }

  call BroadJointGenotyping.JointGenotyping {
    input:
      unpadded_intervals_file = unpadded_intervals_file,

      callset_name = callset_name,
      sample_name_map = sample_name_map,

      ref_fasta = ref_fasta,
      ref_fasta_index = ref_fasta_index,
      ref_dict = ref_dict,

      dbsnp_vcf = dbsnp_vcf,
      dbsnp_vcf_index = dbsnp_vcf_index,

      small_disk = small_disk,
      medium_disk = medium_disk,
      large_disk = large_disk,
      huge_disk = huge_disk,    
      snp_recalibration_tranche_values = snp_recalibration_tranche_values,
      snp_recalibration_annotation_values = snp_recalibration_annotation_values,
      indel_recalibration_tranche_values = indel_recalibration_tranche_values,
      indel_recalibration_annotation_values = indel_recalibration_annotation_values,  

      haplotype_database = haplotype_database,    

      eval_interval_list = eval_interval_list,
      hapmap_resource_vcf = hapmap_resource_vcf,
      hapmap_resource_vcf_index = hapmap_resource_vcf_index,
      omni_resource_vcf = omni_resource_vcf,
      omni_resource_vcf_index = omni_resource_vcf_index,
      one_thousand_genomes_resource_vcf = one_thousand_genomes_resource_vcf,
      one_thousand_genomes_resource_vcf_index = one_thousand_genomes_resource_vcf_index,
      mills_resource_vcf = mills_resource_vcf,
      mills_resource_vcf_index = mills_resource_vcf_index,
      axiomPoly_resource_vcf = axiomPoly_resource_vcf,
      axiomPoly_resource_vcf_index = axiomPoly_resource_vcf_index,
      dbsnp_resource_vcf = dbsnp_resource_vcf,
      dbsnp_resource_vcf_index = dbsnp_resource_vcf_index,

      excess_het_threshold = excess_het_threshold,
      vqsr_snp_filter_level = vqsr_snp_filter_level,
      vqsr_indel_filter_level = vqsr_indel_filter_level,
      snp_vqsr_downsampleFactor = snp_vqsr_downsampleFactor,
      targets_interval_list = targets_interval_list,

      top_level_scatter_count = top_level_scatter_count,
      gather_vcfs = gather_vcfs,
      snps_variant_recalibration_threshold = snps_variant_recalibration_threshold,
      rename_gvcf_samples = rename_gvcf_samples,
      unbounded_scatter_count_scale_factor = unbounded_scatter_count_scale_factor,
      gnarly_scatter_count = gnarly_scatter_count,
      use_gnarly_genotyper = use_gnarly_genotyper,
      use_allele_specific_annotations = use_allele_specific_annotations,
      cross_check_fingerprints = cross_check_fingerprints,
      scatter_cross_check_fingerprints = scatter_cross_check_fingerprints,
      run_vets = run_vets
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyFiles as CopyFile1 {
      input:
        source_file1 = JointGenotyping.detail_metrics_file,
        source_file2 = JointGenotyping.summary_metrics_file,
        source_file3 = JointGenotyping.crosscheck_fingerprint_check,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }

    call GcpUtils.MoveOrCopyFileArray as CopyFile2 {
      input:
        source_files = JointGenotyping.output_vcfs,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }

    call GcpUtils.MoveOrCopyFileArray as CopyFile3 {
      input:
        source_files = JointGenotyping.output_vcf_indices,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }

    call GcpUtils.MoveOrCopyFileArray as CopyFile4 {
      input:
        source_files = JointGenotyping.output_intervals,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    # Metrics from either the small or large callset
    File detail_metrics_file = select_first([CopyFile1.output_file1, JointGenotyping.detail_metrics_file])
    File summary_metrics_file = select_first([CopyFile1.output_file2, JointGenotyping.summary_metrics_file])

    # Outputs from the small callset path through the wdl.
    Array[File] output_vcfs = select_first([CopyFile2.outputFiles, JointGenotyping.output_vcfs])
    Array[File] output_vcf_indices = select_first([CopyFile3.outputFiles, JointGenotyping.output_vcf_indices])

    # Output the interval list generated/used by this run workflow.
    Array[File] output_intervals = select_first([CopyFile4.outputFiles, JointGenotyping.output_intervals])

    # Output the metrics from crosschecking fingerprints.
    File? crosscheck_fingerprint_check = select_first([CopyFile1.output_file3, JointGenotyping.crosscheck_fingerprint_check])
  }
  meta {
    allowNestedInputs: true
  }
}
