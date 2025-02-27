version 1.0

workflow VUMCMoveSingleSampleWESResult {
  input {
    String genoset
    String GRID
    String target_bucket
    String? project_id

    Array[String] quality_yield_metrics

    Array[String] unsorted_read_group_base_distribution_by_cycle_pdf
    Array[String] unsorted_read_group_base_distribution_by_cycle_metrics
    Array[String] unsorted_read_group_insert_size_histogram_pdf
    Array[String] unsorted_read_group_insert_size_metrics
    Array[String] unsorted_read_group_quality_by_cycle_pdf
    Array[String] unsorted_read_group_quality_by_cycle_metrics
    Array[String] unsorted_read_group_quality_distribution_pdf
    Array[String] unsorted_read_group_quality_distribution_metrics

    String read_group_alignment_summary_metrics

    String? cross_check_fingerprints_metrics

    String selfSM

    String calculate_read_group_checksum_md5

    String agg_alignment_summary_metrics
    String agg_bait_bias_detail_metrics
    String agg_bait_bias_summary_metrics
    String agg_insert_size_histogram_pdf
    String agg_insert_size_metrics
    String agg_pre_adapter_detail_metrics
    String agg_pre_adapter_summary_metrics
    String agg_quality_distribution_pdf
    String agg_quality_distribution_metrics
    String agg_error_summary_metrics

    String? fingerprint_summary_metrics
    String? fingerprint_detail_metrics

    String duplicate_metrics
    String? output_bqsr_reports

    String gvcf_summary_metrics
    String gvcf_detail_metrics

    String hybrid_selection_metrics

    String output_cram
    String output_cram_index
    String output_cram_md5

    File validate_cram_file_report

    File output_vcf
    File output_vcf_index
  }

  String gcs_output_dir = sub(target_bucket, "/+$", "")
  String target_folder = "~{gcs_output_dir}/~{genoset}/~{GRID}"

  scatter(file in quality_yield_metrics){
    String moved_quality_yield_metrics_file = "~{target_folder}/~{basename(file)}"
  }
  Array[String] moved_quality_yield_metrics = moved_quality_yield_metrics_file

  scatter(file in unsorted_read_group_base_distribution_by_cycle_pdf){
    String moved_unsorted_read_group_base_distribution_by_cycle_pdf_file = "~{target_folder}/~{basename(file)}"
  }
  Array[String] moved_unsorted_read_group_base_distribution_by_cycle_pdf = moved_unsorted_read_group_base_distribution_by_cycle_pdf_file

  scatter(file in unsorted_read_group_base_distribution_by_cycle_metrics){
    String moved_unsorted_read_group_base_distribution_by_cycle_metrics_file = "~{target_folder}/~{basename(file)}"
  }
  Array[String] moved_unsorted_read_group_base_distribution_by_cycle_metrics = moved_unsorted_read_group_base_distribution_by_cycle_metrics_file

  scatter(file in unsorted_read_group_insert_size_histogram_pdf){
    String moved_unsorted_read_group_insert_size_histogram_pdf_file = "~{target_folder}/~{basename(file)}"
  }
  Array[String] moved_unsorted_read_group_insert_size_histogram_pdf = moved_unsorted_read_group_insert_size_histogram_pdf_file

  scatter(file in unsorted_read_group_insert_size_metrics){
    String moved_unsorted_read_group_insert_size_metrics_file = "~{target_folder}/~{basename(file)}"
  }
  Array[String] moved_unsorted_read_group_insert_size_metrics = moved_unsorted_read_group_insert_size_metrics_file

  scatter(file in unsorted_read_group_quality_by_cycle_pdf){
    String moved_unsorted_read_group_quality_by_cycle_pdf_file = "~{target_folder}/~{basename(file)}"
  }
  Array[String] moved_unsorted_read_group_quality_by_cycle_pdf = moved_unsorted_read_group_quality_by_cycle_pdf_file

  scatter(file in unsorted_read_group_quality_by_cycle_metrics){
    String moved_unsorted_read_group_quality_by_cycle_metrics_file = "~{target_folder}/~{basename(file)}"
  }
  Array[String] moved_unsorted_read_group_quality_by_cycle_metrics = moved_unsorted_read_group_quality_by_cycle_metrics_file

  scatter(file in unsorted_read_group_quality_distribution_pdf){
    String moved_unsorted_read_group_quality_distribution_pdf_file = "~{target_folder}/~{basename(file)}"
  }
  Array[String] moved_unsorted_read_group_quality_distribution_pdf = moved_unsorted_read_group_quality_distribution_pdf_file

  scatter(file in unsorted_read_group_quality_distribution_metrics){
    String moved_unsorted_read_group_quality_distribution_metrics_file = "~{target_folder}/~{basename(file)}"
  }
  Array[String] moved_unsorted_read_group_quality_distribution_metrics = moved_unsorted_read_group_quality_distribution_metrics_file

  String moved_read_group_alignment_summary_metrics = "~{target_folder}/~{basename(read_group_alignment_summary_metrics)}"

  String old_cross_check_fingerprints_metrics = "~{cross_check_fingerprints_metrics}"
  String moved_cross_check_fingerprints_metrics = if old_cross_check_fingerprints_metrics == "" then "" else "~{target_folder}/~{basename(old_cross_check_fingerprints_metrics)}"

  String moved_selfSM = "~{target_folder}/~{basename(selfSM)}"

  String moved_calculate_read_group_checksum_md5 = "~{target_folder}/~{basename(calculate_read_group_checksum_md5)}"
  
  String moved_agg_alignment_summary_metrics = "~{target_folder}/~{basename(agg_alignment_summary_metrics)}"
  String moved_agg_bait_bias_detail_metrics = "~{target_folder}/~{basename(agg_bait_bias_detail_metrics)}"
  String moved_agg_bait_bias_summary_metrics = "~{target_folder}/~{basename(agg_bait_bias_summary_metrics)}"
  String moved_agg_insert_size_histogram_pdf = "~{target_folder}/~{basename(agg_insert_size_histogram_pdf)}"
  String moved_agg_insert_size_metrics = "~{target_folder}/~{basename(agg_insert_size_metrics)}"
  String moved_agg_pre_adapter_detail_metrics = "~{target_folder}/~{basename(agg_pre_adapter_detail_metrics)}"
  String moved_agg_pre_adapter_summary_metrics = "~{target_folder}/~{basename(agg_pre_adapter_summary_metrics)}"
  String moved_agg_quality_distribution_pdf = "~{target_folder}/~{basename(agg_quality_distribution_pdf)}"
  String moved_agg_quality_distribution_metrics = "~{target_folder}/~{basename(agg_quality_distribution_metrics)}"
  String moved_agg_error_summary_metrics = "~{target_folder}/~{basename(agg_error_summary_metrics)}"

  String old_fingerprint_summary_metrics = "~{fingerprint_summary_metrics}"
  String moved_fingerprint_summary_metrics = if old_fingerprint_summary_metrics == "" then "" else "~{target_folder}/~{basename(old_fingerprint_summary_metrics)}"

  String old_fingerprint_detail_metrics = "~{fingerprint_detail_metrics}"
  String moved_fingerprint_detail_metrics = if old_fingerprint_detail_metrics == "" then "" else "~{target_folder}/~{basename(old_fingerprint_detail_metrics)}"

  String moved_duplicate_metrics = "~{target_folder}/~{basename(duplicate_metrics)}"

  String old_output_bqsr_reports = "~{output_bqsr_reports}"
  String moved_output_bqsr_reports = if old_output_bqsr_reports == "" then "" else "~{target_folder}/~{basename(old_output_bqsr_reports)}"

  String moved_gvcf_summary_metrics = "~{target_folder}/~{basename(gvcf_summary_metrics)}"
  String moved_gvcf_detail_metrics = "~{target_folder}/~{basename(gvcf_detail_metrics)}"
  
  String moved_hybrid_selection_metrics = "~{target_folder}/~{basename(hybrid_selection_metrics)}"

  String moved_output_cram = "~{target_folder}/~{basename(output_cram)}"
  String moved_output_cram_index = "~{target_folder}/~{basename(output_cram_index)}"
  String moved_output_cram_md5 = "~{target_folder}/~{basename(output_cram_md5)}"

  String moved_validate_cram_file_report = "~{target_folder}/~{basename(validate_cram_file_report)}"

  String moved_output_vcf = "~{target_folder}/~{basename(output_vcf)}"
  String moved_output_vcf_index = "~{target_folder}/~{basename(output_vcf_index)}"

  call MoveResult as mf {
    input:
      target_folder = target_folder,
      project_id = project_id,

      quality_yield_metrics = quality_yield_metrics,

      unsorted_read_group_base_distribution_by_cycle_pdf = unsorted_read_group_base_distribution_by_cycle_pdf,
      unsorted_read_group_base_distribution_by_cycle_metrics = unsorted_read_group_base_distribution_by_cycle_metrics,
      unsorted_read_group_insert_size_histogram_pdf = unsorted_read_group_insert_size_histogram_pdf,
      unsorted_read_group_insert_size_metrics = unsorted_read_group_insert_size_metrics,
      unsorted_read_group_quality_by_cycle_pdf = unsorted_read_group_quality_by_cycle_pdf,
      unsorted_read_group_quality_by_cycle_metrics = unsorted_read_group_quality_by_cycle_metrics,
      unsorted_read_group_quality_distribution_pdf = unsorted_read_group_quality_distribution_pdf,
      unsorted_read_group_quality_distribution_metrics = unsorted_read_group_quality_distribution_metrics,

      read_group_alignment_summary_metrics = read_group_alignment_summary_metrics,

      cross_check_fingerprints_metrics = cross_check_fingerprints_metrics,

      selfSM = selfSM,

      calculate_read_group_checksum_md5 = calculate_read_group_checksum_md5,

      agg_alignment_summary_metrics = agg_alignment_summary_metrics,
      agg_bait_bias_detail_metrics = agg_bait_bias_detail_metrics,
      agg_bait_bias_summary_metrics = agg_bait_bias_summary_metrics,
      agg_insert_size_histogram_pdf = agg_insert_size_histogram_pdf,
      agg_insert_size_metrics = agg_insert_size_metrics,
      agg_pre_adapter_detail_metrics = agg_pre_adapter_detail_metrics,
      agg_pre_adapter_summary_metrics = agg_pre_adapter_summary_metrics,
      agg_quality_distribution_pdf = agg_quality_distribution_pdf,
      agg_quality_distribution_metrics = agg_quality_distribution_metrics,
      agg_error_summary_metrics = agg_error_summary_metrics,

      fingerprint_summary_metrics = fingerprint_summary_metrics,
      fingerprint_detail_metrics = fingerprint_detail_metrics,

      duplicate_metrics = duplicate_metrics,
      output_bqsr_reports = output_bqsr_reports,

      gvcf_summary_metrics = gvcf_summary_metrics,
      gvcf_detail_metrics = gvcf_detail_metrics,
      
      hybrid_selection_metrics = hybrid_selection_metrics,

      output_cram = output_cram,
      output_cram_index = output_cram_index,
      output_cram_md5 = output_cram_md5,

      validate_cram_file_report = validate_cram_file_report,

      output_vcf = output_vcf,
      output_vcf_index = output_vcf_index
  }

  output {
    Array[String] target_quality_yield_metrics = moved_quality_yield_metrics
    
    Array[String] target_unsorted_read_group_base_distribution_by_cycle_pdf = moved_unsorted_read_group_base_distribution_by_cycle_pdf
    Array[String] target_unsorted_read_group_base_distribution_by_cycle_metrics = moved_unsorted_read_group_base_distribution_by_cycle_metrics
    Array[String] target_unsorted_read_group_insert_size_histogram_pdf = moved_unsorted_read_group_insert_size_histogram_pdf
    Array[String] target_unsorted_read_group_insert_size_metrics = moved_unsorted_read_group_insert_size_metrics
    Array[String] target_unsorted_read_group_quality_by_cycle_pdf = moved_unsorted_read_group_quality_by_cycle_pdf
    Array[String] target_unsorted_read_group_quality_by_cycle_metrics = moved_unsorted_read_group_quality_by_cycle_metrics
    Array[String] target_unsorted_read_group_quality_distribution_pdf = moved_unsorted_read_group_quality_distribution_pdf
    Array[String] target_unsorted_read_group_quality_distribution_metrics = moved_unsorted_read_group_quality_distribution_metrics

    String target_read_group_alignment_summary_metrics = moved_read_group_alignment_summary_metrics

    String target_cross_check_fingerprints_metrics = moved_cross_check_fingerprints_metrics

    String target_selfSM = moved_selfSM

    String target_calculate_read_group_checksum_md5 = moved_calculate_read_group_checksum_md5

    String target_agg_alignment_summary_metrics = moved_agg_alignment_summary_metrics
    String target_agg_bait_bias_detail_metrics = moved_agg_bait_bias_detail_metrics
    String target_agg_bait_bias_summary_metrics = moved_agg_bait_bias_summary_metrics
    String target_agg_insert_size_histogram_pdf = moved_agg_insert_size_histogram_pdf
    String target_agg_insert_size_metrics = moved_agg_insert_size_metrics
    String target_agg_pre_adapter_detail_metrics = moved_agg_pre_adapter_detail_metrics
    String target_agg_pre_adapter_summary_metrics = moved_agg_pre_adapter_summary_metrics
    String target_agg_quality_distribution_pdf = moved_agg_quality_distribution_pdf
    String target_agg_quality_distribution_metrics = moved_agg_quality_distribution_metrics
    String target_agg_error_summary_metrics = moved_agg_error_summary_metrics

    String target_fingerprint_summary_metrics = moved_fingerprint_summary_metrics
    String target_fingerprint_detail_metrics = moved_fingerprint_detail_metrics

    String target_duplicate_metrics = moved_duplicate_metrics
    String target_output_bqsr_reports = moved_output_bqsr_reports

    String target_gvcf_summary_metrics = moved_gvcf_summary_metrics
    String target_gvcf_detail_metrics = moved_gvcf_detail_metrics

    String target_hybrid_selection_metrics = moved_hybrid_selection_metrics

    String target_output_cram = moved_output_cram
    String target_output_cram_index = moved_output_cram_index
    String target_output_cram_md5 = moved_output_cram_md5

    String target_validate_cram_file_report = moved_validate_cram_file_report

    String target_output_vcf = moved_output_vcf
    String target_output_vcf_index = moved_output_vcf_index

    Int target_file_moved = mf.target_file_moved
  }
}

task MoveResult {
  input {
    String target_folder
    String? project_id

    Array[String] quality_yield_metrics

    Array[String] unsorted_read_group_base_distribution_by_cycle_pdf
    Array[String] unsorted_read_group_base_distribution_by_cycle_metrics
    Array[String] unsorted_read_group_insert_size_histogram_pdf
    Array[String] unsorted_read_group_insert_size_metrics
    Array[String] unsorted_read_group_quality_by_cycle_pdf
    Array[String] unsorted_read_group_quality_by_cycle_metrics
    Array[String] unsorted_read_group_quality_distribution_pdf
    Array[String] unsorted_read_group_quality_distribution_metrics

    String read_group_alignment_summary_metrics

    String? cross_check_fingerprints_metrics

    String selfSM

    String calculate_read_group_checksum_md5

    String agg_alignment_summary_metrics
    String agg_bait_bias_detail_metrics
    String agg_bait_bias_summary_metrics
    String agg_insert_size_histogram_pdf
    String agg_insert_size_metrics
    String agg_pre_adapter_detail_metrics
    String agg_pre_adapter_summary_metrics
    String agg_quality_distribution_pdf
    String agg_quality_distribution_metrics
    String agg_error_summary_metrics

    String? fingerprint_summary_metrics
    String? fingerprint_detail_metrics

    String duplicate_metrics
    String? output_bqsr_reports

    String gvcf_summary_metrics
    String gvcf_detail_metrics

    String hybrid_selection_metrics

    String output_cram
    String output_cram_index
    String output_cram_md5

    File validate_cram_file_report

    File output_vcf
    File output_vcf_index
  }

  command <<<

set -e

gsutil -m ~{"-u " + project_id} mv ~{sep="" quality_yield_metrics} \
  ~{sep=" " unsorted_read_group_base_distribution_by_cycle_pdf} \
  ~{sep=" " unsorted_read_group_base_distribution_by_cycle_metrics} \
  ~{sep=" " unsorted_read_group_insert_size_histogram_pdf} \
  ~{sep=" " unsorted_read_group_insert_size_metrics} \
  ~{sep=" " unsorted_read_group_quality_by_cycle_pdf} \
  ~{sep=" " unsorted_read_group_quality_by_cycle_metrics} \
  ~{sep=" " unsorted_read_group_quality_distribution_pdf} \
  ~{sep=" " unsorted_read_group_quality_distribution_metrics} \
  ~{read_group_alignment_summary_metrics} \
  ~{selfSM} \
  ~{calculate_read_group_checksum_md5} \
  ~{agg_alignment_summary_metrics} \
  ~{agg_bait_bias_detail_metrics} \
  ~{agg_bait_bias_summary_metrics} \
  ~{agg_insert_size_histogram_pdf} \
  ~{agg_insert_size_metrics} \
  ~{agg_pre_adapter_detail_metrics} \
  ~{agg_pre_adapter_summary_metrics} \
  ~{agg_quality_distribution_pdf} \
  ~{agg_quality_distribution_metrics} \
  ~{agg_error_summary_metrics} \
  ~{fingerprint_summary_metrics} \
  ~{fingerprint_detail_metrics} \
  ~{duplicate_metrics} \
  ~{output_bqsr_reports} \
  ~{gvcf_summary_metrics} \
  ~{gvcf_detail_metrics} \
  ~{hybrid_selection_metrics} \
  ~{output_cram} \
  ~{output_cram_index} \
  ~{output_cram_md5} \
  ~{validate_cram_file_report} \
  ~{output_vcf} \
  ~{output_vcf_index} \
  ~{target_folder}/

>>>

  runtime {
    docker: "google/cloud-sdk"
    preemptible: 1
    disks: "local-disk 10 HDD"
    memory: "2 GiB"
  }
  output {
    Int target_file_moved = 1
  }
}
