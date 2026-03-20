version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCPheTK {
  input {
    File cohort_file_path
    File phecode_count_file_path
    String phecode_version
    String sex_at_birth_col
    String covariate_cols
    String independent_variable_of_interest
    String output_file_prefix

    Int min_cases=50
    Int min_phecode_count=1

    String? target_gcp_folder
  }

  call PheTK {
    input:
      cohort_file_path = cohort_file_path,
      phecode_count_file_path = phecode_count_file_path,
      phecode_version = phecode_version,
      sex_at_birth_col = sex_at_birth_col,
      covariate_cols = covariate_cols,
      independent_variable_of_interest = independent_variable_of_interest,
      output_file_prefix = output_file_prefix,
      min_cases = min_cases,
      min_phecode_count = min_phecode_count
  }

  call PheTKVis {
    input:
      phewas_result_file = PheTK.output_phewas_file,
      output_file_prefix = output_file_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyFourFiles as CopyFile {
      input:
        source_file1 = PheTK.output_phewas_file,
        source_file2 = PheTKVis.output_phewas_manhattan_file,
        source_file3 = PheTKVis.output_phewas_manhattan_no_labels_file,
        source_file4 = PheTKVis.output_phewas_forest_file,
        is_move_file = false,
        target_gcp_folder = select_first([target_gcp_folder]),
    }
  }

  output {
    File output_phewas_file = select_first([CopyFile.output_file1, PheTK.output_phewas_file])
    File output_phewas_manhattan_file = select_first([CopyFile.output_file2, PheTKVis.output_phewas_manhattan_file])
    File output_phewas_manhattan_no_labels_file = select_first([CopyFile.output_file3, PheTKVis.output_phewas_manhattan_no_labels_file])
    File output_phewas_forest_file = select_first([CopyFile.output_file4, PheTKVis.output_phewas_forest_file])
  }
}

task PheTK {
  input {
    File cohort_file_path
    File phecode_count_file_path
    String phecode_version
    String sex_at_birth_col
    String covariate_cols
    String independent_variable_of_interest
    String output_file_prefix

    Int min_cases
    Int min_phecode_count
    
    String docker = "phetk/phetk:0.2.2"

    Int preemptible = 3
    Int cpu = 1
    Int memory_gb = 10
  }

  Int disk_size = ceil(size([cohort_file_path, phecode_count_file_path],"GB")) + 10

  String output_file = output_file_prefix + ".phewas.tsv"

  command <<<

python3 -m phetk.phewas \
  --cohort_file_path ~{cohort_file_path} \
  --phecode_count_file_path ~{phecode_count_file_path} \
  --phecode_version ~{phecode_version} \
  --sex_at_birth_col ~{sex_at_birth_col} \
  --covariate_cols ~{covariate_cols} \
  --independent_variable_of_interest ~{independent_variable_of_interest} \
  --min_cases ~{min_cases} \
  --min_phecode_count ~{min_phecode_count} \
  --output_file_path  ~{output_file}

>>>

  runtime {
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    # The output has to be defined as File, otherwise the file would not be delocalized
    File output_phewas_file = "~{output_file}"
  }
}

task PheTKVis {
  input {
    File phewas_result_file
    String output_file_prefix
    
    String docker = "phetk/phetk:0.2.2"

    Int preemptible = 3
    Int cpu = 1
    Int memory_gb = 10
  }

  Int disk_size = ceil(size([phewas_result_file],"GB")) + 10

  command <<<
#!/bin/bash

set -e

# Create Python script
cat > phetk_vis.py << 'EOF'

from phetk.phewas import PheWAS
from phetk.plot import Plot

p = Plot("~{phewas_result_file}")

p.manhattan(label_values="p_value", label_count=16, save_plot=True, output_file_path="~{output_file_prefix}_manhattan.png")

p.manhattan(label_values="p_value", label_count=0, save_plot=True, output_file_path="~{output_file_prefix}_manhattan_no_labels.png")

p.forest(highlight_significance=True, show_p_value_asterisks=True, save_plot=True, output_file_path="~{output_file_prefix}_forest.png")

EOF

python3 phetk_vis.py

>>>

  runtime {
    docker: docker
    preemptible: preemptible
    disks: "local-disk " + disk_size + " HDD"
    memory: memory_gb + " GiB"
  }
  output {
    File output_phewas_manhattan_file = "~{output_file_prefix}_manhattan.png"
    File output_phewas_manhattan_no_labels_file = "~{output_file_prefix}_manhattan_no_labels.png"
    File output_phewas_forest_file = "~{output_file_prefix}_forest.png"
  }
}

