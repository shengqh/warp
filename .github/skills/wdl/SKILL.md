---
name: wdl
description: WDL (Workflow Description Language) workflows — writing, editing, formatting, and creating new WDL pipeline files. Use for any task involving .wdl files, workflow introductions/headers, WDL tasks, scatter/gather patterns, parameter_meta, or VUMC biostatistics pipelines.
applyTo: "**/*.wdl"
---

# WDL Workflow Introduction Format

Every WDL workflow file in this codebase begins with a structured header comment block and includes a `parameter_meta` block inside the workflow. This format ensures that every workflow is self-documented with clear descriptions of its purpose, inputs, outputs, and steps, and that all inputs are documented in a machine-readable way for downstream tools.

---

## Header Comment Block

Use `##` (double hash) comment lines with embedded Markdown headings.

```wdl
version 1.0

## Copyright Vanderbilt Health, <YEAR>
##
## <Workflow Title>
##
## <One or two sentence description of the workflow purpose.>
## Developed by VUMC Biostatistics for <domain>.
## Author: <Name> (<email>)
##
## ### Workflow Purpose:
## <Detailed explanation of why this workflow exists and what problem it solves.>
##
## ### Workflow Steps:
## 1. **<StepName>**: <What this step does.>
## 2. **<StepName>**: <What this step does.>
## 3. **<StepName (Optional)>**: <Condition under which this step runs.>
##
## ### Inputs:
## - <input_name>: <Description>
## - <input_name>: <Description>
## - target_gcp_folder: Optional Google Cloud Storage folder path for copying results.
##
## ### Outputs:
## - <output_name>: <Description>
##
## ### Notes:
## - <Any important implementation detail, limitation, or dependency.>

import "..."

workflow MyWorkflow {
  ...
}
```

**Rules:**
- Use `##` for every comment line; use `##` alone for blank lines within the block.
- The very first line after `version 1.0` (separated by a blank line) is the copyright notice: `## Copyright Vanderbilt Health, <YEAR>`, followed by a `##` blank line, then the workflow title.
- Section headings use Markdown `###` inside the comment: `## ### Section:`.
- Step names within "Workflow Steps" are **bold**: `## 1. **StepName**: description.`
- Include `Author:` field with name and email.
- The `Notes:` section is optional; include it when there are non-obvious behaviors or dependencies.
- `import` statements come immediately after the header block, before the `workflow` block.
- `target_gcp_folder` is always described as **Optional** in the inputs section.
- Do not include implementation details (Docker images, runtime parameters) in the header — those belong in task files.

---

## `parameter_meta` Block

The `parameter_meta` block provides machine-readable documentation for every workflow input. It lives **inside** the `workflow` block, after the `meta` block and before the workflow body.

### Workflow-level `parameter_meta` (simple string form)

Use plain string values for workflow inputs. This is the standard form for workflows.

```wdl
workflow MyWorkflow {
  input {
    ContaminationSites contamination_sites
    AlignmentReferences alignment_references
    Array[File]? input_cram_list
    Array[File]? input_bam_list
    String base_file_name
    Float rsq_threshold = 1.0
    Int reads_per_split = 20000000
    Boolean save_bam_file = false
  }

  meta {
    allowNestedInputs: true
  }

  parameter_meta {
    contamination_sites: "Struct containing files for contamination estimation"
    alignment_references: "Struct containing reference files for alignment with BWA mem"
    input_cram_list: "Array of CRAM files to be used as workflow input. Must be specified if `input_bam_list` is not provided"
    input_bam_list: "Array of unmapped BAM files to be used as workflow input. Must be specified if `input_cram_list` is not provided"
    base_file_name: "Base name for each of the output files."
    rsq_threshold: "Threshold for a read quality metric that is produced by the sequencing platform"
    reads_per_split: "Number of reads by which to split the CRAM prior to alignment"
    save_bam_file: "If true, save intermediate outputs used by downstream pipelines; otherwise they will not be kept as outputs."
  }

  ...
}
```

### Task-level `parameter_meta` (object form)

Inside a `task`, use the object form for `File` inputs that support delocalization, adding `localization_optional: true`. Other inputs still use plain strings.

```wdl
task MyTask {
  input {
    File input_bam
    File input_bam_index
    String sample_name
  }

  parameter_meta {
    input_bam: {
      description: "A BAM or CRAM file to process",
      localization_optional: true
    }
    input_bam_index: {
      description: "Index file for the BAM or CRAM input",
      localization_optional: true
    }
    sample_name: "Name of the sample being processed"
  }

  ...
}
```

**Rules for `parameter_meta`:**
- Every input variable declared in the `input {}` block must have a corresponding entry.
- Workflow-level: always use the simple `param: "description"` string form.
- Task-level: use the object form `param: { description: "...", localization_optional: true }` for `File` inputs that can be streamed from GCS without full localization; use plain strings for non-file inputs.
- For optional inputs (`?` type suffix or inputs with defaults), the description should indicate they are optional and describe the default behavior (e.g., `"Optional. If not provided, all samples are included."`).
- For Struct-type inputs, describe the struct's overall purpose, not individual fields.
- Keep descriptions concise but complete — one sentence is typically sufficient.
- Do not duplicate `parameter_meta` entries for computed variables or intermediate values; only document declared inputs.
