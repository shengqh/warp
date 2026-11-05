---
name: wdl
description: "Format, write, or edit WDL (Workflow Description Language) files in the Warp project. Use for .wdl formatting, workflow headers, inputs and outputs, imports, calls, scatter blocks, conditionals, or parameter_meta."
---

# WDL Formatting and Workflow Conventions

Use this skill when formatting an existing WDL file or writing a workflow that follows the Warp/VUMC Biostatistics conventions. Treat formatting as a presentation-only change: preserve the workflow's behavior, expressions, names, imports, defaults, call aliases, and output types unless the user also requests a functional change.

---

## Formatting Procedure

1. Read the whole target file before editing. Check nearby WDL files and imported task/workflow definitions when needed to preserve local naming and syntax conventions.
2. Keep the WDL version declaration at the top. Separate it from the documentation header with a blank line when a header is present.
3. Keep the structured documentation header before imports, with each documentation line prefixed by `##`; use `##` for blank lines inside the comment block.
4. Put imports after the header and before the workflow or task definition, with one import per line.
5. Indent nested blocks consistently with two spaces per level. Put opening braces on the declaration line and closing braces at the declaration's indentation.
6. Keep one declaration or statement per line. In `input`, `output`, `parameter_meta`, and call `input:` blocks, put each entry on its own line. Align block contents by indentation, not by padding names or values into columns.
7. Use blank lines to separate logical groups, such as related inputs, local declarations, calls, and outputs. Avoid adding blank lines between every statement.
8. For `scatter` and `if` blocks, put the block body on subsequent indented lines and close the block at its opening statement's indentation. Keep the existing condition, iteration expression, and scope unchanged.
9. Review the diff to confirm that only whitespace, line breaks, and explicitly requested documentation changed. Do not silently fix misspellings, rename identifiers, reorder inputs, restructure logic, or change defaults while formatting.
10. If a WDL formatter or validator is configured in the project, use it after editing. Otherwise, report that no formatter/validator was run rather than implying the file was validated.

## Documentation Header

Use `##` comment lines with embedded Markdown headings for a structured workflow header when the file has one or when the user asks for documentation to be added. Follow the existing header rather than inserting a new one into files that intentionally omit it.

```wdl
version 1.0

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
- Preserve existing header content and ordering. Do not invent a copyright notice, author, domain, or other factual information.
- Section headings use Markdown `###` inside the comment: `## ### Section:`.
- Step names within "Workflow Steps" are **bold**: `## 1. **StepName**: description.`
- The `Notes:` section is optional; include it when there are non-obvious behaviors or dependencies.
- `import` statements come immediately after the header block, before the `workflow` block.
- Describe optional inputs accurately, including `target_gcp_folder` when present.
- Do not include implementation details (Docker images, runtime parameters) in the header — those belong in task files.

---

## Workflow Layout

Follow the supplied workflow's organization when applicable:

```wdl
workflow MyWorkflow {
  input {
    Array[String] identifiers

    Array[File] input_files

    String output_prefix
    String option = "default"
  }

  Int item_count = length(identifiers)

  Array[Int] indices = range(item_count)
  scatter(index in indices) {
    call SomeTask as ProcessItem {
      input:
        input_file = input_files[index],
        output_prefix = output_prefix + "." + identifiers[index]
    }
  }

  if (defined(optional_destination)) {
    String destination = select_first([optional_destination])
    call SomeTask as CopyResults {
      input:
        source_files = ProcessItem.output_files,
        target = destination
    }
  }

  output {
    Array[File] results = ProcessItem.output_files
  }
}
```

The sample is a layout illustration, not a template to copy blindly. Preserve legal WDL syntax and the file's existing style where it differs. In particular, do not change conditional expressions or optional-value handling just to match the illustration.

## `parameter_meta` Block

When a workflow already uses `parameter_meta`, or the user requests it, document every declared input. Place it inside the workflow after `input` (and after `meta` if present), before the workflow body. Do not add a `parameter_meta` block solely as part of a formatting-only change.

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
- Workflow-level: use the simple `param: "description"` string form.
- Task-level: use the object form `param: { description: "...", localization_optional: true }` for `File` inputs that can be streamed from GCS without full localization; use plain strings for non-file inputs.
- For optional inputs (`?` type suffix or inputs with defaults), the description should indicate they are optional and describe the default behavior (e.g., `"Optional. If not provided, all samples are included."`).
- For Struct-type inputs, describe the struct's overall purpose, not individual fields.
- Keep descriptions concise but complete — one sentence is typically sufficient.
- Do not duplicate `parameter_meta` entries for computed variables or intermediate values; only document declared inputs.
