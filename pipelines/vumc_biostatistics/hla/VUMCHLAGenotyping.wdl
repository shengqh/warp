version 1.0

## VUMC HLA Genotyping Workflow
##
## This workflow is a mirror and modification of the AllOfUs HLA Genotyping workflow.
## It performs HLA genotyping using a consensus approach from multiple tools.
## Developed by VUMC Biostatistics for HLA analysis.
## Author: Quanhu Sheng (quanhu.sheng.1@vumc.org)
##
## ### Workflow Purpose:
## This pipeline determines an individual's HLA type from whole-genome or whole-exome sequencing data.
## It uses a consensus calling strategy by combining results from HLA-HD, Polysolver, and OptiType.
##
## ### Workflow Steps:
## 1. **MakeHLAOnlyBamsAndFastqs**: Extracts reads from the HLA region of the input BAM/CRAM and converts them to a new BAM and paired-end FASTQ files.
## 2. **HLAHD**: Runs HLA-HD on the extracted FASTQ files.
## 3. **Polysolver**: Runs Polysolver on the HLA-region BAM file.
## 4. **Optitype**: Runs OptiType on the extracted FASTQ files.
## 5. **Consensus**: Compares the results from the three tools. If Polysolver and OptiType agree with each other but differ from HLA-HD, their result is used for genes A, B, and C. Otherwise, the HLA-HD result is used. For other genes, the HLA-HD result is always used.
## 6. **CopyFile (Optional)**: If a `target_gcp_folder` is provided, copies the main result files to the specified Google Cloud Storage location.
##
## ### Inputs:
## - Docker images for GATK, HLA-HD, Polysolver, and OptiType.
## - A sample name.
## - An input BAM or CRAM file with its index.
## - A reference genome (FASTA, FAI, DICT).
## - An interval list file for HLA regions.
## - Helper Python scripts for result conversion and counting.
## - An HLA groups file for allele conversion.
## - An optional Google Cloud Storage folder path (`target_gcp_folder`) for copying results.
##
## ### Outputs:
## - `consensus`: The final consensus HLA genotype file.
## - `hlahd_raw_result`: Raw result from HLA-HD.
## - `hlahd_converted_result`: Converted result from HLA-HD.
## - `optitype_result`: Result from OptiType.
## - `polysolver_result`: Result from Polysolver.
## - `hlahd_two_field_count`: The number of two-field alleles identified by HLA-HD.
## - `hlahd_overridden`: The number of times the HLA-HD result was overridden by the Polysolver/OptiType consensus for genes A, B, or C.


import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCHLAGenotyping {

    String pipeline_version = "aou_9.0.0"

    input {
        String gatk_docker
        String hlahd_docker
        String polysolver_docker
        String optitype_docker

        String sample_name

        File original_bam       # this can be a BAM or CRAM
        File original_bam_idx
        File ref_fasta
        File ref_fai
        File ref_dict
        File hla_intervals

        File convert_alleles_python_script
        File count_two_field_alleles_python_script
        File hla_groups_file

        String? target_gcp_folder
    }

    call MakeHLAOnlyBamsAndFastqs {
        input:
            sample_name = sample_name,
            gatk_docker = gatk_docker,
            original_bam = original_bam,
            original_bam_idx = original_bam_idx,
            ref_fasta = ref_fasta,
            ref_fai = ref_fai,
            ref_dict = ref_dict,
            hla_intervals = hla_intervals
    }

    call HLAHD {
        input:
            sample_name = sample_name,
            hlahd_docker = hlahd_docker,
            first_end_fastq = MakeHLAOnlyBamsAndFastqs.first_end_fastq,
            second_end_fastq = MakeHLAOnlyBamsAndFastqs.second_end_fastq,
            convert_alleles_python_script = convert_alleles_python_script,
            count_two_field_alleles_python_script = count_two_field_alleles_python_script,
            hla_groups_file = hla_groups_file
    }

    call Polysolver {
        input:
            sample_name = sample_name,
            polysolver_docker = polysolver_docker,
            hla_bam = MakeHLAOnlyBamsAndFastqs.hla_bam,
            hla_bam_idx = MakeHLAOnlyBamsAndFastqs.hla_bam_idx
    }

    call Optitype {
        input:
            sample_name = sample_name,
            optitype_docker = optitype_docker,
            first_end_fastq = MakeHLAOnlyBamsAndFastqs.first_end_fastq,
            second_end_fastq = MakeHLAOnlyBamsAndFastqs.second_end_fastq
    }

    call Consensus {
      input:
        sample_name = sample_name,
        hla_hd_result = HLAHD.converted_result,
        polysolver_result = Polysolver.result,
        optitype_result = Optitype.result
    }

    if(defined(target_gcp_folder)){
      call GcpUtils.MoveOrCopyFiles as CopyFile {
        input:
          source_file1 = "~{HLAHD.raw_result}",
          source_file2 = "~{HLAHD.converted_result}",
          source_file3 = "~{Optitype.result}",
          source_file4 = "~{Polysolver.result}",
          source_file5 = "~{Consensus.consensus}",
          source_file6 = "~{MakeHLAOnlyBamsAndFastqs.hla_bam}",
          source_file7 = "~{MakeHLAOnlyBamsAndFastqs.hla_bam_idx}",
          is_move_file = false,
          target_gcp_folder = select_first([target_gcp_folder])
      }
    }

    output {
        File hla_bam = select_first([CopyFile.output_file6, MakeHLAOnlyBamsAndFastqs.hla_bam])
        File hla_bam_idx = select_first([CopyFile.output_file7, MakeHLAOnlyBamsAndFastqs.hla_bam_idx])

        File hlahd_raw_result = select_first([CopyFile.output_file1, HLAHD.raw_result])
        File hlahd_converted_result = select_first([CopyFile.output_file2, HLAHD.converted_result])
        File optitype_result = select_first([CopyFile.output_file3, Optitype.result])
        File polysolver_result = select_first([CopyFile.output_file4, Polysolver.result])
        File consensus = select_first([CopyFile.output_file5, Consensus.consensus])

        Int hlahd_two_field_count = HLAHD.two_field_count
        Int hlahd_overriden = Consensus.hlahd_overridden
    }
}

task MakeHLAOnlyBamsAndFastqs {
    input {
        String sample_name

        String gatk_docker
        File original_bam       # this can be a BAM or CRAM
        File original_bam_idx
        File ref_fasta          # GATK PrintReads requires a reference for CRAMs
        File ref_fai
        File ref_dict
        File hla_intervals

        Int cpu = 2
        Int num_threads = 4
        Int mem_gb = 4
        Int additional_disk_size_gb = 10
        Int boot_disk_gb = 10
        Int max_retries = 0
        Int preemptible = 1
    }

    Int disk_gb = ceil(size([original_bam, original_bam_idx, ref_fasta, ref_fai, ref_dict, hla_intervals ], "GB")) + additional_disk_size_gb

    # Remove the optional localization_optional tags since it doesn't work in Terra.
    # It requires to add terra project service account to the bucket IAM even GCP project id is provided.
    # parameter_meta{
    #     hla_intervals: {localization_optional: true}
    #     ref_fasta: {localization_optional: true}
    #     ref_fai: {localization_optional: true}
    #     ref_dict: {localization_optional: true}
    #     original_bam: {localization_optional: true}
    #     original_bam_idx: {localization_optional: true}
    # }

    command <<<
        # this command also produces the accompanying index hla.bai
        # the PairedReadFilter is necessary for SamtoFastq to succeed
        gatk PrintReads -R ~{ref_fasta} -I ~{original_bam} -L ~{hla_intervals} -O hla-unsorted.bam 


        echo "We are running ValidateSamFile on the output of PrintReads:"
        gatk ValidateSamFile -I hla-unsorted.bam

        samtools sort --threads ~{num_threads} hla-unsorted.bam > ~{sample_name}.hla.bam

        # using gatk instead of samtools for indexing avoids ERROR:INVALID_INDEX_FILE_POINTER in the output
        # of ValidateSamFile.  I'm not sure what that means or if it's important, but might as well not have it.
        gatk BuildBamIndex -I ~{sample_name}.hla.bam

        echo "We are running ValidateSamFile on the output of samtools sorting and re-indexing"
        gatk ValidateSamFile -I ~{sample_name}.hla.bam

        # The "*" MUST be in quotes.  -T "*" indicates that all tags are copied to output.
        samtools fastq --threads ~{num_threads} -n -T "*" -0 /dev/null -1 first_end.fq -2 second_end.fq ~{sample_name}.hla.bam
    >>>

    runtime {
        docker: gatk_docker
        bootDiskSizeGb: boot_disk_gb
        memory: mem_gb + " GB"
        disks: "local-disk " + disk_gb + " SSD"
        preemptible: preemptible
        maxRetries: max_retries
        cpu: cpu
    }

    output {
        File hla_bam = "~{sample_name}.hla.bam"
        File hla_bam_idx = "~{sample_name}.hla.bai"
        File first_end_fastq = "first_end.fq"
        File second_end_fastq = "second_end.fq"
    }
}

task HLAHD {
    input {
        String sample_name

        String hlahd_docker
        File first_end_fastq
        File second_end_fastq

        File convert_alleles_python_script
        File count_two_field_alleles_python_script
        File hla_groups_file

        Int min_read_length = 75
        Float trim_ratio = 0.95

        Int cpu = 2
        Int num_threads = 4

        Int mem_gb = 20
        Int additional_disk_size_gb = 10
        Int boot_disk_gb = 10
        Int max_retries = 3
        Int preemptible = 1
    }

    Int disk_gb = ceil(size([first_end_fastq, second_end_fastq ], "GB")) + additional_disk_size_gb

    command <<<
        mkdir OUTPUT

        hlahd.sh -t ~{num_threads} -m ~{min_read_length} -c ~{trim_ratio} \
            -f /HLA/hlahd.1.7.1/freq_data/ \
            ~{first_end_fastq} ~{second_end_fastq} \
            /HLA/hlahd.1.7.1/HLA_gene.split.3.32.0.txt \
            /HLA/hlahd.1.7.1/dictionary SAMPLE_ID \
            OUTPUT

        mv OUTPUT/SAMPLE_ID/result/SAMPLE_ID_final.result.txt ./raw1.txt

        # HLA-HD alleles are in the form HLA-A*23:01:01
        # here we remove the "HLA-" from every allele, remove lines that say Couldn't read result file,
        # and convert "Not typed" (with a pesky space) to "NA"
        sed 's/HLA-//g' raw1.txt | grep -v "Couldn't read result" | sed 's/Not typed/NA/g' > raw2.txt

        # HLA-HD emits a '-' in the second allele to denote a homozygous genotype.  We expand this out.
        # also, it sometimes emits a third and fourth allele, which are weaker guesses that we discard
        # finally, we sort in lexicographical order
        touch raw3.txt
        while read gene allele1 allele2 other_alleles; do
            printf "${gene}\t" >> raw3.txt
            if [[ ${allele2} == "-" ]]; then
               printf "${allele1}\t${allele1}\n" >> raw3.txt   # copy allele1 for homozygous
            elif [[ ${allele1} < ${allele2} ]]; then
               printf "${allele1}\t${allele2}\n" >> raw3.txt
            else
               printf "${allele2}\t${allele1}\n" >> raw3.txt
            fi
         done < raw2.txt


        python3 ~{convert_alleles_python_script} raw3.txt ~{hla_groups_file} > ~{sample_name}.hlahd_converted.txt

        python3 ~{count_two_field_alleles_python_script} ~{sample_name}.hlahd_converted.txt > two_field_count.txt

        mv raw3.txt ~{sample_name}.hlahd_raw.txt
    >>>

    runtime {
        docker: hlahd_docker
        bootDiskSizeGb: boot_disk_gb
        memory: mem_gb + " GB"
        disks: "local-disk " + disk_gb + " SSD"
        preemptible: preemptible
        maxRetries: max_retries
        cpu: cpu
    }

    output {
        File raw_result = "~{sample_name}.hlahd_raw.txt"
        File converted_result = "~{sample_name}.hlahd_converted.txt"
        Int two_field_count = read_int("two_field_count.txt")
    }
}

task Polysolver {
    input {
        String sample_name

        String polysolver_docker
        String polysolver_reference = "hg38"
        File hla_bam
        File hla_bam_idx

        Int cpu = 2
        Int num_threads = 4
        Int mem_gb = 20
        Int additional_disk_size_gb = 10
        Int boot_disk_gb = 10
        Int max_retries = 3
        Int preemptible = 1
    }

    Int disk_gb = ceil(size([hla_bam, hla_bam_idx ], "GB")) + additional_disk_size_gb

    command <<<

        # polysolver (positional) args are, in order:
        #   -bam: path to the BAM file to be used for HLA typing
        #	-race: ethnicity of the individual (Caucasian, Black, Asian or Unknown)
        #	-includeFreq: flag indicating whether population-level allele frequencies should be used as priors (0 or 1)
        #	-build: reference genome used in the BAM file (hg18, hg19 or hg38)
        #	-format: fastq format (STDFQ, ILMFQ, ILM1.8 or SLXFQ; see Novoalign documentation)
        #	-insertCalc: flag indicating whether empirical insert size distribution should be used in the model (0 or 1)
        #	-outDir: output directory

        mkdir output

        /home/polysolver/scripts/shell_call_hla_type ~{hla_bam} Unknown 1 ~{polysolver_reference} STDFQ 0 output

        echo "Here is ls of the output directory:"
        ls output

        cd /cromwell_root
        mv output/* .

        mv winners.hla.txt raw1.txt

        echo "RAW1"
        cat raw1.txt

        # Polysover output looks like
        # HLA-A	hla_a_23_01_01	hla_a_34_02_01
        # HLA-B	hla_b_44_03_01	hla_b_45_01
        # HLA-C	hla_c_04_01_01_04	hla_c_06_02_01_01
        # we convert HLA-A to just A, delete "hla_", convert a_23_01_01 to A*23:01:01, and remove the fourth field

        sed 's/HLA-//g' raw1.txt | sed 's/hla_//g' | tr '[:lower:]' '[:upper:]' > raw2.txt
        sed 's/\([A-C]\)_/\1\*/g' raw2.txt | sed 's/_/:/g' > raw3.txt
        sed 's/\([0-9]*:[0-9]*:[0-9]*\):[0-9]*/\1/g' raw3.txt > raw4.txt

        echo "RAW2"
        cat raw2.txt

        echo "RAW3"
        cat raw3.txt

        echo "RAW4"
        cat raw4.txt

        # finally, we sort in lexicographical order
        touch sorted.txt
        while read gene allele1 allele2; do
            printf "${gene}\t" >> sorted.txt
            if [[ ${allele1} < ${allele2} ]]; then
               printf "${allele1}\t${allele2}\n" >> sorted.txt
            else
               printf "${allele2}\t${allele1}\n" >> sorted.txt
            fi
        done < raw4.txt

        mv sorted.txt ~{sample_name}.polysolver_sorted.txt
    >>>

    runtime {
        docker: polysolver_docker
        bootDiskSizeGb: boot_disk_gb
        memory: mem_gb + " GB"
        disks: "local-disk " + disk_gb + " SSD"
        preemptible: preemptible
        maxRetries: max_retries
        cpu: cpu
    }

    output {
        File result = "~{sample_name}.polysolver_sorted.txt"
    }
}

task Optitype {
    input {
        String sample_name

        String optitype_docker
        File first_end_fastq
        File second_end_fastq

        Int mem_gb = 20
        Int additional_disk_size_gb = 10
        Int boot_disk_gb = 10
        Int max_retries = 3
        Int preemptible = 1
        Int cpu = 1
    }

    Int disk_gb = ceil(size([first_end_fastq, second_end_fastq ], "GB")) + additional_disk_size_gb

    command <<<
        python /usr/local/bin/OptiType/OptiTypePipeline.py --input ~{first_end_fastq} ~{second_end_fastq} --dna --verbose --outdir ./output

        ls ./output

        #Optitype puts output not in the specificed outdir dir directly but rather in a time-stamped subfolder
        echo "Here are tsvs"
        find ./output -type f -name "*.tsv"

        echo "here is everything"
        find ./output -name "*"

        find ./output -type f -name "*.tsv" | while read file; do cp $file .; done

        echo "Here is all tsv"
        ls *.tsv

        mv *.tsv optitype_raw.tsv

        # Optitype format is two lines of output
        # A1	A2	B1	B2	C1	C2	Reads	Objective
        # 0	A*23:01	A*34:02	B*45:04	B*44:03	C*06:02	C*04:01	845.0	806.9750000000003

        # we convert it to
        # A A*23:01	A*34:02
        # B B*45:04	B*44:03
        # C C*06:02	C*04:01

        tail -n +2 optitype_raw.tsv | cut -f 2-7 | \
            while read a1 a2 b1 b2 c1 c2; do printf "A\t${a1}\t${a2}\nB\t${b1}\t${b2}\nC\t${c1}\t${c2}\n"; done > optitype.tsv

        # finally, we sort in lexicographical order
        touch sorted.txt
        while read gene allele1 allele2; do
            printf "${gene}\t" >> sorted.txt
            if [[ ${allele1} < ${allele2} ]]; then
               printf "${allele1}\t${allele2}\n" >> sorted.txt
            else
               printf "${allele2}\t${allele1}\n" >> sorted.txt
            fi
         done < optitype.tsv

         mv sorted.txt ~{sample_name}.optitype_sorted.txt
    >>>

    runtime {
        docker: optitype_docker
        bootDiskSizeGb: boot_disk_gb
        memory: mem_gb + " GB"
        disks: "local-disk " + disk_gb + " SSD"
        preemptible: preemptible
        maxRetries: max_retries
        cpu: cpu
    }

    output {
        File result = "~{sample_name}.optitype_sorted.txt"
    }
}

task Consensus {
	input {
    String sample_name
		File hla_hd_result
    File polysolver_result
    File optitype_result
		Int disk_space = 10
	}

	command <<<
        # from previous tasks, all the files are in a standard format of eg:
        # A   A*15:01:02        A*17:02:01
        # B   B*3:01:02         B*3:01:02   # (note that for homozygous we have converted dashes in HLA-HD)
        # etc, with Polysolver and Optitype only ioncluding A, B, C and HLA-HD going further.
        # Optitype has two-field resolution, Polysolver has 4, HLA-HD usually has 3 but sometimes 2
        # the two alleles for each gene are sorted lexicographically

        head -n 3 ~{hla_hd_result} > HLAHD_ABC
        tail -n +4 ~{hla_hd_result} > HLA_HD_REST

        # make two-field versions of everything
        for file in HLAHD_ABC ~{polysolver_result} ~{optitype_result}; do
            sed 's/\([0-9]*:[0-9]*\):[0-9]*/\1/g' ${file} > ${file}_TWO_FIELD

            echo "original file ${file}:"
            cat ${file}

            echo "two-field file ${file}_TWO_FIELD:"
            cat ${file}_TWO_FIELD
        done

        # paste the everything together
        paste ~{polysolver_result}_TWO_FIELD ~{polysolver_result} HLAHD_ABC_TWO_FIELD HLAHD_ABC ~{optitype_result}_TWO_FIELD > PASTED

        echo "Pasted:"
        cat PASTED

        hlahd_overridden=0
        touch consensusABC
        while read gene1 ps1 ps2 gene2 ps3f1 ps3f2 gene3 hlahd1 hlahd2 gene4 hlahd3f1 hlahd3f2 gene5 opt1 opt2; do
             ps=${ps1}${ps2}
             hlahd=${hlahd1}${hlahd2}
             opt=${opt1}${opt2}

            if [[ ${hlahd} != ${ps} && ${ps} == ${opt} ]]; then
                ((hlahd_overridden++))
                printf "${gene1}\t${ps3f1}\t${ps3f2}\n" >> consensusABC
            else
                printf "${gene1}\t${hlahd3f1}\t${hlahd3f2}\n" >> consensusABC
            fi
      done < PASTED

      echo ${hlahd_overridden} > hlahd_overridden.txt
      cat consensusABC HLA_HD_REST > ~{sample_name}.consensus.txt
	>>>

    runtime {
        docker: "continuumio/anaconda:latest"
        disks: "local-disk " + disk_space + " SSD"
    }

    output {
        File consensus = "~{sample_name}.consensus.txt"
        Int hlahd_overridden = read_int("hlahd_overridden.txt")
    }
}
