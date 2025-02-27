version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow VUMCCombineIlluminaQC {
  input {
    String project_id
    File qc_list_file
    String output_prefix
    String? target_gcp_folder  
  }

  call CombineIlluminaQC {
    input:
      project_id = project_id,
      qc_list_file = qc_list_file,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = CombineIlluminaQC.combined_long_file,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_file = select_first([CopyFile.output_file, CombineIlluminaQC.combined_long_file])
  }
}

task CombineIlluminaQC {
  input {
    String project_id
    File qc_list_file
    String output_prefix
    String docker = "shengqh/hail_gcp:20241127"
  }

  command <<<

cat<<EOF > combine.py

import pandas as pd
import os
import gcsfs
from google.cloud import storage

pd.options.mode.chained_assignment = None

sclient = storage.Client()
google_project = "~{project_id}"

fs = gcsfs.GCSFileSystem(project=google_project, requester_pays=True)

def read_requestor_pay_csv(fs, csv, header=None):
    try:
        with fs.open(csv, mode='rt') as f:
            result = [line for line in f]
        return(result)
    except Exception:
        raise Exception(f"Failed to read {csv}: {Exception}")
    return(result)

qcfiles = pd.read_csv("~{qc_list_file}", header=0)
qcfiles.head()

def parse_object_url(url):
    parts = url.split('/')
    result = "/".join(parts[2:len(parts)])
    return(result)

def parse_url(url):
    parts = url.split('/')
    bucket = parts[2]
    blob_name = "/".join(parts[3:len(parts)])
    return((bucket, blob_name))

def gcp_file_exists(url, storage_client, google_project):
    bucket_name, blob_name = parse_url(url)
    bucket = storage_client.bucket(bucket_name, user_project = google_project) 
    stats = storage.Blob(bucket=bucket, name=blob_name).exists(storage_client)
    return(stats)

output_file = "~{output_prefix}.long.csv"

with open(output_file, "wt") as fout:
    qc_tbl=None
    missed=0
    for index, row in qcfiles.iterrows():
        if index % 100 == 0 and index != 0:
            print(f"{index} / {qcfiles.shape[0]}, {missed} missed ...")
            
        grid = row['GRID']
        qc_file = row['URL']
        
        if not gcp_file_exists(qc_file, sclient, google_project):
            missed = missed + 1
            continue
        
        qc_url = parse_object_url(qc_file)
        cur_qc = read_requestor_pay_csv(fs=fs, csv=qc_url, header=None)
        for line in cur_qc:
            fout.write(f"{grid},{line}")

EOF

python3 combine.py

>>>

  runtime {
    docker: docker
    memory: 10 + " GiB"
    disks: "local-disk " + 10 + " HDD"
    cpu: 1
    preemptible: 1
  }

  output {
    File combined_long_file = "~{output_prefix}.long.csv"
  }
}
