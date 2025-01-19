version 1.0

import "../../../tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

/**
 * Workflow: VUMCGetFilesizeOfListLarge
 * 
 * Description:
 * This workflow calculates the file sizes of a list of GCP files. It is designed to handle large datasets efficiently. 
 * The qc_list_file might exceed 10000000 bytes which is the limitation of read_lines in WDL.
 * 
 * Input Parameters:
 * - List[File] qc_list_file: A list of files for which the sizes need to be calculated. Two columns, the first is name and the second is the URL.
 * - String output_prefix: The output prefix.
 * 
 * Author:
 * Quanhu Sheng, quanhu.sheng.1@vumc.org
 */
 Workflow: VUMCGetFilesizeOfListLarge {
  input {
    List[File] qc_list_file
    String output_prefix
    String project_id
    String? target_gcp_folder  
  }

  call GetFileSizeInArray {
    input:
      project_id = project_id,
      qc_list_file = qc_list_file,
      output_prefix = output_prefix
  }

  if(defined(target_gcp_folder)){
    call GcpUtils.MoveOrCopyOneFile as CopyFile {
      input:
        source_file = GetFileSizeInArray.output_size_file,
        is_move_file = false,
        project_id = project_id,
        target_gcp_folder = select_first([target_gcp_folder])
    }
  }

  output {
    File output_file = select_first([CopyFile.output_file, GetFileSizeInArray.output_size_file])
  }
}

task GetFileSizeInArray {
  input {
    String project_id
    File qc_list_file
    String output_prefix
    String docker = "shengqh/hail_gcp:20241127"
  }

  command <<<

cat<<EOF > size.py

import pandas as pd
import os
import gcsfs
from google.cloud import storage

pd.options.mode.chained_assignment = None

sclient = storage.Client()
google_project = "~{project_id}"

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

def sizeof_fmt(num, suffix="B"):
    for unit in ["", "K", "M", "G", "T"]:
        if abs(num) < 1024.0:
            return f"{num:3.2f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:3.2f} {suffix}"

def gcp_file_size(url, storage_client, google_project):
    if gcp_file_exists(url, storage_client, google_project):
        bucket_name, blob_name = parse_url(url)
        bucket = storage_client.bucket(bucket_name, user_project=google_project) 
        blob = bucket.get_blob(blob_name)
        return(blob.size)

output_file = "~{output_prefix}.size.csv"

with open(output_file, "wt") as fout:
    fout.write(f"GRID,URL,FileSize\n")
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
        
        size = gcp_file_size(qc_file, sclient, google_project)
        size_str = sizeof_fmt(size)
        fout.write(f"{grid},{qc_file},{size_str}\n")

EOF

python3 size.py

>>>

  runtime {
    docker: docker
    memory: 10 + " GiB"
    disks: "local-disk " + 10 + " HDD"
    cpu: 1
    preemptible: 1
  }

  output {
    File output_size_file = "~{output_prefix}.size.csv"
  }
}
