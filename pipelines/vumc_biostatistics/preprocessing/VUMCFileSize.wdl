version 1.0

workflow VUMCFileSize {
  input {
    File input_file1
    File? input_file2
    File? input_file3
    File? input_file4
    File? input_file5
    File? input_file6
  }

  Float file1_size = size(input_file1)
  Float file1_size_gb = file1_size / 1024 / 1024 / 1024

  if(defined(input_file2)){
    Float file2_size =  size(input_file2)
    Float file2_size_gb = file2_size / 1024 / 1024 / 1024
  }

  if(defined(input_file3)){
    Float file3_size =  size(input_file3)
    Float file3_size_gb = file3_size / 1024 / 1024 / 1024
  }

  if(defined(input_file4)){
    Float file4_size =  size(input_file4)
    Float file4_size_gb = file4_size / 1024 / 1024 / 1024
  }

  if(defined(input_file5)){
    Float file5_size =  size(input_file5)
    Float file5_size_gb = file5_size / 1024 / 1024 / 1024
  }

  if(defined(input_file6)){
    Float file6_size =  size(input_file6)
    Float file6_size_gb = file6_size / 1024 / 1024 / 1024
  }

  output {
    Float input_file1_size = file1_size
    Float input_file1_size_gb = file1_size_gb
    Float? input_file2_size = file2_size
    Float? input_file2_size_gb = file2_size_gb
    Float? input_file3_size = file3_size
    Float? input_file3_size_gb = file3_size_gb
    Float? input_file4_size = file4_size
    Float? input_file4_size_gb = file4_size_gb
    Float? input_file5_size = file5_size
    Float? input_file5_size_gb = file5_size_gb
    Float? input_file6_size = file6_size
    Float? input_file6_size_gb = file6_size_gb
  }
}
