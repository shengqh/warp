if [[ "0" == "1" ]]; then
  mkdir -p /nobackup/h_cqs/shengq2/biovu/agd163k/chrom-msvcf/cromwell
  cd /nobackup/h_cqs/shengq2/biovu/agd163k/chrom-msvcf/cromwell

  java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
    -jar /data/cqs/softwares/wdl/cromwell-84.jar \
    run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCBed2HailMatrix.wdl \
    -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCBed2HailMatrix.inputs_chrM.json \
    --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
fi

if [[ "1" == "1" ]]; then
  mkdir -p /nobackup/h_cqs/shengq2/biovu/agd35k/ICA-AGD/cromwell
  cd /nobackup/h_cqs/shengq2/biovu/agd35k/ICA-AGD/cromwell

  java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
    -jar /data/cqs/softwares/wdl/cromwell-84.jar \
    run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCBed2HailMatrix.wdl \
    -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCBed2HailMatrix.inputs_chrY.json \
    --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
fi

if [[ "0" == "1" ]]; then
  mkdir -p /nobackup/h_cqs/shengq2/biovu/agd35k/ICA-AGD/cromwell
  cd /nobackup/h_cqs/shengq2/biovu/agd35k/ICA-AGD/cromwell

  java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
    -jar /data/cqs/softwares/wdl/cromwell-84.jar \
    run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCBed2HailMatrix.wdl \
    -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCBed2HailMatrix.inputs_chr12.json \
    --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
fi