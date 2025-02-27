mkdir -p /nobackup/h_cqs/shengq2/biovu/agd163k/chrom-msvcf/cromwell
cd /nobackup/h_cqs/shengq2/biovu/agd163k/chrom-msvcf/cromwell

if [[ "0" == "1" ]]; then
  echo "Preparing agd163k_chrM.primary_pass.bgen.mt"
  java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
    -jar /data/cqs/softwares/wdl/cromwell-84.jar \
    run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCBgen2HailMatrix.wdl \
    -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCBgen2HailMatrix.inputs_chrM.json \
    --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
fi

if [[ "1" == "1" ]]; then
  echo "Preparing agd163k_chr22.primary_pass.bgen.mt"
  java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
    -jar /data/cqs/softwares/wdl/cromwell-84.jar \
    run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCBgen2HailMatrix.wdl \
    -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCBgen2HailMatrix.inputs_chr22.json \
    --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
fi

