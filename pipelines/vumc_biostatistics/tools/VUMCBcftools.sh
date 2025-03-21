mkdir -p /nobackup/h_cqs/shengq2/biovu/temp
cd /nobackup/h_cqs/shengq2/biovu/temp

if [ '0' == '1' ]; then
  java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
    -jar /data/cqs/softwares/wdl/cromwell-84.jar \
    run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/tools/VUMCBcftools.wdl \
    -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/tools/VUMCBcftools.gtonly.inputs.json \
    --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
fi

if [ '1' == '1' ]; then
  java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
    -jar /data/cqs/softwares/wdl/cromwell-84.jar \
    run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/tools/VUMCBcftools.wdl \
    -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/tools/VUMCBcftools.extract.inputs.json \
    --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
fi