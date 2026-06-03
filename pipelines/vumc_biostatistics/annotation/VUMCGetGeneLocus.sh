if [ ! -d "/data/cqs/shengq2/temp" ]; then
  mkdir /data/cqs/shengq2/temp
fi

cd /data/cqs/shengq2/temp

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/annotation/VUMCGetGeneLocus.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/annotation/VUMCGetGeneLocus.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
