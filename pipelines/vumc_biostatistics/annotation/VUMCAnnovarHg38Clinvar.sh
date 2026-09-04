if [ ! -d "/data/cqs/shengq2/temp" ]; then
  mkdir /data/cqs/shengq2/temp
fi

cd /data/cqs/shengq2/temp

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /home/shengq2/program/warp/pipelines/vumc_biostatistics/annotation/VUMCAnnovarHg38Clinvar.wdl \
  -i /home/shengq2/program/warp/pipelines/vumc_biostatistics/annotation/VUMCAnnovarHg38Clinvar.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
