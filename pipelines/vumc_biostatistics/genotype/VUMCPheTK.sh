if [ ! -d "/data/cqs/shengq2/temp" ]; then
  mkdir -p /data/cqs/shengq2/temp
fi

cd /data/cqs/shengq2/temp

java -Dconfig.file=/nobackup/h_cqs/shengq2/program/cqsperl/config/wdl/cromwell.local_auto_pull.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/genotype/VUMCPheTK.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/genotype/VUMCPheTK.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
