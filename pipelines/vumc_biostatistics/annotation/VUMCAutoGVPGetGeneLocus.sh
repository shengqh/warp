if [ ! -d "/nobackup/h_cqs/shengq2/biovu/cromwell" ]; then
  mkdir /nobackup/h_cqs/shengq2/biovu/cromwell
fi

cd /nobackup/h_cqs/shengq2/biovu/cromwell

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/annotation/VUMCAutoGVPGetGeneLocus.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/annotation/VUMCAutoGVPGetGeneLocus.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
