if [ ! -d "/nobackup/h_cqs/shengq2/biovu/cromwell" ]; then
  mkdir /nobackup/h_cqs/shengq2/biovu/cromwell
fi

cd /nobackup/h_cqs/shengq2/biovu/cromwell

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /home/shengq2/program/warp/pipelines/vumc_biostatistics/annotation/VUMCAnnovarWithVcfColumns.wdl \
  -i /home/shengq2/program/warp/pipelines/vumc_biostatistics/annotation/VUMCAnnovarWithVcfColumns.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
