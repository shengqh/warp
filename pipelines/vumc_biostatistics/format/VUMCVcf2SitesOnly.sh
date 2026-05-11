cd /nobackup/h_cqs/shengq2/biovu/temp

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCVcf2SitesOnly.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCVcf2SitesOnly.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
