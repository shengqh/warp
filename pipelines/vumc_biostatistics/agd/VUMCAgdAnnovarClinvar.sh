mkdir -p /nobackup/h_cqs/shengq2/temp
cd /nobackup/h_cqs/shengq2/temp
java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local_auto_pull.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-53.1.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/agd/VUMCAgdAnnovarClinvar.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/agd/VUMCAgdAnnovarClinvar.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json