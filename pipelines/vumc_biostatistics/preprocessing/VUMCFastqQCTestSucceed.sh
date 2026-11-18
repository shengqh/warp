cd /nobackup/h_cqs/shengq2/test

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local_auto_pull.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-53.1.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/preprocessing/VUMCFastqQC.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/preprocessing/VUMCFastqQC.inputs.local.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
