mkdir -p /nobackup/h_cqs/shengq2/test
cd /nobackup/h_cqs/shengq2/test

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCUntarPairFASTQ.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCUntarPairFASTQ.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
