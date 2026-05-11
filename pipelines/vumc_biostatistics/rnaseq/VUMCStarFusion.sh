cd /nobackup/h_cqs/shengq2/test
java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /home/shengq2/program/warp/pipelines/vumc_biostatistics/rnaseq/VUMCStarFusion.wdl \
  -i /home/shengq2/program/warp/pipelines/vumc_biostatistics/rnaseq/VUMCStarFusion.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json