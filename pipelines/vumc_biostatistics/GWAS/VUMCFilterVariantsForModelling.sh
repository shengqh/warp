mkdir -p /nobackup/h_cqs/shengq2/biovu/demo
cd /nobackup/h_cqs/shengq2/biovu/demo

#increased mac to 800 to speed up the process
java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCFilterVariantsForModelling.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCFilterVariantsForModelling.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json