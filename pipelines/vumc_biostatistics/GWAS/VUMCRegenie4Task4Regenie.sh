cd /panfs/accrepfs.vampire/nobackup/h_cqs/shengq2/biovu/demo

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-90.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task4Regenie.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task4Regenie.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json