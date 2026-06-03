mkdir -p /data/cqs/shengq2/temp
cd /data/cqs/shengq2/temp

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /home/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task1CreateCohortPsam.wdl \
  -i /home/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task1CreateCohortPsam.inputs.ancestry_removeGRID.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
