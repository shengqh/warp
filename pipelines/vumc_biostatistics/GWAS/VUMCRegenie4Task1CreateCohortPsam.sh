mkdir -p /nobackup/h_cqs/shengq2/biovu/demo
cd /nobackup/h_cqs/shengq2/biovu/demo

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-84.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task1CreateCohortPsam.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task1CreateCohortPsam.inputs.ancestry.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
  
java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-84.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task1CreateCohortPsam.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task1CreateCohortPsam.inputs.grid.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-84.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task1CreateCohortPsam.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task1CreateCohortPsam.inputs.both.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
