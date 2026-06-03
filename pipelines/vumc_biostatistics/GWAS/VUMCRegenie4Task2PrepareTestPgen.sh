mkdir -p /data/cqs/shengq2/temp
cd /data/cqs/shengq2/temp

# java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
#   -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
#   run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task2PrepareTestPgen.wdl \
#   -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task2PrepareTestPgen.inputs.json \
#   --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task2PrepareTestPgen.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCRegenie4Task2PrepareTestPgen.QCV2.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
