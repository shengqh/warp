mkdir -p /nobackup/h_cqs/shengq2/biovu/agd35k/pgen
cd /nobackup/h_cqs/shengq2/biovu/agd35k/pgen

#increased mac to 800 to speed up the process
java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-84.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCQCFilter.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/GWAS/VUMCQCFilter.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json