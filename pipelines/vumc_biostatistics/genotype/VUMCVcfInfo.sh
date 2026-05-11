cd /nobackup/h_vangard_1/shengq2/biovu

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /home/shengq2/program/warp/pipelines/vumc_biostatistics/genotype/VUMCVcfInfo.wdl \
  -i /home/shengq2/program/warp/pipelines/vumc_biostatistics/genotype/VUMCVcfInfo.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json