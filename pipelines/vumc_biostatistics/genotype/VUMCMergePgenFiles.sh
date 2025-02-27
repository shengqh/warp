cd /nobackup/h_cqs/shengq2/biovu/agd35k/primary_pass_pgen

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-84.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/genotype/VUMCMergePgenFiles.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/genotype/VUMCMergePgenFiles.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json