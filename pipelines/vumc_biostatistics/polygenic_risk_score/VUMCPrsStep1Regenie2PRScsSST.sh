cd /nobackup/h_cqs/shengq2/biovu/prs/

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-84.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/polygenic_risk_score/VUMCPrsStep1Regenie2PRScsSST.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/polygenic_risk_score/VUMCPrsStep1Regenie2PRScsSST.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
