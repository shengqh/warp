cd /data/h_gelbard_lab/projects/20250605_PRS

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-84.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/polygenic_risk_score/VUMCPRScs.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/polygenic_risk_score/VUMCPRScs.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
