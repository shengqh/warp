cd /data/h_gelbard_lab/projects/20250605_PRS

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/plink/VUMCPlink2PolygenicRiskScore.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/plink/VUMCPlink2PolygenicRiskScore.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
