cd /nobackup/h_cqs/shengq2/biovu/20250519_red_gates_fundation_snv_extraction/20250805/
java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-84.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/agd/VUMCAgdPgenExtractSampleWithVariantByPvarFormatResult.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/agd/VUMCAgdPgenExtractSampleWithVariantByPvarFormatResult.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json