cd /nobackup/h_cqs/shengq2/biovu/megaex1.1/TOPMed_imputed/EUR
java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-84.jar \
  run /home/shengq2/program/warp/pipelines/vumc_biostatistics/genotype/VUMCVcfExcludeSamples.wdl \
  -i /home/shengq2/program/warp/pipelines/vumc_biostatistics/genotype/VUMCVcfExcludeSamples.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json