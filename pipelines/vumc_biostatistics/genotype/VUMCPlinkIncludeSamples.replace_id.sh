cd /nobackup/h_cqs/shengq2/biovu/agd35k/filter
java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-84.jar \
  run /home/shengq2/program/warp/pipelines/vumc_biostatistics/genotype/VUMCPlinkIncludeSamples.wdl \
  -i /data/cqs/shengq2/program/warp/pipelines/vumc_biostatistics/genotype/VUMCPlinkIncludeSamples.replace_id.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json