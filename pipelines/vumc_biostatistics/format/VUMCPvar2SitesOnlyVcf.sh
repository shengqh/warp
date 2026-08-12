mkdir -p /nobackup/h_cqs/shengq2/biovu/agd35k/cromwell
cd /nobackup/h_cqs/shengq2/biovu/agd35k/cromwell

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCPvar2SitesOnlyVcf.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCPvar2SitesOnlyVcf.inputs.agd35k.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json

# java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
#   -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
#   run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCPvar2SitesOnlyVcf.wdl \
#   -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCPvar2SitesOnlyVcf.inputs.agd250k.json \
#   --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json  