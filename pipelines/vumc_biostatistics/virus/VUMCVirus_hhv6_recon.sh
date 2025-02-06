mkdir -p /nobackup/h_cqs/shengq2/biovu/virus
cd /nobackup/h_cqs/shengq2/biovu/virus
java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-84.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/virus/VUMCVirus_hhv6_recon.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/virus/VUMCVirus_hhv6_recon.inputs.all_virus.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json