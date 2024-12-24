mkdir -p /nobackup/h_cqs/shengq2/biovu/temp
cd /nobackup/h_cqs/shengq2/biovu/temp

#increased mac to 800 to speed up the process
java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-84.jar \
  run /nobackup/h_cqs/shengq2/program/warp/tasks/vumc_biostatistics/order_files_by_strings.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/tasks/vumc_biostatistics/order_files_by_strings.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json