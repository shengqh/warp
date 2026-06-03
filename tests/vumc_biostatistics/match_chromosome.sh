if [ ! -d "/data/cqs/shengq2/temp" ]; then
  mkdir /data/cqs/shengq2/temp
fi

cd /data/cqs/shengq2/temp

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /nobackup/h_cqs/shengq2/program/warp/tests/vumc_biostatistics/match_chromosome.wdl \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
