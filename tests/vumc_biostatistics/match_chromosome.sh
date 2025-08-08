if [ ! -d "/nobackup/h_cqs/shengq2/biovu/cromwell" ]; then
  mkdir /nobackup/h_cqs/shengq2/biovu/cromwell
fi

cd /nobackup/h_cqs/shengq2/biovu/cromwell

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/wdl/cromwell-84.jar \
  run /nobackup/h_cqs/shengq2/program/warp/tests/vumc_biostatistics/match_chromosome.wdl \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
