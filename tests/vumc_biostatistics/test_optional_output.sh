cd /nobackup/h_cqs/shengq2/test
java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
  run /nobackup/h_cqs/shengq2/program/warp/tests/vumc_biostatistics/test_optional_output.wdl