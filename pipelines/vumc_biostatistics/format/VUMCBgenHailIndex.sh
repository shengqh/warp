mkdir -p /data/cqs/shengq2/temp
cd /data/cqs/shengq2/temp

if [[ "1" == "1" ]]; then
  echo "Preparing agd163k_chr22.primary_pass.bgen.mt"
  java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
    -jar /data/cqs/softwares/cromwell/cromwell-90.jar \
    run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCBgenHailIndex.wdl \
    -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/format/VUMCBgenHailIndex.inputs.json \
    --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
fi

