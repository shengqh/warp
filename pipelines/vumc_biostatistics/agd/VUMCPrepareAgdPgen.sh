mkdir -p /nobackup/h_cqs/shengq2/biovu/agd163k/chrom-msvcf/cromwell
cd /nobackup/h_cqs/shengq2/biovu/agd163k/chrom-msvcf/cromwell

if [[ ! -s cromwell_finalOutputs/agd163k_chrM.primary_pass.pgen ]]; then
  echo "Preparing agd163k_chrM.primary_pass.pgen"
  java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
    -jar /data/cqs/softwares/wdl/cromwell-84.jar \
    run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/agd/VUMCPrepareAgdPgen.wdl \
    -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/agd/VUMCPrepareAgdPgen.inputs_chrM.json \
    --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
fi

if [[ ! -s cromwell_finalOutputs/agd163k_chr22.primary_pass.pgen ]]; then
  echo "Preparing agd163k_chr22.primary_pass.pgen"
  java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.local.conf \
    -jar /data/cqs/softwares/wdl/cromwell-84.jar \
    run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/agd/VUMCPrepareAgdPgen.wdl \
    -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/agd/VUMCPrepareAgdPgen.inputs_chr22.json \
    --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json
fi
