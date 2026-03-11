cd /nobackup/h_cqs/shengq2/test

java -Dconfig.file=/data/cqs/softwares/cqsperl/config/wdl/cromwell.slurm.20220714.conf \
  -jar /data/cqs/softwares/cromwell/cromwell-92.jar \
  run /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/dna_seq/germline/variant_calling/VUMCVariantCalling.wdl \
  -i /nobackup/h_cqs/shengq2/program/warp/pipelines/vumc_biostatistics/dna_seq/germline/variant_calling/VUMCVariantCalling.inputs.json \
  --options /data/cqs/softwares/cqsperl/config/wdl/cromwell.options.json