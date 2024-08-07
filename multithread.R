library(jqr)
library(dplyr)
library(tidyjson)
library(ggplot2)
library(tidyr)

d<-c(readLines('../b2024r/bench1.out'),readLines('../b2024r/bench2.out'))|>
  jq('{
  build:(.build|del(.cargo_cfg,.diff_files,.cargo_features)),
  seqlock_mode:first(.build.cargo_features[] | select(contains("SEQLOCK"))),
  bench:.benches | to_entries | .[] | ({name:.key}+.value)
  } | (.bench_name=.bench.name) | del(.bench.name)')|>
  spread_all()