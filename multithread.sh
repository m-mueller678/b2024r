for ((i=1; i<=3; i++)); do
  KEY_COUNT=3e7 KEY_NAME=dense64 THREADS=24 LOOKUP_DURATION=15 cargo bench -q --bench multithread --features seqlock_eub
  KEY_COUNT=3e7 KEY_NAME=dense64 THREADS=24 LOOKUP_DURATION=15 cargo bench -q --bench multithread --features seqlock_ub
  KEY_COUNT=3e7 KEY_NAME=dense64 THREADS=24 LOOKUP_DURATION=15 cargo bench -q --bench multithread --features seqlock_ab
  KEY_COUNT=3e7 KEY_NAME=dense64 THREADS=24 LOOKUP_DURATION=15 cargo bench -q --bench multithread --features seqlock_asm
done
