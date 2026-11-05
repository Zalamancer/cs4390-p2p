#!/usr/bin/env bash
# Starter script for the final demo, following the project guidelines:
#   time 0     tracker, peer 1 (small file), peer 2 (large file)
#   time 30s   peers 3 to 8 start and download both files
#   time 1:30  peers 9 to 13 start and download both files; peers 1 and 2 stop
# Everything runs under the folder "run", which is emptied first, so the tracker
# starts with no tracker files. All output also goes to demo.log.
# Usage: ./start_demo.sh [config file]

cd "$(dirname "$0")" || exit 1
CONFIG=${1:-demo.cfg}
source "$CONFIG" || exit 1

make -f makefile || exit 1

exec > >(tee demo.log) 2>&1

rm -rf run
mkdir -p run/seed_files

# Run a peer in its own folder, with its own config files.
start_peer() {
    local n=$1 mode=$2 dir="run/peer$n"
    mkdir -p "$dir/shared"
    cp peer1/peer "$dir/peer"
    printf '%s\n%s\n%s\n' "$TRACKER_PORT" "$TRACKER_IP" "$UPDATE_INTERVAL_SEC" > "$dir/clientThreadConfig.cfg"
    printf '%s\nshared\n' "$((PEER_BASE_PORT + n))" > "$dir/serverThreadConfig.cfg"
    [ -n "$3" ] && cp "run/seed_files/$3" "$dir/shared/"
    (cd "$dir" && exec ./peer --name "Peer$n" $mode --threads "$THREADS" \
        --segment-bytes "$SEGMENT_BYTES" --chunk-delay-ms "$CHUNK_DELAY_MS" \
        --retry-sec "$RETRY_SEC" --bad-peer-sec "$BAD_PEER_SEC" </dev/null) &
    PIDS[$n]=$!
}

cleanup() {
    for pid in "${PIDS[@]}" "$TRACKER_PID"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null
    done
    wait 2>/dev/null
}
trap cleanup EXIT

head -c "$SMALL_FILE_BYTES" /dev/urandom > "run/seed_files/$SMALL_FILE"
head -c "$LARGE_FILE_BYTES" /dev/urandom > "run/seed_files/$LARGE_FILE"

printf '%s\ntorrents\n%s\n' "$TRACKER_PORT" "$DEAD_PEER_SEC" > run/sconfig
cp tracker run/tracker
(cd run && exec ./tracker </dev/null) &
TRACKER_PID=$!
sleep 1

echo "=== time 0: tracker, peer 1 and peer 2 ==="
START=$SECONDS
start_peer 1 "" "$SMALL_FILE"
start_peer 2 "" "$LARGE_FILE"

sleep "$SECOND_WAVE_SEC"
echo "=== time $((SECONDS - START))s: starting peers 3 to 8 ==="
for n in 3 4 5 6 7 8; do
    start_peer "$n" --auto
done

sleep "$((THIRD_WAVE_SEC - SECOND_WAVE_SEC))"
echo "=== time $((SECONDS - START))s: starting peers 9 to 13 ==="
for n in 9 10 11 12 13; do
    start_peer "$n" --auto
done

for n in 1 2; do
    kill "${PIDS[$n]}" 2>/dev/null
    wait "${PIDS[$n]}" 2>/dev/null
    unset "PIDS[$n]"
    echo "Peer$n terminated"
done

# Wait for every downloader to hold both complete files.
everyone_finished() {
    for n in 3 4 5 6 7 8 9 10 11 12 13; do
        [ -f "run/peer$n/shared/$SMALL_FILE" ] && [ -f "run/peer$n/shared/$LARGE_FILE" ] || return 1
    done
}
while ! everyone_finished; do
    if [ "$((SECONDS - START))" -ge "$FINISH_TIMEOUT_SEC" ]; then
        echo "=== timed out waiting for downloads ==="
        break
    fi
    sleep 2
done

echo "=== time $((SECONDS - START))s: checking every downloaded file ==="
failures=0
for n in 3 4 5 6 7 8 9 10 11 12 13; do
    for file in "$SMALL_FILE" "$LARGE_FILE"; do
        if ! cmp -s "run/seed_files/$file" "run/peer$n/shared/$file"; then
            echo "Peer$n: $file is missing or different"
            failures=$((failures + 1))
        fi
    done
done

if [ "$failures" -eq 0 ]; then
    echo "=== DEMO PASSED: peers 3 to 13 all hold exact copies of both files ==="
else
    echo "=== DEMO FAILED: $failures problem(s) ==="
    exit 1
fi
