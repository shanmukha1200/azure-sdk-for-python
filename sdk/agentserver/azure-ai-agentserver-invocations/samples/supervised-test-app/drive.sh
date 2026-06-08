#!/usr/bin/env bash
# Drive the supervised test app: show readiness across a crash and a hang.
set -u
BASE="${BASE:-http://localhost:8088}"

code() { curl -s -o /dev/null -w "%{http_code}" "$1"; }

echo "== baseline =="
echo "whoami:   $(curl -s "$BASE/control/whoami")"
echo "readiness: $(code "$BASE/readiness")  (expect 200)"

echo
echo "== crash (worker dies, supervisor restarts) =="
curl -s "$BASE/control/crash" >/dev/null
for i in $(seq 1 20); do
  echo "  t+$((i))x0.25s  readiness=$(code "$BASE/readiness")"
  sleep 0.25
done
echo "whoami:   $(curl -s "$BASE/control/whoami")  (expect NEW pid)"

echo
echo "== hang (worker alive but unresponsive for 8s) =="
curl -s "$BASE/control/hang?seconds=8" >/dev/null &
sleep 1
for i in $(seq 1 10); do
  echo "  t+$((i))s  readiness=$(code "$BASE/readiness")  (expect 424 while frozen)"
  sleep 1
done
wait
echo "readiness: $(code "$BASE/readiness")  (expect 200 again)"
