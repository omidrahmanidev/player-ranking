#!/usr/bin/env bash
set -euo pipefail
player=${1:-player-42}
score=${2:-50}
if [[ ! "$player" =~ ^[a-zA-Z0-9_-]{1,64}$ || ! "$score" =~ ^[0-9]+$ ]]; then
  printf 'Usage: bash scripts/send-score.sh PLAYER POSITIVE_SCORE\n' >&2
  exit 1
fi
event_id=${EVENT_ID:-$(cat /proc/sys/kernel/random/uuid)}
event_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl --fail-with-body -sS "${GATEWAY_URL:-http://localhost:8080}/api/v1/scores" \
  -H 'Content-Type: application/json' \
  --data "{\"eventId\":\"$event_id\",\"playerId\":\"$player\",\"score\":$score,\"eventTime\":\"$event_time\"}"
printf '\n'
