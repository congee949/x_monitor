#!/bin/bash
set -uo pipefail
cd /root/x_monitor

main_rc=0
/usr/bin/timeout 25m /usr/bin/python3 twitter_monitor.py "$@" || main_rc=$?

# Hermes preference provenance is a best-effort sidecar.  Its transport must
# never turn a successful X Monitor run into a cron failure (or hide a real
# monitor failure), so the final status is always the primary timeout/python rc.
if ! /usr/bin/timeout 2m /root/x_monitor/sync_sent_content_ledger.sh; then
    echo "WARNING: sent-content ledger sync failed (ignored; x-monitor rc=${main_rc})" >&2
fi

exit "$main_rc"
