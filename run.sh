#!/bin/bash
set -euo pipefail
cd /root/x_monitor
exec /usr/bin/timeout 25m /usr/bin/python3 twitter_monitor.py "$@"
