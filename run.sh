#!/bin/bash
cd /root/x_monitor
exec /usr/bin/python3 twitter_monitor.py "$@"
