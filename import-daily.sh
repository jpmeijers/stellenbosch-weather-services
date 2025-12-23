#!/bin/bash
#schedule this file for daily execution by cron

cd -P -- "$(dirname -- "$0")"

# Sonbesie
./sonbesie/TDailyHTML.py

