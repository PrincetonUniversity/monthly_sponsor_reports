#!/bin/bash

ssh tiger '/home/jdh4/bin/monthly_sponsor_reports/data_for_user_reports.sh'
DT=$(date --date='-1 months' +%Y%m%d)
OUTFILE=cache_sacct_${DT}_tiger_users.csv
scp tiger:/home/jdh4/bin/monthly_sponsor_reports/${OUTFILE} .
