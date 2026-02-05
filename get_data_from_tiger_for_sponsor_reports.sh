#!/bin/bash

ssh tiger '/home/jdh4/bin/monthly_sponsor_reports/data_for_sponsor_reports.sh'
DT=$(date --date='-3 months' +%Y%m%d)
OUTFILE=cache_sacct_${DT}_tiger.csv
scp tiger:/home/jdh4/bin/monthly_sponsor_reports/${OUTFILE} .
