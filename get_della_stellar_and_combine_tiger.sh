#!/bin/bash

cd /home/jdh4/FROM_TIGER/monthly_sponsor_reports

module purge
module load anaconda3/2023.3
python -uB monthly_sponsor_reports.py --report=users --months=1 --basepath=$(pwd)

DT=$(date --date='-1 months' +%Y%m%d)
OUTFILE=cache_sacct_${DT}_tiger_users.csv

# remove the first line of the tiger file
sed -i '1d' ${OUTFILE}

cat cache_sacct_${DT}.csv ${OUTFILE} > CACHE.csv
wc -l cache_sacct_${DT}.csv
wc -l ${OUTFILE}
wc -l CACHE.csv
