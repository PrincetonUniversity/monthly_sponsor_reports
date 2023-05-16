#!/bin/bash
PY3=/usr/licensed/anaconda3/2023.3/bin
MTH=/home/jdh4/bin/monthly_sponsor_reports
SECS=$(date +%s)
${PY3}/python -uB ${MTH}/monthly_sponsor_reports.py \
                         --report-type=sponsors \
                         --months=3 \
                         --basepath=${MTH} \
                         --email > ${MTH}/archive/sponsors.log.${SECS} 2>&1
