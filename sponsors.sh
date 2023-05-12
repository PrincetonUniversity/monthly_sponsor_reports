#!/bin/bash
PY3=/usr/licensed/anaconda3/2023.3/bin
MTH=/home/jdh4/bin/monthly_sponsor_reports
SECS=$(date +%s)
${PY3}/python -uB ${MTH}/monthly_sponsor_reports.py \
                         --report-type=sponsors \
                         --basepath=${MTH} \
                         --months=3 > ${MTH}/archive/sponsors.log.${SECS} 2>&1
