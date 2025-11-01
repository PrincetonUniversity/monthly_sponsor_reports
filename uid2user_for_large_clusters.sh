#!/bin/bash

cd /home/jdh4/monthly_reports_with_storage/monthly_sponsor_reports

# stellar
ssh stellar 'getent passwd | awk -F":" "{print \$3\",\" \$1}"' > stellar.uids
wc -l stellar.uids

# tiger
ssh tiger   'getent passwd | awk -F":" "{print \$3\",\" \$1}"' > tiger.uids
wc -l tiger.uids
 
# della
getent passwd | awk -F":" '{print $3","$1}' > della.uids
wc -l della.uids

# combine files
cat della.uids stellar.uids tiger.uids | sort | uniq > master.uids
rm della.uids stellar.uids tiger.uids

# remove entries with begin with a comma
sed -i '/^,/d' master.uids

wc -l master.uids
