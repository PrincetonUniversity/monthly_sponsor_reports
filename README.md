**February 22, 2025: This code is still being generalized for use at all institutions.**

# Monthly Sponsor and User Reports

The software in this repo generates monthly sponsor and user reports for the Research Computing clusters. Below is an example sponsor report:

```
Sponsor: Garegin Andrea (gandrea)
 Period: Nov 1, 2021 - Jan 31, 2022

You are receiving this report because you sponsor researchers on the
Research Computing systems. The report below shows the researchers
that you sponsor as well as their cluster usage. Only researchers
that ran at least one job during the reporting period appear in the
tables below. There are no financial costs for using the systems.


                                   Della                                   
----------------------------------------------------------------------------
 NetID          Name        CPU-hours    GPU-hours  Jobs Account Partition(s)
-----------------------------------------------------------------------------
edevonte    Egino Devonte  125017 (59%)       0     3465   phys           cpu
mlakshmi   Moacir Lakshmi   82638 (39%)       0       63    lit   cpu,datasci 
  rgozde     Robert Gözde    4238  (2%)    1018      255    lit       cpu,gpu

Your group used 211893 CPU-hours or 1.7% of the 12321247 total CPU-hours
on Della. Your group is ranked 20 of 169 by CPU-hours used. Similarly,
your group used 1018 GPU-hours or 1.2% of the 88329 total GPU-hours
yielding a ranking of 18 of 169 by GPU-hours used.


                                   Tiger                                   
-----------------------------------------------------------------------------
 NetID       Name         CPU-hours    GPU-hours   Jobs  Account Partition(s)
-----------------------------------------------------------------------------
jiryna   Jaxson Iryna   1065273 (92%)        0      152    math    serial,cpu 
  sime    Shahnaz Ime     98071  (8%)     3250    11092     pol           gpu 

Your group used 1163344 CPU-hours or 3.0% of the 35509100 total CPU-hours
on Tiger. Your group is ranked 7 of 101 by CPU-hours used. Similarly,
your group used 3250 GPU-hours or 0.6% of the 554101 total GPU-hours
yielding a ranking of 45 of 101 by GPU-hours used.


                                   Detailed Breakdown
--------------------------------------------------------------------------------------
Cluster  NetID  Partition  CPU-hours CPU-rank CPU-eff GPU-hours GPU-rank GPU-eff  Jobs
--------------------------------------------------------------------------------------
 Della  edevonte     cpu     125017   12/231     88%      N/A       N/A     N/A   3465
 Della  mlakshmi     cpu      80638  121/231     68%      N/A       N/A     N/A     11
 Della  mlakshmi      ds       2000     2/16     71%      N/A       N/A     N/A     22
 Della    rgozde     cpu       3238     6/79     95%      N/A       N/A     N/A     41
 Della    rgozde     gpu       1000    16/49     91%      250      7/17     52%    101
 Tiger    jiryna  serial    1065273    17/22     91%      N/A       N/A     N/A    252
 Tiger      sime     gpu      98071    26/41      9%     3250     29/41     17%    192


Definitions: A 2-hour job (wall-clock time) that allocates 4 CPU-cores
consumes 8 CPU-hours. Similarly, a 2-hour job that allocates 4 GPUs
consumes 8 GPU-hours. If a group is ranked 5 of 20 then it used the
fifth most CPU-hours or GPU-hours of the 20 groups.

Replying to this email will open a ticket with CSES. Please reply
with questions/comments or to unsubscribe from these reports.
```

The names above were created [randomly](https://www.behindthename.com/random/).

## Usage

Obtain the code:

```bash
$ ssh <YourNetID>@della.princeton.edu
$ git clone https://github.com/PrincetonUniversity/monthly_sponsor_reports.git
$ cd monthly_sponsor_reports
$ wget https://raw.githubusercontent.com/jdh4/job_defense_shield/main/efficiency.py
```

Examine the help menu:

```bash
$ module load anaconda3/2023.3
$ python monthly_sponsor_reports.py -h
usage: monthly_sponsor_reports.py [-h] --report-type {sponsors,users} \
                                       --months N \
                                       --basepath PATH \
                                       [--email]

Monthly Sponsor and User Reports

options:
  -h, --help            show this help message and exit
  --report-type {sponsors,users}
                        Specify the report type
  --months N            Reporting period covers N months
  --basepath PATH       Specify the path to this script
  --email               Flag to send reports via email
```

Run the unit tests:

```bash
$ python -uB -m unittest tests/test_monthly_sponsor_reports.py -v
```

### Sponsor Reports

If all of the tests pass then do a dry run (which takes a few minutes):

```bash
$ python monthly_sponsor_reports.py --report-type=sponsors \
                                    --months=3 \
                                    --basepath=$(pwd)
```

It is normal to see warnings like the following during the dry run:

```
...
W: Sponsor entry of mbreixo (Mari Breixo) found for jiryna on stellar. Corrected to mbreixo.
W: Sponsor entry of USER found for ec2342 on della. Corrected to ecmorale.
W: Primary sponsor for kb4172 taken from CSV file.
W: User mlakshmi has multiple primary sponsors: gandrea,cbus. Using gandrea.
...
```

The output will be sent to stdout instead of email for the dry run. If the output looks good then run once more with emails enabled:

```bash
$ python monthly_sponsor_reports.py --report-type=sponsors \
                                    --months=3 \
                                    --basepath=$(pwd) \
                                    --email
```

### User Reports

For user reports, one uses:

```bash
$ python monthly_sponsor_reports.py --report-type=users \
                                    --months=1 \
                                    --basepath=$(pwd)
```

And then:

```
$ python monthly_sponsor_reports.py --report-type=users \
                                    --months=1 \
                                    --basepath=$(pwd) \
                                    --email
```

## Dry Run

- hard code the date range at the top  
- comment out the assert statement which checks for 1st or 15th of month  
- run it  
- then remove brakefile, uncomment assert, comment date range and remove cache files  


## Definitions

A 2-hour job (wall-clock time) that allocates 4 CPU-cores consumes 8 CPU-hours. Similarly, a 2-hour job that allocates 4 GPUs consumes 8 GPU-hours. If a group is ranked 5 of 20 then it used the fifth most CPU-hours or GPU-hours of the 20 groups.

`CPU-eff` is the CPU efficiency. It is computed as the ratio of the sums of the CPU time used by all of the CPU-cores and the CPU time allocated, which is the product of the elapsed time of the job and the number of CPU-cores, over all jobs. A 4 CPU-core job that runs for 100 minutes where each CPU-core uses the CPU for 90 minutes has an efficiency of 90%.

## Cron

These reports run under cron on tigergpu:

```bash
[jdh4@tigergpu ~]$ crontab -l
SHELL=/bin/bash
MAILTO=admin@princeton.edu
MTH=/home/jdh4/bin/monthly_sponsor_reports
52 8  1 * * ${MTH}/sponsors.sh
52 8 15 * * ${MTH}/users.sh
```

The scripts are:

```bash
$ cat sponsors.sh
#!/bin/bash
PY3=/usr/licensed/anaconda3/2023.3/bin
MTH=/home/jdh4/bin/monthly_sponsor_reports
SECS=$(date +%s)
${PY3}/python -uB ${MTH}/monthly_sponsor_reports.py \
                         --report-type=sponsors \
                         --months=3 \
                         --basepath=${MTH} \
                         --email > ${MTH}/archive/sponsors.log.${SECS} 2>&1
```

```bash
$ cat users.sh
#!/bin/bash
PY3=/usr/licensed/anaconda3/2023.3/bin
MTH=/home/jdh4/bin/monthly_sponsor_reports
SECS=$(date +%s)
${PY3}/python -uB ${MTH}/monthly_sponsor_reports.py \
                         --report-type=users \
                         --months=1 \
                         --basepath=${MTH} \
                         --email > ${MTH}/archive/users.log.${SECS} 2>&1
```

## Partitions

The code depends on the partitions across the clusters:

```
$ sacct -S 2022-04-01 -L -a -X -n -o cluster,partition%25 | sort | uniq
     della                    callan 
     della                       cpu 
     della               cpu,physics 
     della                    cryoem 
     della               datascience 
     della                     donia 
     della                       gpu 
     della                    gpu-ee 
     della                   gputest 
     della                     malik 
     della                    motion 
     della                    orfeus 
     della                   physics 
     della               physics,cpu 
   perseus                       all 
   stellar                       all 
   stellar                     cimes 
   stellar                       gpu 
   stellar                      pppl 
   stellar                        pu 
   stellar                    serial 
    tiger2                       cpu 
    tiger2                    cryoem 
    tiger2                       ext 
    tiger2                       gpu 
    tiger2                    motion 
    tiger2                    serial 
  traverse                       all 
     tukey                       all 
```

It is a good idea to make sure that the partitions used in the code are up to date.

## One-liners

The commands below illustrates the essence of the software in this repo. To compute the CPU-seconds of all jobs on the `cimes` partition in April 2022:

```
$ sacct -a -X -n -S 2022-04-01T00:00:00 -E 2022-04-30T23:59:59 -o cputimeraw --partition cimes | awk '{sum += $1} END {print sum}'
```

To compute the CPU-seconds used by the user `aturing` in April 2022:

```
$ sacct -u aturing -X -n -S 2022-04-01T00:00:00 -E 2022-04-30T23:59:59 -o cputimeraw | awk '{sum += $1} END {print sum}'
```

To compute CPU-hours (not CPU-seconds) for user `msbc` on the cluster `perseus`:

```
$ sacct -u msbc -X -n -M perseus -S 2020-05-15T00:00:00 -E 2021-05-14T23:59:59 -o cputimeraw | awk '{sum += $1} END {print int(sum/3600)}'
```

List jobs of user `aturing` on the cluster `tiger2` on the partition `cpu` sorted by CPU-seconds:

```
$ sacct -u aturing -X -P -n -M tiger2 -r cpu -S 2023-08-01T00:00:00 -E 2023-08-31T23:59:59 -o jobid,cputimeraw,start,nnodes,ncpus,jobname | sort -k2 -n -t'|' -r | sed 's/|/\t/g'
```

CPU-hours of GPU jobs on Stellar by users in cbe account (watch out for accounts with a comma in the name like "astro,kunz"):

```
sacct -o cputimeraw -a -P -X -n -M stellar --starttime=2021-10-15 -E 2022-10-14 --accounts=cbe --partition=gpu | awk '{sum += $1} END {print int(sum/3600)}'
```

CPU-hours for each account on Stellar:

```
for act in `sacct -S 2022-04-01 -M stellar -a -X -n -o account | sort | uniq`; do printf "$act "; sacct -o cputimeraw -a -P -X -n --starttime=2022-04-01 -E now --accounts=${act} | awk '{sum += $1} END {print int(sum/3600)}';  done
```

GPU-hours for a given user:

```
sacct -M stellar -r gpu -u ps9551 -X -P -n -S 2025-05-01T00:00:00 -E now -o elapsedraw,alloctres,state | grep gres/gpu=[1-9] | grep -v RUNNING | sed -E "s/\|.*gpu=/,/" | awk -F"," '{sum += $1*$2} END {print int(sum/3600)}'
```

Compute cpu-hours for certain users:

```bash
#/bin/bash
  
# always examine the accounts since they can get combined like on della with "cpu,physics"

STARTDATE="2017-08-01"
ENDDATE="2020-10-01"
CLUSTER="perseus"
ACCOUNTS="astro,kunz"

USERS=$(sacct -S ${STARTDATE} -E ${ENDDATE} -M ${CLUSTER} -a -X -n -o user --accounts=${ACCOUNTS} | sort | uniq)
for USER in ${USERS}
do
    sacct -S ${STARTDATE} -E ${ENDDATE} -M ${CLUSTER} -X -n -o cputimeraw -u ${USER} --accounts=${ACCOUNTS} 1>/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "${USER} NULL"
        continue
    fi
    printf "${USER} "
    sacct -S ${STARTDATE} -E ${ENDDATE} -M ${CLUSTER} -X -n -o cputimeraw -u ${USER} --accounts=${ACCOUNTS} | awk '{sum += $1} END {print int(sum/3600)}'
done
```

Percentage of available GPU-hours used on cryoem:

```
$ DAYS=90; GPUS=136; PARTITION=cryoem; sacct -M della -a -X -P -n -S $(date -d"${DAYS} days ago" +%F) -E now -o elapsedraw,alloctres,state --partition=${PARTITION} | grep gres/gpu=[1-9] | sed -E "s/\|.*gpu=/,/" | awk -v gpus=${GPUS} -v days=${DAYS} -F"," '{sum += $1*$2} END {print "GPU-hours used = "int(100*sum/3600/24/gpus/days)"%"}'
GPU-hours used = 10%
```

## Be Aware

- A sponsor will only receive a report if one of their users ran at least one job in the reporting period.  
- If the sponsor is not found for a given user on a given cluster then that record is omitted. These events can be seen in the output and should be addressed. 
- The script must be executed on a machine that can talk to ldap1.rc.princeton.edu.  
- The script is written to only send emails on the 1st and 15th of the month. It cannot runs twice in a 7 day period (see `.brakefile` in "sanity checks and safeguards" in Python script).  
- Rankings on Stellar are over all groups, i.e., the PU/PPPL and CIMES portions are not separated.
