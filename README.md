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
 Della  rgozde    serial     11688      6/79     95%      N/A       N/A     N/A     41
 Tiger  jiryna  all(cpu)         1     17/22      --      N/A       N/A     N/A      9
 Tiger  jiryna  all(gpu)       498     26/41      9%       40     29/41     17%    366


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
```

Run the unit tests:

```bash
$ module load anaconda3/2021.11
$ python -uB -m unittest tests/test_monthly_sponsor_reports.py -v
```

### Sponsor Reports

If all of the tests pass then do a dry run (which takes a few minutes):

```bash
$ python monthly_sponsor_reports.py --report-type=sponsors --months=3
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
$ python monthly_sponsor_reports.py --report-type=sponsors --months=3 --email
```

### User Reports

For user reports, one uses:

```bash
$ python monthly_sponsor_reports.py --report-type=users --months=1
```

And then:

```
$ python monthly_sponsor_reports.py --report-type=users --months=1 --email
```

## Dry Run

- hard code the date range at the top  

- comment out the assert statement which checks for 1st or 15th of month  

- run it  

- then remove brakefile, uncomment assert and comment date range

## Definitions

A 2-hour job (wall-clock time) that allocates 4 CPU-cores consumes 8 CPU-hours. Similarly, a 2-hour job that allocates 4 GPUs consumes 8 GPU-hours. If a group is ranked 5 of 20 then it used the fifth most CPU-hours or GPU-hours of the 20 groups.

`CPU-eff` is the CPU efficiency. It is computed as the ratio of the sums of the CPU time used by all of the CPU-cores and the CPU time allocated, which is the product of the elapsed time of the job and the number of CPU-cores, over all jobs. A 4 CPU-core job that runs for 100 minutes where each CPU-core uses the CPU for 90 minutes has an efficiency of 90%.

## Cron

These reports run under cron on tigergpu:

```
[jdh4@tigergpu ~]$ crontab -l
55 8 1 * * /usr/licensed/anaconda3/2021.11/bin/python -uB /home/jdh4/bin/monthly_sponsor_reports/monthly_sponsor_reports.py --report-type=sponsors --months=3 --email > /home/jdh4/bin/monthly_sponsor_reports/output.log 2>&1
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
     della                     malik 
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

## Be Aware

- A sponsor will only receive a report if one of their users ran at least one job in the reporting period.  
- If the sponsor is not found for a given user on a given cluster then that record is omitted. These events can be seen in the output and should be addressed. 
- The script must be executed on a machine that can talk to ldap1.rc.princeton.edu.  
- The script is written to only send emails once on the 1st of the month and then not again for at least 27 days (see `.brakefile` in "sanity checks and safeguards" in Python script).  
- Rankings on Stellar are over all groups, i.e., the PU/PPPL and CIMES portions are not separated.
