# Monthly Sponsor Reports

The software in this repo generates monthly sponsor reports for the Research Computing clusters. Below is an example report:

```
Sponsor: Garegin Andrea (gandrea)
 Period: Nov 1, 2021 - Jan 31, 2022


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
on Tiger. Your group is ranked 5 of 101 by CPU-hours used. Similarly,
your group used 3250 GPU-hours or 0.5% of the 554101 total GPU-hours
yielding a ranking of 89 of 101 by GPU-hours used.


You are receiving this report because you sponsor researchers on the
Research Computing systems. The report above shows the researchers
that you sponsor as well as their cluster usage. Only researchers
that ran at least one job during the reporting period appear in the
table(s) above. There is no financial cost for using the systems.

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
$ python -m unittest tests/test_monthly_sponsor_reports.py -v
```

If all of the tests pass then do a dry run (which takes a few minutes):

```bash
$ python monthly_sponsor_reports.py --months=3
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
$ python monthly_sponsor_reports.py --months=3 --email
```

## Definitions

A job that runs on 4 CPU-cores for 2 hours consumes 8 CPU-hours. Similarly, a job that uses 2 GPUs for 4 hours consumes 8 GPU-hours.

## Cron

These reports run under cron on tigergpu:

```
[jdh4@tigergpu ~]$ crontab -l
56 8 1 * * /usr/licensed/anaconda3/2021.11/bin/python -u -B /home/jdh4/bin/monthly_sponsor_reports/monthly_sponsor_reports.py --months=3 --email > /home/jdh4/bin/monthly_sponsor_reports/output.log 2>&1
```

## Be Aware

- A sponsor will only receive a report if one of their users ran at least one job in the reporting period.  
- If the sponsor is not found for a given user on a given cluster then that record is omitted. These events can be seen in the output and should be addressed. 
- The script must be executed on a machine that can talk to ldap1.rc.princeton.edu.  
- The script is written to only send emails once on the 1st of the month and then not again for at least 27 days (see `.brakefile` in "sanity checks and safeguards" in Python script).
