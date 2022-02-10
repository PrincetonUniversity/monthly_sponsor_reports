# Monthly Sponsor Reports

The software in this repo generates monthly sponsor reports for the large Research Computing clusters. Below is an example report:

```
Sponsor: Alan M. Turing (aturing)
 Period: Nov 1, 2021 - Jan 31, 2022

                                   Della                                   
-------------------------------------------------------------------------------
 NetID           Name           CPU-hours  GPU-hours  Jobs Account Partition(s)
-------------------------------------------------------------------------------
rfeynman      Richard Feynman       7233        0     3465   phys           cpu 
 fitzger  F. Scott Fitzgerald       4793        0       63    lit   cpu,datasci  
   tmorr        Toni Morrison        182        6      255    lit       cpu,gpu 

                                   Tiger                                   
-------------------------------------------------------------------------------
 NetID           Name           CPU-hours  GPU-hours  Jobs Account Partition(s)
-------------------------------------------------------------------------------
  conway     John Conway           99219        0      152   math    serial,cpu 
  plewis     Peter Lewis            8071     2042    11092    pol           gpu 

Only users that ran at least one job during the reporting period appear in
the table(s) above. Replying to this email will open a ticket with CSES.
```

## Usage

Obtain the code:

```
$ ssh <YourNetID>@della.princeton.edu
$ git clone https://github.com/PrincetonUniversity/monthly_sponsor_reports.git
$ cd monthly_sponsor_reports
```

Run the unit tests:

```
$ module load anaconda3/2021.11
$ python -m unittest tests/test_monthly_sponsor_reports.py -v
```

If all of the tests pass then do a dry run (which takes a few minutes):

```
$ python monthly_sponsor_reports.py --start 2021-11-01 --end 2022-01-31
```

It is normal to see warnings like the following during the dry run:

```
...
W: Sponsor entry of stf (Stephan A. Fueglistaler) found for aadcroft on stellar. Corrected to stf.
W: Sponsor entry of USER found for ecmorale on della. Corrected to ecmorale.
W: Primary sponsor for fj4172 taken from CSV file.
W: User yixiaoc has multiple primary sponsors: rcar,weinan. Using rcar.
...
```

The output will be sent to stdout instead of email for the dry run. If the output looks good then run once more with emails enabled:

```
$ python monthly_sponsor_reports.py --start 2021-11-01 --end 2022-01-31 --email
```

## Cron

These reports run under cron on tigergpu:

```
[jdh4@tigergpu ~]$ crontab -l
```

## Be Aware

- A sponsor will only receive a report if one of their users ran at least one job in the reporting period.  
- If the sponsor is not found for a given user on a given cluster then that record is omitted. These events can be seen in the output and should be addressed.  
- The script must be run on a machine that can talk to ldap1.rc.princeton.edu.  
