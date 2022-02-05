# Monthly Sponsor Reports

One can generate a monthly report for each sponsor on the RC clusters:

```
$ ssh <YourNetID>@della.princeton.edu
$ git clone https://github.com/PrincetonUniversity/monthly_sponsor_reports.git
$ cd monthly_sponsor_reports
$ module load anaconda3/2021.11
$ python -m unittest monthly_sponsor_reports.py -v
```

If the tests all pass then do a dry run:

```
$ python monthly_sponsor_reports.py --start 2021-11-01 --end 2022-01-31
```

It is normal to see warnings like this:

```
...
W: Sponsor entry of stf (Stephan A. Fueglistaler) found for aadcroft on stellar. Corrected to stf.
W: Sponsor entry of USER found for ecmorale on della. Corrected to ecmorale.
W: Primary sponsor for fj4172 taken from CSV file.
W: User yixiaoc has multiple primary sponsors: rcar,weinan. Using rcar.
...
```

The output will be sent to stdout instead of email. If the output looks good then run once more with emails enabled:

```
$ python monthly_sponsor_reports.py --start 2021-11-01 --end 2022-01-31 --email
```
