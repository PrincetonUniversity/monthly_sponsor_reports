# Monthly Sponsor Reports

One can generate a monthly report for each sponsor on the RC clusters:

```
$ ssh <YourNetID>@della.princeton.edu
$ git clone https://github.PrincetonUniversity/monthly_sponsor_reports.git
$ module load anaonda3/2021.11
$ python monthly_sponsor_reports.py --start 2021-11-01 --end 2022-01-31 --email
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
