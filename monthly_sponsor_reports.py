#!/usr/licensed/anaconda3/2021.5/bin/python

import argparse
import os
import time
import subprocess
from datetime import datetime
from datetime import timedelta
import numpy as np
import pandas as pd

from sponsor import sponsor_full_name
from sponsor import sponsor_per_cluster

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# conversion factors
SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 3600
HOURS_PER_DAY = 24

def send_email(s, addressee, start, end, sender="cses@princeton.edu"):
  msg = MIMEMultipart('alternative')
  msg['Subject'] = f"Slurm Accounting Report ({start.strftime('%-m/%-d/%Y')} - {end.strftime('%-m/%-d/%Y')})"
  msg['From'] = sender
  msg['To'] = addressee
  text = "None"
  html = f'<html><head></head><body><font face="Courier New, Courier, monospace"><pre>{s}</pre></font></body></html>'
  part1 = MIMEText(text, 'plain'); msg.attach(part1) 
  part2 = MIMEText(html, 'html');  msg.attach(part2)
  s = smtplib.SMTP('localhost')
  s.sendmail(sender, addressee, msg.as_string())
  s.quit()
  return None

def raw_dataframe_from_sacct(flags, start_date, end_date, fields, renamings=[], numeric_fields=[], use_cache=False):
  fname = f"cache_sacct_{start_date.strftime('%Y%m%d')}.csv"
  if use_cache and os.path.exists(fname):
    print("\nUsing cache file.\n", flush=True)
    rw = pd.read_csv(fname, low_memory=False)
  else:
    cmd = f"sacct {flags} -S {start_date.strftime('%Y-%m-%d')}T00:00:00 -E {end_date.strftime('%Y-%m-%d')}T23:59:59 -o {fields}"
    if use_cache: print("\nCalling sacct (which may require several seconds) ... ", end="", flush=True)
    output = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, timeout=300, text=True, check=True)
    if use_cache: print("done.", flush=True)
    lines = output.stdout.split('\n')
    if lines != [] and lines[-1] == "": lines = lines[:-1]
    rw = pd.DataFrame([line.split("|") for line in lines])
    rw.columns = fields.split(",")
    rw.rename(columns=renamings, inplace=True)
    rw[numeric_fields] = rw[numeric_fields].apply(pd.to_numeric)
    if use_cache: rw.to_csv(fname, index=False)
  return rw

def gpus_per_job(tres):
  # billing=8,cpu=4,mem=16G,node=1
  # billing=112,cpu=112,gres/gpu=16,mem=33600M,node=4
  if "gres/gpu=" in tres:
    for part in tres.split(","):
      if "gres/gpu=" in part:
        gpus = int(part.split("=")[-1])
        assert gpus > 0
    return gpus
  else:
    return 0

def is_gpu_job(tres):
  return 1 if "gres/gpu=" in tres and not "gres/gpu=0" in tres else 0

def add_new_and_derived_fields(df):
  # new and derived fields
  df["gpus"] = df.alloctres.apply(gpus_per_job)
  df["gpu-seconds"] = df.apply(lambda row: row["elapsedraw"] * row["gpus"], axis='columns')
  df["gpu-job"] = df.alloctres.apply(is_gpu_job)
  df["cpu-only-seconds"] = df.apply(lambda row: 0 if row["gpus"] else row["cpu-seconds"], axis="columns")
  df["elapsed-hours"] = df.elapsedraw.apply(lambda x: round(x / SECONDS_PER_HOUR, 1))
  df["start-date"] = df.start.apply(lambda x: x if x == "Unknown" else datetime.fromtimestamp(int(x)).strftime("%a %-m/%-d"))
  df["cpu-hours"] = df["cpu-seconds"] / SECONDS_PER_HOUR
  df["gpu-hours"] = df["gpu-seconds"] / SECONDS_PER_HOUR
  return df

def uniq_series(series):
  return ",".join(sorted(set(series)))

def shorten(name):
  if len(name) > 18:
    first, last = name.split()
    return f"{first[0]}. {last}"
  else:
    return name

def format_user_name(s):
  if not s: return s
  names = list(filter(lambda x: x not in ['Jr.', 'II', 'III', 'IV'], s.split()))
  if len(names) == 1:
    return names[0]
  else:
    return shorten(f"{names[0]} {names[-1]}")

def add_heading(x, c):
  rows = x.split("\n")
  max_chars = max([len(row) for row in rows])
  ct = round((max_chars - len(c) - 2) / 2)
  divider = " " * ct + " " + c[0].upper() + c[1:] + " " + " " * ct
  rows.insert(0, divider)
  rows.insert(1, "-" * len(divider))
  rows.insert(3, "-" * len(divider))
  return "\n".join(rows)

def special_requests(sponsor, cluster, cl, start_date, end_date):
  if sponsor == "jtromp" and cluster == "traverse":
    days = (end_date - start_date).days
    gpu_hours_available = 0.07 * 46 * 4 * days * HOURS_PER_DAY
    gpu_hours_used = cl["gpu-hours"].sum()
    return f"\n\nPercent usage of Tromp GPU nodes is {round(100 * gpu_hours_used / gpu_hours_available, 1)}%."
  else:
    return ""


if __name__ == "__main__":

  parser = argparse.ArgumentParser(description='Monthly sponsor reports')
  parser.add_argument('--days', type=int, default=30, metavar='N',
                      help='Create report over N previous days from now (default: 14)')
  parser.add_argument('--start', type=str, default="", metavar='S',
                      help='Start date with format YYYY-MM-DD')
  parser.add_argument('--end', type=str, default="", metavar='E',
                      help='End date with format YYYY-MM-DD')
  parser.add_argument('--email', action='store_true', default=False,
                      help='Send reports via email')
  args = parser.parse_args()

  start_date = datetime.strptime(args.start, '%Y-%m-%d')
  end_date   = datetime.strptime(args.end,   '%Y-%m-%d')
  #start_date = datetime.now() - timedelta(days=args.days)

  # pandas display settings
  pd.set_option("display.max_rows", None)
  pd.set_option("display.max_columns", None)
  pd.set_option("display.width", 1000)

  # convert Slurm timestamps to seconds
  os.environ["SLURM_TIME_FORMAT"] = "%s"

  flags = "-L -a -X -P -n"
  fields = "jobid,user,cluster,account,partition,cputimeraw,elapsedraw,timelimitraw,nnodes,ncpus,alloctres,submit,eligible,start"
  renamings = {"user":"netid", "cputimeraw":"cpu-seconds", "nnodes":"nodes", "ncpus":"cores", "timelimitraw":"limit-minutes"}
  numeric_fields = ["cpu-seconds", "elapsedraw", "limit-minutes", "nodes", "cores", "submit", "eligible"]
  df = raw_dataframe_from_sacct(flags, start_date, end_date, fields, renamings, numeric_fields, use_cache=True)

  # filter pending jobs and clean
  df = df[pd.notnull(df.alloctres) & (df.alloctres != "")]
  df.start = df.start.astype("int64")
  df.cluster   =   df.cluster.str.replace("tiger2", "tiger")
  df.partition = df.partition.str.replace("datascience", "datasci")
  df.partition = df.partition.str.replace("physics", "phys")
  df = add_new_and_derived_fields(df)
 
  # get sponsor info for each unique netid (this minimizes ldap calls)
  user_sponsor = df[["netid"]].drop_duplicates().sort_values("netid").copy()
  user_sponsor["sponsor-dict"] = user_sponsor.netid.apply(lambda n: sponsor_per_cluster(n, verbose=True))

  # create the main dataframe
  d = {"cpu-hours":np.sum, "gpu-hours":np.sum, "netid":np.size, "partition":uniq_series, "account":uniq_series}
  dg = df.groupby(by=["cluster", "netid"]).agg(d).rename(columns={"netid":"jobs"}).reset_index()
  dg = dg.merge(user_sponsor, on="netid", how="left")
  dg["sponsor"] = dg.apply(lambda row: row["sponsor-dict"][row["cluster"]], axis='columns')
  dg["name"] = dg["sponsor-dict"].apply(lambda x: x["displayname"]).apply(format_user_name)
  dg = dg.sort_values(["cluster", "sponsor", "cpu-hours"], ascending=[True, True, False])
  dg["cpu-hours"] = dg["cpu-hours"].apply(round).astype("int64")
  dg["gpu-hours"] = dg["gpu-hours"].apply(round).astype("int64")

  #dg.sponsor = dg.sponsor.str.replace("curt", "halverson")

  # check for null values
  if not dg[pd.isna(dg["sponsor"])].empty: print(dg[pd.isna(dg["sponsor"])])
  if not dg[pd.isna(dg["name"])].empty:    print(dg[pd.isna(dg["name"])])

  # write out dataframe
  cols = ["cluster", "sponsor", "netid", "name", "cpu-hours", "gpu-hours", "jobs", "account", "partition"]
  fname = f"cluster_sponsor_user_{datetime.now().strftime('%Y%m%d')}.csv"
  dg[cols].to_csv(fname, index=True)
  #import sys; sys.exit()

  # prepare reports per sponsor
  cols = ["netid", "name", "cpu-hours", "gpu-hours", "jobs", "account", "partition"]
  renamings = {"netid":"NetID", "name":"Name", "cpu-hours":"CPU-hours", "gpu-hours":"GPU-hours", \
               "jobs":"Jobs", "account":"Account", "partition":"Partition(s)"}
  clusters = ("della", "stellar", "tiger", "traverse")
  #sponsors = dg[pd.notnull(dg.sponsor)].sponsor.sort_values().unique()
  sponsors = ["jtromp", "vonholdt", "pdebene", "azp", "rcar", "mawebb", "muellerm", "gvecchi", "curt"]
  for sponsor in sponsors:
    s = ""
    name = sponsor_full_name(sponsor, verbose=True)
    sp = dg[dg.sponsor == sponsor]
    for cluster in clusters:
      cl = sp[sp.cluster == cluster]
      if not cl.empty:
        s += "\n"
        s += add_heading(cl[cols].rename(columns=renamings).to_string(index=False, justify="center"), cluster)
        s += special_requests(sponsor, cluster, cl, start_date, end_date)
        s += "\n\n"
    # each sponsor is gauranteed to have at least one user by construction
    S = "\n"
    S += f"Sponsor: {name} ({sponsor})\n"
    fmt = "%b %-d, %Y"
    S += f" Period: {start_date.strftime(fmt)} - {end_date.strftime(fmt)}\n\n"
    S += s
    S += "\nOnly users that ran at least one job during the reporting period appear in\nthe table(s) above. Replying to this email will open a ticket with CSES.\n"

    send_email(S, "halverson@princeton.edu", start_date, end_date) if args.email else print(S)
    #send_email(S, f"{sponsor}@princeton.edu", start_date, end_date) if args.email else print(S)
    #if sponsor == "macohen": send_email(S, "bdorland@pppl.gov", start_date, end_date) if args.email else print(S)
