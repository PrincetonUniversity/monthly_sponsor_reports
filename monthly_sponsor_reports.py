import os
import sys
import time
import math
import argparse
import subprocess
import textwrap
import calendar
from datetime import date
from datetime import datetime
from random import random
import numpy as np
import pandas as pd

from sponsor import sponsor_full_name
from sponsor import sponsor_per_cluster
from sponsor import get_full_name_of_user

from efficiency import get_stats_dict  # wget https://raw.githubusercontent.com/jdh4/job_defense_shield/main/efficiency.py
from efficiency import cpu_efficiency
from efficiency import gpu_efficiency
from efficiency import cpu_memory_usage

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# if a sponsor or user unsubscribes then search this code for unsubscribed_sponsors
# or unsubscribed_users and add their netid to the list

# enter the gpu partitions here
GPU_CLUSTER_PARTITIONS = ["della__cryoem(gpu)", "della__gpu", "della__gpu-ee(gpu)", "stellar__gpu",
                          "tiger__cryoem(gpu)", "tiger__gpu", "tiger__motion", "traverse__all(gpu)", "della__gputest"]

# conversion factors
SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 3600
HOURS_PER_DAY = 24
BASEPATH = os.getcwd()
#BASEPATH = "/home/jdh4/bin/monthly_sponsor_reports"

def get_date_range(today, N, report_type="sponsors"):
  #return date(2022, 10, 15), date(2022, 11, 14)
  #return date(2022, 9, 1), date(2022, 11, 30)
  # argparse restricts values of N
  def subtract_months(mydate, M):
    year, month = mydate.year, mydate.month
    month -= M
    if month <= 0:
      year  -= 1
      month += 12
    return year, month
  if report_type == "sponsors":
    year, month = subtract_months(today, N)
    start_date = date(year, month, 1)
    year, month = subtract_months(today, 1)
    _, last_day_of_month = calendar.monthrange(year, month)
    end_date = date(year, month, last_day_of_month)
  elif report_type == "users":
    year, month = subtract_months(today, N)
    start_date = date(year, month, 15)  
    end_date = date(today.year, today.month, 14)
  else:
    sys.exit("Error: get_date_range(): report_type does not match choices.")
  return start_date, end_date

def send_email(s, addressee, start_date, end_date, sender="cses@princeton.edu"):
  msg = MIMEMultipart('alternative')
  msg['Subject'] = f"Slurm Accounting Report ({start_date.strftime('%b %-d, %Y')} - {end_date.strftime('%b %-d, %Y')})"
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
  fname = f"{BASEPATH}/cache_sacct_{start_date.strftime('%Y%m%d')}.csv"
  if use_cache and os.path.exists(fname):
    print("Reading cache file ... ", end="", flush=True)
    rw = pd.read_csv(fname, low_memory=False)
    print("done.", flush=True)
  else:
    cmd = f"sacct {flags} -S {start_date.strftime('%Y-%m-%d')}T00:00:00 -E {end_date.strftime('%Y-%m-%d')}T23:59:59 -o {fields}"
    if use_cache: print("Calling sacct (which may require several seconds) ... ", end="", flush=True)
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

def add_proportion_in_parenthesis(dframe, column_name, replace=False):
  if dframe.shape[0] == 1: return dframe
  total = dframe[column_name].sum()
  if total == 0: return dframe
  dframe["proportion"] = 100 * dframe[column_name] / total
  dframe["proportion"] = dframe["proportion"].apply(round)
  name = column_name if replace else f"{column_name}-cmb"
  dframe[name] = dframe.apply(lambda row: f"{round(row[column_name])} ({row['proportion']}%)", axis='columns')
  dframe = dframe.drop(columns=["proportion"])
  # align cpu-hours by adding spaces before proportion
  max_chars = max([item.index(")") - item.index("(") for item in dframe[name]])
  def add_spaces(item):
    num_spaces_to_add = max_chars - (item.index(")") - item.index("("))
    return item[:item.index("(")] + " " * num_spaces_to_add + item[item.index("("):]
  dframe[name] = dframe[name].apply(add_spaces)
  return dframe

def delineate_partitions(cluster, gpu_job, partition):
  if cluster == "della"   and partition == "cryoem" and gpu_job:
    return f"{partition}(gpu)"
  elif cluster == "della" and partition == "cryoem" and not gpu_job:
    return f"{partition}(cpu)"
  elif cluster == "tiger" and partition == "cryoem" and gpu_job:
    return f"{partition}(gpu)"
  elif cluster == "tiger" and partition == "cryoem" and not gpu_job:
    return f"{partition}(cpu)"
  elif cluster == "della" and partition == "gpu-ee" and gpu_job:
    return f"{partition}(gpu)"
  elif cluster == "della" and partition == "gpu-ee" and not gpu_job:
    return f"{partition}(cpu)"
  elif cluster == "traverse" and gpu_job:
    return "all(gpu)"
  elif cluster == "traverse" and not gpu_job:
    return "all(cpu)"
  else:
    return partition

def add_new_and_derived_fields(df):
  df["gpus"] = df.alloctres.apply(gpus_per_job)
  df["gpu-seconds"] = df.apply(lambda row: row["elapsedraw"] * row["gpus"], axis='columns')
  df["gpu-job"] = df.alloctres.apply(is_gpu_job)
  df["cpu-only-seconds"] = df.apply(lambda row: 0 if row["gpu-job"] else row["cpu-seconds"], axis="columns")
  df["elapsed-hours"] = df.elapsedraw.apply(lambda x: round(x / SECONDS_PER_HOUR, 1))
  df["start-date"] = df.start.apply(lambda x: datetime.fromtimestamp(int(x)).strftime("%a %-m/%-d"))
  df["cpu-hours"] = df["cpu-seconds"] / SECONDS_PER_HOUR
  df["gpu-hours"] = df["gpu-seconds"] / SECONDS_PER_HOUR
  df["admincomment"] = df["admincomment"].apply(get_stats_dict)
  # clean partitions for traverse and cryoem (della and tiger)
  df.partition = df.apply(lambda row: delineate_partitions(row["cluster"], row["gpu-job"], row["partition"]), axis="columns")
  df["cluster-partition"] = df.apply(lambda row: f"{row['cluster']}__{row['partition']}", axis="columns")
  return df

def uniq_series(series):
  return ",".join(sorted(set(series)))

def shorten(name):
  if len(name) > 14:
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

def groupby_cluster_partition_netid_and_get_sponsor(df, user_sponsor):
  d = {"cpu-hours":np.sum, "gpu-hours":np.sum, "netid":np.size, "partition":min, "account":uniq_series}
  dg = df.groupby(by=["cluster-partition", "netid"]).agg(d).rename(columns={"netid":"jobs"}).reset_index()
  dg["cluster"] = dg["cluster-partition"].apply(lambda x: x.split("__")[0])
  dg = dg.merge(user_sponsor, on="netid", how="left")
  dg["sponsor"] = dg.apply(lambda row: row["sponsor-dict"][row["cluster"]], axis='columns')
  dg["name"] = dg["sponsor-dict"].apply(lambda x: x["displayname"]).apply(format_user_name)
  dg = dg.sort_values(["cluster", "sponsor", "cpu-hours"], ascending=[True, True, False])
  dg["cpu-hours"] = dg["cpu-hours"].apply(round).astype("int64")
  dg["gpu-hours"] = dg["gpu-hours"].apply(round).astype("int64")
  return dg

def compute_cpu_and_gpu_efficiencies(df, clusparts):
  # Here we create a new dataframe containing the efficiencies.
  # This is good to do in isolation since we may need to filter out some jobs and
  # a lot can go wrong in general when computing these quantities.
  # Idea is to join this with the main dataframe and run fillna in
  # case the jobs of a user got filtered out when computing efficiencies.

  eff = pd.DataFrame()
  for cluspart in clusparts:
    ce = df[(df["cluster-partition"] == cluspart) & \
            (df["elapsedraw"] >= 0.1 * SECONDS_PER_HOUR) & \
            (df.admincomment != {})].copy()
    if ce.empty: continue  # prevents next line from failing
    ce[f"cpu-tuples"] = ce.apply(lambda row: cpu_efficiency(row["admincomment"], row["elapsedraw"], row["jobid"], row["cluster"]), axis="columns")
    ce[f"cpu-seconds-used"]  = ce[f"cpu-tuples"].apply(lambda x: x[0])
    ce[f"cpu-seconds-total"] = ce[f"cpu-tuples"].apply(lambda x: x[1])
    if cluspart in GPU_CLUSTER_PARTITIONS:
      ce[f"gpu-tuples"] = ce.apply(lambda row: gpu_efficiency(row["admincomment"], row["elapsedraw"], row["jobid"], row["cluster"]), axis="columns")
      ce[f"gpu-seconds-used"]  = ce[f"gpu-tuples"].apply(lambda x: x[0])
      ce[f"gpu-seconds-total"] = ce[f"gpu-tuples"].apply(lambda x: x[1])
    before = ce.shape[0]
    ce = ce[ce[f"cpu-seconds-used"] <= ce[f"cpu-seconds-total"]]  # dropping bad data
    if (before - ce.shape[0] > 0): print(f"W: Dropped {before - ce.shape[0]} rows of {before} on {cluspart} while computing CPU efficiencies")
    if cluspart in GPU_CLUSTER_PARTITIONS:
      before = ce.shape[0]
      ce = ce[ce[f"gpu-seconds-used"] <= ce[f"gpu-seconds-total"]]
      if (before - ce.shape[0] > 0): print(f"W: Dropped {before - ce.shape[0]} rows of {before} on {cluspart} while computing GPU efficiencies")
    if cluspart not in GPU_CLUSTER_PARTITIONS:
      d = {"netid":np.size, "cpu-seconds-used":np.sum, "cpu-seconds-total":np.sum}
      ce = ce.groupby("netid").agg(d).rename(columns={"netid":"jobs"}).reset_index(drop=False)
      ce["CPU-eff"] = ce.apply(lambda row: f'{round(100.0 * row["cpu-seconds-used"] / row["cpu-seconds-total"])}%' if row["cpu-seconds-total"] != 0 else "--", axis="columns")
      ce["GPU-eff"] = "N/A"
    else:
      d = {"netid":np.size, "cpu-seconds-used":np.sum, "cpu-seconds-total":np.sum, "gpu-seconds-used":np.sum, "gpu-seconds-total":np.sum}
      ce = ce.groupby("netid").agg(d).rename(columns={"netid":"jobs"}).reset_index(drop=False)
      ce["CPU-eff"] = ce.apply(lambda row: f'{round(100.0 * row["cpu-seconds-used"] / row["cpu-seconds-total"])}%' if row["cpu-seconds-total"] != 0 else "--", axis="columns")
      ce["GPU-eff"] = ce.apply(lambda row: f'{round(100.0 * row["gpu-seconds-used"] / row["gpu-seconds-total"])}%' if row["gpu-seconds-total"] != 0 else "--", axis="columns")
    ce["cluster-partition"] = cluspart
    ce = ce[["netid", "CPU-eff", "GPU-eff", "cluster-partition"]]
    eff = pd.concat([eff, ce])
  return eff

def check_for_nulls(dg):
  if not dg[pd.isna(dg["sponsor"])].empty:
    print("\nSponsor not found for the following users (these rows will be dropped):")
    print(dg[pd.isna(dg["sponsor"])][["netid", "name", "cluster-partition", "account", "sponsor"]])
  if not dg[pd.isna(dg["name"])].empty:
    print("\nName is missing for the following users (email reports will a show blank name):")
    print(dg[pd.isna(dg["name"])][["netid", "name", "cluster-partition", "account"]])
  return None

def add_cpu_and_gpu_rankings(dg, x):
  def cpu_ranking(cluspart, user, cpuhours):
    cpu_hours_rank = pd.Index(x[x["cluster-partition"] == cluspart].sort_values(by="cpu-hours", ascending=False).netid).get_loc(user) + 1
    total_users    = x[x["cluster-partition"] == cluspart]["netid"].unique().size
    return f"{total_users}/{total_users}" if cpuhours == 0 else f"{cpu_hours_rank}/{total_users}"
  def gpu_ranking(cluspart, user, gpuhours):
    if cluspart not in GPU_CLUSTER_PARTITIONS: return "N/A"
    gpu_hours_rank = pd.Index(x[x["cluster-partition"] == cluspart].sort_values(by="gpu-hours", ascending=False).netid).get_loc(user) + 1
    total_users    = x[x["cluster-partition"] == cluspart]["netid"].unique().size
    return f"{total_users}/{total_users}" if gpuhours == 0 else f"{gpu_hours_rank}/{total_users}"
  dg["CPU-rank"] = dg.apply(lambda row: cpu_ranking(row["cluster-partition"], row["netid"], row["cpu-hours"]), axis="columns")
  dg["GPU-rank"] = dg.apply(lambda row: gpu_ranking(row["cluster-partition"], row["netid"], row["gpu-hours"]), axis="columns")
  return dg

def collapse_by_sponsor(dg):
  d = {"cpu-hours":np.sum, "gpu-hours":np.sum, "jobs":np.sum, "partition":uniq_series, "account":uniq_series, "name":min, "sponsor":min}
  sg = dg.groupby(by=["cluster", "netid"]).agg(d).reset_index()
  sg = sg.sort_values(["cluster", "sponsor", "cpu-hours"], ascending=[True, True, False])
  return sg

def add_heading(df_str, cluster):
  rows = df_str.split("\n")
  width = max([len(row) for row in rows])
  padding = " " * max(1, math.ceil((width - len(cluster)) / 2))
  divider = padding + cluster[0].upper() + cluster[1:] + padding
  rows.insert(0, divider)
  rows.insert(1, "-" * len(divider))
  rows.insert(3, "-" * len(divider))
  return "\n".join(rows)

def format_percent(x):
  if x >= 10:
    return round(x)
  elif x >= 1:
    return round(x, 1)
  else:
    return '%s' % float('%.1g' % x)

def special_requests(sponsor, cluster, cl, start_date, end_date):
  if sponsor == "jtromp" and cluster == "traverse":
    days = (end_date - start_date).days
    gpu_hours_available = 0.07 * 46 * 4 * days * HOURS_PER_DAY
    gpu_hours_used = cl["gpu-hours"].sum()
    return f"\n\nPercent usage of Tromp GPU nodes is {round(100 * gpu_hours_used / gpu_hours_available, 1)}%."
  else:
    return ""

def create_user_report(name, netid, start_date, end_date, body):
  if netid == "cpena":   name = "Catherine J. Pena"
  if netid == "javalos": name = "Jose L. Avalos"
  if netid == "alemay":  name = "Amelie Lemay"
  if netid == "alvaros": name = "Alvaro Luna"

  report  = f"\n  User: {name}\n"
  report += f"Period: {start_date.strftime('%b %-d, %Y')} - {end_date.strftime('%b %-d, %Y')}\n"
  report += "\n"
  opening = (
            'You are receiving this report because you ran at least one job on the Research Computing systems '
            'in the past month. The table below shows your cluster usage:'
            )
  report += "\n".join(textwrap.wrap(opening, width=80))
  report += "\n\n"
  report += body
  defs = (
         'Definitions: A 2-hour job (wall-clock time) that allocates 4 CPU-cores '
         'consumes 8 CPU-hours. Similarly, a 2-hour job that allocates 4 GPUs '
         'consumes 8 GPU-hours. CPU-eff is the CPU efficiency. GPU-eff is the '
         'GPU efficiency which is equivalent to the GPU utilization (as obtained by the "nvidia-smi" command). '
         'If your rank is 5/20 then you used the fifth most CPU-hours (or GPU-hours) of the 20 users. The Sponsor column indicates the NetID of your '
         'cluster sponsor. If the cluster sponsor is incorrect then please notify CSES by replying to this email. '
  )
  report += "\n".join(textwrap.wrap(defs, width=80))
  report += "\n"
  report += textwrap.dedent(f"""
            Please use the resources efficiently. You can see job efficiency data by
            running the "jobstats" command on a given JobID, for example:

              $ jobstats 1234567

            Follow the link at the bottom of the "jobstats" output for more detailed
            information.

            To see your job history over the last 30 days, run this command:

              $ shistory -d 30

            Add the following lines to your Slurm scripts to receive an email report with
            efficiency information after each job finishes:

              #SBATCH --mail-type=begin
              #SBATCH --mail-type=end
              #SBATCH --mail-user={netid}@princeton.edu
  """)
  report += textwrap.dedent(f"""
            Want to improve your computational skills? Register for PICSciE/RC workshops:

              https://researchcomputing.princeton.edu/workshops
  """)
  report += "\n"
  reply = (
  'Replying to this email will open a ticket with CSES. Please reply '
  'with questions, changes to your sponsorship or to unsubscribe from these reports.'
  )
  report += "\n".join(textwrap.wrap(reply, width=80))
  return report

def create_report(name, sponsor, start_date, end_date, body):
  if sponsor == "cpena":   name = "Catherine J. Pena"
  if sponsor == "javalos": name = "Jose L. Avalos"
  report  = f"\nSponsor: {name} ({sponsor})\n"
  report += f" Period: {start_date.strftime('%b %-d, %Y')} - {end_date.strftime('%b %-d, %Y')}\n"
  opening = """
  You are receiving this report because you sponsor researchers on the
  Research Computing systems. The report below shows the researchers
  that you sponsor as well as their cluster usage. Only researchers
  that ran at least one job during the reporting period appear in the
  tables below. There are no financial costs for using the systems.
  """
  report += textwrap.dedent(opening)
  report += "\n"
  report += body
  footer = (
  "Definitions: A 2-hour job (wall-clock time) that allocates 4 CPU-cores "
  "consumes 8 CPU-hours. Similarly, a 2-hour job that allocates 4 GPUs "
  "consumes 8 GPU-hours. "
  "A CPU-rank (or GPU-rank) of 5/20 means that the user consumed "
  "the 5th most CPU-hours (or GPU-hours) of the 20 users on the partition. "
  "CPU-eff is the CPU efficiency. GPU-eff is the "
  'GPU efficiency which is equivalent to the GPU utilization (as obtained by the "nvidia-smi" command).'
  )
  report += "\n"
  report += "\n".join(textwrap.wrap(footer, width=75))
  report += "\n\n"
  footer = (
  "Replying to this email will open a ticket with CSES. Please reply "
  "with questions/comments or to unsubscribe from these reports."
  )
  report += "\n".join(textwrap.wrap(footer, width=75))
  return report


if __name__ == "__main__":

  parser = argparse.ArgumentParser(description='Monthly Sponsor and User Reports')
  #parser.add_argument('--start', type=str, default="", metavar='S',
  #                    help='Start date with format YYYY-MM-DD')
  #parser.add_argument('--end', type=str, default="", metavar='E',
  #                    help='End date with format YYYY-MM-DD')
  parser.add_argument('--report-type', required=True, type=str, choices=["sponsors", "users"],
                      help='Specify the report type')
  parser.add_argument('--months', required=True, type=int, metavar='N', choices=range(1, 8),
                      help='Reporting period covers N months')
  parser.add_argument('--email', action='store_true', default=False,
                      help='Send reports via email')

  args = parser.parse_args()
  #start_date = datetime.strptime(args.start, '%Y-%m-%d')
  #end_date   = datetime.strptime(args.end,   '%Y-%m-%d')
  start_date, end_date = get_date_range(date.today(), args.months, report_type=args.report_type)
  print(f"\nReporting period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

  # pandas display settings
  pd.set_option("display.max_rows", None)
  pd.set_option("display.max_columns", None)
  pd.set_option("display.width", 1000)

  # convert Slurm timestamps to seconds
  os.environ["SLURM_TIME_FORMAT"] = "%s"

  flags = "-L -a -X -P -n"
  fields = "jobid,user,cluster,account,partition,cputimeraw,elapsedraw,timelimitraw,nnodes,ncpus,alloctres,submit,eligible,start,admincomment"
  renamings = {"user":"netid", "cputimeraw":"cpu-seconds", "nnodes":"nodes", "ncpus":"cores", "timelimitraw":"limit-minutes"}
  numeric_fields = ["cpu-seconds", "elapsedraw", "limit-minutes", "nodes", "cores", "submit", "eligible"]
  df = raw_dataframe_from_sacct(flags, start_date, end_date, fields, renamings, numeric_fields, use_cache=True)

  # filter pending jobs and clean
  df = df[pd.notnull(df.alloctres) & (df.alloctres != "")]

  before = df.shape[0]
  df = df[df.start.str.isnumeric()]  # added on 6/30/2022 for jobs 8201421 (tiger), 40265195 (della)
  if (df.shape[0] != before): print(f"\nW: {before - df.shape[0]} rows dropped because start was not numeric\n")
  df.start = df.start.astype("int64")

  df.cluster   =   df.cluster.str.replace("tiger2", "tiger")
  df.partition = df.partition.str.replace("datascience", "datasci")
  df.partition = df.partition.str.replace("cpu,physics", "physics")  # due to bill changing partitions of pending jobs
  df.partition = df.partition.str.replace("physics,cpu", "cpu")      # due to bill changing partitions of pending jobs

  if not args.email: print("Adding new and derived fields (which may require several seconds) ... ", end="", flush=True)
  df = add_new_and_derived_fields(df)
  if not args.email: print("done.", flush=True)

  # partitions
  print("\n")
  print("== Raw pairs ==")
  print(df[["cluster", "partition"]].drop_duplicates().sort_values(["cluster", "partition"]).to_string(index=False))
  print("\n")
  print("Be sure to explicitly specify the GPU cluster-partitions in GPU_CLUSTER_PARTITIONS.")
  print("GPU cluster-partitions are denoted by ***GPU*** below:")
  clusparts = np.sort(df["cluster-partition"].unique())
  for cluspart in clusparts:
    stars = "***GPU***" if cluspart in GPU_CLUSTER_PARTITIONS else 9 * " "
    print(f"{stars} {cluspart}")
  print("\n\n")

  # get sponsor info for each unique netid (this minimizes ldap calls)
  user_sponsor = df[["netid"]].drop_duplicates().sort_values("netid").copy()
  if not args.email: print("Getting sponsor for each user (which may require several seconds) ... ", end="\n", flush=True)
  user_sponsor["sponsor-dict"] = user_sponsor.netid.apply(lambda netid: sponsor_per_cluster(netid, verbose=True))

  # perform a two-column groupby (cluster-partition and netid) and then join users to their sponsors
  dg = groupby_cluster_partition_netid_and_get_sponsor(df, user_sponsor)
  _ = check_for_nulls(dg)

  # compute cpu and gpu efficiencies when possible
  user_eff = compute_cpu_and_gpu_efficiencies(df, clusparts)

  dg = pd.merge(dg, user_eff, how="left", on=["cluster-partition", "netid"])
  dg[["CPU-eff", "GPU-eff"]] = dg[["CPU-eff", "GPU-eff"]].fillna("--")
  dg = add_cpu_and_gpu_rankings(dg, dg.copy())

  # sanity checks and safeguards
  brakefile = f"{BASEPATH}/.brakefile"
  if args.email:
    d = {"della":"curt", "stellar":"curt", "tiger":"curt", "traverse":"curt", "displayname":"Garrett Wright"}
    assert sponsor_per_cluster(netid="gbwright") == d, "RC ldap may be down"
    assert dg.shape[0] > 100, "Not enough records in dg dataframe"
    # script can only run once on the 1st or 15th of the month
    if os.path.exists(brakefile):
      seconds_since_emails_last_sent = datetime.now().timestamp() - os.path.getmtime(brakefile)
      assert seconds_since_emails_last_sent > 7 * HOURS_PER_DAY * SECONDS_PER_HOUR, "Emails sent within last 7 days"
  with open(brakefile, "w") as f:
    f.write("")

  # write dataframe to file for archiving
  cols = ["cluster", "sponsor", "netid", "name", "cpu-hours", "CPU-eff", "CPU-rank", "gpu-hours", "GPU-eff", "GPU-rank", \
          "jobs", "account", "partition", "cluster-partition"]
  fname = f"{BASEPATH}/cluster_sponsor_user_{start_date.strftime('%-d%b%Y')}_{end_date.strftime('%-d%b%Y')}.csv"
  dg[cols].to_csv(fname, index=False)

  # create reports (each sponsor is guaranteed to have at least one user by construction above)
  cols1 = ["cluster", "partition", "cluster-partition", "cpu-hours", "CPU-rank", "CPU-eff", \
           "gpu-hours", "GPU-rank", "GPU-eff", "jobs", "account", "sponsor"]
  cols2 = ["netid", "name", "cpu-hours", "gpu-hours", "jobs", "account", "partition"]
  cols3 = ["cluster", "netid", "partition", "cpu-hours", "CPU-rank", "CPU-eff", "gpu-hours", \
           "GPU-rank", "GPU-eff", "jobs", "cluster-partition"]
  renamings = {"netid":"NetID", "name":"Name", "cpu-hours":"CPU-hours", "gpu-hours":"GPU-hours", \
               "jobs":"Jobs", "account":"Account", "partition":"Partition", "cluster":"Cluster", \
               "sponsor":"Sponsor"}
  # next two lines throw away null sponsors and null users (which are revealed above in output)
  sponsors = dg[pd.notnull(dg.sponsor)].sponsor.sort_values().unique()
  users    = dg[pd.notnull(dg.netid)].netid.sort_values().unique()
  print(f"Total sponsors: {sponsors.size}")
  print(f"Total users:    {users.size}")
  print(f"Total jobs:     {df.shape[0]}")

  if args.report_type == "users":
    assert datetime.now().strftime("%-d") == "15", "Script will only run on 15th of the month"
    # remove unsubscribed users and those that left the university
    unsubscribed_users = ["bfaber", "ceerc", "sting", "ib4025", "mchitoto", "musslick"]
    users = set(users) - set(unsubscribed_users)
    for user in sorted(users):
      sp = dg[dg.netid == user]
      rows = pd.DataFrame()
      if args.email: print(f"User: {user}")
      for cluspart in clusparts:
        cl = sp[sp["cluster-partition"] == cluspart].copy()
        if not cl.empty:
          rows = rows.append(cl[cols1].rename(columns=renamings))
      rows["GPU-hours"] = rows.apply(lambda row: row["GPU-hours"] if row["cluster-partition"] in GPU_CLUSTER_PARTITIONS else "N/A", axis="columns")
      rows["GPU-eff"]   = rows.apply(lambda row: row["GPU-eff"]   if row["cluster-partition"] in GPU_CLUSTER_PARTITIONS else "N/A", axis="columns")
      if (rows[rows["cluster-partition"].isin(GPU_CLUSTER_PARTITIONS)].shape[0] == 0):
        rows.drop(columns=["GPU-hours", "GPU-rank", "GPU-eff"], inplace=True)
      rows.drop(columns=["cluster-partition"], inplace=True)
      body = "\n".join([2 * " " + row for row in rows.to_string(index=False, justify="center").split("\n")])
      body += "\n\n"
      report = create_user_report(get_full_name_of_user(user), user, start_date, end_date, body)
      print(report)
      if args.email: send_email(report, f"{user}@princeton.edu", start_date, end_date)
      if args.email and random() < 0.01: send_email(report, "halverson@princeton.edu", start_date, end_date)
  elif args.report_type == "sponsors":
    assert datetime.now().strftime("%-d") == "1", "Script will only run on 1st of the month"
    ov = collapse_by_sponsor(dg)
    # remove unsubscribed sponsors and those that left the university
    unsubscribed_sponsors = ["aturing", "mzaletel"]
    sponsors = set(sponsors) - set(unsubscribed_sponsors)
    for sponsor in sorted(sponsors):
      sp = ov[ov.sponsor == sponsor]
      body = ""
      if args.email: print(f"Sponsor: {sponsor}")
      for cluster in ("della", "stellar", "tiger", "traverse"):
        cl = sp[sp.cluster == cluster]
        if not cl.empty:
          # determine where a group ranks relative to other groups
          cpu_hours_by_sponsor = cl["cpu-hours"].sum()
          gpu_hours_by_sponsor = cl["gpu-hours"].sum()
          cpu_hours_total = ov[ov.cluster == cluster]["cpu-hours"].sum()
          gpu_hours_total = ov[ov.cluster == cluster]["gpu-hours"].sum()
          cpu_hours_pct = 0 if cpu_hours_total == 0 else format_percent(100 * cpu_hours_by_sponsor / cpu_hours_total)
          gpu_hours_pct = 0 if gpu_hours_total == 0 else format_percent(100 * gpu_hours_by_sponsor / gpu_hours_total)
          cpu_hours_rank  = ov[ov.cluster == cluster].groupby("sponsor").agg({"cpu-hours":np.sum}).sort_values(by="cpu-hours", ascending=False).index.get_loc(sponsor) + 1
          gpu_hours_rank  = ov[ov.cluster == cluster].groupby("sponsor").agg({"gpu-hours":np.sum}).sort_values(by="gpu-hours", ascending=False).index.get_loc(sponsor) + 1
          total_sponsors  = ov[ov.cluster == cluster]["sponsor"].unique().size
          if cpu_hours_by_sponsor == 0:
            cpu_hours_rank = total_sponsors
          if gpu_hours_by_sponsor == 0:
            gpu_hours_rank = total_sponsors

          cl = add_proportion_in_parenthesis(cl.copy(), "cpu-hours", replace=True)
          body += "\n"
          body += add_heading(cl[cols2].rename(columns=renamings).to_string(index=False, justify="center"), cluster)
          body += special_requests(sponsor, cluster, cl, start_date, end_date)
          body += f"\n\nYour group used {cpu_hours_by_sponsor} CPU-hours or {cpu_hours_pct}% of the {cpu_hours_total} total CPU-hours"
          body += f"\non {cluster[0].upper() + cluster[1:]}. Your group is ranked {cpu_hours_rank} of {total_sponsors} by CPU-hours used."
          if gpu_hours_by_sponsor != 0:
            body +=  " Similarly,"
            body += f"\nyour group used {gpu_hours_by_sponsor} GPU-hours or {gpu_hours_pct}% of the {gpu_hours_total} total GPU-hours"
            body += f"\nwhich yields a ranking of {gpu_hours_rank} of {total_sponsors} by GPU-hours used."
          body += "\n\n"

      # details dataframe
      details = dg[dg.sponsor == sponsor][cols3].rename(columns=renamings).sort_values(["Cluster", "NetID", "Partition"])
      details["GPU-hours"] = details.apply(lambda row: row["GPU-hours"] if row["cluster-partition"] in GPU_CLUSTER_PARTITIONS else "N/A", axis="columns")
      details["GPU-rank"]  = details.apply(lambda row: row["GPU-rank"]  if row["cluster-partition"] in GPU_CLUSTER_PARTITIONS else "N/A", axis="columns")
      details["GPU-eff"]   = details.apply(lambda row: row["GPU-eff"]   if row["cluster-partition"] in GPU_CLUSTER_PARTITIONS else "N/A", axis="columns")
      if (details[details["cluster-partition"].isin(GPU_CLUSTER_PARTITIONS)].shape[0] == 0):
        details.drop(columns=["GPU-hours", "GPU-rank", "GPU-eff"], inplace=True)
      details.drop(columns=["cluster-partition"], inplace=True)
      details["Cluster"] = details["Cluster"].apply(lambda x: f"{x[0].upper()}{x[1:]}")
      df_str = details.to_string(index=False, justify="center")
      max_width = max(map(len, df_str.split("\n")))
      heading = "Detailed Breakdown"
      padding = " " * max(1, math.ceil((max_width - len(heading)) / 2))
      body += "\n"
      body += padding + heading + padding + "\n"
      body += "-" * max_width + "\n"
      for i, line in enumerate(df_str.split("\n")):
        body += line + "\n"
        if i == 0: body += "-" * max_width + "\n"
      body += "\n"

      # create report
      name = sponsor_full_name(sponsor, verbose=True)
      report = create_report(name, sponsor, start_date, end_date, body)
      print(report)

      if args.email:
        send_email(report, f"{sponsor}@princeton.edu", start_date, end_date)
        if sponsor == "macohen": send_email(report, "bdorland@pppl.gov", start_date, end_date)
        if sponsor == "macohen": send_email(report, "pbisbal@pppl.gov"  , start_date, end_date)
        if random() < 0.025: send_email(report, "halverson@princeton.edu", start_date, end_date)
  else:
    sys.exit("Error: report_type does not match choices.")
