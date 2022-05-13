import os
import subprocess
import pandas as pd

def sponsor_per_cluster(netid, verbose=True):
  """Sponsor found for all large clusters even if user does not have an account on a specific cluster."""
  cmd = f"ldapsearch -x -H ldap://ldap1.rc.princeton.edu -b dc=rc,dc=princeton,dc=edu uid={netid} displayname manager description"
  output = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, timeout=5, text=True, check=True)
  lines = output.stdout.split('\n')
  if lines != [] and lines[-1] == "": lines = lines[:-1]
  
  # get primary manager (if more than 1 then take first)
  line_index = 0
  displayname = None
  managers = []
  for i, line in enumerate(lines):
    if "displayname: " in line:
      displayname = line.split(": ")[1].strip()
    if "manager: " in line and "uid=" in line:
      managers.append(line.split("uid=")[1].split(",")[0])
      line_index = i
  if managers == []:
    primary = None
    # try looking in CSV file if available
    fname = "users_left_university_from_robert_knight.csv"
    if os.path.exists(fname):
      rk = pd.read_csv(fname)
      if not rk[rk.Netid_ == netid].empty:
        primary = rk[rk.Netid_ == netid].Sponsor_Netid_.values[0]
        if not displayname: displayname = rk[rk.Netid_ == netid].Name_.values[0]
        if verbose: print(f"W: Primary sponsor for {netid} taken from CSV file.")
    if not primary and verbose: print(f"W: No primary sponsor found for {netid} in CSES LDAP or CSV file.")
  elif len(managers) > 1:
    if verbose: print(f"W: User {netid} has multiple primary sponsors: {','.join(managers)}. Using {managers[0]}.")
    primary = managers[0]
  else:
    primary = managers[0]
  if not displayname and verbose: print(f"W: Name not found for user {netid} in CSES LDAP.")

  # get all cluster-specific sponsors and name of user
  sponsor = {"della":primary, "stellar":primary, "tiger":primary, "tigressdata":primary, "traverse":primary, "displayname":displayname}
  s = ""
  for line in lines[line_index:]:
    s += line.strip() if not line.startswith("#") else ""
  for cluster in sponsor.keys():
    x = f"{cluster}:"
    if x in s:
      sponsor_netid = s.split(x)[1].split("=")[0]
      if sponsor_netid == "USER":
        if verbose: print(f"W: Sponsor entry of {sponsor_netid} found for {netid} on {cluster}. Corrected to {netid}.")
        sponsor_netid = netid
      if "(" in sponsor_netid:
        tmp = sponsor_netid.split("(")[0].strip()
        if verbose: print(f"W: Sponsor entry of {sponsor_netid} found for {netid} on {cluster}. Corrected to {tmp}.")
        sponsor_netid = tmp
      sponsor[cluster] = sponsor_netid

  return sponsor


def sponsor_full_name(netid, verbose=True):
  """Return the full name of the sponsor for the given netid of the sponsor."""
  cmd = f"ldapsearch -x -H ldap://ldap1.rc.princeton.edu -b dc=rc,dc=princeton,dc=edu uid={netid} displayname"
  output = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, timeout=5, text=True, check=True)
  lines = output.stdout.split('\n')
  if lines != [] and lines[-1] == "": lines = lines[:-1]

  displayname = None
  for line in lines:
    if "displayname: " in line:
      displayname = line.split(": ")[1].strip()
  
  if not displayname and verbose: print(f"W: Name not found in CSES LDAP for sponsor {netid}.")
  return displayname
