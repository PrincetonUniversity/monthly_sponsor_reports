import os
import subprocess
import pandas as pd
import base64
import unicodedata

def sponsor_per_cluster(netid, verbose=True):
  """Sponsor found for all large clusters even if user does not have an account on a specific cluster."""
  cmd = f"ldapsearch -x -H ldap://ldap01.rc.princeton.edu -b dc=rc,dc=princeton,dc=edu uid={netid} displayname manager description"
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


def strip_accents(s):
  return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')


def sponsor_full_name(netid, verbose=True, strip_accents=True):
  """Return the full name of the sponsor for the given netid of the sponsor."""
  cmd = f"ldapsearch -x -H ldap://ldap01.rc.princeton.edu -b dc=rc,dc=princeton,dc=edu uid={netid} displayname"
  output = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, timeout=5, text=True, check=True)
  lines = output.stdout.split('\n')
  displayname = None
  for line in lines:
    if "displayname:: " in line:
      rawname = line.split(":: ")[1].strip()
      displayname = base64.b64decode(rawname).decode("utf-8")
      return strip_accents(displayname) if strip_accents else displayname
    if "displayname: " in line:
      displayname = line.split(": ")[1].strip()
      return displayname
  if displayname is None and verbose: print(f"W: Name not found in CSES LDAP for sponsor {netid}.")
  return displayname


def get_full_name_of_user(netid, strip_accents=True):
  """Return the full name of the user by calling ldapsearch."""
  cmd = f"ldapsearch -x uid={netid} displayname"
  output = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, timeout=5, text=True, check=True)
  lines = output.stdout.split('\n')
  for line in lines:
    if "displayname:: " in line:
      raw_name = line.split(":: ")[1].strip()
      full_name = base64.b64decode(raw_name).decode("utf-8")
      if strip_accents: full_name = strip_accents(full_name)
      return f"{full_name} ({netid})"
    if line.startswith("displayname:"):
      full_name = line.replace("displayname:", "").strip()
      # consider removing next line since b64 is now handled properly
      if full_name.replace(".", "").replace(",", "").replace(" ", "").replace("-", "").isalpha():
        return f"{full_name} ({netid})"
  return netid


def get_name_of_user_from_log(netid):
  """Return the full name of the user from the log file."""
  with open("tigress_user_changes.log", "r") as f:
    data = f.readlines()
  pattern = f" {netid} "
  logname = None
  for line in data:
    if pattern in line:
      if f" Added user {netid} (" in line:
        logname = line.split(f" Added user {netid} (")[-1].split(")")[0].split(" - ")[-1]
      if f" Removed user {netid} (" in line:
        logname = line.split(f" Removed user {netid} (")[-1].split(")")[0]
  return logname


def get_sponsor_netid_of_user_from_log(netid):
  """Return the sponsor netid for a given user netid from the log file."""
  with open("tigress_user_changes.log", "r") as f:
    data = f.readlines()
  pattern = f" {netid} "
  sponsor = None
  for line in data:
    if pattern in line:
      if f" Added user {netid} " in line and " with sponsor " in line:
        sponsor = line.split(" with sponsor ")[-1].split()[0]
      if f" Removed user {netid} " in line and " sponsor " in line:
        sponsor = line.split(" sponsor ")[-1].split(";")[0]
  return sponsor
