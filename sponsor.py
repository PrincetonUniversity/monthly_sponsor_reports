import os
import subprocess
import pandas as pd
import base64
import unicodedata
import re


def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')


def get_sponsor_netid_per_cluster_dict_from_ldap(netid, verbose=True, strip=False):
  """Returns a dictionary of sponsor netids for a given user netid for the large clusters."""
  ldap = "ldap://ldap01.rc.princeton.edu"
  cmd = f"ldapsearch -x -H {ldap} -b dc=rc,dc=princeton,dc=edu uid={netid} displayname manager description"
  output = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, timeout=5, text=True, check=True)
  lines = output.stdout.split('\n')
  if lines != [] and lines[-1] == "": lines = lines[:-1]
  
  # get primary manager (if more than 1 then take first)
  line_index = 0
  displayname = None
  managers = []
  for i, line in enumerate(lines):
    if "displayname:: " in line:
      rawname = line.split(":: ")[1].strip()
      displayname = base64.b64decode(rawname).decode("utf-8")
      displayname = strip_accents(displayname) if strip else displayname
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
    if verbose:
      print(f"W: User {netid} has multiple primary sponsors: {','.join(managers)}. Using {managers[0]}.")
    primary = managers[0]
  else:
    primary = managers[0]
  if not displayname and verbose: print(f"W: Name not found for user {netid} in CSES LDAP.")

  # get all cluster-specific sponsors and name of user
  sponsor = {"della":primary,
             "stellar":primary,
             "tiger":primary,
             "tigressdata":primary,
             "traverse":primary,
             "displayname":displayname}
  s = ""
  for line in lines[line_index:]:
    s += line.strip() if not line.startswith("#") else ""
  for cluster in sponsor.keys():
    x = f"{cluster}:"
    if x in s:
      sponsor_netid = s.split(x)[1].split("=")[0]
      if sponsor_netid == "USER":
        if verbose:
          print(f"W: Sponsor entry of {sponsor_netid} found for {netid} on {cluster}. Corrected to {netid}.")
        sponsor_netid = netid
      if "(" in sponsor_netid:
        tmp = sponsor_netid.split("(")[0].strip()
        if verbose:
          print(f"W: Sponsor entry of {sponsor_netid} found for {netid} on {cluster}. Corrected to {tmp}.")
        sponsor_netid = tmp
      sponsor[cluster] = sponsor_netid
  return sponsor


def get_full_name_from_ldap(netid, use_rc=False, include_netid=False, verbose=True, strip=True):
  """Return the full name for the given netid by using either rc or university ldap."""
  if use_rc:
    ldap = "ldap://ldap01.rc.princeton.edu"
    cmd = f"ldapsearch -x -H {ldap} -b dc=rc,dc=princeton,dc=edu uid={netid} displayname"
  else:
    cmd = f"ldapsearch -x uid={netid} displayname"
  output = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, timeout=5, text=True, check=True)
  lines = output.stdout.split('\n')
  displayname = None
  for line in lines:
    if "displayname:: " in line:
      rawname = line.split(":: ")[1].strip()
      displayname = base64.b64decode(rawname).decode("utf-8")
      displayname = strip_accents(displayname) if strip else displayname
      return f"{displayname} ({netid})" if include_netid else displayname
    if "displayname: " in line:
      displayname = line.split(": ")[1].strip()
      return f"{displayname} ({netid})" if include_netid else displayname
  if displayname is None and verbose:
    print(f"W: Name not found in LDAP for {netid} with use_rc={use_rc}.")
  return displayname


def get_full_name_of_user_from_log(netid, flnm="tigress_user_changes.log"):
    """Return the full name of the user from the log file. Loop over all
       lines (i.e., do not return on first match)."""
    with open(flnm, "r", encoding="utf-8") as f:
        lines = f.readlines()
    pattern = f" {netid} "
    logname = None
    for line in lines:
        if pattern in line:
            if f" Added user {netid} (" in line:
                logname = line.split(f" Added user {netid} (")[-1].split(")")[0].split(" - ")[-1]
            if f" Removed user {netid} (" in line:
                logname = line.split(f" Removed user {netid} (")[-1].split(")")[0]
    return logname


def get_sponsor_netid_of_user_from_log(netid, flnm="tigress_user_changes.log"):
    """Return the sponsor netid for a given user netid from the log file.
       Loop over all lines (i.e., do not return on first match)."""
    with open(flnm, "r", encoding="utf-8") as f:
        lines = f.readlines()
    pattern = f" {netid} "
    sponsor = None
    for line in lines:
        if pattern in line:
            if f" Added user {netid} " in line and " with sponsor " in line:
                sponsor = line.split(" with sponsor ")[-1].split()[0]
            if f" Removed user {netid} " in line and " sponsor " in line:
                sponsor = line.split(" sponsor ")[-1].split(";")[0]
    return sponsor


def build_uid_username_dictionaries(uids: set[str], flnm="tigress_user_changes.log"):
    """Return a uid-to-username and username-to-uid dictionary for a given
       set of uids. Each uid is stored as a string."""
    uid2user = {"0": "root"}
    user2uid = {"root": "0"}
    if os.path.isfile(flnm):
        with open(flnm, "r", encoding="utf-8") as f:
            lines = f.readlines()
        pattern = r"Added user \w+ \(\d+ -"
        for line in lines:
            match = re.findall(pattern, line)
            if match:
                uid = match[0].split("(")[1].split()[0]
                netid = match[0].split("(")[0].strip().split()[-1]
                assert uid.isnumeric(), f"{uid} is not numeric: {line}"
                uid2user[uid] = netid
                user2uid[netid] = uid
    else:
        print(f"{flnm} was not found.")
    for uid in uids:
        if uid not in uid2user:
            found = False
            ldap = "ldap://ldap01.rc.princeton.edu"
            cmd = f"ldapsearch -x -H {ldap} -b dc=rc,dc=princeton,dc=edu uidNumber={uid} uid"
            output = subprocess.run(cmd,
                                    stdout=subprocess.PIPE,
                                    shell=True,
                                    timeout=5,
                                    text=True,
                                    check=True)
            lines = output.stdout.split('\n')
            for line in lines:
                if line.startswith("uid: "):
                    netid = line.split()[1]
                    uid2user[uid] = netid
                    user2uid[netid] = uid
                    found = True
                    break
            if not found:
                print(f"A netid for uid {uid} was not found.")
    return uid2user, user2uid
