import requests
import pandas as pd

# ls -ld *[[:upper:]]* | tr -s ' ' | awk -F" " '{print $3","$4","$9}' > ~/sponsor_fileset
# cborca is 155500
# ldapsearch -x -H ldap://ldap01.rc.princeton.edu -b dc=rc,dc=princeton,dc=edu uidNumber=155500
# not assigned IMAI
# filesets['jjara'] = ['JJARA']
# filesets['alexeys'] = ['FRNN']
# filesets['sjardin'] = ['M3DC1']
# filesets['131126'] = ['PHYSICS']
# filesets['pnisysad'] = ['PNI']
# filesets['116515'] = ['PPPL']

############################

params = {'query': "gpfs_quota_block_limit_hard_bytes{}"}
response = requests.get('http://vigilant2:8480/api/v1/query', params)
r = response.json()

clusters = []
filesetname = []
fs = []
uid = []

r = r["data"]["result"]
for res in r:
    if "cluster" in res["metric"]:
        clusters.append(res["metric"]["cluster"])
    if "filesetname" in res["metric"]:
        filesetname.append(res["metric"]["filesetname"])
    if "fs" in res["metric"]:
        fs.append(res["metric"]["fs"])
    if "uid" in res["metric"]:
        uid.append(res["metric"]["uid"])
    if "uid" in res["metric"] and res["metric"]["uid"] == "150340":
        print("==", res["metric"]["cluster"], res["metric"]["fs"],"==")
        print(res["metric"]["job"])
        print(res["metric"]["filesetname"])
        print(round(int(res["value"][1]) / 1024**3), "GB")

print(sorted(set(clusters)))
print(sorted(set(filesetname)))
print(sorted(set(fs)))
print(len(set(uid)))

#clusters ['TDE', 'della', 'stellar', 'stellar,traverse', 'tiger', 'tigress']
#fs ['cimes.storage', 'della.gpfs', 'projects.storage', 'projects2.storage', 'stellar.gpfs', 'tiger2.gpfs', 'tigress.storage'] 

# "quota_type": "GRP",  and no uid
# "quota_type": "USR",

params = {'query': "gpfs_quota_block_usage_bytes{}"}
response = requests.get('http://vigilant2:8480/api/v1/query', params)
r = response.json()

used = 0
used2 = 0

d = {}

r = r["data"]["result"]
for res in r:
    M = res["metric"]
    if "fs" in M:
        fs = M["fs"]
        fsn = M["filesetname"]
        key = f"{fs}.{fsn}"
        if "uid" in M:
            uid = M["uid"]
            if uid in d:
                assert key not in d[uid], f"{d[uid]}"
                d[uid][key] = int(res["value"][1])
            else:
                d[uid] = {key: int(res["value"][1])}
        elif M["quota_type"] in ("GRP", "FILESET"):
            uid = fsn
            if "gid" in M:
                gid = M["gid"]
                key = f"{key}.{gid}"
            if uid in d:
                assert key not in d[uid], f"{uid}, {key}, {d[uid]}"
                d[uid][key] = int(res["value"][1])
            else:
                d[uid] = {key: int(res["value"][1])}
        else:
            print("Neither: ", M)
    else:
        print("No fs:", M)

    if "fs" in res["metric"] and res["metric"]["fs"] == "cimes.storage":
        used += int(res["value"][1])
    if "filesetname" in res["metric"] and res["metric"]["filesetname"] == "GREGOR":
        used2 += int(res["value"][1])
        #print(res["metric"]["fs"], "==**==")
    if "uid" in res["metric"] and res["metric"]["uid"] == "150340":
        print("==", res["metric"]["cluster"], res["metric"]["fs"],"==")
        print(res["metric"]["job"])
        print(res["metric"]["filesetname"])
        print(round(int(res["value"][1]) / 1024**3), "GB")

#import sys; sys.exit()
print(used / 1024**5)
print(used)
print(used2 / 1024**5)
print(used2)

for stor, val in d["150340"].items():
    print(stor, round(val / 1024**3))

#print(d)

vals = []
vals = [item for x in d.values() for item in x.keys()]
#print(sorted(set(vals)))
#print(set(d.keys()) - set(trans.keys()))

uid_user = pd.read_csv("master.uids",
                       names=["uid", "username"],
                       dtype={"uid": str, "username": str})

uid_user.dropna(inplace=True)
uid_user = uid_user[uid_user.uid != "0"]
print(uid_user.info())
print(uid_user)

df = pd.DataFrame(d)
df = df.T
df.reset_index(inplace=True, names="uid")
df = df.merge(uid_user, how="left", on="uid")
print(df.head())
print(df[df.username == "jdh4"].dropna(axis=1).squeeze().to_dict())
print(df[["uid", "username", "projects2.storage.WEBB"]].dropna(subset="projects2.storage.WEBB"))

"""
'data': {'resultType': 'vector', 'result': [{'metric': {'__name__': 'gpfs_quota_block_limit_hard_bytes', 'cluster': 'TDE', 'def_quota': 'off', 'fid': '0', 'filesetname': 'root', 'fs': 'projects2.storage', 'gid': '0', 'instance': 'tigerdata-gateway:9001', 'job': 'tigerdata ESS GPFS', 'quota': 'on', 'quota_type': 'GRP', 'remarks': 'default off', 'service': 'gpfs'}, 'value': [1727323287.354, '0']}, {'metric': {'__name__': 'gpfs_quota_block_limit_hard_bytes', 'cluster': 'TDE', 'def_quota': 'off', 'fid': '1', 'filesetname': 'AFMtest', 'fs': 'projects2.storage', 'gid': '0', 'instance': 'tigerdata-gateway:9001', 'job': 'tigerdata ESS GPFS', 'quota': 'on', 'quota_type': 'GRP', 'remarks': 'i', 'service': 'gpfs'}, 'value': [1727323287.354, '0']},
"""
