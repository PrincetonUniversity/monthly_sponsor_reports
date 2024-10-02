import requests
import pandas as pd


class DataStorage:
   
    PROM_SERVER = "http://vigilant2:8480"

    def __init__(self, field):
        self.field = field
        self.params = {"query": f"{self.field}" + "{}"}
        self.response = requests.get(f"{self.PROM_SERVER}/api/v1/query", self.params).json()
        self._build_dict()

    def _build_dict(self):
        self.d = {}
        r = self.response["data"]["result"]
        for res in r:
            M = res["metric"]
            if "fs" in M:
                fs = M["fs"]
                fsn = M["filesetname"]
                key = f"{fs}.{fsn}"
                if "uid" in M:
                    uid = M["uid"]
                    if uid in self.d:
                        assert key not in self.d[uid], f"{self.d[uid]}"
                        self.d[uid][key] = int(res["value"][1])
                    else:
                        self.d[uid] = {key: int(res["value"][1])}
                elif M["quota_type"] in ("GRP", "FILESET"):
                    uid = fsn
                    if "gid" in M:
                        gid = M["gid"]
                        key = f"{key}.{gid}"
                    if uid in self.d:
                        assert key not in self.d[uid], f"{uid}, {key}, {d[uid]}"
                        self.d[uid][key] = int(res["value"][1])
                    else:
                        self.d[uid] = {key: int(res["value"][1])}
                else:
                    print("Neither: ", M)
            else:
                print("No fs:", M)

if __name__ == "__main__":
    ds = DataStorage("gpfs_quota_block_usage_bytes")
    uids = ds.d.keys()

    uid_user = pd.read_csv("master.uids",
                       names=["uid", "username"],
                       dtype={"uid": str, "username": str})
    uid_user.dropna(inplace=True)
    uid_user = uid_user[uid_user.uid != "0"]
    udict = dict(zip(uid_user.uid.values, uid_user.username.values))
    udict["WEBB"] = "mawebb"

    for uid in uids:
        if uid not in uid_user.uid.values:
            pass
            #print(uid)

    for uid, pay in ds.d.items():
        if "projects2.storage.WEBB" in pay:
            if uid in udict:
                print(uid, udict[uid])
            else:
                print(uid, "***")
