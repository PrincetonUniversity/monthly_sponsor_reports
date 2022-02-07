import sys
sys.path.append("../")
import unittest
import pandas as pd
from sponsor import sponsor_full_name
from sponsor import sponsor_per_cluster
import monthly_sponsor_reports as msr


class TestSponsorName(unittest.TestCase):

  def test_sponsor_name(self):
    assert "Pablo G. Debenedetti" == sponsor_full_name(netid="pdebene")

  def test_sponsor_name(self):
    assert None == sponsor_full_name(netid="bigfoot", verbose=False)


class TestSponsor(unittest.TestCase):

  def test_sponsor_per_cluster(self):
    d = {"della":"curt", "stellar":"curt", "tiger":"curt", "traverse":"curt", "displayname":"Jonathan D. Halverson"}
    assert sponsor_per_cluster(netid="jdh4") == d

    d = {"della":"curt", "stellar":"curt", "tiger":"wtang", "traverse":"curt", "displayname":"Garrett Wright"}
    assert sponsor_per_cluster(netid="gbwright") == d

    d = {"della":None, "stellar":None, "tiger":None, "traverse":None, "displayname":None}
    assert sponsor_per_cluster(netid="bigfoot", verbose=False) == d


class TestMonthlySponsorReports(unittest.TestCase):

  def test_gpus_per_job(self):
    assert msr.gpus_per_job("billing=8,cpu=4,mem=16G,node=1") == 0
    assert msr.gpus_per_job("billing=50,cpu=32,gres/gpu=1,mem=4000M,node=1") == 1
    assert msr.gpus_per_job("billing=112,cpu=112,gres/gpu=16,mem=33600M,node=4") == 16

  def test_add_new_and_derived_fields(self):
    jobs = [[ 100, 100, "billing=8,cpu=1,mem=16G,node=1", 0, 123456789, 0],
            [ 200, 100, "billing=8,cpu=2,gres/gpu=1,mem=16G,node=1", 1, 123456789, 100],
            [1600, 200, "billing=8,cpu=8,gres/gpu=4,mem=16G,node=1", 4, 123456789, 800]]
    df = pd.DataFrame(jobs)
    df.columns = ["cpu-seconds", "elapsedraw", "alloctres", "gpus", "start", "gpu-seconds"]
    result = msr.add_new_and_derived_fields(df)
    expected = pd.Series([0, 100 / 3600, 800 / 3600]).rename("gpu-hours")
    pd.testing.assert_series_equal(result["gpu-hours"], expected)

    expected = pd.Series([100, 0, 0]).rename("cpu-only-seconds")
    pd.testing.assert_series_equal(result["cpu-only-seconds"], expected)

  def test_double_groupby(self):
    # construct simulated sacct output
    jobs = [[ 100, "jdh4",     "tiger", "cses", "cpu", 25000, 50000, 100000, 1, 1, "billing=8,cpu=1,mem=16G,node=1",            123456789, 234567890, 234567890],
            [ 101, "jdh4",     "tiger", "cses", "cpu", 20000, 10000, 100000, 1, 2, "billing=8,cpu=2,mem=16G,node=1",            123456789, 234567890, 234567890],
            [ 102, "jdh4",     "tiger", "cses", "gpu", 20000, 10000, 100000, 1, 2, "billing=8,cpu=2,gres/gpu=1,mem=16G,node=1", 123456789, 234567890, 234567890],
            [ 103, "gbwright", "tiger", "cses", "gpu", 80000, 20000, 100000, 1, 4, "billing=8,cpu=4,gres/gpu=4,mem=16G,node=1", 123456789, 234567890, 234567890],
            [ 104, "bill",     "della", "cses", "cpu", 30000, 10000, 100000, 1, 3, "billing=8,cpu=3,mem=16G,node=1",            123456789, 234567890, 234567890]]
    df = pd.DataFrame(jobs)
    fields = "jobid,netid,cluster,account,partition,cpu-seconds,elapsedraw,limit-minutes,nodes,cores,alloctres,submit,eligible,start"
    df.columns = fields.split(",")
    df = msr.add_new_and_derived_fields(df)
    # construct the expected result
    expected = [["della", "curt", "bill", "William Wichser",     round(30000/3600),           0,                   1, "cses", "cpu"],
                ["tiger", "curt", "jdh4", "Jonathan Halverson",  round((25000+2*20000)/3600), round(10000/3600),   3, "cses", "cpu,gpu"],
                ["tiger", "wtang", "gbwright", "Garrett Wright", round(4*20000/3600),         round(4*20000/3600), 1, "cses", "gpu"]]
    expected = pd.DataFrame(expected)
    cols = ["cluster", "sponsor", "netid", "name", "cpu-hours", "gpu-hours", "jobs", "account", "partition"]
    expected.columns = cols
    expected["cpu-hours"] = expected["cpu-hours"].astype("int64")
    expected["gpu-hours"] = expected["gpu-hours"].astype("int64")
    # get the actual result
    d1 = {"della":"curt", "stellar":"curt", "tiger":"wtang", "traverse":"curt", "displayname":"Garrett Wright"}
    d2 = {"della":"curt", "stellar":"curt", "tiger":"curt", "traverse":"curt", "displayname":"Jonathan D. Halverson"}
    d3 = {"della":"curt", "stellar":"curt", "tiger":"curt", "traverse":"curt", "displayname":"William Wichser"}
    user_sponsor = [["gbwright", d1], ["jdh4", d2], ["bill", d3]]
    user_sponsor = pd.DataFrame(user_sponsor)
    user_sponsor.columns = ["netid", "sponsor-dict"]
    actual = msr.groupby_cluster_netid_and_get_sponsor(df, user_sponsor)[cols].reset_index(drop=True)
    # compare the two dataframes
    pd.testing.assert_frame_equal(actual, expected)
