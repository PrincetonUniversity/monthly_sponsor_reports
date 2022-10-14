import sys
sys.path.append("../")
import unittest
import pandas as pd
from datetime import date
from sponsor import sponsor_full_name
from sponsor import sponsor_per_cluster
import monthly_sponsor_reports as msr


class TestDateRange(unittest.TestCase):

  def test_range(self):
    assert msr.get_date_range(date(2022,  5,  1), 1, report_type="sponsors") == (date(2022,  4,  1), date(2022,  4, 30))
    assert msr.get_date_range(date(2022,  5,  1), 3, report_type="sponsors") == (date(2022,  2,  1), date(2022,  4, 30))
    assert msr.get_date_range(date(2022,  5,  1), 7, report_type="sponsors") == (date(2021, 10,  1), date(2022,  4, 30))
    assert msr.get_date_range(date(2022,  1,  1), 3, report_type="sponsors") == (date(2021, 10,  1), date(2021, 12, 31))
    assert msr.get_date_range(date(2022,  1,  1), 1, report_type="sponsors") == (date(2021, 12,  1), date(2021, 12, 31))
    assert msr.get_date_range(date(2022, 11,  1), 5, report_type="sponsors") == (date(2022,  6,  1), date(2022, 10, 31))
    assert msr.get_date_range(date(2024,  3,  1), 5, report_type="sponsors") == (date(2023, 10,  1), date(2024,  2, 29))
    assert msr.get_date_range(date(2022,  5,  1), 1, report_type="sponsors") == (date(2022,  4,  1), date(2022,  4, 30))
    assert msr.get_date_range(date(2022,  5, 15), 1, report_type="users")    == (date(2022,  4, 15), date(2022,  5, 14))
    assert msr.get_date_range(date(2022,  1, 15), 1, report_type="users")    == (date(2021, 12, 15), date(2022,  1, 14))
    assert msr.get_date_range(date(2022, 12, 15), 1, report_type="users")    == (date(2022, 11, 15), date(2022, 12, 14))


class TestDelineatePartitions(unittest.TestCase):

  def test_delineate_partitions(self):
    assert "all(cpu)" == msr.delineate_partitions("traverse", 0, "all")
    assert "all(gpu)" == msr.delineate_partitions("traverse", 1, "all")
    assert "gpu" == msr.delineate_partitions("della", 1, "gpu")
    assert "cpu" == msr.delineate_partitions("tiger", 0, "cpu")
    assert "all" == msr.delineate_partitions("stellar", 0, "all")


class TestSponsorName(unittest.TestCase):

  def test_sponsor_name(self):
    assert "Pablo G. Debenedetti" == sponsor_full_name(netid="pdebene")

  def test_sponsor_name(self):
    assert None == sponsor_full_name(netid="bigfoot", verbose=False)


class TestSponsor(unittest.TestCase):

  def test_sponsor_per_cluster(self):
    d = {"della":"curt", "stellar":"curt", "tiger":"curt", "traverse":"curt", "displayname":"Jonathan D. Halverson"}
    assert sponsor_per_cluster(netid="jdh4") == d

    d = {"della":"curt", "stellar":"curt", "tiger":"curt", "traverse":"curt", "displayname":"Garrett Wright"}
    assert sponsor_per_cluster(netid="gbwright") == d

    d = {"della":None, "stellar":None, "tiger":None, "traverse":None, "displayname":None}
    assert sponsor_per_cluster(netid="bigfoot", verbose=False) == d


class TestMonthlySponsorReports(unittest.TestCase):

  def test_gpus_per_job(self):
    assert msr.gpus_per_job("billing=8,cpu=4,mem=16G,node=1") == 0
    assert msr.gpus_per_job("billing=50,cpu=32,gres/gpu=1,mem=4000M,node=1") == 1
    assert msr.gpus_per_job("billing=112,cpu=112,gres/gpu=16,mem=33600M,node=4") == 16

  def test_add_new_and_derived_fields(self):
    jobs = [["della", "cpu",  100, 100, "billing=8,cpu=1,mem=16G,node=1", 0, 123456789, 0, "JS1:None"],
            ["della", "gpu",  200, 100, "billing=8,cpu=2,gres/gpu=1,mem=16G,node=1", 1, 123456789, 100, "JS1:None"],
            ["della", "cpu", 1600, 200, "billing=8,cpu=8,gres/gpu=4,mem=16G,node=1", 4, 123456789, 800, "JS1:None"]]
    df = pd.DataFrame(jobs)
    df.columns = ["cluster", "partition", "cpu-seconds", "elapsedraw", "alloctres", "gpus", "start", "gpu-seconds", "admincomment"]
    result = msr.add_new_and_derived_fields(df)
    expected = pd.Series([0, 100 / 3600, 800 / 3600]).rename("gpu-hours")
    pd.testing.assert_series_equal(result["gpu-hours"], expected)

    expected = pd.Series([100, 0, 0]).rename("cpu-only-seconds")
    pd.testing.assert_series_equal(result["cpu-only-seconds"], expected)

  def test_double_groupby(self):
    # construct simulated sacct output
    jobs = [[100, "jdh4",     "tiger", "cses", "cpu", 50000, 50000, 100000, 1, 1, "billing=8,cpu=1,mem=16G,node=1",            123456789, 234567890, 234567890, "JS1:None"],
            [101, "jdh4",     "tiger", "cses", "cpu", 20000, 10000, 100000, 1, 2, "billing=8,cpu=2,mem=16G,node=1",            123456789, 234567890, 234567890, "JS1:None"],
            [102, "jdh4",     "tiger", "cses", "gpu", 20000, 10000, 100000, 1, 2, "billing=8,cpu=2,gres/gpu=1,mem=16G,node=1", 123456789, 234567890, 234567890, "JS1:None"],
            [103, "gbwright", "tiger", "cses", "gpu", 80000, 20000, 100000, 1, 4, "billing=8,cpu=4,gres/gpu=4,mem=16G,node=1", 123456789, 234567890, 234567890, "JS1:None"],
            [104, "bill",     "della", "cses", "cpu", 30000, 10000, 100000, 1, 3, "billing=8,cpu=3,mem=16G,node=1",            123456789, 234567890, 234567890, "JS1:None"]]
    df = pd.DataFrame(jobs)
    fields = "jobid,netid,cluster,account,partition,cpu-seconds,elapsedraw,limit-minutes,nodes,cores,alloctres,submit,eligible,start,admincomment"
    df.columns = fields.split(",")
    df = msr.add_new_and_derived_fields(df)
    d1 = {"della":"curt", "stellar":"curt", "tiger":"wtang", "traverse":"curt", "displayname":"Garrett Wright"}
    d2 = {"della":"curt", "stellar":"curt", "tiger":"curt",  "traverse":"curt", "displayname":"Jonathan D. Halverson"}
    d3 = {"della":"curt", "stellar":"curt", "tiger":"curt",  "traverse":"curt", "displayname":"William Wichser"}
    user_sponsor = [["gbwright", d1], ["jdh4", d2], ["bill", d3]]
    user_sponsor = pd.DataFrame(user_sponsor)
    user_sponsor.columns = ["netid", "sponsor-dict"]
    cols = ["cluster-partition", "netid", "cpu-hours", "gpu-hours", "jobs", "partition", "account", "cluster", "sponsor", "name"]
    actual = msr.groupby_cluster_partition_netid_and_get_sponsor(df, user_sponsor)[cols].reset_index(drop=True)

    # construct the expected result
    expected = [["della__cpu", "bill",     round(30000/3600),                 0, 1, "cpu", "cses", "della", "curt", "W. Wichser"     ],
                ["tiger__cpu", "jdh4",     round(70000/3600),                 0, 2, "cpu", "cses", "tiger", "curt", "J. Halverson"   ],
                ["tiger__gpu", "jdh4",     round(20000/3600), round(10000/3600), 1, "gpu", "cses", "tiger", "curt", "J. Halverson"   ],
                ["tiger__gpu", "gbwright", round(80000/3600), round(80000/3600), 1, "gpu", "cses", "tiger", "wtang", "Garrett Wright"]]
    expected = pd.DataFrame(expected)
    expected.columns = cols
    expected["cpu-hours"] = expected["cpu-hours"].astype("int64")
    expected["gpu-hours"] = expected["gpu-hours"].astype("int64")

    # compare the two dataframes
    pd.testing.assert_frame_equal(actual, expected)

  def test_rankings(self):
    jobs = [["della__gpu", "jdh8",   100,  700],
            ["della__gpu", "jdh6",   200,  500],
            ["della__gpu", "jdh5",   400,  200],
            ["della__gpu", "jdh7",     0,  100],
            ["della__gpu", "jdh4", 16000,  900]]
    df = pd.DataFrame(jobs)
    fields = "cluster-partition,netid,cpu-hours,gpu-hours"
    df.columns = fields.split(",")

    result = msr.add_cpu_and_gpu_rankings(df, df.copy())
    result = result.sort_values("netid").reset_index(drop=True)
    expected = pd.Series(["1/5", "2/5", "3/5", "5/5", "4/5"]).rename("CPU-rank")
    pd.testing.assert_series_equal(result["CPU-rank"], expected)
    expected = pd.Series(["1/5", "4/5", "3/5", "5/5", "2/5"]).rename("GPU-rank")
    pd.testing.assert_series_equal(result["GPU-rank"], expected)
