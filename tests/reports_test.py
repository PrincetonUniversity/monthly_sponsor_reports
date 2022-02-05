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

  #def test_double_groupby(self):
  #  fields = "jobid,user,cluster,account,partition,cputimeraw,elapsedraw,timelimitraw,nnodes,ncpus,alloctres,submit,eligible,start"
  #  renamings = {"user":"netid", "cputimeraw":"cpu-seconds", "nnodes":"nodes", "ncpus":"cores", "timelimitraw":"limit-minutes"}
  #  numeric_fields = ["cpu-seconds", "elapsedraw", "limit-minutes", "nodes", "cores", "submit", "eligible"]
  #  df = [["12345"]
  #  cols = ["cluster", "sponsor", "netid", "name", "cpu-hours", "gpu-hours", "jobs", "account", "partition"]
  #  msr.double_groupby(df) == expected
~                                               
