'''This is the configuration file for sending periodic reports.'''



#################
## UNSUBSCRIBE ##
#################
'''List of usernames of unsubscribed sponsors. If none then
   use UNSUBSCRIBED_SPONSORS = []'''
UNSUBSCRIBED_SPONSORS = ["einstein", "newton"]

'''List of usernames of unsubscribed users. If none then
   use UNSUBSCRIBED_USERS = []'''
UNSUBSCRIBED_USERS = ["user42"]



#################################
## CLUSTERS AND GPU PARTITIONS ##
#################################
'''Specify the names of the clusters. This is used for the
   call to sacct to get the data. There must be at least
   one cluster in the list.'''
CLUSTERS = ["cluster1", "cluster2", "cluster3"]

'''Specify the cluster-partition pairs that run GPU jobs. The
   form is [("cluster", "parition")]. All other cluster-partition
   pairs are assumed to run CPU-only jobs.'''
GPU_CLUSTER_PARTITIONS = [("della", "gpu"),
                          ("della", "pli"),
                          ("della", "pli-c"),
                          ("della", "pli-p"),
                          ("della", "mig"),
                          ("stellar", "gpu"),
                          ("tiger", "cryoem(gpu)"),
                          ("tiger", "gpu"),
                          ("tiger", "motion"),
                          ("traverse", "all(gpu)")]



###########
## EMAIL ##
###########
'''What is the email address of the sender of the reports? This will
   also serve as the reply-to address.'''
SENDER_EMAIL = "rc@princeton.edu"

'''Enter of list of administrators to received a fraction of the reports.
   If none then use ADMIN_EMAILS = []'''
ADMIN_EMAILS = ["admin1@princeton.edu", "admin2@princeton.edu"]

'''The code will send some fraction of the reports to the ADMIN_EMAILS so
   that administrators can check the results. What fraction of the
   reports should be sent to the ADMIN_EMAILS? If you have 100 sponsors
   and want to receive about 5 reports then use ADMIN_FRAC = 0.05. To
   send no reports to admins use ADMIN_FRAC = 0.'''
ADMIN_FRAC = 0.05



###############
## RENAMINGS ##
###############
'''Override full names based on username. Useful if full name has accent
   characters.'''
CORRECTED_FULL_NAMES = [("cpena", "Catherine J. Pena"),
                        ("javalos", "Jose L. Avalos"),
                        ("alemay", "Amelie Lemay"),
                        ("simoes", "Tiago R. Simoes"),
                        ("alvaros", "Alvaro Luna")]

'''Rename partitions. This is useful for dealing with long
   partition names.'''
PARTITION_RENAMINGS = [("datascience", "datasci")]

'''Rename clusters. When to use slurm database name and when this one?
   This is applied after the call to sacct.'''
CLUSTER_RENAMINGS = [("tiger2", "tiger")]
