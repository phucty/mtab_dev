# Change this value to your project directory
DIR_ROOT = "/Users/phucnguyen/git/mtab"

DOMAIN_ONLINE = "https://mtab.app"
DOMAIN_LOCAL = "http://0.0.0.0:5000"
DOMAIN = DOMAIN_ONLINE

# Dataset Directories
dir_tables = DIR_ROOT + "/data/{challenge}/{data_name}/tables"

# Target files
dir_cea_tar = DIR_ROOT + "/data/{challenge}/{data_name}/cea.csv"
dir_cta_tar = DIR_ROOT + "/data/{challenge}/{data_name}/cta.csv"
dir_cpa_tar = DIR_ROOT + "/data/{challenge}/{data_name}/cpa.csv"

# Result files
dir_cea_res = DIR_ROOT + "/results/{challenge}/{data_name}/{source}/cea.csv"
dir_cta_res = DIR_ROOT + "/results/{challenge}/{data_name}/{source}/cta.csv"
dir_cpa_res = DIR_ROOT + "/results/{challenge}/{data_name}/{source}/cpa.csv"


# Request config
LIMIT_TIME_OUT = 7200  # 7200 86400
LIMIT_RETRIES = 5
