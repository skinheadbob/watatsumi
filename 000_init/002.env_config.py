# Databricks notebook source
# DBTITLE 1,'private' global variables
# the list of valid ENV
_WATATSUMI_INTERACTIVE_ENV_LIST = ['dev','prod']
_WATATSUMI_RESERVED_ENV_LIST = ['cicd']
_WATATSUMI_ENV_LIST = _WATATSUMI_INTERACTIVE_ENV_LIST + _WATATSUMI_RESERVED_ENV_LIST

# which ENV is currently active
_WATATSUMI_ENV = None

_WATATSUMI_YAML = f"""
default:
  infra:
    azure:
      storage_account:
        name: your_adls_gen2_sa
        url: https://your_adls_gen2_sa.blob.core.windows.net
    dbws:
      # How to setup Databricks Secret Scope with Azure KeyVault:
      # https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes  
      secret_scope: your_azure_keyvault_secret_scope
      temp_dir_path: /tmp

dev:
  infra:
    azure:
      storage_account:
        container:
          name: openwatatsumi-dev
          sas_secret: sa-your_adls_gen2_sa-container-openwatatsumi-dev-rw-sas
    dbws:
      mount_point: /mnt/openwatatsumi_dev
      hive_database: openwatatsumi_dev
    
cicd:
  infra:
    azure:
      storage_account:
        container:
          name: openwatatsumi-cicd
          sas_secret: sa-your_adls_gen2_sa-container-openwatatsumi-cicd-rw-sas
    dbws:
      mount_point: /mnt/openwatatsumi_cicd
      hive_database: openwatatsumi_cicd    
    
prod:
  infra:
    azure:
      storage_account:
        container:
          name: openwatatsumi-prod
          sas_secret: sa-your_adls_gen2_sa-container-openwatatsumi-prod-rw-sas
    dbws:
      mount_point: /mnt/openwatatsumi_prod
      hive_database: openwatatsumi_prod 
"""

_WATATSUMI_ZONE_TABLENAME_PREFIX_DICT = {
  # 'landing' zone holds .csv/.zip files that are NOT DeltaLake format, hence no tablename prefix
  'bronze':'cu_', # external data converted to DeltaLake format with no content change
  'silver':'ag_', # 
  'gold':'au_', # could be SCD2 DIM, could be Fact
  'departure':'dep_', # remember what is delivered to external systems
  'datascience':'ds_', # where data scientists create tables
  'bizintel':'bi_', # metrics that data engineers / sales / data scientists are interested in, e.g. quality of vendor data
  'temp':'tmp_', # ad-hoc experiments
}

import logging
logger = logging.getLogger('default_logger')

# COMMAND ----------

# DBTITLE 1,define: set_env(), get_env(), enable_debug_logging()
def _init_logger():
  global logger
  import sys
  if not logger.handlers:
    logger.setLevel(level=logging.INFO)
    logger_handler = logging.StreamHandler(sys.stdout)
    logger_handler.setFormatter(logging.Formatter('%(asctime)s-%(levelname)s: %(message)s'))
    logger.addHandler(logger_handler)
    return

def enable_debug_logging():
  global logger
  logger.setLevel(level=logging.DEBUG)
  logger.debug('logger level set as DEBUG')
  return

def set_env( input_env:str=None ):
  global _WATATSUMI_INTERACTIVE_ENV_LIST
  global _WATATSUMI_RESERVED_ENV_LIST
  global _WATATSUMI_ENV
  
  if input_env is None:
    try:
      dbutils.widgets.remove('RESERVED ENV')
    except:
      pass
    dbutils.widgets.dropdown('ENV', 'dev', _WATATSUMI_INTERACTIVE_ENV_LIST, '01 ENV')
    input_env = dbutils.widgets.get('ENV')
  else:
    try:
      dbutils.widgets.remove('ENV')
    except:
      pass
    dbutils.widgets.dropdown('RESERVED ENV', input_env, [input_env], '00 RESERVED ENV')
  
  if input_env not in _WATATSUMI_ENV_LIST:
    raise RuntimeError(f'unsupported ENV "{input_env}", only "{_WATATSUMI_ENV_LIST}" are supported')
  
  if _WATATSUMI_ENV is None:
    _WATATSUMI_ENV = input_env
    _init_logger()
  elif _WATATSUMI_ENV != input_env:
    raise RuntimeError(f'to switch ENV, please "Clear State" (dynamically switching ENV is not supported)')
  return

def get_env():
  global _WATATSUMI_ENV
  if _WATATSUMI_ENV is None:
    raise RuntimeError(f'please call "set_env()" first')
  return _WATATSUMI_ENV

# COMMAND ----------

# DBTITLE 1,define: get_cfg(cfg_spec:str)
def get_cfg( cfg_spec:str ):
  """
  e.g. get_cfg( 'yellowbrick.database' ) 
  YAML config is defined in _WATATSUMI_YAML
  """
  import yaml
  from glom import glom
  global _WATATSUMI_YAML
  try:
    env_spec = f'{get_env()}.{cfg_spec}'
    return glom(yaml.load(_WATATSUMI_YAML,Loader=yaml.FullLoader),env_spec)
  except:
    default_spec = f'default.{cfg_spec}'
    return glom(yaml.load(_WATATSUMI_YAML,Loader=yaml.FullLoader),default_spec)
