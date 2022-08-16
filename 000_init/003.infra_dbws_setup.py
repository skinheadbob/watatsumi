# Databricks notebook source
# DBTITLE 1,define: get_secret_cfg(key:str)
def get_secret_cfg(cfg_spec:str):
  """
  Get 'secret' from Azure KeyVault, using the Value of 'cfg_spec'
  """
  # How to setup Databricks Secret Scope with Azure KeyVault:
  # https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes 
  return dbutils.secrets.get(scope=get_cfg('infra.dbws.secret_scope'), key=get_cfg(cfg_spec) )

# COMMAND ----------

# DBTITLE 1,define: ensure_mnt()
def ensure_mnt():
  """
  /mnt/[env] must be mounted to Storage Account Container [env]
  """
  existing_mount_points = { m.mountPoint:m.source for m in dbutils.fs.mounts() }

  storage_account_name = get_cfg('infra.azure.storage_account.name')
  container_name = get_cfg('infra.azure.storage_account.container.name')
  container_sas_conf_key = f'fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net'
  container_sas = get_secret_cfg('infra.azure.storage_account.container.sas_secret')

  dbws_mount_point = get_cfg('infra.dbws.mount_point')
  dbws_mount_source = f'wasbs://{container_name}@{storage_account_name}.blob.core.windows.net'

  if dbws_mount_point not in existing_mount_points:
    logger.info(f'mounting source {dbws_mount_source} to mount point {dbws_mount_point} ...')
    dbutils.fs.mount(
      source = dbws_mount_source,
      mount_point = dbws_mount_point,
      extra_configs = {container_sas_conf_key:container_sas}
    )
  elif existing_mount_points[dbws_mount_point] != dbws_mount_source:
    raise RuntimeError(f""" 
    unexpected source on mount point '{dbws_mount_point}': 
      was expecting: {dbws_mount_source}
       actually got: {existing_mount_points[dbws_mount_point]} """)
  else:
    logger.debug(f'mount point "{dbws_mount_point}" is sourced from "{dbws_mount_source}" as expected')
  return

# COMMAND ----------

# DBTITLE 1,define: make_dbfs_temp_dir()
def make_dbfs_temp_dir():
  import pytz
  from coolname import generate_slug
  from datetime import datetime
  time_part = datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y%m%d_%H%M%S')
  text_part = generate_slug(2)
  dbfs_temp_dir_path = f"/dbfs{get_cfg('dbws.mount_point')}{get_cfg('dbws.temp_dir_path')}/{time_part}_{text_part}"
  
  import subprocess
  subprocess.run(f'mkdir -p "{dbfs_temp_dir_path}"',check=True,shell=True)
  return dbfs_temp_dir_path

# COMMAND ----------

# DBTITLE 1,define: remove_dbfs_dir()
def remove_dbfs_dir( mnt_dir:str ):
  # TODO not really sure why 'dbutils.fs.rm(mnt_dir,recurse=True)' does not delete nested directries
  try:
    for f in dbutils.fs.ls(mnt_dir):
      if f.isDir():
        remove_dbfs_dir(f.path)
      dbutils.fs.rm(f.path,recurse=True)
  except Exception as e:
    pass
  return dbutils.fs.rm(mnt_dir,recurse=True)
