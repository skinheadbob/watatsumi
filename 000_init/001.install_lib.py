# Databricks notebook source
_REQUIRED_LIB_LIST = [
  'pyyaml', 'glom',
  'coolname', 'tqdm',
  'azure-storage-blob',

  'google-cloud-storage', # ingest from GCS
  'azure-storage-file-share', # ingest from Azure FileShare
  'pysmb', # ingest from NAS
  'Office365-REST-Python-Client', # ingest from SharePoint
  
  # 'openpyxl', # parse Excel
  
  # ingest from HBase
  # 'happybase',
  
  # ingest from Microsoft Access, may also need to 'apt-get install mdbtools'
  # 'sqlparse', # parse 
  # 'cutlet', 'unidic-lite', 
  
  # ingest from Microsoft SQLServer
  # 'pymssql',
  
  # ingest from BigQuery
  # 'google-cloud-bigquery','db-dtypes',
  
  # ingest from TreasureData
  # 'pytd',
  
  # Japan postal code
  # 'posuto',  
]
_PIP_INSTALL_LIBS = ' '.join(_REQUIRED_LIB_LIST)

print(f'installing {len(_REQUIRED_LIB_LIST)} python libs: {_REQUIRED_LIB_LIST} ...')
%pip install -q $_PIP_INSTALL_LIBS
