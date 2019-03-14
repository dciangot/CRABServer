#pylint: disable=C0103,W0105,broad-except,logging-not-lazy,W0702,C0301,R0902,R0914,R0912,R0915

"""
Configuration file for CRAB standalone Publisher
"""
from __future__ import division
from WMCore.Configuration import Configuration

config = Configuration()
config.section_('General')

config.General.asoworker = 'asoless'
config.General.isOracle = True
config.General.oracleDB = 'vocms035.cern.ch'
config.General.oracleFileTrans = '/crabserver/dev/filetransfers'
config.General.oracleUserTrans = '/crabserver/dev/fileusertransfers'
config.General.logLevel = 'INFO'
config.General.pollInterval = 1800
config.General.publish_dbs_url = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter'
config.General.block_closure_timeout = 9400
config.General.serverDN = '/DC=ch/DC=cern/OU=computers/CN=vocms0109.cern.ch'
config.General.cache_area = 'https://vocms035.cern.ch/crabserver/dev/filemetadata'
#config.General.cache_area = 'https://cmsweb.cern.ch/crabserver/prod/filemetadata'
config.General.workflow_expiration_time = 3
config.General.serviceCert = '/data/certs/hostcert.pem'
config.General.serviceKey = '/data/certs/hostkey.pem'
config.General.logMsgFormat = '%(asctime)s:%(levelname)s:%(module)s:%(name)s: %(message)s'
config.General.max_files_per_block = 100
config.General.opsCert = '/data/certs/servicecert.pem'
config.General.opsKey = '/data/certs/servicekey.pem'
config.General.cache_path = '/crabserver/dev/filemetadata'
config.General.task_path = '/crabserver/dev/task'

config.section_('Publisher')
config.Publisher.logMsgFormat = '%(asctime)s:%(levelname)s: %(message)s'
