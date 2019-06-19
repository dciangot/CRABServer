#! /bin/env python

from __future__ import absolute_import, division, print_function
import logging
import os
import json
# import re
# from argparse import ArgumentParser

from CMSRucio import CMSRucio
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Storage.TrivialFileCatalog import readTFC
# import fts3.rest.client.easy as fts3
# from datetime import timedelta
from ServerUtilities import encodeRequest

from TransferInterface.RegisterFiles import submit
from TransferInterface.MonitorTransfers import monitor



def chunks(l, n):
    """
    Yield successive n-sized chunks from l.
    :param l: list to splitt in chunks
    :param n: chunk size
    :return: yield the next list chunk
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def mark_transferred(ids, oracleDB):
    """
    Mark the list of files as tranferred
    :param ids: list of Oracle file ids to update
    :return: 0 success, 1 failure
    """
    os.environ["X509_CERT_DIR"] = os.getcwd()

    already_list = []
    if os.path.exists("task_process/transfers/transferred_files.txt"):
        with open("task_process/transfers/transferred_files.txt", "r") as list_file:
            for _data in list_file.readlines():
                already_list.append(_data.split("\n")[0])

    ids = [x for x in ids if x not in already_list]

    if len(ids) > 0:
        try:
            logging.debug("Marking done %s" % ids)

            data = dict()
            data['asoworker'] = 'rucio'
            data['subresource'] = 'updateTransfers'
            data['list_of_ids'] = ids
            data['list_of_transfer_state'] = ["DONE" for _ in ids]

            oracleDB.post('/filetransfers',
                          data=encodeRequest(data))
            logging.info("Marked good %s" % ids)
            with open("task_process/transfers/transferred_files.txt", "a+") as list_file:
                for id_ in ids:
                    list_file.write("%s\n" % id_)
        except Exception:
            logging.exception("Error updating documents")
            return 1
    else:
        logging.info("Nothing to update (Done)")
    return 0


def mark_failed(ids, failures_reasons, oracleDB):
    """
    Mark the list of files as failed
    :param ids: list of Oracle file ids to update
    :param failures_reasons: list of strings with transfer failure messages
    :return: 0 success, 1 failure
    """
    os.environ["X509_CERT_DIR"] = os.getcwd()

    if len(ids) > 0:

        try:
            data = dict()
            data['asoworker'] = 'rucio'
            data['subresource'] = 'updateTransfers'
            data['list_of_ids'] = ids
            data['list_of_transfer_state'] = ["FAILED" for _ in ids]
            data['list_of_failure_reason'] = failures_reasons
            data['list_of_retry_value'] = [0 for _ in ids]

            oracleDB.post('/filetransfers',
                          data=encodeRequest(data))
            logging.info("Marked failed %s" % ids)
        except Exception:
            logging.exception("Error updating documents")
            return None
    else:
        logging.info("Nothing to update (Failed)")

    return ids


def monitor_manager_rucio(user, taskname):
    """Monitor Rucio replica locks for user task
    :param user: user HN name
    :type user: str
    :param taskname: [description]
    :type taskname: [type]
    :return: [description]
    :rtype: [type]
    """

    # Get proxy and rest endpoint information
    proxy = None
    if os.path.exists('task_process/rest_filetransfers.txt'):
        with open("task_process/rest_filetransfers.txt", "r") as _rest:
            _rest.readline().split('\n')[0]
            proxy = os.getcwd() + "/" + _rest.readline()
            logging.info("Proxy: %s", proxy)
            os.environ["X509_USER_PROXY"] = proxy

    if not proxy:
        logging.info('No proxy available yet - waiting for first post-job')
        return None

    try:
        monitor(user, taskname, logging)
    except Exception:
        logging.exception('Monitor process failed.')

    return 0

def perform_transfers_rucio(inputFile, lastLine, direct=False):
    """
    get transfers submitted and update last read line number
    :param inputFile: file name containing post job files ready
    :type inputFile: str
    :param lastLine: last line processed
    :type lastLine: int
    :param direct: job output stored on temp or directly, defaults to False
    :param direct: bool, optional
    :return: (username,taskname) or None in case of critical error
    :rtype: tuple or None
    """

    if not os.path.exists(inputFile):
        return None, None

    # Get proxy and rest endpoint information
    proxy = None
    if os.path.exists('task_process/rest_filetransfers.txt'):
        with open("task_process/rest_filetransfers.txt", "r") as _rest:
            rest_filetransfers = _rest.readline().split('\n')[0]
            proxy = os.getcwd() + "/" + _rest.readline()
            logging.info("Proxy: %s", proxy)
            os.environ["X509_USER_PROXY"] = proxy

    if not proxy:
        logging.info('No proxy available yet - waiting for first post-job')
        return None

    logging.info("starting from line: %s" % lastLine)

    file_to_submit = []
    to_submit_columns = ["source_lfn",
                         "destination_lfn",
                         "id",
                         "source",
                         "destination",
                         "checksums",
                         "filesize"
                         ]
    transfers = []
    user = None
    taskname = None
    destination = None

    with open(inputFile) as _list:
        doc = json.loads(_list.readlines()[0])
        user = doc['username']
        taskname = doc['taskname']

    # Save needed info in ordered lists
    with open(inputFile) as _list:
        for _data in _list.readlines()[lastLine:]:
            file_to_submit = []
            try:
                lastLine += 1
                doc = json.loads(_data)
            except Exception:
                continue
            for column in to_submit_columns:
                if column not in ['checksums']:
                    file_to_submit.append(doc[column])
                if column == "checksums":
                    file_to_submit.append(doc["checksums"]["adler32"])
            transfers.append(file_to_submit)
            destination = doc["destination"]

    # Store general job metadata
    job_data = {'taskname': taskname,
                'username': user,
                'destination': destination,
                'proxy': proxy,
                'rest': rest_filetransfers}

    # Pass collected info to submit function
    if len(transfers) > 0:
        if not direct:
            try:
                submit((transfers, to_submit_columns), job_data, logging)
                # TODO: send to dashboard
            except Exception:
                logging.exception('Submission process failed.')

            # update last read line
            with open("task_process/transfers/last_transfer_new.txt", "w+") as _last:
                _last.write(str(lastLine))
            os.rename("task_process/transfers/last_transfer_new.txt", "task_process/transfers/last_transfer.txt")

        elif direct:
            try:
                submit((transfers, to_submit_columns), job_data, logging, direct=True)
            except Exception:
                logging.exception('Registering direct stage files failed.')

            # update last read line
            with open("task_process/transfers/last_transfer_direct_new.txt", "w+") as _last:
                _last.write(str(lastLine))
            os.rename("task_process/transfers/last_transfer_direct_new.txt", "task_process/transfers/last_transfer_direct.txt")

    return user, taskname

def submission_manager_rucio():
    """
    Wrapper for Rucio submission algorithm. 
    :return: results of perform_transfers function (non direct stageout only)
    :rtype: tuple or None
    """
    last_line = 0
    if os.path.exists('task_process/transfers/last_transfer.txt'):
        with open("task_process/transfers/last_transfer.txt", "r") as _last:
            read = _last.readline()
            last_line = int(read)
            logging.info("last line is: %s", last_line)

    # TODO: if the following fails check not to leave a corrupted file
    r = perform_transfers_rucio("task_process/transfers.txt",
                          last_line)

    return r

class CRABDataInjector(CMSRucio):
    """
    General Class for injecting a cms dataset in rucio
    """

    def __init__(self, dataset, site, rse=None, scope='cms',
                 uuid=None, check=True, lifetime=None, dry_run=None, account=None, auth_type=None, creds=None):

        super(CRABDataInjector, self).__init__(account=account, auth_type=auth_type, scope=scope, dry_run=dry_run, creds=creds)
        self.dataset = dataset
        self.site = site
        if rse is None:
            rse = site
        self.rse = rse
        self.uuid = uuid
        self.check = check
        self.lifetime = lifetime

    def add_dataset(self):
        """[summary]

        """

        logging.info(self.site)
        self.register_dataset(self.dataset, '', self.lifetime)

        self.cli.add_replication_rule(dids=[{'scope': self.scope, 'name': "/"+self.dataset}],
                                      copies=1,
                                      rse_expression="{0}=True".format(self.rse),
                                      comment="")