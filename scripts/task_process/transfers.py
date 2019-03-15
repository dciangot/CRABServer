#!/usr/bin/python
"""

"""
from __future__ import absolute_import, division, print_function
import json
import logging
import os

from TransferInterface.RegisterFiles import submit
from TransferInterface.MonitorTransfers import monitor


if not os.path.exists('task_process/transfers'):
    os.makedirs('task_process/transfers')

logging.basicConfig(
    filename='task_process/transfers/transfer_inject.log',
    level=logging.DEBUG,
    format='%(asctime)s[%(relativeCreated)6d]%(threadName)s: %(message)s'
)

USER = None
TASKNAME = None


def perform_transfers(inputFile, lastLine, direct=False):
    """
    get transfers and update last read line number
    :param inputFile:
    :param lastLine:
    :return:
    """
    if not os.path.exists(inputFile):
        return None, None

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

    job_data = {'taskname': taskname,
                'username': user,
                'destination': destination,
                'proxy': proxy,
                'rest': rest_filetransfers}

    if len(transfers) > 0:
        if not direct:
            try:
                submit((transfers, to_submit_columns), job_data, logging)
                # TODO: send to dashboard
            except Exception:
                logging.exception('Submission process failed.')

            with open("task_process/transfers/last_transfer_new.txt", "w+") as _last:
                _last.write(str(lastLine))

            os.rename("task_process/transfers/last_transfer_new.txt", "task_process/transfers/last_transfer.txt")
        elif direct:
            try:
                submit((transfers, to_submit_columns), job_data, logging, direct=True)
                # TODO: send to dashboard
            except Exception:
                logging.exception('Submission process failed.')

            with open("task_process/transfers/last_transfer_direct_new.txt", "w+") as _last:
                _last.write(str(lastLine))

            os.rename("task_process/transfers/last_transfer_direct_new.txt", "task_process/transfers/last_transfer_direct.txt")

    return user, taskname


def monitor_manager(user, taskname):
    """[summary]

    """
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


def submission_manager():
    """

    """
    last_line = 0
    if os.path.exists('task_process/transfers/last_transfer.txt'):
        with open("task_process/transfers/last_transfer.txt", "r") as _last:
            read = _last.readline()
            last_line = int(read)
            logging.info("last line is: %s", last_line)
            _last.close()
    
    # TODO: if the following fails check not to leave a corrupted file
    r = perform_transfers("task_process/transfers.txt",
                          last_line)

    if os.path.exists('task_process/transfers/last_transfer_direct.txt'):
        with open("task_process/transfers/last_transfer_direct.txt", "r") as _last:
            read = _last.readline()
            last_line = int(read)
            logging.info("last line is: %s", last_line)
            _last.close()

    # TODO: if the following fails check not to leave a corrupted file
    perform_transfers("task_process/transfers_direct.txt",
                      last_line, direct=True)

    return r


def algorithm():
    """
    script algorithm
    - create fts REST HTTPRequest
    - delegate user proxy to fts if needed
    - check for fts jobs to monitor and update states in oracle
    - get last line from last_transfer.txt
    - gather list of file to transfers
        + group by source
        + submit ftsjob and save fts jobid
        + update info in oracle
    - append new fts job ids to fts_jobids.txt
    """

    user = None
    try:
        user, taskname = submission_manager()
    except Exception:
        logging.exception('Submission proccess failed.')

    if not user:
        logging.info('Nothing to monitor yet.')
        return
    try:
        monitor_manager(user, taskname)
    except Exception:
        logging.exception('Monitor proccess failed.')

    return


if __name__ == "__main__":
    try:
        algorithm()
    except Exception:
        logging.exception("error during main loop")
    logging.debug("transfers.py exiting")