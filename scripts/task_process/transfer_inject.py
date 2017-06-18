#!/usr/bin/python

import json
import urllib
import logging
import os

import pycurl
from io import BytesIO
import fts3.rest.client.easy as fts3
from datetime import timedelta
from RESTInteractions import HTTPRequests
from ServerUtilities import  encodeRequest

logging.basicConfig(filename='task_process/transfer_inject.log', level=logging.DEBUG)
proxy = os.environ.get('X509_USER_PROXY')


def apply_tfc_to_lfn(site, lfn, c):
    """
    Take a CMS_NAME:lfn string and make a pfn.

    :param site: site name
    :param lfn:
    :param c: curl session
    :return: pfn
    """

    # curl https://cmsweb.cern.ch/phedex/datasvc/json/prod/tfc?node=site
    # TODO: cache the tfc rules
    input_dict = {'node': site, 'lfn': lfn, 'protocol': "srmv2", 'custodial': 'n'}
    c.setopt(c.URL, 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn?'+urllib.urlencode(input_dict))
    e = BytesIO()
    c.setopt(pycurl.WRITEFUNCTION, e.write)
    c.perform()
    # print(e.getvalue().decode('UTF-8'))
    results = json.loads(e.getvalue().decode('UTF-8'))

    e.close()
    return results["phedex"]["mapping"][0]["pfn"]


def chunks(l, n):
    """
    Yield successive n-sized chunks from l.
    :param l: list to splitt in chunks
    :param n: chunk size
    :return: yield the next list chunk
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def mark_transferred(oracleDB, ids):
    """
    Mark the list of files as tranferred
    :param oracleDB: oracleDB httpRequest object
    :param ids: list of Oracle file ids to update
    :return: 0 success, 1 failure
    """
    try:
        logging.debug("Marking done %s" % ids)

        data = dict()
        data['asoworker'] = 'vocms059'
        data['subresource'] = 'updateTransfers'
        data['list_of_ids'] = ids
        data['list_of_transfer_state'] = ["DONE" for x in ids]

        oracleDB.post('filetransfers',
                      data=encodeRequest(data))
        logging.debug("Marked good %s" % ids)
    except Exception:
        logging.exception("Error updating documents")
        return 1
    return 0


def mark_failed(oracleDB, ids, failures_reasons):
    """
    Mark the list of files as failed
    :param oracleDB: oracleDB httpRequest object
    :param ids: list of Oracle file ids to update
    :param failures_reasons: list of strings with transfer failure messages
    :return: 0 success, 1 failure
    """

    try:
        data = dict()
        data['asoworker'] = 'vocms059'
        data['subresource'] = 'updateTransfers'
        data['list_of_ids'] = ids
        data['list_of_transfer_state'] = ["FAILED" for _ in ids]
        data['list_of_failure_reason'] = [failures_reasons for _ in ids]
        data['list_of_retry_value'] = [0 for _ in ids]

        oracleDB.post('filetransfers',
                      data=encodeRequest(data))
        logging.debug("Marked failed %s" % ids)
    except Exception:
        logging.exception("Error updating documents")
        return 1
    return 0


def submit(context, toTrans):
    """
    submit tranfer jobs

    - group files to be transferred by source site
    - prepare jobs chunks of max 200 transfers
    - submit fts job

    :param context: fts client context
    :param toTrans: [source pfn, destination pfn, oracle file id, source site]
    :return: list of jobids submitted
    """
    # prepare rest job with 200 files per job
    jobids = []

    oracleDB = HTTPRequests('asotest3.cern.ch/crabserver/dev/',
                            proxy,
                            proxy)

    sources = list(set([x[3] for x in toTrans]))
    for source in sources:

        tx_from_source = [x for x in toTrans if x[3] == source]

        for files in chunks(tx_from_source, 200):
            logging.info("Processing transfers from: %s" % source)

            c = pycurl.Curl()
            # create destination and source pfns for job
            transfers = []
            for lfn in files:
                transfers.append(fts3.new_transfer(lfn[0],
                                                   lfn[1],
                                                   metadata={'oracleId': lfn[2]}
                                                   )
                                 )
            c.close()

            logging.info("Submitting %s transfers to FTS server" % len(files))

            # Submit fts job
            job = fts3.new_job(transfers,
                               overwrite=True,
                               verify_checksum=True,
                               # TODO: add user DN to metadata
                               metadata={"issuer": "ASO"},
                               copy_pin_lifetime=-1,
                               bring_online=None,
                               source_spacetoken=None,
                               spacetoken=None,
                               max_time_in_queue=6,
                               retry=3,
                               retry_delay=3,
                               reuse=True
                               )
            # TODO: fts retries?? check delay

            jobid = fts3.submit(context, job)
            jobids.append(jobid)

            # TODO: manage exception here, what we should do?

            fileDoc = dict()
            fileDoc['asoworker'] = 'vocms059'
            fileDoc['subresource'] = 'updateTransfers'
            fileDoc['list_of_ids'] = [x[2] for x in files]
            fileDoc['list_of_transfer_state'] = ["SUBMITTED" for x in files]
            fileDoc['list_of_fts_instance'] = ['https://fts3.cern.ch:8446/' for x in files]
            fileDoc['list_of_fts_id'] = [jobid for x in files ]

            logging.info("Marking submitted %s files" % (len(fileDoc['list_of_ids'])))
            result = oracleDB.post('filetransfers',
                                   data=encodeRequest(fileDoc))
            logging.info("Marked submitted %s files" % (fileDoc['list_of_ids']))

    return jobids


def get_status(fts, jobid):
    """
    get transfers state per jobid

    - check if the fts job is in final state (FINISHED, FINISHEDDIRTY, CANCELED, FAILED)
    - get file transfers states and get corresponding oracle ID from FTS file metadata
    - update states on oracle

    :param fts: FTS REST requesHandler
    :param jobid: fts jobid
    :return : bool for states update success or not
    """

    logging.info("Getting state of job %s" % (jobid))
    # FIXME: don't work with pycurl in schedds, directly rest?

    oracleDB = HTTPRequests('asotest3.cern.ch/crabserver/dev/',
                            proxy,
                            proxy)

    done = False
    doneReady = -1
    failedReady = -1

    status = fts.get("jobs/"+jobid)[0]

    logging.info("State of job %s: %s" % (jobid, status["job_state"]))

    # TODO: if in final state get with list_files=True and the update_states
    if status["job_state"] in ['FINISHED', 'FINISHEDDIRTY', "FAILED", "CANCELED"]:
        file_status = fts.get("jobs/%s/files" % jobid)[0]

        failed_id = []
        failed_reasons = []
        done_id = []
        for file_status in file_status:
            _id = file_status['file_metadata']['oracleId']
            tx_state = file_status['file_state']

            if tx_state == 'FINISHED':
                done_id.append(_id)
            else:
                failed_id.append(_id)
                if file_status['reason']:
                    logging.info('Failure reason: ' + file_status['reason'])
                    failed_reasons.append(file_status['reason'])
                else:
                    logging.exception('Failure reason not found')
                    failed_reasons.append('unable to get failure reason')

        try:
            logging.info('Marking job %s files done and %s files  failed for job %s' % (len(done_id), len(failed_id), jobid))
            if len(done_id)>0:
                doneReady = mark_transferred(oracleDB, done_id)
            else:
                doneReady = 0
            if len(failed_id)>0:
                failedReady = mark_failed(oracleDB, failed_id, failed_reasons)
            else:
                failedReady = 0
        except Exception:
            logging.exception('Failed to update states')

    if doneReady == 0 and failedReady == 0:
        done = True
    return done


def perform_transfers(inputFile, lastLine, _lastFile, context):
    """
    get transfers and update last read line number
    :param inputFile:
    :param lastLine:
    :return:
    """

    transfers = []
    logging.info("starting from line: %s" % lastLine)

    with open(inputFile) as _list:
        for _data in _list.readlines()[lastLine:]:
            lastLine += 1
            data = json.loads(_data)

            c = pycurl.Curl()
            # create destination and source pfns for job
            pfn_source = apply_tfc_to_lfn(data["source"], data["source_lfn"], c)
            pfn_dest = apply_tfc_to_lfn(data["destination"], data["destination_lfn"], c)
            c.close()
            transfers.append((pfn_source, pfn_dest, data['_id'], data["source"]))

        jobids = submit(context, transfers)

        for jobid in jobids:
            logging.info("Monitor link: https://fts3.cern.ch:8449/fts3/ftsmon/#/job/"+jobid)

        # TODO: send to dashboard
        _lastFile.write(str(lastLine))

    return transfers, jobids


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

    :return:
    """

    # TODO: pass by configuration
    fts = HTTPRequests('fts3.cern.ch:8446/',
                        proxy,
                        proxy)

    context = fts3.Context('https://fts3.cern.ch:8446', proxy, proxy, verify=True)
    logging.info("Delegating proxy: "+fts3.delegate(context, lifetime=timedelta(hours=48), force=False))

    with open("task_process/fts_jobids.txt", "r") as _jobids:
        lines = _jobids.readlines()
        jobs_done = []
        jobs_ongoing = []
        for line in lines:
            if line:
                jobid = line.split('\n')[0]
                if jobid:
                    done = get_status(fts, jobid)
                    if done:
                        jobs_done.append(line)
                    else:
                        jobs_ongoing.append(line)
        _jobids.close()

    with open("task_process/fts_jobids.txt", "w") as _jobids:
        for line in jobs_ongoing:
            _jobids.write(line)
        _jobids.close()

    with open("task_process/last_transfer.txt", "r") as _last:
        read = _last.readline()
        last_line = int(read)
        logging.info("last line is: %s" % last_line)
        _last.close()

    # TODO: if the following fails check not to leave a corrupted file
    with open("task_process/last_transfer.txt", "w") as _last:
        transfers, jobids = perform_transfers("task_process/transfers.txt", last_line, _last, context)
        _last.close()

    with open("task_process/fts_jobids.txt", "a") as _jobids:
        for job in jobids:
            _jobids.write(str(job)+"\n")
        _jobids.close()

    # TODO: upload to oracle with fts
    # TODO: multithreading submit? https://www.tutorialspoint.com/python/python_multithreading.htm

    return

if __name__ == "__main__":
    try:
        algorithm()
    except Exception:
        logging.exception("error during main loop")
    logging.debug("transfer_inject.py exiting")

