from __future__ import print_function
import os
import time
import json
import pprint
import hashlib
import datetime
import traceback
from httplib import HTTPException
from ServerUtilities import getLock
from RESTInteractions import HTTPRequests ## Why not to use from WMCore.Services.Requests import Requests
from ServerUtilities import TRANSFERDB_STATES, PUBLICATIONDB_STATES, encodeRequest, oracleOutputMapping

from TaskWorker.Actions.PostJob import first_pj_execution, TransferCacheLoadError, G_JOB_REPORT_NAME, get_file_index, NotFound


class ASOServerJob(object):
    """
    Class used to inject transfer requests to ASO database.
    """
    def __init__(self, logger, aso_start_time, aso_start_timestamp, dest_site, source_dir,
                 dest_dir, source_sites, job_id, filenames, reqname, log_size,
                 log_needs_transfer, job_report_output, job_ad, crab_retry, retry_timeout,
                 job_failed, transfer_logs, transfer_outputs, rest_host, rest_uri_no_api):
        """
        ASOServerJob constructor.
        """
        self.logger = logger
        self.docs_in_transfer = None
        self.crab_retry = crab_retry
        self.retry_timeout = retry_timeout
        self.couch_server = None
        self.couch_database = None
        self.job_id = job_id
        self.dest_site = dest_site
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        self.job_failed = job_failed
        self.source_sites = source_sites
        self.filenames = filenames
        self.reqname = reqname
        self.job_report_output = job_report_output
        self.log_size = log_size
        self.log_needs_transfer = log_needs_transfer
        self.transfer_logs = transfer_logs
        self.transfer_outputs = transfer_outputs
        self.job_ad = job_ad
        self.failures = {}
        self.aso_start_time = aso_start_time
        self.aso_start_timestamp = aso_start_timestamp
        proxy = os.environ.get('X509_USER_PROXY', None)
        self.aso_db_url = self.job_ad['CRAB_ASOURL']
        self.rest_host = rest_host
        self.rest_uri_no_api = rest_uri_no_api
        self.rest_uri_file_user_transfers = rest_uri_no_api + "/fileusertransfers"
        self.rest_uri_file_transfers = rest_uri_no_api + "/filetransfers"
        self.found_doc_in_db = False
        #I don't think it is necessary to default to asynctransfer here, we are taking care of it
        #in dagman creator and if CRAB_ASODB is not there it means it's old task executing old code
        #But just to make sure...
        self.aso_db_name = self.job_ad.get('CRAB_ASODB', 'asynctransfer') or 'asynctransfer'
        try:
            if first_pj_execution():
                self.logger.info("Will use ASO server at %s." % (self.aso_db_url))
            self.server = HTTPRequests(self.rest_host, proxy, proxy, retry=2)
        except Exception as ex:
            msg = "Failed to connect to ASO database: %s" % (str(ex))
            self.logger.exception(msg)
            raise RuntimeError(msg)

    def save_docs_in_transfer(self):
        """ The function is used to save into a file the documents we are transfering so
            we do not have to query couch to get this list every time the postjob is restarted.
        """
        try:
            filename = 'transfer_info/docs_in_transfer.%s.%d.json' % (self.job_id, self.crab_retry)
            with open(filename, 'w') as fd:
                json.dump(self.docs_in_transfer, fd)
        except:
            #Only printing a generic message, the full stacktrace is printed in execute()
            self.logger.error("Failed to save the docs in transfer. Aborting the postjob")
            raise

    def load_docs_in_transfer(self):
        """ Function that loads the object saved as a json by save_docs_in_transfer
        """
        try:
            filename = 'transfer_info/docs_in_transfer.%s.%d.json' % (self.job_id, self.crab_retry)
            with open(filename) as fd:
                self.docs_in_transfer = json.load(fd)
        except:
            #Only printing a generic message, the full stacktrace is printed in execute()
            self.logger.error("Failed to load the docs in transfer. Aborting the postjob")
            raise

    def check_transfers(self):
        self.load_docs_in_transfer()
        failed_killed_transfers = []
        done_transfers = []
        starttime = time.time()
        if self.aso_start_timestamp:
            starttime = self.aso_start_timestamp
        if first_pj_execution():
            self.logger.info("====== Starting to monitor ASO transfers.")
        try:
            with getLock('get_transfers_statuses'):
                ## Get the transfer status in all documents listed in self.docs_in_transfer.
                transfers_statuses = self.get_transfers_statuses()
        except TransferCacheLoadError as e:
            self.logger.info("Error getting the status of the transfers. Deferring PJ. Got: %s" % e)
            return 4
        msg = "Got statuses: %s; %.1f hours since transfer submit."
        msg = msg % (", ".join(transfers_statuses), (time.time()-starttime)/3600.0)
        self.logger.info(msg)
        all_transfers_finished = True
        doc_ids = [doc_info['doc_id'] for doc_info in self.docs_in_transfer]
        for transfer_status, doc_id in zip(transfers_statuses, doc_ids):
            ## States to wait on.
            if transfer_status in ['new', 'acquired', 'retry', 'unknown', 'submitted']:
                all_transfers_finished = False
                continue
            ## Good states.
            elif transfer_status in ['done']:
                if doc_id not in done_transfers:
                    done_transfers.append(doc_id)
                continue
            ## Bad states.
            elif transfer_status in ['failed', 'killed', 'kill']:
                if doc_id not in failed_killed_transfers:
                    failed_killed_transfers.append(doc_id)
                    msg = "Stageout job (internal ID %s) failed with status '%s'." % (doc_id, transfer_status)
                    doc = self.load_transfer_document(doc_id)
                    if doc and ('failure_reason' in doc) and doc['failure_reason']:
                        ## reasons:  The transfer failure reason(s).
                        ## app:      The application that gave the transfer failure reason(s).
                        ##           E.g. 'aso' or '' (meaning the postjob). When printing the
                        ##           transfer failure reasons (e.g. below), print also that the
                        ##           failures come from the given app (if app != '').
                        ## severity: Either 'permanent' or 'recoverable'.
                        ##           It is set by PostJob in the perform_transfers() function,
                        ##           when is_failure_permanent() is called to determine if a
                        ##           failure is permanent or not.
                        reasons, app, severity = doc['failure_reason'], 'aso', None
                        msg += " Failure reasons follow:"
                        if app:
                            msg += "\n-----> %s log start -----" % str(app).upper()
                        msg += "\n%s" % reasons
                        if app:
                            msg += "\n<----- %s log finish ----" % str(app).upper()
                        self.logger.error(msg)
                    else:
                        reasons, app, severity = "Failure reason unavailable.", None, None
                        self.logger.error(msg)
                        self.logger.warning("WARNING: no failure reason available.")
                    self.failures[doc_id] = {'reasons': reasons, 'app': app, 'severity': severity}
            else:
                exmsg = "Got an unknown transfer status: %s" % (transfer_status)
                raise RuntimeError(exmsg)
        if all_transfers_finished:
            msg = "All transfers finished."
            self.logger.info(msg)
            if failed_killed_transfers:
                msg  = "There were %d failed/killed transfers:" % (len(failed_killed_transfers))
                msg += " %s" % (", ".join(failed_killed_transfers))
                self.logger.info(msg)
                self.logger.info("====== Finished to monitor ASO transfers.")
                return 1
            self.logger.info("====== Finished to monitor ASO transfers.")
            return 0
        ## If there is a timeout for transfers to complete, check if it was exceeded
        ## and if so kill the ongoing transfers. # timeout = -1 means no timeout.
        if self.retry_timeout != -1 and time.time() - starttime > self.retry_timeout:
            msg  = "Post-job reached its timeout of %d seconds waiting for ASO transfers to complete." % (self.retry_timeout)
            msg += " Will cancel ongoing ASO transfers."
            self.logger.warning(msg)
            self.logger.info("====== Starting to cancel ongoing ASO transfers.")
            docs_to_cancel = {}
            reason = "Cancelled ASO transfer after timeout of %d seconds." % (self.retry_timeout)
            for doc_info in self.docs_in_transfer:
                doc_id = doc_info['doc_id']
                if doc_id not in done_transfers + failed_killed_transfers:
                    docs_to_cancel.update({doc_id: reason})
            cancelled, not_cancelled = self.cancel(docs_to_cancel, max_retries = 2)
            for doc_id in cancelled:
                failed_killed_transfers.append(doc_id)
                app, severity = None, None
                self.failures[doc_id] = {'reasons': reason, 'app': app, 'severity': severity}
            if not_cancelled:
                msg = "Failed to cancel %d ASO transfers: %s" % (len(not_cancelled), ", ".join(not_cancelled))
                self.logger.error(msg)
                self.logger.info("====== Finished to cancel ongoing ASO transfers.")
                self.logger.info("====== Finished to monitor ASO transfers.")
                return 2
            self.logger.info("====== Finished to cancel ongoing ASO transfers.")
            self.logger.info("====== Finished to monitor ASO transfers.")
            return 1
        ## defer the execution of the postjob
        return 4

    def run(self):
        """
        This is the main method in ASOServerJob. Should be called after initializing
        an instance.
        """
        with getLock('get_transfers_statuses'):
            self.docs_in_transfer = self.inject_to_aso()
        if self.docs_in_transfer == False:
            exmsg = "Couldn't upload document to ASO database"
            raise RuntimeError(exmsg)
        if not self.docs_in_transfer:
            self.logger.info("No files to transfer via ASO. Done!")
            return 0
        self.save_docs_in_transfer()
        return 4

    def recordASOStartTime(self):
        ## Add the post-job exit code and error message to the job report.
        job_report = {}
        try:
            with open(G_JOB_REPORT_NAME) as fd:
                job_report = json.load(fd)
        except (IOError, ValueError):
            pass
        job_report['aso_start_timestamp'] = int(time.time())
        job_report['aso_start_time'] = str(datetime.datetime.now())
        with open(G_JOB_REPORT_NAME, 'w') as fd:
            json.dump(job_report, fd)

    def inject_to_aso(self):
        """
        Inject documents to ASO database if not done by cmscp from worker node.
        """
        self.found_doc_in_db = False  # This is only for oracle implementation and we want to check before adding new doc.
        self.logger.info("====== Starting to check uploads to ASO database.")
        docs_in_transfer = []
        output_files = []
        now = str(datetime.datetime.now())
        last_update = int(time.time())

        if self.aso_start_timestamp == None or self.aso_start_time == None:
            self.aso_start_timestamp = last_update
            self.aso_start_time = now
            msg  = "Unable to determine ASO start time from job report."
            msg += " Will use ASO start time = %s (%s)."
            msg  = msg % (self.aso_start_time, self.aso_start_timestamp)
            self.logger.warning(msg)

        role = str(self.job_ad['CRAB_UserRole'])
        if str(self.job_ad['CRAB_UserRole']).lower() == 'undefined':
            role = ''
        group = str(self.job_ad['CRAB_UserGroup'])
        if str(self.job_ad['CRAB_UserGroup']).lower() == 'undefined':
            group = ''

        task_publish = int(self.job_ad['CRAB_Publish'])

        # TODO: Add a method to resolve a single PFN.
        if self.transfer_outputs:
            for output_module in self.job_report_output.values():
                for output_file_info in output_module:
                    file_info = {}
                    file_info['checksums'] = output_file_info.get(u'checksums', {'cksum': '0', 'adler32': '0'})
                    file_info['outsize'] = output_file_info.get(u'size', 0)
                    file_info['direct_stageout'] = output_file_info.get(u'direct_stageout', False)
                    if (output_file_info.get(u'output_module_class', '') == u'PoolOutputModule' or \
                        output_file_info.get(u'ouput_module_class',  '') == u'PoolOutputModule'):
                        file_info['filetype'] = 'EDM'
                    elif output_file_info.get(u'Source', '') == u'TFileService':
                        file_info['filetype'] = 'TFILE'
                    else:
                        file_info['filetype'] = 'FAKE'
                    if u'pfn' in output_file_info:
                        file_info['pfn'] = str(output_file_info[u'pfn'])
                    output_files.append(file_info)
        found_log = False
        for source_site, filename in zip(self.source_sites, self.filenames):
            ## We assume that the first file in self.filenames is the logs archive.
            if found_log:
                if not self.transfer_outputs:
                    continue
                source_lfn = os.path.join(self.source_dir, filename)
                dest_lfn = os.path.join(self.dest_dir, filename)
                file_type = 'output'
                ifile = get_file_index(filename, output_files)
                if ifile is None:
                    continue
                size = output_files[ifile]['outsize']
                checksums = output_files[ifile]['checksums']
                ## needs_transfer is False if and only if the file was staged out
                ## from the worker node directly to the permanent storage.
                needs_transfer = not output_files[ifile]['direct_stageout']
                file_output_type = output_files[ifile]['filetype']
            else:
                found_log = True
                if not self.transfer_logs:
                    continue
                source_lfn = os.path.join(self.source_dir, 'log', filename)
                dest_lfn = os.path.join(self.dest_dir, 'log', filename)
                file_type = 'log'
                size = self.log_size
                checksums = {'adler32': 'abc'}
                ## needs_transfer is False if and only if the file was staged out
                ## from the worker node directly to the permanent storage.
                needs_transfer = self.log_needs_transfer
            self.logger.info("Working on file %s" % (filename))
            doc_id = hashlib.sha224(source_lfn).hexdigest()
            doc_new_info = {'state': 'new',
                            'source': source_site,
                            'destination': self.dest_site,
                            'checksums': checksums,
                            'size': size,
                            'last_update': last_update,
                            'start_time': now,
                            'end_time': '',
                            'job_end_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                            'retry_count': [],
                            'failure_reason': [],
                            ## The 'job_retry_count' is used by ASO when reporting to dashboard,
                            ## so it is OK to set it equal to the crab (post-job) retry count.
                            'job_retry_count': self.crab_retry,
                           }
            if not needs_transfer:
                msg  = "File %s is marked as having been directly staged out"
                msg += " from the worker node to the permanent storage."
                msg  = msg % (filename)
                self.logger.info(msg)
                doc_new_info['state'] = 'done'
                doc_new_info['end_time'] = now
            ## Set the publication flag.
            publication_msg = None
            if file_type == 'output':
                publish = task_publish
                if publish and self.job_failed:
                    publication_msg  = "Disabling publication of output file %s,"
                    publication_msg += " because job is marked as failed."
                    publication_msg  = publication_msg % (filename)
                    publish = 0
                if publish and file_output_type != 'EDM':
                    publication_msg  = "Disabling publication of output file %s,"
                    publication_msg += " because it is not of EDM type."
                    publication_msg  = publication_msg % (filename)
                    publish = 0
            else:
                ## This is the log file, so obviously publication should be turned off.
                publish = 0
            ## What does ASO needs to do for this file (transfer and/or publication) is
            ## saved in this list for the only purpose of printing a better message later.
            aso_tasks = []
            if needs_transfer:
                aso_tasks.append("transfer")
            if publish:
                aso_tasks.append("publication")
            if not (needs_transfer or publish):
                ## This file doesn't need transfer nor publication, so we don't need to upload
                ## a document to ASO database.
                if publication_msg:
                    self.logger.info(publication_msg)
                msg  = "File %s doesn't need transfer nor publication."
                msg += " No need to inject a document to ASO."
                msg  = msg % (filename)
                self.logger.info(msg)
            else:
                ## This file needs transfer and/or publication. If a document (for the current
                ## job retry) is not yet in ASO database, we need to do the upload.
                needs_commit = True
                try:
                    doc = self.getDocByID(doc_id)
                    ## The document was already uploaded to ASO database. It could have been
                    ## uploaded from the WN in the current job retry or in a previous job retry,
                    ## or by the postjob in a previous job retry.
                    transfer_status = doc.get('state')
                    if not transfer_status:
                        # This means it is RDBMS database as we changed fields to match what they are :)
                        transfer_status = doc.get('transfer_state')
                    if doc.get('start_time') == self.aso_start_time or \
                       doc.get('start_time') == self.aso_start_timestamp:
                        ## The document was uploaded from the WN in the current job retry, so we don't
                        ## upload a new document. (If the transfer is done or ongoing, then of course we
                        ## don't want to re-inject the transfer request. OTOH, if the transfer has
                        ## failed, we don't want the postjob to retry it; instead the postjob will exit
                        ## and the whole job will be retried).
                        msg  = "LFN %s (id %s) is already in ASO database"
                        msg += " (it was injected from the worker node in the current job retry)"
                        msg += " and file transfer status is '%s'."
                        msg  = msg % (source_lfn, doc_id, transfer_status)
                        self.logger.info(msg)
                        needs_commit = False
                    else:
                        ## The document was uploaded in a previous job retry. This means that in the
                        ## current job retry the injection from the WN has failed or cmscp did a direct
                        ## stageout. We upload a new stageout request, unless the transfer is still
                        ## ongoing (which should actually not happen, unless the postjob for the
                        ## previous job retry didn't run).
                        msg  = "LFN %s (id %s) is already in ASO database (file transfer status is '%s'),"
                        msg += " but does not correspond to the current job retry."
                        msg  = msg % (source_lfn, doc_id, transfer_status)
                        if transfer_status in ['acquired', 'new', 'retry']:
                            msg += "\nFile transfer status is not terminal ('done', 'failed' or 'killed')."
                            msg += " Will not inject a new document for the current job retry."
                            self.logger.info(msg)
                            needs_commit = False
                        else:
                            msg += " Will inject a new %s request." % (' and '.join(aso_tasks))
                            self.logger.info(msg)
                            msg = "Previous document: %s" % (pprint.pformat(doc))
                            self.logger.debug(msg)
                except NotFound:
                    ## The document was not yet uploaded to ASO database (if this is the first job
                    ## retry, then either the upload from the WN failed, or cmscp did a direct
                    ## stageout and here we need to inject for publication only). In any case we
                    ## have to inject a new document.
                    msg  = "LFN %s (id %s) is not in ASO database."
                    msg += " Will inject a new %s request."
                    msg  = msg % (source_lfn, doc_id, ' and '.join(aso_tasks))
                    self.logger.info(msg)
                    if publication_msg:
                        self.logger.info(publication_msg)
                    input_dataset = str(self.job_ad['DESIRED_CMSDataset'])
                    if str(self.job_ad['DESIRED_CMSDataset']).lower() == 'undefined':
                        input_dataset = ''
                    primary_dataset = str(self.job_ad['CRAB_PrimaryDataset'])
                    if input_dataset:
                        input_dataset_or_primary_dataset = input_dataset
                    elif primary_dataset:
                        input_dataset_or_primary_dataset = '/'+primary_dataset # Adding the '/' until we fix ASO
                    else:
                        input_dataset_or_primary_dataset = '/'+'NotDefined' # Adding the '/' until we fix ASO
                    doc = {'_id': doc_id,
                           'inputdataset': input_dataset_or_primary_dataset,
                           'rest_host': str(self.job_ad['CRAB_RestHost']),
                           'rest_uri': str(self.job_ad['CRAB_RestURInoAPI']),
                           'lfn': source_lfn,
                           'source_lfn': source_lfn,
                           'destination_lfn': dest_lfn,
                           'checksums': checksums,
                           'user': str(self.job_ad['CRAB_UserHN']),
                           'group': group,
                           'role': role,
                           'dbs_url': str(self.job_ad['CRAB_DBSURL']),
                           'workflow': self.reqname,
                           'jobid': self.job_id,
                           'publication_state': 'not_published',
                           'publication_retry_count': [],
                           'type': file_type,
                           'publish': publish,
                          }
                    ## TODO: We do the following, only because that's what ASO does when a file has
                    ## been successfully transferred. But this modified LFN makes no sence when it
                    ## starts with /store/temp/user/, because the modified LFN is then
                    ## /store/user/<username>.<hash>/bla/blabla, i.e. it contains the <hash>, which
                    ## is never part of a destination LFN, but only of temp source LFNs.
                    ## Once ASO uses the source_lfn and the destination_lfn instead of only the lfn,
                    ## this should not be needed anymore.
                    if not needs_transfer:
                        doc['lfn'] = source_lfn.replace('/store/temp', '/store', 1)
                except Exception as ex:
                    msg = "Error loading document from ASO database: %s" % (str(ex))
                    try:
                        msg += "\n%s" % (traceback.format_exc())
                    except AttributeError:
                        msg += "\nTraceback unavailable."
                    self.logger.error(msg)
                    return False
                ## If after all we need to upload a new document to ASO database, let's do it.
                if needs_commit:
                    doc.update(doc_new_info)
                    msg = "ASO job description: %s" % (pprint.pformat(doc))
                    self.logger.info(msg)
                    commit_result_msg = self.updateOrInsertDoc(doc)
                    if 'error' in commit_result_msg:
                        msg = "Error injecting document to ASO database:\n%s" % (commit_result_msg)
                        self.logger.info(msg)
                        return False
                    ## If the upload succeds then record the timestamp in the fwjr (if not already present)
                    self.recordASOStartTime()
                ## Record all files for which we want the post-job to monitor their transfer.
                if needs_transfer:
                    doc_info = {'doc_id'     : doc_id,
                                'start_time' : doc.get('start_time')
                               }
                    docs_in_transfer.append(doc_info)

        self.logger.info("====== Finished to check uploads to ASO database.")

        return docs_in_transfer

    def getDocByID(self, doc_id):
        docInfo = self.server.get(self.rest_uri_file_user_transfers, data=encodeRequest({'subresource': 'getById', "id": doc_id}))
        if docInfo and len(docInfo[0]['result']) == 1:
            # Means that we have already a document in database!
            docInfo = oracleOutputMapping(docInfo)
            # Just to be 100% sure not to break after the mapping been added
            if not docInfo:
                self.found_doc_in_db = False
                raise NotFound('Document not found in database')
            # transfer_state and publication_state is a number in database. Lets change
            # it to lowercase until we will end up support for CouchDB.
            docInfo[0]['transfer_state'] = TRANSFERDB_STATES[docInfo[0]['transfer_state']].lower()
            docInfo[0]['publication_state'] = PUBLICATIONDB_STATES[docInfo[0]['publication_state']].lower()
            # Also change id to doc_id
            docInfo[0]['job_id'] = docInfo[0]['id']
            self.found_doc_in_db = True  # This is needed for further if there is a need to update doc info in DB
            return docInfo[0]
        else:
            self.found_doc_in_db = False
            raise NotFound('Document not found in database!')

    def updateOrInsertDoc(self, doc):
        """"""
        returnMsg = {}
        if not self.found_doc_in_db:
            # This means that it was not founded in DB and we will have to insert new doc
            newDoc = {'id': doc['_id'],
                      'username': doc['user'],
                      'taskname': doc['workflow'],
                      'start_time': self.aso_start_timestamp,
                      'destination': doc['destination'],
                      'destination_lfn': doc['destination_lfn'],
                      'source': doc['source'],
                      'source_lfn': doc['source_lfn'],
                      'filesize': doc['size'],
                      'publish': doc['publish'],
                      'transfer_state': doc['state'].upper(),
                      'publication_state': 'NEW' if doc['publish'] else 'NOT_REQUIRED',
                      'job_id': doc['jobid'],
                      'job_retry_count': doc['job_retry_count'],
                      'type': doc['type'],
                      'rest_host': doc['rest_host'],
                      'rest_uri': doc['rest_uri']}
            try:
                self.server.put(self.rest_uri_file_user_transfers, data=encodeRequest(newDoc))
            except HTTPException as hte:
                msg  = "Error uploading document to database."
                msg += " Transfer submission failed."
                msg += "\n%s" % (str(hte.headers))
                returnMsg['error'] = msg
        else:
            # This means it is in database and we need only update specific fields.
            newDoc = {'id': doc['id'],
                      'username': doc['username'],
                      'taskname': doc['taskname'],
                      'start_time': self.aso_start_timestamp,
                      'source': doc['source'],
                      'source_lfn': doc['source_lfn'],
                      'filesize': doc['filesize'],
                      'transfer_state': doc.get('state', 'NEW').upper(),
                      'publication_state': 'NEW' if doc['publish'] else 'NOT_REQUIRED',
                      'job_id': doc['jobid'],
                      'job_retry_count': doc['job_retry_count'],
                      'transfer_retry_count': 0,
                      'subresource': 'updateDoc'}
            try:
                self.server.post(self.rest_uri_file_user_transfers, data=encodeRequest(newDoc))
            except HTTPException as hte:
                msg  = "Error updating document in database."
                msg += " Transfer submission failed."
                msg += "\n%s" % (str(hte.headers))
                returnMsg['error'] = msg
        return returnMsg

    def build_failed_cache(self):
        aso_info = {
            "query_timestamp": time.time(),
            "query_succeded": False,
            "query_jobid": self.job_id,
            "results": {},
        }
        tmp_fname = "aso_status.%d.json" % (os.getpid())
        with open(tmp_fname, 'w') as fd:
            json.dump(aso_info, fd)
        os.rename(tmp_fname, "aso_status.json")

    def get_transfers_statuses(self):
        """
        Retrieve the status of all transfers from the cached file 'aso_status.json'
        or by querying an ASO database view if the file is more than 5 minutes old
        or if we injected a document after the file was last updated. Calls to
+       get_transfers_statuses_fallback() have been removed to not generate load
        on couch.
        """
        statuses = []
        query_view = False
        if not os.path.exists("aso_status.json"):
            query_view = True
        aso_info = {}
        if not query_view:
            query_view = True
            try:
                with open("aso_status.json") as fd:
                    aso_info = json.load(fd)
            except:
                msg = "Failed to load transfer cache."
                self.logger.exception(msg)
                raise TransferCacheLoadError(msg)
            last_query = aso_info.get("query_timestamp", 0)
            last_jobid = aso_info.get("query_jobid", "unknown")
            last_succeded = aso_info.get("query_succeded", True)
            # We can use the cached data if:
            # - It is from the last 15 minutes, AND
            # - It is from after we submitted the transfer.
            # Without the second condition, we run the risk of using the previous stageout
            # attempts results.
            if (time.time() - last_query < 900) and (last_query > self.aso_start_timestamp):
                self.logger.info("Using the cache since it is up to date (last_query=%s) and it is after we submitted the transfer (aso_start_timestamp=%s)", last_query, self.aso_start_timestamp)
                query_view = False
                if not last_succeded:
                    #no point in continuing if the last query failed. Just defer the PJ and retry later
                    msg = ("Not using info about transfer statuses from the trasnfer cache. "
                           "Deferring the postjob."
                           "PJ num %s failed to load the information from the DB and cache has not expired yet." % last_jobid)
                    raise TransferCacheLoadError(msg)
            for doc_info in self.docs_in_transfer:
                doc_id = doc_info['doc_id']
                if doc_id not in aso_info.get("results", {}):
                    self.logger.debug("Changing query_view back to true")
                    query_view = True
                    break
        if query_view:
            self.logger.debug("Querying ASO RDBMS database.")
            try:
                view_results = self.server.get(self.rest_uri_file_user_transfers, data=encodeRequest({'subresource': 'getTransferStatus',
                                                                                                      'username': str(self.job_ad['CRAB_UserHN']),
                                                                                                      'taskname': self.reqname}))
                view_results_dict = oracleOutputMapping(view_results, 'id')
                # There is so much noise values in aso_status.json file. So lets provide a new file structure.
                # We will not run ever for one task which can use also RDBMS and CouchDB
                # New Structure for view_results_dict is
                # {"DocumentHASHID": [{"id": "DocumentHASHID", "start_time": timestamp, "transfer_state": NUMBER!, "last_update": timestamp}]}
                for document in view_results_dict:
                    view_results_dict[document][0]['state'] = TRANSFERDB_STATES[view_results_dict[document][0]['transfer_state']].lower()
            except:
                msg = "Error while querying the RDBMS (Oracle) database."
                self.logger.exception(msg)
                self.build_failed_cache()
                raise TransferCacheLoadError(msg)
            aso_info = {
                "query_timestamp": time.time(),
                "query_succeded": True,
                "query_jobid": self.job_id,
                "results": view_results_dict,
            }
            tmp_fname = "aso_status.%d.json" % (os.getpid())
            with open(tmp_fname, 'w') as fd:
                json.dump(aso_info, fd)
            os.rename(tmp_fname, "aso_status.json")
        else:
            self.logger.debug("Using cached ASO results.")
        if not aso_info:
            raise TransferCacheLoadError("Unexpected error. aso_info is not set. Deferring postjob")
        for doc_info in self.docs_in_transfer:
            doc_id = doc_info['doc_id']
            if doc_id not in aso_info.get("results", {}):
                msg = "Document with id %s not found in the transfer cache. Deferring PJ." % doc_id
                raise TransferCacheLoadError(msg)                ## Not checking timestamps for oracle since we are not injecting from WN
            ## and it cannot happen that condor restarts screw up things.
            transfer_status = aso_info['results'][doc_id][0]['state']
            statuses.append(transfer_status)
        return statuses

    def load_transfer_document(self, doc_id):
        """
        Wrapper to load a document from CouchDB or RDBMS, catching exceptions.
        """
        doc = None
        try:
            doc = self.getDocByID(doc_id)
        except HTTPException as hte:
            msg = "Error retrieving document from ASO database for ID %s: %s" % (doc_id, str(hte.headers))
            self.logger.error(msg)
        except Exception:
            msg = "Error retrieving document from ASO database for ID %s" % (doc_id)
            self.logger.exception(msg)
        except NotFound as er:
            msg = "Document is not found in ASO RDBMS database for ID %s" % (doc_id)
            self.logger.warning(msg)
        return doc

    def get_transfers_statuses_fallback(self):
        """
        Retrieve the status of all transfers by loading the corresponding documents
        from ASO database and checking the 'state' field.
        """
        msg = "Querying transfers statuses using fallback method (i.e. loading each document)."
        self.logger.debug(msg)
        statuses = []
        for doc_info in self.docs_in_transfer:
            doc_id = doc_info['doc_id']
            doc = self.load_transfer_document(doc_id)
            status = doc['transfer_state'] if doc else 'unknown'
            statuses.append(status)
        return statuses

    def get_failures(self):
        """
        Retrieve failures bookeeping dictionary.
        """
        return self.failures