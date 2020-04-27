
import os
from RESTInteractions import HTTPRequests
from ServerUtilities import encodeRequest
from TransferInterface import chunks, mark_failed, CRABDataInjector
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
import threading


def submit(trans_tuple, job_data, log, direct=False):
    """Manage threads for transfers submission through Rucio

    :param trans_tuple: ordered list of needed xfer info (transfers, to_submit_columns)
    :type trans_tuple: tuple
    :param job_data: general CRAB job metadata
    :type job_data: dict
    :param log: log object
    :type log: logging
    :param direct: job output stored on temp or directly, defaults to False
    :param direct: bool, optional
    """
    threadLock = threading.Lock()
    threads = []
    to_update = []
    
    toTrans = trans_tuple
    proxy = job_data['proxy']
    rest_filetransfers = job_data['rest']
    user = job_data['username']
    destination = job_data['destination']
    taskname = job_data['taskname']

    try:
        phedex = PhEDEx(responseType='xml',
                        httpDict={'key': proxy,
                                  'cert': proxy,
                                  'pycurl': True})
    except Exception:
        log.exception('PhEDEx exception.')
        return

    # Split threads by source RSEs
    sources = set([x['source'] for x in toTrans])
    #list(set([x[columns.index('source')] for x in toTrans]))

    os.environ["X509_CERT_DIR"] = os.getcwd()
    log.info("Connection to %s with proxy in:\n %s" % (rest_filetransfers,proxy))
    oracleDB = HTTPRequests(rest_filetransfers,
                            proxy,
                            proxy)
                            #verbose=True)

    # mapping lfn <--> pfn
    for source in sources:
        ids = [ x["id"] for x in toTrans if x['source'] == source ]
        src_lfns = [x['source_lfn'] for x in toTrans if x['source'] == source]
        dst_lfns = [x['destination_lfn'] for x in toTrans if x['source'] == source]

        sorted_source_pfns = []
        sorted_dest_lfns = []
        sorted_dest_pfns = []

        # workaround for phedex.getPFN issue --> shuffling output order w.r.t. the list in input
        # split files to be transferred from a common source into chucks of 10 files and than ask phedex for lfn2pfn
        try:
            for chunk in chunks(src_lfns, 10):
                unsorted_source_pfns = [[k[1], str(x)] for k, x in phedex.getPFN(source, chunk).items()]
                for order_lfn in chunk:
                    for lfn, pfn in unsorted_source_pfns:
                        if order_lfn == lfn:
                            sorted_source_pfns.append(pfn)
                            break

            for chunk in chunks(dst_lfns, 10):
                unsorted_dest_pfns = [[k[1], str(x)] for k, x in phedex.getPFN(toTrans[0][4], chunk).items()]
                for order_lfn in chunk:
                    for lfn, pfn in unsorted_dest_pfns:
                        if order_lfn == lfn:
                            sorted_dest_pfns.append(pfn)
                            sorted_dest_lfns.append(lfn)
                            break

        except Exception as ex:
            log.error("Failed to map lfns to pfns: %s", ex)
            mark_failed(ids, ["Failed to map lfn to pfn: " + str(ex) for _ in ids], oracleDB)

        source_pfns = sorted_source_pfns
        dest_lfns = sorted_dest_lfns

        # saving file sizes and checksums
        filesizes = [x['filesize'] for x in toTrans if x['source'] == source]
        checksums = [x['checksums'] for x in toTrans if x['source'] == source]
        pubnames = [x['publishname'] for x in toTrans if x['source'] == source]

        # ordered list of replicas information
        #jobs = zip(source_pfns, dest_lfns, ids, checksums, filesizes, pubnames)

        jobs = {
            "source_pfns": source_pfns,
            "dest_lfns": dest_lfns,
            "ids": ids,
            "checksum": checksums,
            "filesizes": filesizes,
            "pubnames": pubnames,
            "destination": destination,
            "source": source,
            "taskname": taskname,
            "user": user
        }

        #job_columns = ['source_pfns', 'dest_lfns', 'ids', 'checksums', 'filesizes', 'pubnames']

        # ordered list of transfers details
        #tx_from_source = [[job, source, taskname, user, destination] for job in jobs]
        #tx_columns = ['job', 'source', 'taskname', 'user', 'destination']

        # split submission process in chunks of max 200 files
        #for files in chunks(tx_from_source, 200):
        #    if not direct:
        log.info("Submitting %s jobs", len(jobs["ids"]))
        if not direct:
        submit_thread( 
            log,
            jobs,
            proxy,
            to_update
        )
        elif direct:
            log.info("Registering direct stageout: %s", files)
            submit_thread(
                log,
                jobs,
                proxy,
                to_update,
                direct=True
            )

    if len(to_update) == 0:
        return False
    # update statuses in oracle table as per threads result
    for fileDoc in to_update:
        try:
            log.debug("%s/filetransfers?%s" % (rest_filetransfers, encodeRequest(fileDoc)))
            oracleDB.post('/filetransfers', data=encodeRequest(fileDoc))
            log.info("Marked submitted %s files" % (fileDoc['list_of_ids']))
        except Exception:
            log.exception('Failed to mark files as submitted on DBs')

    return True


class submit_thread(threading.Thread):
    """

    """
    def __init__(self, threadLock, log, files, proxy, toUpdate, direct=False):

    def submit_job(log,files,proxy,toUpdate,direct=False):

        log.info("Submitting %s transfers to Rucio server" % len(files["ids"]))

        taskname = files['taskname']
        scope = 'user.' + files['username']

        try:
            os.environ["X509_USER_PROXY"] = proxy
            log.info("Initializing Rucio client for %s", taskname)
            crabInj = CRABDataInjector(taskname,
                                       files['destination'],
                                       account= files['username'],
                                       scope=scope,
                                       auth_type='x509_proxy')

            # Check if the corresponding dataset is already present in RUCIO
            # In case is missing try to create it with the corresponding rule
            self.log.info("Checking for current dataset")
            crabInj.cli.get_did(scope, taskname)
        except Exception as ex:
            self.log.warn("Failed to find dataset %s:%s On Rucio server: %s", "user.%s" % username, taskname, ex)
            try:
                crabInj.add_dataset()
            except Exception as ex:
                self.log.error("Failed to create dataset %s:%s on Rucio server: %s", "user.%s" % taskname, taskname, ex)
                self.threadLock.release()
                return

        try:
            # save the lfn of files directly staged by CRAB if any
            direct_files = []
            if os.path.exists('task_process/transfers/registered_direct_files.txt'):
                with open("task_process/transfers/registered_direct_files.txt", "r") as list_file:
                    direct_files = [x.split('\n')[0] for x in list_file.readlines()]
            
            # get needed information from file dict. Discarding direct staged files
            dest_lfns = []
            source_pfns = []
            sizes = []
            checksums = []

            for index, lfn in files["dest_lfns"].enumerate():
                if lfn not in direct_files:
                    dest_lfns.append(lfn)
                    source_pfns.append(f["source_pfns"][index])
                    sizes.append(f["filesizes"][index])
                    checksums.append(f["checksums"][index])

            log.info(files["source"]+"_Temp")
            log.info(dest_lfns)
            log.info(source_pfns)

            if direct:
                try:
                    log.info("Registering direct files")
                    crabInj.register_crab_replicas(files['destination'], dest_lfns, sizes, None)
                    crabInj.attach_files(dest_lfns, taskname)
                    with open("task_process/transfers/registered_direct_files.txt", "a+") as list_file:
                        for dest_lfn in dest_lfns:
                            list_file.write("%s\n" % dest_lfn)
                    self.log.info("Registered {0} direct files.".format(len(dest_lfns)))
                    self.log.debug("Registered direct files: {0}".format(dest_lfns))
                    self.threadLock.release()
                    return
                except Exception:
                    self.log.exception("Failed to register direct files.")
                    self.threadLock.release()
                    return

            # Eventullay registering files in Rucio Temp RSE
            self.log.info("Registering temp file")
            crabInj.register_temp_replicas(files["source"]+"_Temp", dest_lfns, source_pfns, sizes, checksums)
            #crabInj.register_temp_replicas(self.source+"_Temp", dest_lfns, source_pfns, sizes, None)
            crabInj.attach_files(dest_lfns, taskname)

        except Exception:
            self.log.exception("Failed to register replicas")
            self.threadLock.release()
            return

        # update statuses on OracleDB
        try:
            fileDoc = dict()
            fileDoc['asoworker'] = 'rucio'
            fileDoc['subresource'] = 'updateTransfers'
            fileDoc['list_of_ids'] = [x['ids'] for x in self.files]
            fileDoc['list_of_transfer_state'] = ["SUBMITTED" for _ in self.files]
            fileDoc['list_of_fts_instance'] = ['https://fts3.cern.ch:8446/' for _ in self.job]
            fileDoc['list_of_fts_id'] = ['NA' for _ in self.job]

            self.log.info("Marking submitted %s files" % (len(fileDoc['list_of_ids'])))
            self.toUpdate.append(fileDoc)
        except Exception:
            self.log.exception("Failed to update status in oracle")
            return
