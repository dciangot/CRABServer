from __future__ import print_function

import os
import sys
import json
import uuid
import commands
import unittest
import datetime
import tempfile
from shutil import move

from ServerUtilities import getLock
from TaskWorker.Actions.PostJob import G_ERROR_SUMMARY_FILE_NAME, G_FJR_PARSE_RESULTS_FILE_NAME, PostJob


class NotFound(Exception):
    """Not Found is raised only if there is no document found in RDBMS.
       This makes PostJob to submit new transfer request to database."""
    pass

DEFER_NUM = -1

def first_pj_execution():
    return DEFER_NUM == 0


def prepareErrorSummary(logger, fsummary, job_id, crab_retry):
    """Parse the job_fjr file corresponding to the current PostJob. If an error
       message is found, it is inserted into the error_summary.json file
    """

    ## The job_id and crab_retry variables in PostJob are integers, while here we
    ## mostly use them as strings.
    job_id = str(job_id)
    crab_retry = str(crab_retry)

    error_summary = []
    error_summary_changed = False
    fjr_file_name = "job_fjr." + job_id + "." + crab_retry + ".json"

    with open(fjr_file_name) as frep:
        try:
            rep = None
            exit_code = -1
            rep = json.load(frep)
            if not 'exitCode' in rep:
                raise Exception("'exitCode' key not found in the report")
            exit_code = rep['exitCode']
            if not 'exitMsg'  in rep:
                raise Exception("'exitMsg' key not found in the report")
            exit_msg = rep['exitMsg']
            if not 'steps'    in rep:
                raise Exception("'steps' key not found in the report")
            if not 'cmsRun'   in rep['steps']:
                raise Exception("'cmsRun' key not found in report['steps']")
            if not 'errors'   in rep['steps']['cmsRun']:
                raise Exception("'errors' key not found in report['steps']['cmsRun']")
            if rep['steps']['cmsRun']['errors']:
                ## If there are errors in the job report, they come from the job execution. This
                ## is the error we want to report to the user, so write it to the error summary.
                if len(rep['steps']['cmsRun']['errors']) != 1:
                    #this should never happen because the report has just one step, but just in case print a message
                    logger.info("More than one error found in report['steps']['cmsRun']['errors']. Just considering the first one.")
                msg  = "Updating error summary for jobid %s retry %s with following information:" % (job_id, crab_retry)
                msg += "\n'exit code' = %s" % (exit_code)
                msg += "\n'exit message' = %s" % (exit_msg)
                msg += "\n'error message' = %s" % (rep['steps']['cmsRun']['errors'][0])
                logger.info(msg)
                error_summary = [exit_code, exit_msg, rep['steps']['cmsRun']['errors'][0]]
                error_summary_changed = True
            else:
                ## If there are no errors in the job report, but there is an exit code and exit
                ## message from the job (not post-job), we want to report them to the user only
                ## in case we know this is the terminal exit code and exit message. And this is
                ## the case if the exit code is not 0. Even a post-job exit code != 0 can be
                ## added later to the job report, the job exit code takes precedence, so we can
                ## already write it to the error summary.
                if exit_code != 0:
                    msg  = "Updating error summary for jobid %s retry %s with following information:" % (job_id, crab_retry)
                    msg += "\n'exit code' = %s" % (exit_code)
                    msg += "\n'exit message' = %s" % (exit_msg)
                    logger.info(msg)
                    error_summary = [exit_code, exit_msg, {}]
                    error_summary_changed = True
                else:
                    ## In case the job exit code is 0, we still have to check if there is an exit
                    ## code from post-job. If there is a post-job exit code != 0, write it to the
                    ## error summary; otherwise write the exit code 0 and exit message from the job
                    ## (the message should be "OK").
                    postjob_exit_code = rep.get('postjob', {}).get('exitCode', -1)
                    postjob_exit_msg  = rep.get('postjob', {}).get('exitMsg', "No post-job error message available.")
                    if postjob_exit_code != 0:
                        ## Use exit code 90000 as a general exit code for failures in the post-processing step.
                        ## The 'crab status' error summary should not show this error code,
                        ## but replace it with the generic message "failed in post-processing".
                        msg  = "Updating error summary for jobid %s retry %s with following information:" % (job_id, crab_retry)
                        msg += "\n'exit code' = 90000 ('Post-processing failed')"
                        msg += "\n'exit message' = %s" % (postjob_exit_msg)
                        logger.info(msg)
                        error_summary = [90000, postjob_exit_msg, {}]
                    else:
                        msg  = "Updating error summary for jobid %s retry %s with following information:" % (job_id, crab_retry)
                        msg += "\n'exit code' = %s" % (exit_code)
                        msg += "\n'exit message' = %s" % (exit_msg)
                        logger.info(msg)
                        error_summary = [exit_code, exit_msg, {}]
                    error_summary_changed = True
        except Exception as ex:
            logger.info(str(ex))
            ## Write to the error summary that the job report is not valid or has no error
            ## message
            if not rep:
                exit_msg = 'Invalid framework job report. The framework job report exists, but it cannot be loaded.'
            else:
                exit_msg = rep['exitMsg'] if 'exitMsg' in rep else 'The framework job report could be loaded, but no error message was found there.'
            msg  = "Updating error summary for jobid %s retry %s with following information:" % (job_id, crab_retry)
            msg += "\n'exit code' = %s" % (exit_code)
            msg += "\n'exit message' = %s" % (exit_msg)
            logger.info(msg)
            error_summary = [exit_code, exit_msg, {}]
            error_summary_changed = True

    # Write the fjr report summary of this postjob to a file which task_process reads incrementally
    if error_summary_changed and os.path.isfile("/etc/enable_task_daemon"):
        with getLock(G_FJR_PARSE_RESULTS_FILE_NAME):
            with open(G_FJR_PARSE_RESULTS_FILE_NAME, "a+") as fjr_parse_results:
                fjr_parse_results.write(json.dumps({job_id : {crab_retry : error_summary}}) + "\n")

    # Read, update and re-write the error_summary.json file
    try:
        error_summary_old_content = {}
        if os.stat(G_ERROR_SUMMARY_FILE_NAME).st_size != 0:
            fsummary.seek(0)
            error_summary_old_content = json.load(fsummary)
    except (IOError, ValueError):
        ## There is nothing to do if the error_summary file doesn't exist or is invalid.
        ## Just recreate it.
        logger.info("File %s is empty, wrong or does not exist. Will create a new file." % (G_ERROR_SUMMARY_FILE_NAME))
    error_summary_new_content = error_summary_old_content
    error_summary_new_content[job_id] = {crab_retry : error_summary}

    ## If we have updated the error summary, write it to the json file.
    ## Use a temporary file and rename to avoid concurrent writing of the file.
    if error_summary_changed:
        logger.debug("Writing error summary file")
        tempFilename = (G_ERROR_SUMMARY_FILE_NAME + ".%s") % os.getpid()
        with open(tempFilename, "w") as tempFile:
            json.dump(error_summary_new_content, tempFile)
        move(tempFilename, G_ERROR_SUMMARY_FILE_NAME)
        logger.debug("Written error summary file")


class PermanentStageoutError(RuntimeError):
    pass


class RecoverableStageoutError(RuntimeError):
    pass


class TransferCacheLoadError(RuntimeError):
    pass


def get_file_index(file_name, output_files):
    """
    Get the index location of the given file name within the list of output files
    dict infos from the job report.
    """
    for i, outfile in enumerate(output_files):
        if ('pfn' not in outfile):
            continue
        json_pfn = os.path.split(outfile['pfn'])[-1]
        pfn = os.path.split(file_name)[-1]
        left_piece, fileid = pfn.rsplit("_", 1)
        right_piece = fileid.split(".", 1)[-1]
        pfn = left_piece + "." + right_piece
        if pfn == json_pfn:
            return i
    return None


class testServer(unittest.TestCase):
    def generateJobJson(self, sourceSite = 'srm.unl.edu'):
        return {"steps" : {
            "cmsRun" : { "input" : {},
              "output":
                {"outmod1" :
                    [ { "output_module_class": "PoolOutputModule",
                        "input": ["/test/input2",
                                   "/test/input2"
                                  ],
                        "events": 200,
                        "size": 100,
                        "SEName": sourceSite,
                        "runs": { 1: [1, 2, 3],
                                   2: [2, 3, 4]},
                      }]}}}}

    def setUp(self):
        self.postjob = PostJob()
        #self.job = ASOServerJob()
        #status, crab_retry, max_retries, restinstance, resturl, reqname, id,
        #outputdata, job_sw, async_dest, source_dir, dest_dir, *filenames
        self.full_args = ['0', 0, 2, 'restinstance', 'resturl',
                          'reqname', 1234, 'outputdata', 'sw', 'T2_US_Vanderbilt']
        self.json_name = "jobReport.json.%s" % (self.full_args[6])
        open(self.json_name, 'w').write(json.dumps(self.generateJobJson()))

    def makeTempFile(self, size, pfn):
        fh, path = tempfile.mkstemp()
        try:
            inputString = "CRAB3POSTJOBUNITTEST"
            os.write(fh, (inputString * ((size/len(inputString))+1))[:size])
            os.close(fh)
            cmd = "env -u LD_LIBRAY_PATH lcg-cp -b -D srmv2 -v file://%s %s" % (path, pfn)
            print(cmd)
            status, res = commands.getstatusoutput(cmd)
            if status:
                exmsg = "Couldn't make file: %s" % (res)
                raise RuntimeError(exmsg)
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def getLevelOneDir(self):
        return datetime.datetime.now().strftime("%Y-%m")

    def getLevelTwoDir(self):
        return datetime.datetime.now().strftime("%d%p")

    def getUniqueFilename(self):
        return "%s-postjob.txt" % (uuid.uuid4())

    def testNonexistent(self):
        self.full_args.extend(['/store/temp/user/meloam/CRAB3-UNITTEST-NONEXISTENT/b/c/',
                               '/store/user/meloam/CRAB3-UNITTEST-NONEXISTENT/b/c',
                               self.getUniqueFilename()])
        self.assertNotEqual(self.postjob.execute(*self.full_args), 0)

    source_prefix = "srm://dcache07.unl.edu:8443/srm/v2/server?SFN=/mnt/hadoop/user/uscms01/pnfs/unl.edu/data4/cms"

    def testExistent(self):
        source_dir  = "/store/temp/user/meloam/CRAB3-UnitTest/%s/%s" % \
                        (self.getLevelOneDir(), self.getLevelTwoDir())
        source_file = self.getUniqueFilename()
        source_lfn  = "%s/%s" % (source_dir, source_file)
        dest_dir = source_dir.replace("temp/user", "user")
        self.makeTempFile(200, "%s/%s" %(self.source_prefix, source_lfn))
        self.full_args.extend([source_dir, dest_dir, source_file])
        self.assertEqual(self.postjob.execute(*self.full_args), 0)

    def tearDown(self):
        if os.path.exists(self.json_name):
            os.unlink(self.json_name)

