import re
import time
import socket
import urllib
import datetime
import traceback

import classad
import htcondor

from httplib import HTTPException

from ServerUtilities import FEEDBACKMAIL
import TaskWorker.WorkerExceptions
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException
from ServerUtilities import insertJobIdSid

import HTCondorLocator
import HTCondorUtils

import WMCore.Database.CMSCouch as CMSCouch

import ApmonIf

WORKFLOW_RE = re.compile("[a-z0-9_]+")

class DagmanKiller(TaskAction):
    """
    Given a task name, kill the corresponding task in HTCondor.

    We do not actually "kill" the task off, but put the DAG on hold.
    """

    def executeInternal(self, apmon, *args, **kwargs):
        #Marco: I guess these value errors only happens for development instances
        if 'task' not in kwargs:
            raise ValueError("No task specified.")
        self.task = kwargs['task']
        self.killAllFlag = len(self.task['kill_ids']) == 0
        if 'tm_taskname' not in self.task:
            raise ValueError("No taskname specified")
        self.workflow = self.task['tm_taskname']
        if 'user_proxy' not in self.task:
            raise ValueError("No proxy provided")
        self.proxy = self.task['user_proxy']

        self.logger.info("About to kill workflow: %s." % self.workflow)

        self.workflow = str(self.workflow)
        if not WORKFLOW_RE.match(self.workflow):
            raise Exception("Invalid workflow name.")

        # Query HTCondor for information about running jobs and update Dashboard appropriately
        if self.task['tm_collector']:
            self.backendurls['htcondorPool'] = self.task['tm_collector']
        loc = HTCondorLocator.HTCondorLocator(self.backendurls)

        address = ""
        try:
            self.schedd, address = loc.getScheddObjNew(self.task['tm_schedd'])
        except Exception as exp:
            msg  = "The CRAB server backend was not able to contact the Grid scheduler."
            msg += " Please try again later."
            msg += " If the error persists send an e-mail to %s." % (FEEDBACKMAIL)
            msg += " Message from the scheduler: %s" % (str(exp))
            self.logger.exception("%s: %s" % (self.workflow, msg))
            raise TaskWorkerException(msg)

        ad = classad.ClassAd()
        ad['foo'] = self.task['kill_ids']
        try:
            hostname = socket.getfqdn()
        except:
            hostname = ''

        if not self.killAllFlag:
            const = "CRAB_ReqName =?= %s && member(CRAB_Id, %s) || member(CRAB_ParentId, %s)" % (HTCondorUtils.quote(self.workflow), ad.lookup("foo").__repr__(), ad.lookup("foo").__repr__())
        else:
            const = 'CRAB_ReqName =?= %s && TaskType=?="Job"' % HTCondorUtils.quote(self.workflow)
        try:
            for ad in list(self.schedd.xquery(const, ['CRAB_Id', 'CRAB_Retry'])):
                if ('CRAB_Id' not in ad) or ('CRAB_Retry' not in ad):
                    continue
                jobid = str(ad.eval('CRAB_Id'))
                jobretry = str(ad.eval('CRAB_Retry'))
                jinfo = {'broker': hostname,
                         'bossId': jobid,
                         'StatusValue': 'killed',
                        }
                insertJobIdSid(jinfo, jobid, self.workflow, jobretry)
                self.logger.info("Sending kill info to Dashboard: %s" % str(jinfo))
                apmon.sendToML(jinfo)
        except:
            self.logger.exception("Failed to notify Dashboard of job kills") #warning

        # Note that we can not send kills for jobs not in queue at this time; we'll need the
        # DAG FINAL node to be fixed and the node status to include retry number.
        if self.killAllFlag:
            return self.killAll(const)
        else:
            return self.killJobs(self.task['kill_ids'], const)


    def killJobs(self, ids, const):
        ad = classad.ClassAd()
        ad['foo'] = ids
        with HTCondorUtils.AuthenticatedSubprocess(self.proxy, logger=self.logger) as (parent, rpipe):
            if not parent:
                self.schedd.act(htcondor.JobAction.Remove, const)
        try:
            results = rpipe.read()
        except EOFError:
            results = "Timeout executing condor remove command"
        if results != "OK":
            msg  = "The CRAB server backend was not able to kill these jobs %s," % (ids)
            msg += " because the Grid scheduler answered with an error."
            msg += " This is probably a temporary glitch. Please try again later."
            msg += " If the error persists send an e-mail to %s." % (FEEDBACKMAIL)
            msg += " Error reason: %s" % (results)
            raise TaskWorkerException(msg)


    def killAll(self, jobConst):

        # We need to keep ROOT DAG in hold until periodic remove kicks in.
        # See DagmanSubmitter.py#L390 (dagAd["PeriodicRemove"])
        # This is needed in case user wants to resubmit.
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= %s" % HTCondorUtils.quote(self.workflow)

        # Holding DAG job does not mean that it will remove all jobs
        # and this must be done separately
        # --------------------------------------
        # From HTCondor documentation
        # http://research.cs.wisc.edu/htcondor/manual/v8.3/2_10DAGMan_Applications.html#SECTION003107000000000000000
        # --------------------------------------
        # After placing the condor_dagman job on hold, no new node jobs will be submitted,
        # and no PRE or POST scripts will be run. Any node jobs already in the HTCondor queue
        # will continue undisturbed. If the condor_dagman job is left on hold, it will remain
        # in the HTCondor queue after all of the currently running node jobs are finished.
        # --------------------------------------
        # TODO: Remove jobConst query when htcondor ticket is solved
        # https://htcondor-wiki.cs.wisc.edu/index.cgi/tktview?tn=5175

        with HTCondorUtils.AuthenticatedSubprocess(self.proxy) as (parent, rpipe):
            if not parent:
                with self.schedd.transaction() as dummytsc:
                    self.schedd.act(htcondor.JobAction.Hold, rootConst)
                    self.schedd.act(htcondor.JobAction.Remove, jobConst)
        results = rpipe.read()
        if results != "OK":
            msg  = "The CRAB server backend was not able to kill the task,"
            msg += " because the Grid scheduler answered with an error."
            msg += " This is probably a temporary glitch. Please try again later."
            msg += " If the error persists send an e-mail to %s." % (FEEDBACKMAIL)
            msg += " Error reason: %s" % (results)
            raise TaskWorkerException(msg)


    def execute(self, *args, **kwargs):
        """
        The execute method of the DagmanKiller class.
        """
        apmon = ApmonIf.ApmonIf()
        try:
            self.executeInternal(apmon, *args, **kwargs)
            try:
                ## AndresT: If a task was in FAILED status before the kill, then the new status
                ## after killing some jobs should be FAILED again, not SUBMITTED. However, in
                ## the long term we would like to introduce a final node in the DAG, and I think
                ## the idea would be that the final node will put the task status into FAILED or
                ## COMPLETED (in the TaskDB) once all jobs are finished. In that case I think
                ## also the status method from HTCondorDataWorkflow would not have to return any
                ## adhoc task status anymore (it would just return what is in the TaskDB) and
                ## that also means that FAILED task status would only be a terminal status that
                ## I guess should not accept a kill (because it doesn't make sense to kill a
                ## task for which all jobs have already finished -successfully or not-).
                configreq = {'subresource': 'state',
                             'workflow': kwargs['task']['tm_taskname'],
                             'status': 'KILLED' if self.killAllFlag else 'SUBMITTED'}
                self.logger.debug("Setting the task as successfully killed with %s" % (str(configreq)))
                self.server.post(self.resturi, data = urllib.urlencode(configreq))
            except HTTPException as hte:
                self.logger.error(hte.headers)
                msg  = "The CRAB server successfully killed the task,"
                msg += " but was unable to update the task status to %s in the database." % (configreq['status'])
                msg += " This should be a harmless (temporary) error."
                raise TaskWorkerException(msg)
        finally:
            apmon.free()

