from TransferInterface import Transfers

class FTS_Transfers(Transfers):
    def __init__(self, config, logWarning, logDebug):
        super().__init__(config, logWarning, logDebug)
        fts = HTTPRequests(FTS_ENDPOINT.split("https://")[1],
                           proxy,
                           proxy)

        logging.info("using user's proxy from %s", proxy)
        ftsContext = fts3.Context(FTS_ENDPOINT, proxy, proxy, verify=True)
        logging.info("Delegating proxy to FTS...")
        delegationId = fts3.delegate(ftsContext, lifetime=timedelta(hours=48), delegate_when_lifetime_lt=timedelta(hours=24), force=False)
        delegationStatus = fts.get("delegation/"+delegationId)
        logging.info("Delegated proxy valid until %s", delegationStatus[0]['termination_time'])

        with open("task_process/transfers.txt") as _list:
            _data = _list.readlines()[0]
            try:
                doc = json.loads(_data)
                username = doc["username"]
                taskname = doc["taskname"]
                destination = doc["destination"]
            except Exception as ex:
                msg = "Username gathering failed with\n%s" % str(ex)
                logging.warn(msg)
                raise ex

        try:
            logging.info("Initializing Rucio client")
            os.environ["X509_USER_PROXY"] = proxy
            logging.info("Initializing Rucio client for %s", taskname)
            rucioClient = CRABDataInjector(taskname,
                                        destination,
                                        account=username,
                                        scope="user."+username,
                                        auth_type='x509_proxy')
        except Exception as exc:
            msg = "Rucio initialization failed with\n%s" % str(exc)
            logging.warn(msg)
            raise exc

    def register():
        """
        """
    def monitor():
        """
        """
    def update():
        """
        """

def algorithm():
    do_transfers = FTS_Transfers(config, logWarning, logDebug)

    do_transers.register()

    do_transfers.monitor()

    do_transfers.update()


    return

if __name__ == "__main__":
    try:
        algorithm()
    except Exception:
        logging.exception("error during main loop")
    logging.info("transfer_inject.py exiting")