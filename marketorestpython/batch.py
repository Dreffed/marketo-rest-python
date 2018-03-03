import client
import time
import deque
from datetime import datetime
from marketorestpython.helper.http_lib import HttpLib
from marketorestpython.helper.exceptions import MarketoException

class MarketoClient(client.MarketoClient):
    create_queue = None     # used to save the jobs that need to be created
    start_queue = None      # used to store the jobs that need to be started
    monitor_queue = None    # used to store the jobs that have started and might be complete
    download_queue = None   # used to store the jobs that are ready to be downloaded

    def __init__(self, munchkin_id, client_id, client_secret, api_limit=None):
        super(MarketoClient, self).__init__(munchkin_id, client_id, client_secret, api_limit)

        # init the batch frameworks...
        

    def _bulk_extract(self):
        """ see http://developers.marketo.com/rest-api/bulk-extract/

        """


