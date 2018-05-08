import time
import os
import pickle

from collections import deque
from datetime import date, datetime, timezone, timedelta
from dateutil.tz import tzoffset
from dateutil import tz

from marketorestpython.client import MarketoClient
from marketorestpython.helper.http_lib import HttpLib
from marketorestpython.helper.exceptions import MarketoException

xstr = lambda s: s or ''

class MarketoClientBatch(MarketoClient):
    def __init__(self, munchkin_id, client_id, client_secret, 
                api_limit=None, api_size_limit=None, api_days_max=None):
        super(MarketoClientBatch, self).__init__(munchkin_id, client_id, client_secret, api_limit)

        # init the batch frameworks...
        self.API_DAYS_MAX = api_days_max
        self.API_SIZE_LIMIT = api_size_limit
        self.API_MAX_QUEUE = 9
        self.API_QUEUE_COUNT = self.API_MAX_QUEUE
        self.data = {}
        self._name = self.__class__.__name__ #name
        self._pickleName = "{}_{}_batch.pickle".format(self._name, munchkin_id)

    def _api_call(self, method, endpoint, *args, **kwargs):
        request = HttpLib()
        print('Request:{}\n\t{}\n\t{}'.format(method, endpoint, *args))
        result = getattr(request, method)(endpoint, *args, **kwargs)
        self.API_CALLS_MADE += 1
        if self.API_LIMIT and self.API_CALLS_MADE >= self.API_LIMIT:
            raise Exception({'message': '# of API Calls exceeded the limit as specified in the Python script: '
                                        + str(self.API_LIMIT), 'code': '416'})
        return result

    def _download_file(self, url, file_name):
        import requests
        
        chunk_size = 8096

        response = requests.get(url, stream=True)
        bytes_transferred = 0
        with open(file_name, 'wb') as file_handle:
            for chunk in response.iter_content(chunk_size):
                file_handle.write(chunk)
                bytes_transferred += len(chunk)
        return bytes_transferred

    def execute(self, method, *args, **kargs):
        ''' 
        override the underlying method as we only need to run the batch calls here, and interface with the
        necessary queues
        '''

        result = None

        '''
            max 10 rechecks
        '''
        for _ in range(0,10):
            try:

                method_map={
                    'create_bulk_extract': self.create_bulk_extract,
                    'get_bulk_jobs': self.get_bulk_jobs,
                    'start_bulk_job': self.start_bulk_job,
                    'status_bulk_job': self.status_bulk_job,
                    'retrieve_bulk_job': self.retrieve_bulk_job,
                    'cancel_bulk_job': self.cancel_bulk_job
                }
                result = method_map[method](*args,**kargs)
            except MarketoException as e:
                '''
                601 -> auth token not valid
                602 -> auth token expired
                '''
                if e.code in ['601', '602']:
                    self.authenticate()
                    continue
                else:
                    raise Exception({'message':e.message, 'code':e.code})
            break
        return result

    def _get_isodate(self, dt, end_dt = None):
        '''
        returns the date range as start, end objects for yesterday
        '''
        if end_dt is None:
            end_dt = dt

        start_at = datetime.combine(dt, datetime.min.time()).replace(microsecond=0)
        start_at = start_at.replace(tzinfo=tz.tzlocal())
        start_at = start_at.isoformat()

        end_at = datetime.combine(end_dt, datetime.max.time()).replace(microsecond=0)
        end_at = end_at.replace(tzinfo=tz.tzlocal())
        end_at = end_at.isoformat()

        return {'startAt': start_at, 'endAt': end_at}

    def _load_pickle_data(self):
        if os.path.exists(self._pickleName):
            print('Loading Saved Data... [%s]' % self._pickleName)
            with open(self._pickleName, 'rb') as handle:
                self.data = pickle.load(handle)

    def _save_pickle_data(self):
        print('Saving Data... [%s]' % self._pickleName)
        with open(self._pickleName, 'wb') as handle:
            pickle.dump(self.data, handle)

    # --------- batch runner ---------
    def run_batch(self, start_dt=None, end_dt=None, table=None, fields=None):
        '''
        This will create the jobs, then start it.
        It will also query the job queue for running, completed and failed jobs
        For jobs that are completed it will download them only if the file is not present

        '''
        if end_dt < start_dt:
            end_dt, start_dt = start_dt, end_dt

        if table is None:
            table = 'leads'

        if fields is None:
            if table == 'leads':
                resp = super().describe()

                field_md = []
                fields = []
                for r in resp:
                    field = {}
                    field['name'] = r['displayName']
                    field['api'] = r['rest']['name']
                    field['type'] = r['dataType']
                    if 'length' in r:
                        field['length'] = r['length']
                    field_md.append(field)
                    if field['api'][-3:] != '__c':
                        fields.append(field['api'])
                
        batch_label = '{}:{}:{}'.format(table.lower(), start_dt.date(), end_dt.date())

        if self.API_DAYS_MAX is None:
            self.API_DAYS_MAX = 30
        
        # set up the data...
        if 'requests' not in self.data:
            self.data['requests'] = {}

        if 'batches' not in self.data:
            self.data['batches'] = {}

        if batch_label in self.data['batches']:
            # we've already requested this batch
            raise ValueError("{} has already been processed!".format(batch_label))

        self.data['batches'][batch_label] = {
            'requsted': datetime.now,
            'table': table, 
            'start': start_dt,
            'end': end_dt
        }

        # use the step date to batch up the dump into months
        step_dt = start_dt
        while step_dt < end_dt:
            # step_dt is the end of the month or end_dt
            add_days = self.API_DAYS_MAX

            step_dt = start_dt + timedelta(add_days)   
            if step_dt > end_dt:
                step_dt = end_dt

            step_label = '{}:{}:{}'.format(table.lower(), start_dt.strftime("%Y-%m-%d"), step_dt.strftime("%Y-%m-%d"))

            if step_label in self.data['batches']:
                print('Skipping already requested....')
                continue

            # now to create the job...
            filter = {
                "createdAt": self._get_isodate(start_dt, step_dt)
            }    
            self.data['batches'][step_label] = {
                'requested': datetime.now,
                'start': start_dt,
                'end': step_dt,
                'filter': filter
            }
            print(filter)
            
            response = self.execute('create_bulk_extract', table=table, filter=filter, fields=fields)

            # we have a response...
            for result in response:
                # get the exportID, and save this...
                '''
                    {
                        "exportId": "ce45a7a1-f19d-4ce2-882c-a3c795940a7d",
                        "status": "Created",
                        "createdAt": "2017-01-21T11:47:30-08:00",
                        "queuedAt": "2017-01-21T11:48:30-08:00",
                        "format": "CSV",
                    }                
                '''
                export_id = result['exportId']
                self.data['batches'][step_label]['export_id'] = export_id

                if export_id not in self.data:
                    self.data[export_id] = {}
                    self.data[export_id]['calls'] = []

                result['startAt'] = start_dt
                result['endAt'] = step_dt
                self.data[export_id]['name'] = step_label
                self.data[export_id]['batch'] = batch_label

                self.data[export_id]['calls'].append(result)

            # set to the next day
            start_dt = step_dt + timedelta(1)

        self._save_pickle_data()

        # for each job create 
        print('Job Initialized!')
        return self.data

    def check_batch(self, table = None):
        self._load_pickle_data()
        if table is None:
            table = 'leads'

        if not self.API_MAX_QUEUE:
            self.API_MAX_QUEUE = 10
        
        self.API_QUEUE_COUNT = self.API_MAX_QUEUE

        status = 'Created,Queued,Processing,Cancelled,Completed,Failed'
        
        batch_counts = {'Items': 0,
            'Created': 0,
            'Queued': 0,
            'Processing': 0,
            'Cancelled': 0,
            'Completed': 0,
            'Failed': 0,
            'FileSize': 0,
            'Rowcount': 0
        }

        response = self.execute('get_bulk_jobs', table=table, status=status)
        for result in response:
            batch_counts['Items'] += 1
            export_id = result['exportId']
            if export_id not in self.data:
                self.data[export_id] = {}
                self.data[export_id]['status'] = 'Init'
                self.data[export_id]['calls'] = []
            
            if result['status'] in ['Queued','Processing']:
                self.API_QUEUE_COUNT -= 1

            batch_counts[result['status']] += 1

            if 'status' not in self.data[export_id]:
                self.data[export_id]['status'] = 'Init'

            if self.data[export_id]['status'] != result['status']:
                for key in result:
                    self.data[export_id][key] = result[key]

            if 'fileSize' in result:
                batch_counts['FileSize'] += result['fileSize']
            
            if 'numberOfRecords' in result:
                batch_counts['Rowcount'] += result['numberOfRecords']   

        self._save_pickle_data()        

        print('\tBatch counts:')
        for key in batch_counts:
            print('\t-- {}:\t{}'.format(key, batch_counts[key]))
        return response        

    def process_batch(self, table=None):
        '''
        this will start the created jobs...
        '''
        self._load_pickle_data()
        if table is None:
            table = 'leads'

        status = 'Created'
        if not self.API_MAX_QUEUE:
            self.API_MAX_QUEUE = 10
        
        if not self.API_QUEUE_COUNT:
            self.API_QUEUE_COUNT = self.API_MAX_QUEUE
        
        response = self.execute('get_bulk_jobs', table=table, status=status)

        for result in response: #['result']:
            export_id = result['exportId']

            if export_id not in self.data:
                self.data[export_id] = {}
                self.data[export_id]['status'] = 'Init'
                self.data[export_id]['calls'] = []

            if result['status'] == 'Created' and self.API_QUEUE_COUNT > 0:
                print('starting export: [{}]'.format(export_id))
                try:
                    r = self.execute('start_bulk_job', export_id=export_id, table=table)
                except MarketoException:
                    print('Marketo: unable to start bulk jobs.')
                    self.API_QUEUE_COUNT = 0
                    break
                except:
                    print('Some thing else went wrong!')
                    self.API_QUEUE_COUNT = 0
                    break

                for key in r[0]:
                    self.data[export_id][key] = r[0][key]

                self.data[export_id]['calls'].append(r[0])
                self.API_QUEUE_COUNT -= 1

            if self.API_QUEUE_COUNT <= 0:
                print('Too many items in the queue...')
                break

        self._save_pickle_data()
        return response   

    def download_batch(self, table=None):
        '''
        this will download the completed jobs...
        '''
        self._load_pickle_data()
        if table is None:
            table = 'leads'

        status = 'Completed'
        
        response = self.execute('get_bulk_jobs', table=table, status=status)
        results = []
        for result in response:
            export_id = result['exportId']
            format = result['format'].lower()
            if result['status'] == 'Completed':
                if export_id not in self.data:
                    self.data[export_id] = {}
                    self.data[export_id]['status'] = 'Completed'
                    self.data[export_id]['calls'] = []

                file_name = '{}.{}'.format(export_id, format)
                    
                if 'filename' not in self.data[export_id] and not os.path.exists(file_name):
                    print('fetching {} -> {}'.format(export_id, format))
                    r = self.retrieve_bulk_job(export_id=export_id, format=format, table=table)
                    for key in r:
                        result[key] = r[key]
                        self.data[export_id][key] = r[key]
                    results.append(result)
                    self.data[export_id]['calls'].append(r)

        self._save_pickle_data()      

        return results

    def cancel_batch(self, table= None, batch_label = None, step_label = None):
        self._load_pickle_data()
        if table is None:
            table = 'leads'

        if batch_label is not None:
            pass

        if step_label is not None:
            pass

        status = 'Created,Queued,Processing'
        response = self.execute('get_bulk_jobs', table=table, status=status)
        results = []
        for result in response:
            export_id = result['exportId']
            resp = self.execute('cancel_bulk_job', export_id = export_id, table=table)
            results.extend(resp)
        
        return results
                
    # --------- BULK EXTRACT ---------
    def create_bulk_extract(self, table=None, fields=None, filter=None, format=None, column_header=None):
        self.authenticate()
        args = {
            'access_token': self.token
        }
        body = {}
        if table is None:
            table = 'leads'
        if fields is not None:
            body['fields'] = fields
        if column_header is not None:
            args['columnHeaderNames'] = column_header
        if filter is None:
            # get the last 24 hours period by default
            isodate = self._get_isodate(date.today() - timedelta(1))
            filter = {
            "createdAt": isodate
            }    
        body['filter'] = filter
        if format is None:
            format='CSV'
        body['format'] = format
        if table.lower == 'leads' and fields is None: raise ValueError("Required argument 'fields' is none.")
        result = self._api_call('post', self.host + "/bulk/v1/" + table + "/export/create.json", args, data=body)
        if result is None: raise Exception("Empty Response")
        if not result['success'] : raise MarketoException(result['errors'][0])
        return result['result']

    def get_bulk_jobs(self, table=None, status=None, next_page=None, batch_size=None):
        self.authenticate()
        print('=: ' + self.token)
        args = {
            'access_token': self.token
        }
        q_mark = '?'

        if table is None:
            table = 'leads'
        if status is not None:
            status='{}status={}'.format(q_mark, status)
            q_mark = '&'
        if next_page is not None:
            next_page='{}nextPageToken={}'.format(q_mark,next_page)
            q_mark = '&'
        if batch_size is not None:
            batch_size='{}batchSize={}'.format(q_mark, batch_size)
            q_mark = '&'
        url = '{}/bulk/v1/{}/export.json{}{}{}'.format(self.host, table, xstr(status), xstr(next_page), xstr(batch_size))

        result = self._api_call('get', url, args)
        if result is None: raise Exception("Empty Response")
        if not result['success'] : raise MarketoException(result['errors'][0])
        return result['result']

    def start_bulk_job(self, export_id, table=None):
        self.authenticate()
        if export_id is None: raise ValueError("Required argument 'exportId' is none.")
        args = {
            'access_token': self.token
        }

        if table is None:
            table = 'leads'
        result = self._api_call('post', self.host + "/bulk/v1/" + table + "/export/" + export_id + "/enqueue.json", args)
        if not result['success'] : raise MarketoException(result['errors'][0])
        return result['result']
        
    def status_bulk_job(self, export_id, table=None):
        self.authenticate()
        if export_id is None: raise ValueError("Required argument 'exportId' is none.")
        args = {
            'access_token': self.token
        }
        if table is None:
            table = 'leads'
        result = self._api_call('get', self.host + "/bulk/v1/" + table + "/export/" + export_id + "/status.json", args)
        if result is None: raise Exception("Empty Response")
        if not result['success'] : raise MarketoException(result['errors'][0])
        return result['result']

    def retrieve_bulk_job(self, export_id, format=None, table=None):
        self.authenticate()
        if export_id is None: raise ValueError("Required argument 'exportId' is none.")
        if table is None:
            table = 'leads'
        if format is None:
            format = 'csv'

        url = self.host + "/bulk/v1/" + table + "/export/" + export_id + "/file.json?access_token=" + self.token
        file_name = '{}.{}'.format(export_id, format)
        result = {'filename': file_name, 'url': url, 'size': 0}
        result['size'] = self._download_file(url, file_name)
        result['success'] = True
        return result

    def cancel_bulk_job(self, export_id, table=None):
        self.authenticate()
        if export_id is None: raise ValueError("Required argument 'exportId' is none.")
        args = {
            'access_token': self.token
        }
        if table is None:
            table = 'leads'
        result = self._api_call('post', self.host + "/bulk/v1/" + table + "/export/" + export_id + "/cancel.json", args)
        if result is None: raise Exception("Empty Response")
        if not result['success'] : raise MarketoException(result['errors'][0])
        return result['result']
