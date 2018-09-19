import os
from json import load
from datetime import date, timedelta, datetime
from marketorestpython.client import MarketoClient
from marketorestpython.batch import MarketoClientBatch

# Load credentials from json file
with open("marketo_batch_settings.json", "r") as file:  
    settings = load(file)

server = settings['deploy']

munchkin_id = settings['config'][server]['munchkin_id']         ### Enter Munchkin ID
client_id = settings['config'][server]['client_id']             ### enter client ID (find in Admin > LaunchPoint > view details)
client_secret = settings['config'][server]['client_secret']     ### enter client secret (find in Admin > LaunchPoint > view details)

table = settings['load']

print('Authenticating for batch jobs...')
batch_days = settings['batch']['days']
mcb = MarketoClientBatch(munchkin_id, client_id, client_secret, api_days_max=batch_days)
mcb.authenticate()
print('\tAuthenticated!')

end_dt = datetime.strptime(settings['batch']['end'], '%Y-%m-%d')
start_dt = datetime.strptime(settings['batch']['start'], '%Y-%m-%d')
print('export from {} -> {}'.format(start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')))

if settings['run_processes']['create'] is True:
    data = mcb.run_batch(start_dt=start_dt, end_dt=end_dt,table=table)

print('==== Status... Q:{}:{}'.format(mcb.API_QUEUE_COUNT, mcb.API_MAX_QUEUE))
resp = mcb.check_batch(table=table)
print('[[[')
for r in resp:
    if 'status' in r:
        if r['status'] in ['Created']:
            print('\t{} -> {}:{}'.format(r['exportId'], r['status'], r['createdAt']))
print(']]] Q:{}:{}'.format(mcb.API_QUEUE_COUNT, mcb.API_MAX_QUEUE))

if settings['run_processes']['queue'] is True:
    print('==== Start Jobs... Q:{}:{}'.format(mcb.API_QUEUE_COUNT, mcb.API_MAX_QUEUE))
    resp = mcb.process_batch(table=table)
    print('[[[')
    for r in resp:
        if 'status' in r:
            if r['status'] in ['Processing', 'Queued']:
                print('\t{} -> {}:{}'.format(r['exportId'], r['status'], r['createdAt']))
    print(']]] Q:{}:{}'.format(mcb.API_QUEUE_COUNT, mcb.API_MAX_QUEUE))

    print('==== Status... Q:{}:{}'.format(mcb.API_QUEUE_COUNT, mcb.API_MAX_QUEUE))
    resp = mcb.check_batch(table=table)

if settings['run_processes']['download'] is True:
    print('==== Downloads...')
    print('[[[')
    resp = mcb.download_batch(table=table)
    for r in resp:
        print('{}\t{}\t{}'.format(r['exportId'], r['numberOfRecords'],r['fileSize']))
    print(']]]')

if settings['run_processes']['cancel'] is True:
    print('*** CANCELLING ***')
    resp = mcb.cancel_batch(table=table)
    for r in resp:
        print('{}\t{}\t{}'.format(r['exportId'], r['status'],r['createdAt']))

if settings['run_processes']['display'] is True:
    print('pickle...')
    data = mcb.data
    if 'requests' in data:
        reqs = data['requests']
        for r in reqs:
            print(r)    

    print('\n==== batches')
    labels = {}
    if 'batches' in data:
        batches = data['batches']
        for batch in batches:
            print(batch)
            if 'export_id' in batches[batch]:
                export_id = batches[batch]['export_id']

                if export_id not in labels:
                    labels[export_id] = batches[batch]

    print('\n==== Exports')
    for key in data:
        if key not in ['batches','requests']:
            print(key)
            item = data[key]
            for field in item:
                if field == 'calls':
                    print('\t{}: {}'.format(field, len(item[field])))
                else:
                    print('\t{}: {}'.format(field, item[field]))

if settings['run_processes']['rename'] is True:
    print('renaming...')
    data = mcb.data
    labels = {}
    if 'batches' in data:
        batches = data['batches']
        for batch in batches:
            if 'export_id' in batches[batch]:
                export_id = batches[batch]['export_id']

                if export_id not in labels:
                    labels[export_id] = batches[batch]

    for key in data:
        if key not in ['batches','requests']:
            item = data[key]
            new_name = None
            if 'exportId' in item:
                export_id = item['exportId']
                if export_id in labels:
                    new_name = '{}_{}.{}.{}'.format(labels[export_id]['start'].strftime('%Y%m%d'), labels[export_id]['end'].strftime('%Y%m%d'), table, item['format'].lower())

            file_name = '{}.{}'.format(export_id, item['format'].lower())

            if new_name is not None and os.path.exists(file_name):
                # rename this file to
                os.rename(file_name, new_name)

print('Batch run completed!')
