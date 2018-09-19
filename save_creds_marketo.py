import json

# save creds
settings = {}

settings['deploy'] = "Prod"
settings['load'] = None

settings['config'] = {
    'Sandbox': {
        'munchkin_id': None,
        'client_id': None,
        'client_secret': None
    },
    'Prod': {
        'munchkin_id': None,
        'client_id': None,
        'client_secret':  None
    }
}

settings['run_processes'] = {
    'create': False,
    'queue': True,
    'download': True,
    'display': True,
    'cancel': False,
    'rename': True
}

settings['tables'] = ['activities', 'leads']

# Save the credentials object to file
with open("marketo_batch_settings.json", "w") as file:  
    json.dump(settings, file)
