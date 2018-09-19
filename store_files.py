# https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-python

## Use this only for Azure AD service-to-service authentication
from azure.common.credentials import ServicePrincipalCredentials

## Use this only for Azure AD end-user authentication
from azure.common.credentials import UserPassCredentials

## Use this only for Azure AD multi-factor authentication
from msrestazure.azure_active_directory import AADTokenCredentials

## Required for Azure Data Lake Storage Gen1 account management
from azure.mgmt.datalake.store import DataLakeStoreAccountManagementClient
from azure.mgmt.datalake.store.models import DataLakeStoreAccount

## Required for Azure Data Lake Storage Gen1 filesystem management
from azure.datalake.store import core, lib, multithread

# Common Azure imports
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.resource.resources.models import ResourceGroup
from file_utils import get_info, scanFiles

## Use these as needed for your application
import os, stat, logging, getpass, pprint, uuid, time, re
from json import load

def auth_azure(tenant_id, resource='https://datalake.azure.net/'):
    adlCreds = lib.auth(tenant_id=tenant_id, resource=resource)
    return adlCreds

def load_settings(filename=r"datalake_settings.json"):
    # Load credentials from json file
    with open(filename, "r") as file:  
        settings = load(file)
    return settings

settings = load_settings()

subscription_id = settings['credentials']['subscription_id']
tenant_id = settings['credentials']['tenant_id']

dataLake_store_name = settings['paths']['datalake_store_name']
datalake_path = settings['paths']['datalake_path']
source_dir = settings['paths']['source']
archive_dir = settings['paths']['archive']

## authenticate
adlCreds = auth_azure(tenant_id)

## Create a filesystem client object
adlsFileSystemClient = core.AzureDLFileSystem(adlCreds, store_name=dataLake_store_name)

## Upload a file
print(source_dir)

uploaded = 0
found = 0
regex_filter = re.compile(r"[\d]{8}_[\d]{8}\.activities\.csv")
for f in scanFiles(source_dir, regex_filter): 
    print("\tuploading: {}".format(f['file']))
    found += 1
    file_path = os.path.join(f['folder'], f['file'])
    dl_path = '{}/{}'.format(datalake_path, f['file'])
    arc_path = os.path.join(archive_dir, f['file'])
    try:
        multithread.ADLUploader(adlsFileSystemClient, 
            lpath=file_path, 
            rpath=dl_path, 
            nthreads=64, overwrite=True, buffersize=4194304, blocksize=4194304)

        # move the file to the archive file
        os.rename(file_path, arc_path)

        uploaded += 1  

    except Exception as ex:
        print(ex)

print("uploaded {}".format(uploaded))