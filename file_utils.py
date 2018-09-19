import os
import stat
import re
import time

def get_info(filepath):
    time_format = "%Y-%m-%d %H:%M:%S"
    try:
        file_stats = os.stat(filepath)
        modification_time = time.strftime(time_format,time.localtime(file_stats[stat.ST_MTIME]))
        access_time = time.strftime(time_format,time.localtime(file_stats[stat.ST_ATIME]))
        file_size = file_stats[stat.ST_SIZE]

    except Exception as e:
        modification_time, access_time, file_size = ["","",0]
        print("ERROR: {}".format(e))

    return modification_time, access_time, file_size

def scanFiles(folder, regex_filter = None):
    """ This method will recursively scan the specified folder and add records if neccessary"""
 
    for dirpath, _, filenames in os.walk(folder):
        for filename in filenames:
            filepath = os.path.join(dirpath,filename)

            # sieve the file, accept the file on regex match
            m = None
            if regex_filter is not None:
                m = re.match(regex_filter, filename)
                if not m:
                    continue

            try:
                mTime, aTime, fSize = get_info(filepath)
                # save the format
                data = {
                    "folder" : dirpath, 
                    "file" : filename, 
                    "modified" : mTime, 
                    "accessed" : aTime, 
                    "size" : fSize
                }
                yield data

            except Exception as e:
                print("Error: scanning file stats - {}".format(e))
