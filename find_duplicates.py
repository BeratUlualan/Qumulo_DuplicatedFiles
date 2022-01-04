import time
from datetime import datetime
import json
import qumulo
import os
from qumulo.rest_client import RestClient
import logging

# Read credentials
with open('credentials.json') as json_file:
    json_object = json.load(json_file)

# Parse cluster credentials
cluster_address = json_object['cluster_address']
port_number = json_object['port_number']
username = json_object['username']
password = json_object['password']

# Connect to the cluster
rc = RestClient(cluster_address, port_number)
rc.login(username, password)
logging.info('Connection established with {}'.format(cluster_address))

# Script inputs
directory_path = json_object['directory_path']

start_time = datetime.now().strftime("%s")

seen = set()
dupes = []
dupes_path = []

def file_operation (object):
    modification_time = object['modification_time'].split("T")[0]
    modification_time_epoch = datetime.strptime(modification_time, '%Y-%m-%d').strftime("%s")
    file_name = object['name']
    
    if file_name in seen:
        dupes.append(file_name)
        dupes_path.append(object['path'])
    else:
        seen.add(file_name)
	
def main (objects):
	for r in range(len(objects)):
		current_time = datetime.now().strftime("%s")
		delta_time = int(current_time) - int(start_time)
		print ("Processing..."+ str(delta_time) + "seconds", end="\r")
		object = objects[r]
		if object['type'] == "FS_FILE_TYPE_FILE":
			file_operation (object)
		elif object['type'] == "FS_FILE_TYPE_DIRECTORY":
			next_page = "first"
			while next_page != "":
				r = None
				if next_page == "first":
					try:
						r = rc.fs.read_directory(path=object['path'], page_size=1000)
					except:
						next
				else:
					r = rc.request("GET", next_page)
				if not r:
					break
				dir_objects = r['files']
				main(dir_objects)
				if 'paging' in r and 'next' in r['paging']:
					next_page = r['paging']['next']
				else:
					next_page = ""

if __name__ == "__main__":
    objects = rc.fs.read_directory(path=directory_path, page_size=10000)['files']
    main(objects)
    print(dupes)
    print(dupes_path)
