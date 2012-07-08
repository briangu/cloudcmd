import os
import sys
import urllib

rootdir = sys.argv[1]
url = 'http://localhost:8080/records'
projection = 'cloudcmd'

for root, subFolders, files in os.walk(rootdir):
  for file in files:
    f = open(os.path.join(root,file), 'r')
    data = f.read()
    params = urllib.urlencode({
	'projection': projection,
	'record': data
    })
    id = urllib.urlopen(url, params).read()
    print id
 
