#!/usr/bin/env python3
## Sean Landry, sean.d.landry@gmail.com, sean.landry@cellsignal.com
## version 03june2017

"""
Upload to S3 with metadata.

Metadata notes
- for directories use "mode":"509", mode 33204 does NOT work
- for files use "mode":"33204"
- currently these values are hard coded

Usage:
    s3up <localdir> <s3key> [--metadata METADATA]
    s3up -h | --help 

Options: 
    <localdir>               local directory file path
    <s3key>                  s3 key, e.g. cst-compbio-research-00-buc/
    --metadata METADATA      metadata in json format '{"uid":"6812", "gid":"6812", "mode":"33204"}'
    -h --help                show this screen.
""" 

from docopt import docopt
import subprocess
import os
import sys
import json

if __name__== "__main__":
    """
    Command line arguments.
    """  

    options = docopt(__doc__)

    ## parse some args
    my_bucket = options['<s3key>'].split('/', 1)[0]
    local = options['<localdir>']
    metadir = json.loads(options['--metadata'])
    metadir["mode"] = "509"
    metadirjs = json.dumps(metadir)
    metafile = json.loads(options['--metadata'])
    metafile["mode"] = "33204"
    metafilejs = json.dumps(metafile)


    if len(options['<s3key>'].split('/', 1)) > 1:
        key = options['<s3key>'].split('/', 1)[1]
    else:
        key = options['<s3key>']

    sys.stderr.write("bucket: " + my_bucket + " key: " + key + "\n") 

    ## find local directories and sort
    d = subprocess.Popen(["find", local, "-type", "d"], 
                         stdout = subprocess.PIPE, shell = False)

    f = subprocess.Popen(["find", local, "-type", "f"], 
                         stdout = subprocess.PIPE, shell = False)
    
    dsort = subprocess.Popen(["sort", "-n"], stdin = d.stdout, shell = False, 
                             stdout = subprocess.PIPE)
    
    fsort = subprocess.Popen(["sort", "-n"], stdin = f.stdout, shell = False, 
                             stdout = subprocess.PIPE)
    
    ## sorted directories and files as lists
    d_keys = dsort.communicate()[0].decode().strip().split('\n')
    f_keys = fsort.communicate()[0].decode().strip().split('\n')
     
    ## check if s3key exists and has metadata if not create it
    try:
        check = subprocess.Popen(["aws", "s3api", "head-object", "--bucket", 
                                 my_bucket, "--key", key], 
                                 stdout = subprocess.PIPE, shell = False)
        
        ## if s3key does exist but has no metadata then update it now
        meta = json.loads(check.communicate()[0].decode())['Metadata']
        
        if len(meta) == 0:
            sys.stderr.write('no metadata found for ' + key + ' updating...\n')
            
            subprocess.run(["aws", "s3api", "copy-object", "--bucket", 
                           my_bucket, "--key", key, "--copy-source", 
                           my_bucket + "/" + key, "--metadata", metadirjs, 
                           "--metadata-directive", "REPLACE"])
    
    except:
        sys.stderr.write('creating key now...\n')
       
        subprocess.run(["aws", "s3api", "put-object", "--bucket", my_bucket, 
                        "--key", key, "--metadata", metadirjs])
         
        ## debug
        #check = subprocess.Popen(["aws", "s3api", "head-object", "--bucket", 
        #                          my_bucket, "--key", key], 
        #                          stdout = subprocess.PIPE, shell = False)
        
        #print(json.loads(check.communicate()[0].decode())['Metadata'])
        
    ## skip first key in list becuase it is the command line arg
    for k in d_keys[1:]: 
        nextK = key + k[len(local) + 1:] + '/'
        sys.stderr.write("creating folder... '" + nextK + "'\n")

        subprocess.run(["aws", "s3api", "put-object", "--bucket", my_bucket, 
                        "--key", nextK, "--metadata", metadirjs])
    
    for f in f_keys:
        nextF = key + f[len(local) + 1:]
        sys.stderr.write("uploading '" + f + "'\n")
        sys.stderr.write("s3 key: '" + nextF + "'\n")
        
        subprocess.run(["aws", "s3api", "put-object", "--bucket", my_bucket, 
                        "--key", nextF, "--body", f, "--metadata", metafilejs])

