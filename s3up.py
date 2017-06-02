#!/usr/bin/env python3
## Sean Landry, sean.d.landry@gmail.com, sean.landry@cellsignal.com

"""
Upload to S3 with metadata.

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

if __name__== "__main__":
    """
    Command line arguments.
    """  

    options = docopt(__doc__)

    aws_args = ['aws', 's3api', 'put-object']

    ## example upload
    ## aws s3api put-object --bucket cst-compbio-research-00-buc --key home/sean.landry@cellsignal.com/test/s3up.py --body s3up.py --metadata '{"uid":"6812", "gid":"6812", "mode":"33204"}'

    my_bucket = options['<s3key>'].split('/', 1)[0]
    
    if len(options['<s3key>'].split('/', 1)) > 1:
        key = options['<s3key>'].split('/', 1)[1]
    else:
        key = options['<s3key>']

    sys.stderr.write("bucket: " + my_bucket + " key: " + key + "\n") 

    d = subprocess.Popen(["find", options['<localdir>'], "-type", "d"], stdout = subprocess.PIPE, shell = False)
    f = subprocess.Popen(["find", options['<localdir>'], "-type", "f"], stdout = subprocess.PIPE, shell = False)

    dsort = subprocess.Popen(["sort"], stdin = d.stdout, shell = False, stdout = subprocess.PIPE)
    fsort = subprocess.Popen(["sort"], stdin = f.stdout, shell = False, stdout = subprocess.PIPE)
    d_keys = dsort.communicate()[0].decode().strip().split('\n')
    f_keys = fsort.communicate()[0].decode().strip().split('\n')
    
    for k in d_keys:
        #print(key + k.rsplit('/', 1)[-1])
        nextK = key + k.rsplit('/', 1)[-1] + '/'
        #print(options['--metadata']) 
        subprocess.run(["aws", "s3api", "put-object", "--acl", "public-read-write", "--bucket", my_bucket, "--key", nextK, "--metadata", options['--metadata']])
    
    

