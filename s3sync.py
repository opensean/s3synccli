#!/usr/bin/env python3
## Sean Landry, sean.d.landry@gmail.com, sean.landry@cellsignal.com
## version 05june2017
## Description: s3syncsli --> sync local directory with s3 bucket.  
##              Contains the SmartS3Sync() class.

"""
Sync local data with S3 maintaining metadata.  This program will accept 
only directories as positional arguments.

Metadata notes
- for directories use "mode":"509", mode 33204 does NOT work
- for files use "mode":"33204"

Usage:
    s3sync <localdir> <s3path> [--metadata METADATA --meta_dir_mode METADIR --meta_file_mode METAFILE --profile PROFILE]
    s3sync -h | --help 

Options: 
    <localdir>                 local directory file path
    <s3path>                   s3 key, e.g. cst-compbio-research-00-buc/
    --metadata METADATA        metadata in json format e.g. '{"uid":"6812", "gid":"6812"}'
    --meta_dir_mode METADIR    mode to use for directories in metadata [default: 509]
    --meta_file_mode METAFILE  mode to use for files in metadata [default: 33204]
    --profile PROFILE          aws profile name [default: default]
    -h --help                  show this screen.
""" 

from docopt import docopt
import subprocess
import os
import sys
import json
import boto3
from botocore.exceptions import ClientError
from collections import OrderedDict

class StatUtility():
    
    def dzip_meta(self, key):
        stat = os.stat(key)
        return {a:b for a,b in zip(["uid", "gid", "mode", "mtime", "size"],
                                   [str(stat.st_uid), str(stat.st_gid),
                                    str(stat.st_mode), str(stat.st_mtime),
                                    str(stat.st_size)])}

class DirectoryWalk(StatUtility):

    def __init__(self, local = None):
        self.local = local
        self.root = None
        self.file = None
        self.isdir = True
        self.walk_dir(local)

    def walk_dir(self, local):
        d = sorted(os.walk(local))
        if len(d) == 0 and os.path.isfile(local):
            self.isdir = False
        for a,b,c in d:
            if not self.root:
                self.root = OrderedDict({a:self.dzip_meta(a)})
            else:
                self.root.update({a:self.dzip_meta(a)})
            if c:
                for f in c:
                    if not self.file:
                        self.file = OrderedDict({os.path.join(a, f):self.dzip_meta(os.path.join(a, f))})
                    else:
                        self.file.update({os.path.join(a, f):self.dzip_meta(os.path.join(a, f))})
    
    def toS3Keys(self, keys, s3path, isdir = True):
        try:
            s3 = []
            for k,v in keys.items():
                if isdir:
                    s3.append([{os.path.join(s3path.split('/', 1)[1], k[len(self.local) + 1:] + '/'):v}])
                else:
                    s3.append([{os.path.join(s3path.split('/', 1)[1], k[len(self.local) + 1:]):v}])
            return s3
        except AttributeError as e:
            sys.stderr.write(str(e) + '\n')

        

class SmartS3Sync():

    def __init__(self, local = None, s3path = None, metadata = None, profile = 'default', meta_dir_mode = "509", meta_file_mode = "33204"):
        self.local = local
        self.s3path = s3path
        self.bucket = s3path.split('/', 1)[0]
        self.metadir, self.metafile = self.parse_meta(metadata, dirmode = meta_dir_mode, filemode = meta_file_mode)
        self.keys = self.parse_prefix(s3path, self.bucket)
        self.profile = profile
        self.session = boto3.Session(profile_name = self.profile)
        self.s3cl = boto3.client('s3')
        self.s3rc = boto3.resource('s3')

    def parse_meta(self, meta = None, dirmode = None, filemode = None):
        """
        Parses passed json argument and adds hardcoded mode for files
        and directories.
        
        Args:
            meta (str): json metadata.

        Returns:
            metadirjs, metafilejs (json): json string specific for dirs and 
                                          files.

        """ 
        if meta:
            metadir = json.loads(meta)
            metafile = json.loads(meta)
        else:
            metadir = {}
            metafile = {}
        if 'mode' not in metadir:
            metadir["mode"] = dirmode
            metafile["mode"] = filemode
        if 'uid' not in metadir:
            metadir["uid"] = str(os.geteuid())
            metafile["uid"] = str(os.geteuid())
        if 'gid' not in metadir:
            metadir["gid"] = str(os.getgid())
            metafile["gid"] = str(os.getgid())
        
        metadirjs = json.dumps(metadir)
        metafilejs = json.dumps(metafile)
        return metadirjs, metafilejs

    def parse_prefix(self, path = None, bucket = None):
        """
        Parse an s3 prefix key path.

        Args:
            path (str): entire s3 path.

        Returns:
            path (str): path withouth bucket name.

        """
        parts = path.split('/')
        prefixes = None
        
        temp = path[len(bucket) + 1:]
        while '/' in temp:
            temp = temp.rsplit('/', 1)[0]
            if temp:
                if not prefixes:
                    prefixes = OrderedDict({temp + '/': json.loads(self.metadir)})
                else:
                    prefixes.update({temp + '/': json.loads(self.metadir)})


        return prefixes  


    def key_exists(self, key = None):
        """
        Check if an s3 key exists.

        Args:
            key (str): s3 key

        Returns:
            dict

        """
        return self.s3cl.head_object(Bucket = self.bucket, Key = key)
        
    def meta_update(self, key = None, metadata = None):
        """
        Update the metadata for an s3 object.

        Args:
            key (str):  s3 key.
            metadata (dict):  metadata to attach to the object.


        """
        my_bucket = self.s3rc.Bucket(self.bucket)
        my_bucket.Object(key).copy_from(CopySource = my_bucket.name +'/' + key, Metadata = metadata, MetadataDirective='REPLACE')
        
    def create_key(self, key = None, metadata = None): 
        """
        Create an s3 object, also known as put-object.

        Args:
            key (str):}}  s3 key.
            metadata (dict):  metadata to attach to the object.

        """
        return self.s3cl.put_object(Bucket = self.bucket, Key = key, Metadata = metadata) 
     
    def s3_content_check(self, Prefix):
        # Create a reusable Paginator
        paginator = self.s3cl.get_paginator('list_objects_v2')

        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=Prefix)

        for page in page_iterator:
            for key in page['Contents']:
                print(key)   


    def s3ls(self, Prefix):
        # Create a reusable Paginator
        paginator = self.s3cl.get_paginator('list_objects_v2')
        
        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=Prefix)
        
        for page in page_iterator:
            
            for key in page['Contents']:
                print(key)

    def verify_keys(self, keys = None):
        """
        Check if the keys in list exist.  If the do not exist create them.

        Args:
            keys (lst): list of keys.
            meta (json str): metadata to attach to the keys
        """
        ## structure of each k --> {'home/somefoler/': {metadata}}
        for k,v in keys.items():
            try:
                check = self.key_exists(key = k) 
                 
                ## if key does exist check for metadata
                metaresult = check['Metadata']
                if len(metaresult) == 0:
                    try:
                        ## if no metadata then add some now
                        sys.stderr.write('no metadata found for ' + k + ' updating...\n')
                        update = self.meta_update(key = k, metadata = v)
                    except ClientError as e:
                        ## allow continue to allow existing directory structure such as '/home'
                        sys.stderr.write(str(e) + "\n")
                         
                if metaresult != v:
                    try:
                        sys.stderr.write('bad metadata found for ' + k + ' updating...\n')
                        update = self.meta_update(key = k, metadata = v)
                    except ClientError as e:
                        ## allow continue to allow existing directory structure such as '/home'
                        sys.stderr.write(str(e) + "\n")
                        
            except ClientError:
                ## key does not exist so lets create it
                try: 
                    sys.stderr.write("creating key '" + k + "'\n")

                    make_key = self.create_key(key = k, metadata = v)
                     
                except ClientError:
                    ## Access Denied, s3 permission error
                    sys.stderr.write("exiting...\n")
                    sys.exit()


    def sync(self):
        """
        Completes a sync between a local path and s3 bucket path.

        """
        ## verify the s3path passed as command line arg
        self.verify_keys(keys = self.keys)
        s3url = 's3://' + self.s3path
        
        if os.isfile(self.local):
            with open(self.local, 'rb') as f:
                meta = StatUtility.dzip_meta(self, key = self.local)
                key = self.s3path.split('/', 1)[1] +  self.local.rsplit('/', 1)[1]
                try:
                    self.s3cl.upload_fileobj(f, self.bucket, key, ExtraArgs = meta)
                    sys.stderr.write("upload: " + self.local + " to " + key + "\n")
                except ClientError as e:
                    sys.stderr.write(str(e) + "\n")
        else:
            ## verify local dirs converted to s3keys
            walk = DirectoryWalk(self.local)
            self.verify_keys(keys = walk.toS3Keys(walk.root, self.s3path))

            ## complete sync
            subprocess.run(["aws", "s3", "sync", self.local, s3url, "--metadata",
                         self.metafile, "--profile", self.profile])

if __name__== "__main__":
    """
    Command line arguments.
    """  

    options = docopt(__doc__)

    s3_sync = SmartS3Sync(local = options['<localdir>'], 
                        s3path = options['<s3path>'], 
                        metadata = options['--metadata'], 
                        profile = options['--profile'],
                        meta_dir_mode = options['--meta_dir_mode'],
                        meta_file_mode = options['--meta_file_mode'])

    s3_sync.sync()
