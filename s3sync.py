#!/usr/bin/env python3
## Sean Landry, sean.d.landry@gmail.com, sean.landry@cellsignal.com
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
__author__= "Sean Landry"
__email__= "sean.d.landry@gmail.com"
__data__= "09june2017"
__version__= "0.1"

from docopt import docopt
import subprocess
import sys
import json
import boto3
from botocore.exceptions import ClientError
from collections import OrderedDict
from datetime import datetime
import os
import hashlib
from binascii import unhexlify
import threading

class S3SyncUtility():
    
    def __init__(self):
        self.name = "S3SyncUtility"

## https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
## https://stackoverflow.com/questions/6591047/etag-definition-changed-in-amazon-s3/28877788#28877788
    def md5(self, fname, part_size = 8 * 1024 * 1024):
        #print("part_size", part_size)
        #print("file size", os.path.getsize(fname))
        if os.path.isfile(fname): 
            hash_md5 = hashlib.md5()
            blockcount = 0
            md5Lst = []
            with open(fname, "rb") as f:
                for chunk in iter(lambda: f.read(part_size), b""):
                    hash_md5 = hashlib.md5()
                    hash_md5.update(chunk)
                    #print(hash_md5.hexdigest())
                    md5Lst.append(hash_md5.hexdigest())
                    blockcount += 1
            
            if blockcount <= 1:
                #print(hash_md5.hexdigest())
                return hash_md5.hexdigest()
            else:
                c = ''.join(md5Lst)
                c = unhexlify(c)
                hash_md5 = hashlib.md5()
                hash_md5.update(c)
                #print(hash_md5.hexdigest() + '-' + str(blockcount))
                return hash_md5.hexdigest() + '-' + str(blockcount)
        else:
            return ''

    def dzip_meta(self, key):
        stat = os.stat(key)
        return {a:b for a,b in zip(["uid", "gid", "mode", "mtime", "size", "ETag", "local"],
                                   [str(stat.st_uid), str(stat.st_gid),
                                    str(stat.st_mode), str(stat.st_mtime),
                                    str(stat.st_size), str(self.md5(key)), key])}



class DirectoryWalk():

    def __init__(self, local = None):
        self.local = local
        self.root = None
        self.file = None
        self.isdir = True
        self.walk_dir(local)

    def walk_dir(self, local):
        s3util = S3SyncUtility()
        d = sorted(os.walk(local))
        if len(d) == 0 and os.path.isfile(local):
            self.isdir = False
        for a,b,c in d:
            if not self.root:
                self.root = OrderedDict({a:s3util.dzip_meta(a)})
            else:
                self.root.update({a:s3util.dzip_meta(a)})
            if c:
                for f in c:
                    if not self.file:
                        self.file = OrderedDict({os.path.join(a, f):s3util.dzip_meta(os.path.join(a, f))})
                    else:
                        self.file.update({os.path.join(a, f):s3util.dzip_meta(os.path.join(a, f))})
    
    def toS3Keys(self, keys, s3path, isdir = True):
        try:
            s3 = None
            for k,v in keys.items():
                if isdir:
                    if s3 == None:
                        ## omit first 
                        if os.path.join(s3path.split('/', 1)[1], k[len(self.local) + 1:] + '/') != '/':
                            s3 = OrderedDict({os.path.join(s3path.split('/', 1)[1], k[len(self.local) + 1:] + '/'):v})
                    else:
                        s3.update({os.path.join(s3path.split('/', 1)[1], k[len(self.local) + 1:] + '/'):v})
                else:
                    if s3 == None:
                        s3 = OrderedDict({os.path.join(s3path.split('/', 1)[1], k[len(self.local) + 1:]):v})
                    else:
                        s3.update({os.path.join(s3path.split('/', 1)[1], k[len(self.local) + 1:]):v})
            return s3
        except AttributeError as e:
            sys.stderr.write(str(e) + '\n')

## ProgressPercentage is straight from the documentation
## http://boto3.readthedocs.io/en/latest/_modules/boto3/s3/transfer.html
class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

class SmartS3Sync():

    def __init__(self, local = None, s3path = None, metadata = None, 
                 profile = 'default', meta_dir_mode = "509", 
                 meta_file_mode = "33204"):
        
        self.local = local
        self.s3path = s3path
        self.bucket = s3path.split('/', 1)[0]
        self.metadir, self.metafile = self.parse_meta(metadata, 
                            dirmode = meta_dir_mode, filemode = meta_file_mode)
        self.keys = self.parse_prefix(s3path, self.bucket)
        self.walk = DirectoryWalk(local)
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
            prefixes (lst): list of prefixes
            e.g. ['home/', 'home/sean.landry/']

        """
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
        my_bucket.Object(key).copy_from(CopySource = my_bucket.name +'/' + key,
                                        Metadata = metadata, 
                                        MetadataDirective='REPLACE')
        
    def create_key(self, key = None, metadata = None): 
        """
        Create an s3 object, also known as put-object.

        Args:
            key (str): s3 key.
            metadata (dict):  metadata to attach to the object.

        """
        return self.s3cl.put_object(Bucket = self.bucket, Key = key, 
                                    Metadata = metadata) 
     

    def verify_keys(self, keys = None):
        """
        Check if the keys in list exist.  If the do not exist create them.

        Args:
            keys (OrderedDict): list of keys.
            meta (dict): metadata to attach to the keys
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
                        sys.stderr.write('no metadata found for ' + k 
                                         + ' updating...\n')
                        update = self.meta_update(key = k, metadata = v)
                    except ClientError as e:
                        ## allow continue to allow existing directory structure                        
                        ## such as '/home' that may not have metadata but 
                        ## allowed by bucket policy

                        sys.stderr.write(str(e) + "\n")
            
                if metaresult != v:
                    try:
                        sys.stderr.write('bad metadata found for ' + k 
                                         + ' updating...\n')
                        update = self.meta_update(key = k, metadata = v)
                    except ClientError as e:
                        ## allow continue to allow existing directory structure
                        ## such as '/home' that may not have metadata but 
                        ## allowed by bucket policy
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

    def query(self, prefix, search):
        # Create a reusable Paginator
        paginator = self.s3cl.get_paginator('list_objects_v2')

        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(Bucket = self.bucket,
                                 Prefix = prefix)

        matches = None

        ## look for kyes in object first, iterate until all pages are 
        ## exhausted or all keys have been found
        for page in page_iterator:
            for k,v in search.items():
                if matches:
                    matches.update({k:item for item in page['Contents'] if item['Key'] == k})
                else:
                    matches = OrderedDict({k:item for item in page['Contents'] if item['Key'] == k})
            if len(matches) == len(search):
                ## no need to continue page iteration if we have found 
                ## all keys
                return matches
        return matches

    def compare_etag(self, s3localfilekeys, matches):
        
        ## compare ETags to determine which files need to be uploaded
        needs_sync = None
        
        for k,v in s3localfilekeys.items():
            a = v['ETag']
            try:
                b = matches[k]['ETag'].replace('"', '')
                #print(a, b)
            except KeyError as e:
                #sys.stderr.write(a + " needs upload \n")
                if needs_sync:
                    needs_sync[k] = v
                else:
                    needs_sync = OrderedDict({k:v})
        return needs_sync

    def sync_file(self):
        util = S3SyncUtility()
        
        localD[self.local] = util.dzip_meta(key = self.local)
        
        key = self.s3path.split('/', 1)[1] + self.local.rsplit('/', 1)[1]
        
        matches = self.query(key, localD)
        
        needs_sync = self.compare_etag(localD, matches)

        ## if needs_sync
        with open(self.local, 'rb') as f:
            meta = {}
            meta['Metadata'] = search[self.local].values()
            try:
                sys.stderr.write("upload: " + self.local + " to " + key
                                 + "\n")

                self.s3cl.upload_fileobj(f, self.bucket, key,
                                ExtraArgs = meta,
                                Callback = ProgressPercentage(fname))

                sys.stderr.write("\n")

            except ClientError as e:
                sys.stderr.write(str(e) + "\n")

    def sync_dir(self):
         ## local dirs converted to s3keys
         s3localdirkeys = self.walk.toS3Keys(self.walk.root, self.s3path)
         ## local files converted to s3keys
         s3localfilekeys = self.walk.toS3Keys(self.walk.file, self.s3path,
                                              isdir = False)
         matches = self.query(self.s3path[len(self.bucket) + 1:], s3localfilekeys)

         #print(matches)
         needs_sync = self.compare_etag(s3localfilekeys, matches)
       
         if needs_sync:
             ## make list of directory keys to check prior to upload
             keys_to_check = []
             for k,v in needs_sync.items():
                 keys_to_check.append(k.rsplit('/', 1)[0] + '/')
             keys_to_check = sorted(set(keys_to_check))
             keys_to_check = OrderedDict([(k,s3localdirkeys[k]) for k in keys_to_check if k not in self.s3path])

             ## verify keys
             self.verify_keys(keys = keys_to_check)

             ## complete sync
             for k, v in needs_sync.items():
                 with open(v['local'], 'rb') as f:
                     meta = {}
                     meta['Metadata'] = v

                     sys.stderr.write("upload: " + v['local'] + " to "
                                       + k + "\n")

                     self.s3cl.upload_fileobj(f, self.bucket, k,
                                  ExtraArgs = meta,
                                  Callback = ProgressPercentage(v['local']))

                     sys.stderr.write("\n")

         else:
             sys.stderr.write('S3 bucket is up to date\n')

    def sync(self):
        """
        Completes a sync between a local directory or file and an s3 bucket.  

        """
        ## verify the s3path
        self.verify_keys(keys = self.keys)
        s3url = 's3://' + self.s3path
        
        if os.path.isfile(self.local):
            self.sync_file(self.local)

        elif os.path.isdir(self.local):
            self.sync_dir()

        else:
            sys.stderr.write('ERROR --> ' + self.local + 
                             'is not a file or a directory!\n')
           
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
