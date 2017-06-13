#!/usr/bin/env python3
## Sean Landry, sean.d.landry@gmail.com
## Description: s3synccli --> sync local directory and or file with an s3 bucket.  
##              Contains the S3SyncUtility, DirectoryWalk, ProgressPercentage,
##              and SmartS3Sync() classes.

"""
Sync local data with S3 maintaining metadata.  Maintaining metadata is crucial
for working with S3 as a mounted file system via s3fs. 


Metadata notes
--------------
when in doubt:

    - for directories use "mode":"509"
    - for files use "mode":"33204"

Usage:
    s3sync <localdir> <s3path> [--metadata METADATA --meta_dir_mode METADIR --meta_file_mode METAFILE --uid UID --gid GID --profile PROFILE]
    s3sync -h | --help 

Options: 
    <localdir>                 local directory file path
    <s3path>                   s3 key, e.g. cst-compbio-research-00-buc/
    --metadata METADATA        metadata in json format e.g. '{"uid":"6812", "gid":"6812"}'
    --meta_dir_mode METADIR    mode to use for directories in metadata if none is found locally [default: 509]
    --meta_file_mode METAFILE  mode to use for files in metadata if none if found locally [default: 33204]
    --profile PROFILE          aws profile name [default: default]
    --uid UID                  user id that will overide any uid information detected for files and directories
    --gid GID                  groud id that will overid any gid information detected for files and directories
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
import magic 


class S3SyncUtility():
    
    def __init__(self):
        self.name = "S3SyncUtility"

    ## https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
    ## https://stackoverflow.com/questions/6591047/etag-definition-changed-in-amazon-s3/28877788#28877788
    
    def md5(self, fname, part_size = 8 * 1024 * 1024):
        """
        Calculate the md5sum for a file using the specified part_size.
        If a file is larger than the part size then the md5sum is calculated
        using the same approach used by AWS for multipart uploads to ensure 
        Etags can be compared during sync.

        Args:
            fname (str): local file path.
            part_size: file upload part-size bytes.

        Returns:
            md5sum

        """
        if os.path.isfile(fname): 
            hash_md5 = hashlib.md5()
            blockcount = 0
            md5Lst = []
            with open(fname, "rb") as f:
                for chunk in iter(lambda: f.read(part_size), b""):
                    hash_md5 = hashlib.md5()
                    hash_md5.update(chunk)
                    md5Lst.append(hash_md5.hexdigest())
                    blockcount += 1
            
            if blockcount <= 1:
                return hash_md5.hexdigest()
            else:
                ## calculate aws multipart upload etag md5 equivalent
                c = ''.join(md5Lst)
                c = unhexlify(c)
                hash_md5 = hashlib.md5()
                hash_md5.update(c)
                return hash_md5.hexdigest() + '-' + str(blockcount)
        else:
            return ''

    def dzip_meta(self, key, md5sum = False):
        """
        Create a dictionary of local file or dir path with associated os.stat data.

        Args:
           key(str); local file or dir path.

        Returns:
            (dict): in the format {'local/fileordir':{'uid':'1000', 'mode':'509', etc...}}
        """
        mystat = os.stat(key)
        keyLst = ["uid", "gid", "mode", "mtime", "size", "ETag", "local"]

        ## if md5sum False avoid calculating md5sum
        if md5sum:
            statLst = [str(mystat.st_uid), str(mystat.st_gid),
                                    str(mystat.st_mode), str(mystat.st_mtime),
                                    str(mystat.st_size), str(self.md5(key)), key]
        else:
            statLst = [str(mystat.st_uid), str(mystat.st_gid),
                                    str(mystat.st_mode), str(mystat.st_mtime),
                                    str(mystat.st_size), '', key]
        
        return {a:b for a,b in zip(keyLst, statLst)}



class DirectoryWalk():

    def __init__(self, local = None, md5sum = False):
        self.local = local
        self.root = None
        self.file = None
        self.isdir = True
        self.md5sum = md5sum
        self.walk_dir(local)

    def walk_dir(self, local):
        """
        Use os.walk to iterate a local directory and capture os.stat info.

        Args:
            local(str): local directory path.

        """
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
        """
        Convert local directory and/or file paths to s3 keys.

        Args:
            keys (dict): local file/directory paths with associated metadata.
            s3path (str): s3 bucket key path (e.g. buc00/home/)
            isdir (boolean): True keys are directories, False keys are objects.

        """
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


class ProgressPercentage(object):
    
    ## ProgressPercentage is straight from the documentation
    ## http://boto3.readthedocs.io/en/latest/_modules/boto3/s3/transfer.html

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
                 meta_file_mode = "33204", uid = None, gid = None,
                 local_data_path = os.environ.get('HOME') + '/.s3synccli',
                 local_md5_data = 'local_md5_store.json',
                 logs_data = 'logger.json'):
        
        self.local = local
        self.s3path = s3path
        self.bucket = s3path.split('/', 1)[0]
        self.walk = DirectoryWalk(local)
        self.profile = profile
        self.uid = uid
        self.gid = gid
        self.metadir, self.metafile = self.parse_meta(metadata,
                            dirmode = meta_dir_mode, filemode = meta_file_mode, uid = uid, gid = gid)
        self.keys = self.parse_prefix(s3path, self.bucket, self.metadir)
        self.session = boto3.Session(profile_name = self.profile)
        self.s3cl = boto3.client('s3')
        self.s3rc = boto3.resource('s3')
        self.local_data_path = self.init_local_data(local_data_path)
        self.local_md5_data = local_md5_data
        self.logs_data = logs_data

    def parse_meta(self, meta = None, dirmode = None, filemode = None, uid = None, gid = None):
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
        if uid:
            metadir["uid"] = uid
            metafile["uid"] = uid
        if gid:
            metadir["gid"] = gid
            metafile["gid"] = gid
        
        metadirjs = json.dumps(metadir)
        metafilejs = json.dumps(metafile)
        return metadirjs, metafilejs

    def parse_prefix(self, path = None, bucket = None, metadir = None):
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
                    prefixes = OrderedDict({temp + '/': json.loads(metadir)})
                else:
                    prefixes.update({temp + '/': json.loads(metadir)})


        return prefixes  

    def init_local_data(self, local_data_path):
    
        if not os.path.exists(local_data_path):
            os.mkdir(local_data_path)
        
        return local_data_path

    def check_local_md5_store(self, keys):

        if not os.path.exists(self.local_data_path):
            os.mkdir(self.local_data_path)
        
        md5_data_path = os.path.join(self.local_data_path, self.local_md5_data)  
       
        if os.path.isfile(md5_data_path):
            with open(md5_data_path, 'r') as f:
                fjson = json.loads(f)
                print(fjson)
        else:
             sys.stderr.write('no local md5 data found, calculating now ...\n')
             
             util = S3SyncUtility()
             for k,v in keys.items():
                 keys[k]['Etag'] = util.md5(k)
             
             sys.stderr.write('writing md5 data to ' + md5_data_path + '\n')

             with open(md5_data_path, 'w') as f:
                 json.dump(keys, f)

             return keys
    

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
                 
                check = self.s3cl.head_object(Bucket = self.bucket, Key = k)
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

                    make_key = self.s3cl.put_object(Bucket = self.bucket, Key = k,
                                                    Metadata = v)
                     
                except ClientError:
                    ## Access Denied, s3 permission error
                    sys.stderr.write("exiting...\n")
                    sys.exit()

    def query(self, prefix, search):
        """
        Query an s3 bucket using paginator and list-objects-v2.

        Args:
            prefix (str): s3 key used to filter bucket.
            search (OrderedDict): s3 keys to search.

        Returns:
            matches (OrderedDict): matching s3 keys.
            
        e.g. {'s3key/path': {'uid':'1000', 'Etag':'###', 'mode':'33204', etc...'}}


        """
        # Create a reusable Paginator
        paginator = self.s3cl.get_paginator('list_objects_v2')

        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(Bucket = self.bucket,
                                 Prefix = prefix)

        matches = None
    
        try:
            ## look for keys in object first, iterate until all pages are 
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
        except KeyError as e:
            sys.stderr.write(str(e) + ' ... s3 key does not exist yet\n')

        return matches

    def compare_etag(self, s3localfilekeys, matches):
        """
        Compare local etag(md5sum) values with s3 etag values.

        Args:
            s3localfilekeys (OrderedDict): local filepaths converted to s3
                                           s3 keys.  Local metadata is stored.
            
            e.g. {'s3key/path': {'uid':'1000', 'Etag':'###', 'mode':'33204', etc...'}}

            matches (OrderedDict): s3 object keys with metadata.

            e.g. {'s3key/path': {'uid':'1000', 'Etag':'###', 'mode':'33204', etc...'}}

        Returns:
            needs_sync (OrderedDict): files that need upload because of Etag difference.

            e.g. {'s3key/path': {'uid':'1000', 'Etag':'###', 'mode':'33204', etc...'}}

        """
        ## compare ETags to determine which files need to be uploaded
        needs_sync = None
        
        for k,v in s3localfilekeys.items():
            a = v['ETag']
            try:
                b = matches[k]['ETag'].replace('"', '')
                #print(a, b)
            except (KeyError, TypeError) as e:
                #sys.stderr.write(a + " needs upload \n")
                if needs_sync:
                    needs_sync[k] = v
                else:
                    needs_sync = OrderedDict({k:v})
        return needs_sync

    def sync_file(self):
        """
        Sync a local file with an s3 bucket.
 
        """
        util = S3SyncUtility()
        local_file_dict = {} 
        
        key = self.s3path.split('/', 1)[1] + self.local.rsplit('/', 1)[1]
        
        local_file_dict[key] = util.dzip_meta(key = self.local)
        matches = self.query(key, local_file_dict)
                
        needs_sync = self.compare_etag(local_file_dict, matches)
        
        if needs_sync:
            ## verify the s3path
            self.verify_keys(keys = self.keys)

            with open(self.local, 'rb') as f:
                meta = {}
                ## load the magic file() function
                m = magic.open(magic.MAGIC_NONE)
                m_result = m.load()
                meta['ContentType'] = m.file(self.local).split(';')[0]
                meta['Metadata'] = local_file_dict[key]

                ## check for uid & gid
                if self.uid:
                    meta['Metadata']['uid'] = self.uid
                if self.gid:
                    meta['Metadata']['gid'] = self.gid

                try:
                    sys.stderr.write("upload: " + self.local + " to " + key
                                     + "\n")

                    self.s3cl.upload_fileobj(f, self.bucket, key,
                                    ExtraArgs = meta,
                                    Callback = ProgressPercentage(self.local))

                    sys.stderr.write("\n")

                except ClientError as e:
                    sys.stderr.write(str(e) + "\n")
        else:
            sys.stderr.write(self.local + ' is up to date.\n')

    def sync_dir(self):
        """
        Sync a local directory with an s3 bucket.

        """
        ## local dirs converted to s3keys
        s3localdirkeys = self.walk.toS3Keys(self.walk.root, self.s3path)
        ## local files converted to s3keys
        s3localfilekeys = self.walk.toS3Keys(self.walk.file, self.s3path,
                                             isdir = False)

        s3LocalDirAndFileKeys = s3localdirkeys
        for k,v in s3localfilekeys.items():
            s3LocalDirAndFileKeys.update({k:v})

        matches = self.query(self.s3path[len(self.bucket) + 1:], s3LocalDirAndFileKeys)

        #print(matches)
        needs_sync = self.compare_etag(s3LocalDirAndFileKeys, matches)
         
        if needs_sync:
            ## verify the s3path
            self.verify_keys(keys = self.keys)    
            
            ## complete sync
            for k, v in needs_sync.items():
                meta = {}
                m = magic.open(magic.MAGIC_NONE)
                m_result = m.load()
                meta['ContentType'] = m.file(v['local']).split(';')[0]
                meta['Metadata'] = v
                
                ## check for uid & gid
                if self.uid:
                    meta['Metadata']['uid'] = self.uid
                if self.gid:
                    meta['Metadata']['gid'] = self.gid
                
                if not k.endswith('/'):
                    with open(v['local'], 'rb') as f:
                        sys.stderr.write("upload: " + v['local'] + " to "
                                          + k + "\n")

                        self.s3cl.upload_fileobj(f, self.bucket, k,
                                     ExtraArgs = meta,
                                     Callback = ProgressPercentage(v['local']))

                        sys.stderr.write("\n")
                else:

                    try:
                        sys.stderr.write("creating key '" + k + "'\n")
                        make_key = self.s3cl.put_object(Bucket = self.bucket, Key = k,
                                    Metadata = meta['Metadata'], ContentType = meta['ContentType'])

                    except ClientError:
                        ## Access Denied, s3 permission error
                        sys.stderr.write("exiting...\n")
                        sys.exit()

        else:
            sys.stderr.write('S3 bucket is up to date\n')

    def sync(self):
        """
        Complete a sync between a local directory or file and an s3 bucket.  

        """
        
        if os.path.isfile(self.local):
            self.sync_file()

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
                        meta_file_mode = options['--meta_file_mode'],
                        uid = options['--uid'],
                        gid = options['--gid'])

    s3_sync.sync()
