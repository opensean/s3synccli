#!/usr/bin/env python3
## Sean Landry, sean.d.landry@gmail.com
## Description: s3synccli --> sync local directory and or file with an s3 bucket.  
##              Contains the S3SyncUtility, DirectoryWalk, ProgressPercentage,
##              and SmartS3Sync() classes.

"""
Sync local data with S3 while maintaining metadata.  Maintaining metadata is 
crucial for working with S3 as a mounted file system via s3fs. 


Metadata notes
--------------
when in doubt:

    - for directories use "mode":"509"
    - for files use "mode":"33204"

Usage:
    s3sync <localdir> <s3path> [--metadata METADATA] [--meta_dir_mode METADIR]
                               [--meta_file_mode METAFILE] [--uid UID] 
                               [--gid GID] [--profile PROFILE] [--localcache] 
                               [--localcache_dir CACHEDIR] [--interval INTERVAL] 
                               [--log LOGLEVEL] [--log_dir LOGDIR]
    s3sync -h | --help 

Options: 
    <localdir>                   local directory file path
    
    <s3path>                     s3 key, e.g. cst-compbio-research-00-buc/
    
    --metadata METADATA          metadata in json format 
                                 e.g. '{"uid":"6812", "gid":"6812"}'
    
    --meta_dir_mode METADIR      mode to use for directories in metadata if 
                                 none is found locally [default: 509]
    
    --meta_file_mode METAFILE    mode to use for files in metadata if none if 
                                 found locally [default: 33204]
    
    --profile PROFILE            aws profile name 
    
    --uid UID                    user id that will overide any uid information
                                 detected for files and directories
    
    --gid GID                    group id that will overid any gid information
                                 detected for files and directories
    
    --localcache                 use local data stored in --localcache_dir to 
                                 save on md5sum computation.
    
    --localcache_dir CACHEDIR    directory in which to store 
                                 local_md5_cache.json.gz, default: 
                                 os.path.join(os.environ.get('HOME'), '.s3sync') 
    
    --interval INTERVAL          enter any number greater than 0 to start 
                                 autosync mode, program will sync every 
                                 interval (min)
    
    --log LOGLEVEL               set the logger level (threshold), available 
                                 options include DEBUG, INFO, WARNING, ERROR, 
                                 or CRITICAL. [default: DEBUG]
    
    --log_dir LOGDIR             file path to directory in which to store the 
                                 logs. No log files are created if this option
                                 is ommited.
    -h --help                    show this screen.
""" 
__author__= "Sean Landry"
__email__= "sean.d.landry@gmail.com, sean.landry@celllsignal.com"
__data__= "16june2017"
__version__= "0.1.0"

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
import time
import gzip
import logging
from logging.handlers import TimedRotatingFileHandler

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
                                    str(mystat.st_mode), str(int(mystat.st_mtime)),
                                    str(mystat.st_size), str(self.md5(key)), key]
        else:
            statLst = [str(mystat.st_uid), str(mystat.st_gid),
                                    str(mystat.st_mode), str(int(mystat.st_mtime)),
                                    str(mystat.st_size), '', key]
        
        return {a:b for a,b in zip(keyLst, statLst)}




class DirectoryWalk():

    def __init__(self, local = None, md5sum = False):
        self.local = local
        self.root = OrderedDict({})
        self.file = OrderedDict({})
        self.isdir = True
        self.md5sum = md5sum
        self.logger = logging.getLogger(self.__class__.__name__)
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
            self.root.update({a:s3util.dzip_meta(a)})
            if c:
                for f in c:
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
        
            s3 = OrderedDict({})
            for k,v in keys.items():
                if isdir:
                    ## omit first 
                    if os.path.join(s3path.split('/', 1)[1], k[len(self.local) + 1:] + '/') != '/':
                        s3.update({os.path.join(s3path.split('/', 1)[1], k[len(self.local) + 1:] + '/'):v})
                else:
                    s3.update({os.path.join(s3path.split('/', 1)[1], k[len(self.local) + 1:]):v})
            
            return s3
        
        except AttributeError as e:
            self.logger.exception(str(e))


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
            sys.stderr.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stderr.flush()
        

class SmartS3Sync():

    def __init__(self, local = None, s3path = None, metadata = None, 
                 profile = None, meta_dir_mode = "509", 
                 meta_file_mode = "33204", uid = None, gid = None,
                 localcache = False,
                 localcache_dir = None,
                 localcache_fname = 's3sync_md5_cache.json.gz', 
                 log = logging.DEBUG, library = logging.CRITICAL):
        
        self.local = local
        self.s3path = s3path
        self.bucket = s3path.split('/', 1)[0]
        self.walk = DirectoryWalk(local)
        self.uid = uid
        self.gid = gid
        self.logger = self.init_logger(log, library = library)
        self.metadir, self.metafile = self.parse_meta(metadata,
                            dirmode = meta_dir_mode, filemode = meta_file_mode, uid = uid, gid = gid)
        self.keys = self.parse_prefix(s3path, self.bucket, self.metadir)
        self.s3cl = None
        self.s3rc = None
        self.session = self.init_boto3session(profile)
        self.localcache = localcache
        self.localcache_fname = localcache_fname
        self.localcache_dir = self.init_localcache(localcache_dir, localcache)
        

    def init_logger(self, log = logging.DEBUG, library = logging.CRITICAL):
        ## prevent library loggers from printing to log by setting level 
        ## to critical
        logging.getLogger('boto3').setLevel(library)
        logging.getLogger('botocore').setLevel(library)
        logging.getLogger('nose').setLevel(library)
        logging.getLogger('s3transfer').setLevel(library)
        rootLogger = logging.getLogger()
        class_logger = logging.getLogger(self.__class__.__name__)   
        
        ## change root logger level if not equal to log arg, necessary if 
        ## working with SmartS3Sync in ipython session, logging library uses
        ## a hierarchy structure meaning child loggers will pass args to 
        ## parent first, root loggers default level is 30 (logging.WARNING),
        ## need to set root logger to log arg level to allow child logger a 
        ## chance to handle log output
        if rootLogger.level != log:
            logging.basicConfig(level = log)

        class_logger.setLevel(log)
        
        return class_logger
    
    def init_boto3session(self, profile):
        """
        Initialize a boto3 session, s3 client, and s3 resource.

        Checks for credentials in the following orderd:
            1. profile arg, checks for profile in .aws config
            2. environment variables--> AWS_ACCESS_KEY_ID, 
               AWS_SECRET_ACCESS_KEY, and AWS_DEFAULT_REGION
            3. 'default' profile in .aws config
        
        Args:
            profile (str): aws profile name.
        
        Returns:
            session (boto3.Session)
        """
        if profile:
            ## use profile names passed in args, looks for .aws config file
            session = boto3.Session(profile_name = profile)
            if session:
                self.s3cl = boto3.client('s3')
                self.s3rc = boto3.resource('s3')
                self.logger.debug('using ' + profile + ' profile in '
                                  + '.aws/config and .aws/credentials')
                return session
        elif os.environ.get('AWS_ACCESS_KEY_ID') and os.environ.get('AWS_SECRET_ACCESS_KEY') and os.environ.get('AWS_DEFAULT_REGION'):
            ## use environment variables
            session = boto3.Session(aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
                                     aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY'),
                                     region_name = os.environ.get('AWS_DEFAULT_REGION'))
            if session:
                ## only intialize client and resource if valid session is established
                self.s3cl = boto3.client('s3')
                self.s3rc = boto3.resource('s3')
                self.logger.debug('using environment variables for aws credentials')
                return session

        ## use 'default' in .aws config file
        session = boto3.Session() 
        if session:
            self.s3cl = boto3.client('s3')
            self.s3rc = boto3.resource('s3')
            self.logger.debug('using default profile in .aws/config and '
                                  + '.aws/credentials')
            return session
        else:
            self.logger.critical('Cannot establish aws boto3 session, ' + 
                                 + 'exiting...')
            sys.exit()

    def init_localcache(self, localcache_dir, localcache):
        """
        
        """
        if localcache and not localcache_dir or localcache and not os.path.exists(localcache_dir):
            
            localcache_dir = os.path.join(os.environ.get('HOME'), '.s3sync/')
            self.logger.warning('local cache directory not found using '
                                + 'default --> ' + localcache_dir)
           
            try:
                self.logger.info('creating ' + localcache_dir)
                os.mkdir(localcache_dir)
            except FileExistsError:
                self.logger.info(localcache_dir 
                                  + ' already exists, skipping...')
                
        return localcache_dir

    
    def check_localcache(self, keys):
        """

        """
        if not os.path.exists(self.localcache_dir):
            os.mkdir(self.localcache_dir)
        
        md5_data = os.path.join(self.localcache_dir, self.localcache_fname)  
        
        util = S3SyncUtility()

        if os.path.isfile(md5_data):
            fdict = {}
            keys_updated = OrderedDict({})
            with gzip.open(md5_data, 'rb') as f:
                
                fdict = json.loads(f.read().decode())
                
                for k,v in keys.items():
                    try:
                        ## check last modified, if different compute md5
                        if fdict[v['local']]['mtime'] != v['mtime']:
                            keys_updated.update({k:v})
                            
                            keys_updated[k]['ETag'] = util.md5(k)
                            fdict[v['local']] = {'ETag':keys_updated[k]['ETag'], 'mtime':v['mtime']}
                        else:
                            ## if same last modified, use stored md5 tag
                            keys_updated.update({k:v})
                            keys_updated[k]['ETag'] = fdict[v['local']]['ETag']
                    except KeyError:
                        ## if key not found locally compute md5, and store local
                        keys_updated.update({k:v})
                        keys_updated[k]['ETag'] = util.md5(v['local'])
                        
                        ## update local data store
                        fdict[v['local']] = {'ETag':keys_updated[k]['ETag'], 'mtime':v['mtime']}
            
            with gzip.open(md5_data, 'w') as f:
                json_str = json.dumps(fdict)
                json_bytes = json_str.encode('utf-8')
                f.write(json_bytes)
          
            return keys_updated
        else:
             self.logger.info('no local md5 data found, calculating now ...')
            
             ## using the below strategy to avoid any itermediate python 
             ## objects, encase local data store is large
             with gzip.open(md5_data, 'w') as f:
                 
                 self.logger.info('writing md5 data to ' + md5_data)
                 
                 f.write(b'{')
                 
                 count = 0
                 total = len(keys) - 1
                 
                 for k,v in keys.items():
                     keys[k]['ETag'] = util.md5(v['local'])
                     nextLn = '    "' + v['local'] + '": {"mtime": "' + v['mtime'] + '" , "ETag": "' + v['ETag'] + '"}'
                     f.write(nextLn.encode())
                     
                     if count < total:
                         f.write(b',\n')
                     
                     count += 1
                 
                 f.write(b'}')
              
             return keys
     
     
    def parse_prefix(self, path = None, bucket = None, metadir = None):
        """
        Parse an s3 prefix key path.

        Args:
            path (str): entire s3 path.

        Returns:
            prefixes (lst): list of prefixes
            e.g. ['home/', 'home/sean.landry/']

        """
        prefixes = OrderedDict({})
        
        temp = path[len(bucket) + 1:]
        while '/' in temp:
            temp = temp.rsplit('/', 1)[0]
            if temp:
                prefixes.update({temp + '/': json.loads(metadir)})
        return prefixes  



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
        metadir['mtime'] = str(int(time.time()))
        metafile['mtime'] = str(int(time.time()))
        metadirjs = json.dumps(metadir)
        metafilejs = json.dumps(metafile)
        return metadirjs, metafilejs

   

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
                        self.logger.info('no metadata found for ' + k 
                                         + ' updating...')
                        update = self.meta_update(key = k, metadata = v)
                    except ClientError as e:
                        ## allow continue to allow existing directory structure                        
                        ## such as '/home' that may not have metadata but 
                        ## allowed by bucket policy

                        self.logger.error(str(e) + "...skipping.")
            
                if metaresult != v:
                    try:
                        self.logger.info('bad metadata found for ' + k 
                                         + ' updating...')
                        update = self.meta_update(key = k, metadata = v)
                    except ClientError as e:
                        ## allow continue to allow existing directory structure
                        ## such as '/home' that may not have metadata but 
                        ## allowed by bucket policy
                        self.logger.error(str(e) + "...skipping.")
                        
            except ClientError:
                ## key does not exist so lets create it
                try: 
                    self.logger.info("creating key '" + k + "'")

                    make_key = self.s3cl.put_object(Bucket = self.bucket, Key = k,
                                                    Metadata = v)
                     
                except ClientError:
                    ## Access Denied, s3 permission error
                    self.logger.exception("exiting...")
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
            self.logger.info(prefix + ' key does not exist yet')

        return matches

    def compare_etag(self, s3localfilekeys, matches):
        """
        Compare local etag(md5sum) values with s3 etag values.

        Args:
            s3localfilekeys (OrderedDict): local filepaths converted to s3
                                           s3 keys.  Local metadata is stored.
            
            matches (OrderedDict): s3 object keys with metadata.

        Returns:
            needs_sync (OrderedDict): files that need upload because of Etag difference.
            
            OrderedDict structure for args and return:
            
            {'s3key/path': {'uid':'1000', 'Etag':'###', 'mode':'33204', etc...'}}

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
        
        local_file_dict[key] = util.dzip_meta(key = self.local, md5sum = True)
        
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
               
                ## remove unneccesary metadata 
                rm_local_etag = meta['Metadata'].pop('ETag')
                rm_local_path = meta['Metadata'].pop('local')
                
                ## check for uid & gid
                if self.uid:
                    meta['Metadata']['uid'] = self.uid
                if self.gid:
                    meta['Metadata']['gid'] = self.gid

                try:
                    self.logger.info("upload: " + self.local + " to " + key)

                    self.s3cl.upload_fileobj(f, self.bucket, key,
                                    ExtraArgs = meta,
                                    Callback = ProgressPercentage(self.local))

                except ClientError as e:
                    self.logger.exception('upload failed')
        else:
            self.logger.info(self.local + ' is up to date.')

    def sync_dir(self):
        """
        Sync a local directory with an s3 bucket.

        """
        utility = S3SyncUtility()
        ## local dirs converted to s3keys
        s3localdirkeys = self.walk.toS3Keys(self.walk.root, self.s3path)
        ## local files converted to s3keys
        s3localfilekeys = self.walk.toS3Keys(self.walk.file, self.s3path,
                                             isdir = False)
        if s3localdirkeys:
            s3LocalDirAndFileKeys = s3localdirkeys
        else:
            s3LocalDirAndFileKeys = OrderedDict({})
        
        for k,v in s3localfilekeys.items():
            s3LocalDirAndFileKeys.update({k:v})


        if self.localcache:
            self.logger.info('checking local cache...')
            s3LocalDirAndFileKeys = self.check_localcache(s3LocalDirAndFileKeys)
        else:
           for k,v in s3LocalDirAndFileKeys.items():
               s3LocalDirAndFileKeys[k]['ETag'] = utility.md5(s3LocalDirAndFileKeys[k]['local'])
        
        ## paginate bucket
        matches = self.query(self.s3path[len(self.bucket) + 1:], 
                             s3LocalDirAndFileKeys)

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
                        self.logger.info("upload: " + v['local'] + " to "+ k)
                
                        l = v['local']

                        ## remove unneccesary metadata 
                        rm_local_etag = meta['Metadata'].pop('ETag')
                        rm_local_path = meta['Metadata'].pop('local')
                        
                        self.s3cl.upload_fileobj(f, self.bucket, k,
                                     ExtraArgs = meta,
                                     Callback = ProgressPercentage(l))
                        sys.stderr.write('\n')
                else:

                    try:
                        self.logger.info("creating key '" + k)
                        make_key = self.s3cl.put_object(Bucket = self.bucket, Key = k,
                                    Metadata = meta['Metadata'], ContentType = meta['ContentType'])

                    except ClientError as e:
                        ## Access Denied, s3 permission error
                        self.logger.exception("exiting")
                        sys.exit()

        else:
            self.logger.info('S3 bucket is up to date')

    def sync(self, interval = None):
        """
        Complete a sync between a local directory or file and an s3 bucket.  

        """
        autosync = True
        while autosync: 
            if os.path.isfile(self.local):
                self.sync_file()

            elif os.path.isdir(self.local):
                self.sync_dir()

            else:
                self.logger.critical(self.local + 'is not a file or a '
                                     + 'directory!\n')
                sys.exit()
            if not interval:
                autosync = False
            else:
                interval = float(interval)
                for i in range(int(interval * 60), 0, -1):
                    sys.stderr.write('next sync in %d seconds\r' % i)
                    sys.stderr.flush()
                    time.sleep(1)


def main():
    """
    Command line arguments.
    """  
    
    ## command line args
    options = docopt(__doc__)
    
    ## setup s3sync logger
    # command line argument. Convert to upper case to allow the user to
    # specify --log=DEBUG or --log=debug
    numeric_level = getattr(logging, options['--log'].upper(), None)
    
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)

    module_logger = logging.getLogger()
    module_logger.setLevel(numeric_level)
    #rootLogger = logging.getLogger()
    #if rootLogger.level != numeric_level:
    #    logging.basicConfig(level = numeric_level)

    if options['--log_dir']:
        ## create file handler
        if options['--interval']:
            dateTag = datetime.now().strftime("%Y-%b-%d")
            fh = TimedRotatingFileHandler(options['--log_dir'] 
                                    + "/%s_s3sync.log" % dateTag, 
                                    when = 'M', 
                                    interval =  float(options['--interval']))
        
        else:
            dateTag = datetime.now().strftime("%Y-%b-%d_%H-%M-%S")
            fh = logging.FileHandler(options['--log_dir'] 
                                     + "/%s_s3sync.log" % dateTag)
        
        
        fh_formatter = logging.Formatter('%(asctime)s %(filename)s %(name)s.%(funcName)s() - %(levelname)s:%(message)s')
        fh.setFormatter(fh_formatter)

        module_logger.addHandler(fh)

    
    # create console handler
    console = logging.StreamHandler()
    console.setLevel(numeric_level)

    # create formatter and add it to the handler
    console_formatter = logging.Formatter('%(name)s.%(funcName)s() - %(levelname)s:%(message)s')
    console.setFormatter(console_formatter)
    
    module_logger.addHandler(console)

    s3_sync = SmartS3Sync(local = options['<localdir>'], 
                        s3path = options['<s3path>'], 
                        metadata = options['--metadata'], 
                        profile = options['--profile'],
                        meta_dir_mode = options['--meta_dir_mode'],
                        meta_file_mode = options['--meta_file_mode'],
                        uid = options['--uid'],
                        gid = options['--gid'],
                        localcache = options['--localcache'],
                        localcache_dir = options['--localcache_dir'],
                        log = numeric_level)

    s3_sync.sync(interval = options['--interval'])

if __name__== "__main__":

    
    main()
    

