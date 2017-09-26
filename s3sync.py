#!/usr/bin/env python3
## Sean Landry, sean.d.landry@gmail.com

"""

Sync local data with S3 while maintaining metadata.  Maintaining metadata is 
crucial for working with S3 as a mounted file system via s3fs. 

The order of <path> args determines the direciton of sync.  First, <path> is 
the source, the second <path> is the destination.  The s3 path must begin with
the prefix 's3://'.

e.g.
sync from s3 bucket (download)
./s3sync.py s3://mybucket/docs /home/myhome/docs --log debug 

sync to s3 bucket (upload)
./s3sync.py /home/myhome/docs s3://mybucket/docs --log debug 

Metadata notes
--------------
when in doubt:

    - for directories use "mode":"509"
    - for files use "mode":"33204"

Usage:
    s3sync.py <path> <path> [options]
    s3sync.py -h | --help 

Options: 
    <path>                       a local path or s3 bucket path

    --force                      force upload even if local files have not 
                                 changed, ignore md5 cache.
    
    --metadata METADATA          metadata in json format 
                                 e.g. '{"uid":"6812", "gid":"6812"}'
    
    --meta-dir-mode METADIR      mode to use for directories in metadata if 
                                 none is found locally [default: 509]
    
    --meta-file-mode METAFILE    mode to use for files in metadata if none if 
                                 found locally [default: 33204]
    
    --profile PROFILE            aws profile name 
    
    --uid UID                    user id that will overide any uid information
                                 detected for files and directories
    
    --gid GID                    group id that will overid any gid information
                                 detected for files and directories
    
    --localcache                 use local data stored in --localcache_dir to 
                                 save on md5sum computation.
    
    --localcache-dir CACHEDIR    directory in which to store 
                                 local_md5_cache.json.gz, default: 
                                 os.path.join(os.environ.get('HOME'), '.s3sync') 
    
    --localcache-fname FILENAME  file name to use for local cache.  Use this 
                                 arg to to explicity specify cache name or use 
                                 an existing cache file.
    
    --interval INTERVAL          enter any number greater than 0 to start 
                                 autosync mode, program will sync every 
                                 interval (min)
    
    --log LOGLEVEL               set the logger level (threshold), available 
                                 options include DEBUG, INFO, WARNING, ERROR, 
                                 or CRITICAL. [default: INFO]
    
    --log-dir LOGDIR             file path to directory in which to store the 
                                 logs. No log files are created if this option
                                 is ommited.
    -h --help                    show this screen.
""" 
__author__= "Sean Landry"
__email__= "sean.d.landry@gmail.com"
__date__= "21sep2017"
__version__= "0.2.0"

from docopt import docopt
import subprocess
import sys
import json
import boto3
from botocore.exceptions import ClientError
from collections import OrderedDict
import os
import hashlib
from binascii import unhexlify
import threading
import magic
import datetime
import time
import gzip
import logging
from logging.handlers import TimedRotatingFileHandler
import uuid

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
            ## md5sum dev/null
            return "d41d8cd98f00b204e9800998ecf8427e"

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
        self.logger.debug('walking local directory or file')
        s3util = S3SyncUtility()
        d = sorted(os.walk(local))
        if len(d) == 0 and os.path.isfile(local):
            self.logger.debug(local + ' is a file.')
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
                        self.logger.debug('local: ' + k + ' s3key: ' + os.path.join(s3path.split('/', 1)[1], k[len(self.local) + 1:] + '/'))
                else:
                    s3.update({os.path.join(s3path.split('/', 1)[1], k[len(self.local) + 1:]):v})
                    self.logger.debug('local: ' + k + ' s3key: ' + os.path.join(s3path.split('/', 1)[1], k[len(self.local) + 1:]))
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
                 localcache_fname = None,
                 log = logging.INFO, library = logging.CRITICAL):
        
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
        self.localcache_fname = self.init_localcache_fname(localcache_fname)
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
        self.logger.debug('intializing boto3 session')
        if profile:
            ## use profile names passed in args, looks for .aws config file
            session = boto3.Session(profile_name = profile)
            if session:
                self.s3cl = session.client('s3')
                self.s3rc = session.resource('s3')
                self.logger.debug('using ' + profile + ' profile in '
                                  + '.aws/config and .aws/credentials')
                return session
        
        ## boto3 by default will then check for environment variables and 
        ## then check the ~/.aws/config file

        session = boto3.Session() 
        if session:
            self.s3cl = session.client('s3')
            self.s3rc = session.resource('s3')
            self.logger.debug('using default profile in .aws/config and '
                                  + '.aws/credentials')
            return session
        else:
            self.logger.critical('Cannot establish aws boto3 session, ' + 
                                 + 'exiting...')
            sys.exit()

    def init_localcache_fname(self, localcache_fname):
        """

        """
        if localcache_fname:
            return localcache_fname
        else:
            seq = ('{0:%Y-%m-%d_%H-%M-%S}'.format(datetime.datetime.now()), str(uuid.uuid4()), 's3sync_md5_cache.json.gz')
            return '_'.join(seq)

    def init_localcache(self, localcache_dir, localcache):
        """
        
        """
        if localcache and not localcache_dir or localcache and not os.path.exists(localcache_dir):
            self.logger.debug('intializing localcache')            
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
        Check a localcache file for md5 data already calculated to save on 
        computation.

        Args:
            keys (OrderedDict): 
            {'local/path': {'uid':'1000', 'Etag':'###', 'mode':'33204', etc...'}}
        """
        self.logger.info('using localcache file: '+ self.localcache_fname)

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

    def queryS3(self, prefix, search = OrderedDict({}), return_all_objects = True):
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
                if return_all_objects:
                    if matches:
                        matches.update({item['Key']:item for item in page['Contents']})
                    else:
                        matches = OrderedDict({item['Key']:item for item in page['Contents']})                    
                else:
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

    def compare_etag(self, source, destination, fromS3 = False):
        """
        Compare local etag(md5sum) values with s3 etag values.

        Args:
            source (OrderedDict): s3 keys with metadata.
            
            destination (OrderedDict): s3 keys with metadata.

        Returns:
            needs_sync (OrderedDict): files that need upload/dowload because 
            of Etag difference.
            
            OrderedDict structure for args and return:
            
            {'s3key/path': {'uid':'1000', 'Etag':'###', 'mode':'33204', etc...'}}

            this dictionary will include a 'local' key that has the file path 
            to a local file before conversion to an s3 key for eTag lookup.
        """
        ## compare ETags to determine which files need to be uploaded
        needs_sync = None
        for k,v in source.items():
            a = v['ETag'].replace('"', '')  ## handles formatting when result is from s3
            try:
                b = destination[k]['ETag'].replace('"', '') ## handles formatting when result is from s3
                if a == b:    
                    self.logger.debug('match found destination: ' + b + ' source: ' + a + ' s3path: ' + k)
                else:
                    if needs_sync:
                        needs_sync[k] = v
                    else:
                        needs_sync = OrderedDict({k:v})
            except (KeyError, TypeError) as e:
                if fromS3:
                    self.logger.debug(k + ':' + a + " needs download")
                else:
                    self.logger.debug(v['local'] + ':'+ a + " needs upload")
                if needs_sync:
                    needs_sync[k] = v
                else:
                    needs_sync = OrderedDict({k:v})
        return needs_sync

    def sync_file_toS3(self, force = False, show_progress = True):
        """
        Sync a local file with to an s3 bucket.
 
        """
        util = S3SyncUtility()
        local_file_dict = {} 
        
        key = self.s3path.split('/', 1)[1] + self.local.rsplit('/', 1)[1]
         
        if force:
            local_file_dict[key] = util.dzip_meta(key = self.local, md5sum = True)
            ## force an upload of all files
            needs_sync = local_file_dict
            self.logger.warning('using force, ignoring local cache and s3 '
                                'bucket contents, uploading all files')
        
        else:
            if self.localcache:
                local_file_dict[key] = util.dzip_meta(key = self.local, md5sum = False)
                self.logger.info('checking local cache...')
                local_file_dict = self.check_localcache(local_file_dict) 
            else:
                local_file_dict[key] = util.dzip_meta(key = self.local, md5sum = True)

            self.logger.debug('paginate (queryS3) bucket')
            matches = self.queryS3(key, local_file_dict)
            
            self.logger.debug('comparing etags (md5sum)')
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
                meta['Metadata'] = local_file_dict[key].copy()
               
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
                    if show_progress:
                        self.s3cl.upload_fileobj(f, self.bucket, key,
                                        ExtraArgs = meta,
                                        Callback = ProgressPercentage(self.local))
                        sys.stderr.write('\n')
                    else:
                        self.s3cl.upload_fileobj(f, self.bucket, key,
                                        ExtraArgs = meta)
 
                except ClientError as e:
                    self.logger.exception('upload failed')

            self.verify_sync(needs_sync)
        else:
            self.logger.info(self.local + ' is up to date.')

    def sync_dir_toS3(self, force = False, show_progress = True):
        """
        Sync a local directory with to an s3 bucket.

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

        
        if force:
            ## force an upload of all files
            
            for k,v in s3LocalDirAndFileKeys.items():
                s3LocalDirAndFileKeys[k]['ETag'] = utility.md5(s3LocalDirAndFileKeys[k]['local'])
            needs_sync = s3LocalDirAndFileKeys
            self.logger.warning('using force, ignoring local cache and s3 '
                                'bucket contents, uploading all files')
        
        else:
            if self.localcache:
                self.logger.info('checking local cache...')
                s3LocalDirAndFileKeys = self.check_localcache(s3LocalDirAndFileKeys)
            else:
               for k,v in s3LocalDirAndFileKeys.items():
                   s3LocalDirAndFileKeys[k]['ETag'] = utility.md5(s3LocalDirAndFileKeys[k]['local'])
            
            self.logger.debug('paginate (queryS3) bucket')
            ## paginate bucket
            matches = self.queryS3(self.s3path[len(self.bucket) + 1:], 
                                 s3LocalDirAndFileKeys)

            self.logger.debug('comparing etags (md5sum)')
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
                
                ## copy v becuase intact dict is needed to verify sync
                meta['Metadata'] = v.copy()


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
                        
                        if show_progess:
                            self.s3cl.upload_fileobj(f, self.bucket, k,
                                         ExtraArgs = meta,
                                         Callback = ProgressPercentage(l))
                            sys.stderr.write('\n')
                        else:
                            self.s3cl.upload_fileobj(f, self.bucket, k,
                                         ExtraArgs = meta)
 
                else:

                    try:
                        self.logger.info("creating key '" + k)
                        make_key = self.s3cl.put_object(Bucket = self.bucket, Key = k,
                                    Metadata = meta['Metadata'], ContentType = meta['ContentType'])

                    except ClientError as e:
                        ## Access Denied, s3 permission error
                        self.logger.exception("exiting")
                        sys.exit()
            
            self.verify_sync(needs_sync)
        else:
            self.logger.info('S3 bucket is up to date')
   

    def sync_dir_fromS3(self, force = False, show_progress = True):
        if force:
            needs_sync = self.queryS3(self.s3path[len(self.bucket) + 1:], 
                                 return_all_objects = True)            
            self.logger.warning('using force, ignoring local cache and will '
                                + 'download all objects from bucket path')
        else:

            utility = S3SyncUtility()
       
            s3localdirkeys = None
            s3localfilekeys = OrderedDict({})

            if os.path.isdir(self.local):
                self.logger.debug('found existing local directory with name "' + self.local + '"'
                                  + ' checking for existing files')
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
                self.logger.debug('updating dict with s3 keys ' + k + ':' + str(v))
                s3LocalDirAndFileKeys.update({k:v})
        
            if self.localcache:
                self.logger.info('checking local cache...')
                s3LocalDirAndFileKeys = self.check_localcache(s3LocalDirAndFileKeys)
            else:
               for k,v in s3LocalDirAndFileKeys.items():
                   self.logger.debug('not using localcache, calculating md5 sum now for "' + v['local'] + '"')
                   s3LocalDirAndFileKeys[k]['ETag'] = utility.md5(s3LocalDirAndFileKeys[k]['local'])
                   self.logger.debug(s3LocalDirAndFileKeys[k]['ETag']) 
            
            self.logger.debug('paginate (queryS3) bucket')
            ## paginate bucket
            all_s3_objects= self.queryS3(self.s3path[len(self.bucket) + 1:], 
                                         return_all_objects = True)

            self.logger.debug('comparing etags (md5sum)')

            needs_sync = self.compare_etag(all_s3_objects, s3LocalDirAndFileKeys, fromS3 = True)
        if needs_sync:
            
            ## complete sync
            for k, v in needs_sync.items():
                v['local'] = os.path.join(self.local, k[len(self.s3path[len(self.bucket) + 1:]):])
               
                if not k.endswith('/'):
                    try:
                        self.logger.info('making local directory ' 
                             + v['local'].rsplit('/', 1)[0])
                        os.makedirs(v['local'].rsplit('/', 1)[0])
                    except FileExistsError as e:
                        self.logger.info('local directory already exists, skipping...')

                    with open(v['local'], 'wb') as f:
                        try:
                            self.logger.info("download: " + k + " to "
                                             + v['local'])
                
                            
                            self.s3cl.download_fileobj(
                                    Bucket = self.bucket, 
                                    Key = k,
                                    Fileobj= f)

                        except ClientError as e:
                            ## Access Denied, s3 permission error
                            self.logger.exception("exiting")
                            sys.exit()
            
            self.verify_sync(needs_sync, fromS3 = True)
        else:
            self.logger.info('local directory "' + self.local + '" is up to date with s3://"'+ self.s3path +'"')
 
 
    
    def sync_file_fromS3(self, force = False, show_progress = True):
        """
        Sync a file from an s3 bucket.
 
        """
        util = S3SyncUtility()
        local_file_dict = {} 
        
        key = self.s3path.split('/', 1)[1] 
         
        if force:
            needs_sync = self.queryS3(key, return_all_objects = True, fromS3 = True)
            self.logger.warning('using force, ignoring local cache and s3 '
                                'bucket contents, downloading all files')
        
        else:
            if self.localcache:
                local_file_dict[key] = util.dzip_meta(key = self.local, md5sum = False)
                self.logger.info('checking local cache...')
                local_file_dict = self.check_localcache(local_file_dict) 
            elif os.path.isfile(self.local):
                local_file_dict[key] = util.dzip_meta(key = self.local, md5sum = True)
            s3_content = self.s3cl.head_object(Bucket = self.bucket, Key = key)
           
            matches = OrderedDict({key:s3_content})
            
            self.logger.debug('comparing etags (md5sum)')
            needs_sync = self.compare_etag(matches, local_file_dict, fromS3 = True)
        
        if needs_sync:

            with open(self.local, 'wb') as f:
                try:
                    self.logger.info("download: " + key + " to " + self.local)

                    self.s3cl.download_fileobj(Bucket = self.bucket, 
                                               Key = key,
                                               Fileobj = f)
                    
                except ClientError as e:
                    self.logger.exception('download failed')

            self.verify_sync(needs_sync)
        else:
            self.logger.info(self.local + ' is up to date.')

       

    def verify_sync(self, just_synced, fromS3 = False):
        """
        Verify the completed sync.

        Args:
            just_synced (OrderedDict): items just synced to bucket.
        
        OrderedDict structure:
        {'s3key/path': {'uid':'1000', 'Etag':'###', 'mode':'33204', etc...'}}
        
        """

        self.logger.info('verifying sync')
        ## paginate bucket
        matches = self.queryS3(self.s3path[len(self.bucket) + 1:],
                                    just_synced)
        faulty_syncs = self.compare_etag(just_synced, matches, fromS3 = fromS3)
        
        if faulty_syncs:
            for k,v in faulty_syncs.items():
                self.logger.error('bad upload: ' + v['local'])
        else:
            self.logger.info('sync verified')

    def sync(self, interval = None, force = False, fromS3 = False, show_progress = True):
        """
        Complete a sync between a local directory or file and an s3 bucket.  

        Args:
            interval (float): sync interval in minutes.
            force (boolean): force sync, ignore localcache.
            fromS3 (boolean): direction of sync.
            show_progress (boolean): show sync progress.
        """
        autosync = True
        while autosync:
            if fromS3:
                self.logger.info('preparing to sync FROM S3')
                if self.s3path.endswith('/'):
                    self.sync_dir_fromS3(force = force, 
                                         show_progress = show_progress)
                else:
                    self.sync_file_fromS3(force = force,
                                          show_progress = show_progress)
            else:
                self.logger.info('preparing to sync TO S3')
                if os.path.isfile(self.local):
                    self.sync_file_toS3(force = force, 
                                        show_progress = show_progress)

                elif os.path.isdir(self.local):
                    self.sync_dir_toS3(force = force,
                                       show_progress, show_progress)

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


def main(options):
   
    ## setup s3sync logger
    # command line argument. Convert to upper case to allow the user to
    # specify --log=DEBUG or --log=debug
    numeric_level = getattr(logging, options['--log'].upper(), None)
    
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)

    module_logger = logging.getLogger()
    module_logger.setLevel(numeric_level)
    
    if options['--log-dir']:
        ## create file handler
        if options['--interval']:
            dateTag = datetime.datetime.now().strftime("%Y-%b-%d")
            fh = TimedRotatingFileHandler(options['--log-dir'] 
                                    + "/%s_s3sync.log" % dateTag, 
                                    when = 'M', 
                                    interval =  float(options['--interval']))
        
        else:
            dateTag = datetime.datetime.now().strftime("%Y-%b-%d_%H-%M-%S")
            fh = logging.FileHandler(options['--log-dir'] 
                                     + "/%s_s3sync.log" % dateTag)
        
        
        fh_formatter = logging.Formatter('%(asctime)s %(filename)s %(name)s.%(funcName)s() - %(levelname)s:%(message)s')
        fh.setFormatter(fh_formatter)

        module_logger.addHandler(fh)

    
    # create console handler
    console = logging.StreamHandler()
    console.setLevel(numeric_level)

    # create formatter and add it to the handler
    console_formatter = logging.Formatter('%(asctime)s %(name)s.%(funcName)s() - %(levelname)s:%(message)s')
    console.setFormatter(console_formatter)
    
    module_logger.addHandler(console)

    local = ''
    s3path = ''
    fromS3 = False
    for path in options['<path>']:
        if 's3://' == path[0:5]:
            s3path = path[5:] ## trim the 's3://' we dont need it anymore
            if options['<path>'][0][5:] == s3path:
                fromS3 = True
        else:
            local = path

    if len(s3path[5:]) == 0:
        raise RuntimeError('s3 path not valid format')



    s3_sync = SmartS3Sync(local = local,
                        s3path = s3path, 
                        metadata = options['--metadata'], 
                        profile = options['--profile'],
                        meta_dir_mode = options['--meta-dir-mode'],
                        meta_file_mode = options['--meta-file-mode'],
                        uid = options['--uid'],
                        gid = options['--gid'],
                        localcache = options['--localcache'],
                        localcache_dir = options['--localcache-dir'],
                        localcache_fname = options['--localcache-fname'],
                        log = numeric_level)

    s3_sync.sync(interval = options['--interval'], 
                 force = options['--force'],
                 fromS3 = fromS3)

if __name__== "__main__":
    """
    Command line arguments.
    """  
    
    ## command line args
    options = docopt(__doc__)
    
    main(options)
    

