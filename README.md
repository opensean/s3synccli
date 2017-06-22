# s3synccli

## using s3synccli

### run from source

```
    $ git clone https://github.com/opensean/s3synccli.git
    $ cd s3synccli
    $ python3 s3sync.py -h
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
                                   [--localcache_dir CACHEDIR] 
                                   [--localcache_fname FILENAME] 
                                   [--interval INTERVAL] [--force]
                                   [--log LOGLEVEL] [--log_dir LOGDIR]
        s3sync -h | --help 
    
    Options: 
        <localdir>                   local directory file path
        
        <s3path>                     s3 key, e.g. cst-compbio-research-00-buc/
    
        --force                      force upload even if local files have not 
                                     changed, ignore md5 cache.
        
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
        
        --localcache_fname FILENAME  file name to use for local cache.  Use this 
                                     arg to to explicity specify cache name or use 
                                     an existing cache file.
        
        --interval INTERVAL          enter any number greater than 0 to start 
                                     autosync mode, program will sync every 
                                     interval (min)
        
        --log LOGLEVEL               set the logger level (threshold), available 
                                     options include DEBUG, INFO, WARNING, ERROR, 
                                     or CRITICAL. [default: INFO]
        
        --log_dir LOGDIR             file path to directory in which to store the 
                                     logs. No log files are created if this option
                                     is ommited.
        -h --help                    show this screen.

```

### grab the container

```
    $ docker pull opensean/s3synccli:latest
```
or build the container from source.

```
    $ git clone https://github.com/opensean/s3synccli.git
    $ cd s3synccli
    $ docker build -t some_container_repo/s3synccli:lastest .
```

### container structure

*Important container directories*

```/s3sync/data```

The local directory to be synced to the s3 bucket should be mounted to the
 ```/s3sync/data``` directory of the container using the ```-v``` docker run 
arg.  For example, ```-v /path/to/local/data:/s3sync/data```.

```/s3sync/.s3ync```

A local directory to store the md5 cache should be mounted to the 
```/s3sync/.s3sync``` directory of the container usin the ```-v``` docker run 
arg.  For example, ```-v /path/to/local/cache:/s3sync/.s3sync```.

**Note:** The container will need the proper permissions to read from the 
directory mounted to ```/s3sync/data``` and read/write permissions for the 
directory mounted to ```/s3sync/.s3sync```.  For instance, the container will 
encounter permission errors when trying to read from an NFS due to root 
squashing.  To avoid permission errors run the container with a UID using the 
```-u``` docker run arg.  For example, ```-u 1000```.

```/s3sync/logs```

refer to the **logging** section of this readme.

### AWS credentials

AWS credentials can be shared with the container by mounting an .aws folder 
that contains the standard 'config' and 'credentials' files that are created 
when configuring the awscli.  One can also use envrironment variables via a 
.env file.

#### mounting .aws

If running the container **WITH** a UID the local .aws directory should be 
mounted here, ```-v /path/to/local/.aws:/.aws```

If running the container **WITHOUT** a UID the local .aws directory should be 
mounted here, ```-v /path/to/local/.aws:/root/.aws```


#### using environment variables


#### IAM credentials
AWS credentials can be passed to the container as environment variables using a
.env file with the following format:

```
    AWS_ACCESS_KEY_ID=youraccesskey
    AWS_SECRET_ACCESS_KEY=yoursecretaccesskey
    AWS_DEFAULT_REGION=defaultregion(e.g. us-east-1)
```

The .env file uses the convention VAR=varvalue.  The .env file can be passed 
to the container at run time using the ```--env-file``` docker run arg.  
For example, ```--env-file /path/to/.env```

Boto3 is the workhorse of s3synccli and supports the use of many different 
environment variables which can be referenced here:

http://boto3.readthedocs.io/en/latest/guide/configuration.html

#### assume role provider

**TO DO**

### run container as an executable

Put everything together and run the container as an exectuble.  For example,

```
    $ docker run --rm --env-file /path/to/env/.env -u 1000 \
                 -v /path/to/local/dir/:/s3sync/data \
                 -v /path/to/local/cache:/s3sync/.s3sync \
                 opensean/s3synccli:latest \
                 s3bucket/path/to/dir/

```

**Note:** when running the container again to sync the same directory pass the
```--localcache_fname``` arg to use the same local cache file otherwise the 
md5sums will be recalculated and store in a new cache with a new uuid.


Pass an ```--interval x``` (unit is minutes) arg to start autosync mode in
which the program will sync every x number of minutes as long as the container
is running.  Use the ```-d``` docker run arg to run the container in detached
mode.

```
    $ docker run --rm --env-file /path/to/env/.env -u 1000 \
                 -v /path/to/local/dir/:/s3sync/data \
                 -v /path/to/local/cache:/s3sync/.s3sync \
                 opensean/s3synccli:latest \
                 s3bucket/path/to/dir/ --interval 5

```

### start bash shell in the container

A shell can be started in the container to experiment with the python program or 
code directly by overiding the container entrypoint.  For example,

```
   $ docker run -it --rm --entrypoint bash --env-file /path/to/env/.env \ 
                -u 1000 \
                -v /path/to/local/dir/:/s3sync/data \
                -v /path/to/local/cache:/s3sync/.s3sync \
                opensean/s3synccli:latest \
                s3bucket/path/to/dir/ 

```

Once the shell session is active one can run the python code directly.

```
   $ python3 s3sync.py data s3bucket/path/to/dir/ --localcache --localcache_dir .s3sync 
```

### logging

s3synccli allows for the customization of logging behavior using the 
```--log``` and ```--log_dir``` args.  At build time the container
creates the directory ```/s3sync/logs``` for use as the 
```--logs_dir``` arg when logs files are required.  The following is an example
running the container as an executable with the logging threshold set to 
```DEBUG``` and a directory to store the logs files generated.

```
    $ docker run --rm --env-file /path/to/env/.env \
                 -u 1000 \
                 -v /path/to/local/dir/:/s3sync/data \
                 -v /path/to/local/cache:/s3sync/.s3sync \
                 -v /path/to/local/logs:/s3sync/logs \
                 opensean/s3synccli:latest \
                 s3bucket/path/to/dir/ \
                 --log DEBUG \
                 --log_dir /s3sync/logs
```

**Note:** if the ```--log_dir``` arg is ommited no log files are generated but
one can still customize the logging output to the console by setting the 
```--log``` arg.  For example,

```
    $ docker run --rm --env-file /path/to/env/.env \
                 -u 1000 \
                 -v /path/to/local/dir/:/s3sync/data \
                 -v /path/to/local/cache:/s3sync/.s3sync \
                 -v /path/to/local/logs:/s3sync/logs \
                 opensean/s3synccli:latest \
                 s3bucket/path/to/dir/ \
                 --log INFO

```

### Future

- docker compose
    - use a fleet of containers to sync multiple directories
    - easier to run
    - all args contained within docker-compose.yml

