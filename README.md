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
        s3sync <localdir> <s3path> [--metadata METADATA] [--meta-dir-mode METADIR]
                                   [--meta-file-mode METAFILE] [--uid UID] 
                                   [--gid GID] [--profile PROFILE] [--localcache] 
                                   [--localcache-dir CACHEDIR] 
                                   [--localcache-fname FILENAME] 
                                   [--interval INTERVAL] [--force]
                                   [--log LOGLEVEL] [--log-dir LOGDIR]
        s3sync -h | --help 
    
    Options: 
        <localdir>                   local directory file path
        
        <s3path>                     s3 key, e.g. cst-compbio-research-00-buc/
    
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
squashing.  To avoid permission errors run the container with a UID and GID 
using the ```-u``` docker run arg.  For example, ```-u 1000:1000```.
It is important to pass both a UID and GID otherwise any files written by the 
container may have the incorrect permissions.

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
    $ docker run --rm --env-file /path/to/env/.env -u 1000:1000 \
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
    $ docker run --rm --env-file /path/to/env/.env -u 1000:1000 \
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
                -u 1000:1000 \
                -v /path/to/local/dir/:/s3sync/data \
                -v /path/to/local/cache:/s3sync/.s3sync \
                opensean/s3synccli:latest \
                s3bucket/path/to/dir/ 

```

Once the shell session is active one can run the python code directly.

```
   $ python3 s3sync.py data s3bucket/path/to/dir/ --localcache --localcache-dir .s3sync 
```

### logging

s3synccli allows for the customization of logging behavior using the 
```--log``` and ```--log-dir``` args.  At build time the container
creates the directory ```/s3sync/logs``` for use with the 
```--logs-dir LOGDIR``` arg when logs files are required.  The following is an
example running the container as an executable with the logging threshold set to 
```DEBUG``` and a directory to store the logs files generated.

```
    $ docker run --rm --env-file /path/to/env/.env \
                 -u 1000:1000 \
                 -v /path/to/local/dir/:/s3sync/data \
                 -v /path/to/local/cache:/s3sync/.s3sync \
                 -v /path/to/local/logs:/s3sync/logs \
                 opensean/s3synccli:latest \
                 s3bucket/path/to/dir/ \
                 --log DEBUG \
                 --log-dir /s3sync/logs
```

**Note:** if the ```--log-dir``` arg is ommited no log files are generated but
one can still customize the logging output to the console by setting the 
```--log``` arg.  For example,

```
    $ docker run --rm --env-file /path/to/env/.env \
                 -u 1000:1000 \
                 -v /path/to/local/dir/:/s3sync/data \
                 -v /path/to/local/cache:/s3sync/.s3sync \
                 -v /path/to/local/logs:/s3sync/logs \
                 opensean/s3synccli:latest \
                 s3bucket/path/to/dir/ \
                 --log INFO

```

### docker-compose

The following are the contents of the ```docker-compose.yml``` file contained 
in this repository that demonstrates an example of syncing the same local 
directory to multiple buckets using docker-compose.  A docker-compose.yml can 
be configure to accomplish scenarios such as the one show below, syncing 
multiple directories to (different or same) bucket, etc...


#### environment varialbes

Environment variables are confusing when it comes to docker-compose and the 
documentation does not explain the difference between an *env_file* and  an 
*.env* file well.  

The *.env* can contain variables to be substituted in the compose file itself.  
The *.env* used is local to where the docker-compose command is run.  For 
example, if a I run the ```docker-compose up``` command in my */home/opensean/*
directory, then docker will look for a *.env* file in */home/opensean/*, from 
what I understand you can't control that behavoir of docker-compose. 
CAUTION any existing shell environment variables will override the 
variables specified in the *.env*.


The *env_file* key in a docker-compose file allows one to specify environment 
variables that are shared with the containers environment.  The values are not 
substituted into a docker-compose file but shared injected directly into the 
container's environment.


*docker-compose.yml*

```
    version: '3'
    services:
        s3sync00:
                env_file: example.env
                image: opensean/s3synccli:latest
                container_name: s3sync00
                command: example-s3-00-buc/home/docs --interval 5 --log-dir /s3sync/logs
                volumes:
                        - /local/path/to/docs:/s3sync/data
                        - /local/path/to/logs:/s3sync/logs
                        - /local/path/to/.s3sync:/s3sync/.s3sync
               
                user: $MY_USER:$MY_GROUP

        s3sync01:
                env_file: example.env
                image: opensean/s3synccli:latest
                container_name: s3sync01
                command: example-s3-01-buc/dir1/docs --interval 5 --log-dir /s3sync/logs
                volumes:
                        - /local/path/to/docs:/s3sync/data
                        - /local/path/to/logs:/s3sync/logs
                        - /local/path/to/.s3sync:/s3sync/.s3sync
               
                user: $MY_USER:$MY_GROUP

```
*.env*

```
    MY_USER=1000
    MY_GROUP=1000
```

*example.env*

```
    AWS_ACCESS_KEY_ID=youraccesskey
    AWS_SECRET_ACCESS_KEY=yoursecretaccesskey
    AWS_DEFAULT_REGION=defaultregion(e.g. us-east-1)
```
