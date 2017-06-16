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
        s3sync <localdir> <s3path> [--metadata METADATA --meta_dir_mode METADIR --meta_file_mode METAFILE --uid UID --gid GID --profile PROFILE --localcache --localcache_dir CACHEDIR --interval INTERVAL]
        s3sync -h | --help 
    
    Options: 
        <localdir>                        local directory file path
        <s3path>                          s3 key, e.g. cst-compbio-research-00-buc/
        --metadata METADATA               metadata in json format e.g. '{"uid":"6812", "gid":"6812"}'
        --meta_dir_mode METADIR           mode to use for directories in metadata if none is found locally [default: 509]
        --meta_file_mode METAFILE         mode to use for files in metadata if none if found locally [default: 33204]
        --profile PROFILE                 aws profile name 
        --uid UID                         user id that will overide any uid information detected for files and directories
        --gid GID                         group id that will overid any gid information detected for files and directories
        --localcache                      use local data stored in .s3sync/s3sync_md5_cache.json.gz to save on md5sum computation.
        --localcache_dir CACHEDIR         directory in which to store local_md5_cache.json.gz, default: os.path.join(os.environ.get('HOME'), '.s3sync') 
        --interval INTERVAL               enter any number greater than 0 to start autosync mode, program will sync every interval (min)
        -h --help                         show this screen.
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

*Note: The container will need the proper permissions to read from the 
directory mounted to ```/s3sync/data``` and read/write permissions for the 
directory mounted to ```/s3sync/.s3sync```.  For instance, the container will 
encounter permission errors when trying to read from an NFS due to root 
squashing.  To avoid permission errors run the container with a UID using the 
```-u``` docker run arg.  For example, ```-u 1000```.

### AWS credentials

AWS credentials can be shared with the container by mounting an .aws folder 
that contains the standard 'config' and 'credentials' files that are created 
when configuring the awscli or using envrironment variables via a .env file.

#### mounting .aws

If running the container **WITH** a UID the local .aws directory should be 
mounted here, ```-v /path/to/local/.aws:/.aws```

If running the container **WITHOUT** a UID the local .aws directory should be 
mounted here, ```-v /path/to/local/.aws:/root/.aws```


#### using environment variables

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


### run container as an executable

Put everything together and run the container as an exectuble.  For example,

```
    $ docker run -it --rm --env-file /path/to/env/.env -u 1000 \
                 -v /path/to/local/dir/:/s3sync/data \
                 -v /path/to/local/cache:/s3sync/.s3sync \
                 some_container_repo/s3synccli:0.1 \
                 s3bucket/path/to/dir/

```

Pass an ```--interval x``` (unit is minutes) arg to start autosync mode in
which the program will sync every x number of minutes as long as the container
is running.  Use the ```-d``` docker run arg to run the container in detached
mode.

```
    $ docker run -it --rm --env-file /path/to/env/.env -u 1000 \
                 -v /path/to/local/dir/:/s3sync/data \
                 -v /path/to/local/cache:/s3sync/.s3sync \
                 some_container_repo/s3synccli:0.1 \
                 s3bucket/path/to/dir/ --interval 5

```

### start bash shell in the container

A shell can be started in the container to experiment with python program or 
code directly by overiding the container entrypoint.  For example,

```
   $ docker run -it --rm --entrypoint bash --env-file /path/to/env/.env \ 
               -u 1000 \
               -v /path/to/local/dir/:/s3sync/data \
               -v /path/to/local/cache:/s3sync/.s3sync \
               some_container_repo/s3synccli:0.1 \
               s3bucket/path/to/dir/ 

```

Once the shell session is active one can run the python code directly.

```
   $ python3 s3sync.py data s3bucket/path/to/dir/ --localcache --localcache_dir .s3sync 
```

### Future

- docker compose
    - use a fleet of containers to sync multiple directories
    - easier to run
    - all args contained within docker-compose.yml

