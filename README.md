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

The local directory to be synce to the s3 bucket should be mounted to the
 ```/s3sync/data``` container directory

```/s3sync/.s3ync```



The s3sync program will sync data from '/s3sync/data' and cache any md5 data
to '/s3sync/.s3sync'.

Mount the local directory to be synced to the '/s3sync/data' directory of the
container and mount a local directory to the '/s3sync/.s3sync' directory of the
container to store the md5 cache.

### explore the container bash session

Start a bash shell within the container by overiding the entrypoint.

```
    docker run -it --rm --entrypoint bash some_container_repo/s3synccli:latest
```

## AWS Credentials
##  .env

*Important container directories*

The s3sync program will sync data from '/s3sync/data' and cache any md5 data
to '/s3sync/.s3sync'.

Mount the local directory to be synced to the '/s3sync/data' directory of the
container and mount a local directory to the '/s3sync/.s3sync' directory of the 
container to store the md5 cache.

AWS credentials can be passed to the container as environment variables using a 
.env file with the following format:

```
    AWS_ACCESS_KEY_ID=youraccesskey
    AWS_SECRET_ACCESS_KEY=yoursecretaccesskey
    AWS_DEFAULT_REGION=defaultregion(e.g. us-east-1)
```

The .env file uses the convention VAR=varvalue.

### UID

The container should be passed a UID using the '-u' arg to ensure it has proper
permission to read from the mounted data directory and write to the mounted 
md5 cache directory.

### running the container with mounted volumes, .env, and UID

Example

```
    docker run -it --rm --env-file /path/to/env/.env -u 1000  -v /path/to/local/dir/:/s3sync/data -v /path/to/local/cache:/s3sync/.s3sync some_container_repo/s3synccli:0.1 bash
```

### Execute sync to s3 bucket

Once the shell session is active the following can be run to sync the local 
directory with and s3 bucket.

```
   python3 s3sync.py data s3bucket/path/to/dir/ --localcache --localcache_dir .s3sync 
```

### running the container as an executable

```
    docker run -it --rm --env-file /path/to/env/.env -u 1000  -v /path/to/local/dir/:/s3sync/data -v /path/to/local/cache:/s3sync/.s3sync some_container_repo/s3synccli:0.1 s3bucket/path/to/dir/

```

Pass an ```--interval x``` (unit is minutes) arg to start autosync mode in which the program will sync every x number of minutes.

```
    docker run -it --rm --env-file /path/to/env/.env -u 1000  -v /path/to/local/dir/:/s3sync/data -v /path/to/local/cache:/s3sync/.s3sync some_container_repo/s3synccli:0.1 s3bucket/path/to/diri/ --interval 5

```


### Future

- docker compose
    - use a fleet of containers to sync multiple directories
    - easier to run
    - entry command contained within docker-compose.yml

