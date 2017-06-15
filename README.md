# s3synccli docker container

## build container

The following needs to be run in the same directory as the Dockerfile.

```
    docker build -t compbio-research/s3synccli:0.1 .
```

## running container bash session

Start a bash shell within the container.

```
    docker run -it --rm compbio-research/s3synccli:0.1 bash
```

## volumes, .env, UID

*Important container directories*

The s3sync program will sync data from '/s3sync/data' and cache any md5 data
in '/s3sync/.s3sync'

Mount the local directory to be synced to the '/s3sync/data' directory of the
container and mount a local directory to the '/s3sync/.s3sync' directory of the 
container to store the md5 cache.

AWS credentials can be passed to the container using a .env file.  The 
program will look for the following environment variables:

AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION

The .env file uses the convention VAR=varvalue.

The container should be passed a UID using the '-u' arg to ensure it has proper
permission to read from the mounted data directory and write to the mounted 
md5 cache directory.

### running the container with mounted volumes, .env, and UID

Example

```
    docker run -it --rm --env-file /path/to/env/.env -u 1000  -v /path/to/local/dir/:/s3sync/data -v /path/to/local/cache:/s3sync/.s3sync some_container_repo/s3synccli:0.1 bash
```

### Execute sync to s3 bucket

```
   python3 s3sync.py data s3bucket/path/to/dir/ --localcache 
```
