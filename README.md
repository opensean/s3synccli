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

## mounting local volumes for s3synccli


### data

### local cache

### logs
