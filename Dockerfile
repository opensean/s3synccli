#########################
## s3syccli dockerfile ##
#########################

FROM fedora:25
LABEL maintainer "Sean Landry, sean.d.landry@gmail.com, sean.landry@cellsignal.com"
LABEL s3synccli.version="0.1" \
      s3synccli.description="python tool to sync local directory or file with an s3 bucket while preserving metadata"

RUN dnf install -y python3-boto3 python3-docopt python3-magic
                   
RUN mkdir /s3sync
COPY . /s3sync/
RUN mkdir /s3sync/data
RUN mkdir /s3sync/.s3sync
RUN chmod -R o+rwx /s3sync/

WORKDIR /s3sync
