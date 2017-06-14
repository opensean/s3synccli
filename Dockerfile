###############################
## s3syccli dockerfile

FROM fedora:25
LABEL maintainer "Sean Landry, sean.d.landry@gmail.com, sean.landry@cellsignal.com"
LABEL s3synccli.version="0.1" \
      s3synccli.description="python tool to sync local directory with s3 bucket while preserving metadata"

RUN useradd -ms /bin/bash s3synccli
COPY . /home/s3synccli/
RUN chown -R s3synccli /home/s3synccli
USER s3synccli
WORKDIR /home/s3synccli
