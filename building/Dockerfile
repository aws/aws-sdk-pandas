FROM lambci/lambda:build-python3.6

RUN yum install -y \
    boost-devel \
    jemalloc-devel \
    bison \
    flex \
    autoconf \
    python36-devel

RUN pip3 install --upgrade pip six cython pytest cmake wheel

ADD requirements.txt /root/
RUN pip3 install -r /root/requirements.txt
RUN rm -rf /root/requirements.txt
ADD requirements-dev.txt /root/
RUN pip3 install -r /root/requirements-dev.txt
RUN rm -rf /root/requirements-dev.txt

RUN pip3 install boto3==1.12.22 botocore==1.15.22

ENTRYPOINT ["/bin/sh"]