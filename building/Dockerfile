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
RUN pip3 install --upgrade -r /root/requirements.txt
RUN rm -rf /root/requirements.txt

RUN pip3 install boto3==1.10.34 botocore==1.13.34

ENTRYPOINT ["/bin/sh"]