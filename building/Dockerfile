FROM lambci/lambda:build-python3.7

RUN yum install -y \
    boost-devel \
    jemalloc-devel \
    bison \
    flex \
    autoconf \
    python37-devel

RUN pip3 install --upgrade pip

RUN pip3 install --upgrade six numpy pandas cython pytest cmake wheel

ADD requirements.txt /root/
RUN pip3 install --upgrade -r /root/requirements.txt
RUN rm -rf /root/requirements.txt
ADD requirements-dev.txt /root/
RUN pip3 install --upgrade -r /root/requirements-dev.txt
RUN rm -rf /root/requirements-dev.txt

RUN pip3 install boto3==1.10.34
RUN pip3 install botocore==1.13.34

ENTRYPOINT ["/bin/sh"]