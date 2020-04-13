ARG base_image
ARG py_dev

FROM ${base_image}

RUN yum install -y \
    boost-devel \
    jemalloc-devel \
    bison \
    flex \
    autoconf \
    ninja-build \
    ${py_dev}

RUN pip3 install --upgrade pip six cython cmake hypothesis

ADD requirements.txt /root/
RUN pip3 install -r /root/requirements.txt

ADD requirements-dev.txt /root/
RUN pip3 install -r /root/requirements-dev.txt

RUN rm -rf /root/requirements*

ENTRYPOINT ["/bin/sh"]