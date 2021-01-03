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

# Based on https://gist.github.com/diriver63/b72a954fa0da4851d89e5086aa13c6e8
RUN curl ftp://ftp.unixodbc.org/pub/unixODBC/unixODBC-2.3.9.tar.gz -O && \
    tar xvzf unixODBC-2.3.9.tar.gz && \
    cd unixODBC-2.3.9 && \
    mkdir /odbc-build && \
    ./configure --sysconfdir=/opt --disable-gui --disable-drivers --enable-iconv \
     --with-iconv-char-enc=UTF8 --with-iconv-ucode-enc=UTF16LE --prefix=/opt && \
    make && \
    make install && \
    cd .. && \
    rm -r unixODBC-2.3.9 && \
    rm unixODBC-2.3.9.tar.gz

RUN curl https://packages.microsoft.com/config/rhel/7/prod.repo > /etc/yum.repos.d/mssql-release.repo && \
    ACCEPT_EULA=Y yum -y install msodbcsql17 unixODBC-devel && \
    export CFLAGS="-I/opt/include" && \
    export LDFLAGS="-L/opt/lib" && \
    cd /opt && \
    cp -r /opt/microsoft/msodbcsql17/ . && \
    rm -rf /opt/microsoft/ && \
    printf "[ODBC Driver 17 for SQL Server]\n\nDriver=/opt/msodbcsql17/lib64/libmsodbcsql-17.6.so.1.1\n\nUsageCount=1\n" > /opt/odbcinst.ini && \
    printf "[ODBC Driver 17 for SQL Server]\n\nDriver = ODBC Driver 17 for SQL Server\n\n Trace = No\n" > /opt/odbc.ini

RUN pip3 install --upgrade pip six cython cmake hypothesis

ADD requirements.txt /root/
RUN pip3 install -r /root/requirements.txt

ADD requirements-dev.txt /root/
# Removing "-e ." installation
RUN head -n -2 /root/requirements-dev.txt > /root/temp.txt
RUN mv /root/temp.txt /root/requirements-dev.txt
RUN pip3 install -r /root/requirements-dev.txt

RUN rm -rf /root/requirements*

ENTRYPOINT ["/bin/sh"]