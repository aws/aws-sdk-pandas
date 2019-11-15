FROM openjdk:8-jre-stretch

ARG SPARK_VERSION=2.4.4

RUN apt-get update -y
RUN apt-get install -y jq make build-essential libssl-dev zlib1g-dev libbz2-dev \
libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
xz-utils tk-dev libffi-dev liblzma-dev python-openssl git gcc curl postgresql libpq-dev
RUN curl https://pyenv.run | bash
RUN echo 'eval "$(pyenv init -)"' >> /root/.bashrc
ENV PATH="/root/.pyenv/bin:$PATH"
RUN pyenv install 3.6.8
RUN pyenv install 3.7.4
RUN pyenv global 3.7.4 3.6.8
ENV PYSPARK_PYTHON=python
ENV PIP=/root/.pyenv/shims/pip
RUN $PIP install --upgrade pip
RUN $PIP install --no-cache pyspark==${SPARK_VERSION}
RUN eval "$(pyenv init -)" && \
    export SPARK_HOME=$(python -c "import sys; print([x for x in sys.path if x.endswith('site-packages')][0])")/pyspark && \
    curl --url "http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar" --output ${SPARK_HOME}/jars/aws-java-sdk-1.7.4.jar && \
    curl --url "http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar" --output ${SPARK_HOME}/jars/hadoop-aws-2.7.3.jar && \
    mkdir -p ${SPARK_HOME}/conf && \
    echo spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem >> ${SPARK_HOME}/conf/spark-defaults.conf
ADD requirements.txt /root/
RUN $PIP install --upgrade -r /root/requirements.txt
RUN rm -rf /root/requirements.txt
ADD requirements-dev.txt /root/
RUN $PIP install --upgrade -r /root/requirements-dev.txt
RUN rm -rf /root/requirements-dev.txt

ENTRYPOINT ["/bin/sh"]