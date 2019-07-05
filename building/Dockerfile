FROM lambci/lambda:build-python3.7

RUN pip install --upgrade pip

ADD requirements.txt /root/
RUN pip install -r /root/requirements.txt
RUN rm -rf /root/requirements.txt
ADD requirements-dev.txt /root/
RUN pip install -r /root/requirements-dev.txt
RUN rm -rf /root/requirements-dev.txt

ENTRYPOINT ["/bin/sh"]