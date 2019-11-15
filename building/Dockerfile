FROM lambci/lambda:build-python3.6

RUN pip3 install --upgrade pip

ADD requirements.txt /root/
RUN pip3 install --upgrade -r /root/requirements.txt
RUN rm -rf /root/requirements.txt
ADD requirements-dev.txt /root/
RUN pip3 install --upgrade -r /root/requirements-dev.txt
RUN rm -rf /root/requirements-dev.txt

ENTRYPOINT ["/bin/sh"]