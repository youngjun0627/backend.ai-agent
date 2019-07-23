FROM lablup/backendai-krunner-python:centos7

ARG PREFIX=/opt/backend.ai

COPY requirements.txt /root/
RUN ${PREFIX}/bin/pip install --no-cache-dir -U pip setuptools
RUN ${PREFIX}/bin/pip install --no-cache-dir -U -r /root/requirements.txt && \
    ${PREFIX}/bin/pip list

# Volume definition must come at last since all changes made to it afterwards
# are automatically discarded by Docker.
VOLUME ${PREFIX}

CMD ["${PREFIX}/bin/python"]

# vim: ft=dockerfile
