FROM lablup/backendai-krunner-python:alpine3.8

ARG PREFIX=/opt/backend.ai

COPY requirements.txt /root/
# for installing source-distributed Python packages, we need build-base.
# (we cannot just run manylinux wheels in Alpine due to musl-libc)
RUN apk add --no-cache build-base
RUN ${PREFIX}/bin/pip install --no-cache-dir -U pip setuptools
RUN ${PREFIX}/bin/pip install --no-cache-dir -U -r /root/requirements.txt && \
    ${PREFIX}/bin/pip list

# Volume definition must come at last since all changes made to it afterwards
# are automatically discarded by Docker.
VOLUME ${PREFIX}

CMD ["${PREFIX}/bin/python"]

# vim: ft=dockerfile
