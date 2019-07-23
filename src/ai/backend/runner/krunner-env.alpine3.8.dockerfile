FROM lablup/backendai-krunner-python:alpine3.8

ARG PREFIX=/opt/backend.ai

# for installing source-distributed Python packages, we need build-base.
# (we cannot just run manylinux wheels in Alpine due to musl-libc)
RUN apk add --no-cache build-base xz linux-headers
RUN ${PREFIX}/bin/pip install --no-cache-dir -U pip setuptools

COPY requirements.txt /root/
RUN ${PREFIX}/bin/pip install --no-cache-dir -U -r /root/requirements.txt && \
    ${PREFIX}/bin/pip list

# Create directories to be used for additional bind-mounts by the agent
RUN mkdir -p ${PREFIX}/lib/python3.6/site-packages/ai/backend/kernel && \
    mkdir -p ${PREFIX}/lib/python3.6/site-packages/ai/backend/helpers

# Build the image archive
RUN cd ${PREFIX}; \
    tar cJf /root/image.tar.xz ./*

LABEL ai.backend.krunner.version=1
CMD ["${PREFIX}/bin/python"]

# vim: ft=dockerfile
