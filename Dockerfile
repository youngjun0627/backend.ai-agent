FROM python:3.6

VOLUME /usr/src/sorna-agent
VOLUME kernel-volumes:/var/lib/sorna/volumes
WORKDIR /usr/src/sorna-agent

# During build, we need to copy the initial version.
# Afterwards, we mount the host working copy as a volume here.
COPY . /usr/src/sorna-agent

RUN pip install -U pip wheel setuptools
RUN pip install -r requirements-dev.txt

CMD python -m sorna.agent
