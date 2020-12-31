FROM gitpod/workspace-full

# Install custom tools, runtimes, etc.
# For example "bastet", a command-line tetris clone:
# RUN brew install bastet
#
# More information: https://www.gitpod.io/docs/config-docker/

#  - wmcore: wget <wmcore.tgz> && tar <> && export PYTHONPATH
#  - fts3-rest: wget <> && tar && export
#  - rucio
RUN wget https://github.com/dmwm/WMCore/archive/1.3.6.crab5.tar.gz && \
    tar -xzvf 1.3.6.crab5.tar.gz && \
    mv WMCore-1.3.6.crab5 /home/gitpod/WMCore

RUN wget https://gitlab.cern.ch/fts/fts-rest/-/archive/v3.9.4/fts-rest-v3.9.4.tar.gz && \
    tar -xzvf fts-rest-v3.9.4.tar.gz && \
    mv fts-rest-v3.9.4 /home/gitpod/fts3-rest

RUN pip install rucio-clients

ENV PYTHONPATH=$PYTHONPATH:/home/gitpod/WMCore/src/python:/home/gitpod/fts3-rest/src