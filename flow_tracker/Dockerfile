ARG BASE_IMAGE=mathiser/dicom_flow-base:latest
FROM ${BASE_IMAGE}

LABEL authors="Mathis Ersted Rasmussen"
ADD requirements /requirements
RUN pip install -r /requirements

ENV APP_DIR=/opt/RadDeploy/file_storage

RUN mkdir -p $APP_DIR
WORKDIR $APP_DIR
COPY . ./
WORKDIR $APP_DIR/src

ENV CONF_DIR=$APP_DIR/conf.d
ENV CURRENT_CONF=$APP_DIR/current_config.yaml

ENTRYPOINT ["/usr/local/bin/python3", "main.py"]