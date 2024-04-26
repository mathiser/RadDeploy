ARG BASE_IMAGE=mathiser/dicom_flow-base:latest
FROM ${BASE_IMAGE}

LABEL authors="Mathis Ersted Rasmussen"
ADD requirements /requirements
RUN pip install -r /requirements

ENV APP_DIR=/opt/RadDeploy/storescp
RUN mkdir -p $APP_DIR
WORKDIR $APP_DIR
COPY . ./
WORKDIR $APP_DIR/src

ENV CONF_DIR=$APP_DIR/conf.d
ENV CURRENT_CONF=$APP_DIR/current_config.yaml

HEALTHCHECK --interval=30s --timeout=60s --retries=3 CMD /usr/local/bin/python3 healthcheck.py || exit 1
ENTRYPOINT ["/usr/local/bin/python3", "main.py"]