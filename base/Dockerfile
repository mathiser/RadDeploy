FROM python:3.12-slim
LABEL authors="Mathis Ersted Rasmussen"

ENV APP_DIR="/opt/DicomFlow/"
ENV LOG_DIR="/opt/DicomFlow/logs"
ENV CONF_DIR="/opt/DicomFlow/conf.d"
ENV FILES_DIR="/opt/DicomFlow/files"
ENV DEPENDENCIES_DIR="/opt/DicomFlow/dependencies"
RUN mkdir -p  $LOG_DIR $FILES_DIR $DEPENDENCIES_DIR

ADD ./DicomFlowLib $DEPENDENCIES_DIR/DicomFlowLib
RUN pip install $DEPENDENCIES_DIR/DicomFlowLib

WORKDIR $APP_DIR
