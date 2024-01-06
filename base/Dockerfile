FROM python:3.12-slim
LABEL authors="Mathis Ersted Rasmussen"

RUN apt-get update &&\
    apt-get install -y git  &&\
    rm -rf /var/cache/apt

ENV LOG_DIR="/opt/DicomFlow/logs"
ENV FILES_DIR="/opt/DicomFlow/files"
ENV SRC_DIR="/opt/DicomFlow/src"
ENV DEPENDENCIES_DIR="/opt/DicomFlow/dependencies"
RUN mkdir -p  $LOG_DIR $FILES_DIR $SRC_DIR $DEPENDENCIES_DIR

COPY ./DicomFlowLib $DEPENDENCIES_DIR/DicomFlowLib
RUN pip install -e $DEPENDENCIES_DIR/DicomFlowLib

WORKDIR $SRC_DIR
