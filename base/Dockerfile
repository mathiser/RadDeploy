FROM python:3.12-slim
LABEL authors="Mathis Ersted Rasmussen"

ENV APP_BASE_DIR="/opt/RadDeploy/"
ENV DEPENDENCIES_DIR=$APP_BASE_DIR/dependencies

RUN mkdir -p $DEPENDENCIES_DIR

ENV PYTHONUNBUFFERED=1

RUN chmod 753 -R $APP_BASE_DIR

ADD ./DicomFlowLib $DEPENDENCIES_DIR/DicomFlowLib
RUN pip install $DEPENDENCIES_DIR/DicomFlowLib

