ARG BASE_IMAGE=mathiser/dicom_flow-base:latest
FROM ${BASE_IMAGE}

LABEL authors="Mathis Ersted Rasmussen"
ADD requirements /requirements
RUN pip install -r /requirements

ENV FLOWS_DIR="/opt/DicomFlow/flows"
RUN mkdir -p $FLOWS_DIR

COPY . ./

WORKDIR /opt/DicomFlow/src/

ENTRYPOINT ["/usr/local/bin/python3", "main.py"]