ARG BASE_IMAGE=mathiser/dicom_flow-base:latest
FROM ${BASE_IMAGE}

LABEL authors="Mathis Ersted Rasmussen"
ADD requirements /requirements
RUN pip install -r /requirements

COPY . ./

WORKDIR /opt/DicomFlow/src/

HEALTHCHECK --interval=30s --timeout=60s --retries=3 CMD curl http://file-storage:80/ || exit 1

ENTRYPOINT ["/usr/local/bin/python3", "main.py"]