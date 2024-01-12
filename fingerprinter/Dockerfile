FROM mathiser/dicom_flow-base:latest
LABEL authors="Mathis Ersted Rasmussen"
ADD requirements /requirements
RUN pip install -r /requirements

ENV FLOWS_DIR="/opt/DicomFlow/flow.d"
RUN mkdir -p $FLOWS_DIR

COPY . ./

WORKDIR /opt/DicomFlow/src/

ENTRYPOINT ["/usr/local/bin/python3", "main.py"]