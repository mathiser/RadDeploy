FROM mathiser/dicom_flow-base:latest
LABEL authors="Mathis Ersted Rasmussen"
ADD requirements /requirements
RUN pip install -r /requirements

COPY . ./

WORKDIR /opt/DicomFlow/src/

ENTRYPOINT ["/usr/local/bin/python3", "main.py"]