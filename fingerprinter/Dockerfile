FROM mathiser/dicom_flow-base:latest
LABEL authors="Mathis Ersted Rasmussen"
ADD requirements /requirements
RUN pip install -r /requirements

ENV FLOWS_DIR="/opt/DicomFlow/flows"
RUN mkdir -p $FLOWS_DIR

COPY . ./

CMD ["bash", "-c", "python3 main.py"]