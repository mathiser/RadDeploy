FROM mathiser/dicom_flow-base:latest
LABEL authors="Mathis Ersted Rasmussen"
ADD requirements /requirements
RUN pip install -r /requirements

COPY . ./

CMD ["bash", "-c", "python3 main.py"]