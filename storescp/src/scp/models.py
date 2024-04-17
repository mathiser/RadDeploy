import tarfile
from io import BytesIO
from typing import List, Dict, Any

from pydantic import BaseModel, ConfigDict

from DicomFlowLib.data_structures.flow import Destination


class SCPAssociation(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    assoc_id: int
    sender: Destination
    dicom_files: List[BytesIO] = []
    src_uid: str | None = None

    def add_dicom_file(self, dicom_file: BytesIO):
        dicom_file.seek(0)
        self.dicom_files.append(dicom_file)
        dicom_file.seek(0)

    def as_tar(self) -> BytesIO:
        tar_file = BytesIO()
        with tarfile.TarFile.open(fileobj=tar_file, mode="w") as tf:
            for i, dicom_file in enumerate(self.dicom_files):
                dicom_file.seek(0, 2)
                info = tarfile.TarInfo(name=f"{i}.dcm")
                info.size = dicom_file.tell()
                dicom_file.seek(0)
                tf.addfile(info, dicom_file)
                dicom_file.seek(0)
        tar_file.seek(0)
        return tar_file


class SCPAssociationManager:
    def __init__(self):
        self.scp_associations: Dict[int, SCPAssociation] = {}

    def maybe_add_scp_association(self, scp_association: SCPAssociation) -> SCPAssociation:
        # Check if already created
        if not self.get_scp_association(scp_association.assoc_id):
            # Add to assoc dict
            self.scp_associations[scp_association.assoc_id] = scp_association

        return self.get_scp_association(assoc_id=scp_association.assoc_id)

    def get_scp_association(self, assoc_id) -> SCPAssociation | None:
        if assoc_id in self.scp_associations.keys():
            return self.scp_associations[assoc_id]
        else:
            return None

    def delete_scp_association(self, assoc_id: int):
        try:
            del self.scp_associations[assoc_id]
        except:
            pass