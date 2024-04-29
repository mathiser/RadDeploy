import tarfile
from io import BytesIO

from RadDeployLib.data_structures.flow import Destination
from scp.models import SCPAssociationManager, SCPAssociation


def get_scp_association(assoc_id):
    return SCPAssociation(
        assoc_id=assoc_id,
        sender=Destination(host="127.0.0.1",
                           port=10000,
                           ae_title="ae_title"))


def test_scp_association_manager():
    man = SCPAssociationManager()

    # insert scp association
    ass1 = man.maybe_add_scp_association(get_scp_association(1))
    assert 1 in man.scp_associations.keys()
    ass1.add_dicom_file(BytesIO(b"Hello Dicom"))

    # Check that it does not overwrite
    man.maybe_add_scp_association(get_scp_association(1))
    assert ass1 == man.get_scp_association(1)

    # Check that delete works
    man.delete_scp_association(1)
    assert 1 not in man.scp_associations.keys()

    # Check that None is returned on non-existing assoc
    assert man.get_scp_association(404) is None

    # Check that as tar returns a proper tar file with one item:
    ass1 = man.maybe_add_scp_association(get_scp_association(1))
    dicom_file = BytesIO(b"Hello Dicom")

    ass1.add_dicom_file(dicom_file)
    tar = ass1.as_tar()

    with tarfile.TarFile.open(fileobj=tar) as tf:
        assert len(tf.getmembers()) == 1
        member = BytesIO(tf.extractfile("0.dcm").read())
        assert member.read() == dicom_file.read()
