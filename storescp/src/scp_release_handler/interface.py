import threading
from abc import abstractmethod

from scp.models import SCPAssociation


class SCPReleaseHandlerInterface(threading.Thread):
    @abstractmethod
    def release(self, scp_association: SCPAssociation):
        pass