import threading
from abc import abstractmethod

from storescp.src.scp.models import SCPAssociation


class SCPReleaseHandlerInterface(threading.Thread):
    @abstractmethod
    def release(self, scp_association: SCPAssociation):
        pass