from abc import abstractmethod
from io import BytesIO


class FileStorageClientInterface:

    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def post(self, file: BytesIO):
        pass

    @abstractmethod
    def clone(self, uid):
        pass

    @abstractmethod
    def get_hash(self, uid: str):
        pass

    @abstractmethod
    def exists(self, uid: str):
        pass

    @abstractmethod
    def get(self, uid: str):
        pass

    @abstractmethod
    def delete(self, uid: str):
        pass


