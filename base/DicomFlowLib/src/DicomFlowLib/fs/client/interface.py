from abc import abstractmethod
from io import BytesIO


class FileStorageClientInterface:
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

    @abstractmethod
    def get_file_path(self, uid):
        pass

    @abstractmethod
    def file_exists(self, uid):
        pass

    @abstractmethod
    def remote_local_match(self, uid):
        pass

    @abstractmethod
    def write_file_to_disk(self, uid: str, file: BytesIO):
        pass
