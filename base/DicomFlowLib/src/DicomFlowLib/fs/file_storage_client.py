from io import BytesIO

import requests

from DicomFlowLib.log import CollectiveLogger


class FileStorageClient:
    def __init__(self, logger: CollectiveLogger, file_storage_host: str, file_storage_port: int):
        super().__init__()
        self.logger = logger
        self.url = "http://" + file_storage_host + ":" + str(file_storage_port)

    def post(self, file: BytesIO) -> str:
        res = requests.post(self.url, files={"tar_file": file})
        if res.ok:
            return res.json()
        elif res.status_code == 404:
            raise FileNotFoundError
        else:
            self.logger.error(res.json())
            res.raise_for_status()

    def clone(self, uid):
        self.logger.debug(f"Clone file on uid: {uid}", finished=False)
        res = requests.put(self.url, params={"uid": uid})
        if res.ok:
            self.logger.debug(f"Clone file on uid: {uid}", finished=True)
            return res.json()
        elif res.status_code == 404:
            raise FileNotFoundError
        else:
            self.logger.error(res.json())
            res.raise_for_status()


    def get(self, uid: str):
        self.logger.debug(f"Serving file with uid: {uid}", finished=False)
        res = requests.get(self.url, params={"uid": uid})
        if res.ok:
            self.logger.debug(f"Serving file with uid: {uid}", finished=True)
            return BytesIO(res.content)
        elif res.status_code == 404:
            raise FileNotFoundError
        else:
            self.logger.error(res.json())
            res.raise_for_status()

    def delete(self, uid: str):
        self.logger.debug(f"Deleting file with uid: {uid}", finished=False)
        res = requests.delete(self.url, params={"uid": uid})

        if res.ok:
            self.logger.debug(f"Deleting file with uid: {uid}", finished=True)
            return res.json()
        elif res.status_code == 404:
            raise FileNotFoundError
        else:
            self.logger.error(res.json())
            res.raise_for_status()
