from pydantic import BaseModel


class FileContext(BaseModel):
    file: bytes


# class FileContext(BaseContext):
#     class Config:
#         arbitrary_types_allowed = True
#
#     file: BytesIO | None = None
#     checksum: str | None = None
#
#     def __del__(self):
#         if self.file is not None:
#             self.file.close()
#
#     def add_file(self, file: BytesIO):
#         self.file = file
#         self.checksum = hashlib.sha256(self.file.read()).hexdigest()
#         self.file.seek(0)
#
#     def generate_tar_from_path(self, path):
#         self.file = BytesIO()
#         with tarfile.TarFile.open(fileobj=self.file, mode="w") as tf:
#             tf.add(path, arcname=path)
#         self.file.seek(0)
#         self.checksum = hashlib.sha256(self.file.read()).hexdigest()
#         self.file.seek(0)
#
#     def iter_file(self):
#         assert self.file
#         try:
#             while chunk := self.file.read(1024 * 64):
#                 yield chunk
#         finally:
#             self.file.seek(0)