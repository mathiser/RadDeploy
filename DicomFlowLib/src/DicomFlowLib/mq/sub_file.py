<<<<<<< HEAD
import hashlib
from io import BytesIO

from .base import MQBase


class MQSubFile(MQBase):
    def __init__(self, hostname: str, port: int, log_level: int = 10):
        super().__init__(hostname, port, log_level)

    def consume_queue_to_file(self, meta):
        q = self.declare_queue(queue=meta["queue"])

        file = BytesIO()
        while True:
            try:
                method_frame, header_frame, body = chan.basic_get(meta["queue"])
                if method_frame:
                    self.logger.error(method_frame)
                    file.write(body)
                    chan.basic_ack(method_frame.delivery_tag)
                else:
                    self.logger.debug('End of queue')

                    # Reset pointer
                    file.seek(0)

                    # Checksum files
                    checksum = self.checksum(file)
                    if checksum == meta["checksum"]:
                        return file
                    else:
                        raise Exception("File is corrupt")
            except Exception as e:
                self.logger.error(e)
                raise e
            finally:
                self.tear_down_connection_and_channel(connection=conn, channel=chan)

    def checksum(self, tmpfile: BytesIO):
        hash = hashlib.sha256(tmpfile.read()).hexdigest()
        tmpfile.seek(0)
        return hash

if __name__ == "__main__":
    pass
=======
# import hashlib
# from io import BytesIO
#
# from .pub import MQPub
#
#
# class MQSubFile(MQPub):
#     def __init__(self, hostname: str, port: int, log_level: int = 10):
#         super().__init__(hostname, port, log_level)
#
#     def consume_queue_to_file(self, meta):
#         q = self.declare_queue(queue=meta["queue"])
#
#         file = BytesIO()
#         conn, chan = self.set_connection_and_channel()
#         while True:
#             try:
#                 method_frame, header_frame, body = chan.basic_get(meta["queue"])
#                 if method_frame:
#                     self.logger.error(method_frame)
#                     file.write(body)
#                     chan.basic_ack(method_frame.delivery_tag)
#                 else:
#                     self.logger.debug('End of queue')
#
#                     # Reset pointer
#                     file.seek(0)
#
#                     # Checksum files
#                     checksum = self.checksum(file)
#                     if checksum == meta["checksum"]:
#                         return file
#                     else:
#                         raise Exception("File is corrupt")
#             except Exception as e:
#                 self.logger.error(e)
#                 raise e
#             finally:
#                 self.close(connection=conn, channel=chan)
#
#     def checksum(self, tmpfile: BytesIO):
#         hash = hashlib.sha256(tmpfile.read()).hexdigest()
#         tmpfile.seek(0)
#         return hash
#
# if __name__ == "__main__":
#     pass
>>>>>>> ab68691
