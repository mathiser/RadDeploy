import hashlib
from io import BytesIO


def hash_buf(buf, buffer_size=65536):
    sha1 = hashlib.sha1()

    while True:
        data = buf.read(buffer_size)
        if not data:
            break
        sha1.update(data)

    return sha1.hexdigest()


def hash_file(file: str | BytesIO, buffer_size=65536):
    # BUF_SIZE is totally arbitrary, change for your app!
    if isinstance(file, str):
        with open(file, 'rb') as f:
            return hash_buf(f, buffer_size)
    else:
        file.seek(0)
        hash = hash_buf(file)
        file.seek(0)
        return hash
