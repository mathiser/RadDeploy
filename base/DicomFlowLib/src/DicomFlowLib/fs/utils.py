import hashlib


def hash_file(path, buffer_size=65536):
    # BUF_SIZE is totally arbitrary, change for your app!
    sha1 = hashlib.sha1()

    with open(path, 'rb') as f:
        while True:
            data = f.read(buffer_size)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()