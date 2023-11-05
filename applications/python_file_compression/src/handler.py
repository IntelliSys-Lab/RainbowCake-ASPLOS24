import datetime
import io
import os
import shutil
import uuid
import zlib

folder_name = "acmart-master"
local_path = "./"

def compress(path, key):
    shutil.make_archive(os.path.join(path, key), 'zip', root_dir=path)
    archive_name = '{}.zip'.format(key)
    archive_size = os.path.getsize(os.path.join(path, archive_name))

    return archive_name, archive_size

def handler(event, context=None):
    archive_name, archive_size = compress(local_path, folder_name)

    return {
        "result": "{} compression in size {} finished!".format(archive_name, archive_size)
    }


if __name__ == "__main__":
    event = {}
    print(handler(event))