import os
import sys
import stat
import subprocess

import ffmpeg

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

image_name = "watermark.png"
video_name = "hi_chitanda_eru.mp4"
local_path = "./"


def call_ffmpeg(args):
    ret = subprocess.run([os.path.join("./", 'ffmpeg', 'ffmpeg'), '-y'] + args,
            #subprocess might inherit Lambda's input for some reason
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    # if ret.returncode != 0:
    #     print('Invocation of ffmpeg failed!')
    #     print('Out: ', ret.stdout.decode('utf-8'))
    #     raise RuntimeError()

# https://github.com/kkroening/ffmpeg-python
def to_video(duration):
    output = 'processed_hi_chitanda_eru.mp4'
    call_ffmpeg([
        "-i", local_path + video_name,
        "-i", local_path + image_name,
        "-t", "{}".format(duration),
        "-filter_complex", "[0]trim=start_frame=0:end_frame=50[v0];\
        [0]trim=start_frame=100:end_frame=150[v1];[v0][v1]concat=n=2[v2];[1]hflip[v3];\
        [v2][v3]overlay=eof_action=repeat[v4];[v4]drawbox=50:50:120:120:red:t=5[v5]",
        "-map", "[v5]",
        local_path + output])

    return "Video {} finished!".format(output)

def handler(event, context=None):
    duration = 5

    # Restore executable permission
    ffmpeg_binary = os.path.join("./", 'ffmpeg', 'ffmpeg')
    st = os.stat(ffmpeg_binary)
    os.chmod(ffmpeg_binary, st.st_mode | stat.S_IEXEC)

    # Process media
    result = to_video(duration)

    return {
        "result": result
    }

if __name__ == "__main__":
    event = {
        "duration": 10
    }

    print(handler(event))
    