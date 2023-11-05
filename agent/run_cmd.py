import subprocess

#
# Function utilities
# 

def run_cmd(cmd):
    pong = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    result = pong.stdout.read().decode().replace('\n', '')

    return result
