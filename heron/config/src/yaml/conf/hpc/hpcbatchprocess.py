import sys
import subprocess
import time

def main(argv):
    start_process(argv)
    check()
    pass

def start_process(argv):
    print argv[1:]
    subprocess.Popen(argv[1:])
    pass

def check():
    count = 0
    while True:
        time.sleep(10)
        if count == 100:
            break
        count += 1

if __name__ == "__main__":
    main(sys.argv)