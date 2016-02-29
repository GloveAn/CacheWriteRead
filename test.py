#! /usr/python
# python test.py /dev/mapper/test 1048576

import sys
import os

if(len(sys.argv) < 3):
    print("usage:")
    print("    python test.py /dev/mapper/test 1048576")
    quit()

name = sys.argv[1]
length = int(sys.argv[2])
sector = [i % 256 for i in range(512)]

fd = os.open(name, os.O_RDWR)
if fd < 0:
    print("open " + name + " fail.")
    quit()

os.lseek(fd, os.SEEK_SET, 0)
for i in range(length):
    blob = ''
    for j in range(512):
        blob += chr(sector[(j + i) % 512])

    os.write(fd, blob)

os.lseek(fd, os.SEEK_SET, 0)
for i in range(length):
    blob = ''
    for j in range(512):
        blob += chr(sector[(j + i) % 512])

    data = os.read(fd, 512)
    if data != blob:
        print("integrity check fail.")
        quit()

os.close(fd)
print("every thing is ok.")
