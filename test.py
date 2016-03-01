#! /usr/python
# python test.py /dev/mapper/test 1048576

from __future__ import print_function
import sys
import os

def main():
    if len(sys.argv) < 3:
        print("usage:")
        print("    python test.py /dev/mapper/test 1048576")
        quit()

    name = sys.argv[1]
    length = int(sys.argv[2])
    fd = os.open(name, os.O_RDWR)
    if fd < 0:
        print("open " + name + " fail.")
        quit()

    if len(sys.argv) == 4:
        for c in sys.argv[3]:
            if c == 'r':
                read_test(fd, length)
            if c == 'w':
                write_test(fd, length)
    else:
        write_test(fd, length)
        read_test(fd, length)

    os.close(fd)
    print("every thing is ok.")

def write_test(fd, length):
    sector = [i % 256 for i in range(512)]

    os.lseek(fd, os.SEEK_SET, 0)
    for i in range(length):
        blob = ''
        for j in range(512):
            blob += chr(sector[(j + i) % 512])

        if not(i % 32):
            print("\rwrite progress %6.2f%%" % (i * 100.0 / length), end="")

        if os.write(fd, blob) != 512:
            print("\nwrite error.")
            quit()
    print("\rwrite progress %6.2f" % 100.0)

def read_test(fd, length):
    sector = [i % 256 for i in range(512)]

    os.lseek(fd, os.SEEK_SET, 0)
    for i in range(length):
        blob = ''
        for j in range(512):
            blob += chr(sector[(j + i) % 512])

        if not(i % 32):
            print("\rread progress  %6.2f%%" % (i * 100.0 / length), end="")

        data = os.read(fd, 512)
        if data != blob:
            print("")
            if len(data) < 512:
                print("read error.")

            print("integrity check fail.")
            print("corrupted sector: %d" % i)
            print("data in hex")
            print(":".join("%02X" % ord(c) for c in data))
            print("blob in hex")
            print(":".join("%02X" % ord(c) for c in blob))
            quit()
    print("\rread progress  %6.2f" % 100.0)

if __name__ == '__main__':
    main()
