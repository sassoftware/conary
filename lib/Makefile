LDFLAGS=-lelf
CFLAGS=-Wall -I/usr/include/python2.3

elf.so: elf.o
	gcc -shared -o elf.so elf.o -lelf

elf.o:	elf.c

