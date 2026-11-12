CC = $(CROSS_COMPILE)gcc
CFLAGS ?= -Wall -Werror -g

.PHONY: all default clean

all: aesdsocket

default: aesdsocket

aesdsocket: aesdsocket.c
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o $@

clean:
	rm -f aesdsocket *.o
