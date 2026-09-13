CROSS_COMPILE ?=
CC ?= $(CROSS_COMPILE)gcc
CFLAGS ?= -Wall -Wextra -g -pthread
LDFLAGS ?=
TARGET = aesdsocket

.PHONY: all default clean

default: all

all: $(TARGET)

$(TARGET): aesdsocket.c
	$(CC) $(CFLAGS) -o $(TARGET) aesdsocket.c -pthread $(LDFLAGS)

clean:
	rm -f $(TARGET) *.o
