CROSS_COMPILE ?=
CC ?= $(CROSS_COMPILE)gcc
CFLAGS ?= -Wall -Wextra -g -pthread
LDFLAGS ?=
TARGET = aesdsocket

# Redirect reads/writes to /dev/aesdchar (assignment 8) instead of
# /var/tmp/aesdsocketdata, and drop the 10s timestamp. Build with
# USE_AESD_CHAR_DEVICE=0 to get the assignment 5/6 behavior back.
USE_AESD_CHAR_DEVICE ?= 1

.PHONY: all default clean

default: all

all: $(TARGET)

$(TARGET): aesdsocket.c
	$(CC) $(CFLAGS) -DUSE_AESD_CHAR_DEVICE=$(USE_AESD_CHAR_DEVICE) -o $(TARGET) aesdsocket.c -pthread $(LDFLAGS)

clean:
	rm -f $(TARGET) *.o
