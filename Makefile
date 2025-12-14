# ...existing code...
CC = gcc
CFLAGS = -g -Wall -Iinclude
SRC = src/sqlite3.c
OBJ = $(SRC:.c=.o)
TARGET =

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

sqlite_benchmark: tests/sqlite-kv-benchmark.c src/sqlite3.c
	$(CC) $(CFLAGS) -o $@ $^

test: sqlite_benchmark

clean:
	rm -f $(OBJ) $(TARGET) sqlite_benchmark benchmark.db
