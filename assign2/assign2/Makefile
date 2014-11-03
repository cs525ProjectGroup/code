CC=gcc
CFLAGS=-std=c99 -I.
DEPS = dberror.h storage_mgr.h test_helper.h buffer_mgr_stat.h buffer_mgr.h dt.h
OBJECTS = dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o test_assign2_1.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

test_assign2_1: $(OBJECTS)
	$(CC) -o  $@ $^ $(CFLAGS)

clean:
	rm -f $(OBJECTS)