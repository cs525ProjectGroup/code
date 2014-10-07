CC=gcc
CFLAGS=-std=c99 -I.
DEPS = dberror.h storage_mgr.h test_helper.h
OBJECTS = dberror.o storage_mgr.o test_assign1_1.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

test_assign1: $(OBJECTS)
	$(CC) -o  $@ $^ $(CFLAGS)

clean:
	rm -f $(OBJECTS)