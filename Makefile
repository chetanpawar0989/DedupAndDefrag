SRC := helloworld
PARM1 := tmp
TESTFILE := test_file

all:
	gcc -g -Wall $(SRC).c `pkg-config fuse --cflags --libs` -o $(SRC)

clean:
	rm $(SRC)

mount:
	./$(SRC) $(PARM1)

unmount:
	fusermount -u $(PARM1)

