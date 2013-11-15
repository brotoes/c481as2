CC=mpicc
CFLAGS=-DDEBUG

psrs: psrs.c psrs.h

test:
	mpirun -np 4 ./psrs

clean:
	rm psrs
