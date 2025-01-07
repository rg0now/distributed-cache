# Variables
SRC = memslap.cc options.cc
OBJ = memslap.o options.o 
EXEC = memslap

CC=g++
CFLAGS=-O3 -DNDEBUG -std=gnu++11 -fPIE -fvisibility=hidden # -MD -MT 
INC = -I "./" -I libmemcached/ -I /usr/include/libmemcached -I /usr/include/postgresql
LFLAGS=-O3 -DNDEBUG 
LIBS = -lpq -ldl -lmemcached -lmemcachedutil

# Rules
all: $(EXEC)

%.o: %.cc
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

$(EXEC): $(OBJ)
	$(CC) $(LFLAGS) $(OBJ) -o $@ $(LIBS) 

clean:
	rm -f $(OBJ) $(EXEC)
