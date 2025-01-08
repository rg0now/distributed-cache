# Variables
SRC = memslap.cc options.cc
OBJ = memslap.o options.o 
EXEC = memslap

CC=g++
DEPFLAGS = -MD -MP
CXXFLAGS=-O3 -DNDEBUG -std=gnu++11 -fPIE -fvisibility=hidden $(DEPFLAGS)
INC = -I "./" -I libmemcached/ -I /usr/include/libmemcached -I /usr/include/postgresql
LFLAGS=-O3 -DNDEBUG 
LIBS = -lpq -ldl -lmemcached -lmemcachedutil

all: $(EXEC)

%.o: %.cc
	$(CC) $(CXXFLAGS) $(INC) -c $< -o $@

$(EXEC): $(OBJ)
	$(CC) $(LFLAGS) $(OBJ) -o $@ $(LIBS) 

clean:
	rm -f $(OBJ) $(EXEC)

# Include the generated .d files
-include $(OBJ:.o=.d)

