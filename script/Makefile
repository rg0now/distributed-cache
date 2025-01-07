# Variables
SRC = db_filler.cc
OBJ = db_filler.o
EXEC = db_filler

CC=g++
CFLAGS=-O3 -DNDEBUG -std=gnu++11 -fPIE -fvisibility=hidden
INC = -I /usr/include/postgresql
LFLAGS=-O3 -DNDEBUG 
LIBS = -lpq

# Rules
all: $(EXEC)

%.o: %.cc
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

$(EXEC): $(OBJ)
	$(CC) $(LFLAGS) $(OBJ) -o $@ $(LIBS) 

clean:
	rm -f $(OBJ) $(EXEC)
