#!/bin/bash

erl -make # Compiles all Erlang files automagically
if [ $? -eq 0 ];
then
	echo "Running program..."
	# Use the following line if you want to disable networking for some reason
	#erl -pa ./main.erl -run main -run init stop -noshell
	erl -sname example1@localhost -setcookie thecookiemonster -pa ./main.erl -run main -run init stop -noshell
else
	echo "Compilation failed."
fi
