/*
Note: The bison parser has been replaced by a DOM parser+walker.
The bison code is left in the build tree in case it is useful
elsewhere.

This makefile is purely to record how to rebuild the parser(s)
from the original .y files.
*/

.PHONEY: ce
.PHONEY: dap4

all: ce

# Requires bison 3.0 or later
ce:
	bison  -v ce.y -o CEBisonParser.java
	if test -f ${CETARGET} ; then rm ${CETARGET}; fi

/* No longer used */
dap4:
	bison  -v dap4.y -o Dap4BisonParser.java
	if test -f ${DAP4TARGET} ; then rm ${DAP4TARGET}; fi

clean::
	rm -fr Dap4BisonParser.java *.output

