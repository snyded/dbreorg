# makefile
# This makes "dbreorg"

dbreorg: dbreorg.ec
	esql -static -O dbreorg.ec -o dbreorg -s
	@rm -f dbreorg.c
