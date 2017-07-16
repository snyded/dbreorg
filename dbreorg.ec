/*
    dbreorg.ec - generates SQL to reorg a database table or index
    Copyright (C) 1996,1997  David A. Snyder
 
    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; version 2 of the License.
 
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
 
    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
*/

#ifndef lint
static char sccsid[] = "@(#)dbreorg.ec 1.8  11/10/97 11:58:36  11/17/97 12:01:34";
#endif /* not lint */


#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <malloc.h>

$char	iowner[8+1], towner[8+1], tabtype[1+1];
$char	clustered[1+1], locklevel[1+1], idxtype[1+1];
$long	tableid, is_logging, is_ansi;
$short	part[16];
char	*database = NULL, *table = NULL, *extentsize = "16", *nextsize = NULL;
char	*idxname = NULL, *dbspace = NULL, *basefile = NULL, *maxextents = "8";
char	sqlfile[BUFSIZ], unlfile[BUFSIZ];
int	debug = 0, dflg = 0, errflg = 0, eflg = 0, iflg = 0, mflg = 0, nflg = 0;
int	rflg = 0, sflg = 0, tflg = 0, schemaflg = 0, viewsflg = 0, walktree();
void	exit();

struct tree {
	long	tabid;
	struct tree *ntreep;
};

main(argc, argv)
int     argc;
char    *argv[];
{
	$char	sqlstmt[BUFSIZ], sqlstmt2[BUFSIZ], itabname[18+1];
	char	*dot, *buf;
	extern char	*optarg;
	extern int	optind, opterr;
	register int	c;
	void	create_constraints(), create_index(), create_views(), dberror(), dbschema(), extentrpt();

	/* Print copyright message */
	(void)fprintf(stderr, "DBREORG version 1.8, Copyright (C) 1996,1997 David A. Snyder\n\n");

	/* get command line options */
	while ((c = getopt(argc, argv, "bd:e:i:m:n:rs:t:v:")) != EOF)
		switch (c) {
		case 'b':
			debug++;
			break;
		case 'd':
			if (rflg || mflg)
				errflg++;
			else {
				dflg++;
				database = optarg;
			}	
			break;
		case 'e':
			if (rflg || iflg || mflg || schemaflg || viewsflg)
				errflg++;
			else {
				eflg++;
				extentsize = optarg;
			}
			break;
		case 'i':
			if (eflg || mflg || nflg || rflg || tflg || viewsflg)
				errflg++;
			else {
				iflg++;
				idxname = optarg;
			}
			break;
		case 'm':
			if (dflg || eflg || iflg || nflg || sflg || tflg || schemaflg || viewsflg)
				errflg++;
			else {
				mflg++;
				maxextents = optarg;
			}
			break;
		case 'n':
			if (rflg || iflg || mflg || schemaflg || viewsflg)
				errflg++;
			else {
				nflg++;
				nextsize = optarg;
			}
			break;
		case 'r':
			if (dflg || eflg || iflg || nflg || sflg || tflg || schemaflg || viewsflg)
				errflg++;
			else
				rflg++;
			break;
		case 's':
			if (!strcmp(optarg, "chema"))
				if (eflg || iflg || mflg || nflg || rflg || sflg || viewsflg)
					errflg++;
				else
					schemaflg++;
			else
				if (rflg || mflg || schemaflg || viewsflg)
					errflg++;
				else {
					sflg++;
					dbspace = optarg;
				}
			break;
		case 't':
			if (iflg || rflg || mflg)
				errflg++;
			else {
				tflg++;
				table = optarg;
			}
			break;
		case 'v':
			if (eflg || iflg || mflg || nflg || rflg || sflg || schemaflg)
				errflg++;
			else
				viewsflg++;
			break;
		default:
			errflg++;
			break;
		}

	if (argc > optind)
		basefile = argv[argc - 1];

	if (!sflg)
		dbspace = database;

	/* validate command line options */
	if (errflg || ((!dflg || !eflg || !tflg || !*basefile) && (!dflg || !iflg || !*basefile) && !rflg && !schemaflg && !viewsflg)) {
		(void)fprintf(stderr, "usage: %s -d dbname -t tabname -e extent [-n next] [-s dbspace] file\n", argv[0]);
		(void)fprintf(stderr, "       %s -d dbname -i idxname [-s dbspace] file\n", argv[0]);
		(void)fprintf(stderr, "       %s -r [-m max]\n", argv[0]);
		(void)fprintf(stderr, "       %s -d dbname -t tabname -schema file\n", argv[0]);
		(void)fprintf(stderr, "       %s -d dbname -t tabname -views file\n", argv[0]);
		exit(1);
	}

	/* report or script */
	if (rflg) {
		extentrpt();
		exit(0);
	}

	/* generate output filenames */
	(void)sprintf(sqlfile, "%s.sql", basefile);
	(void)sprintf(unlfile, "%s.unl", basefile);

	/* open the specified database */
	(void)sprintf(sqlstmt, "database %s", database);
	$prepare db_exec from $sqlstmt;
	if (sqlca.sqlcode)
		dberror("prepare db_exec");
	$execute db_exec;
	if (sqlca.sqlcode)
		dberror("execute db_exec");

	/* get log mode(s) of the opened database */
	(void)sprintf(sqlstmt, "select is_logging, is_ansi from sysmaster:sysdatabases where name = \"%s\"", database);
	$prepare log_exec from $sqlstmt;
	if (sqlca.sqlcode)
		dberror("prepare log_exec");
	$execute log_exec into $is_logging, $is_ansi;
	if (sqlca.sqlcode)
		dberror("execute log_exec");

	if (!iflg) {
		/* get the tabid,owner,tabtype, and locklevel for the table specified */
		if ((dot = strchr(table, '.')) == NULL) {
			(void)sprintf(sqlstmt, "select tabid, owner, tabtype, locklevel from 'informix'.systables where tabname = \"%s\"", table);
			buf = "select y.tabid, y.owner, y.tabname, part1, part2, part3, part4, part5, part6, part7, part8, part9, part10, part11, part12, part13, part14, part15, part16, sysconstraints.owner, sysconstraints.constrname from 'informix'.sysconstraints, 'informix'.sysreferences, 'informix'.systables x, 'informix'.systables y, 'informix'.sysindexes";
			(void)sprintf(sqlstmt2, "%s where x.tabname = \"%s\" and x.tabid = sysreferences.ptabid and sysreferences.constrid = sysconstraints.constrid and sysconstraints.idxname = sysindexes.idxname and sysindexes.tabid = y.tabid", buf, table);
		} else {
			*dot = NULL;
			(void)strcpy(towner, table);
			table = ++dot;
			(void)sprintf(sqlstmt, "select tabid, owner, tabtype, locklevel from 'informix'.systables where owner = \"%s\" and tabname = \"%s\"", towner, table);
			buf = "select y.tabid, y.owner, y.tabname, part1, part2, part3, part4, part5, part6, part7, part8, part9, part10, part11, part12, part13, part14, part15, part16, sysconstraints.owner, sysconstraints.constrname from 'informix'.sysconstraints, 'informix'.sysreferences, 'informix'.systables x, 'informix'.systables y, 'informix'.sysindexes";
			(void)sprintf(sqlstmt2, "%s where x.owner = \"%s\" and x.tabname = \"%s\" and x.tabid = sysreferences.ptabid and sysreferences.constrid = sysconstraints.constrid and sysconstraints.idxname = sysindexes.idxname and sysindexes.tabid = y.tabid", buf, towner, table);
		}
		$prepare get_ownlev from $sqlstmt;
		if (sqlca.sqlcode)
			dberror("prepare get_ownlev");
		$execute get_ownlev into $tableid, $towner, $tabtype, $locklevel;
		if (sqlca.sqlcode) {
			if (sqlca.sqlcode == 100) {
				sqlca.sqlcode = -206;
				(void)strcpy(sqlca.sqlerrm, table);
			}
			dberror("execute get_ownlev");
		}
		if (*tabtype != 'T') {
			(void)fprintf(stderr, "%s: Only tables or indexes can be reorganized.\n", argv[0]);
			exit(1);
		}
	} else {
		/* get the owners,type,cluster,parts for the index specified */
		if ((dot = strchr(idxname, '.')) == NULL)
			(void)sprintf(sqlstmt, "select tabname, systables.owner, sysindexes.owner, sysindexes.tabid, idxtype, clustered, part1, part2, part3, part4, part5, part6, part7, part8, part9, part10, part11, part12, part13, part14, part15, part16 from 'informix'.systables, 'informix'.sysindexes where idxname = \"%s\" and systables.tabid = sysindexes.tabid", idxname);
		else {
			*dot = NULL;
			(void)strcpy(iowner, idxname);
			idxname = ++dot;
			(void)sprintf(sqlstmt, "select tabname, systables.owner, sysindexes.owner, sysindexes.tabid, idxtype, clustered, part1, part2, part3, part4, part5, part6, part7, part8, part9, part10, part11, part12, part13, part14, part15, part16 from 'informix'.systables, 'informix'.sysindexes where sysindexes.owner = \"%s\" and idxname = \"%s\" and systables.tabid = sysindexes.tabid", iowner, idxname);
		}
		$prepare get_idxinfo from $sqlstmt;
		if (sqlca.sqlcode)
			dberror("prepare get_idxinfo");
		$execute get_idxinfo into $itabname, $towner, $iowner, $tableid, $idxtype, $clustered, $part[0], $part[1], $part[2], $part[3], $part[4], $part[5], $part[6], $part[7], $part[8], $part[9], $part[10], $part[11], $part[12], $part[13], $part[14], $part[15];
		if (sqlca.sqlcode) {
			if (sqlca.sqlcode == 100) {
				if (debug)
					(void)fprintf(stderr, "SQL statment: execute get_idxinfo\n");
				(void)fprintf(stderr, "-???: The specified index (%s) is not in the database.\n", idxname);
				exit(1);
			}
			dberror("execute get_idxinfo");
		}
		ldchar(iowner, strlen(iowner), iowner);
		ldchar(itabname, strlen(itabname), itabname);
		table = itabname;
	}
	ldchar(towner, strlen(towner), towner);

	(void)sprintf(sqlstmt, "select colname from 'informix'.syscolumns where tabid = ? and colno = ?", tableid);
	$prepare get_colname from $sqlstmt;
	if (sqlca.sqlcode)
		dberror("prepare get_colname");

	/* open the sqlfile for writing */
	if (!freopen(sqlfile, "w", stdout)) {
		perror(sqlfile);
		exit(1);
	}

	/* generate some SQL code */
	if (!schemaflg && !viewsflg) {
		(void)fprintf(stderr, "*** Generating DATABASE statement ***\n");
		(void)printf("database %s;\n\n", database);
		if (is_logging) {
			(void)fprintf(stderr, "*** Generating SET ISOLATION statement ***\n");
			(void)printf("set isolation to dirty read;\n\n");
		}
		if (!iflg) {
			(void)fprintf(stderr, "*** Generating UNLOAD statement ***\n");
			(void)printf("unload to %s select * from \"%s\".%s;\n\n", unlfile, towner, table);
			(void)fprintf(stderr, "*** Generating DROP TABLE statement ***\n");
			(void)printf("drop table \"%s\".%s;\n", towner, table);
		} else {
			(void)fprintf(stderr, "*** Generating DROP INDEX statement ***\n");
			(void)printf("drop index \"%s\".%s;\n\n", iowner, idxname);
		}
	}
	if (!iflg) {
		if (!viewsflg) {
			(void)fprintf(stderr, "*** Generating the Schema ***\n");
			dbschema();
			if (!schemaflg) {
				create_constraints(sqlstmt2);
				(void)fprintf(stderr, "*** Generating the Constraints ***\n");
			}
		}
		(void)fprintf(stderr, "*** Generating the Views ***\n");
		create_views();
	} else {
		(void)fprintf(stderr, "*** Generating CREATE INDEX statement ***\n");
		create_index();
	}
	if (!schemaflg && !viewsflg) {
		if (is_logging) {
			(void)fprintf(stderr, "*** Generating COMMIT WORK statement ***\n");
			(void)printf("\ncommit work;\n");
		}
		(void)fprintf(stderr, "*** Generating UPDATE STATISTICS statement ***\n");
		(void)printf("\nupdate statistics for table \"%s\".%s;\n", towner, table);
	}

	return(0);
}


void
extentrpt()
{
	$char	sqlstmt[BUFSIZ], dbsname[18+1], tabname[18+1];
	$int	extents, pages;

	/* open the sysmaster database */
	$database sysmaster;
	if (sqlca.sqlcode)
		dberror("database sysmaster");

	(void)sprintf(sqlstmt, "select count(*), dbsname, tabname, sum(size) from 'informix'.sysextents group by 2, 3 having count(*) > %d order by 2, 1 desc, 3", atoi(maxextents));
	$prepare extentrpt from $sqlstmt;
	if (sqlca.sqlcode)
		dberror("prepare extentrpt");
	$declare extentcurs cursor for extentrpt;
	if (sqlca.sqlcode)
		dberror("declare extentcurs");

	$open extentcurs;
	if (sqlca.sqlcode)
		dberror("open extentcurs");
	$fetch extentcurs into $extents, $dbsname, $tabname, $pages;
	if (sqlca.sqlcode < 0)
		dberror("fetch(1) extentcurs");
	(void)printf("%-7.7s\t%-18.18s\t%-18.18s\t%6.6s\n", "Extents", "Database", "Table", " Pages");
	(void)printf("%-7.7s\t%-18.18s\t%-18.18s\t%6.6s\n", "-------", "------------------", "------------------", "------");
	while (sqlca.sqlcode != SQLNOTFOUND) {
		(void)printf("%5d\t%-18.18s\t%-18.18s\t%6d\n", extents, dbsname, tabname, pages);
		$fetch extentcurs into $extents, $dbsname, $tabname, $pages;
		if (sqlca.sqlcode < 0)
			dberror("fetch(2) extentcurs");
	}
}


void
dbschema()
{
	FILE	*pp;
	char	sysstmt[BUFSIZ];
	register int	c;

	/* execute the "dbschema" command and capture it's output */
	(void)sprintf(sysstmt, "dbschema -d %s -t '%s'.%s -p all -s all -f all|grep -v \"^No \"", database, towner, table);
	if (!(pp = popen(sysstmt, "r"))) {
		perror(sysstmt);
		exit(1);
	}

	/* read and throw away all the header info and comments */
	while ((c = getc(pp)) != '}') ;

	/* read and print all the create table info */
	while ((c = getc(pp)) != ';')
	    (void)putchar(c); 

	if (!schemaflg && !viewsflg) {
		/* insert the in dbspace, extent size and optional next size */
		(void)printf(" in %s extent size %s", dbspace, extentsize);
		if (nflg)
			(void)printf(" next size %s", nextsize);
		(void)printf(" lock mode %s;\n", (*locklevel == 'R') ? "row" : "page");
	} else
	    (void)putchar(c); 

	/* read and print the revoke info */
	while ((c = getc(pp)) != ';')
	    (void)putchar(c); 

	if (!schemaflg && !viewsflg) {
		if (is_logging && !is_ansi) {
			(void)fprintf(stderr, "*** Generating BEGIN WORK statement ***\n");
			(void)printf(";\n\nbegin work");
		}
		(void)fprintf(stderr, "*** Generating LOCK TABLE statement ***\n");
		(void)printf(";\n\nlock table \"%s\".%s in exclusive mode;\n", towner, table);
		(void)fprintf(stderr, "*** Generating LOAD statement ***\n");
		(void)printf("\nload from %s insert into \"%s\".%s;", unlfile, towner, table);
	} else
	    (void)putchar(c); 

	/* read and print everything else */
	while ((c = getc(pp)) != EOF)
	    (void)putchar(c); 
}


void
create_views()
{
	$char	sqlstmt[BUFSIZ], viewtext[64+1];
	$int	tabid;
	$short	seqno;

	$create temp table tabids (tabid integer) with no log;
	if (sqlca.sqlcode)
		dberror("create temp table tabids");

	if (walktree(tableid)) {
		sqlca.sqlcode = -406;
		*sqlca.sqlerrm = NULL;
		dberror("walktree");
	}

	(void)sprintf(sqlstmt, "select tabid, seqno, viewtext from 'informix'.sysviews where tabid in (select unique tabid from tabids) order by 1, 2");
	$prepare doviews from $sqlstmt;
	if (sqlca.sqlcode)
		dberror("prepare doviews");
	$declare viewcurs cursor for doviews;
	if (sqlca.sqlcode)
		dberror("declare viewcurs");

	$open viewcurs;
	if (sqlca.sqlcode)
		dberror("open viewcurs");
	$fetch viewcurs into $tabid, $seqno, $viewtext;
	if (sqlca.sqlcode < 0)
		dberror("fetch(1) viewcurs");
	while (sqlca.sqlcode != SQLNOTFOUND) {
		(void)printf("%s", viewtext);
		if (strrchr(viewtext, ';'))
			(void)putchar('\n');
		$fetch viewcurs into $tabid, $seqno, $viewtext;
		if (sqlca.sqlcode < 0)
			dberror("fetch(2) viewcurs");
	}

	$drop table tabids;
	if (sqlca.sqlcode)
		dberror("drop table tabids");
}


walktree(parent_tabid)
long	parent_tabid;
{
	$char	sqlstmt[BUFSIZ];
	$long	tabid;
	int	retval;
	struct tree *treep, *streep;
	void	free_treep();

	/* Allocate the first element of the linked list and C-NULL it */
	if ((treep = (struct tree *)malloc(sizeof(struct tree))) == NULL)
		return(1);
	treep->tabid = (long)NULL;
	treep->ntreep = (struct tree *)NULL;

	/* Save the beginning of the linked list */
	streep = treep;

	/* Build a cursor */
	(void)sprintf(sqlstmt, "select unique dtabid from 'informix'.sysdepend where btabid = %d", parent_tabid);
	$prepare findviews from $sqlstmt;
	if (sqlca.sqlcode)
		dberror("prepare findviews");
	$declare foundcurs cursor for findviews;
	if (sqlca.sqlcode)
		dberror("declare foundcurs");

	/* Blow through the cursor and build the linked list */
	$open foundcurs;
	if (sqlca.sqlcode)
		dberror("open foundcurs");
	$fetch foundcurs into $tabid;
	if (sqlca.sqlcode < 0)
		dberror("fetch(1) foundcurs");
	while (sqlca.sqlcode != SQLNOTFOUND) {
		if ((treep->ntreep = (struct tree *)malloc(sizeof(struct tree))) == NULL) {
			free_treep(streep);
			return(1);
		}
		treep = treep->ntreep;
		treep->tabid = tabid;
		treep->ntreep = (struct tree *)NULL;
		$fetch foundcurs into $tabid;
		if (sqlca.sqlcode < 0)
			dberror("fetch(2) foundcurs");
	}

	/* Rewind to the beginning of the linked list */
	treep = streep;

	/* Blow through the linked list and insert the tabid's into tabids */
	while (treep->ntreep != NULL) {
		treep = treep->ntreep;
		tabid = treep->tabid;

		$insert into tabids values ($tabid);
		if (sqlca.sqlcode)
			dberror("insert into tabids");

		if ((retval = walktree(tabid))) {
			free_treep(streep);
			return(retval);
		}
	}

	/* Free up all the allocated memory */
	free_treep(streep);

	return(0);
}


void
free_treep(streep)
struct tree *streep;
{
	struct tree *treep;

	/* Rewind to the beginning of the linked list (for the last time) */
	treep = streep;

	/* Blow throught the linked list and "free" all the elements */
	while (treep->ntreep != NULL) {
		streep = treep->ntreep;
		free((void *)treep);
		treep = streep;
	}
	free((void *)treep);
}


void
create_index()
{
	$char	colname[18+1];
	$short	partnum;
	char	buf[BUFSIZ];
	register int	i;

	(void)printf("create%s %sindex \"%s\".%s on \"%s\".%s (", (*idxtype == 'U') ? " unique" : "", (*clustered == 'C') ? "cluster " : "", iowner, idxname, towner, table);

	for (i = 0; part[i]; i++) {
		partnum = (short)abs((int)part[i]);
		$execute get_colname into $colname using $tableid, $partnum;
		if (sqlca.sqlcode)
			dberror("execute get_colname");
		ldchar(colname, strlen(colname), colname);
		(void)printf("%s%s%s", (!i) ? "" : ", ", colname, (part[i] < 0) ? " desc" : "");
	}

	if (sflg)
		(void)sprintf(buf, " in %s", dbspace);
	else
		(void)sprintf(buf, "");
	(void)printf(")%s;\n", buf);
}


void
create_constraints(sqlstmt)
$char	*sqlstmt;
{
	$char	colname[18+1], rowner[8+1], rtable[18+1], cowner[8+1], cname[18+1];
	$long	rtabid;
	$short	partnum;
	register int	i;

	$prepare conststmt from $sqlstmt;
	if (sqlca.sqlcode)
		dberror("prepare conststmt");
	$declare constcurs cursor for conststmt;
	if (sqlca.sqlcode)
		dberror("declare constcurs");

	$open constcurs;
	if (sqlca.sqlcode)
		dberror("open constcurs");
	$fetch constcurs into $rtabid, $rowner, $rtable, $part[0], $part[1], $part[2], $part[3], $part[4], $part[5], $part[6], $part[7], $part[8], $part[9], $part[10], $part[11], $part[12], $part[13], $part[14], $part[15], $cowner, $cname;
	if (sqlca.sqlcode < 0)
		dberror("fetch(1) constcurs");
	while (sqlca.sqlcode != SQLNOTFOUND) {
		ldchar(rowner, strlen(rowner), rowner);
		ldchar(rtable, strlen(rtable), rtable);
		ldchar(cowner, strlen(cowner), cowner);
		ldchar(cname, strlen(cname), cname);
		(void)printf("alter table \"%s\".%s add constraint (foreign key (", rowner, rtable);
		for (i = 0; part[i]; i++) {
			partnum = (short)abs((int)part[i]);
			$execute get_colname into $colname using $rtabid, $partnum;
			if (sqlca.sqlcode)
				dberror("execute get_colname");
			ldchar(colname, strlen(colname), colname);
			(void)printf("%s%s", (!i) ? "" : ", ", colname);
		}
		(void)printf(")\n    references \"%s\".%s constraint \"%s\".%s);\n\n", towner, table, cowner, cname);

		$fetch constcurs into $rtabid, $rowner, $rtable, $part[0], $part[1], $part[2], $part[3], $part[4], $part[5], $part[6], $part[7], $part[8], $part[9], $part[10], $part[11], $part[12], $part[13], $part[14], $part[15], $cowner, $cname;
		if (sqlca.sqlcode < 0)
			dberror("fetch(2) constcurs");
	}
}


void
dberror(object)
char	*object;
{
	int	msglen;
	char	buf[BUFSIZ], errmsg[BUFSIZ];

	if (debug)
		(void)fprintf(stderr, "SQL statment: %s\n", object);

	(void)rgetlmsg(sqlca.sqlcode, errmsg, sizeof(errmsg), &msglen);
	(void)sprintf(buf, errmsg, sqlca.sqlerrm);
	(void)fprintf(stderr, "%d: %s", sqlca.sqlcode, buf);

	exit(1);
}


