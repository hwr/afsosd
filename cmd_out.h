/*
 * Copyright Christof Hanke (christof.hanke@rzg.mpg.de)
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   1. Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *   2. Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in
 *      the documentation and/or other materials provided with the
 *      distribution.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 * OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __RXOSD_CMD_INCL__
#define	__RXOSD_CMD_INCL__	    1

/* For pretty output */
#define T_MAX_CELLS 20
#define T_MAX_CELLCONTENT_LEN 30
#define T_TYPE_ASCII_SPARTAN 0
#define T_TYPE_ASCII_INNERLINE 1
#define T_TYPE_ASCII_FULLNET 2
#define T_TYPE_HTML 3
#define T_TYPE_CSV 4
#define T_TYPE_MAX 4
#define T_CELL_JUST_LEFT -1
#define T_CELL_JUST_CENTRE 0
#define T_CELL_JUST_RIGHT 1

struct TableCell {
    int Width;
    int Justification;
    char Content[T_MAX_CELLCONTENT_LEN];
    struct TableCell *next;
    struct TableCell* (*append) (struct TableCell *,char *Content, int Width,int Justification);
    struct TableCell* (*set) (struct TableCell *,char *Content, int Width,int Justification) ;
};

struct TableRow {
    struct TableCell *Head;
    struct TableRow *next;
    struct TableRow*  (*append) (struct TableRow *, struct TableCell *);
    struct TableRow* (*set) (struct TableRow *, struct TableCell *);
};

struct Table {
    int Type;
    int CellsperRow;
    /* Attribtues required for formatted ASCII-ouput */
    int *CellWidth;
    int RowLength; /* number of character per Row */
    char CellSeparator;
    char RowSeparator;
    /* Basic subentities */
    struct TableCell *Header;
    struct TableRow *Body;
    struct TableCell *Footer;
    /* ouput methods provided for this table */
    void (*printHeader) (struct Table *);
    void (*printBody) (struct Table *);
    void (*printFooter) (struct Table *);
    void (*printRow) (struct Table *, struct TableCell *);
    void (*setLayout) (struct Table *Table,int CellsperRow,int *allCellWidth,int*chosenIdx);
    void (*setType) (struct Table *Table,int TableType);
    int* (*newTableLayout) (int CellsperRow);
};

extern void PrintTime(afs_uint32 intdate);
extern void PrintDate(afs_uint32 intdate);
extern void sprintDate( char *string, afs_uint32 intdate);
extern void printTable(struct Table *Table);
extern struct TableRow* newTableRow(void);
extern struct TableCell* newTableCell(void);
extern struct Table* newTable(void);
void freeTable(struct Table *aTable);
struct TableCell* setTableCell(struct TableCell *aCell, char *Content, int Width,int Justification);

#endif /* __RXOSD_CMD_INCL__ */
