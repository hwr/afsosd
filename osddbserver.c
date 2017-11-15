/*
 * Copyright (c) 2007, Hartmut Reuter,
 * RZG, Max-Planck-Institut f. Plasmaphysik.
 * All Rights Reserved.
 *
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

#include <afsconfig.h>
#include <afs/param.h>

#include <afs/stds.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/stat.h>
#include <errno.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef AFS_NT40_ENV
#include <winsock2.h>
#include <WINNT/afsevent.h>
#endif
#ifdef HAVE_SYS_FILE_H
#include <sys/file.h>
#endif
#include <time.h>
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif
#include <stdio.h>

#ifdef HAVE_STRING_H
#include <string.h>
#else
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif
#endif

#include <rx/xdr.h>
#include <rx/rx.h>
#include <rx/rxstat.h>
#include <rx/rx_globals.h>
#include <afs/cellconfig.h>
#include <afs/keys.h>
#include <afs/auth.h>
#include <lock.h>
#include <ubik.h>
#include <afs/afsutil.h>
#include <afs/cellconfig.h>
#include <afs/com_err.h>
#include <afs/audit.h>
#include "osddb.h"
#include "../rxosd/afsosd.h"

struct osddb_ops_v0 *osddb = NULL;

#define MAXLWP 16
const char *osd_dbaseName;
extern struct afsconf_dir *osddb_confdir;	/* osddb configuration dir */
int lwps = 9;

struct ubik_dbase *OSD_dbase;
extern afs_int32 ubik_nBuffers;

extern int LogLevel;
int smallMem = 0;
int rxJumbograms = 1;		/* default is to send and receive jumbo grams */
int rxMaxMTU = -1;
int haveAlwaysSyncSite = 0;

#define OSD_TIMEOUT	400
#define ADDRSPERSITE 16         /* Same global is in rx/rx_user.c */
afs_uint32 SHostAddrs[ADDRSPERSITE];

struct afsconf_dir *osdb_confdir;

struct OsdList osds;

#define MAX_OSDDB_THREADS 128
#define NOSDDBRPCS 50

afs_uint32 statisticStart;
osddb_stat stats[NOSDDBRPCS];
osddb_statList statList;

#define STAT_INDICES 400
afs_int32 stat_index[STAT_INDICES];
afs_int32 rxBind = 0;

/*
  defined in <ubik.h> if UBIK_INTERNALS is set
 */
extern int ubeacon_AmSyncSite(void);
#include "AFS_component_version_number.c"

/************************************************************************/

int
main(int argc, char *argv[])
{
    afs_int32 code;
    afs_int32 myHost;
    struct rx_service *tservice;
    struct rx_securityClass **sc;
    afs_int32 numSc = 3;
    struct afsconf_dir *tdir;
    struct ktc_encryptionKey tkey;
    struct afsconf_cell info;
    struct hostent *th;
    char hostname[200];
    int noAuth = 0, index, i;
    char commandLine[150];
    char clones[MAXHOSTSPERCELL];
    afs_uint32 host = ntohl(INADDR_ANY);
    char *logpath;
    struct logOptions logopts;

#ifdef	AFS_AIX32_ENV
    /*
     * The following signal action for AIX is necessary so that in case of a
     * crash (i.e. core is generated) we can include the user's data section
     * in the core dump. Unfortunately, by default, only a partial core is
     * generated which, in many cases, isn't too useful.
     */
    struct sigaction nsa;

    rx_extraPackets = 100;	/* should be a switch, I guess... */
    sigemptyset(&nsa.sa_mask);
    nsa.sa_handler = SIG_DFL;
    nsa.sa_flags = SA_FULLDUMP;
    sigaction(SIGABRT, &nsa, NULL);
    sigaction(SIGSEGV, &nsa, NULL);
#endif
    osi_audit_init();

    /* Parse command line */
    for (index = 1; index < argc; index++) {
	if (strcmp(argv[index], "-noauth") == 0) {
	    noAuth = 1;

	} else if (strcmp(argv[index], "-p") == 0) {
	    lwps = atoi(argv[++index]);
	    if (lwps > MAXLWP) {
		printf("Warning: '-p %d' is too big; using %d instead\n",
		       lwps, MAXLWP);
		lwps = MAXLWP;
	    }

	} else if (strcmp(argv[index], "-nojumbo") == 0) {
	    rxJumbograms = 0;

	} else if (strcmp(argv[index], "-rxbind") == 0) {
	    rxBind = 1;

	} else if (!strcmp(argv[index], "-rxmaxmtu")) {
	    if ((index + 1) >= argc) {
		fprintf(stderr, "missing argument for -rxmaxmtu\n");
		return -1;
	    }
	    rxMaxMTU = atoi(argv[++index]);

	} else if (strcmp(argv[index], "-smallmem") == 0) {
	    smallMem = 1;

	} else if (strcmp(argv[index], "-trace") == 0) {
	    extern char rxi_tracename[80];
	    strcpy(rxi_tracename, argv[++index]);

       } else if (strcmp(argv[index], "-auditlog") == 0) {
           char oldName[MAXPATHLEN];
           char *fileName = argv[++index];

#ifndef AFS_NT40_ENV
           struct stat statbuf;

           if ((lstat(fileName, &statbuf) == 0)
               && (S_ISFIFO(statbuf.st_mode))) {
           } else
#endif
           {
               strcpy(oldName, fileName);
               strcat(oldName, ".old");
               rename(fileName, oldName);
           }
           if (osi_audit_file(fileName)) {
               printf("Warning: auditlog %s not writable, ignored.\n", fileName);
           }
	} else if (strcmp(argv[index], "-enable_peer_stats") == 0) {
	    rx_enablePeerRPCStats();
	} else if (strcmp(argv[index], "-enable_process_stats") == 0) {
	    rx_enableProcessRPCStats();
	} else if (strcmp(argv[index], "-ubikbuffers") == 0) {
	    ubik_nBuffers = atoi(argv[++index]);
	} else if (strcmp(argv[index], "-haveAlwaysSyncSite") == 0) {
	    haveAlwaysSyncSite = 1;
	} else {
	    /* support help flag */
#ifndef AFS_NT40_ENV
	    printf("Usage: osddbserver [-p <number of processes>] [-nojumbo] "
		   "[-rxmaxmtu <bytes>] [-rxbind] "
		   "[-auditlog <log path>] "
		   "[-enable_peer_stats] [-enable_process_stats] "
		   "[-ubikbuffers <n>]"
		   "[-help]\n");
#else
	    printf("Usage: osddbserver [-p <number of processes>] [-nojumbo] "
		   "[-rxmaxmtu <bytes>] [-rxbind] "
		   "[-auditlog <log path>] "
		   "[-enable_peer_stats] [-enable_process_stats] "
		   "[-haveAlwaysSyncSite] "
		   "[-help]\n");
#endif
	    fflush(stdout);
	    exit(0);
	}
    }

    for (i=0; i<STAT_INDICES; i++)
        stat_index[i] = -1;
    statisticStart = FT_ApproxTime();

    /* Initialize dirpaths */
    if (!(initAFSDirPath() & AFSDIR_SERVER_PATHS_OK)) {
#ifdef AFS_NT40_ENV
	ReportErrorEventAlt(AFSEVT_SVR_NO_INSTALL_DIR, 0, argv[0], 0);
#endif
	fprintf(stderr, "%s: Unable to obtain AFS server directory.\n",
		argv[0]);
	exit(2);
    }
    ConstructLocalPath("osddb", AFSDIR_SERVER_DB_DIRPATH, (char **)&osd_dbaseName);

    code = ConstructLocalLogPath("OSDLog", &logpath);
    memset(&logopts, 0, sizeof(logopts));
    logopts.lopt_filename = logpath;
    OpenLog(&logopts);	/* set up logging */
    SetupLogSignals();

    tdir = afsconf_Open(AFSDIR_SERVER_ETC_DIRPATH);
    if (!tdir) {
	printf
	    ("osddb: can't open configuration files in dir %s, giving up.\n",
	     AFSDIR_SERVER_ETC_DIRPATH);
	exit(1);
    }
#ifdef AFS_NT40_ENV
    /* initialize winsock */
    if (afs_winsockInit() < 0) {
	ReportErrorEventAlt(AFSEVT_SVR_WINSOCK_INIT_FAILED, 0, argv[0], 0);
	fprintf(stderr, "osddb: couldn't initialize winsock. \n");
	exit(1);
    }
#endif
    /* get this host */
    gethostname(hostname, sizeof(hostname));
    th = gethostbyname(hostname);
    if (!th) {
	printf("osddb: couldn't get address of this host (%s).\n",
	       hostname);
	exit(1);
    }
    memcpy(&myHost, th->h_addr, sizeof(afs_int32));

    /* get list of servers */
    code =
	afsconf_GetExtendedCellInfo(tdir, NULL, AFSCONF_VLDBSERVICE, &info,
				    clones);
    if (code) {
	printf("osddb: Couldn't get cell server list for 'afsvldb'.\n");
	exit(2);
    }

    /* rxvab no longer supported */
    memset(&tkey, 0, sizeof(tkey));

    if (noAuth)
	afsconf_SetNoAuthFlag(tdir, 1);

    if (rxBind) {
	afs_int32 ccode;
#ifndef AFS_NT40_ENV
        if (AFSDIR_SERVER_NETRESTRICT_FILEPATH ||
            AFSDIR_SERVER_NETINFO_FILEPATH) {
            char reason[1024];
            ccode = afsconf_ParseNetFiles(SHostAddrs, NULL, NULL,
				  ADDRSPERSITE, reason,
				  AFSDIR_SERVER_NETINFO_FILEPATH,
				  AFSDIR_SERVER_NETRESTRICT_FILEPATH);
        } else
#endif
	{
            ccode = rx_getAllAddr(SHostAddrs, ADDRSPERSITE);
        }
        if (ccode == 1) {
            host = SHostAddrs[0];
	    rx_InitHost(host, OSDDB_SERVER_PORT);
	}
    }

    ubik_nBuffers = 512;
    ubik_CRXSecurityProc = afsconf_ClientAuth;
    ubik_CRXSecurityRock = (char *)tdir;
    ubik_SRXSecurityProc = afsconf_ServerAuth;
    ubik_SRXSecurityRock = (char *)tdir;
    ubik_CheckRXSecurityProc = afsconf_CheckAuth;
    ubik_CheckRXSecurityRock = (char *)tdir;
    {
        struct vol_data_v0 voldata = {
           &tdir,
           &LogLevel,
           NULL,
           NULL,
           NULL,
           NULL
       };
        struct init_osddb_inputs input = {
            &voldata,
            &OSD_dbase
        };
        struct init_osddb_outputs output = {
            &osddb
        };

        code =
            ubik_ServerInitByInfo(myHost, OSDDB_SERVER_PORT, &info, clones, osd_dbaseName, &OSD_dbase);
        if (code) {
            printf ("Ubik init failed for OSDDB: %s, continuing without OSDDB\n",
                       afs_error_message(code));
	    exit(4);
        }
        if (OSD_dbase) {
            code = load_libafsosd("init_osddbserver", &input, &output);
            if (code) {
                printf("Loading libafsosd.so failed with code %d, continuing without OSDDB\n",
                        code);
	        exit(5);
            }
        }
    }
    if (!rxJumbograms) {
	rx_SetNoJumbo();
    }
    if (rxMaxMTU != -1) {
	if (rx_SetMaxMTU(rxMaxMTU) != 0) {
            printf("rxMaxMTU %d invalid\n", rxMaxMTU);
            return -1;
        }
    }
    rx_SetRxDeadTime(50);

    afsconf_GetKey(tdir, 999, &tkey);
    afsconf_BuildServerSecurityObjects(tdir, &sc, &numSc);
    tservice =
	rx_NewServiceHost(host, OSDDB_SERVER_PORT, OSDDB_SERVICE_ID,
		 "osddb server", sc, numSc, (osddb->op_OSDDB_ExecuteRequest));
    if (!tservice) {
	printf("osddb: Could not create OSDDB rx service\n");
	exit(3);
    }
    rx_SetMinProcs(tservice, 2);
    if (lwps < 4)
	lwps = 4;
    rx_SetMaxProcs(tservice, lwps);

    tservice =
	rx_NewServiceHost(host, 0, RX_STATS_SERVICE_ID, "rpcstats", sc, 3,
		      RXSTATS_ExecuteRequest);
    if (tservice == (struct rx_service *)NULL) {
	printf("osddb: Could not create rpc stats rx service\n");
	exit(3);
    }
    rx_SetMinProcs(tservice, 2);
    rx_SetMaxProcs(tservice, 4);

    for (commandLine[0] = '\0', i = 0; i < argc; i++) {
	if (i > 0)
	    strcat(commandLine, " ");
	strcat(commandLine, argv[i]);
    }
    printf("%s\n", cml_version_number);	/* Goes to the log */
    rx_StartServer(1);
    return 0; /* not reachable */
}
