/*
 *  Read a byte of a file in order to bring it online
 */

/* the following includes copied from volume.c */

#include <afsconfig.h>
#include <afs/param.h>

#include <roken.h>
#include <afs/opr.h>

#include <ctype.h>
#include <stddef.h>

#ifdef HAVE_SYS_FILE_H
#include <sys/file.h>
#endif

#ifdef AFS_PTHREAD_ENV
# include <opr/lock.h>
#else
# include <opr/lockstub.h>
#endif

#include <afs/afsint.h>

#include <rx/rx_queue.h>

#ifndef AFS_NT40_ENV
#if !defined(AFS_SGI_ENV)
#ifdef  AFS_OSF_ENV
#include <ufs/fs.h>
#else /* AFS_OSF_ENV */
#ifdef AFS_VFSINCL_ENV
#define VFS
#ifdef  AFS_SUN5_ENV
#include <sys/fs/ufs_fs.h>
#else
#if defined(AFS_DARWIN_ENV) || defined(AFS_XBSD_ENV)
#include <ufs/ufs/dinode.h>
#include <ufs/ffs/fs.h>
#else
#include <ufs/fs.h>
#endif
#endif
#else /* AFS_VFSINCL_ENV */
#if !defined(AFS_AIX_ENV) && !defined(AFS_LINUX20_ENV) && !defined(AFS_XBSD_ENV) && !defined(AFS_DARWIN_ENV)
#include <sys/fs.h>
#endif
#endif /* AFS_VFSINCL_ENV */
#endif /* AFS_OSF_ENV */
#endif /* AFS_SGI_ENV */
#endif /* !AFS_NT40_ENV */

#ifdef  AFS_AIX_ENV
#include <sys/vfs.h>
#else
#ifdef  AFS_HPUX_ENV
#include <mntent.h>
#else
#if     defined(AFS_SUN_ENV) || defined(AFS_SUN5_ENV)
#ifdef  AFS_SUN5_ENV
#include <sys/mnttab.h>
#include <sys/mntent.h>
#else
#include <mntent.h>
#endif
#else
#ifndef AFS_NT40_ENV
#if defined(AFS_SGI_ENV)
#include <mntent.h>
#else
#ifndef AFS_LINUX20_ENV
#include <fstab.h>              /* Need to find in libc 5, present in libc 6 */
#endif
#endif
#endif /* AFS_SGI_ENV */
#endif
#endif /* AFS_HPUX_ENV */
#endif

#include <afs/errors.h>
#include <afs/afssyscalls.h>
#include <afs/afsutil.h>
#include <afs/dir.h>

#define afs_lseek(FD, O, F)   lseek64(FD, (off64_t) (O), F)

#ifdef O_LARGEFILE

#define afs_stat                stat64
#define afs_open                open64
#define afs_fopen               fopen64
#define afs_fstat               fstat64
#ifndef AFS_NT40_ENV
#if defined (AFS_HAVE_STATVFS64)
#  define afs_statvfs           statvfs64
#elif defined (AFS_HAVE_STATFS64)
#  define afs_statfs            statfs64
#elif defined (AFS_HAVE_STATVFS)
#  define afs_statvfs           statvfs
#else
#  define afs_statfs            statfs
#endif /* !AFS_HAVE_STATVFS64 */
#endif /* !AFS_NT40_ENV */

#else /* !O_LARGEFILE */

#define afs_stat                stat
#define afs_open                open
#define afs_fopen               fopen
#define afs_fstat               fstat
#define afs_lseek               lseek
#ifndef AFS_NT40_ENV
#if defined (AFS_HAVE_STATVFS)
#  define afs_statvfs           statvfs
#else /* !AFS_HAVE_STATVFS */
#  define afs_statfs            statfs
#endif /* !AFS_HAVE_STATVFS */
#endif /* !AFS_NT40_ENV */

#endif /* !O_LARGEFILE */


#include <afs/stds.h>
#include <afs/fileutil.h>
#include <afs/unified_afs.h>
#include "rxosd_hsm.h"
#include "rxosd_ihandle.h"

char *whoami;

char *iname[3] = {"C-library", "HPSS", "DCACHE"};
char *initname[3] = {"nothing", "init_rxosd_hpss", "init_rxosd_dcache"};

extern struct ih_posix_ops ih_dcache_ops;
time_t hpssLastAuth = 0;


int
myopen(const char *path, int flags, mode_t mode, afs_uint64 size)
{
    return open64(path, flags, mode);
}

struct ih_posix_ops clib_ops = {
    myopen,
    close,
    read,
    NULL,
    write,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    stat64,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
};

struct ih_posix_ops *myops = &clib_ops;

static void
usage(void)
{
    fprintf(stderr,"usage: %s [-hpss] [-dcache] filename\n", whoami);
    exit(5);
}

int
main(int argc, char *argv[])
{
    char filename[256];
    char buffer;
    struct afs_stat status;
    int fd, verbose = 1;
    struct timeval starttime, endtime;
    struct timezone timezone;
    /* to simulate a tape mount for testing */
    int sleeptime = 0;
    float seconds;
    FILE *log;
    char *principal = NULL;
    char *keytab = NULL;
    char *hpssPath = "";
    afs_int32 interface = 0;
    afs_int32 code;
    struct hsm_interface_input input;
    struct hsm_interface_output output;
    struct rxosd_var rxosd_var;
    struct hsm_auth_ops *auth_ops = NULL;

    log = fopen("/tmp/readabyte.log", "a+");

    argc--; argv++;
    while (argc > 0 && argv[0][0] == '-') {
        switch (argv[0][1]) {
	case	'd' :
		interface =  DCACHE_INTERFACE;
		break;
	case	'h' :
		if (argc == 4) {	/* backward compatibility */
                    argc--; argv++;
		    principal = argv[0];
                    argc--; argv++;
		    keytab = argv[0];
		}
		interface = HPSS_INTERFACE;
		break;
        case    'v' :
                verbose = 1;
                break;
            default: usage();
        }
        argc--; argv++;
    }

    if (argc < 1) usage();

    if (interface) {         /* load HPSS support from shared library */
        rxosd_var.pathOrUrl = &hpssPath;
        rxosd_var.principal = &principal;
        rxosd_var.keytab = &keytab;
        rxosd_var.lastAuth = &hpssLastAuth;
        input.var = &rxosd_var;
        output.opsPtr = &myops;
        output.authOps = &auth_ops;

        code = load_libafshsm(interface, initname[interface], &input, &output);
        if (code) {
            fprintf(log, "Loading shared library for %s failed with code %d, aborting\n",
                    iname[interface], code);
            return -1;
        }
	if (auth_ops && auth_ops->authenticate) {
	    code = (auth_ops->authenticate)();
	    if (code) {
                fprintf(log, "Authentication to HPSS failed with %d, aborting\n",
				code);
                return -1;
            }
	}

    }

    if (argv[0][0] == '/')
        sprintf(filename,"%s", argv[0]);
    else
        sprintf(filename,"%s/%s", hpssPath, argv[0]);

    if (sleeptime) {
	if (verbose) fprintf(log, "Now sleeping %u seconds\n", sleeptime);
	sleep(sleeptime);
    }
    if (verbose) gettimeofday(&starttime, &timezone);
    if (myops->stat64(filename, &status) == -1) {
       perror("stat");
       fprintf(log, "for %s\n", filename);
       exit(1);
    }
    fd = myops->open(filename, O_RDONLY, 0, 0);
    if (fd < 0) {
       perror("open");
       fprintf(log, "for %s\n", filename);
       exit(2);
    }
    if (myops->read(fd, &buffer, 1) != 1) {
	fprintf(log,"Couldn't read one byte from %s\n", filename);
	exit(4);
    }
    myops->close(fd);

    if (auth_ops && auth_ops->unauthenticate)
	(auth_ops->unauthenticate)();

    if (verbose) {
        gettimeofday(&endtime, &timezone);
        seconds = endtime.tv_sec + endtime.tv_usec *.000001
             -starttime.tv_sec - starttime.tv_usec *.000001;
        fprintf(log, "%s online %.0f sec %llu\n",
		filename, seconds, (long long unsigned)status.st_size);
    }

    return(0);
}
