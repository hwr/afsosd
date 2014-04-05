/*
 * Copyright (c) 2011, Hartmut Reuter,
 * RZG, Max-Planck-Institut f. Plasmaphysik.
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

#include <afsconfig.h>
#include <afs/param.h>


#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#ifdef	AFS_SGI_ENV
#undef SHARED			/* XXX */
#endif
#ifdef AFS_NT40_ENV
#include <fcntl.h>
#else
#include <sys/param.h>
#include <sys/file.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>

#ifndef AFS_LINUX20_ENV
#include <net/if.h>
#ifndef AFS_ARM_DARWIN_ENV
#include <netinet/if_ether.h>
#endif
#endif
#endif
#ifdef AFS_HPUX_ENV
/* included early because of name conflict on IOPEN */
#include <sys/inode.h>
#ifdef IOPEN
#undef IOPEN
#endif
#endif /* AFS_HPUX_ENV */
#include <afs/stds.h>
#include <rx/xdr.h>
#include <rx/rx_queue.h>
#include <afs/nfs.h>
#include <lwp.h>
#include <lock.h>
#include <afs/afsint.h>
#include "vicedosd.h"
#include <afs/vldbint.h>
#include <afs/errors.h>
#include <afs/ihandle.h>
#include <afs/vnode.h>
#include <afs/volume.h>
#include <afs/ptclient.h>
#include <afs/ptuser.h>
#include <afs/prs_fs.h>
#include <afs/acl.h>
#include <rx/rx.h>
#include <rx/rx_globals.h>
#include <sys/stat.h>
#if ! defined(AFS_SGI_ENV) && ! defined(AFS_AIX32_ENV) && ! defined(AFS_NT40_ENV) && ! defined(AFS_LINUX20_ENV) && !defined(AFS_DARWIN_ENV) && !defined(AFS_XBSD_ENV)
#include <sys/map.h>
#endif
#if !defined(AFS_NT40_ENV)
#include <unistd.h>
#endif
#if !defined(AFS_SGI_ENV) && !defined(AFS_NT40_ENV)
#ifdef	AFS_AIX_ENV
#include <sys/statfs.h>
#include <sys/lockf.h>
#else
#if !defined(AFS_SUN5_ENV) && !defined(AFS_LINUX20_ENV) && !defined(AFS_DARWIN_ENV) && !defined(AFS_XBSD_ENV)
#include <sys/dk.h>
#endif
#endif
#endif
#include <afs/cellconfig.h>
#include <afs/keys.h>

#include <signal.h>
#include <afs/partition.h>
#include "../viced/viced_prototypes.h"
#include "../viced/viced.h"
#include "../viced/host.h"
#include "../viced/callback.h"
#include <afs/unified_afs.h>
#include <afs/audit.h>
#include <afs/afsutil.h>
#include <afs/dir.h>
#include "afsosd.h"
#include "osddbuser.h"

#define FetchStatusProtocol ResidencyMask

extern struct vol_data_v0 *voldata;
struct viced_data_v0 *viceddata = NULL;
extern void SetDirHandle(DirHandle * dir, Vnode * vnode);
extern void FidZap(DirHandle * file);
extern void FidZero(DirHandle * file);
extern int CheckVnode(AFSFid * fid, Volume ** volptr, Vnode ** vptr, int lock);
extern struct Volume *getAsyncVolptr(struct rx_call *call, AFSFid *Fid,
				     afs_uint64 transid, afs_uint64 *offset,
				     afs_uint64 *length);

#ifdef AFS_PTHREAD_ENV
pthread_mutex_t fileproc_glock_mutex;
pthread_mutex_t active_glock_mutex;
#define ACTIVE_LOCK \
    osi_Assert(pthread_mutex_lock(&active_glock_mutex) == 0)
#define ACTIVE_UNLOCK \
    osi_Assert(pthread_mutex_unlock(&active_glock_mutex) == 0)
pthread_mutex_t async_glock_mutex;
#define ASYNC_LOCK \
    osi_Assert(pthread_mutex_lock(&async_glock_mutex) == 0)
#define ASYNC_UNLOCK \
    osi_Assert(pthread_mutex_unlock(&async_glock_mutex) == 0)
#else /* AFS_PTHREAD_ENV */
#define ACTIVE_LOCK
#define ACTIVE_UNLOCK
#define ASYNC_LOCK
#define ASYNC_UNLOCK
#endif /* AFS_PTHREAD_ENV */

#ifdef O_LARGEFILE
#define afs_stat	stat64
#define afs_fstat	fstat64
#define afs_open	open64
#else /* !O_LARGEFILE */
#define afs_stat	stat
#define afs_fstat	fstat
#define afs_open	open
#endif /* !O_LARGEFILE */

/* Useful local defines used by this module */

#define	DONTCHECK	0
#define	MustNOTBeDIR	1
#define	MustBeDIR	2

#define	TVS_SDATA	1
#define	TVS_SSTATUS	2
#define	TVS_CFILE	4
#define	TVS_SLINK	8
#define	TVS_MKDIR	0x10

#define	CHK_FETCH	0x10
#define	CHK_FETCHDATA	0x10
#define	CHK_FETCHACL	0x11
#define	CHK_FETCHSTATUS	0x12
#define	CHK_STOREDATA	0x00
#define	CHK_STOREACL	0x01
#define	CHK_STORESTATUS	0x02

#define	OWNERREAD	0400
#define	OWNERWRITE	0200
#define	OWNEREXEC	0100
#ifdef USE_GROUP_PERMS
#define GROUPREAD       0040
#define GROUPWRITE      0020
#define	GROUPREXEC	0010
#endif

/* The following errors were not defined in NT. They are given unique
 * names here to avoid any potential collision.
 */
#define FSERR_ELOOP 		 90
#define FSERR_EOPNOTSUPP	122
#define FSERR_ECONNREFUSED	130

#define	NOTACTIVECALL	0
#define	ACTIVECALL	1

#define CREATE_SGUID_ADMIN_ONLY 1

#include "vol_osd.h"
#include "osddb.h"
#include "vol_osd_prototypes.h"
#include "rxosd.h"

extern afs_uint32 local_host;

#define RX_OSD                                  2
#define POSSIBLY_OSD          	          0x10000
#define RX_OSD_NOT_ONLINE   		0x1000000

extern afs_uint64 max_move_osd_size;
extern afs_int32 max_move_osd_size_set_by_hand;
extern afs_int64 minOsdFileSize;

extern struct afsconf_dir *confDir;
extern afs_int32 dataVersionHigh;

extern int SystemId;
#if FS_STATS_DETAILED
struct fs_stats_FullPerfStats afs_FullPerfStats;
extern int AnonymousID;
#endif /* FS_STATS_DETAILED */
#if OPENAFS_VOL_STATS
static const char nullString[] = "";
#endif /* OPENAFS_VOL_STATS */

extern struct timeval statisticStart;
extern afs_int64 lastRcvd;
extern afs_int64 lastSent;
extern afs_uint32 KBpsRcvd[96];
extern afs_uint32 KBpsSent[96];
extern afs_int32 FindOsdPasses;
extern afs_int32 FindOsdIgnoreOwnerPass;
extern afs_int32 FindOsdIgnoreLocationPass;
extern afs_int32 FindOsdIgnoreSizePass;
extern afs_int32 FindOsdWipeableDivisor;
extern afs_int32 FindOsdNonWipeableDivisor;
extern afs_int32 FindOsdUsePrior;

extern afs_int32 fastRestore;

static int GetLinkCountAndSize(Volume * vp, FdHandle_t * fdP, int *lc,
		    afs_sfsize_t * size);

struct afs_FSStats {
    afs_int32 NothingYet;
};

struct afs_FSStats afs_fsstats;

int ClientsWithAccessToFileserverPartitions = 0;

afs_int32 MaybeStore_OSD(Volume * volptr, Vnode * targetptr,
		struct AFSFid * Fid,
		struct client * client, struct rx_call * Call,
		afs_fsize_t Pos, afs_fsize_t Length, afs_fsize_t FileLength,
		Vnode *parentwhentargetnotdir, char *fileName);

afs_int32 FetchData_OSD(Volume * volptr, Vnode **targetptr,
		struct rx_call * Call, afs_sfsize_t Pos,
		afs_sfsize_t Len, afs_int32 Int64Mode,
		int client_vice_id, afs_int32 MyThreadEntry);



#ifdef AFS_SGI_XFS_IOPS_ENV
#include <afs/xfsattrs.h>
static int
GetLinkCount(Volume * avp, struct stat *astat)
{
    if (!strcmp("xfs", astat->st_fstype)) {
	return (astat->st_mode & AFS_XFS_MODE_LINK_MASK);
    } else
	return astat->st_nlink;
}
#else
#define GetLinkCount(V, S) (S)->st_nlink
#endif

#define MAXFSIZE (~(afs_fsize_t) 0)
#define AFS_ASYNC_IO_TIMEOUT 60

struct asyncTrans {
    struct asyncTrans *next;
    afs_uint64 transid;
    afs_uint64 offset;
    afs_uint64 length;
    afs_uint32 expires;
    afs_uint32 flag;
    afs_uint32 host;
    afs_uint16 port;
};

struct asyncAccess {
    struct asyncAccess *next;
    AFSFid fid;
    Volume *volptr;
    struct asyncTrans *users;
    afs_uint32 writer;
    afs_uint32 readers;
    afs_uint32 waiters;
    pthread_cond_t cond;
};

struct asyncAccess *asyncAccesses = 0;
afs_uint64 maxAsyncTransid = 0;
afs_uint32 activeTransactions = 0;
afs_uint32 activeFiles = 0;
afs_uint32 maxActiveFiles = 0;
afs_uint32 maxActiveTransactions = 0;

#define ASYNC_WRITING 1

static afs_int32
createAsyncTransaction(struct rx_call *call, AFSFid *Fid, afs_int32 flag,
                        afs_fsize_t offset, afs_fsize_t length,
                        afs_uint64 *transid, afs_uint32 *expires)
{
    afs_int32 code;
    struct asyncAccess *a;
    struct asyncTrans *t;
    afs_uint32 host = 0;
    afs_uint16 port = 0;
    Volume *tvolptr = 0;

    if (call) {
	struct rx_peer* peer = rx_PeerOf(rx_ConnectionOf(call));
        host = rx_HostOf(peer);
        port = rx_PortOf(peer);
    }

    ViceLog(3, ("createAsynctransaction %u.%u.%u flag 0x%x from %u.%u.%u.%u:%u\n",
                        Fid->Volume,
                        Fid->Vnode,
                        Fid->Unique,
                        flag,
                        (ntohl(host) >> 24) & 0xff,
                        (ntohl(host) >> 16) & 0xff,
                        (ntohl(host) >> 8) & 0xff,
                        ntohl(host) & 0xff,
                        ntohs(port)));
    /* we should protect the transaction against a 'vos move' */
    code = CheckVnode(Fid, &tvolptr, NULL, READ_LOCK);
    if (code) {
        ViceLog(0, ("createAsynctransaction CheckVnode for %u.%u.%u returned %d\n",
                        Fid->Volume,
                        Fid->Vnode,
                        Fid->Unique,
                        code));
        return code;
    }
    ASYNC_LOCK;
    for (a = (struct asyncAccess *) asyncAccesses; a; a=a->next) {
        if (a->fid.Volume == Fid->Volume && a->fid.Vnode == Fid->Vnode)
            break;
    }
    if (!a) {
        a = (struct asyncAccess *) malloc(sizeof(struct asyncAccess));
        if (!a) {
            ASYNC_UNLOCK;
            return ENOMEM;
        }
        memset(a, 0, sizeof(struct asyncAccess));
        a->fid = *Fid;
        a->volptr = tvolptr;
        tvolptr = 0;
        a->next = (struct asyncAccess *) asyncAccesses;
        asyncAccesses = a;
        activeFiles++;
        if (activeFiles > maxActiveFiles)
            maxActiveFiles = activeFiles;
    } else if (!a->volptr) {
        a->volptr = tvolptr;
        tvolptr = 0;
    }
    if (tvolptr)
        VPutVolume(tvolptr);
    t = (struct asyncTrans *) malloc(sizeof(struct asyncTrans));
    if (!t) {
        ASYNC_UNLOCK;
        return ENOMEM;
    }
    memset(t, 0, sizeof(struct asyncTrans));
    activeTransactions++;
    if (activeTransactions > maxActiveTransactions);
        maxActiveTransactions = activeTransactions;
    if (flag & ASYNC_WRITING) {
        if (a->writer && flag == ASYNC_WRITING) {
            if (a->users->host == host && a->users->port == port) {
                /* Old client may do multiple GetOSDlocation before StoreMini */
                free(t);
                activeTransactions--;
                t = a->users;
                t->offset = offset;
                t->length = length;
                if (transid)
                    *transid = t->transid;
                t->flag = flag;
                t->expires = FT_ApproxTime() + AFS_ASYNC_IO_TIMEOUT + 10;
                if (expires)
                    *expires = AFS_ASYNC_IO_TIMEOUT;
                ASYNC_UNLOCK;
                ViceLog(3, ("Implicit EndAsyncTransaction %u.%u.%u from %u.%u.%u.%u:%u by reusing old transaction\n",
                        Fid->Volume,
                        Fid->Vnode,
                        Fid->Unique,
                        (ntohl(host) >> 24) & 0xff,
                        (ntohl(host) >> 16) & 0xff,
                        (ntohl(host) >> 8) & 0xff,
                        ntohl(host) & 0xff,
                        ntohs(port)));
                return 0;
            }
        }
        if (a->readers || a->writer) { /* Have to wait for transactions to end */
            a->waiters++;
            while (a->readers || a->writer) {
                CV_WAIT(&a->cond, &async_glock_mutex);
	    }
	    a->waiters--;
	}
	a->writer = 1;
    } else {
        if (a->writer) { /* Have to wait for write transaction to end */
            a->waiters++;
            while (a->writer) {
	 	CV_WAIT(&a->cond, &async_glock_mutex);
	    }            a->waiters--;
        }
        a->readers++;
    }
    t->next = a->users;
    a->users = t;
    t->host = host;
    t->port = port;
    t->offset = offset;
    t->length = length;
    if (transid) {
        t->transid = ++maxAsyncTransid;
        *transid = t->transid;
    }
    t->flag = flag;
    if (flag & (CALLED_FROM_STOREDATA | CALLED_FROM_FETCHDATA))
        t->expires = 0xffffffff;
    else
        t->expires = FT_ApproxTime() + AFS_ASYNC_IO_TIMEOUT + 10;
    if (expires)
        *expires = AFS_ASYNC_IO_TIMEOUT;
    ASYNC_UNLOCK;
    return 0;
}

static afs_int32
extendAsyncTransaction(struct rx_call *call, AFSFid *Fid, afs_uint64 transid,
                        afs_uint32 *expires)
{
    struct asyncAccess *a, *a2;
    struct asyncTrans *t, *t2;
    afs_uint32 host = 0;
    afs_uint16 port = 0;

    if (call) {
	struct rx_peer* peer = rx_PeerOf(rx_ConnectionOf(call));
        host = rx_HostOf(peer);
        port = rx_PortOf(peer);
    }
    ASYNC_LOCK;
    a2 = (struct asyncAccess *) &asyncAccesses;
    for (a=a2->next; a; a=a->next) {
        if (a->fid.Volume == Fid->Volume && a->fid.Vnode == Fid->Vnode)
            break;
        a2 = a;
    }
    if (!a) {
        ASYNC_UNLOCK;
        return ENOENT;
    }
    t2 = (struct asyncTrans *) &a->users;       /* only for use of field next */
    for (t=t2->next; t; t=t->next) {
        if (call && (host == t->host && port == t->port && t->transid == transid))
            break;
        if (!call && t->transid == transid)
            break;
        t2 = t;
    }
    if (!t) {   /* we couldin't identify the transaction */
        ASYNC_UNLOCK;
        return ENOENT;
    }
    t->expires = FT_ApproxTime() + AFS_ASYNC_IO_TIMEOUT + 10;
    *expires = AFS_ASYNC_IO_TIMEOUT;
    ASYNC_UNLOCK;
    return 0;
}

static afs_int32
EndAsyncTransaction(struct rx_call *call, AFSFid *Fid, afs_uint64 transid)
{
    struct asyncAccess *a, *a2;
    struct asyncTrans *t, *t2;
    afs_uint32 host = 0;
    afs_uint16 port = 0;

    if (call) {
	struct rx_peer* peer = rx_PeerOf(rx_ConnectionOf(call));
        host = rx_HostOf(peer);
        port = rx_PortOf(peer);
    }
    ViceLog(3, ("EndAsyncTransaction %u.%u.%u from %u.%u.%u.%u:%u\n",
                        Fid->Volume,
                        Fid->Vnode,
                        Fid->Unique,
                        (ntohl(host) >> 24) & 0xff,
                        (ntohl(host) >> 16) & 0xff,
                        (ntohl(host) >> 8) & 0xff,
                        ntohl(host) & 0xff,
                        ntohs(port)));
    ASYNC_LOCK;
    a2 = (struct asyncAccess *) &asyncAccesses;
    for (a=a2->next; a; a=a->next) {
        if (a->fid.Volume == Fid->Volume && a->fid.Vnode == Fid->Vnode)
            break;
        a2 = a;
    }
    if (!a) {
        ASYNC_UNLOCK;
        ViceLog(1, ("EndAsyncTransaction: couldn't find file %u.%u.%u called from %u.%u.%u.%u:%u\n",
                        Fid->Volume,
                        Fid->Vnode,
                        Fid->Unique,
                        (ntohl(host) >> 24) & 0xff,
                        (ntohl(host) >> 16) & 0xff,
                        (ntohl(host) >> 8) & 0xff,
                        ntohl(host) & 0xff,
                        ntohs(port)));
        return ENOENT;
    }
    t2 = (struct asyncTrans *) &a->users;       /* only for use of field next */
    for (t=t2->next; t; t=t->next) {
        if (call && (host == t->host && port == t->port && t->transid == transid))
            break;
        if (!call && t->transid == transid)
            break;
        t2 = t;
    }
    if (!t) {   /* we couldn't identify the transaction */
        ASYNC_UNLOCK;
        ViceLog(1, ("EndAsyncTransaction: couldn't find transaction for %u.%u.%u from %u.%u.%u.%u:%u\n",
                        Fid->Volume,
                        Fid->Vnode,
                        Fid->Unique,
                        (ntohl(host) >> 24) & 0xff,
                        (ntohl(host) >> 16) & 0xff,
                        (ntohl(host) >> 8) & 0xff,
                        ntohl(host) & 0xff,
                        ntohs(port)));
        return ENOENT;
    }
    t2->next = t->next;
    free(t);
    activeTransactions--;
    if (a->writer)
        a->writer = 0;
    else
        a->readers--;
    if (a->readers) {   /* still someone else reading. We can't do any more */
        ASYNC_UNLOCK;
        return 0;
    }
    if (a->waiters) {   /* wake up the waiters */
	osi_Assert(pthread_cond_broadcast(&a->cond) == 0);
	ASYNC_UNLOCK;
        return 0;
    }
    a2->next = a->next;
    activeFiles--;
    ASYNC_UNLOCK;
    if (a->volptr)
        VPutVolume(a->volptr);
    free(a);
    return 0;
}

Volume *
getAsyncVolptr(struct rx_call *call, AFSFid *Fid, afs_uint64 transid,
               afs_uint64 *offset, afs_uint64 *length)
{
    struct asyncAccess *a, *a2;
    struct asyncTrans *t, *t2;
    afs_uint32 host = 0;
    afs_uint16 port = 0;
    Volume *volptr = 0;
    *offset = 0;
    *length = 0;

    if (call) {
	struct rx_peer* peer = rx_PeerOf(rx_ConnectionOf(call));
        host = rx_HostOf(peer);
        port = rx_PortOf(peer);
    }
    ViceLog(3, ("getAsyncVolptr %u.%u.%u from %u.%u.%u.%u:%u\n",
                        Fid->Volume,
                        Fid->Vnode,
                        Fid->Unique,
                        (ntohl(host) >> 24) & 0xff,
                        (ntohl(host) >> 16) & 0xff,
                        (ntohl(host) >> 8) & 0xff,
                        ntohl(host) & 0xff,
                        ntohs(port)));
    ASYNC_LOCK;
    a2 = (struct asyncAccess *) &asyncAccesses;
    for (a=a2->next; a; a=a->next) {
        if (a->fid.Volume == Fid->Volume && a->fid.Vnode == Fid->Vnode)
            break;
        a2 = a;
    }
    if (!a) {
        ASYNC_UNLOCK;
        ViceLog(1, ("getAsyncVolptr: couldn't find file %u.%u.%u called from %u.%u.%u.%u:%u\n",
                        Fid->Volume,
                        Fid->Vnode,
                        Fid->Unique,
                        (ntohl(host) >> 24) & 0xff,
                        (ntohl(host) >> 16) & 0xff,
                        (ntohl(host) >> 8) & 0xff,
                        ntohl(host) & 0xff,
                        ntohs(port)));
        return volptr;
    }
    t2 = (struct asyncTrans *) &a->users;       /* only for use of field next */
    for (t=t2->next; t; t=t->next) {
        if (call && (host == t->host && port == t->port && t->transid == transid))
            break;
        if (!call && t->transid == transid)
            break;
        t2 = t;
    }
    if (!t) {   /* we couldin't identify the transaction */
        ASYNC_UNLOCK;
        ViceLog(1, ("getAsyncVolptr: couldn't find transaction for %u.%u.%u from %u.%u.%u.%u:%u\n",
                        Fid->Volume,
                        Fid->Vnode,
                        Fid->Unique,
                        (ntohl(host) >> 24) & 0xff,
                        (ntohl(host) >> 16) & 0xff,
                        (ntohl(host) >> 8) & 0xff,
                        ntohl(host) & 0xff,
                        ntohs(port)));
        return volptr;
    }
    volptr = a->volptr;
    *offset = t->offset;
    *length = t->length;
    a->volptr = NULL;
    ASYNC_UNLOCK;
    return volptr;
}

afs_int32
FakeEndAsyncStore(AFSFid *fid, afs_uint64 transid)
{
    Error errorCode;
    afs_int32 blocks;
    Vnode *targetptr = 0;
    Volume *volptr = 0;
    Inode ino;
    afs_sfsize_t DataLength;
    afs_uint64 VnodeLength;
    afs_int32 linkCount;
    FdHandle_t *fdP;

    errorCode = CheckVnode(fid, &volptr, &targetptr, WRITE_LOCK);
    if (errorCode) {
        ViceLog(0, ("FakeEndAsyncStore: CheckVnode or %u.%u.%u returned %d\n",
                        fid->Volume, fid->Vnode, fid->Unique, errorCode));
        goto Out;
    }
    VN_GET_LEN(VnodeLength, targetptr);
    ino = VN_GET_INO(targetptr);
    if (VALID_INO(ino)) {       /* normal AFS file in local_disk */
        fdP = IH_OPEN(targetptr->handle);
        if (fdP) {
            if (GetLinkCountAndSize(volptr, fdP, &linkCount, &DataLength) >= 0) {
                VN_SET_LEN(targetptr, DataLength);
                blocks = (afs_int32) ((DataLength - VnodeLength) >> 10);
                V_diskused(volptr) += blocks;
            }
            FDH_CLOSE(fdP);
        }
    }
    else {
        errorCode = actual_length(volptr, &targetptr->disk,
                                targetptr->vnodeNumber, &DataLength);
        if (!errorCode) {
            VN_SET_LEN(targetptr, DataLength);
            blocks = (afs_int32) ((DataLength - VnodeLength) >> 10);
            V_diskused(volptr) += blocks;
        }
    }
    targetptr->disk.unixModifyTime = FT_ApproxTime();
    targetptr->disk.serverModifyTime = FT_ApproxTime();
    targetptr->changed_newTime = 1;
    VPutVnode(&errorCode, targetptr);
    VPutVolume(volptr);
Out:
    EndAsyncTransaction(NULL, fid, transid);
    return errorCode;
}

afs_int32
TimeoutAsyncTransactions(void)
{
    struct asyncAccess *a, *a2;
    struct asyncTrans *t, *t2;
    char hoststr[16];
    afs_uint32 now;

restart:
    now = FT_ApproxTime();
    ASYNC_LOCK;
    a2 = (struct asyncAccess *) &asyncAccesses;
    for (a=a2->next; a; a=a->next) {
        t2 = (struct asyncTrans *) &a->users;   /* only for use of field next */
        for (t=t2->next; t; t=t->next) {
            if (now > t->expires) {
                if (a->writer) {
                    ViceLog(0,("Async store transaction %llu for %u.%u.%u with flag 0x%x timed out\n",
                        t->transid,
                        a->fid.Volume, a->fid.Vnode, a->fid.Unique,
                        t->flag));
                    ASYNC_UNLOCK;
                    FakeEndAsyncStore(&a->fid, t->transid);
                    goto restart;
                } else {
                    ViceLog(0,("Async fetch transaction %llu for %u.%u.%u from %s:%d with flag 0x%x timed out\n",
                        t->transid,
                        a->fid.Volume, a->fid.Vnode, a->fid.Unique,
                        afs_inet_ntoa_r(t->host, hoststr), ntohs(t->port),
                        t->flag));
                    t2->next = t->next;
                    free(t);
                    activeTransactions--;
                    a->readers--;
                    if (a->waiters && !a->readers) {
                        osi_Assert(pthread_cond_broadcast(&a->cond) == 0);
                    }
                    t = t2;
                }
                if (!a->writer && !a->readers && !a->waiters) {
                    a2->next = a->next;
                    activeFiles--;
                    ASYNC_UNLOCK;
                    if (a->volptr)
                        VPutVolume(a->volptr);
                    free(a);
                    goto restart;
                }
            } else
                t2 = t;
        }
        a2 = a;
    }
    ASYNC_UNLOCK;
    return 0;
}

afs_int32
asyncActive(AFSFid *fid)
{
    struct asyncAccess *a;

    ASYNC_LOCK;
    for (a=asyncAccesses; a; a=a->next) {
        if (a->fid.Volume == fid->Volume
          && a->fid.Vnode == fid->Vnode
          && a->fid.Unique == fid->Unique) {
            ASYNC_UNLOCK;
            return 1;
        }
    }
    ASYNC_UNLOCK;
    return 0;
}
char *ipstring(char *str, struct rx_call *acall, afs_int32 lng)
{
    struct rx_peer *peer = rx_PeerOf(rx_ConnectionOf(acall));
    if (lng < 22)
	return "string too short";
    memset(str, 0, lng);
    sprintf(str, "%u.%u.%u.%u:%u",
            (ntohl(rx_HostOf(peer)) >> 24) & 0xff,
            (ntohl(rx_HostOf(peer)) >> 16) & 0xff,
            (ntohl(rx_HostOf(peer)) >> 8) & 0xff,
            ntohl(rx_HostOf(peer)) & 0xff,
            ntohs(rx_PortOf(peer)));
    return str;
}

afs_int32
FsCmd(struct rx_call * acall, struct AFSFid * Fid,
		    struct FsCmdInputs * Inputs,
		    struct FsCmdOutputs * Outputs)
{
    Error code = 0;

    switch (Inputs->command) {
    case CMD_OSD_ARCHIVE:
	{
	    struct rx_connection *tcon = 0;
	    struct host *thost = 0;
	    Volume *volptr = 0;
	    Vnode *targetptr = 0, *parentwhentargetnotdir = 0;
	    Vnode tparent;
	    afs_int32 rights, anyrights;
	    struct client *client = 0;
	    afs_uint64 transid = 0;

	    code = createAsyncTransaction(acall, Fid, CALLED_FROM_FETCHDATA,
					0, MAXFSIZE, &transid, NULL);
	    if (code) {
	        Outputs->code = code;
	        code = 0;
	        break;
	    }

	    if ((code = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
		goto Bad_OSD_Archive;

	    if ((code =
	 	GetVolumePackage(acall, Fid, &volptr, &targetptr, MustNOTBeDIR,
			  &parentwhentargetnotdir, &client, SHARED_LOCK,
			  &rights, &anyrights))) {
		goto Bad_OSD_Archive;
	    }
	    if (VanillaUser(client)) {
	        if (Inputs->int32s[1] || !(rights & PRSFS_ADMINISTER)) {
	            code = EACCES;
		    goto Bad_OSD_Archive;
		}
	    }
	    if (parentwhentargetnotdir != NULL) {
		memcpy((char *)&tparent, (char *)parentwhentargetnotdir, sizeof(tparent));
		VPutVnode(&code, parentwhentargetnotdir);
		parentwhentargetnotdir = NULL;
	    }
	    code = osd_archive(targetptr, Inputs->int32s[0], Inputs->int32s[1]);

    Bad_OSD_Archive:
	    PutVolumePackage(acall, parentwhentargetnotdir, targetptr, (Vnode *) 0,
			volptr, &client);
	    CallPostamble(tcon, code, thost);
	    if (transid)
		EndAsyncTransaction(acall, Fid, transid);

	    Outputs->code = code;
	    code = 0;
	    break;
	}
    case CMD_WIPEFILE:
	{
	    struct rx_connection *tcon = 0;
	    struct host *thost = 0;
	    Volume *volptr = 0;
	    Vnode *targetptr = 0, *parentwhentargetnotdir = 0;
	    afs_int32 rights, anyrights;
	    struct client *client = 0;
	    struct AFSStoreStatus InStatus;
	    afs_uint32 version = Inputs->int32s[0];
	    afs_uint64 transid = 0;

	    code = createAsyncTransaction(acall, Fid, CALLED_FROM_STOREDATA,
					0, MAXFSIZE, &transid, NULL);
	    if (code) {
	        Outputs->code = code;
	        code = 0;
	        break;
	    }

	    if ((code = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
		goto Bad_OSD_Wipe;

	    if ((code =
	 	GetVolumePackage(acall, Fid, &volptr, &targetptr, MustNOTBeDIR,
			  &parentwhentargetnotdir, &client, WRITE_LOCK,
			  &rights, &anyrights))) {
		goto Bad_OSD_Wipe;
	    }
	    memset(&InStatus, 0, sizeof(InStatus));
	    if (version && VanillaUser(client)) {
	        code = EACCES;
		goto Bad_OSD_Wipe;
	    }
	    if ((code =
	 	Check_PermissionRights(targetptr, client, rights, CHK_STOREDATA,
				&InStatus))) {
	        if (VanillaUser(client) && !(rights & PRSFS_ADMINISTER)) {
	            code = EACCES;
		    goto Bad_OSD_Wipe;
		}
            }
	    code = remove_osd_online_file(targetptr, version);

    Bad_OSD_Wipe:
	    PutVolumePackage(acall, parentwhentargetnotdir, targetptr, (Vnode *) 0,
			volptr, &client);
	    if (transid)
		EndAsyncTransaction(acall, Fid, transid);

	    CallPostamble(tcon, code, thost);
	    Outputs->code = code;
	    code = 0;
	    break;
	}
    case CMD_STRIPED_OSD_FILE:
	{
	    struct rx_connection *tcon = 0;
	    struct host *thost = 0;
	    Volume *volptr = 0;
	    Vnode *targetptr = 0, *parentwhentargetnotdir = 0;
	    afs_int32 rights, anyrights;
	    struct client *client = 0;
	    struct AFSStoreStatus InStatus;

	    if ((code = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
		goto Bad_StripedOsdFile;

	    if ((code =
	 	GetVolumePackage(acall, Fid, &volptr, &targetptr, MustNOTBeDIR,
			  &parentwhentargetnotdir, &client, WRITE_LOCK,
			  &rights, &anyrights))) {
		goto Bad_StripedOsdFile;
	    }
	    memset(&InStatus, 0, sizeof(InStatus));
	    if ((code =
	 	Check_PermissionRights(targetptr, client, rights, CHK_STOREDATA,
				&InStatus))) {
		goto Bad_StripedOsdFile;
            }
	    code = CreateStripedOsdFile(targetptr, Inputs->int32s[0],
					Inputs->int32s[1], Inputs->int32s[2],
					Inputs->int64s[0]);

    Bad_StripedOsdFile:
	    PutVolumePackage(acall, parentwhentargetnotdir, targetptr, (Vnode *) 0,
			volptr, &client);
	    CallPostamble(tcon, code, thost);
	    Outputs->code = code;
	    code = 0;
	    break;
	}
    case CMD_REPLACE_OSD:
	{
	    struct rx_connection *tcon = 0;
	    struct host *thost = 0;
	    Volume *volptr = 0;
	    Vnode *targetptr = 0, *parentwhentargetnotdir = 0;
	    afs_int32 rights, anyrights;
	    struct client *client = 0;
	    struct AFSStoreStatus InStatus;
	    afs_uint64 transid = 0;

	    code = createAsyncTransaction(acall, Fid, CALLED_FROM_STOREDATA,
					0, MAXFSIZE, &transid, NULL);
	    if (code) {
	        Outputs->code = code;
	        code = 0;
	        break;
	    }

	    if ((code = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
		goto Bad_ReplaceOSD;

	    if ((code =
	 	GetVolumePackage(acall, Fid, &volptr, &targetptr, MustNOTBeDIR,
			  &parentwhentargetnotdir, &client, SHARED_LOCK,
			  &rights, &anyrights))) {
		goto Bad_ReplaceOSD;
	    }
	    memset(&InStatus, 0, sizeof(InStatus));
	    if (VanillaUser(client)) {
		code = EPERM;
		goto Bad_ReplaceOSD;
	    }
	    if (parentwhentargetnotdir != NULL) {
		VPutVnode(&code, parentwhentargetnotdir);
		parentwhentargetnotdir = NULL;
	    }
	    code = replace_osd(targetptr, Inputs->int32s[0], Inputs->int32s[1],
				&Outputs->int32s[0]);
	    if (!code) {
		rx_KeepAliveOn(acall);
		BreakCallBack(client->host, Fid, 0);
	    }

    Bad_ReplaceOSD:
	    PutVolumePackage(acall, parentwhentargetnotdir, targetptr, (Vnode *) 0,
			volptr, &client);
	    if (transid)
		EndAsyncTransaction(acall, Fid, transid);

	    CallPostamble(tcon, code, thost);
	    Outputs->code = code;
	    code = 0;
	    break;
	}
    case CMD_GET_ARCH_OSDS:
	{
	    struct rx_connection *tcon = 0;
	    struct host *thost = 0;
	    Volume *volptr = 0;
	    Vnode *targetptr = 0, *parentwhentargetnotdir = 0;
	    afs_int32 rights, anyrights;
	    struct client *client = 0;

	    if ((code = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
		goto Bad_Get_Arch_Osds;

	    if ((code =
	 	GetVolumePackage(acall, Fid, &volptr, &targetptr, MustNOTBeDIR,
			  &parentwhentargetnotdir, &client, READ_LOCK,
			  &rights, &anyrights))) {
		goto Bad_Get_Arch_Osds;
	    }
	    if (VanillaUser(client) && !(rights & PRSFS_ADMINISTER)) {
	        code = EACCES;
		goto Bad_Get_Arch_Osds;
	    }
	    code = get_arch_osds(targetptr, &Outputs->int64s[0],
				 &Outputs->int32s[0]);
        Bad_Get_Arch_Osds:
	    PutVolumePackage(acall, parentwhentargetnotdir, targetptr, (Vnode *) 0,
			volptr, &client);
	    CallPostamble(tcon, code, thost);
	    Outputs->code = code;
	    code = 0;
	    break;
	}
    case CMD_LIST_OSDS:
	{
	    struct rx_connection *tcon = 0;
	    struct host *thost = 0;
	    Volume *volptr = 0;
	    Vnode *targetptr = 0, *parentwhentargetnotdir = 0;
	    afs_int32 rights, anyrights;
	    struct client *client = 0;
	    struct AFSStoreStatus InStatus;

	    if ((code = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
		goto Bad_List_Osds;

	    if ((code =
	 	GetVolumePackage(acall, Fid, &volptr, &targetptr, MustNOTBeDIR,
			  &parentwhentargetnotdir, &client, READ_LOCK,
			  &rights, &anyrights))) {
		goto Bad_List_Osds;
	    }
	    memset(&InStatus, 0, sizeof(InStatus));
	    if ((code =
	 	Check_PermissionRights(targetptr, client, rights, CHK_FETCHDATA,
				&InStatus))) {
	        if (VanillaUser(client)) {
	            code = EACCES;
		    goto Bad_List_Osds;
		}
            }
	    code = list_osds(targetptr, &Outputs->int32s[0]);

        Bad_List_Osds:
	    PutVolumePackage(acall, parentwhentargetnotdir, targetptr, (Vnode *) 0,
			volptr, &client);
	    CallPostamble(tcon, code, thost);
	    Outputs->code = code;
	    code = 0;
	    break;
	}
    case CMD_SET_POLICY:
	{
	    struct rx_connection *tcon = 0;
	    struct host *thost = 0;
	    Volume *volptr = 0;
	    Vnode *targetptr = 0, *parentwhentargetnotdir = 0;
	    afs_int32 rights, anyrights;
	    struct client *client = 0;
	    afs_uint32 policy = Inputs->int32s[0];

	    if ((code = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
		goto Bad_SetPolicy;

	    if (!(Fid->Vnode & 1)) {	/* Must be a directory */
		code = ENOTDIR;
		goto Bad_SetPolicy;
	    }

	    if ((code =
	 	GetVolumePackage(acall, Fid, &volptr, &targetptr, MustBeDIR,
			  &parentwhentargetnotdir, &client, WRITE_LOCK,
			  &rights, &anyrights))) {
		goto Bad_SetPolicy;
	    }
	    /* For the momoent we don't give this feature to normal users */
	    if (VanillaUser(client)) {
	        code = EPERM;
		goto Bad_SetPolicy;
	    }

	    if (!V_osdPolicy(volptr)) {
		code = EINVAL;
		goto Bad_SetPolicy;
	    }

	    targetptr->disk.osdPolicyIndex = policy;
	    targetptr->changed_newTime = 1;
 Bad_SetPolicy:
	    PutVolumePackage(acall, parentwhentargetnotdir, targetptr, (Vnode *) 0,
			volptr, &client);
	    CallPostamble(tcon, code, thost);
            Outputs->code = code;
	    code = 0;
	    break;
	}
    case CMD_GET_POLICIES:
        {
            struct rx_connection *tcon = 0;
            struct host *thost = 0;
            Volume *volptr = 0;
            Vnode *targetptr = 0, *parentwhentargetnotdir = 0;
            afs_int32 rights, anyrights;
            struct client *client = 0;

	    if ((code = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
                goto Bad_Get_Policies;

            if ((code =
                GetVolumePackage(acall, Fid, &volptr, &targetptr, 0,
                          &parentwhentargetnotdir, &client, READ_LOCK,
                          &rights, &anyrights)))
                goto Bad_Get_Policies;

            Outputs->int32s[0] = (afs_int32)V_osdPolicy(volptr);
            if ( Fid->Vnode&1 )
                Outputs->int32s[1] = (afs_int32)targetptr->disk.osdPolicyIndex;
            else
                Outputs->int32s[1] =
                        (afs_int32)parentwhentargetnotdir->disk.osdPolicyIndex;

          Bad_Get_Policies:
            PutVolumePackage(acall, parentwhentargetnotdir, targetptr, (Vnode *) 0,
                        volptr, &client);
            CallPostamble(tcon, code, thost);
            Outputs->code = code;
            code = 0;
            break;
        }
    default:
        code = EINVAL;
    }
    ViceLog(1,("FsCmd: cmd = %d, code=%d\n",
			Inputs->command, Outputs->code));
    return code;
}

afs_int32
SRXAFSOSD_FsCmd(struct rx_call * acall, struct AFSFid * Fid,
		    struct FsCmdInputs * Inputs,
		    struct FsCmdOutputs * Outputs)
{
    afs_int32 errorCode;

    errorCode = FsCmd(acall, Fid, Inputs, Outputs);
    return errorCode;
}

afs_int32
SRXAFSOSD_CheckOSDconns(struct rx_call *acall)
{
    char str[22];
    ViceLog(1,("SRXAFSOSD_CheckOSDconns called from %s\n",
		ipstring(str, acall, sizeof(str))));
    checkOSDconnections();
    return 0;
}

afs_int32
GetOSDlocation(struct rx_call *acall, AFSFid *Fid, afs_uint64 offset,
                        afs_uint64 length, afs_uint64 filelength,
			afs_int32 flag,
                        AFSFetchStatus *OutStatus, AFSCallBack *CallBack,
                        struct async *a)
{
#if defined(AFS_NAMEI_ENV)
    Vnode * targetptr = 0;              /* pointer to input fid */
    Vnode * parentwhentargetnotdir = 0; /* parent of Fid to get ACL */
    int     errorCode = 0;              /* return code for caller */
    Volume * volptr = 0;                /* pointer to the volume header */
    struct client * client = 0;         /* pointer to client structure */
    struct rx_connection *tcon;
    struct host *thost;
    afs_int32 rights, anyrights;        /* rights for this and any user */
    struct AFSStoreStatus InStatus;      /* Input status for new dir */
    afs_int64 InitialVnodeFileLength;
    afs_int64 maxLength;
    afs_int64 Delta, blocks;
    afs_int32 writing = flag & OSD_WRITING;
    afsUUID *tuuid;
    char str[22];

#define hundredMB 100 * 1024 * 1024

    ViceLog(1,("GetOSDlocation: Fid = %u.%u.%u for %s offset %llu length %llu\n",
            Fid->Volume, Fid->Vnode, Fid->Unique, writing ? "write" : "read",
            offset, length));

    if (a->type == 1) {
        struct osd_file *file;
        file = a->async_u.p1.osd_fileList_val;
	if (!file) {
	    file = (struct osd_file *) malloc(sizeof(struct osd_file));
	    memset(file, 0, sizeof(struct osd_file));
	    a->async_u.p1.osd_fileList_val = file;
	    a->async_u.p1.osd_fileList_len = 1;
	}
        file->segmList.osd_segmList_len = 0; /* just to have an initial value */
        file->segmList.osd_segmList_val = 0;
    } else
	return EINVAL;
    if ((errorCode = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
        goto Bad_GetOSDloc;

    tuuid = &thost->interface->uuid;
    if (!tuuid) {
	ViceLog(0, (" No thost->interface-uuid\n"));
	tuuid = NULL;
    }

    /*
     * Get associated volume/vnode for the stored file; caller's rights
     * are also returned
     */
    if ((errorCode = GetVolumePackage(acall, Fid, &volptr, &targetptr,
                                 MustNOTBeDIR, &parentwhentargetnotdir,
                                 &client,
                                 flag & FS_OSD_COMMAND ? READ_LOCK : WRITE_LOCK,
                                 &rights, &anyrights))) {
	if (writing || (flag & FS_OSD_COMMAND))
            goto Bad_GetOSDloc;
	if ((errorCode = GetVolumePackage(acall, Fid, &volptr, &targetptr,
                                 MustNOTBeDIR, &parentwhentargetnotdir,
                                 &client, READ_LOCK,
                                 &rights, &anyrights)))
            goto Bad_GetOSDloc;
    }

    memset(&InStatus, 0, sizeof(InStatus));
    if ((errorCode = Check_PermissionRights(targetptr, client, rights,
                        writing ? CHK_STOREDATA : CHK_FETCHDATA, &InStatus)))
            goto Bad_GetOSDloc;

    /* Get the updated File's status back to the caller */
    if (OutStatus)
        GetStatus(targetptr, OutStatus, rights, anyrights, parentwhentargetnotdir);

    VN_GET_LEN(InitialVnodeFileLength, targetptr);
    maxLength = InitialVnodeFileLength;
    if (writing) { /* only set for write request */
        if (filelength)
	    maxLength = filelength;
        else if (offset + length > 0) 	/* corrective action for old clients */
	    filelength = maxLength;
        if (offset + length > maxLength)
	    maxLength = offset + length;
        Delta = maxLength - InitialVnodeFileLength;
        if (Delta > 0) {
            blocks = Delta >> 10;
            if (V_maxquota(volptr)) {
	        if (blocks > (V_maxquota(volptr) - V_diskused(volptr))) {
                    if ((blocks + V_diskused(volptr) - V_maxquota(volptr)) * 100 / V_maxquota(volptr) > 5 ) { /* allow 5 % over quota */
                        errorCode = ENOSPC;
                        goto Bad_GetOSDloc;
		    }
		} else
		    Delta = ((afs_int64)(V_maxquota(volptr)
					- V_diskused(volptr))) << 10;
            } else
		Delta = 0x40000000;
	    if (Delta > 0x40000000)
		Delta = 0x40000000;
            maxLength += Delta;
        }
    }
    if (a->type == 1 || a->type == 2)
        errorCode = get_osd_location(volptr, targetptr, flag, client->ViceId,
				offset, length, filelength,
				rx_PeerOf(rx_ConnectionOf(acall)),
				tuuid, maxLength, a);
    else
	errorCode = EINVAL;

    if (CallBack && !errorCode && !writing && !(flag & FS_OSD_COMMAND)) {
	afs_int32 cb;
        /* if a r/w volume, promise a callback to the caller */
        if (VolumeWriteable(volptr)) {
	    cb = AddCallBack1(client->host, Fid, 0, 1, 0);
            SetCallBackStruct(cb, CallBack);
        } else {
            struct AFSFid myFid;
            bzero(&myFid, sizeof(struct AFSFid));
            myFid.Volume = Fid->Volume;
	    cb = AddCallBack1(client->host, &myFid, 0, 3, 0);
            SetCallBackStruct(cb, CallBack);
        }
    }

Bad_GetOSDloc:
    if (errorCode && !(flag & (FS_OSD_COMMAND | CALLED_FROM_START_ASYNC))) {
	EndAsyncTransaction(acall, Fid, 0);
    }
    PutVolumePackage(acall, parentwhentargetnotdir, targetptr, (Vnode *)0,
		     volptr, &client);
    if (errorCode && errorCode != 1096)
        ViceLog(0,("GetOsdLoc for %u.%u.%u returns %d to %s\n",
		Fid->Volume, Fid->Vnode, Fid->Unique, errorCode,
		ipstring(str, acall, sizeof(str))));
    else
        ViceLog(1,("GetOsdLoc for %u.%u.%u returns %d\n",
			Fid->Volume, Fid->Vnode, Fid->Unique, errorCode));
    errorCode = CallPostamble(tcon, errorCode, thost);
    if (errorCode < 0)
        errorCode = EIO;
    return errorCode;
#else
    return ENOSYS;
#endif
}

afs_int32 evalclient(void *rock, afs_int32 user)
{
     afs_int32 i;
     struct client *client = (struct client *)rock;
     for ( i = 0 ; i < client->CPS.prlist_len ; i++ ) {
         if ( client->CPS.prlist_val[i] == user )
             return 1;
     }
     return 0;
}

afs_int32
ApplyOsdPolicy(struct rx_call *acall, AFSFid *Fid, afs_uint64 length,
	  afs_uint32 *protocol)
{
    Vnode * targetptr = 0;              /* pointer to input fid */
    Vnode * parentwhentargetnotdir = 0; /* parent of Fid to get ACL */
    Error   errorCode = 0;              /* return code for caller */
    Volume * volptr = 0;                /* pointer to the volume header */
    struct client * client = 0;         /* pointer to client structure */
    struct rx_connection *tcon;
    struct host *thost;
    afs_int32 rights, anyrights;        /* rights for this and any user */
    afs_int64 InitialVnodeFileLength;
    struct AFSStoreStatus InStatus;
    DirHandle dir;
    char fileName[256];

    ViceLog(1,("SRXAFSOSD_ApplyOsdPolicy for %u.%u.%u, length %llu\n",
			Fid->Volume, Fid->Vnode, Fid->Unique, length));
    *protocol = 1; /* default: store in local partition */

    if ((errorCode = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
        goto Bad_ApplyOsdPolicy;

    /*
     * Get associated volume/vnode for the stored file; caller's rights
     * are also returned
     */
    if ((errorCode = GetVolumePackage(acall, Fid, &volptr, &targetptr,
                                     MustNOTBeDIR, &parentwhentargetnotdir,
                                     &client, WRITE_LOCK,
                                     &rights, &anyrights)))
        goto Bad_ApplyOsdPolicy;

    memset(&InStatus, 0, sizeof(InStatus));
    if ((errorCode = Check_PermissionRights(targetptr, client, rights,
                        CHK_STOREDATA, &InStatus)))
            goto Bad_ApplyOsdPolicy;

    VN_GET_LEN(InitialVnodeFileLength, targetptr);

    /* *protocol = 1; */  /* RX_FILESERVER as default makes old clients unhappy*/
    if (!V_osdPolicy(volptr))
	goto Bad_ApplyOsdPolicy;
    if (targetptr->disk.type == vFile
      && InitialVnodeFileLength <= max_move_osd_size) {
	unsigned int policyIndex = parentwhentargetnotdir->disk.osdPolicyIndex;
	int nameNeeded = policy_uses_file_name(policyIndex) ||
			 policy_uses_file_name(V_osdPolicy(volptr));
	/* determine file name in case we need it for policy evaluation */
	if ( nameNeeded ) {
	    SetDirHandle(&dir, parentwhentargetnotdir);
	    if ((errorCode = afs_dir_InverseLookup(&dir, Fid->Vnode,
				targetptr->disk.uniquifier, fileName, 255)))
		fileName[0] = '\0';
	    FidZap(&dir);
	} else
	    fileName[0] = '\0';
	errorCode = createFileWithPolicy(Fid, length, policyIndex, fileName,
				targetptr, volptr, evalclient, client);
	if (!errorCode)
	    *protocol = 2; 	/* RX_OSD */
	else {
	    if (errorCode == ENOENT)
		errorCode = 0;
	    else
	        ViceLog(0,("SRXAFSOSD_ApplyOsdPolicy: createFileWithPolicy failed "
			    "with %d for %u.%u.%u (policy %d)\n",
			    errorCode, V_id(volptr), targetptr->vnodeNumber,
			    targetptr->disk.uniquifier,
			    policyIndex));
	}
    }
Bad_ApplyOsdPolicy:
    ViceLog(1,("SRXAFSOSD_ApplyOsdPolicy for %u.%u.%u returns %d, protocol %u\n",
			Fid->Volume, Fid->Vnode, Fid->Unique,
			errorCode, *protocol));
    PutVolumePackage(acall, parentwhentargetnotdir, targetptr, (Vnode *)0,
		     volptr, &client);
    errorCode = CallPostamble(tcon, errorCode, thost);
    if (errorCode < 0)
        errorCode = EIO;
    return errorCode;
}

afs_int32
SRXAFSOSD_ApplyOsdPolicy(struct rx_call *acall, AFSFid *Fid, afs_uint64 length,
	  afs_uint32 *protocol)
{
    Error errorCode;

    errorCode = ApplyOsdPolicy(acall, Fid, length, protocol);
    return errorCode;
}

afs_int32
SetOsdFileReady(struct rx_call *acall, AFSFid *Fid, struct cksum *checksum)
{
    Error  error2, errorCode = 0;      /* return code for caller */
    Volume * volptr = 0;                /* pointer to the volume header */
    Vnode * targetptr = 0;              /* pointer to input fid */

    ViceLog(1,("SetOsdFileReady start for %u.%u.%u\n",
                        Fid->Volume, Fid->Vnode, Fid->Unique));
    if (!afsconf_SuperUser(*(voldata->aConfDir), acall, (char *)0)) {
        errorCode = EPERM;
        goto Bad_SetOsdFileReady;
    }
    volptr = VGetVolume(&error2, &errorCode, Fid->Volume);
    if (!volptr)
        goto Bad_SetOsdFileReady;
    ViceLog(1,("SetOsdFileReady got volume with code %d\n", errorCode));
    targetptr = VGetVnode(&errorCode, volptr, Fid->Vnode, WRITE_LOCK);
    if (!targetptr)
        goto Bad_SetOsdFileReady;
    ViceLog(1,("SetOsdFileReady got vnode with code %d\n", errorCode));
    errorCode = set_osd_file_ready(acall, targetptr, checksum);
    if (!errorCode)
        targetptr->changed_newTime = 1;
    ViceLog(1,("SetOsdFileReady called set_osd_file_ready with code %d\n", errorCode));

Bad_SetOsdFileReady:
    if (targetptr) {
        VPutVnode(&error2, targetptr);
        if (error2 && !errorCode)
            errorCode = error2;
    }
    if (volptr)
        VPutVolume(volptr);

    BreakCallBack(NULL, Fid, 1);

    ViceLog(1,("SetOsdFileReady returns %d\n", errorCode));

    if (errorCode < 0)
        errorCode = EIO;
    return errorCode;
}

afs_int32
SRXAFSOSD_SetOsdFileReady(struct rx_call *acall, AFSFid *Fid, struct cksum *checksum)
{
    Error errorCode = RXGEN_OPCODE;
    errorCode = SetOsdFileReady(acall, Fid, checksum);
    return errorCode;
}

afs_int32
GetOsdMetadata(struct rx_call *acall, AFSFid *Fid)
{
    Vnode * targetptr = 0;              /* pointer to input fid */
    Vnode * parentwhentargetnotdir = 0; /* parent of Fid to get ACL */
    int     errorCode = 0;              /* return code for caller */
    Volume * volptr = 0;                /* pointer to the volume header */
    struct client * client = 0;         /* pointer to client structure */
    struct rx_connection *tcon;
    struct host *thost;
    afs_int32 rights, anyrights;        /* rights for this and any user */
    struct AFSStoreStatus InStatus;      /* Input status for new dir */
    afs_int32 tlen = 0;
    afs_uint32 length;
    char *rock = 0;
    char *data = 0;

    if ((errorCode = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
        goto Bad_GetOsdMetadata;

    /*
     * Get associated volume/vnode for the stored file; caller's rights
     * are also returned
     */
    if ((errorCode = GetVolumePackage(acall, Fid, &volptr, &targetptr,
                                     MustNOTBeDIR, &parentwhentargetnotdir,
                                     &client, READ_LOCK,
                                     &rights, &anyrights)))
        goto Bad_GetOsdMetadata;

    memset(&InStatus, 0, sizeof(InStatus));
    if ((errorCode = Check_PermissionRights(targetptr, client, rights,
                        CHK_FETCHDATA, &InStatus))) {
	if (VanillaUser(client))
            goto Bad_GetOsdMetadata;
    }

    if (targetptr->disk.osdMetadataIndex) {
        errorCode = GetMetadataByteString(volptr, &targetptr->disk, (void**)&rock,
					(byte**)&data, &length, targetptr->vnodeNumber);
        if (!errorCode)
            tlen = htonl(length);
        if (rx_Write(acall, (char *)&tlen, sizeof(tlen)) != sizeof(tlen))
            goto Bad_GetOsdMetadata;
        if (!errorCode)
            if (rx_Write(acall, data, length) != length)
	        goto Bad_GetOsdMetadata;
	if (rock)
	    free(rock);
    } else
	errorCode = EINVAL;

Bad_GetOsdMetadata:
    if (errorCode)
        rx_Write(acall, (char *)&tlen, 4);
    PutVolumePackage(acall, parentwhentargetnotdir, targetptr, (Vnode *)0, volptr,
					&client);
    errorCode = CallPostamble(tcon, errorCode, thost);
    return errorCode;
}

afs_int32
SRXAFSOSD_GetOsdMetadata(struct rx_call *acall, AFSFid *Fid)
{
    Error errorCode;

    ViceLog(1,("SRXAFSOSD_GetOsdMetadata start for %u.%u.%u\n",
                        Fid->Volume, Fid->Vnode, Fid->Unique));
    errorCode = GetOsdMetadata(acall, Fid);
    ViceLog(1,("SRXAFSOSD_GetOsdMetadata for %u.%u.%u returns %d\n",
                        Fid->Volume, Fid->Vnode, Fid->Unique, errorCode));
    return errorCode;
}

afs_int32
SRXAFSOSD_UpdateOSDmetadata(struct rx_call *acall, struct ometa *old, struct ometa *new)
{
    Error errorCode = 0, error2;
    AFSFid Fid = {0, 0, 0};
    Vnode *targetptr = 0;
    Volume *volptr = 0;

    if (old->vsn == 1) {
	Fid.Volume = old->ometa_u.t.part_id & 0xffffffff;
	Fid.Vnode = old->ometa_u.t.obj_id & 0x2ffffff;
	Fid.Unique = (old->ometa_u.t.obj_id >> 32) & 0xffffff;
    } else if (old->vsn == 2) {
	Fid.Volume = old->ometa_u.f.rwvol;
	Fid.Vnode = old->ometa_u.f.vN;
	Fid.Unique = old->ometa_u.f.unique;
    } else
	return EINVAL;

    ViceLog(1,("SRXAFSOSD_UpdateOSDmetadata start for %u.%u.%u\n",
                        Fid.Volume, Fid.Vnode, Fid.Unique));

    if (!afsconf_SuperUser(*(voldata->aConfDir), acall, (char *)0))
        return EACCES;

    volptr = VGetVolume(&error2, &errorCode, Fid.Volume);
    if (!volptr)
	goto bad;

    targetptr = VGetVnode(&errorCode, volptr, Fid.Vnode, WRITE_LOCK);
    if (!targetptr)
	goto bad;

    errorCode = update_osd_metadata(volptr, targetptr, old, new);

bad:
    if (targetptr) {
        VPutVnode(&error2, targetptr);
        if (error2 && !errorCode)
            errorCode = error2;
    }
    if (volptr)
        VPutVolume(volptr);
    ViceLog(1,("SRXAFSOSD_UpdateOSDmetadata for %u.%u.%u returns %d\n",
                        Fid.Volume, Fid.Vnode, Fid.Unique, errorCode));

    return errorCode;
}

afs_int32
FetchData_OSD(Volume * volptr, Vnode **targetptr,
		struct rx_call * Call, afs_sfsize_t Pos,
		afs_sfsize_t Len, afs_int32 Int64Mode,
		int client_vice_id, afs_int32 MyThreadEntry)
{
    afs_int64 targLen;
    afs_uint32 hi, lo;
    afs_int32 errorCode;

    if (!((*targetptr)->disk.osdFileOnline)) {
	errorCode = ENODEV;
	if (errorCode) {
            char str[22];
            ViceLog(0, ("FetchData_OSD denying tape fetch for %u.%u.%u requested from %s\n",
                    V_id(volptr), (*targetptr)->vnodeNumber,
		    (*targetptr)->disk.uniquifier,
	    ipstring(str, Call, sizeof(str))));
	    return errorCode;
	}
    }
    VN_GET_LEN(targLen, *targetptr);
    if (Pos + Len > targLen)
	Len = targLen - Pos;
    if (Len < 0)
	Len = 0;
    SplitInt64(Len, hi, lo);
    hi = htonl(hi);
    lo = htonl(lo);
    if (Int64Mode) {
	rx_Write(Call, (char *)&hi, sizeof(hi));
    }
    rx_Write(Call, (char *)&lo, sizeof(lo));
    errorCode = xchange_data_with_osd(Call, targetptr, Pos, Len, targLen, 0,
				    client_vice_id);
    ViceLog(3,("FetchData: xchange_data_with_osd returned %d\n", errorCode));
    if (errorCode)
	return errorCode;
    return 0;
}

afs_int32
legacyFetchData(Volume *volptr, Vnode **targetptr,
			  struct rx_call * Call, afs_sfsize_t Pos,
			  afs_sfsize_t Len, afs_int32 Int64Mode,
			  int client_vice_id, afs_int32 MyThreadEntry,
			  struct in_addr *logHostAddr)
{
    afs_int32 errorCode = EINVAL;

    if ((*targetptr)->disk.osdMetadataIndex && (*targetptr)->disk.type == vFile) {
	ViceLog(1, ("Traditional FetchData on OsdFile %u.%u.%u, "
                        "Pos %llu Len %llu client %s\n",
                            V_id(volptr), (*targetptr)->vnodeNumber,
			    (*targetptr)->disk.uniquifier, Pos, Len,
                            inet_ntoa(*logHostAddr)));
	errorCode = FetchData_OSD(volptr, targetptr, Call, Pos, Len,
				  Int64Mode, client_vice_id, MyThreadEntry);
    }
    return errorCode;
}

afs_int32
SRXAFSOSD_GetPath(struct rx_call *acall, AFSFid *Fid, struct async *a)
{
    afs_int32 errorCode = RXGEN_OPCODE;
    return errorCode;
}

afs_int32
SRXAFSOSD_BringOnline(struct rx_call *acall, AFSFid *Fid,
                      AFSFetchStatus *OutStatus, AFSCallBack *CallBack)
{
    Error errorCode;
    Vnode *targetptr = NULL;                    /* pointer to input fid */
    Vnode *parentwhentargetnotdir = NULL;       /* parent of Fid to get ACL */
    Vnode tparent;
    Volume *volptr = NULL;                      /* pointer to the volume header */
    struct client *client = NULL;               /* pointer to client structure */
    afs_int32 rights, anyrights;                /* rights for this and any user */
    struct rx_connection *tcon;
    struct host *thost;

    if ((errorCode = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
        goto Bad_BringOnline;

    if ((errorCode =
         GetVolumePackage(acall, Fid, &volptr, &targetptr, MustNOTBeDIR,
                          &parentwhentargetnotdir, &client, WRITE_LOCK,
                          &rights, &anyrights)))
        goto Bad_BringOnline;

    if ((errorCode = Check_PermissionRights(targetptr, client, rights,
                        CHK_FETCHDATA, NULL))) {
        if (VanillaUser(client))
            goto Bad_BringOnline;
    }

    /* could be slow, so better give parent directory back */
    if (parentwhentargetnotdir) {
	memcpy((char *)&tparent, (char *)parentwhentargetnotdir, sizeof(tparent));
	VPutVnode(&errorCode, parentwhentargetnotdir);
	parentwhentargetnotdir = NULL;
    }
    if (!targetptr->disk.osdFileOnline) {
        afs_uint32 fileno;
        if (!(VolumeWriteable(volptr))) {    /* No way to bring the file on-line */
            errorCode = EIO;
            goto Bad_BringOnline;
        }
        errorCode = fill_osd_file(targetptr, NULL, 0, &fileno, client->ViceId);
    }

Bad_BringOnline:
    if (!errorCode || errorCode == OSD_WAIT_FOR_TAPE) {
        afs_int32 cb;

        GetStatus(targetptr, OutStatus, rights, anyrights, &tparent);
        /* if a r/w volume, promise a callback to the caller */
        if (VolumeWriteable(volptr)) {
            cb = AddCallBack1(client->host, Fid, 0, 1, 0);
            SetCallBackStruct(cb, CallBack);
        } else {
            struct AFSFid myFid;
            bzero(&myFid, sizeof(struct AFSFid));
            myFid.Volume = Fid->Volume;
            cb = AddCallBack1(client->host, &myFid, 0, 3, 0);
            SetCallBackStruct(cb, CallBack);
	}
    }
    (void)PutVolumePackage(acall, parentwhentargetnotdir, targetptr, (Vnode *) 0,
                           volptr, &client);
    errorCode = CallPostamble(tcon, errorCode, thost);
    return errorCode;
}

afs_int32
SRXAFSOSD_StartAsyncFetch(struct rx_call *acall, AFSFid *Fid, struct RWparm *p,
			  struct async *a, afs_uint64 *transid, afs_uint32 *expires,
			  AFSFetchStatus *OutStatus, AFSCallBack *CallBack)
{
    afs_int32 errorCode = RXGEN_OPCODE;
    afs_uint64 offset, length;
    afs_int32 flag = 0;
    ViceLog(1,("StartAsyncFetch for %u.%u.%u type %d\n",
                        Fid->Volume, Fid->Vnode, Fid->Unique, a->type));
    if (p->type == 1) {
        offset = p->RWparm_u.p1.offset;
        length = p->RWparm_u.p1.length;
    } else if (p->type == 4) {
        offset = p->RWparm_u.p4.offset;
        length = p->RWparm_u.p4.length;
    } else if (p->type == 5) {
        offset = p->RWparm_u.p5.offset;
        length = p->RWparm_u.p5.length;
        flag = p->RWparm_u.p5.flag;
    } else {
        errorCode = RXGEN_SS_UNMARSHAL;
        goto bad;
    }
    if (a->type == 1) {
        a->async_u.p1.osd_fileList_len = 0;
        a->async_u.p1.osd_fileList_val = NULL;
    }
    errorCode = createAsyncTransaction(acall, Fid, CALLED_FROM_START_ASYNC,
                                       offset, length, transid, expires);
    if (errorCode) {
        return errorCode;
    }

    errorCode = RXGEN_SS_UNMARSHAL;
    if (a->type == 1) {
        errorCode = GetOSDlocation(acall, Fid, offset, length, 0,
                                flag | CALLED_FROM_START_ASYNC,
                                OutStatus, CallBack, a);
    } else
	errorCode = RXGEN_SS_UNMARSHAL;
    if (errorCode) {
        EndAsyncTransaction(acall, Fid, *transid);
    }

bad:
    if (errorCode)
        ViceLog(0,("StartAsyncFetch for %u.%u.%u type %d returns %d\n",
                        Fid->Volume, Fid->Vnode, Fid->Unique, a->type,
                        errorCode));
    else
        ViceLog(3,("StartAsyncFetch for %u.%u.%u type %d returns %d\n",
                        Fid->Volume, Fid->Vnode, Fid->Unique, a->type,
                        errorCode));
    return errorCode;
}

afs_int32
SRXAFSOSD_ExtendAsyncFetch(struct rx_call *acall, AFSFid *Fid, afs_uint64 transid,
                      afs_uint32 *expires)
{
    Error errorCode;
    Vnode *targetptr = 0;       /* pointer to input fid */
    Vnode *parentwhentargetnotdir = 0;  /* parent of Fid to get ACL */
    Volume *volptr = 0;         /* pointer to the volume header */
    struct client *client = 0;  /* pointer to client structure */
    afs_int32 rights, anyrights;        /* rights for this and any user */
    struct rx_connection *tcon;
    struct host *thost;

    ViceLog(1,("SRXAFSOSD_ExtendAsyncFetch for %u.%u.%u\n",
                        Fid->Volume, Fid->Vnode, Fid->Unique));

    /*
     * With fastread ExtendAsyncFetch is also used to verify the requestor's
     * right to read this file. Therefore we do here the whole volume and vnode
     * stuff.
     */
    if ((errorCode = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
        goto Bad_ExtendAsyncFetch;
    if ((errorCode =
         GetVolumePackage(acall, Fid, &volptr, &targetptr, DONTCHECK,
                          &parentwhentargetnotdir, &client, READ_LOCK,
                          &rights, &anyrights)))
        goto Bad_ExtendAsyncFetch;
    if ((errorCode =
         Check_PermissionRights(targetptr, client, rights, CHK_FETCHDATA, 0)))
        goto Bad_ExtendAsyncFetch;

    errorCode  = extendAsyncTransaction(acall, Fid, transid, expires);

Bad_ExtendAsyncFetch:
    (void)PutVolumePackage(acall, parentwhentargetnotdir, targetptr,
                           (Vnode *) 0, volptr, &client);
    errorCode = CallPostamble(tcon, errorCode, thost);
    return errorCode;
}

afs_int32
SRXAFSOSD_EndAsyncFetch(struct rx_call *acall, AFSFid *Fid, afs_uint64 transid,
                        afs_uint64 bytes_sent, afs_uint32 osd)
{
    afs_int32 errorCode = RXGEN_OPCODE;
    ViceLog(1,("EndAsyncFetch for %u.%u.%u\n",
                        Fid->Volume, Fid->Vnode, Fid->Unique));
    errorCode = EndAsyncTransaction(acall, Fid, transid);
    if (osd) {
        rxosd_updatecounters(osd, 0, bytes_sent);
    }
    return errorCode;
}

afs_int32
SRXAFSOSD_StartAsyncStore(struct rx_call *acall, AFSFid *Fid, struct RWparm *p,
			  struct async *a, afs_uint64 *maxlength,
			  afs_uint64 *transid, afs_uint32 *expires,
			  AFSFetchStatus *OutStatus)
{
    afs_int32 errorCode = RXGEN_OPCODE;
    afs_uint64 offset, length, filelength;
    afs_int32 flag = 0;

    if (p->type == 4) {
        offset = p->RWparm_u.p4.offset;
        length = p->RWparm_u.p4.length;
        filelength = p->RWparm_u.p4.filelength;
    } else if (p->type == 6) {
        offset = p->RWparm_u.p6.offset;
        length = p->RWparm_u.p6.length;
        filelength = p->RWparm_u.p6.filelength;
        flag = p->RWparm_u.p6.flag;
    } else {
        errorCode = RXGEN_SS_UNMARSHAL;
        goto bad;
    }
    ViceLog(1,("StartAsyncStore for %u.%u.%u type %d filelength %llu\n",
                        Fid->Volume, Fid->Vnode, Fid->Unique, a->type, filelength));
    if (a->type == 1) {
        a->async_u.p1.osd_fileList_len = 0;
        a->async_u.p1.osd_fileList_val = NULL;
    }
    errorCode = createAsyncTransaction(acall, Fid,
                                       OSD_WRITING | CALLED_FROM_START_ASYNC,
                                       offset, length, transid, expires);
    if (errorCode)
        return errorCode;

    errorCode = RXGEN_SS_UNMARSHAL;
    if (a->type == 1) {
        errorCode = GetOSDlocation(acall, Fid, offset, length,
                            filelength, flag | CALLED_FROM_START_ASYNC | OSD_WRITING,
                            OutStatus, NULL, a);
    } else {
        errorCode = RXGEN_SS_UNMARSHAL;
    }
    if (errorCode)
        EndAsyncTransaction(acall, Fid, *transid);

bad:
    if (errorCode)
        ViceLog(0,("StartAsyncStore for %u.%u.%u type %d returns %d\n",
                        Fid->Volume, Fid->Vnode, Fid->Unique, a->type,
                        errorCode));
    else
        ViceLog(3,("StartAsyncStore for %u.%u.%u type %d returns %d\n",
                        Fid->Volume, Fid->Vnode, Fid->Unique, a->type,
                        errorCode));
    return errorCode;
}

afs_int32
SRXAFSOSD_ExtendAsyncStore(struct rx_call *acall, AFSFid *Fid, afs_uint64 transid,
                           afs_uint32 *expires)
{
    Error errorCode;
    ViceLog(1,("SRXAFSOSD_ExtendAsyncStore for %u.%u.%u\n",
                        Fid->Volume, Fid->Vnode, Fid->Unique));

    errorCode = extendAsyncTransaction(acall, Fid, transid, expires);
    return errorCode;
}

afs_int32
SRXAFSOSD_EndAsyncStore(struct rx_call *acall, AFSFid *Fid, afs_uint64 transid,
                        afs_uint64 filelength,
                        afs_uint64 bytes_rcvd, afs_uint64 bytes_sent,
                        afs_uint32 osd,
                        afs_int32 error, struct asyncError *ae,
                        struct AFSStoreStatus *InStatus,
                        struct AFSFetchStatus *OutStatus)
{
    Error errorCode;
    Vnode *targetptr = 0;       /* pointer to input fid */
    Vnode *parentwhentargetnotdir = 0;  /* parent of Fid to get ACL */
    Vnode tparent;
    Volume *volptr = 0;         /* pointer to the volume header */
    struct client *client = 0;  /* pointer to client structure */
    afs_int32 rights, anyrights;        /* rights for this and any user */
    struct rx_connection *tcon;
    struct host *thost;
    afs_uint64 oldlength, offset, length;
    char str[22];

    if ((errorCode = CallPreamble(acall, ACTIVECALL, Fid, &tcon, &thost)))
        goto Bad_EndAsyncStore;

    /*
     * Get volptr back from activeFile to finish old transaction before
     * volserver may get the volume for cloning or moving it.
     */
    volptr = getAsyncVolptr(acall, Fid, transid, &offset, &length);
    /*
     * Get associated volume/vnode for the stored file; caller's rights
     * are also returned
     */
    if ((errorCode =
        GetVolumePackage(acall, Fid, &volptr, &targetptr, MustNOTBeDIR,
                          &parentwhentargetnotdir, &client, WRITE_LOCK,
                          &rights, &anyrights))) {
        goto Bad_EndAsyncStore;
    }
    /* Check if we're allowed to store the data */
    if ((errorCode =
         Check_PermissionRights(targetptr, client, rights, CHK_STOREDATA,
                                InStatus))) {
        goto Bad_EndAsyncStore;
    }

    /* recover_store can be slow, so better give parent directory back */
    if (parentwhentargetnotdir) {
	memcpy((char *)&tparent, (char *)parentwhentargetnotdir, sizeof(tparent));
	VPutVnode(&errorCode, parentwhentargetnotdir);
	parentwhentargetnotdir = NULL;
    }
    if (ae) {
        if (!ae->error) {
            if (ae->asyncError_u.no_new_version)
                goto NothingHappened;
        } else if (ae->error == 1) {
            ViceLog(0,("EndAsyncStore recoverable asyncError for %u.%u.%u from %s\n",
                    Fid->Volume, Fid->Vnode, Fid->Unique,
		    ipstring(str, acall, sizeof(str))));
            errorCode = recover_store(targetptr, ae);
        } else {
            ViceLog(0,("EndAsyncStore unknown asyncError type %d for %u.%u.%u from %s\n",
                    ae->error,
                    Fid->Volume, Fid->Vnode, Fid->Unique,
		    ipstring(str, acall, sizeof(str))));
        }
    }

    VN_GET_LEN(oldlength, targetptr);
    if (filelength < oldlength) {
        ino_t ino;
        ino = VN_GET_INO(targetptr);
        if (ino != 0) {
            FdHandle_t *fdP;
            fdP = IH_OPEN(targetptr->handle);
            FDH_TRUNC(fdP, filelength);
            FDH_CLOSE(fdP);
        }
        if (targetptr->disk.osdMetadataIndex && targetptr->disk.type == vFile) {
            errorCode = truncate_osd_file(targetptr, filelength);
            if (errorCode) {
                ViceLog(0, ("EndAsyncStore: truncate_osd_file %u.%u.%u failed with %d\n",
                        Fid->Volume, Fid->Vnode, Fid->Unique, errorCode));
                        errorCode = 0;
            }
        }
    } else {
        filelength = oldlength;
        if (offset + length > oldlength)
            filelength = offset + length;
    }

    if (filelength != oldlength) {
        afs_int32 blocks = (afs_int32)((filelength - oldlength) >> 10);
        V_diskused(volptr) += blocks;
    }
    VN_SET_LEN(targetptr, filelength);
    /* Update the status of the target's vnode */
    Update_TargetVnodeStatus(targetptr, TVS_SDATA, client, InStatus,
                             targetptr, volptr, 0, 0);
    /* Get the updated File's status back to the caller */
    GetStatus(targetptr, OutStatus, rights, anyrights, &tparent);

    rx_KeepAliveOn(acall);

    BreakCallBack(client->host, Fid, 0);
  NothingHappened:
    errorCode = EndAsyncTransaction(acall, Fid, transid);
  Bad_EndAsyncStore:
    if (osd) {
        rxosd_updatecounters(osd, bytes_rcvd, bytes_sent);
    }
    /* Update and store volume/vnode and parent vnodes back */
    (void)PutVolumePackage(acall, parentwhentargetnotdir, targetptr,
                                         (Vnode *) 0, volptr, &client);
    ViceLog(2, ("EndAsyncStore returns %d for %u.%u.%u\n",
                        errorCode, Fid->Volume, Fid->Vnode, Fid->Unique));

    errorCode = CallPostamble(tcon, errorCode, thost);
    return errorCode;
}

static int
GetLinkCountAndSize(Volume * vp, FdHandle_t * fdP, int *lc,
		    afs_sfsize_t * size)
{
    struct afs_stat_st status;
#ifdef AFS_NAMEI_ENV
    FdHandle_t *lhp;
    lhp = IH_OPEN(V_linkHandle(vp));
    if (!lhp)
	return EIO;
    *lc = namei_GetLinkCount(lhp, fdP->fd_ih->ih_ino, 0, 0, 0);
    FDH_CLOSE(lhp);
    if (*lc < 0)
	return -1;
    if (afs_fstat(fdP->fd_fd, &status) < 0) {
	return -1;
    }
    *size = status.st_size;
    if (status.st_nlink > 1) 		/* Possible after split volume */
	(*lc)++;
    return (*size == -1) ? -1 : 0;
#else

    if (afs_fstat(fdP->fd_fd, &status) < 0) {
	return -1;
    }

    *lc = GetLinkCount(vp, &status);
    *size = status.st_size;
    return 0;
#endif
}

afs_int32
legacyStoreData(Volume * volptr, Vnode **targetptr, struct AFSFid * Fid,
	  struct client * client, struct rx_call * Call,
	  afs_fsize_t Pos, afs_fsize_t Length, afs_fsize_t FileLength)
{
    afs_int32 errorCode;
    afs_uint64 vnodeLength;
    afs_sfsize_t TruncatedLength;	/* local only here */
    struct in_addr logHostAddr;

    logHostAddr.s_addr = rx_HostOf(rx_PeerOf(rx_ConnectionOf(Call)));
    VN_GET_LEN(vnodeLength, *targetptr);
    TruncatedLength = vnodeLength;
    if (FileLength < TruncatedLength)
	TruncatedLength = FileLength;
    if (Length && Pos + Length > TruncatedLength)
	TruncatedLength = Pos + Length;
    ViceLog(1, ("StoreData to osd file %u.%u.%u, pos %llu, length %llu, file length %llu client %s\n",
		    Fid->Volume, Fid->Vnode, Fid->Unique,
		    Pos, Length, FileLength,
		    inet_ntoa(logHostAddr)));
    if (TruncatedLength != vnodeLength) {
	afs_int32 blocks = (afs_int32)((TruncatedLength - vnodeLength) >> 10);
	if (V_maxquota(volptr)
	  && (blocks > (V_maxquota(volptr) - V_diskused(volptr)))) {
	    if ((blocks + V_diskused(volptr) - V_maxquota(volptr)) * 100 / V_maxquota(volptr) > 5 ) /* allow 5 % over quota */
		return ENOSPC;
	}
	V_diskused(volptr) += blocks;
    }
    rx_SetLocalStatus(Call, 1);
    errorCode = xchange_data_with_osd(Call, targetptr, Pos, Length,
				    FileLength, 1, client->ViceId);
    if (errorCode)
	return errorCode;
    VN_GET_LEN(vnodeLength, *targetptr);
    if (TruncatedLength < vnodeLength) {
	errorCode = truncate_osd_file(*targetptr, TruncatedLength);
	if (errorCode)
	    return errorCode;
    }
    if (TruncatedLength != vnodeLength) {
	VN_SET_LEN(*targetptr, TruncatedLength);
	vnodeLength = TruncatedLength;
	(*targetptr)->changed_newTime = 1;
    }
    ViceLog(1, ("StoreData to osd file %u.%u.%u file length %llu returns 0\n",
		    Fid->Volume, Fid->Vnode, Fid->Unique, vnodeLength));
    return 0;
}

/* with RxOSD, prepare an OSD file if applicable */
afs_int32
MaybeStore_OSD(Volume * volptr, Vnode * targetptr, struct AFSFid * Fid,
		  struct client * client, struct rx_call * Call,
		  afs_fsize_t Pos, afs_fsize_t Length, afs_fsize_t FileLength,
		  Vnode *parentwhentargetnotdir, char *fileName)
{
    afs_uint64 InitialVnodeFileLength;
    afs_uint64 RealisticFileLength;
    unsigned int policyIndex = 0;
    VN_GET_LEN(InitialVnodeFileLength, targetptr);
    if (InitialVnodeFileLength <= max_move_osd_size) {
	afs_uint32 tcode;
	RealisticFileLength = Pos + Length;
	if (FileLength > RealisticFileLength)
	    RealisticFileLength = FileLength;
	policyIndex = parentwhentargetnotdir->disk.osdPolicyIndex;
	tcode = createFileWithPolicy(Fid, RealisticFileLength, policyIndex,
				    fileName, targetptr, volptr,
				    evalclient, client);
	if (tcode && tcode != ENOENT)
		ViceLog(0,("MaybeStore_OSD: createFileWithPolicy failed "
			    "with %d for %u.%u.%u (policy %d)\n",
			    tcode, V_id(volptr), targetptr->vnodeNumber,
			    targetptr->disk.uniquifier,
			    policyIndex));
    }
    return 0;
}

void
remove_if_osd_file(Vnode **targetptr)
{
    if ((*targetptr)->disk.type == vFile
                        && (*targetptr)->disk.osdMetadataIndex)
	osdRemove((*targetptr)->volumePtr, &((*targetptr)->disk),
		  (*targetptr)->vnodeNumber);
}

void
fill_status(Vnode *targetptr, afs_fsize_t targetLen, AFSFetchStatus *status)
{
    if (!(targetptr->volumePtr->osdMetadataHandle)) { /* traditional AFS volume */
        status->FetchStatusProtocol = 1;
    } else {
	if (isOsdFile(V_osdPolicy(targetptr->volumePtr), V_id(targetptr->volumePtr),
                      &targetptr->disk, targetptr->vnodeNumber)) {
            status->FetchStatusProtocol = RX_OSD;
            if (!targetptr->disk.osdFileOnline)
                status->FetchStatusProtocol |= RX_OSD_NOT_ONLINE;
        } else {
	    status->FetchStatusProtocol = 1;
	    if (V_osdPolicy(targetptr->volumePtr)
	      && targetptr->disk.type == vFile
	      && targetLen <= max_move_osd_size)
		status->FetchStatusProtocol |= POSSIBLY_OSD;
	}
    }
    if (ClientsWithAccessToFileserverPartitions && VN_GET_INO(targetptr)) {
        namei_t name;
        struct afs_stat_st tstat;

        namei_HandleToName(&name, targetptr->handle);
        if (afs_stat(name.n_path, &tstat) == 0) {
            SplitOffsetOrSize(tstat.st_size,
                    status->Length_hi,
                    status->Length);
            if (tstat.st_size != targetLen) {
                ViceLog(3,("GetStatus: new file length %lu instead of %llu for (%u.%u.%u)\n",
                            tstat.st_size,
                            targetLen,
                            targetptr->volumePtr->hashid,
                            targetptr->vnodeNumber,
                            targetptr->disk.uniquifier));
#ifdef AFS_DEMAND_ATTACH_FS
                if (Vn_state(targetptr) == VN_STATE_EXCLUSIVE) {
#else
                if (WriteLocked(&targetptr->lock)) {
#endif
                    afs_int64 adjustSize;
                    adjustSize = nBlocks(tstat.st_size)
                                            - nBlocks(targetLen);
                    V_diskused(targetptr->volumePtr) += adjustSize;
                    VN_SET_LEN(targetptr, tstat.st_size);
                    targetptr->changed_newTime = 1;
                }
            }
        }
    }
}

extern char **osdExportedVariablesPtr;
extern int RXAFSOSD_ExecuteRequest(struct rx_call *z_call);

extern struct osd_viced_ops_v0 *osdviced;
struct osd_viced_ops_v0 osd_viced_ops_v0;
extern int rx_enable_stats;
int rxcon_client_key = 0;

extern void libafsd_init(void * libafsdrock);

afs_int32
init_viced_afsosd(char *afsversion, char** afsosdVersion, void *inrock, void *outrock,
		  void *libafsosdrock, afs_int32 version)
{
    afs_int32 code;
    struct init_viced_inputs *input = (struct init_viced_inputs *)inrock;
    struct init_viced_outputs *output = (struct init_viced_outputs *)outrock;

    voldata = input->voldata;
    rx_enable_stats = *(voldata->aRx_enable_stats);
    viceddata = input->viceddata;
    rxcon_client_key = *(viceddata->aRxcon_client_key);
    memset(&osd_viced_ops_v0, 0, sizeof(osd_viced_ops_v0));
    osd_viced_ops_v0.op_createAsyncTransaction = createAsyncTransaction;
    osd_viced_ops_v0.op_endAsyncTransaction = EndAsyncTransaction;
    osd_viced_ops_v0.op_asyncActive = asyncActive;
    osd_viced_ops_v0.op_legacyFetchData = legacyFetchData;
    osd_viced_ops_v0.op_legacyStoreData = legacyStoreData;
    osd_viced_ops_v0.op_remove_if_osd_file = remove_if_osd_file;
    osd_viced_ops_v0.op_fill_status = fill_status;
    osd_viced_ops_v0.op_FsCmd = FsCmd;
    osd_viced_ops_v0.op_ApplyOsdPolicy = ApplyOsdPolicy;
    osd_viced_ops_v0.op_RXAFSOSD_ExecuteRequest = RXAFSOSD_ExecuteRequest;

    *(output->osdviced) = &osd_viced_ops_v0;

    code = init_osdvol(afsversion, afsosdVersion, output->osdvol);
    if (!code)
        code = libafsosd_init(libafsosdrock, version);

    return code;
}
