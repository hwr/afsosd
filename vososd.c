/*
 * Copyright (c) 2012, Hartmut Reuter,
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

#include <roken.h>

#ifdef IGNORE_SOME_GCC_WARNINGS
# pragma GCC diagnostic warning "-Wimplicit-function-declaration"
#endif

#ifdef AFS_NT40_ENV
#include <WINNT/afsreg.h>
#endif

#ifdef AFS_AIX_ENV
#include <sys/statfs.h>
#endif
#include <ctype.h>

#include <lock.h>
#include <afs/stds.h>
#include <rx/xdr.h>
#include <rx/rx.h>
#include <rx/rx_queue.h>
#include <rx/rx_globals.h>
#include <afs/nfs.h>
#include <afs/vlserver.h>
#include <afs/cellconfig.h>
#include <afs/keys.h>
#include <afs/afsutil.h>
#include <ubik.h>
#include <afs/afsint.h>
#include <afs/cmd.h>
#include <afs/usd.h>
#include "volser.h"
#include "volint.h"
#include <afs/ihandle.h>
#include <afs/vnode.h>
#include <afs/volume.h>
#include <afs/com_err.h>

#include "../volser/volser_internal.h"
#include "../volser/volser_prototypes.h"
#include "../volser/vsutils_prototypes.h"
#include "../volser/lockdata.h"
#include "../volser/lockprocs_prototypes.h"
#include "afsosd.h"
#include "volserosd.h"
#include "osddb.h"
#include "fs_rxosd_common.h"
#include "osddbuser.h"

#ifdef HAVE_POSIX_REGEX
#include <regex.h>
#endif

#undef rx_SetRxDeadTime
#define rx_SetRxDeadTime(seconds)   (*vos_data->rx_connDeadTime = (seconds))

#define COMMONPARMS     cmd_Seek(ts, 12);\
cmd_AddParm(ts, "-cell", CMD_SINGLE, CMD_OPTIONAL, "cell name");\
cmd_AddParm(ts, "-noauth", CMD_FLAG, CMD_OPTIONAL, "don't authenticate");\
cmd_AddParm(ts, "-localauth",CMD_FLAG,CMD_OPTIONAL,"use server tickets");\
cmd_AddParm(ts, "-verbose", CMD_FLAG, CMD_OPTIONAL, "verbose");\
cmd_AddParm(ts, "-encrypt", CMD_FLAG, CMD_OPTIONAL, "encrypt commands");\
cmd_AddParm(ts, "-noresolve", CMD_FLAG, CMD_OPTIONAL, "don't resolve addresses"); \
cmd_AddParm(ts, "-config", CMD_SINGLE, CMD_OPTIONAL, "config location"); \

#define ERROR_EXIT(code) do { \
    error = (code); \
    goto error_exit; \
} while (0)


extern afs_uint32 GetServer(char *aname);
int localauth = 0;


struct vos_data {
    struct ubik_client **cstruct;
    afs_int32 *aLogLevel;
    int *verbose;
    int *noresolve;
    int *rx_connDeadTime;
};
struct vos_data *vos_data;

#define MAXOSDS 1024

int
UV_Traverse(afs_uint32 *server, afs_int32 vid, afs_uint32 nservers,
                afs_int32 flag, afs_uint32 delay,
                struct sizerangeList *tsrl, struct osd_infoList *tlist)
{
    struct rx_connection *tconn;
    afs_int32 code;
    struct sizerangeList localsrl, *srl;
    struct osd_infoList locallist, *list;
    int i, j, n;
    int policy_statistic = (tsrl == NULL);
    srl = &localsrl;
    list = &locallist;

    srl->sizerangeList_len = 0;
    srl->sizerangeList_val = 0;
    list->osd_infoList_len = 0;
    list->osd_infoList_val = 0;
    for (n=0; n<nservers; n++) {
        if (srl && srl->sizerangeList_val) {
            free(srl->sizerangeList_val);
            srl->sizerangeList_len = 0;
            srl->sizerangeList_val = 0;
        }
        if (list->osd_infoList_val) {
            free(list->osd_infoList_val);
            list->osd_infoList_val = 0;
            list->osd_infoList_len = 0;
        }
        if (*vos_data->verbose)
            fprintf(stderr, "traversing now %u.%u.%u.%u\n",
                        (ntohl(server[n]) >> 24) & 0xff,
                        (ntohl(server[n]) >> 16) & 0xff,
                        (ntohl(server[n]) >> 8) & 0xff,
                        ntohl(server[n]) & 0xff);
        tconn = UV_BindOsd(server[n], AFSCONF_VOLUMEPORT);
        if (tconn) {
            if ( policy_statistic )
                code = AFSVOLOSD_PolicyUsage(tconn, vid, srl, list);
            else {
                code = AFSVOLOSD_Traverse(tconn, vid, delay, flag, srl, list);
            }
            rx_DestroyConnection(tconn);
        }
        if (!code) {
            if ( !policy_statistic )
                for (i=0; i<srl->sizerangeList_len; i++) {
                    struct sizerange *s = &srl->sizerangeList_val[i];
                    struct sizerange *ts = &tsrl->sizerangeList_val[i];
                    if (ts->maxsize == s->maxsize) {
                        ts->bytes += s->bytes;
                        ts->fids += s->fids;
                    } else
                        fprintf(stderr,
                            "found size %llu instead of %llu from server %u\n",
                                    s->maxsize, ts->maxsize, n);
                }
            for (i=0; i<list->osd_infoList_len; i++) {
                struct osd_info *o = &list->osd_infoList_val[i];
                struct osd_info *to;
                for (j=0; j<tlist->osd_infoList_len; j++) {
                    to = &tlist->osd_infoList_val[j];
                    if (to->osdid == o->osdid)
                        break;
                    if (to->osdid == 0) {
                        to->osdid = o->osdid;
                        break;
                    }
                }
                if (j<tlist->osd_infoList_len) {
                    to->fids += o->fids;
                    to->fids1 += o->fids1;
                    to->bytes += o->bytes;
                    to->bytes1 += o->bytes1;
                }
            }
        }
        if (code)
            fprintf(stderr,"AFSVOLOSD_Traverse failed to server %u with %d\n",
                        n, code);
    }
    return code;
}

afs_int32
UV_GetArchCandidates(afs_uint32 server, hsmcandList *list, afs_uint64 minsize,
        afs_uint64 maxsize, afs_uint32 copies, afs_uint32 maxcandidates,
        afs_int32 osd, afs_int32 flag, afs_uint32 delay)
{
    struct rx_connection *tconn;
    afs_int32 code;

    tconn = UV_BindOsd(server, AFSCONF_VOLUMEPORT);
    if (tconn) {
        code = AFSVOLOSD_GetArchCandidates(tconn, minsize, maxsize, copies,
                                        maxcandidates, osd, flag, delay, list);
        rx_DestroyConnection(tconn);
    }
    return code;
}

static int
Archcand(struct cmd_syndesc *as, void *arock)
{
    afs_int32 code;
    struct rx_connection *tcon;
    struct hsmcandList list;
    int i, j;
    afs_uint64 minsize = 0;
    afs_uint64 maxsize = 0x7fffffffffffffff;
    afs_uint32 copies = 1;
    afs_uint32 maxcandidates = 256;
    afs_uint32 server;
    afs_int32 osd = 0, flag = 0;
    afs_uint32 delay = 3600;
    char str[64];

    memset(&list, 0, sizeof(list));

    server = GetServer(as->parms[0].items->data);
    if (as->parms[1].items) {           /* -minsize */
        i = sscanf(as->parms[1].items->data, "%llu%s", &minsize, str);
        if (i == 2) {
            if (str[0] == 'k' || str[0] == 'K')
                minsize = minsize << 10;
            else
            if (str[0] == 'm' || str[0] == 'M')
                minsize = minsize << 20;
            else
            if (str[0] == 'g' || str[0] == 'G')
                minsize = minsize << 30;
            else
                i = 3;
        }
        if (i != 1 && i != 2) {
            fprintf(stderr,"Invalid value for minsize %s.\n",
                        as->parms[1].items->data);
            return 1;
        }
    }
    if (as->parms[2].items) {           /* -maxsize */
        i = sscanf(as->parms[2].items->data, "%llu%s", &maxsize, str);
        if (i == 2) {
            if (str[0] == 'k' || str[0] == 'K')
                maxsize = maxsize << 10;
            else
            if (str[0] == 'm' || str[0] == 'M')
                maxsize = maxsize << 20;
            else
            if (str[0] == 'g' || str[0] == 'G')
                maxsize = maxsize << 30;
            else
                i = 3;
        }
        if ((i != 1 && i != 2) || maxsize < minsize) {
            fprintf(stderr,"Invalid value for maxsize %s.\n",
                        as->parms[2].items->data);
            return 1;
        }
    }
    if (as->parms[3].items) {           /* -copies */
        code = util_GetUInt32(as->parms[3].items->data, &copies);
        if (code || copies < 1 || copies > 4) {
            fprintf(stderr,"Invalid value for copies %s.\n",
                        as->parms[3].items->data);
            return 1;
        }
    }
    if (as->parms[4].items) {           /* -maxcandidates */
        code = util_GetUInt32(as->parms[4].items->data, &maxcandidates);
        if (code || maxcandidates < 1 || maxcandidates > 4096) {
            fprintf(stderr,"Invalid value for maxcandidates %s.\n",
                        as->parms[4].items->data);
            return 1;
        }
    }
    if (as->parms[5].items) {           /* -osd */
        code = util_GetInt32(as->parms[5].items->data, &osd);
        if (code || osd < 2) {
            fprintf(stderr,"Invalid value for osd %s.\n",
                        as->parms[5].items->data);
            return 1;
        }
    }
    if (as->parms[6].items) {           /* -wipeable */
        flag |= ONLY_BIGGER_MINWIPESIZE;
    }
    if (as->parms[7].items) {           /* delay */
        code = sscanf(as->parms[7].items->data, "%u%s", &delay, str);
        if (code == 2) {
            if (str[0] == 'm' || str[0] == 'M')
                delay = delay * 60;
            else if (str[0] == 'h' || str[0] == 'H')
                delay = delay * 3600;
            else if (str[0] == 'd' || str[0] == 'D')
                delay = delay * 3600 * 24;
            else if (str[0] != 's' && str[0] != 'S') {
                fprintf(stderr, "Unknown time unit %s, aborting\n", str);
                return EINVAL;
            }
        }
    }
    if (as->parms[8].items) {           /* -force */
        flag |= FORCE_ARCHCAND;
    }

    rx_SetRxDeadTime(60 * 10);
    for (i = 0; i < MAXSERVERS; i++) {
        struct rx_connection *rxConn = ubik_GetRPCConn(*vos_data->cstruct, i);
        if (rxConn == 0)
            break;
        rx_SetConnDeadTime(rxConn, *vos_data->rx_connDeadTime);
        if (rx_ServiceOf(rxConn))
            rx_ServiceOf(rxConn)->connDeadTime = *vos_data->rx_connDeadTime;
    }

    tcon = UV_BindOsd(server, AFSCONF_VOLUMEPORT);
    if (tcon) {
        code = AFSVOLOSD_GetArchCandidates(tcon, minsize, maxsize, copies,
                                        maxcandidates, osd, flag, delay, &list);
        rx_DestroyConnection(tcon);
    }
    if (!code) {
        afs_uint64 tb = 0;
        printf("Fid                           Weight      Blocks\n");
        for (j=0; j<list.hsmcandList_len; j++) {
            struct hsmcand *c = &list.hsmcandList_val[j];
            snprintf(str,64,"%u.%u.%u", c->volume, c->vnode, c->unique);
            printf("%-28s %10u %8u\n", str, c->weight, c->blocks);
            tb += c->blocks;
        }
        printf("Totally %u files with %llu blocks\n", list.hsmcandList_len, tb);
    } else
        fprintf(stderr, "UV_GetArchCandidates failed with %d\n", code);

    return code;
}

afs_uint32
getOsdId(char *name, char *cell, afs_int32 *code)
{
    *code = 0;
    if (isdigit(name[0])) {
        char *end;
        afs_uint32 result;
        result = strtoul(name, &end, 10);
        if (result != ~(afs_uint32)0 && *end == '\0')
            return result;
    }
    /* Here to put in code to translate osd names ti ids */
    *code = EINVAL;
    return 0;
}

static int
ListObjects(struct cmd_syndesc *as, void *arock)
{
    afs_int32 code, err, bytes;
    char *cell = 0;
    afs_uint32 osd = 0;
    afs_uint32 server = 0;
    afs_uint32 vid = 0;
    afs_uint32 flag = 0;
    struct rx_connection *tcon;
    struct rx_call *call;
    afs_int32 j;
    struct nvldbentry entry;
    char line[128];
    char *p = line;
    afs_uint32 delay = 3600;
    char str[16];

    if (as->parms[12].items)                    /*  -cell  */
        cell = as->parms[12].items->data;
    if (as->parms[3].items)                     /* -size */
        flag |= EXTRACT_SIZE;
    if (as->parms[4].items)                     /* -md5 */
        flag |= EXTRACT_MD5;
    if (as->parms[5].items)                     /* -single */
        flag |= ONLY_HERE;

    if (as->parms[6].items) {                   /* -policies */
        if ( flag & EXTRACT_SIZE ) {
            fprintf(stderr, "Cannot extract size when listing policies.\n");
            return EINVAL;
        }
        if ( flag & EXTRACT_MD5 ) {
            fprintf(stderr, "Cannot extract md5 sums when listing policies.\n");
            return EINVAL;
        }
        if ( flag & ONLY_HERE ) {
            fprintf(stderr,
                "Cannot find single occurences when listing policies.\n");
            return EINVAL;
        }
        flag |= POL_INDICES;
    }
    if (as->parms[7].items) {                           /* -minage */
        code = sscanf(as->parms[7].items->data, "%u%s", &delay, str);
        if (code == 2) {
            if (str[0] == 'm' || str[0] == 'M')
                delay = delay * 60;
            else if (str[0] == 'h' || str[0] == 'H')
                delay = delay * 3600;
            else if (str[0] == 'd' || str[0] == 'D')
                delay = delay * 3600 * 24;
            else if (str[0] != 's' && str[0] != 'S') {
                fprintf(stderr, "Unknown time unit %s, aborting\n", str);
                return EINVAL;
            }
        }
    }
    if (as->parms[8].items)                             /* -wiped */
        flag |= ONLY_WIPED;

    osd = getOsdId(as->parms[0].items->data, cell, &code);
    if (!osd && !(flag & POL_INDICES)) {
        fprintf(stderr, "Osd %s not found (error code %d)\n",
                as->parms[0].items->data, code);
        return EINVAL;
    }
    if (!as->parms[1].items && !as->parms[2].items) {
        fprintf(stderr, "Where? Neither server nor volume were specified\n");
        return EINVAL;
    }
    if (as->parms[1].items)                     /* -server  */
        server = GetServer(as->parms[1].items->data);
    if (as->parms[2].items) {                   /*  -id */
        vid = vsu_GetVolumeID(as->parms[2].items->data, *vos_data->cstruct, &err);
        if (vid == 0) {
            if (err)
		fprintf(STDERR, ": %s\n", afs_error_message(err));
            else
                fprintf(STDERR, "Can't find volume '%s'\n",
                        as->parms[2].items->data);
            return ENOENT;
        }
        code = VLDB_GetEntryByID(vid, -1, &entry);
        if (code) {
            fprintf(STDERR,
                "Could not fetch the entry for volume number %lu from VLDB \n",
                    (unsigned long)(vid));
            return (code);
        }
        if (server) {
            int found = 0;
            for (j=0; j<entry.nServers; j++) {
                if (server == htonl(entry.serverNumber[j])) {
                    if (entry.serverFlags[j] & VLSF_RWVOL) {
                        if (entry.volumeId[0] == vid)
                            found++;
                        if (entry.flags & VLF_BACKEXISTS) {
                            if (entry.volumeId[2] == vid)
                                found++;
                        }
                    }
                    if (entry.serverFlags[j] & VLSF_ROVOL) {
                        if (entry.volumeId[1] == vid)
                            found++;
                    }
                    break;
                }
            }
            if (!found)
                fprintf(stderr, "VLDB doesn't believe volume %u to be on server %s\n",
                        vid, as->parms[1].items->data);
        } else {
            int mask = 0;
            if (entry.volumeId[0] == vid)       /* RW volume */
                mask = VLSF_RWVOL;
            if (entry.volumeId[1] == vid)       /* RO volume */
                mask = VLSF_ROVOL;
            if (entry.volumeId[2] == vid        /* BK volume */
              && entry.flags & VLF_BACKEXISTS)
                mask = VLSF_ROVOL;
            for (j=0; j<entry.nServers; j++) {
                if (entry.serverFlags[j] & mask) {
                    server = htonl(entry.serverNumber[j]);
                    break;
                }
            }
        }
    }
    tcon = UV_BindOsd(server, AFSCONF_VOLUMEPORT);
    if (!tcon) {
        fprintf(stderr, "Couldn't get connection to %x\n", server);
        return EIO;
    }
    call = rx_NewCall(tcon);
    code = StartAFSVOLOSD_ListObjects(call, vid, flag, osd, delay);
    if (code) {
        fprintf(stderr, "Couldn't start RPC to server %x (error code %d)\n",
                    server, code);
        return EIO;
    }    while (1) {
        bytes = rx_Read(call, p, 1);
        if (bytes <= 0)
            break;
        if (*p == '\n') {
            p++;
            *p = 0;
            if (line[0] == 'e')
                fprintf(stderr, "%s", &line[1]);
            else
                fprintf(stdout, "%s", &line[1]);
            p = line;
        } else {
            if (*p == 0)
                break;
            p++;
        }
    }
    if (p != line) {
        if (line[0] == 'e')
            fprintf(stderr, "%s", &line[1]);
        else
            fprintf(stdout, "%s", &line[1]);
    }
    code = rx_EndCall(call, 0);
    rx_DestroyConnection(tcon);
    if (code)
        fprintf(stderr, "RPC failed with code %d\n", code);
    return code;
}

static int
SalvageOSD(struct cmd_syndesc *as, void *arock)
{
    afs_int32 code, err;
    int i, j, bytes;
    afs_uint32 server = 0, vid;
    afs_int32 flags = 8;        /* to say volserver we are using new syntax */
    struct rx_connection *tcon;
    afs_int32 instances = 0;
    afs_int32 localinst = 0;
    struct nvldbentry entry;
    afs_int32 type[MAXTYPES] = {RWVOL, ROVOL, BACKVOL};
    afs_int32 vtype;

    vid = vsu_GetVolumeID(as->parms[0].items->data, *vos_data->cstruct, &err);
    if (!vid) {
        fprintf(stderr, "Volume %s not found\n", as->parms[0].items->data);
        return EINVAL;
    }
    if (as->parms[1].items)             /* server  */
        server = GetServer(as->parms[1].items->data);
    if (as->parms[2].items)             /* update  */
        flags |= SALVAGE_UPDATE;
    if (as->parms[3].items)             /* decrement  */
        flags |= SALVAGE_DECREM;
    if (as->parms[4].items)             /* ignore linkcounts  */
        flags |= SALVAGE_IGNORE_LINKCOUNTS;
    if ((flags & SALVAGE_IGNORE_LINKCOUNTS) && (flags & SALVAGE_DECREM)) {
        fprintf(stderr,"vos salvage: -decrement and -ignorelinkcounts are mutually exclusive.\n");
        return (1);
    }

    code = VLDB_GetEntryByID(vid, -1, &entry);
    if (code) {
        fprintf(STDERR,
            "Could not fetch the entry for volume number %lu from VLDB \n",
                (unsigned long)(vid));
        return (code);
    }
    for (i=0; i<MAXTYPES; i++) {
        vtype = type[i];
        if (entry.volumeId[i] == vid)
            break;
    }
    if (i != 0) {       /* Not RWVOL */
        if (flags & (SALVAGE_UPDATE | SALVAGE_DECREM)) {
            fprintf(STDERR,
            "Only RW-volumes can be salvaged with -update or -decr\n");
            return EINVAL;
        }
    }

    code = ubik_VL_SetLock(*vos_data->cstruct, 0, vid, vtype, VLOP_SALVAGE);
    if (code) {
        if (code == 363542) {
            /* Old vldebserver doesn't understand VLOP_SALVAGE, use VLOP_DUMP instead */
            code = ubik_VL_SetLock(*vos_data->cstruct, 0, vid, vtype, VLOP_DUMP);
        }
        if (code) {
            fprintf(STDERR,
                    "Could not lock volume %lu, aborting\n",
                    (unsigned long)(vid));
            return (code);
        }
    }

    for (j=0; j<entry.nServers; j++) {
        if (entry.serverFlags[j] & VLSF_RWVOL) {
            if (!server)
                server = htonl(entry.serverNumber[j]);
            break;
        }
    }
    for (i=0; i<entry.nServers; i++) {
        if (entry.serverFlags[i] & VLSF_RWVOL) {
            localinst++;
            instances++;
            if (entry.flags & VLF_BACKEXISTS) {
                localinst++;
                instances++;
                if (!server && vid == entry.volumeId[2])
                    server = htonl(entry.serverNumber[i]);
            }
        }
        if (entry.serverFlags[i] & VLSF_ROVOL) {
            instances++;
            if (!server && vid == entry.volumeId[1])
                server = htonl(entry.serverNumber[i]);
            if (entry.serverNumber[i] == entry.serverNumber[j]
              && entry.serverPartition[i] == entry.serverPartition[j])
                localinst++;
        }
    }
    if (as->parms[5].items) {           /* instances  */
        code = util_GetInt32(as->parms[5].items->data, &i);
        if (i != instances)
            fprintf(stderr,"Warning VLDB knows of %u global instances, not %u\n",
                instances, i);
        instances = i;
    }
    if (as->parms[6].items) {           /* localinst  */
        code = util_GetInt32(as->parms[6].items->data, &i);
        if (i != localinst)
            fprintf(stderr,"Warning VLDB knows of %u local instances, not %u\n",
                localinst, i);
        localinst = i;
    }
    if (instances > 15 || localinst > 3) {
        fprintf(stderr, "Bad value: instances must not exceed 15 and localinst must not exceed 3!\n");
        return EINVAL;
    }
    if (!instances || !localinst) {
        fprintf(stderr, "Bad value: instances must not be 0 and localinst must not be 0!\n");
        return EINVAL;
    }
    rx_SetRxDeadTime(60 * 10);
    for (i = 0; i < MAXSERVERS; i++) {
        struct rx_connection *rxConn = ubik_GetRPCConn(*vos_data->cstruct, i);
        if (rxConn == 0)
            break;
        rx_SetConnDeadTime(rxConn, *vos_data->rx_connDeadTime);
        if (rx_ServiceOf(rxConn))
            rx_ServiceOf(rxConn)->connDeadTime = *vos_data->rx_connDeadTime;
    }

    tcon = UV_BindOsd(server, AFSCONF_VOLUMEPORT);
    if (tcon) {
        struct rx_call *call = rx_NewCall(tcon);
        code = StartAFSVOLOSD_Salvage(call, vid, flags, instances, localinst);
        if (!code) {
            char line[128];
            char *p = line;
            while (1) {
                bytes = rx_Read(call, p, 1);
                if (bytes <= 0)
                    break;
                if (*p == '\n') {
                    p++;
                    *p = 0;
                    printf("%s", line);
                    p = line;
                } else {
                    if (*p == 0)
                        break;
                    p++;
                }
            }
            code = rx_EndCall(call, 0);
            if (code)
                fprintf(stderr, "RPC failed with code %d\n", code);
        }
        rx_DestroyConnection(tcon);
    } else
        fprintf(stderr, "Couldn't get connection to %s\n",
                as->parms[0].items->data);
    ubik_VL_ReleaseLock(*vos_data->cstruct, 0, vid, -1,
                                (LOCKREL_OPCODE | LOCKREL_AFSID |
                                 LOCKREL_TIMESTAMP));
    return code;
}

void printlength(afs_uint64 length)
{
    char unit[3];
    afs_uint64 l = length;

    strcpy(unit, " B");
    if (l>1024)
        strcpy(unit, "KB");
    if (l >= 1048576) {
        l = l >> 10;
        strcpy(unit, "KB");
        if (l>1024)
            strcpy(unit, "MB");
    }
    if (l >= 1048576) {
        l = l >> 10;
        strcpy(unit, "MB");
        if (l>1024)
            strcpy(unit, "GB");
    }
    if (l >= 1048576) {
        l = l >> 10;
        strcpy(unit, "GB");
        if (l>1024)
            strcpy(unit, "TB");
    }
    if (l >= 1048576) {
        l = l >> 10;
        strcpy(unit, "TB");
        if (l>1024)
            strcpy(unit, "PB");
    }
    if (l>1024)
        printf("%4llu.%03llu %s",
             l >> 10, ((l % 1024) * 1000) >> 10, unit);
    else
        printf("%8llu %s", l, unit);
    return;
}

#define MAXOSDS 1024

static int
Traverse(struct cmd_syndesc *as, void *arock)
{
    afs_int32 vid=0, code, err;
    struct cmd_item *ti;
    afs_uint32 server[256];
    afs_int32 nservers = 0;
    struct sizerangeList totalsizerange, *srl;
    struct osd_infoList totalinfo, *list;
    char *cell = 0;
    afs_uint64 max, min = 0;
    int i, j, k;
    char unit[8], minunit[8];
    afs_uint64 totalfiles;
    afs_uint64 totalbytes;
    afs_uint64 runningbytes;
    afs_uint64 runningfiles;
    char *newvolume = 0;
    int highest = 0;
    int more = 0;
    int policy_statistic = 0;
    afs_uint32 delay = 0;
    char serverbuf[128];
    char volumebuf[32];
    float percentfiles, percentdata, runpercfiles, runpercdata;
    afs_uint64 maxsize = 4096;
    char str[16];
    afs_int32 flag = 0;

    srl = &totalsizerange;
    list = &totalinfo;
    rx_SetRxDeadTime(60 * 10);
    for (i = 0; i < MAXSERVERS; i++) {
        struct rx_connection *rxConn = ubik_GetRPCConn(*vos_data->cstruct, i);
        if (rxConn == 0)
            break;
        rx_SetConnDeadTime(rxConn, *vos_data->rx_connDeadTime);
        if (rx_ServiceOf(rxConn))
            rx_ServiceOf(rxConn)->connDeadTime = *vos_data->rx_connDeadTime;
    }
    if (as->parms[1].items)
        newvolume = as->parms[1].items->data;
    if (as->parms[2].items)
        more = 1;
    if (as->parms[3].items)
        policy_statistic = 1;
    if (as->parms[4].items) {
        code = sscanf(as->parms[4].items->data, "%u%s", &delay, str);
        if (code == 2) {
            if (str[0] == 'm' || str[0] == 'M')
                delay = delay * 60;
            else if (str[0] == 'h' || str[0] == 'H')
                delay = delay * 3600;
            else if (str[0] == 'd' || str[0] == 'D')
                delay = delay * 3600 * 24;
            else if (str[0] != 's' && str[0] != 'S') {
                fprintf(stderr, "Unknown time unit %s, aborting\n", str);
                return EINVAL;
            }
        }
    }
    if (as->parms[5].items)             /* -onlyosd */
        flag |= 4;
    if (as->parms[6].items)             /* -noosd */
        flag |= 8;
    if (as->parms[12].items)    /* if -cell specified */
        cell = as->parms[12].items->data;
    printf("\nservers:\n");
    if (as->parms[0].items) {
        for (ti = as->parms[0].items; ti; ti = ti->next) {
            printf("\t%s\n", ti->data);
            server[nservers] = GetServer(ti->data);
            if (server[nservers] == 0) {
                fprintf(STDERR, "vos: host '%s' not found in host table\n",
                        ti->data);
                return ENOENT;
            }
            nservers++;
        }
        if (newvolume && nservers > 1) {
            fprintf(stderr, "Only one server per volume possible\n");
            return EINVAL;
        }
    }
    printf("\n");
    if ( policy_statistic )
        srl = NULL;
    else {
        srl->sizerangeList_val = (struct sizerange *)
                                        malloc(48 * sizeof(struct sizerange));
        memset(srl->sizerangeList_val, 0, 48 * sizeof(struct sizerange));
        srl->sizerangeList_len = 48;
        for (i=0; i<srl->sizerangeList_len; i++) {
            srl->sizerangeList_val[i].maxsize = maxsize;
            maxsize = maxsize << 1;
        }
    }
    list->osd_infoList_val = (struct osd_info *)
                                malloc(MAXOSDS * sizeof(struct osd_info));
    memset(list->osd_infoList_val, 0, MAXOSDS * sizeof(struct osd_info));
    list->osd_infoList_len = MAXOSDS;
    do {
        if (newvolume) {
            vid = vsu_GetVolumeID(newvolume, *vos_data->cstruct, &err);
            if (vid == 0) {
                if (err)
		    fprintf(STDERR, ": %s\n", afs_error_message(err));
                else
                    fprintf(STDERR, "vos: can't find volume '%s'\n",
                            as->parms[1].items->data);
                return ENOENT;
            }
        }
        code = UV_Traverse(server, vid, nservers, flag, delay, srl, list);
        if (code) {
            fprintf(stderr, "UV_Traverse returned %d\n", code);
        }
        if (more) {
            char buffer[256];
            int bytes, fields;

            newvolume = 0;
            nservers = 0;
            fprintf(stderr, "Please enter next server and volume to examine or empty input to finish\n");
            bytes = read(0, buffer, 256);
            if (bytes) {
                buffer[bytes] = 0;
                fields = sscanf(buffer, "%s %s\n", serverbuf, volumebuf);
                if (fields > 0) {
                    server[0] = GetServer(serverbuf);
                    nservers = 1;
                    if (fields == 2)
                        newvolume = volumebuf;
                }
            }
            if (!newvolume && !nservers)
                more = 0;
        }
    } while (more);

    if ( policy_statistic ) {
        printf("%8s%15s%15s\n--------------------------------------\n",
                        "Policy", "#dirs", "#volumes");
        for ( i = 1 ; i < list->osd_infoList_len ; i++ ) {
            struct osd_info info = list->osd_infoList_val[i];
            if ( info.fids )
                printf("%8d%15d%15d\n", info.osdid, info.fids, info.fids1);
        }
        printf("\n%8s%15d%15d\n", "unknown", list->osd_infoList_val[0].fids,
                        list->osd_infoList_val[0].fids1);

        printf("\nPolicies not seen: ");
        for ( i = 1 ; i < list->osd_infoList_len ; i++ ) {
            struct osd_info info = list->osd_infoList_val[i];
            if ( info.osdid && !info.fids )
                printf("%d ", info.osdid);
        }
        printf("\n");
        return 0;
    }

    if (srl->sizerangeList_len) {
        struct ubik_client *osddb_client;
        struct OsdList l;
        char unknown[8] = "unknown";
        char *p;

        osddb_client = init_osddb_client(cell, localauth);
        memset(&l, 0, sizeof(l));
        code = ubik_Call((int(*)(struct rx_connection*,...))OSDDB_OsdList,
			 osddb_client, 0, &l);
        if (code) {
                fprintf(stderr, "OSDDB_OsdList failed with code %d\n", code);
                return code;
        }

        strcpy(minunit, " B");
        printf("\nFile Size Range    Files      %%  run %%     Data         %%  run %%\n");
        printf("----------------------------------------------------------------\n");
        totalfiles = 0;
        totalbytes = 0;
        for (i=0; i<srl->sizerangeList_len; i++) {
            if (srl->sizerangeList_val[i].fids) {
                highest = i;
                totalfiles += srl->sizerangeList_val[i].fids;
                totalbytes += srl->sizerangeList_val[i].bytes;
            }
        }
        runningfiles = 0;
        runningbytes = 0;
        for (i=0; i<=highest; i++) {
            max = srl->sizerangeList_val[i].maxsize;
            if (max >= 1024) {
                max = max >> 10;
                strcpy(unit, "KB");
            }
            if (max >= 1024) {
                max = max >> 10;
                strcpy(unit, "MB");
            }
            if (max >= 1024) {
                max = max >> 10;
                strcpy(unit, "GB");
            }
            if (max >= 1024) {
                max = max >> 10;
                strcpy(unit, "TB");
            }
            if (max >= 1024) {
                max = max >> 10;
                strcpy(unit, "PB");
            }
            runningfiles += srl->sizerangeList_val[i].fids;
            percentfiles = srl->sizerangeList_val[i].fids;
            percentfiles = percentfiles * 100;
            percentfiles = percentfiles / totalfiles;
            runpercfiles = runningfiles * 100;
            runpercfiles = runpercfiles / totalfiles;
            runningbytes += srl->sizerangeList_val[i].bytes;
            percentdata = srl->sizerangeList_val[i].bytes;
            percentdata = percentdata * 100 / totalbytes;
            runpercdata = runningbytes * 100;
            runpercdata = runpercdata / totalbytes;
            printf("%3llu %s - %3llu %s %8u %6.2f %6.2f ",
                min, minunit,
                max, unit,
                srl->sizerangeList_val[i].fids,
                percentfiles, runpercfiles);
            printlength(srl->sizerangeList_val[i].bytes);
            printf(" %6.2f %6.2f\n",
                percentdata, runpercdata);
            min = max;
            strcpy(minunit, unit);
        }
        printf("----------------------------------------------------------------\n");
        printf("Totals:      %11llu Files         ",
                        totalfiles);
        printlength(totalbytes);
        printf("\n");
        totalfiles = 0;
        totalbytes = 0;

        printf("\nStorage usage:\n");
        printf("---------------------------------------------------------------\n");
        for (i=0; i<list->osd_infoList_len; i++) {
            max = 0xfffffff;
            for (j=0; j<list->osd_infoList_len; j++) {
                if (list->osd_infoList_val[j].osdid < max) {
                    max = list->osd_infoList_val[j].osdid;
                    k = j;
                }
            }
            if (list->osd_infoList_val[k].fids) {
                int archival = 0;
                totalfiles += list->osd_infoList_val[k].fids;
                totalbytes += list->osd_infoList_val[k].bytes;
                if (list->osd_infoList_val[k].osdid == 1)
                    printf("\t        1 local_disk %10u files    ",
                        list->osd_infoList_val[k].fids);
                else {
                    p = unknown;
                    for (j=0; j<l.OsdList_len; j++) {
                        if (l.OsdList_val[j].id ==
                                list->osd_infoList_val[k].osdid) {
                            p = l.OsdList_val[j].name;
                            if (l.OsdList_val[j].t.etype_u.osd.flags & OSDDB_ARCHIVAL)
                                archival = 1;
                            break;
                        }
                    }
                    printf("  %s Osd %5u %-12s %8u objects  ",
                        archival ? "arch.":"     ",
                        list->osd_infoList_val[k].osdid, p,
                        list->osd_infoList_val[k].fids);
                }
                printlength(list->osd_infoList_val[k].bytes);
                printf("\n");
            }
            list->osd_infoList_val[k].osdid |= 0x8000000;
        }
        printf("---------------------------------------------------------------\n");
        printf("Total                       %11llu objects  ",
                totalfiles);
        printlength(totalbytes);
        printf("\n");

        totalfiles = 0;
        totalbytes = 0;
        printf("\nData without a copy:\n");
        printf("---------------------------------------------------------------\n");
        for (i=0; i<list->osd_infoList_len; i++) {
            max = 0xfffffff;
            for (j=0; j<list->osd_infoList_len; j++) {
                if (list->osd_infoList_val[j].osdid < max) {
                    max = list->osd_infoList_val[j].osdid;
                    k = j;
                }
            }
            if (list->osd_infoList_val[k].fids1) {
                int archival = 0;
                p = unknown;
                for (j=0; j<l.OsdList_len; j++) {
                    if (l.OsdList_val[j].id
                      == (list->osd_infoList_val[k].osdid & 0x7ffffff)) {
                        p = l.OsdList_val[j].name;
                        if (l.OsdList_val[j].t.etype_u.osd.flags & OSDDB_ARCHIVAL)
                            archival = 1;
                        break;
                    }
                }
                totalfiles += list->osd_infoList_val[k].fids1;
                totalbytes += list->osd_infoList_val[k].bytes1;
                if ((list->osd_infoList_val[k].osdid  & 0x7ffffff) == 1)
                    printf("if !replicated: 1 local_disk %10u files    ",
                        list->osd_infoList_val[k].fids1);
                else
                    printf("  %s Osd %5u %-12s %8u objects  ",
                        archival ? "arch.":"     ",
                        list->osd_infoList_val[k].osdid & 0x7ffffff, p,
                        list->osd_infoList_val[k].fids1);
                printlength(list->osd_infoList_val[k].bytes1);
                printf("\n");
            }
            list->osd_infoList_val[k].osdid = 0xfffffff;
        }
        printf("---------------------------------------------------------------\n");
        printf("Total                       %11llu objects  ",
                totalfiles);
        printlength(totalbytes);
        printf("\n");
    } else
        code = 0;

    return code;
}

struct vol_data_v0 vol_data_v0;

int
init_voscmd_afsosd(char *myVersion, char **versionstring,
                void *inrock, void *outrock,
                void *libafsosdrock, afs_int32 version)
{
    afs_int32 code;
    struct cmd_syndesc *ts;
    memset(&vol_data_v0, 0, sizeof(vol_data_v0));
    voldata = &vol_data_v0;
    vos_data = (struct vos_data *) inrock;
    voldata->aLogLevel = vos_data->aLogLevel;

    code = libafsosd_init(libafsosdrock, version);
    if (code)
	return code;

    ts = cmd_CreateSyntax("archcand", Archcand, NULL,
                          "get list of fids which need an archval copy");
    cmd_AddParm(ts, "-server", CMD_SINGLE, 0, "machine name");
    cmd_AddParm(ts, "-minsize", CMD_SINGLE, CMD_OPTIONAL, "minimal file size");
    cmd_AddParm(ts, "-maxsize", CMD_SINGLE, CMD_OPTIONAL, "maximal file size");
    cmd_AddParm(ts, "-copies", CMD_SINGLE, CMD_OPTIONAL, "number of archival copies required, default 1");
    cmd_AddParm(ts, "-candidates", CMD_SINGLE, CMD_OPTIONAL, "number of candidates default 256");
    cmd_AddParm(ts, "-osd", CMD_SINGLE, CMD_OPTIONAL, "id of archival osd");
    cmd_AddParm(ts, "-wipeable", CMD_FLAG, CMD_OPTIONAL, "ignore files < minwipesize");
    cmd_AddParm(ts, "-delay", CMD_SINGLE, CMD_OPTIONAL, "minimum age (default 3600s)");
    cmd_AddParm(ts, "-force", CMD_FLAG, CMD_OPTIONAL, "check also volumes without osdpolicy");
    COMMONPARMS;

    ts = cmd_CreateSyntax("listobjects", ListObjects, NULL,
			  "list all objects on specified osd.");
    cmd_AddParm(ts, "-osd", CMD_SINGLE, 0, "osd or policy id");
    cmd_AddParm(ts, "-server", CMD_SINGLE, CMD_OPTIONAL, "machine name");
    cmd_AddParm(ts, "-id", CMD_SINGLE, CMD_OPTIONAL, "volume name or ID");
    cmd_AddParm(ts, "-size", CMD_FLAG, CMD_OPTIONAL, "extract file size");
    cmd_AddParm(ts, "-md5", CMD_FLAG, CMD_OPTIONAL, "extract md5 checksums");
    cmd_AddParm(ts, "-single", CMD_FLAG, CMD_OPTIONAL, "only here");
    cmd_AddParm(ts, "-policies", CMD_FLAG, CMD_OPTIONAL,
        "list policy references instead");
    cmd_AddParm(ts, "-minage", CMD_SINGLE, CMD_OPTIONAL, "minimum age (default 3600)");
    cmd_AddParm(ts, "-wiped", CMD_FLAG, CMD_OPTIONAL, "only wiped files");
    COMMONPARMS;

    ts = cmd_CreateSyntax("salvage", SalvageOSD, NULL,
                          "sanity check for a volume.");
    cmd_AddParm(ts, "-id", CMD_SINGLE, 0, "volume name or ID");
    cmd_AddParm(ts, "-server", CMD_SINGLE, CMD_OPTIONAL, "machine name");
    cmd_AddParm(ts, "-update", CMD_FLAG, CMD_OPTIONAL, "allow volume update");
    cmd_AddParm(ts, "-decrement", CMD_FLAG, CMD_OPTIONAL, "allow decrement of too high link counts");
    cmd_AddParm(ts, "-ignorelinkcounts", CMD_FLAG, CMD_OPTIONAL, "ignore linkcounts whenupdating the volume. Mutually exclusive with -decrement.");
    cmd_AddParm(ts, "-instances", CMD_SINGLE, CMD_OPTIONAL, "global number of volume instances");
    cmd_AddParm(ts, "-localinst", CMD_SINGLE, CMD_OPTIONAL, "number of volume instances in RW-partition");
    COMMONPARMS;

    ts = cmd_CreateSyntax("traverse", Traverse, NULL,
                          "gather file statistic from server.");
    cmd_AddParm(ts, "-server", CMD_LIST, 0, "machine names");
    cmd_AddParm(ts, "-id", CMD_SINGLE, CMD_OPTIONAL, "volume name or ID");
    cmd_AddParm(ts, "-more", CMD_FLAG, CMD_OPTIONAL, "ask for more servers/volume");
    cmd_AddParm(ts, "-policies", CMD_FLAG, CMD_OPTIONAL, "make policy usage statisticinstead of space usage");
    cmd_AddParm(ts, "-delay", CMD_SINGLE, CMD_OPTIONAL, "age after which files are expected to have a copy");
    cmd_AddParm(ts, "-onlyosd", CMD_FLAG, CMD_OPTIONAL, "traverse only OSD volumes");
    cmd_AddParm(ts, "-noosd", CMD_FLAG, CMD_OPTIONAL, "traverse only non-OSD volumes");
    COMMONPARMS;

    return 0;
}
