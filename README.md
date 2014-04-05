afsosd
======

At the AFS & Kerberos Conference 2011 in Hamburg D. Brashear and Jeffrey Altman
proposed to seperate all AFS/OSD code into shared libraries and have in the
main openafs code only hooks into these libraries. So the plan was to
concentrate all the code in  the already existing subdirectory src/rxosd.

On the AFS & Kerberos Conference 2014 it we decided to keep most of the contants
of the src/rxosd in a new subdirectory src/afsosd which is a git submodule.
The reason for this deciscion was that openafs did not want to have to support
the AFS/OSD extensions in the future.

The contents of src/afsosd is hosted at github in the project
"git://github.com/hwr/afsosd.git"

If you want to compile openafs with afs/osd you need

1)	go to src and do a "git clone git://github.com/hwr/afsosd.git afsosd

2)	do the openafs configure with "--enable-object-storage"

Then the subdirectory src/afsosd is included in the build.

The subdirectory src/rxosd now contains only the code needed to build the
hooks in the main openafs code for the fileserver, volserver, salvager,
fs and vos commands and the cache manager.
