import rpm
import os
import files
import util
import commit

def doImport(DBPATH, rpmFile):
    scratch = DBPATH + "/scratch"
    fileDB = DBPATH + "/files"

    ts = rpm.TransactionSet()
    ts.setVSFlags(~(rpm._RPMVSF_NOSIGNATURES))

    pkgFile = rpmFile;
    if pkgFile[0] != "/":
	pkgFile = os.getcwd() + '/' + pkgFile

    fd = os.open(pkgFile, os.O_RDONLY)
    h = ts.hdrFromFdno(fd)
    os.close(fd)

    pkgName = h['name']
    pkgVersion = h['version']
    pkgRelease = h['release']

    version = "/specifix.com/" + pkgVersion + "-" + pkgRelease

    if (not pkgRelease):
	print pkgFile + " does not appear to be a valid RPM"

    list = h['filenames']
    modes = h['filemodes']
    owners = h['fileusername']
    groups = h['filegroupname']
    mtimes = h['filemtimes']
    md5s = h['filemd5s']

    del h
    del ts

    fileList = []
    for i in range(0, len(list)):
	f = files.RegularFile(list[i])
	f.perms(modes[i] & 0777)
	f.owner(owners[i])
	f.group(groups[i])
	f.mtime(mtimes[i])
	f.md5(md5s[i])
	fileList.append(f)

    util.mkdirChain(scratch)

    os.system("cd %s; rpm2cpio %s | cpio -iumd --quiet" % (scratch, pkgFile))
    commit.finalCommit(DBPATH, pkgName, version, scratch, fileList)
    os.system("rm -rf %s" % scratch)

