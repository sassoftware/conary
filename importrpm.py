import rpm
import os
import files
import util
import commit
import stat

def doImport(dbpath, rpmFile):
    scratch = dbpath + "/scratch"
    fileDB = dbpath + "/files"

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
    rdevs = h['filerdevs']
    linktos = h['filelinktos']
    flags = h['fileflags']

    del h
    del ts

    fileList = []

    # this is a hack for the dev package so we don't need to be root
    # to import it
    mustExtract = 0
    for i in range(0, len(list)):
	if (stat.S_ISREG(modes[i])):
	    if not (flags[i] & rpm.RPMFILE_GHOST):
		f = files.RegularFile(list[i])
		mustExtract = 1
	    else:
		continue
	    f.md5(md5s[i])
	elif (stat.S_ISLNK(modes[i])):
	    f = files.SymbolicLink(list[i])
	    f.linkTarget(linktos[i])
	elif (stat.S_ISDIR(modes[i])):
	    f = files.Directory(list[i])
	elif (stat.S_ISFIFO(modes[i])):
	    f = files.NamedPipe(list[i])
	elif (stat.S_ISSOCK(modes[i])):
	    f = files.Socket(list[i])
	elif (stat.S_ISBLK(modes[i])):
	    f = files.DeviceFile(list[i])
	    major = (rdevs[i] & 0xffff) >> 8
	    f.majorMinor("b", major, rdevs[i] & 0xff)
	elif (stat.S_ISCHR(modes[i])):
	    f = files.DeviceFile(list[i])
	    major = (rdevs[i] & 0xffff) >> 8
	    f.majorMinor("c", major, rdevs[i] & 0xff)
	else:
	    raise TypeError, "unsupported file type for %s" % list[i]

	f.perms(modes[i] & 0777)
	f.owner(owners[i])
	f.group(groups[i])
	f.mtime(mtimes[i])
	fileList.append(f)

    util.mkdirChain(scratch)

    if mustExtract:
	os.system("cd %s; rpm2cpio %s | cpio -iumd --quiet" % 
		    (scratch, pkgFile))
    commit.finalCommit(dbpath, pkgName, version, scratch, fileList)

    if mustExtract:
	os.system("rm -rf %s" % scratch)

