import group
import versions

def checkin(repos, cfg, file):
    f = open(file, "r")
    grp = group.GroupFromTextFile(f, cfg.packagenamespace, repos)
    simpleVer = grp.getSimpleVersion()

    ver = repos.grpLatestVersion(grp.getName(), cfg.defaultbranch)
    if not ver:
	ver = cfg.defaultbranch.copy()
	ver.appendVersionRelease(simpleVer, 1)
    elif ver.trailingVersion() == simpleVer:
	ver.incrementVersionRelease()
    else:
	ver = ver.branch()
	ver.appendVersionRelease(simpleVer, 1)

    grp.setVersion(ver)

    repos.addGroup(grp)

