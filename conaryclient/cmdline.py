from deps import deps

def parseTroveSpec(specStr):
    if specStr.find('[') > 0 and specStr[-1] == ']':
        specStr = specStr[:-1]
        l = specStr.split('[')
        if len(l) != 2:
            raise TroveSpecError, "bad trove spec %s]" % specStr
        specStr, flavorSpec = l
        flavor = deps.parseFlavor(flavorSpec)
        if flavor is None:
            raise TroveSpecError, "bad flavor [%s]" % flavorSpec
    else:
        flavor = None

    if specStr.find("=") >= 0:
        l = specStr.split("=")
        if len(l) != 2:
            raise TroveSpecError, "too many ='s in %s" %specStr
        name, versionSpec = l
    else:
        name = specStr
        versionSpec = None

    return (name, versionSpec, flavor)

def toTroveSpec(name, versionStr, flavor):
    disp = [name]
    if versionStr:
        disp.extend(('=', versionStr))
    if flavor:
        disp.extend(('[', deps.formatFlavor(flavor), ']'))
    return ''.join(disp)

class TroveSpecError(Exception):

    pass

