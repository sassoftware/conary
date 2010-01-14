import struct, zlib

encMap = [ chr(ord('0') + x) for x in range(0, 10) ] + \
         [ chr(ord('A') + x) for x in range(0, 26) ] + \
         [ chr(ord('a') + x) for x in range(0, 26) ] + \
         [ '!', '#', '$', '%', '&', '(', ')', '*', '+', '-',
           ';', '<', '=', '>', '?', '@', '^', '_', '`', '{',
           '|', '}', '~' ]

decMap = [ -1 ] * 256
for i, c in enumerate(encMap):
    decMap[ord(c)] = i

def decodestring(s):
    result = ''
    strLen = ord(s[0])
    if ord('A') <= strLen and strLen <= ord('Z'):
        strLen = strLen - ord('A') + 1
    else:
        strLen = strLen - ord('a') + 26 + 1

    i = 1
    l = []
    while i < len(s):
        val = 0
        snip = s[i:i+5]
        for j in range(min(5, len(snip))):
            val *= 85
            val += decMap[ord(snip[j])]

        if len(snip) < 5:
            for j in range(5 - len(snip)):
                val *= 85

        l.append(val & 0xFFFFFFFF)
        i += 5

    return struct.pack("!" + "L" * len(l), *l)[0:strLen]

def encodestring(s):
    strLen = len(s)
    result = []
    if strLen < 27:
        result.append(chr(ord('A') + (strLen - 1)))
    else:
        result.append(chr(ord('a') + (strLen - 1) - 26))

    i = 0
    while i < strLen:
        snip = s[i:i+4]
        snip += '\0' * (4 - len(snip))
        val = struct.unpack("!L", snip)[0]
        i += 4

        bits = [ 0 ] * 5
        for j in range(4, -1, -1):
            bits[j] = encMap[val % 85]
            val /= 85

        result += bits

    extra = strLen % 4
    if extra:
        result[-1] = result[-1][0:extra + 1]

    return "".join(result)

def decode(inFile, outFile, uncompress = False):
    if uncompress:
        decompressor = zlib.decompressobj()
        filter = decompressor.decompress
    else:
        filter = lambda x: x

    for line in inFile.xreadlines():
        decoded = decodestring(line)
        outFile.write(filter(decoded))

    if uncompress:
        outFile.write(decompressor.flush())

def encode(inFile, outFile, compress = False):
    for x in iterencode(inFile, compress = compress):
        outFile.write(x)

def iterencode(inFile, compress = False):
    BLOCKSIZE = 1024 * 64

    if compress:
        compressObj = zlib.compressobj()

    unencodedBuf = ''
    done = False
    while not done:
        rawData = inFile.read(BLOCKSIZE)
        if not rawData:
            # we're at the end of the file
            done = True
            if compress:
                unencodedBuf += compressObj.flush()
        elif compress:
            unencodedBuf += compressObj.compress(rawData)
        else:
            unencodedBuf += rawData

        stop = len(unencodedBuf)
        i = 0
        while (i + 52) <= stop:
            yield encodestring(unencodedBuf[i:i+52]) + '\n'
            i += 52
        unencodedBuf = unencodedBuf[i:]

    yield encodestring(unencodedBuf) + '\n'
