import struct

# takes a python file object (positioned at the start of an rpm) and
# positions the file pointer at the gzipped cpio archive attached to it
def seekToData(f):
    lead = f.read(96)
    leadMagic = struct.unpack("!i", lead[0:4])[0]

    if (leadMagic & 0xffffffff) != 0xedabeedb: 
	print "0x%x" % leadMagic
	raise IOError, "file is not an RPM"

    # signature block
    sigs = f.read(16)
    (mag1, mag2, mag3, ver, reserverd, entries, size) = \
	struct.unpack("!BBBBiii", sigs)

    if mag1 != 0x8e or mag2 != 0xad or mag3 != 0xe8  or ver != 01:
	raise IOError, "bad magic for signature block"

    f.seek(size + entries * 16, 1)

    # headers
    sigs = f.read(16)
    (mag1, mag2, mag3, ver, reserverd, entries, size) = \
	struct.unpack("!BBBBiii", sigs)

    if mag1 != 0x8e or mag2 != 0xad or mag3 != 0xe8  or ver != 01:
	raise IOError, "bad magic for header"

    f.seek(size + entries * 16, 1)
