The addArchive source method now takes new preserveSetid and
preserveDirectories keyword arguments to honor setuid/setgid
bits, and to preserve directories that are empty, have non-root
owner or group, or have non-default permissions (not 755).
