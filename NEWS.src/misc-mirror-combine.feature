The mirror process can now build bigger changesets, which slightly speeds up
mirror jobs.  If configured using the new 'splitNodes False' option, the mirror
can commit multiple versions of a trove at once, although doing so on an older
repository will cause a crash.
