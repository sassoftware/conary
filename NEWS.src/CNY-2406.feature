Added a new repository content store type 'flat'. In this configuration
contents are stored without any intermediate directories, which may improve
access times and will improve sequential access by e.g. backups. To use it, use
the syntax "contentsDir flat /path/to/store".
