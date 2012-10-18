The --replace-files option is now deprecated. Scripts should use the more
specific options --replace-unmanaged-files, --replace-modified-files,
--replace-managed-files, and --replace-config-files, depending on which
scenarios need to be overridden. Typically only the first two will be useful
and are safe other than that they may overwrite modifications to the system.
