Conary can now automatically reconcile its own database with external capsule databases.
If a package is installed in the RPM database without using Conary, the next Conary update operation will first fabricate a "phantom" trove as a placeholder.
That trove can then be erased, updated to a regular encapsulated trove, or left as-is.
Similarly, if a Conary-managed package is erased from the RPM database, the next update operation will also erase the corresponding Conary trove.
This feature can be disabled by setting the 'syncCapsuleDatabase' configuration option to False.
