# Conary -- Archived Repository

**Notice: This repository is part of a Conary/rpath project at SAS that is no longer supported or maintained. Hence, the repository is being archived and will live in a read-only state moving forward. Issues, pull requests, and changes will no longer be accepted.**

Overview
--------

Conary is a system software provisioning and management tool that brings
concepts from distributed source code control systems such as Git and Mercurial
to system management. Conary provides differential update, rollback,
configuration management, staging/promotion, entitlement, replication,
dependency management, introspection, attribution/lineage, repeatable build,
and layered platform/system definition. Unlike most package-based software
management tools that depend on archive files as their primary mechanism of
distribution, Conary provides networked repositories containing structured
version hierarchies of all the files and organized sets of files in software
products.

Conary has three main components: software repository, system management, and
software build. The system management component manages the state of an
individual system (based on the contents of a Conary repository), the build
component automates building software and collections of software into a Conary
repository, and the Conary repository is a web application that stores versions
of software and collections of software.

Conary models the intended state of a system, such that it can recreate the
same state systematically on other machines, enabling precise staging
(“dev/test/prod”) of changes through a deployment process, and easing
provisioning of large sets of similar or identical systems. A model can be
maintained on a target system or packaged into a Conary repository. Conary also
intelligently preserves intentional local changes on installed systems, such
that an update will not blindly obliterate local changes such as changes to
configuration files.

Conary is also the core technology of a family of tools that further automate
the software build and management process.
