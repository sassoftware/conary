A new update mechanism called "system model" is added.  In this model, a
file called /etc/conary/system-model describes what should be installed
on the system.  This file is modified by certain conary update commands,
and can also be edited with a text editor.  The system model allows a
system to be updated relative to a search path that includes groups as
well as labels, leading to more coherent sets of updated packages.  It
also allows re-starting updates with transient failures; the filename
/etc/conary/system-model.next is reserved for storing the system target
state during an update operation.
