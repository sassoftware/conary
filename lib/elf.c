/*
 * Copyright (c) 2004 Specifix, Inc.
 * All rights reserved
 *
 */

#include <Python.h>

#include <gelf.h>
#include <libelf.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static PyObject * inspect(PyObject *self, PyObject *args);

static PyObject * ElfError;

static PyMethodDef ElfMethods[] = {
    { "inspect", inspect, METH_VARARGS, 
	"inspect an ELF file for dependency information" },
    { NULL, NULL, 0, NULL }
};

static int doInspect(Elf * elf, PyObject * reqList, PyObject * provList) {
    Elf_Scn * sect = NULL;
    GElf_Shdr shdr;
    size_t shstrndx;
    char * name;
    int entries;
    GElf_Dyn sym;
    GElf_Verneed verneed;
    GElf_Vernaux veritem;
    GElf_Verdef verdef;
    GElf_Verdaux verdefItem;
    GElf_Ehdr ehdr;
    Elf_Data * data;
    int i, j;
    int idx, listIdx;
    char * libName;
    char * verdBase;
    char * abi;
    char * ident;
    size_t identSize;
    char * insSet;
    char * class;

    if (elf_kind(elf) != ELF_K_ELF) {
	PyErr_SetString(ElfError, "not a plain elf file");
	return 1;
    }

    ident = elf_getident(elf, &identSize);
    if (identSize < EI_OSABI) {
        PyErr_SetString(ElfError, "missing ELF abi");
	return 1;
    }

    if (ident[EI_CLASS] == ELFCLASS32)
	class = "ELF32";
    else if (ident[EI_CLASS] == ELFCLASS64)
	class = "ELF64";
    else {
	PyErr_SetString(ElfError, "unknown ELF class");
	return 1;
    }

    switch (ident[EI_OSABI]) {
      case ELFOSABI_SYSV:	abi = "SysV";	    break;
      case ELFOSABI_HPUX:	abi = "HPUX";	    break;
      case ELFOSABI_NETBSD:	abi = "NetBSD";	    break;
      case ELFOSABI_LINUX:	abi = "Linux";	    break;
      case ELFOSABI_SOLARIS:	abi = "Solaris";    break;
      case ELFOSABI_AIX:	abi = "Aix";	    break;
      case ELFOSABI_IRIX:	abi = "Irix";	    break;
      case ELFOSABI_FREEBSD:	abi = "FreeBSD";    break;
      case ELFOSABI_TRU64:	abi = "Tru64";	    break;
      case ELFOSABI_MODESTO:	abi = "Modesto";    break;
      case ELFOSABI_OPENBSD:	abi = "OpenBSD";    break;
      case ELFOSABI_ARM:	abi = "ARM";	    break;
      case ELFOSABI_STANDALONE: abi = NULL;	    break;
      default:
        PyErr_SetString(ElfError, "unknown ELF abi");
	return 1;
    }

    if (!gelf_getehdr(elf, &ehdr)) {
	PyErr_SetString(ElfError, "failed to get ELF header");
	return 1;
    }

    switch (ehdr.e_machine) {
      case EM_SPARC:	    insSet = "sparc";	    break;
      case EM_386:	    insSet = "x86";	    break;
      case EM_68K:	    insSet = "68k";	    break;
      case EM_MIPS:	    insSet = "mipseb";	    break;
      case EM_MIPS_RS3_LE:  insSet = "mipsel";	    break;
      case EM_PARISC:	    insSet = "parisc";	    break;
      case EM_960:	    insSet = "960";	    break;
      case EM_PPC:	    insSet = "ppc";	    break;
      case EM_PPC64:	    insSet = "ppc64";	    break;
      case EM_S390:	    insSet = "s390";	    break;
      case EM_ARM:	    insSet = "arm";	    break;
      case EM_IA_64:	    insSet = "ia64";	    break;
      case EM_X86_64:	    insSet = "x86-64";	    break;
      case EM_ALPHA:	    insSet = "alpha";	    break;
      default:
	PyErr_SetString(ElfError, "unknown machine type");
    }

    PyList_Append(reqList, Py_BuildValue("ss(ss)", "abi", class,
					 abi, insSet));

    while ((sect = elf_nextscn(elf, sect))) {
	if (!gelf_getshdr(sect, &shdr)) {
	    PyErr_SetString(ElfError, "error getting section header!");
	    return 1;
	}

	elf_getshstrndx (elf, &shstrndx);
	name = elf_strptr (elf, shstrndx, shdr.sh_name);

	if (!strcmp(name, ".dynamic")) {
	    data = elf_getdata(sect, NULL);

	    entries = shdr.sh_size / shdr.sh_entsize;
	    for (i = 0; i < entries; i++) {
		gelf_getdyn(data, i, &sym);
		if (sym.d_tag == DT_NEEDED) {
		    PyList_Append(reqList, Py_BuildValue("ss()", "soname", 
			   elf_strptr(elf, shdr.sh_link, sym.d_un.d_val)));
		} else if (sym.d_tag == DT_SONAME) {
		    PyList_Append(provList, Py_BuildValue("ss()", "soname", 
			   elf_strptr(elf, shdr.sh_link, sym.d_un.d_val)));
		}

	    }
	} else if (!strcmp(name, ".gnu.version_r")) {
	    if (shdr.sh_type != SHT_GNU_verneed) {
		PyErr_SetString(ElfError, 
			        "wrong type for section .gnu.version_r");
		return 1;
	    }

	    data = elf_getdata(sect, NULL);

	    i = shdr.sh_info;
	    idx = 0;
	    while (i--) {
		if (!gelf_getverneed(data, idx, &verneed)) {
		    PyErr_SetString(ElfError,
				    "failed to get version need info");
		    return 1;
		}

		libName = elf_strptr(elf, shdr.sh_link, verneed.vn_file);

		listIdx = idx + verneed.vn_aux;
		j = verneed.vn_cnt;
		while (j--) {
		    if (!gelf_getvernaux(data, listIdx, &veritem)) {
			PyErr_SetString(ElfError,
				        "failed to get version item");
			return 1;
		    }

		    PyList_Append(reqList, Py_BuildValue("ss(s)", "soname", 
			   libName,
			   elf_strptr(elf, shdr.sh_link, veritem.vna_name)));
		    listIdx += veritem.vna_next;
		}

		idx += verneed.vn_next;
	    }
	} else if (!strcmp(name, ".gnu.version_d")) {
	    if (shdr.sh_type != SHT_GNU_verdef) {
		PyErr_SetString(ElfError,
			        "wrong type for section .gnu.version_d");
		return 1;
	    }

	    data = elf_getdata(sect, NULL);

	    i = shdr.sh_info;
	    idx = 0;
	    while (i--) {
		if (!gelf_getverdef(data, idx, &verdef)) {
		    PyErr_SetString(ElfError,
				    "failed to get version def info");
		    return 1;
		}

		listIdx = idx + verdef.vd_aux;
		if (!gelf_getverdaux(data, listIdx, &verdefItem)) {
		    PyErr_SetString(ElfError,
				    "failed to get version def item");
		    return 1;
		}

		if (verdef.vd_flags & VER_FLG_BASE) {
		    verdBase = elf_strptr(elf, shdr.sh_link, 
					  verdefItem.vda_name);
		} else {
		    PyList_Append(provList, Py_BuildValue("ss(s)", "soname", 
			   verdBase,
			   elf_strptr(elf, shdr.sh_link, verdefItem.vda_name)));
		}

		listIdx += verdefItem.vda_next;
		j = verdef.vd_cnt - 1;
		while (j--) {
		    if (!gelf_getverdaux(data, listIdx, &verdefItem)) {
			PyErr_SetString(ElfError,
				        "failed to get version def item");
			return 1;
		    }

		    listIdx += verdefItem.vda_next;
		}

		idx += verdef.vd_next;
	    }
	}
    }

    return 0;
}

static PyObject * inspect(PyObject *self, PyObject *args) {
    PyObject * reqList, * provList;
    char * fileName;
    int fd;
    Elf * elf;
    int rc;
    char magic[4];

    if (!PyArg_ParseTuple(args, "s", &fileName))
	return NULL;

    fd = open(fileName, O_RDONLY);
    if (fd < 0) {
	PyErr_SetFromErrno(PyExc_IOError);
	return NULL;
    }

    if (read(fd, magic, sizeof(magic)) != 4) {
	close(fd);
	Py_INCREF(Py_None);
	return Py_None;
    }

    if (magic[0] != 0x7f || magic[1] != 0x45 || magic[2] != 0x4c ||
	magic[3] != 0x46) {
	close(fd);
	Py_INCREF(Py_None);
	return Py_None;
    }

    lseek(fd, 0, 0);

    elf = elf_begin(fd, ELF_C_READ_MMAP, NULL);
    if (!elf) {
	PyErr_SetString(ElfError, "error initializing elf file");
	return NULL;
    }

    reqList = PyList_New(0);
    provList = PyList_New(0);

    rc = doInspect(elf, reqList, provList);
    elf_end(elf);
    close(fd);

    if (rc) {
	/* didn't work */
	Py_DECREF(provList);
	Py_DECREF(reqList);
	return NULL;
    }

    /* worked */
    return Py_BuildValue("OO", reqList, provList);
}

PyMODINIT_FUNC
initelf(void)
{
    ElfError = PyErr_NewException("elf.error", NULL, NULL);
    Py_InitModule3("elf", ElfMethods, 
		   "provides access to elf shared library dependencies");
    elf_version(EV_CURRENT);

}
