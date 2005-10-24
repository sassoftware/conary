/*
 *
 * Copyright (c) 2004-2005 rPath, Inc.
 *
 * This program is distributed under the terms of the Common Public License,
 * version 1.0. A copy of this license should have been distributed with this
 * source file in a file called LICENSE. If it is not present, the license
 * is always available at http://www.opensource.org/licenses/cpl.php.
 *
 * This program is distributed in the hope that it will be useful, but
 * without any waranty; without even the implied warranty of merchantability
 * or fitness for a particular purpose. See the Common Public License for
 * full details.
 *
 */

#include <Python.h>

#include <gelf.h>
#include <libelf.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <dlfcn.h>
#include <sys/wait.h>

static PyObject * ElfError;

/* debugging aid */
#if defined(__i386__) || defined(__x86_64__)
# define breakpoint do {__asm__ __volatile__ ("int $03");} while (0)
#endif

static int doInspect(int fd, Elf * elf, PyObject * reqList,
		     PyObject * provList) {
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

    if (elf_kind(elf) == ELF_K_AR) {
	/* if it's an AR archive, recursively call doInspect for all
	   its members */
	Elf *nelf;
	int rc;
	Elf_Cmd command = ELF_C_READ;

	while ((nelf = elf_begin(fd, command, elf)) != NULL) {
	    Elf_Kind kind = elf_kind(nelf);
	    rc = 0;
	    if (kind == ELF_K_ELF || kind == ELF_K_AR)
		rc = doInspect(fd, nelf, reqList, provList);
	    command = elf_next(nelf);
	    if (elf_end(nelf) != 0) {
		PyErr_SetString(ElfError, "error freeing Elf structure");
		return 1;
	    }
	    if (rc)
		return rc;
	}
	return 0;
    }
    
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
      case EM_X86_64:	    insSet = "x86_64";	    break;
      case EM_ALPHA:	    insSet = "alpha";	    break;
      default:
	/* we'll live */
	return 0;
    }

    Py_INCREF(Py_None);
    PyDict_SetItem(reqList, Py_BuildValue("ss(ss)", "abi", class, abi, insSet),
		   Py_None);

    while ((sect = elf_nextscn(elf, sect))) {
	if (!gelf_getshdr(sect, &shdr)) {
	    PyErr_SetString(ElfError, "error getting section header!");
	    return 1;
	}

	elf_getshstrndx (elf, &shstrndx);
	name = elf_strptr (elf, shstrndx, shdr.sh_name);

	if (shdr.sh_type == SHT_NOBITS) {
	    /* this section has no data, skip it */
	    continue;
	}
	
	if (!strcmp(name, ".dynamic")) {
	    data = elf_getdata(sect, NULL);

	    entries = shdr.sh_size / shdr.sh_entsize;
	    for (i = 0; i < entries; i++) {
		gelf_getdyn(data, i, &sym);
		/* pull out DT_NEEDED for depdendencies and DT_SONAME
		   for provides.  Both use the same format so build the
		   value using the same code */
		if (sym.d_tag == DT_NEEDED || sym.d_tag == DT_SONAME) {
		    PyObject *val = Py_BuildValue("ss()", "soname", 
						  elf_strptr(elf, shdr.sh_link,
							     sym.d_un.d_val));
		    Py_INCREF(Py_None);
		    if (sym.d_tag == DT_NEEDED)
			PyDict_SetItem(reqList, val, Py_None);
		    else
			PyDict_SetItem(provList, val, Py_None);
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
		    PyObject *val;
		    if (!gelf_getvernaux(data, listIdx, &veritem)) {
			PyErr_SetString(ElfError,
				        "failed to get version item");
			return 1;
		    }

		    val = Py_BuildValue("ss(s)", "soname", libName,
					elf_strptr(elf, shdr.sh_link,
						   veritem.vna_name));
		    Py_INCREF(Py_None);
		    PyDict_SetItem(reqList, val, Py_None);
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
		    PyObject *val;
		    val = Py_BuildValue("ss(s)", "soname", verdBase,
					elf_strptr(elf, shdr.sh_link,
						   verdefItem.vda_name));
		    Py_INCREF(Py_None);
		    PyDict_SetItem(provList, val, Py_None);
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

/* returns a tuple of two lists, requires, provides or None
   if the file is not a valid ELF file or AR archive */
static PyObject * inspect(PyObject *self, PyObject *args) {
    PyObject * reqList, * provList, *robj;
    char * fileName;
    int fd;
    Elf * elf;
    int rc;
    Elf_Kind kind;

    if (!PyArg_ParseTuple(args, "s", &fileName))
	return NULL;

    fd = open(fileName, O_RDONLY);
    if (fd < 0) {
	PyErr_SetFromErrno(PyExc_IOError);
	return NULL;
    }

    elf = elf_begin(fd, ELF_C_READ, NULL);
    if (!elf) {
	close(fd);
	Py_INCREF(Py_None);
	return Py_None;
    }

    kind = elf_kind(elf);
    if (kind != ELF_K_AR && kind != ELF_K_ELF) {
	close(fd);
	elf_end(elf);
	Py_INCREF(Py_None);
	return Py_None;
    }
    
    reqList = PyDict_New();
    provList = PyDict_New();

    rc = doInspect(fd, elf, reqList, provList);
    elf_end(elf);
    close(fd);

    if (rc) {
	/* didn't work */
	Py_DECREF(provList);
	Py_DECREF(reqList);
	return NULL;
    }

    /* worked */
    robj = Py_BuildValue("OO", PyDict_Keys(reqList), PyDict_Keys(provList));
    Py_DECREF(provList);
    Py_DECREF(reqList);
    return robj;
}

static int isStripped(Elf * elf) {
    Elf_Scn * sect = NULL;
    GElf_Shdr shdr;
    
    while ((sect = elf_nextscn(elf, sect))) {
	if (!gelf_getshdr(sect, &shdr)) {
	    PyErr_SetString(ElfError, "error getting section header!");
	    return -1;
	}

	if (shdr.sh_type == SHT_SYMTAB) {
	    return 0;
	}
    }

    return 1;
}

static PyObject * stripped(PyObject *self, PyObject *args) {
    char * fileName;
    int fd;
    Elf * elf;
    int rc;

    if (!PyArg_ParseTuple(args, "s", &fileName))
	return NULL;

    fd = open(fileName, O_RDONLY);
    if (fd < 0) {
	PyErr_SetFromErrno(PyExc_IOError);
	return NULL;
    }

    lseek(fd, 0, 0);

    elf = elf_begin(fd, ELF_C_READ, NULL);
    if (!elf) {
	PyErr_SetString(ElfError, "error initializing elf file");
	return NULL;
    }

    rc = isStripped(elf);
    elf_end(elf);
    close(fd);

    if (rc == -1) {
	return NULL;
    } else if (rc) {
	Py_INCREF(Py_True);
	return Py_True;
    }

    Py_INCREF(Py_False);
    return Py_False;
}

static int doHasDebug(Elf * elf) {
    Elf_Scn * sect = NULL;
    GElf_Shdr shdr;
    size_t shstrndx;
    char * name;
    
    if (-1 == elf_getshstrndx (elf, &shstrndx)) {
	PyErr_SetString(ElfError, "error getting string table index!");
	return -1;
    }
    
    while ((sect = elf_nextscn(elf, sect))) {
	if (!gelf_getshdr(sect, &shdr)) {
	    PyErr_SetString(ElfError, "error getting section header!");
	    return -1;
	}

	if (shdr.sh_type == SHT_PROGBITS) {
	    if (!gelf_getshdr(sect, &shdr)) {
		PyErr_SetString(ElfError, "error getting section header!");
		return 1;
	    }

	    name = elf_strptr (elf, shstrndx, shdr.sh_name);
	    if (!strncmp(name, ".debug", 6)) {
		return 1;
	    }
	}
    }

    return 0;
}

static PyObject * hasDebug(PyObject *self, PyObject *args) {
    char * fileName;
    int fd;
    Elf * elf;
    int rc;

    if (!PyArg_ParseTuple(args, "s", &fileName))
	return NULL;

    fd = open(fileName, O_RDONLY);
    if (fd < 0) {
	PyErr_SetFromErrno(PyExc_IOError);
	return NULL;
    }

    lseek(fd, 0, 0);

    elf = elf_begin(fd, ELF_C_READ, NULL);
    if (!elf) {
	PyErr_SetString(ElfError, "error initializing elf file");
	return NULL;
    }

    rc = doHasDebug(elf);
    elf_end(elf);
    close(fd);

    if (rc == -1) {
	return NULL;
    } else if (rc) {
	Py_INCREF(Py_True);
	return Py_True;
    }

    Py_INCREF(Py_False);
    return Py_False;
}

static PyObject * hasUnresolvedSymbols(PyObject *self, PyObject *args) {
    char *fileName;
    void *handle;
    int p[2], child, status;
    PyObject *rc;

    if (!PyArg_ParseTuple(args, "s", &fileName))
	return NULL;

    /* set up a pipe to get any error string out of the child */
    if (-1 == pipe(p)) {
	PyErr_SetFromErrno(PyExc_IOError);
	return NULL;
    }
    /* we want to do the dlopen in a child process.  This isolates
       the caller from any nasty side effects like running ini/fini code
       from the dynamic library that's being loaded */
    child = fork();
    if (0 == child) {
	/* close the read side of the pipe */
	close(p[0]);

	/* use RTLD_NOW to discover any unresolved symbols */
	handle = dlopen(fileName, RTLD_NOW);
	if ((void *) 0 == handle) {
	    /* error, write the length of the error and the text
	       to the write side of the pipe */
	    const char *err = dlerror();
	    int len = strlen(err);
	    write(p[1], (void *) &len, sizeof(len));
	    write(p[1], err, strlen(err));
	    close(p[1]);
	    _exit(1);
	}
	dlclose(handle);
	close(p[1]);
	_exit(0);
    }
    /* wait for the child to exit, and collect the status */
    waitpid(child, &status, 0);
    close(p[1]);

    /* child exited with a non-0 status, so there are unresolved symbols */
    if (0 != WEXITSTATUS(status)) {
	/* there's an error, read it from the pipe */
	char *err;
	int len;
	read(p[0], (void *) &len, sizeof(len));
	err = malloc(len);
	if (NULL == err)
	    return PyErr_NoMemory();
	read(p[0], err, len);
	rc = PyString_FromStringAndSize(err, len);
	free(err);
    } else {
	/* child exited with a 0 return code, no unresolved symbols */
	Py_INCREF(Py_False);
	rc = Py_False;
    }
    /* close the read side and return */
    close(p[0]);
    return rc;
}

static PyObject *doGetRPATH(Elf * elf) {
    Elf_Scn * sect = NULL;
    Elf_Data * data;
    GElf_Dyn sym;
    GElf_Shdr shdr;
    size_t shstrndx;
    char * name;
    char * runpath = NULL;
    char * buf;
    int entries, i;

    while ((sect = elf_nextscn(elf, sect))) {
	if (!gelf_getshdr(sect, &shdr)) {
	    PyErr_SetString(ElfError, "error getting section header");
	    return NULL;
	}

	/* skip any section that isn't DYNAMIC */
	if (shdr.sh_type != SHT_DYNAMIC) {
	    continue;
	}

	if (-1 == elf_getshstrndx(elf, &shstrndx)) {
	    PyErr_SetString(ElfError, "error getting string table index");
	    return NULL;
	}

	name = elf_strptr(elf, shstrndx, shdr.sh_name);
	if (NULL == name) {
	    PyErr_SetString(ElfError, "error getting section name");
	    return NULL;
	}

	/* strange. a DYNAMIC section that isn't named .dynamic.
	   better skip it */
	if (strcmp(name, ".dynamic"))
	    continue;

	data = elf_getdata(sect, NULL);
	entries = shdr.sh_size / shdr.sh_entsize;
	for (i = 0; i < entries; i++) {
	    gelf_getdyn(data, i, &sym);
	    /* skip any tag that isn't RPATH or RUNPATH */
	    if (sym.d_tag != DT_RPATH && sym.d_tag != DT_RUNPATH)
		continue;

	    buf = elf_strptr(elf, shdr.sh_link, sym.d_un.d_val);
	    if (NULL != runpath && strcmp(runpath, buf)) {
		/* this should never happen */
		PyErr_SetString(ElfError, "RPATH/RUNPATH entries conflict");
		return NULL;
	    }
	    runpath = buf;
	}
    }
    if (NULL != runpath)
	return PyString_FromString(runpath);
    else {
	Py_INCREF(Py_None);
	return Py_None;
    }
}

static PyObject * getRPATH(PyObject *self, PyObject *args) {
    char * fileName;
    int fd;
    Elf * elf;
    PyObject *rc;

    if (!PyArg_ParseTuple(args, "s", &fileName))
	return NULL;

    fd = open(fileName, O_RDONLY);
    if (fd < 0) {
	PyErr_SetFromErrno(PyExc_IOError);
	return NULL;
    }

    elf = elf_begin(fd, ELF_C_READ, NULL);
    if (!elf) {
	PyErr_SetString(ElfError, "error initializing elf file");
	return NULL;
    }

    rc = doGetRPATH(elf);
    elf_end(elf);
    close(fd);

    return rc;
}

static PyObject * getType(PyObject *self, PyObject *args) {
    char * fileName;
    int fd;
    Elf * elf;
    GElf_Ehdr ehdr;
    PyObject *rc;

    if (!PyArg_ParseTuple(args, "s", &fileName))
	return NULL;

    fd = open(fileName, O_RDONLY);
    if (fd < 0) {
	PyErr_SetFromErrno(PyExc_IOError);
	return NULL;
    }

    elf = elf_begin(fd, ELF_C_READ, NULL);
    if (!elf) {
	PyErr_SetString(ElfError, "error initializing elf file");
	return NULL;
    }

    if (!gelf_getehdr(elf, &ehdr)) {
	PyErr_SetString(ElfError, "failed to get ELF header");
	return NULL;
    }

    rc = PyLong_FromLong(ehdr.e_type);

    elf_end(elf);
    close(fd);

    return rc;
}


static PyMethodDef ElfMethods[] = {
    { "inspect", inspect, METH_VARARGS, 
	"inspect an ELF file for dependency information" },
    { "stripped", stripped, METH_VARARGS, 
	"returns whether or not an ELF file has been stripped" },
    { "hasDebug", hasDebug, METH_VARARGS, 
	"returns whether or not an ELF file has debugging info" },
    { "hasUnresolvedSymbols", hasUnresolvedSymbols, METH_VARARGS,
	"returns whether or not an ELF file has unresolved symbols" },
    { "getRPATH", getRPATH, METH_VARARGS,
        "returns the RPATH or RUNPATH set in an ELF file (if any)" },
    { "getType", getType, METH_VARARGS,
        "returns the ELF type of the file (elf.ET_EXEC, elf.ET_DYN, etc)" },
    { NULL, NULL, 0, NULL }
};


#define ADD_CONST(name) \
PyModule_AddObject(m, #name, PyLong_FromLong(name));

PyMODINIT_FUNC
initelf(void)
{
    PyObject* m;

    ElfError = PyErr_NewException("elf.error", NULL, NULL);
    m = Py_InitModule3("elf", ElfMethods, 
                       "provides access to elf shared library dependencies");

    ADD_CONST(ET_NONE);
    ADD_CONST(ET_REL);
    ADD_CONST(ET_EXEC);
    ADD_CONST(ET_DYN);
    ADD_CONST(ET_CORE);

    elf_version(EV_CURRENT);

}
