from epydoc.apidoc import UNKNOWN
from epydoc import apidoc

def write_class(self, out, doc):
    """
    Write an HTML page containing the API documentation for the
    given class to C{out}.
    
    @param doc: A L{ClassDoc} containing the API documentation
    for the class that should be described.
    """
    longname = doc.canonical_name
    shortname = doc.canonical_name[-1]

    # Write the page header (incl. navigation bar & breadcrumbs)
    self.write_header(out, str(longname))
    self.write_navbar(out, doc)
    self.write_breadcrumbs(out, doc, self.url(doc))

    # Write the name of the class we're describing.
    if doc.is_type(): typ = 'Type'
    elif doc.is_exception(): typ = 'Exception'
    else: typ = 'Class'
    out('<!-- ==================== %s ' % typ.upper() +
        'DESCRIPTION ==================== -->\n')
    out('<h1 class="epydoc">%s %s</h1>' % (typ, shortname))
    out('<p class="nomargin-top">%s</p>\n' % self.pysrc_link(doc))

    if ((doc.bases not in (UNKNOWN, None) and len(doc.bases) > 0) or
        (doc.subclasses not in (UNKNOWN,None) and len(doc.subclasses)>0)):
        # Display bases graphically, if requested.
        if 'umlclasstree' in self._graph_types:
            self.write_class_tree_graph(out, doc, uml_class_tree_graph)
        elif 'classtree' in self._graph_types:
            self.write_class_tree_graph(out, doc, class_tree_graph)
            
        # Otherwise, use ascii-art.
        else:
            # Write the base class tree.
            if doc.bases not in (UNKNOWN, None) and len(doc.bases) > 0:
                out('<pre class="base-tree">\n%s</pre>\n\n' %
                    self.base_tree(doc))

            # Write the known subclasses
            if doc.subclasses in (UNKNOWN, None):
                subclasses = []
            else:
                subclasses = [ x for x in doc.subclasses if x.is_public ]
            if len(subclasses) > 0:
                out('<dl><dt>Known Subclasses:</dt>\n<dd>\n    ')
                out('  <ul class="subclass-list">\n')
                for i, subclass in enumerate(subclasses):
                    href = self.href(subclass, context=doc)
                    if self._val_is_public(subclass): css = ''
                    else: css = ' class="private"'
                    if i > 0: href = ', '+href
                    out('<li%s>%s</li>' % (css, href))
                out('  </ul>\n')
                out('</dd></dl>\n\n')

        out('<hr />\n')
    
    # If the class has a description, then list it.
    if doc.descr not in (None, UNKNOWN):
        out(self.descr(doc, 2)+'\n\n')

    # Write any standarad metadata (todo, author, etc.)
    if doc.metadata is not UNKNOWN and doc.metadata:
        out('<hr />\n')
    self.write_standard_fields(out, doc)

    # Write summary tables describing the variables that the
    # class defines.
    self.write_summary_table(out, "Nested Classes", doc, "class")
    self.write_summary_table(out, "Instance Methods", doc,
                             "instancemethod")
    self.write_summary_table(out, "Class Methods", doc, "classmethod")
    self.write_summary_table(out, "Static Methods", doc, "staticmethod")
    self.write_summary_table(out, "Class Variables", doc,
                             "classvariable")
    self.write_summary_table(out, "Instance Variables", doc,
                             "instancevariable")
    self.write_summary_table(out, "Properties", doc, "property")

    # Write a list of all imported objects.
    if self._show_imports:
        self.write_imports(out, doc)

    # Write detailed descriptions of functions & variables defined
    # in this class.
    # [xx] why group methods into one section but split vars into two?
    # seems like we should either group in both cases or split in both
    # cases.
    self.write_details_list(out, "Method Details", doc, "method")
    self.write_details_list(out, "Class Variable Details", doc,
                            "classvariable")
    self.write_details_list(out, "Instance Variable Details", doc,
                            "instancevariable")
    self.write_details_list(out, "Property Details", doc, "property")

    # Write the page footer (including navigation bar)
    self.write_navbar(out, doc)
    self.write_footer(out)


def _doc_or_ancestor_is_private(self, api_doc):
    name = api_doc.canonical_name
    for i in range(len(name), 0, -1):
        # Is it (or an ancestor) a private var?
        var_doc = self.docindex.get_vardoc(name[:i])
        if var_doc is not None and var_doc.is_public == False:
            return True
        # Is it (or an ancestor) a private module?
        val_doc = self.docindex.get_valdoc(name[:i])
        if (val_doc is not None and isinstance(val_doc, apidoc.ModuleDoc) and
            (val_doc.canonical_name[-1].startswith('_') or not val_doc.is_public)):
            return True
    return False

def write_module_tree_item(self, out, doc, package=None):
    # If it's a private variable, then mark its <li>.
    var = package and package.variables.get(doc.canonical_name[-1])
    priv = ((var is not None and var.is_public is False) or
            (var is None and doc.canonical_name[-1].startswith('_')))
    out('    <li%s> <strong class="uidlink">%s</strong>'
        % (priv and ' class="private"' or '', self.href(doc)))
    if doc.summary not in (None, UNKNOWN):
        out(': <em class="summary">'+
            self.description(doc.summary, doc, 8)+'</em>')
    if doc.submodules != UNKNOWN and doc.submodules:
        if priv: out('\n    <ul class="private">\n')
        else: out('\n    <ul>\n')
        for submodule in doc.submodules:
            if submodule.is_public:
                self.write_module_tree_item(out, submodule, package=doc)
        out('    </ul>\n')
    out('    </li>\n')

def write_module_list(self, out, doc):
    submodules = [ x for x in doc.submodules if x.is_public ]
    if len(submodules) == 0: return
    self.write_table_header(out, "summary", "Submodules")

    for group_name in doc.group_names():
        submodules = doc.submodule_groups[group_name]
        submodules = [ x for x in submodules if x.is_public ]
        if not submodules:
            continue
        if group_name:
            self.write_group_header(out, group_name)
        out('  <tr><td class="summary">\n'
            '  <ul class="nomargin">\n')
        for submodule in submodules:
            self.write_module_tree_item(out, submodule, package=doc)
        out('  </ul></td></tr>\n')
    out(self.TABLE_FOOTER+'\n<br />\n')

# monkey patch epydoc
from epydoc.docwriter.html import HTMLWriter

oldValIsPublic = HTMLWriter._val_is_public
def _val_is_public(self, valdoc):
    if hasattr(valdoc, 'is_public'):
        return valdoc.is_public
    else:
        return oldValIsPublic(self, valdoc)

HTMLWriter._doc_or_ancestor_is_private = _doc_or_ancestor_is_private
HTMLWriter._val_is_public = _val_is_public
HTMLWriter.write_module_tree_item = write_module_tree_item
HTMLWriter.write_module_list = write_module_list
HTMLWriter.write_class = write_class
