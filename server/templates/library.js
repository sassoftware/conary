function append(selId, inputId) {
    sel = document.getElementById(selId);
    text = document.getElementById(inputId).value;

    if(text != "") {
        sel.options[sel.length] = new Option(text, text);
        document.getElementById(inputId).value="";
    }
}

function remove(selId) {
    sel = document.getElementById(selId);
    sel.remove(sel.selectedIndex);
}

function selectAll(selId) {
    sel = document.getElementById(selId);
    for (i=0; i < sel.length; i++) {
        sel.options[i].selected = true;
    }
}

function setValue(selId, entryId) {
    sel = document.getElementById(selId);
    entry = document.getElementById(entryId);
    entry.value = sel.options[sel.selectedIndex].value;
}

function updateMetadata() {
    selectAll('selUrl');
    selectAll('selLicense');
    selectAll('selCategory');
    document.getElementById('submitButton').submit();
}
