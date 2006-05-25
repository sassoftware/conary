<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml" 
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
    <?python
        # Copyright (c) 2005 rpath, Inc.
        #
        # This program is distributed under the terms of the Common Public License,
        # version 1.0. A copy of this license should have been distributed with this
        # source file in a file called LICENSE. If it is not present, the license
        # is always available at http://www.opensource.org/licenses/cpl.php.
        #
        # This program is distributed in the hope that it will be useful, but
        # without any warranty; without even the implied warranty of merchantability
        # or fitness for a particular purpose. See the Common Public License for
        # full details.
        
        from conary.fmtroves import TroveCategories, LicenseCategories
        source = metadata.getSource()
        if metadata.getVersion():
            # the only number that matters in the metadata version is the source revision
            versionStr = metadata.getVersion().split("-")[-1]
        else:
            versionStr = "Initial %s version" % source

        licenses = sorted(x for x in LicenseCategories.values() if "::" in x)
        categories = sorted(x for x in TroveCategories.values() if x.startswith('Topic') and '::' in x)
    ?>

    <!-- function to generate a selection, an input box, and add/remove button pair
         to manage a list of items in a selection. -->
    <div py:def="selectionEditor(itemName, items, ddItems = [])">
        <?python
        selectionName = "sel" + itemName
        fieldName = "new" + itemName
        ?>
        <select name="${selectionName}" id="${selectionName}" size="4" multiple="multiple" style="width: 100%;"
                onclick="javascript:setValue('${selectionName}', '${fieldName}')">
            <option py:for="item in items"
                    py:content="item" value="${item}"/>
        </select>
        <div style="padding: 4px 0px 4px 0px;">
            <input py:if="not ddItems" style="width: 75%;" type="text" name="${fieldName}" id="${fieldName}" />
            <select py:if="ddItems" style="width: 75%;" id="${fieldName}">
                <option py:for="item in ddItems"
                        py:content="item" value="${item}"/>
            </select>

            <input type="button" onclick="javascript:append('${selectionName}', '${fieldName}');" value="Add" />
            <input type="button" onclick="javascript:removeSelected('${selectionName}');" value="Remove" />
        </div>
    </div>

    <!-- source selection dropdown -->
    <select name="source" py:def="sourceSelect(source)">
        <option py:if="source == 'local'" selected="selected" value="local" py:content="'local'" />
        <option py:if="source != 'local'" value="local" py:content="'local'" />
        <option py:if="source == 'freshmeat'" selected="selected" value="freshmeat" py:content="'freshmeat'" />
        <option py:if="source != 'freshmeat'" value="freshmeat" py:content="'freshmeat'" />
    </select>

    <head/>
    <body>
        <div id="inner">
            <h4>${troveName}=${branch.asString().split("/")[-1]}</h4>
            <h4>Metadata revision: ${versionStr}</h4>

            <form method="post" action="updateMetadata">
                <table style="width: 60%;" cellpadding="10">
                    <tr>
                        <td style="width: 25%;" >Short Description:</td>
                        <td><input style="width: 100%;" type="text" name="shortDesc" value="${metadata.getShortDesc()}" /></td>
                    </tr>
                    <tr>
                        <td>Long Description:</td>
                        <td><textarea style="width: 100%;" name="longDesc" rows="4" cols="60">${metadata.getLongDesc()} </textarea></td>
                    </tr>
                    <tr>
                        <td>URLs:</td>
                        <td py:content="selectionEditor('Url', metadata.getUrls())"/>
                    </tr>
                    <tr>
                        <td>Licenses:</td>
                        <td py:content="selectionEditor('License', metadata.getLicenses(), licenses)"/>
                    </tr>
                    <tr>
                        <td>Categories:</td>
                        <td py:content="selectionEditor('Category', metadata.getCategories(), categories)"/>
                    </tr>
                    <tr><td>Source:</td><td>${sourceSelect(source)}</td></tr>
                </table>
                <p><button id="submitButton" onclick="javascript:updateMetadata();">Save Changes</button></p>
                <input type="hidden" name="branch" value="${branch.freeze()}" />
                <input type="hidden" name="troveName" value="${troveName}" />
            </form>

            <form method="post" action="getMetadata">
                <input type="hidden" name="branch" value="${branch.freeze()}" />
                <input type="hidden" name="troveName" value="${troveName}" />
                <input type="hidden" name="source" value="freshmeat" />
                <p><input type="submit" value="Fetch from Freshmeat" /></p>
                <p>Freshmeat project name: <input type="text" name="freshmeatName" value="${troveName[:-7]}" /></p>
            </form> 

            <form method="post" action="metadata">
                <input type="hidden" name="troveName" value="${troveName}" />
                <p><input type="submit" value="Cancel" /></p>
            </form>
        </div> 
    </body>
</html>
