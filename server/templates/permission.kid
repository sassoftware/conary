<?xml version='1.0' encoding='UTF-8'?>
<?python #
import kid; kid.enable_import()
from templates import library
?>

<html xmlns="http://www.w3.org/1999/xhtml" xmlns:py="http://naeblis.cx/ns/kid#">
    <!-- creates a selection dropdown based on a dict, optionally adding an ALL
         option at the top of the list. items = {value: "text"} -->
    <select py:def="makeSelect(elementName, items, all = False)"
            name="{elementName}">
        <option py:if="all" value="">ALL</option>
        <option py:for="value, text in sorted(items.items(), cmp=lambda x, y: cmp(x[1], y[1]))"
                py:content="text" value="{value}"/>
    </select>

    {library.html_header(pageTitle)}
    <body>
        <h2>{pageTitle}</h2>

        <form method="post" action="addPerm">
            <table class="add-perm">
                <tr>
                    <td id="header">Group:</td>
                    <td py:content="makeSelect('group', groups)"/>
                </tr>
                <tr>
                    <td id="header">Label:</td>
                    <td py:content="makeSelect('label', labels, all = True)"/>
                </tr>
                <tr>
                    <td id="header">Trove:</td>
                    <td py:content="makeSelect('item', items, all = True)"/>
                </tr>
                <tr>
                    <td id="header" rowspan="3">Options</td>
                    <td><input type="checkbox" name="write" /> Write access</td>
                </tr>
                <tr>
                    <td><input type="checkbox" name="capped" /> Capped</td>
                </tr>
                <tr>
                    <td><input type="checkbox" name="admin" /> Admin access</td>
                </tr>

            </table>
            <p><input type="submit" value="Add"/></p>
        </form>

        {library.html_footer()}
    </body>
</html>
