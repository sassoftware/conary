<?xml version='1.0' encoding='UTF-8'?>
<?python #
from templates import library
?>

<html xmlns="http://www.w3.org/1999/xhtml" xmlns:py="http://naeblis.cx/ns/kid#">
    {library.html_header(pageTitle)}
    <body>
        <h2>{pageTitle}</h2>

        <form method="post" action="getMetadata">
            <input type="hidden" name="troveName" value="{troveName}" />
            Choose a branch:

            <select name="branch" id="branch">
                <option py:for="branch in branches"
                        py:content="branch.label().asString().split('@')[-1]"
                        value="{branch.freeze()}"/>
            </select>

            <input type="submit" />
        </form>

        {library.html_footer()}
    </body>
</html>
