<?xml version='1.0' encoding='UTF-8'?>
<?python #
from templates import library
?>

<html xmlns="http://www.w3.org/1999/xhtml" xmlns:py="http://naeblis.cx/ns/kid#">
    {library.html_header(pageTitle)}
    <body>
        <h2>{pageTitle}</h2>

        <form action="chooseBranch" method="post">
            <p>
                <div class="formHeader">Pick a trove:</div>
                <select name="troveNameList" size="12" multiple="multiple" style="width: 50%;">
                    <option py:for="trove in troveList"
                            value="{trove}" py:content="trove"/>
                </select>
            </p>
            <p><div class="formHeader">Or enter a trove name:</div><input type="text" name="troveName"/></p>
            <p><input type="submit" /></p>
            <p><input type="submit" value="Freshmeat" name="source" /></p>
        </form>

        {library.html_footer()}
    </body>
</html>
