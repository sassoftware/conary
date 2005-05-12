<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
    ${html_header("Pick Trove")}
    <body>
        <h1>Conary Repository</h1>

        <ul class="menu"><li class="highlighted">Pick Trove</li></ul>
        <ul class="menu submenu"> </ul>

        <div id="content">
            <h2>Pick Trove</h2>
            <form action="chooseBranch" method="post">
                <p>
                    <div class="formHeader">Pick a trove:</div>
                    <select name="troveNameList" size="12" multiple="multiple" style="width: 50%;">
                        <option py:for="trove in troveList"
                                value="${trove}" py:content="trove"/>
                    </select>
                </p>
                <p><div class="formHeader">Or enter a trove name:</div><input type="text" name="troveName"/></p>
                <p><input type="submit" /></p>
                <p><input type="submit" value="Freshmeat" name="source" /></p>
            </form>

            ${html_footer()}
        </div>
    </body>
</html>
