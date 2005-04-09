<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
    ${html_header(pageTitle)}
    <body>
        <h1>Conary Repository</h1>

        <ul class="menu"><li class="highlighted">Error</li></ul>
        <ul class="menu submenu"> </ul>
        
        <div id="content">
            <pre class="error">${error}</pre>
            <p>Please go back and try again.</p>

            ${html_footer()}
        </div>
    </body>
</html>
