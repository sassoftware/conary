<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
    ${html_header(pageTitle)}
    <body>
        <h2>${pageTitle}</h2>
        
        <pre class="error">${error}</pre>
        <p>Please go back and try again.</p>

        ${html_footer()}
    </body>
</html>
