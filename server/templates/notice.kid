<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
    ${html_header(pageTitle)}
    <body>
        <h2>${pageTitle}</h2>

        <p>${message}</p>
        <p>Return to <a href="${url}">${link}</a></p>

        ${html_footer()}
    </body>
</html>
