<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
    ${html_header(pageTitle)}
    <body>
        <h1>Conary Repositry</h1>
        <ul class="menu"><li class="highlighted">${pageTitle}</li></ul>
        <ul class="menu submenu"> </ul>

        <div id="content">
            <p>${message}</p>
            <p>Return to <a href="${url}">${link}</a></p>

            ${html_footer()}
        </div>
    </body>
</html>
