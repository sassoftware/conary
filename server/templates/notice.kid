<?xml version='1.0' encoding='UTF-8'?>
<?python #
from templates import library
?>

<html xmlns="http://www.w3.org/1999/xhtml" xmlns:py="http://naeblis.cx/ns/kid#">
    {library.html_header(pageTitle)}
    <body>
        <h2>{pageTitle}</h2>

        <p>{message}</p>
        <p>Return to <a href="{url}">{link}</a></p>

        {library.html_footer()}
    </body>
</html>
