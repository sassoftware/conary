<?xml version='1.0' encoding='UTF-8'?>
<?python #
from templates import library
?>

<html xmlns="http://www.w3.org/1999/xhtml" xmlns:py="http://naeblis.cx/ns/kid#">
    {library.html_header(pageTitle)}
    <body>
        <h2>{pageTitle}</h2>
        
        <pre class="error">{error}</pre>
        <p>Please go back and try again.</p>

        {library.html_footer()}
    </body>
</html>
