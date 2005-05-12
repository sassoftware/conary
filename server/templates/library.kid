<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#">

    <!-- define the HTML header -->
    <head py:def="html_header(title)">
        <title>${title}</title>
        <link rel="stylesheet" type="text/css" href="${cfg.staticPath}/css/common.css" />
        <link rel="stylesheet" type="text/css" href="${cfg.staticPath}/css/repository.css" />
        <script language="javascript1.2" src="${cfg.staticPath}/javascript/library.js"/>
        <script language="javascript1.2" src="${cfg.staticPath}/javascript/repository.js"/>
    </head>

    <!-- define the HTML footer -->
    <div py:def="html_footer">
        <hr />
        <span class="copyright"><b>Conary Repository Server</b> Copyright &#169; 2005 <a href="http://www.rpath.com/">rpath, Inc.</a></span>
    </div>
</html>
