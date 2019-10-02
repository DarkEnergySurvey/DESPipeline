.. _serviceaccessDescription:

Service Access Files
====================

Service access files are used to store authentication credentials for processes such as
database access and file transport via http. These files are of the format used by
`ConfigParser <https://docs.python.org/2/library/configparser.html>`_ ::

    [file-http]
    user = username
    passwd = userPassword

In the pipeline's nomenclature ``file-http`` is the section name. When parsed
a dictionary containing the entries will be returned. In this example you would
get

.. code-block:: python

    {'user': 'username',
     'passwd': 'userPassword'}

An individual file can contain more than one section, but when parsed only the
requested section is returned.

Since service access files contain user credentials they are required to have restictive
permissions. The only allowable permissions are owner read and write and group read.
