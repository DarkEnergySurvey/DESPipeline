"""
    .. _filemgmt-filemgmt-defs:

    **filemgmt_defs**
    -----------------

    Constants used in file management code

"""

# lower case because appears as wcl section and wcl sections are converted to lowercase
META_HEADERS = 'h'
META_COMPUTE = 'c'
META_WCL = 'w'
META_COPY = 'p'
META_REQUIRED = 'r'
META_OPTIONAL = 'o'

FILETYPE_METADATA = 'filetype_metadata'
FILE_HEADER_INFO = 'file_header'

USE_HOME_ARCHIVE_INPUT = 'use_home_archive_input'
USE_HOME_ARCHIVE_OUTPUT = 'use_home_archive_output'

FM_PREFER_UNCOMPRESSED = [None, '.fz', '.gz']
FM_PREFER_COMPRESSED = ['.fz', '.gz', None]
FM_UNCOMPRESSED_ONLY = [None]
FM_COMPRESSED_ONLY = ['.fz', '.gz']

FM_EXIT_SUCCESS = 0
FM_EXIT_FAILURE = 1
FW_MSG_ERROR = 3
FW_MSG_WARN = 2
FW_MSG_INFO = 1

REQUIRED = 'REQ'
OPTIONAL = 'OPT'

# 1024**4
TB = 1099511627776.0
# 1024**3
GB = 1073741824.0
# 1024**2
MB = 1048576.0
# 1024**1
KB = 1024.0
