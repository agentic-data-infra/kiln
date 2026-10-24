# Build options, set in .buckconfig's [kiln] section or with -c kiln.<name>=.

def _flag(name):
    return read_config("kiln", name, "false") == "true"

# RDMA data path between clients and storage servers. No code behind it yet.
KILN_RDMA = _flag("rdma")

# S3 as a storage tier. No code behind it yet.
KILN_S3 = _flag("s3")

# Preprocessor flags that tell the code which options are on.
OPTION_FLAGS = [
    "-DKILN_RDMA={}".format(1 if KILN_RDMA else 0),
    "-DKILN_S3={}".format(1 if KILN_S3 else 0),
]
