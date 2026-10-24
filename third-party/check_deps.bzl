def check_deps(prefix):
    """Fails unless the generated flags use folly built with GCC 12: the
    toolchain has no flags to match folly's GCC 11 layouts any more."""
    if prefix.startswith("/usr/local"):
        fail("third-party/generated/deps.bzl uses folly from {}, built with GCC 11; rerun scripts/gen_third_party.py".format(prefix))
