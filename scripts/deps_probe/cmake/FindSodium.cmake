# fizz's config asks for Sodium but brings no find module. fizz links
# "sodium" by name, which resolves to -lsodium, so finding the library is
# enough.
find_library(Sodium_LIBRARY NAMES sodium)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Sodium DEFAULT_MSG Sodium_LIBRARY)
