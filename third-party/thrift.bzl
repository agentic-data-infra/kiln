# thrift_cpp_library: C++ code generated from a Thrift IDL file by fbthrift's
# compiler (thrift1, see third-party/BUCK), compiled into a cxx_library.
#
# Generated headers are included as "<package>/gen-cpp2/<name>", e.g.
# "kiln/rpc/gen-cpp2/MetadataService.h". thrift1 names its outputs after the
# IDL file and its services, so the services must be listed.

load("//third-party/generated:deps.bzl", "THRIFT1")

def _outputs(stem, services):
    srcs = [
        stem + "_constants.cpp",
        stem + "_data.cpp",
        stem + "_metadata.cpp",
        stem + "_sinit.cpp",
        stem + "_types.cpp",
        stem + "_types_binary.cpp",
        stem + "_types_compact.cpp",
        stem + "_types_serialization.cpp",
    ]
    headers = [
        stem + "_constants.h",
        stem + "_data.h",
        stem + "_metadata.h",
        stem + "_types.h",
        stem + "_types.tcc",
        stem + "_types_custom_protocol.h",
        stem + "_types_fwd.h",
    ]
    if services:
        headers += [
            stem + "_clients.h",
            stem + "_clients_fwd.h",
            stem + "_handlers.h",
        ]
    for service in services:
        srcs += [
            service + ".cpp",
            service + "AsyncClient.cpp",
            service + "_processmap_binary.cpp",
            service + "_processmap_compact.cpp",
        ]
        headers += [
            service + ".h",
            service + ".tcc",
            service + "AsyncClient.h",
            service + "_custom_protocol.h",
        ]
    return srcs, headers

def thrift_cpp_library(name, src, services = [], deps = [], visibility = None):
    stem = src.removesuffix(".thrift")
    srcs, headers = _outputs(stem, services)
    gen = name + "_gen"
    native.genrule(
        name = gen,
        srcs = [src],
        outs = {f: ["gen-cpp2/" + f] for f in srcs + headers},
        cmd = " ".join([
            THRIFT1,
            "-I /usr/local/include",
            "--gen mstch_cpp2:include_prefix=" + native.package_name(),
            "-o $OUT",
            "$SRCS",
        ]),
    )
    native.cxx_library(
        name = name,
        srcs = [":{}[{}]".format(gen, f) for f in srcs],
        header_namespace = native.package_name() + "/gen-cpp2",
        exported_headers = {f: ":{}[{}]".format(gen, f) for f in headers},
        exported_deps = ["//third-party:fbthrift"] + deps,
        visibility = visibility,
    )
