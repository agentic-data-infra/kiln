// The service between Kiln clients and the metadata server. Compiled by
// thrift1 into kiln/rpc/gen-cpp2 (see BUCK).
//
// Only ping for now: it proves code generation and coroutine (co_*) clients
// and handlers.

package "kiln.dev/rpc"

namespace cpp2 kiln.rpc.thrift

service MetadataService {
  /// Returns 'message'.
  string ping(1: string message);
}
