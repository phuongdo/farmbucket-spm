import Foundation
import FarmHashKit

@inline(__always)
func absInt64AsUInt64(_ x: Int64) -> UInt64 {
    let u = UInt64(bitPattern: x)
    return x >= 0 ? u : (~u &+ 1)
}

/// BigQuery parity:
/// MOD(ABS(FARM_FINGERPRINT(CONCAT(adid, 'test:1', 'salt-2025'))), 100)
func bigQueryBucket(adid: String) -> Int {
    let input = adid + "test:1" + "salt-2025"
    guard let data = input.data(using: .utf8) else { return 0 }
    let fp = FarmHashWrapper.fingerprint64(for: data)     // UInt64
    let signed = Int64(bitPattern: fp)                     // interpret as signed
    let abs64 = absInt64AsUInt64(signed)
    return Int(abs64 % 100)
}

// Demo CLI
let args = Array(CommandLine.arguments.dropFirst())
let ids = args.isEmpty ? ["1234-5678-ABCD", "A1B2C3D4", "xyz-987"] : args
for id in ids {
    print("adid=\(id) bucket=\(bigQueryBucket(adid: id))")
}
