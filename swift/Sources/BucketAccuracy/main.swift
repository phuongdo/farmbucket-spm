import Foundation
import FarmHashKit

@inline(__always)
func absInt64AsUInt64(_ x: Int64) -> UInt64 {
    let u = UInt64(bitPattern: x)
    return x >= 0 ? u : (~u &+ 1)
}

func bigQueryBucket(adid: String) -> Int {
    let input = adid + "test:1" + "salt-2025"
    guard let data = input.data(using: .utf8) else { return 0 }
    let fp = FarmHashWrapper.fingerprint64(for: data)
    let signed = Int64(bitPattern: fp)
    let abs64 = absInt64AsUInt64(signed)
    return Int(abs64 % 100)
}

func run() {
    let args = Array(CommandLine.arguments.dropFirst())
    let csvPath = args.first ?? "test.csv"
    let csvURL = URL(fileURLWithPath: csvPath)

    guard let content = try? String(contentsOf: csvURL, encoding: .utf8) else {
        fputs("Failed to read CSV at \(csvURL.path)\n", stderr)
        exit(1)
    }

    var lines = content.split(whereSeparator: { $0.isNewline })
    if lines.isEmpty {
        print("No rows to evaluate.")
        return
    }

    let header = lines.removeFirst()
    print("Using CSV header: \(header)")

    var total = 0
    var matches = 0

    for line in lines {
        let parts = line.split(separator: ",").map(String.init)
        guard parts.count >= 2,
              let adid = parts.first,
              let expected = Int(parts.last ?? "") else {
            continue
        }
        let predicted = bigQueryBucket(adid: adid)
        if predicted == expected {
            matches += 1
        }
        total += 1
        print("adid=\(adid) expected=\(expected) predicted=\(predicted)")
    }

    if total == 0 {
        print("No valid rows processed.")
        return
    }

    let accuracy = Double(matches) / Double(total) * 100
    print("\nMatches: \(matches)/\(total)  Accuracy: \(String(format: "%.2f", accuracy))%")
}

run()
