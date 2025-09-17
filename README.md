# FarmBucket-SPM

FarmBucket-SPM provides reproducible BigQuery bucket assignments by exposing Google FarmHash Fingerprint64 in multiple environments. The toolkit lets any team apply BigQuery's bucket rule

```
MOD(ABS(FARM_FINGERPRINT(CONCAT(adid, 'test:1', 'salt-2025'))), 100)
```

from native Swift apps or external systems that rely on Python or Java utilities.

## Purpose

- Guarantee that client-side or server-side code assigns users to the exact same buckets as BigQuery.
- Offer a lightweight Swift Package that can be dropped into iOS/macOS projects.
- Supply ready-to-run verification tools (Swift CLI, Python script, Java program) for analytics or data engineering teams.

## What You Get

- `FarmHashKit`: Objective-C++ wrapper around the canonical FarmHash C++ sources, exported as a Swift Package library target.
- `FarmBucketDemo`: Swift executable that exercises the library and prints bucket IDs for sample ADIDs.
- `BucketAccuracy`: Swift executable that replays CSV exports and reports parity versus BigQuery.
- Companion scripts in `python/` and `java/` that mirror the exact hashing logic for non-Swift platforms.

## Swift Integration

Add the package in Xcode (File → Add Packages…) or reference the Git URL from `Package.swift`. Once added:

```swift
import FarmHashKit

let adid = "abcdef0123456789"
let payload = adid + "test:1" + "salt-2025"
let bucket = Int(FarmHashWrapper.fingerprint64(for: Data(payload.utf8)).magnitude % 100)
```

Use the resulting `bucket` to align feature tests, marketing cohorts, or experiment rollouts with BigQuery pipelines.

## Command-Line Verification

Compile once, then replay CSV exports generated from BigQuery.

```bash
swift build
swift run FarmBucketDemo 111 222 333
swift run BucketAccuracy path/to/export.csv   # defaults to ./test.csv when no path is given
```

`test.csv` mirrors the structure returned by

## Python Utility

```bash
python3 py/bucket_accuracy.py             # defaults to ./test.csv
python3 py/bucket_accuracy.py path/to/export.csv
```

The script ports FarmHash Fingerprint64 directly, making it easy to validate data pipelines or to embed the function in ETL jobs without depending on Swift.

## Java Utility

```bash
javac java/BucketAccuracy.java
java -cp java BucketAccuracy                 # defaults to ./test.csv
java -cp java BucketAccuracy path/to/export.csv
```

This standalone class mirrors the same hashing primitives and is suitable for JVM services that need to compute the bucket during request handling or batch processing.

## CSV Expectations

- First column: `adid` (string).
- Optional middle columns are ignored.
- Final column: expected bucket as an integer in `[0, 99]`.

## Developing Further

- The repository vendors the official FarmHash sources (Apache 2.0) under `Sources/FarmHashKit/vendor/FarmHash`.
- Swift tooling currently requires agreeing to the Xcode command-line license before running tests or `git status` on fresh machines (`sudo xcodebuild -license`).
- Contributions should keep hashing logic byte-for-byte aligned with Google's FarmHash to preserve parity.

Questions or integration requests? Reach out to the data platform team to coordinate new language ports or salt changes.
