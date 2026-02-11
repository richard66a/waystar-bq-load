# .NET Upstream Changes Required for GCP Pipeline

This pipeline expects specific behaviors from the upstream .NET uploader. Apply the following changes to prevent missed files, parsing ambiguity, and dedupe issues.

## 1) Enforce GCS upload path

**Requirement:** Upload files to `logs/<filename>.json` (no nested folders). The external table reads `gs://<bucket>/logs/*.json` only.

**Behavior:**
- Use `Path.GetFileName(filePath)` and build the object path as `logs/{filename}`.

## 2) Emit UTC `EventDt` with trailing `Z`

**Requirement:** Serialize `EventDt` in UTC ISO‑8601 with a trailing `Z`, e.g. `2026-02-10T21:10:11.123Z`.

**Suggested pattern:**
- `DateTimeOffset.UtcNow.ToString("o")` (guarantees a `Z` suffix for UTC).

## 3) Deterministic `hash_code` (INT64)

**Requirement:** Avoid `.NET GetHashCode()` (non‑deterministic). Use a stable 64‑bit hash of a canonical string:

```
EventDt|Source|Filename|Bytes|UserName
```

**Suggested implementation (SHA256 → int64):**
- Compute SHA256 over UTF‑8 bytes.
- Take the first 8 bytes as a signed 64‑bit integer (big‑endian recommended).

**Alternative:** Use a stable 64‑bit hash library such as xxHash64 and store the signed result as `INT64`.

## Minimal C# Example

```csharp
using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

public static long ComputeHashCode(string eventDt, string source, string filename, long bytes, string userName)
{
    var canonical = $"{eventDt}|{source}|{filename}|{bytes}|{userName}";
    using var sha = SHA256.Create();
    var digest = sha.ComputeHash(Encoding.UTF8.GetBytes(canonical));
    var value = BinaryPrimitives.ReadInt64BigEndian(digest.AsSpan(0, 8));
    return value;
}

public static string FormatEventDtUtc(DateTimeOffset dt)
{
    return dt.UtcDateTime.ToString("o"); // ISO-8601 with Z
}
```

## Suggested Unit Tests (xUnit)

```csharp
[Fact]
public void HashCode_IsDeterministic()
{
    var dt = "2026-02-10T21:10:11.123Z";
    var a = ComputeHashCode(dt, "FTP-SERVER-01", "/uploads/a.txt", 2048, "12345");
    var b = ComputeHashCode(dt, "FTP-SERVER-01", "/uploads/a.txt", 2048, "12345");
    Assert.Equal(a, b);
}

[Fact]
public void EventDt_IsUtcZ()
{
    var formatted = FormatEventDtUtc(DateTimeOffset.UtcNow);
    Assert.EndsWith("Z", formatted);
}
```

## Verification Steps

- Upload a file and verify it appears under `gs://<bucket>/logs/<filename>.json`.
- Run the ETL and confirm `processed_files.status = SUCCESS`.
- Confirm `event_dt` is parsed correctly in BigQuery.
- Confirm `hash_code` is consistent across re‑uploads of the same data.
