"""
Test script: Simulate old files in output directories and verify cleanup logic.
"""
import os
import sys
import time
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Create a temp output dir structure to simulate
TEST_BASE = ROOT / "tmp" / "_test_cleanup"

DIRS = [
    TEST_BASE / "understat" / "output" / "EPL",
    TEST_BASE / "sofascore" / "output" / "EPL",
]

def setup():
    """Create fake files: some old (>7 days), some recent, one large."""
    for d in DIRS:
        d.mkdir(parents=True, exist_ok=True)

    old_time = time.time() - (10 * 86400)  # 10 days ago
    recent_time = time.time() - (2 * 86400)  # 2 days ago

    files_created = []

    # Old files (should be deleted)
    for i in range(5):
        f = DIRS[0] / f"old_match_{i}.csv"
        f.write_text(f"fake,data,row_{i}")
        os.utime(f, (old_time, old_time))
        files_created.append(("OLD", f))

    # Recent files (should NOT be deleted)
    for i in range(3):
        f = DIRS[1] / f"recent_match_{i}.csv"
        f.write_text(f"fake,data,row_{i}")
        os.utime(f, (recent_time, recent_time))
        files_created.append(("RECENT", f))

    # Large file (should trigger warning, but NOT deleted since it's recent)
    large = DIRS[1] / "large_stats.json"
    large.write_text("x" * (55 * 1024 * 1024))  # 55MB
    os.utime(large, (recent_time, recent_time))
    files_created.append(("LARGE+RECENT", large))

    # Non-matching extension (should be ignored)
    py_file = DIRS[0] / "script.py"
    py_file.write_text("print('hello')")
    os.utime(py_file, (old_time, old_time))
    files_created.append(("IGNORED_EXT", py_file))

    return files_created


def run_cleanup():
    """Run the actual cleanup logic (extracted inline for testing)."""
    RETENTION_DAYS = 7
    LARGE_THRESHOLD_MB = 50
    cutoff = time.time() - (RETENTION_DAYS * 86400)
    
    output_dirs = [
        TEST_BASE / "understat" / "output",
        TEST_BASE / "sofascore" / "output",
    ]
    
    total_files = 0
    total_bytes = 0

    for dir_path in output_dirs:
        if not dir_path.exists():
            continue
        dir_files = 0
        dir_bytes = 0

        for f in dir_path.rglob("*"):
            if not f.is_file():
                continue
            if f.suffix.lower() not in (".csv", ".json", ".html"):
                continue
            try:
                fstat = f.stat()
                size = fstat.st_size
                size_mb = size / (1024 * 1024)

                if size_mb > LARGE_THRESHOLD_MB:
                    print(f"  ⚠ Large file detected: {f.relative_to(TEST_BASE)} ({size_mb:.1f} MB)")

                if fstat.st_mtime < cutoff:
                    f.unlink()
                    dir_files += 1
                    dir_bytes += size
            except Exception as exc:
                print(f"  Error: {f} — {exc}")

        if dir_files > 0:
            print(f"  [DiskCleanup] {dir_path.relative_to(TEST_BASE)}: {dir_files} files deleted, {dir_bytes / (1024*1024):.1f} MB freed")
        total_files += dir_files
        total_bytes += dir_bytes

    print(f"\n  [DiskCleanup] TOTAL: {total_files} files, {total_bytes / (1024*1024):.1f} MB freed")
    return total_files


def verify(files_created, deleted_count):
    """Verify expectations."""
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    errors = 0

    for label, f in files_created:
        exists = f.exists()
        if label == "OLD":
            if exists:
                print(f"  ❌ FAIL: {f.name} should have been DELETED but still exists")
                errors += 1
            else:
                print(f"  ✅ OK: {f.name} was deleted (old file)")
        elif label in ("RECENT", "LARGE+RECENT"):
            if not exists:
                print(f"  ❌ FAIL: {f.name} should have been KEPT but was deleted")
                errors += 1
            else:
                print(f"  ✅ OK: {f.name} was kept (recent file)")
        elif label == "IGNORED_EXT":
            if not exists:
                print(f"  ❌ FAIL: {f.name} (.py) should have been IGNORED but was deleted")
                errors += 1
            else:
                print(f"  ✅ OK: {f.name} was ignored (non-matching extension)")

    if deleted_count == 5:
        print(f"\n  ✅ Deleted count correct: {deleted_count}")
    else:
        print(f"\n  ❌ Deleted count wrong: expected 5, got {deleted_count}")
        errors += 1

    if errors == 0:
        print("\n🎉 ALL TESTS PASSED!")
    else:
        print(f"\n💥 {errors} TEST(S) FAILED!")

    # Cleanup test dir
    import shutil
    shutil.rmtree(TEST_BASE, ignore_errors=True)


if __name__ == "__main__":
    print("=" * 60)
    print("🧪 TESTING DISK CLEANUP LOGIC")
    print("=" * 60 + "\n")

    print("Setting up fake files...")
    files = setup()
    print(f"Created {len(files)} test files.\n")

    print("Running cleanup...\n")
    count = run_cleanup()

    verify(files, count)
