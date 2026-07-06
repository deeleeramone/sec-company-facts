from __future__ import annotations

import json
import sys


def _files(manifest: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for table in (manifest.get("tables") or {}).values():
        for f in table.get("files", []):
            out[f["name"]] = f["sha256"]
    return out


def main() -> int:
    new = json.load(open(sys.argv[1]))
    try:
        old = json.load(open(sys.argv[2]))
    except Exception:
        old = {}
    new_files, old_files = _files(new), _files(old)
    for name, sha in new_files.items():
        if old_files.get(name) != sha:
            print("UP\t" + name)
    for name in old_files:
        if name not in new_files:
            print("RM\t" + name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
