#!/usr/bin/env python3

import pathlib
import re
import sys
import zipfile


def _read_wheel_metadata(whl_path: pathlib.Path) -> tuple[str, str, str]:
    with zipfile.ZipFile(whl_path, "r") as zf:
        dist_info_dirs = sorted(
            {
                pathlib.PurePosixPath(name).parts[0]
                for name in zf.namelist()
                if ".dist-info/" in name
            }
        )
        if not dist_info_dirs:
            raise RuntimeError(f"no dist-info directory found in {whl_path}")

        dist_info = dist_info_dirs[0]
        wheel_text = zf.read(f"{dist_info}/WHEEL").decode("utf-8")
        metadata_text = zf.read(f"{dist_info}/METADATA").decode("utf-8")

    version = ""
    name = ""
    for line in metadata_text.splitlines():
        if line.startswith("Name: "):
            name = line.split(": ", 1)[1].strip()
        elif line.startswith("Version: "):
            version = line.split(": ", 1)[1].strip()

    tag = ""
    for line in wheel_text.splitlines():
        if line.startswith("Tag: "):
            tag = line.split(": ", 1)[1].strip()
            break

    if not name or not version or not tag:
        raise RuntimeError(f"failed to parse wheel metadata from {whl_path}")

    normalized_name = re.sub(r"[-_.]+", "_", name)
    return normalized_name, version, tag


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: normalize_wheel_filename.py <wheel>")

    whl_path = pathlib.Path(sys.argv[1]).resolve()
    if not whl_path.exists():
        raise SystemExit(f"wheel not found: {whl_path}")

    name, version, tag = _read_wheel_metadata(whl_path)
    normalized = whl_path.with_name(f"{name}-{version}-{tag}.whl")
    if normalized != whl_path:
        whl_path.replace(normalized)
    print(normalized)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
