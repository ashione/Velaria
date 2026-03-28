#!/usr/bin/env python3

import base64
import csv
import hashlib
import pathlib
import shutil
import sys
import tempfile
import zipfile


def _read_text(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def _hash_bytes(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return "sha256=" + base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def main() -> int:
    if len(sys.argv) != 8:
        raise SystemExit(
            "usage: build_native_wheel.py <pure_whl> <native_so> <python_tag.txt> "
            "<abi_tag.txt> <platform_tag.txt> <distribution> <out_whl>"
        )

    pure_whl = pathlib.Path(sys.argv[1])
    native_so = pathlib.Path(sys.argv[2])
    python_tag = _read_text(pathlib.Path(sys.argv[3]))
    abi_tag = _read_text(pathlib.Path(sys.argv[4]))
    platform_tag = _read_text(pathlib.Path(sys.argv[5]))
    distribution = sys.argv[6]
    out_whl = pathlib.Path(sys.argv[7])

    with tempfile.TemporaryDirectory(prefix="velaria-wheel-") as tmp:
        tmpdir = pathlib.Path(tmp)
        wheel_root = tmpdir / "wheel"
        with zipfile.ZipFile(pure_whl, "r") as zf:
            zf.extractall(wheel_root)

        dist_info = next(wheel_root.glob("*.dist-info"))
        package_dir = wheel_root / distribution
        package_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(native_so, package_dir / "_velaria.so")

        wheel_metadata = dist_info / "WHEEL"
        lines = wheel_metadata.read_text(encoding="utf-8").splitlines()
        new_lines = []
        for line in lines:
            if line.startswith("Root-Is-Purelib:"):
                new_lines.append("Root-Is-Purelib: false")
            elif line.startswith("Tag:"):
                continue
            else:
                new_lines.append(line)
        new_lines.append(f"Tag: {python_tag}-{abi_tag}-{platform_tag}")
        wheel_metadata.write_text("\n".join(new_lines) + "\n", encoding="utf-8")

        record_path = dist_info / "RECORD"
        records = []
        for path in sorted(wheel_root.rglob("*")):
            if path.is_dir():
                continue
            rel = path.relative_to(wheel_root).as_posix()
            if rel == record_path.relative_to(wheel_root).as_posix():
                records.append((rel, "", ""))
                continue
            data = path.read_bytes()
            records.append((rel, _hash_bytes(data), str(len(data))))

        with record_path.open("w", encoding="utf-8", newline="") as fh:
            csv.writer(fh).writerows(records)

        out_whl.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(out_whl, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in sorted(wheel_root.rglob("*")):
                if path.is_dir():
                    continue
                zf.write(path, path.relative_to(wheel_root).as_posix())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
