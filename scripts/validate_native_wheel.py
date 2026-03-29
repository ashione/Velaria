#!/usr/bin/env python3

import pathlib
import re
import sys
import zipfile

def _find_dist_info_prefix(names: set[str]) -> str:
    for name in sorted(names):
        match = re.match(r"^(velaria-[^/]+\.dist-info)/", name)
        if match:
            return match.group(1)
    raise RuntimeError("missing velaria dist-info directory")


def _read_wheel_text(zf: zipfile.ZipFile, member: str) -> str:
    return zf.read(member).decode("utf-8")


def _expected_members(source_dir: pathlib.Path) -> set[str]:
    if not source_dir.exists():
        raise RuntimeError(f"package source dir not found: {source_dir}")
    if not source_dir.is_dir():
        raise RuntimeError(f"package source path is not a directory: {source_dir}")

    package_name = source_dir.name
    members = {f"{package_name}/_velaria.so"}
    for path in sorted(source_dir.rglob("*.py")):
        members.add(path.relative_to(source_dir.parent).as_posix())
    return members


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_native_wheel.py <wheel> <package_source_dir>")

    wheel_path = pathlib.Path(sys.argv[1]).resolve()
    source_dir = pathlib.Path(sys.argv[2]).resolve()
    if not wheel_path.exists():
        raise SystemExit(f"wheel not found: {wheel_path}")

    with zipfile.ZipFile(wheel_path, "r") as zf:
        names = set(zf.namelist())
        missing = sorted(_expected_members(source_dir) - names)
        if missing:
            raise SystemExit(
                "wheel is missing required members:\n" + "\n".join(f"- {name}" for name in missing)
            )

        dist_info = _find_dist_info_prefix(names)
        wheel_text = _read_wheel_text(zf, f"{dist_info}/WHEEL")
        if "Root-Is-Purelib: false" not in wheel_text:
            raise SystemExit("wheel must declare Root-Is-Purelib: false")
        if "Tag: py3-none-any" in wheel_text:
            raise SystemExit("wheel must not keep pure-python py3-none-any tag")

        native_info = zf.getinfo("velaria/_velaria.so")
        if native_info.file_size <= 0:
            raise SystemExit("velaria/_velaria.so is empty")

    print(f"[wheel-check] ok {wheel_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
