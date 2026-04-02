#!/usr/bin/env python3

import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
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


def _validate_macos_arches(
    zf: zipfile.ZipFile,
    member: str,
    expected_arches: list[str],
) -> None:
    if not expected_arches:
        return
    if shutil.which("lipo") is None:
        raise SystemExit("lipo is required to validate macOS universal2 wheel contents")

    with tempfile.TemporaryDirectory(prefix="velaria-wheel-check-") as tmp:
        extracted = pathlib.Path(tmp) / pathlib.Path(member).name
        extracted.write_bytes(zf.read(member))
        result = subprocess.run(
            ["lipo", "-info", str(extracted)],
            check = True,
            capture_output = True,
            text = True,
        )

    output = result.stdout.strip() or result.stderr.strip()
    missing = [arch for arch in expected_arches if arch not in output]
    if missing:
        raise SystemExit(
            "wheel native extension is missing expected architectures: "
            + ", ".join(missing)
            + f" (lipo output: {output})"
        )


def main() -> int:
    if len(sys.argv) not in (3, 4):
        raise SystemExit(
            "usage: validate_native_wheel.py <wheel> <package_source_dir> [expected_arches_csv]"
        )

    wheel_path = pathlib.Path(sys.argv[1]).resolve()
    source_dir = pathlib.Path(sys.argv[2]).resolve()
    expected_arches = []
    if len(sys.argv) == 4 and sys.argv[3].strip():
        expected_arches = [part.strip() for part in sys.argv[3].split(",") if part.strip()]
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
        _validate_macos_arches(zf, "velaria/_velaria.so", expected_arches)

    print(f"[wheel-check] ok {wheel_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
