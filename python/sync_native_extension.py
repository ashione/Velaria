import pathlib
import shutil
import sys
import os
import stat


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: sync_native_extension.py <built-_velaria.so>")
    workspace = pathlib.Path(os.environ.get("BUILD_WORKSPACE_DIRECTORY", pathlib.Path.cwd())).resolve()
    src = pathlib.Path(sys.argv[1]).resolve()
    dst = workspace / "python" / "velaria" / "_velaria.so"
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        current_mode = dst.stat().st_mode
        dst.chmod(current_mode | stat.S_IWUSR)
        dst.unlink()
    shutil.copy2(src, dst)
    synced_mode = dst.stat().st_mode
    dst.chmod(synced_mode | stat.S_IWUSR)
    print(f"[sync-native-extension] copied {src} -> {dst}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
