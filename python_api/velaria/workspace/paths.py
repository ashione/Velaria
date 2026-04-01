import os
import pathlib


def get_velaria_home() -> pathlib.Path:
    raw = os.environ.get("VELARIA_HOME")
    if raw:
        return pathlib.Path(raw).expanduser().resolve()
    return (pathlib.Path.home() / ".velaria").resolve()


def get_runs_dir() -> pathlib.Path:
    return get_velaria_home() / "runs"


def get_index_dir() -> pathlib.Path:
    return get_velaria_home() / "index"


def ensure_dirs() -> dict[str, pathlib.Path]:
    home = get_velaria_home()
    runs = get_runs_dir()
    index = get_index_dir()
    home.mkdir(parents=True, exist_ok=True)
    runs.mkdir(parents=True, exist_ok=True)
    index.mkdir(parents=True, exist_ok=True)
    return {
        "home": home,
        "runs": runs,
        "index": index,
    }
