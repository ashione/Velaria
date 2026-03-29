def _local_python_config_impl(repository_ctx):
    override = repository_ctx.os.environ.get("VELARIA_PYTHON_BIN", "")
    platform_override = repository_ctx.os.environ.get("VELARIA_PLATFORM_TAG", "")
    candidates = []
    if override:
        candidates.append(override)
    candidates.extend([
        "python3.13",
        "python3.12",
        "python3.11",
        "python3.10",
        "python3.9",
        "python3",
        "python",
    ])

    probe = """
import json, os, platform, sys, sysconfig
inc = sysconfig.get_path("include") or ""
plat = sysconfig.get_path("platinclude") or ""
paths = [p for p in [inc, plat] if p]
header = ""
for p in paths:
    h = os.path.join(p, "Python.h")
    if os.path.exists(h):
        header = h
        break
print(json.dumps({
  "executable": sys.executable,
  "version": sys.version.split()[0],
  "include_dir": os.path.dirname(header) if header else "",
  "header": header,
  "machine": platform.machine().lower(),
  "sys_platform": sys.platform,
  "python_tag": f"cp{sys.version_info[0]}{sys.version_info[1]}",
  "abi_tag": f"cp{sys.version_info[0]}{sys.version_info[1]}",
  "platform_tag": sysconfig.get_platform().replace("-", "_").replace(".", "_"),
}))
"""

    chosen = None
    for candidate in candidates:
        if "/" in candidate or "\\" in candidate:
            python_bin = candidate
        else:
            python_bin = repository_ctx.which(candidate)
            if python_bin == None:
                continue
        result = repository_ctx.execute([python_bin, "-c", probe], quiet = True)
        if result.return_code != 0:
            continue
        payload = json.decode(result.stdout)
        if payload.get("header", ""):
            chosen = payload
            break

    if chosen == None:
        fail("Could not find a local CPython interpreter with Python.h. Set VELARIA_PYTHON_BIN to a usable interpreter.")

    platform_tag = chosen["platform_tag"]
    if platform_override:
        platform_tag = platform_override
    elif chosen.get("sys_platform") == "darwin":
        machine = chosen.get("machine", "")
        if machine == "arm64":
            platform_tag = "macosx_11_0_arm64"
        elif machine == "x86_64" and platform_tag == "macosx_10_13_universal2":
            platform_tag = "macosx_10_13_x86_64"

    repository_ctx.symlink(chosen["include_dir"], "include")
    repository_ctx.file("defs.bzl", 'VELARIA_PYTHON_INCLUDE = "include"\n')
    repository_ctx.file("python_tag.txt", chosen["python_tag"] + "\n")
    repository_ctx.file("abi_tag.txt", chosen["abi_tag"] + "\n")
    repository_ctx.file("platform_tag.txt", platform_tag + "\n")
    repository_ctx.file(
        "BUILD.bazel",
        """
load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "headers",
    hdrs = glob(["include/**/*.h", "include/*.h"]),
    includes = ["include"],
)

exports_files(["include_marker", "python_tag.txt", "abi_tag.txt", "platform_tag.txt"])
""",
    )
    repository_ctx.file("include_marker", "")

local_python_configure = repository_rule(
    implementation = _local_python_config_impl,
    environ = ["VELARIA_PLATFORM_TAG", "VELARIA_PYTHON_BIN"],
)
