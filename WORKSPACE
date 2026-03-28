workspace(name = "cpp-dataflow-distributed-engine")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//tools:python_configure.bzl", "local_python_configure")

http_archive(
    name = "bazel_skylib",
    urls = ["https://github.com/bazelbuild/bazel-skylib/releases/download/1.7.1/bazel-skylib-1.7.1.tar.gz"],
)

http_archive(
    name = "rules_python",
    strip_prefix = "rules_python-0.33.0",
    urls = ["https://github.com/bazelbuild/rules_python/releases/download/0.33.0/rules_python-0.33.0.tar.gz"],
)

load("@rules_python//python:repositories.bzl", "py_repositories")
load("@rules_python//python:repositories.bzl", "python_register_toolchains")

py_repositories()
python_register_toolchains(
    name = "python_3_12",
    python_version = "3.12",
)

load("@rules_python//python:pip.bzl", "pip_parse")

pip_parse(
    name = "pypi",
    requirements_lock = "//python_api:requirements.lock",
    python_interpreter_target = "@python_3_12_host//:python",
)

local_python_configure(name = "velaria_local_python")
