# Velaria Electron `.dmg` 打包方案 v1

## 当前角色

这份文档是 Velaria 可视化 app 的打包执行方案。

它回答的是：

- 如何把 `Node + Python + C++` 全部收敛到一个 Electron app 里
- 如何在 macOS 上产出一个可安装的 `.dmg`
- 仓库里后续需要新增哪些目录、脚本和构建步骤

本文不是 UI 方案，也不是 app service API 方案。
它只聚焦“如何把最终产品打出来”。

## 1. 最终目标

最终交付物固定为：

- 一个 Electron 桌面应用：`Velaria.app`
- 一个 macOS 分发镜像：`Velaria-<version>.dmg`

用户路径固定为：

```text
下载 .dmg
  -> 打开 .dmg
  -> 拖动安装 Velaria.app
  -> 首次启动 app
  -> app 自动启动本地 sidecar
```

用户不需要：

- 手工安装 Python
- 手工安装 Node
- 手工执行 Bazel
- 手工运行 `uv`
- 手工启动后台服务

## 2. 当前仓库可复用基础

当前仓库已经有以下打包基础：

- C++ native extension 构建目标：
  - `//:velaria_pyext`
  - 参考：[BUILD.bazel](../BUILD.bazel)
- Python wheel：
  - `//python:velaria_whl`
- Python native wheel：
  - `//python:velaria_native_whl`
  - 参考：[python/BUILD.bazel](../python/BUILD.bazel)
- Python 包已把 `_velaria.so` 视为 package data：
  - 参考：[python/pyproject.toml](../python/pyproject.toml)
- 已有单文件 CLI 打包脚本，可作为 sidecar 思路参考：
  - 参考：[scripts/build_py_cli_executable.sh](../scripts/build_py_cli_executable.sh)

结论：

- 仓库不需要从零开始解决 Python + C++ 如何分发
- 真正缺的是 Electron 壳和“面向 app 的 sidecar 打包层”

## 3. 固定架构结论

v1 打包架构固定为：

```text
Electron App
  -> Node main process
    -> launch local velaria-service sidecar
      -> sidecar 内含 Python runtime + velaria package + _velaria.so
```

固定原则：

- Electron 负责 UI 和产品宿主
- Python sidecar 负责执行与本地服务
- C++ 只通过 Python sidecar 被带入 app
- Electron 不直接加载 `_velaria.so`
- 用户只安装一个 `.app`

## 4. 为什么采用 sidecar，而不是把 Python 散装塞给 Electron

不采用“Electron 直接找系统 Python”的原因：

- 用户机器未必有兼容 Python
- Python 版本和 wheel ABI 不稳定
- C++ native extension 的加载路径不可控

不采用“Electron 直接调用源码 checkout”的原因：

- 发布态不能依赖 Bazel、源码树和 `uv`

不采用“把 CLI 直接当最终 app 服务”的原因：

- CLI 是命令行入口，不是长期稳定的 GUI 本地服务协议

所以 v1 固定方案是：

- 先构建一个 app 专用 `velaria-service`
- 再把它随 Electron 一起打包

## 5. 产物分层

v1 打包必须拆成四类产物：

### 5.1 C++ 原生产物

- `_velaria.so`

来源：

- `bazel build //:velaria_pyext`

### 5.2 Python 分发产物

推荐主产物：

- `velaria native wheel`

来源：

- `bazel build //python:velaria_native_whl`

### 5.3 Python sidecar 产物

新增产物：

- `velaria-service`

它是一个发布态可执行服务，不是源码入口，不要求用户有 Python 环境。

v1 推荐形式：

- `PyInstaller one-dir` 或 `PyInstaller one-file`

这里更推荐：

- `one-dir`

原因：

- 对内含 native extension、依赖和调试更稳
- 签名和排查动态库问题更容易

### 5.4 Electron 产物

最终产物：

- `Velaria.app`
- `Velaria-<version>.dmg`

## 6. 打包工具选择

v1 固定选择：

- 前端/桌面宿主：`Electron`
- macOS 打包器：`electron-builder`

原因：

- 对 `extraResources`、嵌入 sidecar、签名、`.dmg` 产出更直接
- 对混合分发场景更省事

v1 不选：

- Tauri
- 手写 `.app` bundle
- 先做 web app 再单独包壳

## 7. 目标目录结构

仓库后续目录建议固定为：

```text
app/
  package.json
  electron-builder.yml
  src/
    main/
    preload/
    renderer/
  scripts/
    build-sidecar-macos.sh
    stage-python-runtime.sh
    package-macos.sh
  resources/
    icons/

python/
  velaria_service/
```

发布构建中间目录建议固定为：

```text
out/
  sidecar/
    macos/
      velaria-service/
  electron/
    mac/
      Velaria.app
  dist/
    Velaria-<version>.dmg
```

## 8. App Bundle 内部布局

最终 `.app` 内部布局固定为：

```text
Velaria.app/
  Contents/
    MacOS/
      Velaria
    Resources/
      app.asar
      bin/
        velaria-service/
          velaria-service
          _internal/...
      assets/
        ...
```

约束：

- Python sidecar 不进入 `app.asar`
- sidecar 放到 `Contents/Resources/bin/`
- 如有模型文件或静态数据，放到 `Contents/Resources/assets/`

## 9. 新增执行入口

### 9.1 新增 Python 服务入口

新增文件：

- `python/velaria_service/`

职责：

- 启动本地 HTTP 或 IPC 服务
- 对外提供 app 所需的服务 API
- 调用 `velaria.Session` 与 workspace / artifact 层

v1 不要求完整 API 集，但至少要支持：

- health check
- import preview
- run analysis
- run status
- artifact preview

### 9.2 新增 sidecar 打包脚本

新增脚本：

- `app/scripts/build-sidecar-macos.sh`

职责：

1. 构建 `//:velaria_pyext`
2. 构建 `//python:velaria_native_whl`
3. 在临时目录创建 Python 虚拟环境
4. 安装 native wheel
5. 用 PyInstaller 打出 `velaria-service`
6. 输出到 `out/sidecar/macos/velaria-service/`

### 9.3 新增 Electron 打包脚本

新增脚本：

- `app/scripts/package-macos.sh`

职责：

1. 调用 `build-sidecar-macos.sh`
2. 构建 Electron renderer/main
3. 把 sidecar 拷贝到 `extraResources`
4. 执行 `electron-builder --mac dmg`
5. 在需要时执行签名、公证、staple

## 10. Electron 构建配置

`electron-builder.yml` 需要固定这些配置：

- `appId`
- `productName`
- `asar: true`
- `files`
  - 包含 Electron 主进程、preload、renderer 构建产物
- `extraResources`
  - 包含 `out/sidecar/macos/velaria-service`
- `mac`
  - `target: dmg`
  - `category`
  - `hardenedRuntime: true`
  - `entitlements`
  - `entitlementsInherit`
- `dmg`
  - app icon
  - Applications link

v1 约束：

- sidecar 必须走 `extraResources`
- 不允许只依赖开发机路径

## 11. Electron 主进程逻辑

主进程必须承担以下职责：

### 11.1 定位 sidecar

开发态：

- 从 `out/sidecar/macos/velaria-service/` 启动

发布态：

- 从 `process.resourcesPath + /bin/velaria-service/velaria-service` 启动

### 11.2 启动顺序

启动顺序固定为：

1. app 启动
2. main process 解析运行环境
3. 启动 sidecar
4. 轮询健康检查
5. sidecar ready 后再展示主窗口

### 11.3 退出顺序

退出顺序固定为：

1. Electron 请求关闭
2. main process 通知 sidecar 优雅退出
3. 超时后强制 kill
4. app 退出

## 12. macOS 签名与公证

正式外部分发必须加入：

- code signing
- notarization
- staple

v1 固定顺序：

1. 构建 `Velaria.app`
2. 对 `.app` 和 nested binaries 进行 codesign
3. notarize
4. staple
5. 构建或最终确认 `.dmg`

关键点：

- `velaria-service` 也是 nested executable，必须被签名
- sidecar 内若有动态库或 Python 内嵌二进制，也要纳入签名范围

## 13. 执行计划

### Phase 1: 产物链路打通

目标：

- 在开发机上产出未签名的本地 `.app`
- 能启动 Electron
- Electron 能启动 sidecar
- sidecar 能返回 health check

执行项：

1. 新建 `app/` 工程骨架。
2. 新增 `python/velaria_service/`。
3. 新增 `app/scripts/build-sidecar-macos.sh`。
4. 用 PyInstaller 打出 sidecar。
5. 新增 `electron-builder` 配置。
6. 主进程完成 sidecar 启动与健康检查。

验收：

- `Velaria.app` 可双击启动
- sidecar 能随 app 自动启动
- UI 能显示 “service ready”

### Phase 2: `.dmg` 本地产出

目标：

- 在 macOS 开发机上产出可安装 `.dmg`

执行项：

1. 新增 `app/scripts/package-macos.sh`
2. 接通 `electron-builder --mac dmg`
3. 确认 sidecar 被正确复制到 `.app/Contents/Resources/bin/`
4. 验证安装后 sidecar 路径解析正确

验收：

- 能产出 `Velaria-<version>.dmg`
- 从 `.dmg` 安装后的 `.app` 可以启动
- 无需系统 Python

### Phase 3: 签名与公证

目标：

- 产出可外部分发的 `.dmg`

执行项：

1. 配置 Apple Developer 证书
2. 配置 `APPLE_ID` / `TEAM_ID` / notary 凭证
3. 接通 `electron-builder` 签名
4. 接通 notarization
5. 接通 staple

验收：

- 在非开发机上正常打开
- 不触发明显 Gatekeeper 阻断

## 14. 具体执行清单

以下是后续真正开始做时的执行顺序：

1. 新建 `app/package.json`
2. 新建 `app/electron-builder.yml`
3. 新建 `python/velaria_service/`
4. 新建 `app/scripts/build-sidecar-macos.sh`
5. 新建 `app/scripts/package-macos.sh`
6. 在 `package.json` 中加入：
   - `build:renderer`
   - `build:main`
   - `build:sidecar`
   - `dist:mac`
7. 在 Electron main 中加入：
   - sidecar path resolver
   - sidecar launcher
   - health checker
8. 本地验证开发态启动
9. 本地验证 `.app`
10. 本地验证 `.dmg`
11. 再接签名与公证

## 15. 本地开发与发布态切换

### 开发态

开发态固定为：

- renderer 热更新
- main process 本地运行
- sidecar 通过脚本单独构建
- Electron 主进程直接启动本地 sidecar

### 发布态

发布态固定为：

- sidecar 已经打成可执行目录
- 被复制到 `extraResources`
- Electron 从 `process.resourcesPath` 启动 sidecar

### 禁止事项

禁止：

- 发布态依赖 `uv run`
- 发布态依赖本地 Bazel
- 发布态依赖仓库源码目录
- 发布态依赖系统 Python

## 16. 风险与规避

### 风险 1：native extension 路径不稳定

规避：

- 只通过 native wheel 或 sidecar 构建产物带入发布包
- 不依赖源码树自动发现逻辑

### 风险 2：PyInstaller 与 `_velaria.so` 打包不完整

规避：

- 在 Phase 1 就做最小 health-check sidecar
- 明确检查 `_velaria.so` 是否被正确装入

### 风险 3：签名只签主 app，漏签 sidecar

规避：

- 把 sidecar 当 nested executable 明确纳入签名清单

### 风险 4：开发态和发布态路径逻辑分叉太大

规避：

- 统一 sidecar resolver 接口
- 主进程只通过一套路径查找逻辑工作

## 17. 最终结论

Velaria 的 Electron `.dmg` 打包方案不应该是“把三套运行时胡乱塞进一个安装包”，而应当是：

- C++ 先进入 Python native wheel
- Python 再进入 app 专用 sidecar
- sidecar 再进入 Electron app
- Electron 最终产出 `.app` 与 `.dmg`

这条链路最符合当前仓库已有基础，也最容易一步步打通并调试。
