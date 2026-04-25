import sys

import velaria.cli as _velaria_cli_impl


sys.modules[__name__] = _velaria_cli_impl


if __name__ == "__main__":
    raise SystemExit(_velaria_cli_impl.main())
