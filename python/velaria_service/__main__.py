"""Allow ``python -m velaria_service`` to start the service."""
from __future__ import annotations

from . import main

raise SystemExit(main())
