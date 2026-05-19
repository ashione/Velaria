from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


FetchHistoryFn = Callable[..., list[dict[str, Any]]]
FetchFundamentalsFn = Callable[..., list[dict[str, Any]]]
FetchNewsFn = Callable[..., list[dict[str, Any]]]
FetchQuotesFn = Callable[..., list[dict[str, Any]]]


@dataclass(frozen=True)
class FinanceProviderSpec:
    provider: str
    markets: tuple[str, ...]
    commands: tuple[str, ...]
    freshness: dict[str, str]
    recommended_quote_provider: bool
    recommended_history_provider: bool
    source_url: str
    notes: str

    def to_catalog_row(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "markets": list(self.markets),
            "commands": list(self.commands),
            "freshness": dict(self.freshness),
            "recommended_quote_provider": self.recommended_quote_provider,
            "recommended_history_provider": self.recommended_history_provider,
            "source_url": self.source_url,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class FinanceProviderAdapter:
    spec: FinanceProviderSpec
    fetch_history: FetchHistoryFn | None = None
    fetch_fundamentals: FetchFundamentalsFn | None = None
    fetch_news: FetchNewsFn | None = None
    fetch_quotes: FetchQuotesFn | None = None

    def supports(self, operation: str) -> bool:
        if operation == "fetch_history":
            return self.fetch_history is not None
        if operation == "fetch_fundamentals":
            return self.fetch_fundamentals is not None
        if operation == "fetch_news":
            return self.fetch_news is not None
        if operation == "fetch_quotes":
            return self.fetch_quotes is not None
        return False


class FinanceProviderRegistry:
    def __init__(self, adapters: list[FinanceProviderAdapter]) -> None:
        self._adapters = {adapter.spec.provider: adapter for adapter in adapters}

    def names(self) -> list[str]:
        return sorted(self._adapters)

    def get(self, provider: str) -> FinanceProviderAdapter | None:
        return self._adapters.get(provider)

    def names_for_operation(self, operation: str) -> list[str]:
        return sorted(provider for provider, adapter in self._adapters.items() if adapter.supports(operation))

    def catalog(self) -> list[dict[str, Any]]:
        return [self._adapters[provider].spec.to_catalog_row() for provider in self.names()]
