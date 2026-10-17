from __future__ import annotations

FILTER_KEYS = {name: f"dineiq_filter_{name}" for name in (
    "search", "dates", "location", "item", "category", "segment", "channel",
    "promotion", "performance", "price", "rating", "wastage",
)}
PRICE_RANGE = (0.0, 100000.0)


def clear_filter_state(state) -> None:
    """The next rerun recreates the original unfiltered widget defaults."""
    for key in FILTER_KEYS.values():
        state.pop(key, None)

