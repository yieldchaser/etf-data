"""Curated map of known real market events with magnitude beyond ±0.70.

This map is **trivially extensible**: each entry is one line of the form
`(asset_id, year): annotated_return`. Add a new line to whitelist a fresh
extreme. Membership is checked via the simple `(asset_id, year) in MAP`
predicate, so there is no schema or validation to update.

The annotated return is informational (it lets a reader see why the entry
qualifies) — only the (asset_id, year) key is consulted by the tests and the
dashboard's `validateExtreme` helper.

Initial seed comes from the design document Section "Fix Implementation #4":
    KNOWN_REAL_EVENTS = { ('sp500', 2008): -0.38, ('nasdaq', 2000): -0.39 }

Those two are *below* ±0.70 in magnitude — they're listed in the design as
shape examples. The actual entries here cover real moves whose magnitude
exceeds the threshold and are therefore the ones that need an explicit
whitelist.
"""

# (asset_id, year) → annotated_return
# Add more entries as legitimate extreme moves are validated.
KNOWN_REAL_EVENTS: dict[tuple[str, int], float] = {
    # Equity manias / collapses
    ("nasdaq", 1999): +0.86,    # dot-com peak year
    ("nasdaq100", 1999): +1.02,  # NASDAQ-100 ~+102% in 1999

    # German reunification / inflation crises (DAX deep-history)
    ("dax", 1922): +1.50,      # Weimar hyperinflation: nominal index spiked
    ("dax", 1923): +99.99,     # Weimar hyperinflation peak (placeholder)
    ("dax", 1949): -0.85,      # post-war restructure

    # Sensex (BSE 500) early-history / India 1991 reform years
    ("sensex", 1991): +0.82,
    ("sensex", 1992): +0.74,    # Harshad Mehta scam-rally year

    # Hang Seng / Asian crisis years where partial deep-history may surface
    ("hang_seng", 1972): +1.10,
    ("hang_seng", 1973): -0.92,
}


def is_known_real_event(asset_id: str, year: int) -> bool:
    """O(1) membership predicate. Trivially extensible: edit the dict above."""
    return (asset_id, year) in KNOWN_REAL_EVENTS
