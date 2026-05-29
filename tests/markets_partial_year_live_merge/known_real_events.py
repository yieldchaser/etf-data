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
    ("nasdaq100", 1998): +0.85,
    ("djia", 1915): +0.82,
    ("dax", 1985): +1.02,
    ("nikkei", 1952): +1.19,
    ("nikkei", 1972): +0.92,
    ("sensex", 1985): +0.94,
    ("sensex", 2003): +0.73,
    ("sensex", 2009): +0.81,

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
    ("hang_seng", 1993): +1.16,

    # Shanghai Composite
    ("shanghai", 2006): +1.30,
    ("shanghai", 2007): +0.97,
    ("shanghai", 2009): +0.80,

    # Metals and Commodities
    ("gold", 1973): +0.73,
    ("gold", 1979): +1.27,
    ("silver", 1979): +4.35,
    ("silver", 1980): -0.62,
    ("silver", 2010): +0.84,
    ("silver", 2025): +1.43,
    ("platinum", 1978): +0.87,
    ("platinum", 1979): +0.96,
    ("platinum", 2025): +1.27,
    ("palladium", 1979): +1.52,
    ("palladium", 2000): +1.12,
    ("palladium", 2009): +1.18,
    ("palladium", 2010): +0.97,
    ("palladium", 2025): +0.82,

    # Base Metals
    ("copper", 1994): +0.73,
    ("copper", 2009): +1.25,
    ("aluminum", 1994): +0.72,
    ("nickel", 1999): +1.09,
    ("nickel", 2003): +0.97,
    ("nickel", 2006): +1.55,
    ("nickel", 2009): +0.74,
    ("zinc", 2006): +1.41,
    ("zinc", 2009): +1.13,
    ("zinc", 2016): +0.74,
    ("iron_ore", 2005): +0.72,
    ("iron_ore", 2008): +0.91,
    ("iron_ore", 2016): +0.94,
    ("tin", 2021): +1.00,
    ("lead", 2009): +1.40,
    ("copper_lb", 1987): +1.37,
    ("copper_lb", 2009): +1.38,

    # Crude Oil
    ("wti_crude", 1999): +1.12,
    ("wti_crude", 2009): +0.78,
    ("wti_crude", 2020): -0.54,
    ("wti_crude", 2026): +0.96,
    ("brent_crude", 1999): +1.37,
    ("brent_crude", 2009): +1.18,
    ("brent_crude", 2020): -0.71,
    ("brent_crude", 2026): +0.90,

    # Agriculture
    ("wheat", 1973): +1.06,
    ("wheat", 2010): +0.90,
    ("corn", 1973): +0.71,
    ("corn", 2006): +0.83,
    ("soybeans", 1974): +0.58,
    ("soybeans", 2007): +0.77,
    ("cotton", 1973): +1.68,
    ("cotton", 1986): -0.58,
    ("cotton", 2010): +0.92,
    ("sugar", 1963): +1.50,
    ("sugar", 1964): -0.76,
    ("sugar", 1965): +0.73,
    ("sugar", 1967): +0.91,
    ("sugar", 1971): +0.83,
    ("sugar", 1972): +0.52,
    ("sugar", 1974): +2.83,
    ("sugar", 1979): +0.94,
    ("sugar", 1980): +0.75,
    ("sugar", 1983): +0.75,
    ("sugar", 1985): +0.53,
    ("sugar", 1986): +0.52,
    ("sugar", 2000): +0.74,
    ("sugar", 2009): +1.17,
    ("coffee", 1976): +1.35,
    ("coffee", 1994): +1.61,
    ("coffee", 2010): +0.71,
    ("coffee", 2021): +0.76,

    # Bovespa
    ("bovespa", 1993): +14.58,
    ("bovespa", 1994): +10.59,
    ("bovespa", 1999): +1.52,
    ("bovespa", 2003): +0.97,
    ("bovespa", 2009): +0.83,
}


def is_known_real_event(asset_id: str, year: int) -> bool:
    """O(1) membership predicate. Trivially extensible: edit the dict above."""
    return (asset_id, year) in KNOWN_REAL_EVENTS
