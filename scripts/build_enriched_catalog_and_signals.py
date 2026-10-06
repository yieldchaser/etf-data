import json
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(r"c:\Users\Dell\Github\etf-data")
DOCS_DIR = ROOT / "docs" / "data" / "flows"
DATA_DIR = ROOT / "data" / "flows"
SCRATCH = Path(r"C:\Users\Dell\.gemini\antigravity\brain\64ce4c2a-8828-42ed-bed3-f7cb9cec2b63\scratch")

# Load master profiles, network blotter, pairs, and peaks/troughs
with open(SCRATCH / "master_145_profiles.json", "r", encoding="utf-8") as f:
    master_profiles = json.load(f)
    if isinstance(master_profiles, list):
        master_profiles = {p["ticker"]: p for p in master_profiles}
    elif "profiles" in master_profiles:
        master_profiles = master_profiles["profiles"]

with open(SCRATCH / "master_network_and_blotter.json", "r", encoding="utf-8") as f:
    network_data = json.load(f)

with open(SCRATCH / "master_pairs_results.json", "r", encoding="utf-8") as f:
    pairs_data = json.load(f)

with open(SCRATCH / "pod5_results.json", "r", encoding="utf-8") as f:
    pod5_data = json.load(f)

# Load existing catalog.json
with open(DOCS_DIR / "catalog.json", "r", encoding="utf-8") as f:
    catalog = json.load(f)

# Build quick lookup for blotter signals
live_signals_map = {s["ticker"]: s for s in network_data.get("all_145_latest", [])}
blotter_p5 = pod5_data.get("live_blotter_2026_10", {})
pod5_live_s1 = {s["ticker"]: s for s in blotter_p5.get("strategy_1_live_triggers", [])}
pod5_live_s2 = {s["ticker"]: s for s in blotter_p5.get("strategy_2_live_triggers", [])}
pod5_live_s3 = {s["ticker"]: s for s in blotter_p5.get("strategy_3_live_triggers", [])}
pod5_live_s4 = blotter_p5.get("strategy_4_live_top10_basket", [])

pairs_map = {}
pairs_list = pairs_data if isinstance(pairs_data, list) else pairs_data.get("ecosystems", [])
for p in pairs_list:
    bulls = p.get("bull_tickers", [])
    bears = p.get("bear_tickers", [])
    quad = p.get("current_1d_quadrant", "")
    spread = p.get("current_zscore_spread", 0.0)
    for b in bulls:
        pairs_map[b] = {
            "paired_bear": bears[0] if bears else None,
            "quadrant": quad,
            "spread": spread,
            "ecosystem": p.get("ecosystem", "")
        }
    for br in bears:
        pairs_map[br] = {
            "paired_bull": bulls[0] if bulls else None,
            "quadrant": quad,
            "spread": spread,
            "ecosystem": p.get("ecosystem", "")
        }

# Smart twins
twins_map = {
    "TQQQ": "QLD", "QLD": "TQQQ",
    "SPXL": "SSO", "UPRO": "SSO", "SSO": "SPXL",
    "NVDL": "NVDX", "NVDX": "NVDL",
    "TSLL": "TSLT", "TSLT": "TSLL",
    "MSTU": "MSTX", "MSTX": "MSTU",
    "AAPU": "AAPX", "AAPX": "AAPU",
    "BITX": "BITU", "BITU": "BITX",
    "SOXL": "USD", "USD": "SOXL"
}

ARCHETYPE_MAP = {
    "CAPITULATION_BOUNCE_SPECIALIST": ("WASHOUT_REBOUND_SPECIALIST", "Washout Rebounder"),
    "CAPITULATION_WASHOUT_REBOUNDER": ("WASHOUT_REBOUND_SPECIALIST", "Washout Rebounder"),
    "INFORMED_MOMENTUM_CONTINUATION": ("MOMENTUM_CONTINUATION", "Momentum Continuation"),
    "CONTRARIAN_RETAIL_EXHAUSTION": ("CONTRARIAN_EXHAUSTION", "Contrarian Exhaustion"),
    "FOMO_SHORT_CANDIDATE": ("OVEREXTENDED_FADE", "Overextended Fade"),
    "RETAIL_FOMO_TOPPING_TRAP": ("OVEREXTENDED_TOPPING_TRAP", "Overextended Topping Trap"),
    "WALL_OF_WORRY_SQUEEZER": ("WALL_OF_WORRY_SQUEEZER", "Wall-of-Worry Squeezer"),
    "BALANCED_REGIME_DEPENDENT": ("BALANCED_REGIME_DEPENDENT", "Balanced / Regime Dependent")
}

SIGNAL_MAP = {
    "CAPITULATION_SLINGSHOT_BUY": ("WASHOUT_SLINGSHOT_BUY", "Washout Slingshot BUY"),
    "INFORMED_MOMENTUM_IGNITION_BUY": ("MOMENTUM_IGNITION_BUY", "Momentum Ignition BUY"),
    "WALL_OF_WORRY_SQUEEZE_BUY": ("WALL_OF_WORRY_SQUEEZE_BUY", "Wall-of-Worry SQUEEZE"),
    "FALLING_KNIFE_RETAIL_TRAP_AVOID": ("FALLING_KNIFE_TRAP_AVOID", "Falling-Knife Trap AVOID"),
    "EUPHORIA_BLOWOFF_CAUTION": ("EUPHORIA_BLOWOFF_CAUTION", "Euphoria Blow-Off Top"),
    "DEAD_CAT_SHORT": ("DEAD_CAT_SHORT", "Dead-Cat Inflow SHORT"),
    "NEUTRAL": ("NEUTRAL", "Neutral / Balanced")
}

# Enrich each instrument in catalog
enriched_count = 0
for inst in catalog.get("instruments", []):
    ticker = inst["ticker"]
    prof = master_profiles.get(ticker, {})
    sig_info = live_signals_map.get(ticker, {})
    p_info = pairs_map.get(ticker, {})

    raw_arch = prof.get("archetype", "BALANCED_REGIME_DEPENDENT")
    norm_arch, arch_lbl = ARCHETYPE_MAP.get(raw_arch, ("BALANCED_REGIME_DEPENDENT", "Balanced"))
    inst["archetype"] = norm_arch
    inst["archetype_label"] = arch_lbl
    
    # Live signal
    sig_type = sig_info.get("signal_type", "NEUTRAL")
    # Check pod5 dead cat
    if ticker in pod5_live_s3 and "Dead-Cat" in pod5_live_s3[ticker].get("strategy_signal_and_rationale", ""):
        sig_type = "DEAD_CAT_SHORT"
    elif ticker in pod5_live_s1:
        sig_type = "CAPITULATION_SLINGSHOT_BUY"
    elif ticker in pod5_live_s2:
        sig_type = "INFORMED_MOMENTUM_IGNITION_BUY"

    norm_sig, sig_lbl = SIGNAL_MAP.get(sig_type, ("NEUTRAL", "Neutral / Balanced"))
    inst["live_signal"] = norm_sig
    inst["live_signal_label"] = sig_lbl
    inst["conviction"] = sig_info.get("conviction", 0.0)
    inst["dd_from_60d_high_pct"] = sig_info.get("dd_from_60d_high_pct", 0.0)
    inst["rally_from_60d_low_pct"] = sig_info.get("rally_from_60d_low_pct", 0.0)
    
    # Pairs info
    inst["paired_bear"] = p_info.get("paired_bear")
    inst["paired_bull"] = p_info.get("paired_bull")
    inst["quadrant_state"] = p_info.get("quadrant")
    inst["quadrant_spread"] = p_info.get("spread")
    inst["smart_twin"] = twins_map.get(ticker)
    
    # Mark tier = 'primary' on all items for backwards compatibility
    inst["tier"] = "primary"
    enriched_count += 1

print(f"Enriched {enriched_count} instruments in catalog.")

# Save enriched catalog.json
with open(DOCS_DIR / "catalog.json", "w", encoding="utf-8") as f:
    json.dump(catalog, f, indent=2)

# Build a compact alpha_signals.json for the frontend
alpha_signals_payload = {
    "generated_date": "2026-10-05",
    "active_signals": [
        inst for inst in catalog["instruments"] if inst.get("live_signal") != "NEUTRAL"
    ],
    "top5_conviction_basket": pod5_live_s4,
    "bull_bear_ecosystems": pairs_list,
    "category_rotations_top10": network_data.get("rotation_results_top30", [])[:10],
    "cross_etf_lead_lag_top10": network_data.get("lead_lag_pairs_top50", [])[:10],
    "breadth_regimes": network_data.get("breadth_regimes", {})
}

with open(DOCS_DIR / "alpha_signals.json", "w", encoding="utf-8") as f:
    json.dump(alpha_signals_payload, f, indent=2)

print("Saved enriched catalog.json and alpha_signals.json to docs/data/flows/")
