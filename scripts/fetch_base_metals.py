import json
import pandas as pd
from pathlib import Path

# Paths
OUTPUT_PATH = Path("docs/data/market_returns.json")

# Metadata mapping: key -> (FRED_ID, display_name)
METALS = {
    "copper": ("PCOPPUSDM", "Copper"),
    "aluminum": ("PALUMUSDM", "Aluminum"),
    "nickel": ("PNICKUSDM", "Nickel"),
    "zinc": ("PZINCUSDM", "Zinc"),
    "iron_ore": ("PIORECRUSDM", "Iron Ore"),
    "tin": ("PTINUSDM", "Tin"),
    "lead": ("PLEADUSDM", "Lead"),
}

def main():
    # Load existing JSON
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        print(f"Error: {OUTPUT_PATH} does not exist!")
        return

    assets = data.setdefault("assets", {})

    for key, (fred_id, name) in METALS.items():
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={fred_id}"
        print(f"Fetching {name} ({fred_id}) from {url}...")
        try:
            df = pd.read_csv(url)
            # Columns: observation_date, <fred_id>
            # Drop any row where close value is '.' or missing
            df.columns = ["Date", "Close"]
            df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
            df = df.dropna(subset=["Close"])
            
            # Format Date as YYYY-MM
            df["ym"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m")
            
            # Group by ym and take the last Close (EOP)
            monthly = df.groupby("ym")["Close"].last().reset_index()
            monthly_list = [[row["ym"], round(float(row["Close"]), 4)] for _, row in monthly.iterrows()]
            
            # Sort by date
            monthly_list = sorted(monthly_list, key=lambda x: x[0])
            
            if not monthly_list:
                print(f"  No data for {key}")
                continue
                
            first_ym = monthly_list[0][0]
            last_ym = monthly_list[-1][0]
            
            # Create or update asset
            assets[key] = {
                "meta": {
                    "name": name,
                    "category": "base_metals",
                    "native_ccy": "USD",
                    "source": f"fred:{fred_id}",
                    "first": first_ym,
                    "last": last_ym
                },
                "monthly": monthly_list
            }
            print(f"  Merged {len(monthly_list)} months for {key} ({first_ym} -> {last_ym})")
        except Exception as e:
            print(f"  Error fetching {key}: {e}")

    # Recompute asof
    all_lasts = []
    for v in assets.values():
        last = v.get("meta", {}).get("last")
        if last:
            all_lasts.append(last)
    
    fx = data.get("fx", {})
    for pair in fx.values():
        if pair:
            all_lasts.append(pair[-1][0])
            
    cpi = data.get("cpi", {})
    for cpi_data in cpi.values():
        if cpi_data:
            all_lasts.append(cpi_data[-1][0])
            
    rates = data.get("rates", {})
    for rate_data in rates.values():
        if isinstance(rate_data, dict) and rate_data.get("meta", {}).get("last"):
            all_lasts.append(rate_data["meta"]["last"])
            
    if all_lasts:
        data["asof"] = max(all_lasts)

    # Write output
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"Successfully wrote updated market_returns.json (asof: {data['asof']})")

if __name__ == "__main__":
    main()
