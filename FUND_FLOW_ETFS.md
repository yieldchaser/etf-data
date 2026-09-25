# Curated Leveraged & Tactical ETF Universe — Fund Flow Tracking

**Total Instruments:** 117 Curated Leveraged ETFs across 9 professional categories.

> **Selection Thesis & Architecture:**
> 1. **Aggressively Long-Biased (107 Bull vs 10 Tactical Short Anchors)**: Recognizes the multi-decade structural upward drift of US equity markets. Pruned out dozens of low-conviction/decaying inverse ETFs.
> 2. **Comprehensive AI, Semiconductor & Tech Hardware Coverage**: Full coverage of the AI hardware supply chain from foundries and capital equipment to optical networking (**LITX** Lumentum, **COHX** Coherent, **LRCU** Lam Research, **ASMG** ASML, **ARMG** ARM, **ONX** ON Semi, **NVDL/NVDX** NVIDIA, **SMCX** Super Micro, **DLLL** Dell).
> 3. **Selective Hedging Anchors Only (10 ETFs)**: Retained only the highest-liquidity benchmark shorts for tracking institutional panic hedging and capitulation bottoms (SQQQ, SPXS, TZA, SOXS, SCO, TMV, YANG, NVD, TSLZ, MSTZ).
> 4. **Historical Local Source**: The 117 rows below are the authoritative local leveraged/inverse universe. Historical flow, NAV, and derived fields are Trackinsight-reported values already stored in the local dataset; this document does not assert a live fetch or current API status.

---

## Category Summary

| Category # | Category Name | ETF Count | Orientation | Key Themes / Anchors |
|---|---|---|---|---|
| 1 | 1. Broad Market Equity Index (Bull) | 8 | Bull (+2x / +3x) | NASDAQ-100, S&P 500... |
| 2 | 2. Technology, Semiconductor & Thematic (Bull) | 16 | Bull (+2x / +3x) | Semiconductors, Technology Select, U.S. Technology... |
| 3 | 3. Sector Specific Leveraged (Bull) | 12 | Bull (+2x / +3x) | Big Banks, Financials Select, Regional Banking, U.S. Financials... |
| 4 | 4. Single-Stock Leveraged (Bull) — AI, Semis & High-Beta Tech | 27 | Bull (+2x / +3x) | Coherent Corp, Lumentum Holdings, NVIDIA... |
| 5 | 5. Single-Stock Leveraged (Bull) — Mega-Cap Giants, Crypto & Consumer | 22 | Bull (+2x / +3x) | Apple, Tesla... |
| 6 | 6. Commodities, Energy & Volatility (Bull) | 9 | Bull (+2x / +3x) | Crude Oil, Gold, Gold Miners... |
| 7 | 7. Fixed Income, Currencies & Crypto (Bull) | 8 | Bull (+2x / +3x) | 20+ Year Treasury, 7-10 Year Treasury, EUR/USD... |
| 8 | 8. International / Country (Bull) | 5 | Bull (+2x / +3x) | Brazil, China 50, India, South Korea... |
| 9 | 9. Selective Benchmark Hedging / Tactical Shorts (Pruned to Key Anchors Only) | 10 | Tactical Short | NASDAQ-100, Russell 2000, S&P 500, Semiconductors... |

---

## Featured 24

The Fund Flows interface uses this explicit, deterministic 24-instrument subset drawn only from the 117 local leveraged/inverse rows above:

`TQQQ`, `QLD`, `UPRO`, `SPXL`, `SOXL`, `TECL`, `USD`, `DFEN`, `FAS`, `ERX`, `CURE`, `NVDL`, `NVDX`, `LITX`, `COHX`, `PTIR`, `TSLL`, `MSTU`, `CONL`, `UGL`, `NUGT`, `TMF`, `YINN`, `SQQQ`

There is no long-only catalog, watch tier, or missing-underlying watchlist in this universe.

---

## 1. Broad Market Equity Index (Bull) (8)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **TQQQ** | `TQQQ` | +3x | ProShares UltraPro QQQ ETF | ProShares | NASDAQ-100 | $39,723.4M | 0.97% |
| **QLD** | `QLD` | +2x | ProShares Ultra QQQ ETF | ProShares | NASDAQ-100 | $15,144.9M | 0.98% |
| **UPRO** | `UPRO` | +3x | ProShares UltraPro S&P500 ETF | ProShares | S&P 500 | $5,715.9M | 0.89% |
| **SSO** | `ARCX:SSO` | +2x | ProShares Ultra S&P500 ETF | ProShares | S&P 500 | $9,106.3M | 0.88% |
| **SPXL** | `SPXL` | +3x | Direxion Daily S&P 500 Bull 3X Shares | Direxion | S&P 500 | $7,408.6M | 0.95% |
| **TNA** | `TNA` | +3x | Direxion Daily Small Cap Bull 3x Shares ETF | Direxion | Russell 2000 | $1,263.5M | 1.07% |
| **URTY** | `URTY` | +3x | ProShares UltraPro RUSSELL2000 ETF | ProShares | Russell 2000 | $296.0M | 1.08% |
| **UDOW** | `UDOW` | +3x | ProShares UltraPro DOW30 ETF | ProShares | Dow Jones 30 | $871.5M | 0.95% |

## 2. Technology, Semiconductor & Thematic (Bull) (16)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **SOXL** | `ARCX:SOXL` | +3x | Direxion Daily Semiconductor Bull 3X Shares ETF | Direxion | Semiconductors | $24,161.0M | 0.91% |
| **USD** | `ARCX:USD` | +2x | ProShares Ultra SEMICONDUCTORS ETF | ProShares | Semiconductors | $2,926.4M | 0.95% |
| **TECL** | `TECL` | +3x | Direxion Daily Technology Bull 3X Shares ETF | Direxion | Technology Select | $6,548.9M | 0.94% |
| **ROM** | `ROM` | +2x | ProShares Ultra TECHNOLOGY ETF | ProShares | U.S. Technology | $1,373.6M | 0.95% |
| **FNGU** | `FNGU` | +3x | MicroSectors FANG+ 3x Leveraged ETNs | BMO | NYSE FANG+ | $2,645.0M | 0.95% |
| **BULZ** | `BULZ` | +3x | MicroSectors FANG & Innovation 3X Leveraged ETN | BMO | Tech Innovation | $3,546.4M | 0.95% |
| **QQQU** | `ARCX:QQQU` | +2x | Direxion Daily Magnificent 7 Bull 2X Shares ETF | Direxion | Magnificent Seven | $80.3M | 1.00% |
| **MAGX** | `MAGX` | +2x | Roundhill Daily 2X Long Magnificent Seven ETF | Roundhill Investments | Magnificent Seven | $68.9M | 0.96% |
| **LABU** | `LABU` | +3x | Direxion Daily S&P Biotech Bull 3X Shares ETF - USD | Direxion | S&P Biotech | $600.8M | 0.96% |
| **BIB** | `BIB` | +2x | ProShares Ultra Nasdaq Biotechnology ETF | ProShares | Biotech | $95.3M | 1.19% |
| **TARK** | `TARK` | +2x | Tradr 2X Long Innovation ETF | AXS Investments | ARKK Innovation | $16.4M | 1.39% |
| **MSOX** | `MSOX` | +2x | AdvisorShares MSOS Daily Leveraged ETF | AdvisorShares | U.S. Cannabis | $67.1M | 1.42% |
| **NAIL** | `NAIL` | +3x | Direxion Daily Homebuilders & Supplies Bull 3X Shares ETF | Direxion | Homebuilders | $546.2M | 0.96% |
| **DFEN** | `ARCX:DFEN` | +3x | Direxion Daily Aerospace & Defense Bull 3X Shares ETF | Direxion | Aerospace & Defense | $312.4M | 0.96% |
| **HIBL** | `HIBL` | +3x | Direxion Daily S&P 500 High Beta Bull 3X ETF | Direxion | S&P 500 High Beta | $80.1M | 1.01% |
| **FLYU** | `FLYU` | +3x | MicroSectors Travel 3x Leveraged ETN | BMO | Travel & Airlines | $5.5M | 0.95% |

## 3. Sector Specific Leveraged (Bull) (12)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **FAS** | `FAS` | +3x | Direxion Daily Financial Bull 3x Shares ETF | Direxion | Financials Select | $2,093.1M | 0.92% |
| **UYG** | `UYG` | +2x | ProShares Ultra Financials ETF | ProShares | U.S. Financials | $773.8M | 0.94% |
| **DPST** | `DPST` | +3x | Direxion Daily Regional Banks Bull 3X Shares ETF - USD | Direxion | Regional Banking | $322.6M | 0.92% |
| **BNKU** | `ARCX:BNKU` | +3x | MicroSectors U.S. Big Banks 3x Leveraged ETNs | BMO | Big Banks | $38.4M | 0.35% |
| **ERX** | `ERX` | +2x | Direxion Daily Energy Bull 2x Shares ETF | Direxion | Energy Select | $240.5M | 0.91% |
| **DIG** | `DIG` | +2x | ProShares Ultra Energy ETF | ProShares | Oil & Gas | $97.4M | 1.07% |
| **OILU** | `ARCX:OILU` | +3x | MicroSectors Oil & Gas Exploration & Production 3X Leveraged ETN | BMO | Oil & Gas E&P | $77.8M | 0.95% |
| **WTIU** | ARCX:WTIU | +3x | MicroSectors Energy 3x Leveraged ETN | BMO | MicroSectors U.S. Energy | $37.0M | 0.95% |
| **NRGU** | `ARCX:NRGU` | +3x | MicroSectors U.S. Big Oil 3x Leveraged ETNs | BMO | MicroSectors U.S. Big Oil Index | $89.7M | 0.95% |
| **CURE** | `ARCX:CURE` | +3x | Direxion Daily Healthcare Bull 3X Shares ETF | Direxion | Health Care Select | $170.5M | 0.94% |
| **DRN** | `DRN` | +3x | Direxion Daily Real Estate Bull 3x Shares ETF | Direxion | MSCI US REIT | $38.3M | 0.98% |
| **URE** | `URE` | +2x | ProShares Ultra Real Estate ETF | ProShares | U.S. Real Estate | $51.4M | 1.10% |

## 4. Single-Stock Leveraged (Bull) — AI, Semis & High-Beta Tech (27)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **NVDL** | `NVDL` | +2x | GraniteShares 2x Long NVDA Daily ETF | GraniteShares | NVIDIA (NVDA) | $3,722.8M | 1.06% |
| **NVDX** | `NVDX` | +2x | T-Rex 2X Long NVIDIA Daily Target ETF | Tuttle Capital Management | NVIDIA (NVDA) | $495.8M | 1.05% |
| **LITX** | `LITX` | +2x | Tradr 2X Long LITE Daily ETF | AXS Investments | Lumentum Holdings (LITE) | $207.8M | 1.49% |
| **COHX** | `COHX` | +2x | Tradr 2X Long COHR Daily ETF | AXS Investments | Coherent Corp (COHR) | $129.1M | 1.49% |
| **GLWG** | `GLWG` | +2x | Leverage Shares 2X Long GLW Daily ETF | Leverage Shares | Corning (GLW) | $129.4M | 0.75% |
| **LRCU** | `LRCU` | +2x | Tradr 2X Long LRCX Daily ETF | AXS Investments | Lam Research (LRCX) | $38.9M | 1.30% |
| **MCHU** | `MCHU` | +2x | Tradr 2X Long MCHP Daily ETF | AXS Investments | Microchip Technology (MCHP) | $0.5M | 1.30% |
| **NXPX** | `NXPX` | +2x | Tradr 2X Long NXPI Daily ETF | AXS Investments | NXP Semiconductors (NXPI) | $0.2M | 1.30% |
| **ONX** | `ONX` | +2x | Tradr 2X Long ON Daily ETF | AXS Investments | ON Semiconductor (ON) | $1.7M | 1.30% |
| **ASMG** | `ASMG` | +2x | Leverage Shares 2X Long ASML Daily ETF | Themes Management Company | ASML Holding (ASML) | $32.1M | 0.77% |
| **ARMG** | `ARMG` | +2x | Leverage Shares 2X Long ARM Daily ETF | Themes Management Company | ARM Holdings (ARM) | $92.3M | 0.78% |
| **AMDL** | `AMDL` | +2x | GraniteShares 2x Long AMD Daily ETF | GraniteShares | AMD (AMD) | $1,065.7M | 1.07% |
| **MULL** | `MULL` | +2x | GraniteShares 2x Long MU Daily ETF | GraniteShares | Micron Technology (MU) | $741.3M | 3.06% |
| **AVGX** | `AVGX` | +2x | Defiance Daily Target 2x Long AVGO ETF | Defiance ETFs | Broadcom (AVGO) | $226.0M | 1.31% |
| **TSMU** | `TSMU` | +2x | GraniteShares 2x Long TSM Daily ETF | GraniteShares | Taiwan Semi (TSM) | $45.7M | 2.45% |
| **INTW** | `INTW` | +2x | GraniteShares 2x Long INTC Daily ETF | GraniteShares | Intel (INTC) | $349.3M | 1.85% |
| **MVLL** | `MVLL` | +2x | GraniteShares 2x Long MRVL Daily ETF | GraniteShares | Marvell Technology (MRVL) | $443.5M | 1.50% |
| **QCML** | `QCML` | +2x | GraniteShares 2x Long QCOM Daily ETF | GraniteShares | Qualcomm (QCOM) | $59.9M | 7.39% |
| **DLLL** | `DLLL` | +2x | GraniteShares 2x Long DELL Daily ETF | GraniteShares | Dell Technologies (DELL) | $215.8M | 3.55% |
| **SMCX** | `SMCX` | +2x | Defiance Daily Target 2x Long SMCI ETF | Defiance ETFs | Super Micro (SMCI) | $160.2M | 1.32% |
| **PTIR** | `PTIR` | +2x | GraniteShares 2x Long PLTR Daily ETF | GraniteShares | Palantir (PLTR) | $331.6M | 1.04% |
| **RKLX** | `RKLX` | +2x | Defiance Daily Target 2X Long RKLB ETF | Defiance ETFs | Rocket Lab (RKLB) | $222.9M | 1.63% |
| **CRWL** | `CRWL` | +2x | GraniteShares 2x Long CRWD Daily ETF | GraniteShares | CrowdStrike (CRWD) | $70.0M | 1.82% |
| **PANG** | `PANG` | +2x | Leverage Shares 2X Long PANW Daily ETF | Leverage Shares | Palo Alto Networks (PANW) | $9.9M | 0.76% |
| **NOWL** | `NOWL` | +2x | GraniteShares 2x Long NOW Daily ETF | GraniteShares | ServiceNow (NOW) | $195.0M | 1.51% |
| **ORCX** | `ORCX` | +2x | Defiance Daily Target 2X Long ORCL ETF | Defiance ETFs | Oracle (ORCL) | $247.1M | 1.31% |
| **APPX** | `APPX` | +2x | Tradr 2X Long APP Daily ETF | AXS Investments | AppLovin (APP) | $68.8M | 1.30% |

## 5. Single-Stock Leveraged (Bull) — Mega-Cap Giants, Crypto & Consumer (22)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **TSLL** | `TSLL` | +2x | Direxion Daily TSLA Bull 2X Shares | Direxion | Tesla (TSLA) | $4,008.0M | 0.95% |
| **TSLT** | `TSLT` | +2x | T-REX 2X Long Tesla Daily Target ETF | Tuttle Capital Management | Tesla (TSLA) | $151.5M | 1.05% |
| **AAPU** | `XNMS:AAPU` | +2x | Direxion Daily AAPL Bull 2X Shares | Direxion | Apple (AAPL) | $176.5M | 0.96% |
| **AAPX** | `AAPX` | +2x | T-Rex 2X Long Apple Daily Target ETF | REX Shares | Apple (AAPL) | $10.2M | 1.05% |
| **MSFU** | `XNMS:MSFU` | +2x | Direxion Daily MSFT Bull 2X Shares | Direxion | Microsoft (MSFT) | $520.4M | 0.98% |
| **MSFL** | `MSFL` | +2x | GraniteShares 2x Long MSFT Daily ETF | GraniteShares | Microsoft (MSFT) | $63.8M | 1.30% |
| **AMZU** | `XNMS:AMZU` | +2x | Direxion Daily AMZN Bull 2X Shares | Direxion | Amazon (AMZN) | $300.0M | 0.99% |
| **GGLL** | `GGLL` | +2x | Direxion Daily GOOGL Bull 2X Shares | Direxion | Alphabet (GOOGL) | $1,035.5M | 0.96% |
| **GOOX** | `GOOX` | +2x | T-Rex 2X Long Alphabet Daily Target ETF | REX Shares | Alphabet (GOOGL) | $64.7M | 1.05% |
| **METU** | `XNMS:METU` | +2x | Direxion Daily META Bull 2X Shares | Direxion | Meta Platforms (META) | $510.3M | 1.02% |
| **FBL** | `FBL` | +2x | GraniteShares 2x Long META Daily ETF | GraniteShares | Meta Platforms (META) | $188.2M | 1.09% |
| **MSTU** | `BATS:MSTU` | +2x | T-Rex 2X Long MSTR Daily Target ETF - USD | Tuttle Capital Management | MicroStrategy (MSTR) | $896.1M | 1.05% |
| **MSTX** | `MSTX` | +2x | Defiance Daily Target 2X Long MSTR ETF | Defiance ETFs | MicroStrategy (MSTR) | $403.6M | 1.31% |
| **CONL** | `CONL` | +2x | GraniteShares 2x Long COIN Daily ETF | GraniteShares | Coinbase (COIN) | $614.6M | 1.04% |
| **LLYX** | `LLYX` | +2x | Defiance Daily Target 2x Long LLY ETF | Defiance ETFs | Eli Lilly (LLY) | $45.0M | 1.49% |
| **UBRL** | `UBRL` | +2x | GraniteShares 2x Long UBER Daily ETF | GraniteShares | Uber Technologies (UBER) | $21.0M | 1.35% |
| **SHPU** | `XNMS:SHPU` | +2x | Direxion Daily SHOP Bull 2X ETF | Direxion | Shopify (SHOP) | $10.9M | 2.07% |
| **SOFX** | `SOFX` | +2x | Defiance Daily Target 2X Long SOFI ETF | Defiance ETFs | SoFi Technologies (SOFI) | $55.2M | 1.33% |
| **HOOG** | `HOOG` | +2x | Leverage Shares 2X Long HOOD Daily ETF | Leverage Shares | Robinhood (HOOD) | $74.5M | 0.85% |
| **BABX** | `BABX` | +2x | GraniteShares 2x Long BABA Daily ETF | GraniteShares | Alibaba (BABA) | $123.0M | 1.20% |
| **LULG** | `LULG` | +2x | Leverage Shares 2X Long LULU Daily ETF | Leverage Shares | Lululemon (LULU) | $13.0M | 0.75% |
| **COTG** | `COTG` | +2x | Leverage Shares 2X Long COST Daily ETF | Leverage Shares | Costco (COST) | $10.6M | 0.77% |

## 6. Commodities, Energy & Volatility (Bull) (9)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **UCO** | `ARCX:UCO` | +2x | ProShares Ultra Bloomberg Crude Oil ETF | ProShares | Crude Oil | $427.6M | 1.47% |
| **UGL** | `UGL` | +2x | ProShares Ultra GOLD ETF | ProShares | Gold | $852.6M | 1.19% |
| **SHNY** | `SHNY` | +3x | MicroSectors Gold 3x Leveraged ETN | BMO | Gold | $113.3M | 0.95% |
| **NUGT** | `ARCX:NUGT` | +2x | Direxion Daily Gold Miners Index Bull 2X ETF | Direxion | Gold Miners | $1,155.8M | 1.13% |
| **JNUG** | `JNUG` | +2x | Direxion Daily Junior Gold Miners Index Bull 2X Shares ETF | Direxion | Junior Gold Miners | $480.0M | 1.03% |
| **AGQ** | `AGQ` | +2x | ProShares Ultra SILVER ETF | ProShares | Silver | $1,454.2M | 1.29% |
| **UVXY** | `BATS:UVXY` | +1.5x | ProShares Ultra VIX Short-Term Futures ETF - USD | ProShares | S&P 500 VIX | $284.6M | 1.23% |
| **SVXY** | `SVXY` | -0.5x | ProShares Short VIX Short-Term Futures ETF | ProShares | S&P 500 VIX (Long Vol Compression) | $258.7M | 1.01% |
| **SVIX** | `SVIX` | -1x | Volatility Shares -1x Short VIX Futures ETF | Volatility Shares | S&P 500 VIX (Long Vol Compression) | $134.4M | 3.93% |

## 7. Fixed Income, Currencies & Crypto (Bull) (8)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **TMF** | `TMF` | +3x | Direxion Daily 20+ Year Treasury Bull 3X Shares ETF - USD | Direxion | 20+ Year Treasury | $2,180.7M | 1.01% |
| **UBT** | `UBT` | +2x | ProShares Ultra 20+ YEAR TREASURY ETF | ProShares | 20+ Year Treasury | $59.0M | 0.97% |
| **TYD** | `TYD` | +3x | Direxion Daily 7-10 Year Treasury Bull 3X Shares ETF | Direxion | 7-10 Year Treasury | $27.9M | 1.08% |
| **ULE** | `ULE` | +2x | ProShares Ultra Euro ETF | ProShares | EUR/USD | $4.4M | 0.98% |
| **YCL** | `YCL` | +2x | ProShares Ultra Yen ETF | ProShares | JPY/USD | $31.1M | 0.98% |
| **BITX** | `BITX` | +2x | 2x Bitcoin ETF | Volatility Shares | Bitcoin | $1,394.0M | 2.75% |
| **BITU** | `BITU` | +2x | ProShares Ultra Bitcoin ETF | ProShares | Bitcoin | $642.9M | 0.98% |
| **ETHU** | `ETHU` | +2x | Volatility Shares 2x Ether ETF - USD | Volatility Shares | Ether | $1,466.5M | 2.97% |

## 8. International / Country (Bull) (5)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **YINN** | `YINN` | +3x | Direxion Daily China Bull 3x Shares ETF - USD | Direxion | China 50 | $617.9M | 1.34% |
| **INDL** | `INDL` | +2x | Direxion Daily India Bull 3X Shares ETF | Direxion | India | $53.5M | 1.23% |
| **KORU** | `KORU` | +3x | Direxion Daily South Korea Bull 3X Shares ETF | Direxion | South Korea | $1,452.0M | 1.32% |
| **BRZU** | `BRZU` | +2x | Direxion Daily Brazil Bull 2X Shares ETF | Direxion | Brazil | $117.2M | 1.32% |
| **EDC** | `EDC` | +3x | Direxion Daily MSCI Emerging Markets Bull 3X Shares | Direxion | Emerging Markets | $192.8M | 1.12% |

## 9. Selective Benchmark Hedging / Tactical Shorts (Pruned to Key Anchors Only) (10)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **SQQQ** | `XNMS:SQQQ` | -3x | ProShares UltraPro Short QQQ ETF - USD | ProShares | NASDAQ-100 (Primary Tech Hedge) | $1,863.0M | 0.99% |
| **SPXS** | `ARCX:SPXS` | -3x | Direxion Daily S&P 500 Bear 3X Shares | Direxion | S&P 500 (Primary Market Hedge) | $346.1M | 1.04% |
| **TZA** | `TZA` | -3x | Direxion Daily Small Cap Bear 3x Shares ETF - USD | Direxion | Russell 2000 (Small-Cap Risk-Off) | $249.1M | 0.99% |
| **SOXS** | `ARCX:SOXS` | -3x | Direxion Daily Semiconductor Bear 3X Shares ETF - USD | Direxion | Semiconductors (Cycle Downturn) | $1,672.8M | 1.00% |
| **SCO** | `ARCX:SCO` | -2x | ProShares UltraShort Bloomberg Crude Oil ETF | ProShares | Crude Oil (Bearish Commodity) | $1,075.8M | 1.10% |
| **TMV** | `TMV` | -3x | Direxion Daily 20 Year Plus Treasury Bear 3X Shares ETF | Direxion | 20+ Year Treasury (Yield Spike / Inflation) | $211.7M | 0.97% |
| **YANG** | `YANG` | -3x | Direxion Daily China Bear 3x Shares ETF | Direxion | China 50 (Geopolitical / China Risk) | $103.5M | 1.03% |
| **NVD** | `NVD` | -2x | GraniteShares 2x Short NVDA Daily ETF | GraniteShares | NVIDIA (Single-Stock Tactical Short) | $54.9M | 1.35% |
| **TSLZ** | `TSLZ` | -2x | T-REX 2X Inverse Tesla Daily Target ETF | Tuttle Capital Management | Tesla (Single-Stock Tactical Short) | $27.9M | 1.05% |
| **MSTZ** | `BATS:MSTZ` | -2x | T-Rex 2X Inverse MSTR Daily Target ETF | Tuttle Capital Management | MicroStrategy (Bitcoin Pullback Hedge) | $77.4M | 1.05% |

## Local source and validation

The authoritative source files are `data/flows/curated_catalog_summary.json`, `data/flows/individual/{TICKER}_flows.csv`, `data/flows/all_leveraged_etf_flows.csv`, and `data/flows/Leveraged_ETF_Flows_Master.xlsx`. The validated local universe contains 117 instruments and 170,392 data rows from 2016-01-04 through 2026-09-23. Static UI artifacts are rebuilt from those local files only; external scraping is intentionally disabled.
