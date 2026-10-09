# Curated Leveraged & Tactical ETF Universe — Fund Flow Tracking

**Total Instruments:** 150 Curated Leveraged ETFs across 9 professional categories.

> **Selection Thesis & Architecture:**
> 1. **Aggressively Long-Biased (136 Bull vs 14 Tactical Short Anchors)**: Recognizes the multi-decade structural upward drift of US equity markets. Pruned out decaying low-conviction inverse ETFs while preserving high-conviction directional leaders.
> 2. **Comprehensive AI, Semiconductor & Tech Hardware Coverage**: Full coverage of the AI hardware supply chain from foundries, memory/storage, and capital equipment to optical networking and power semiconductors (**PLTU/PTIR** Palantir, **NVDL/NVDX/NVDU** NVIDIA, **MUU** Micron, **AVGX** Broadcom, **RKLX** Rocket Lab, **SOFX** SoFi, **APPX** AppLovin, **METU** Meta, **AMZU** Amazon, **MSFU** Microsoft, **GOOX** Google, **LITX** Lumentum, **COHX** Coherent, **TSLL** Tesla, **MSTU/MSTX/SMST** MicroStrategy, **CONL** Coinbase).
> 3. **Selective Hedging Anchors Only (14 ETFs)**: Retained only the highest-liquidity benchmark shorts for tracking institutional panic hedging and capitulation bottoms (SQQQ, SPXS, TZA, SOXS, SCO, TMV, YANG, NVD, TSLZ, MSTZ, PLTD, TSLQ, NVDS).
> 4. **Historical Local Source**: The 150 rows below are the authoritative local leveraged/inverse universe. Historical flow, NAV, and derived fields are Trackinsight-reported values already stored in the local dataset; this document does not assert a live fetch or current API status.

---

## Category Summary

| Category # | Category Name | ETF Count | Orientation | Key Themes / Anchors |
|---|---|---|---|---|
| 1 | 1. Broad Market Equity Index (Bull) | 9 | Bull (+2x / +3x) | NASDAQ-100, S&P 500, Russell 2000... |
| 2 | 2. Technology, Semiconductor & Thematic (Bull) | 16 | Bull (+2x / +3x) | Semiconductors, Technology Select, Dow Jones Internet, High Beta... |
| 3 | 3. Sector Specific Leveraged (Bull) | 11 | Bull (+2x / +3x) | Financials Select, Regional Banking, Aerospace & Defense, Retail... |
| 4 | 4. Single-Stock Leveraged (Bull) — AI, Semis & High-Beta Tech | 45 | Bull (+2x / +3x) | Palantir, NVIDIA, Micron, Broadcom, Rocket Lab, AppLovin, SanDisk... |
| 5 | 5. Single-Stock Leveraged (Bull) — Mega-Cap Giants, Crypto & Consumer | 31 | Bull (+2x / +3x) | Apple, Tesla, Meta, Amazon, Microsoft, Alphabet, MicroStrategy, Netflix, SoFi... |
| 6 | 6. Commodities, Energy & Volatility (Bull) | 9 | Bull (+2x / +3x) | Crude Oil, Gold, Gold Miners, Silver... |
| 7 | 7. Fixed Income, Currencies & Crypto (Bull) | 7 | Bull (+2x / +3x) | Bitcoin, Ether, 20+ Year Treasury, 7-10 Year Treasury... |
| 8 | 8. International / Country (Bull) | 8 | Bull (+2x / +3x) | Brazil, China 50, India, South Korea, Emerging Markets... |
| 9 | 9. Selective Benchmark Hedging / Tactical Shorts (Pruned to Key Anchors Only) | 14 | Tactical Short | NASDAQ-100, Russell 2000, Semiconductors, Palantir Bear, Tesla Bear, NVIDIA Bear... |

---

## Featured 24

The Fund Flows interface uses this explicit, deterministic 24-instrument subset drawn only from the 150 local leveraged/inverse rows above:

`TQQQ`, `QLD`, `UPRO`, `SPXL`, `SOXL`, `TECL`, `USD`, `DFEN`, `FAS`, `ERX`, `CURE`, `NVDL`, `NVDX`, `LITX`, `COHX`, `PTIR`, `TSLL`, `MSTU`, `CONL`, `UGL`, `NUGT`, `TMF`, `YINN`, `SQQQ`

There is no long-only catalog, watch tier, or missing-underlying watchlist in this universe.

---

## 1. Broad Market Equity Index (Bull) (9)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **MIDU** | `MIDU` | +3x | Direxion Daily Mid Cap Bull 3X Shares | Direxion | S&P MidCap 400 | $68.0M | 0.95% |
| **QLD** | `QLD` | +2x | ProShares Ultra QQQ ETF | ProShares | NASDAQ-100 | $15,144.9M | 0.98% |
| **SPXL** | `SPXL` | +3x | Direxion Daily S&P 500 Bull 3X Shares | Direxion | S&P 500 | $7,408.6M | 0.95% |
| **SSO** | `ARCX:SSO` | +2x | ProShares Ultra S&P500 ETF | ProShares | S&P 500 | $9,106.3M | 0.88% |
| **TNA** | `TNA` | +3x | Direxion Daily Small Cap Bull 3x Shares ETF | Direxion | Russell 2000 | $1,263.5M | 1.07% |
| **TQQQ** | `TQQQ` | +3x | ProShares UltraPro QQQ ETF | ProShares | NASDAQ-100 | $39,723.4M | 0.97% |
| **UDOW** | `UDOW` | +3x | ProShares UltraPro DOW30 ETF | ProShares | Dow Jones 30 | $871.5M | 0.95% |
| **UPRO** | `UPRO` | +3x | ProShares UltraPro S&P500 ETF | ProShares | S&P 500 | $5,715.9M | 0.89% |
| **URTY** | `URTY` | +3x | ProShares UltraPro RUSSELL2000 ETF | ProShares | Russell 2000 | $296.0M | 1.08% |

## 2. Technology, Semiconductor & Thematic (Bull) (16)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **BIB** | `BIB` | +2x | ProShares Ultra Nasdaq Biotechnology ETF | ProShares | Biotech | $95.3M | 1.19% |
| **CWEB** | `CWEB` | +2x | Direxion Daily CSI China Internet Bull 2X Shares | Direxion | China Internet | $450.0M | 0.95% |
| **DFEN** | `ARCX:DFEN` | +3x | Direxion Daily Aerospace & Defense Bull 3X Shares ETF | Direxion | Aerospace & Defense | $312.4M | 0.96% |
| **HIBL** | `HIBL` | +3x | Direxion Daily S&P 500 High Beta Bull 3X ETF | Direxion | S&P 500 High Beta | $80.1M | 1.01% |
| **LABU** | `LABU` | +3x | Direxion Daily S&P Biotech Bull 3X Shares ETF - USD | Direxion | S&P Biotech | $600.8M | 0.96% |
| **MAGX** | `MAGX` | +2x | Roundhill Daily 2X Long Magnificent Seven ETF | Roundhill Investments | Magnificent Seven | $68.9M | 0.96% |
| **MSOX** | `MSOX` | +2x | AdvisorShares MSOS Daily Leveraged ETF | AdvisorShares | U.S. Cannabis | $67.1M | 1.42% |
| **NAIL** | `NAIL` | +3x | Direxion Daily Homebuilders & Supplies Bull 3X Shares ETF | Direxion | Homebuilders | $546.2M | 0.96% |
| **QQQU** | `ARCX:QQQU` | +2x | Direxion Daily Magnificent 7 Bull 2X Shares ETF | Direxion | Magnificent Seven | $80.3M | 1.00% |
| **ROM** | `ROM` | +2x | ProShares Ultra TECHNOLOGY ETF | ProShares | U.S. Technology | $1,373.6M | 0.95% |
| **SMHX** | `SMHX` | +2x | Direxion Daily Semiconductor Bull 2X Shares | Direxion | Semiconductors | $35.0M | 0.95% |
| **SOXL** | `ARCX:SOXL` | +3x | Direxion Daily Semiconductor Bull 3X Shares ETF | Direxion | Semiconductors | $24,161.0M | 0.91% |
| **TARK** | `TARK` | +2x | Tradr 2X Long Innovation ETF | AXS Investments | ARKK Innovation | $16.4M | 1.39% |
| **TECL** | `TECL` | +3x | Direxion Daily Technology Bull 3X Shares ETF | Direxion | Technology Select | $6,548.9M | 0.94% |
| **USD** | `ARCX:USD` | +2x | ProShares Ultra SEMICONDUCTORS ETF | ProShares | Semiconductors | $2,926.4M | 0.95% |
| **WEBL** | `WEBL` | +3x | Direxion Daily Dow Jones Internet Bull 3X Shares | Direxion | Dow Jones Internet | $82.0M | 0.99% |

## 3. Sector Specific Leveraged (Bull) (11)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **CURE** | `ARCX:CURE` | +3x | Direxion Daily Healthcare Bull 3X Shares ETF | Direxion | Health Care Select | $170.5M | 0.94% |
| **DIG** | `DIG` | +2x | ProShares Ultra Energy ETF | ProShares | Oil & Gas | $97.4M | 1.07% |
| **DPST** | `DPST` | +3x | Direxion Daily Regional Banks Bull 3X Shares ETF - USD | Direxion | Regional Banking | $322.6M | 0.92% |
| **DRN** | `DRN` | +3x | Direxion Daily Real Estate Bull 3x Shares ETF | Direxion | MSCI US REIT | $38.3M | 0.98% |
| **ERX** | `ERX` | +2x | Direxion Daily Energy Bull 2x Shares ETF | Direxion | Energy Select | $240.5M | 0.91% |
| **FAS** | `FAS` | +3x | Direxion Daily Financial Bull 3x Shares ETF | Direxion | Financials Select | $2,093.1M | 0.92% |
| **PILL** | `PILL` | +3x | Direxion Daily Pharmaceutical & Medical Bull 3X Shares | Direxion | Pharmaceuticals | $48.0M | 0.96% |
| **RETL** | `RETL` | +3x | Direxion Daily Retail Bull 3X Shares | Direxion | Retail | $48.0M | 0.98% |
| **URE** | `URE` | +2x | ProShares Ultra Real Estate ETF | ProShares | U.S. Real Estate | $51.4M | 1.10% |
| **UYG** | `UYG` | +2x | ProShares Ultra Financials ETF | ProShares | U.S. Financials | $773.8M | 0.94% |
| **WANT** | `WANT` | +3x | Direxion Daily Consumer Discretionary Bull 3X Shares | Direxion | Consumer Discretionary | $35.0M | 0.95% |

## 4. Single-Stock Leveraged (Bull) — AI, Semis & High-Beta Tech (45)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **AAOX** | `AAOX` | +2x | Tradr 2X Long AAOI Daily ETF | AXS Investments | Applied Optoelectronics (AAOI) | $248.1M | 1.49% |
| **AMDL** | `AMDL` | +2x | GraniteShares 2x Long AMD Daily ETF | GraniteShares | AMD (AMD) | $1,065.7M | 1.07% |
| **APLX** | `APLX` | +2x | Tradr 2X Long APLD Daily ETF | AXS Investments | Applied Digital (APLD) | $64.1M | 1.30% |
| **APPX** | `APPX` | +2x | Tradr 2X Long APP Daily ETF | AXS Investments | AppLovin (APP) | $68.8M | 1.30% |
| **ARMG** | `ARMG` | +2x | Leverage Shares 2X Long ARM Daily ETF | Themes Management Company | ARM Holdings (ARM) | $92.3M | 0.78% |
| **ASMG** | `ASMG` | +2x | Leverage Shares 2X Long ASML Daily ETF | Themes Management Company | ASML Holding (ASML) | $32.1M | 0.77% |
| **AVGX** | `AVGX` | +2x | Defiance Daily Target 2x Long AVGO ETF | Defiance ETFs | Broadcom (AVGO) | $226.0M | 1.31% |
| **AXTX** | `AXTX` | +2x | Tradr 2X Long AXTI Daily ETF | AXS Investments | AXT Inc (AXTI) | $180.2M | 1.49% |
| **COHX** | `COHX` | +2x | Tradr 2X Long COHR Daily ETF | AXS Investments | Coherent Corp (COHR) | $129.1M | 1.49% |
| **CRDU** | `CRDU` | +2x | Tradr 2X Long CRDO Daily ETF | AXS Investments | Credo Technology (CRDO) | $144.9M | 1.30% |
| **CRML** | `CRML` | +2x | Direxion Daily CRM Bull 2X Shares | Direxion | Salesforce | $65.0M | 0.95% |
| **CRWL** | `CRWL` | +2x | GraniteShares 2x Long CRWD Daily ETF | GraniteShares | CrowdStrike (CRWD) | $70.0M | 1.82% |
| **CSEX** | `CSEX` | +2x | Tradr 2X Long CLS Daily ETF | AXS Investments | Celestica (CLS) | $16.6M | 1.30% |
| **CWVX** | `CWVX` | +2x | Tradr 2X Long CRWV Daily ETF | AXS Investments | CoreWeave (CRWV) | $97.9M | 1.30% |
| **DLLL** | `DLLL` | +2x | GraniteShares 2x Long DELL Daily ETF | GraniteShares | Dell Technologies (DELL) | $215.8M | 3.55% |
| **GLWG** | `GLWG` | +2x | Leverage Shares 2X Long GLW Daily ETF | Leverage Shares | Corning (GLW) | $129.4M | 0.75% |
| **INTW** | `INTW` | +2x | GraniteShares 2x Long INTC Daily ETF | GraniteShares | Intel (INTC) | $349.3M | 1.85% |
| **LABX** | `LABX` | +2x | Tradr 2X Long ALAB Daily ETF | AXS Investments | Astera Labs (ALAB) | $89.5M | 1.30% |
| **LITX** | `LITX` | +2x | Tradr 2X Long LITE Daily ETF | AXS Investments | Lumentum Holdings (LITE) | $207.8M | 1.49% |
| **LRCU** | `LRCU` | +2x | Tradr 2X Long LRCX Daily ETF | AXS Investments | Lam Research (LRCX) | $38.9M | 1.30% |
| **MULL** | `MULL` | +2x | GraniteShares 2x Long MU Daily ETF | GraniteShares | Micron Technology (MU) | $741.3M | 3.06% |
| **MUU** | `MUU` | +2x | Direxion Daily MU Bull 2X Shares | Direxion | Micron Technology | $95.0M | 0.95% |
| **MVLL** | `MVLL` | +2x | GraniteShares 2x Long MRVL Daily ETF | GraniteShares | Marvell Technology (MRVL) | $443.5M | 1.50% |
| **NEBX** | `NEBX` | +2x | Tradr 2X Long NBIS Daily ETF | AXS Investments | Nebius Group (NBIS) | $181.9M | 1.30% |
| **NOWL** | `NOWL` | +2x | GraniteShares 2x Long NOW Daily ETF | GraniteShares | ServiceNow (NOW) | $195.0M | 1.51% |
| **NVDL** | `NVDL` | +2x | GraniteShares 2x Long NVDA Daily ETF | GraniteShares | NVIDIA (NVDA) | $3,722.8M | 1.06% |
| **NVDU** | `NVDU` | +2x | Direxion Daily NVDA Bull 2X Shares | Direxion | NVIDIA | $140.0M | 0.95% |
| **NVDX** | `NVDX` | +2x | T-Rex 2X Long NVIDIA Daily Target ETF | Tuttle Capital Management | NVIDIA (NVDA) | $495.8M | 1.05% |
| **NVTX** | `NVTX` | +2x | Tradr 2X Long NVTS Daily ETF | AXS Investments | Navitas Semiconductor (NVTS) | $33.4M | 1.30% |
| **ORCX** | `ORCX` | +2x | Defiance Daily Target 2X Long ORCL ETF | Defiance ETFs | Oracle (ORCL) | $247.1M | 1.31% |
| **PANG** | `PANG` | +2x | Leverage Shares 2X Long PANW Daily ETF | Leverage Shares | Palo Alto Networks (PANW) | $9.9M | 0.76% |
| **PLTU** | `PLTU` | +2x | Direxion Daily PLTR Bull 2X Shares | Direxion | Palantir Technologies | $120.5M | 0.95% |
| **PTIR** | `PTIR` | +2x | GraniteShares 2x Long PLTR Daily ETF | GraniteShares | Palantir (PLTR) | $331.6M | 1.04% |
| **QBTX** | `QBTX` | +2x | Tradr 2X Long QBTS Daily ETF | AXS Investments | D-Wave Quantum (QBTS) | $74.7M | 1.30% |
| **QCML** | `QCML` | +2x | GraniteShares 2x Long QCOM Daily ETF | GraniteShares | Qualcomm (QCOM) | $59.9M | 7.39% |
| **RGTU** | `RGTU` | +2x | Tradr 2X Long RGTI Daily ETF | AXS Investments | Rigetti Computing (RGTI) | $9.8M | 1.30% |
| **RKLX** | `RKLX` | +2x | Defiance Daily Target 2X Long RKLB ETF | Defiance ETFs | Rocket Lab (RKLB) | $222.9M | 1.63% |
| **SMCX** | `SMCX` | +2x | Defiance Daily Target 2x Long SMCI ETF | Defiance ETFs | Super Micro (SMCI) | $160.2M | 1.32% |
| **SNOU** | `SNOU` | +2x | Direxion Daily SNOW Bull 2X Shares | Direxion | Snowflake | $42.0M | 0.95% |
| **SNXX** | `SNXX` | +2x | Tradr 2X Long SNDK Daily ETF | AXS Investments | SanDisk (SNDK) | $1,972.5M | 1.49% |
| **STXX** | `STXX` | +2x | Tradr 2X Long STX Daily ETF | AXS Investments | Seagate Technology (STX) | $13.9M | 1.49% |
| **TEMT** | `TEMT` | +2x | Tradr 2X Long TEM Daily ETF | AXS Investments | Tempus AI (TEM) | $46.4M | 1.30% |
| **TSMU** | `TSMU` | +2x | GraniteShares 2x Long TSM Daily ETF | GraniteShares | Taiwan Semi (TSM) | $45.7M | 2.45% |
| **TTMX** | `TTMX` | +2x | Tradr 2X Long TTMI Daily ETF | AXS Investments | TTM Technologies (TTMI) | $2.1M | 1.30% |
| **WDCX** | `WDCX` | +2x | Tradr 2X Long WDC Daily ETF | AXS Investments | Western Digital (WDC) | $99.2M | 1.49% |

## 5. Single-Stock Leveraged (Bull) — Mega-Cap Giants, Crypto & Consumer (31)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **AAPU** | `XNMS:AAPU` | +2x | Direxion Daily AAPL Bull 2X Shares | Direxion | Apple (AAPL) | $176.5M | 0.96% |
| **AMZU** | `XNMS:AMZU` | +2x | Direxion Daily AMZN Bull 2X Shares | Direxion | Amazon (AMZN) | $300.0M | 0.99% |
| **ASTX** | `BATS:ASTX` | +2x | Tradr 2X Long ASTS Daily ETF | AXS Investments | AST SpaceMobile (ASTS) | $258.8M | 1.30% |
| **BABX** | `BABX` | +2x | GraniteShares 2x Long BABA Daily ETF | GraniteShares | Alibaba (BABA) | $123.0M | 1.20% |
| **BEX** | `BEX` | +2x | Tradr 2X Long BE Daily ETF | AXS Investments | Bloom Energy (BE) | $218.2M | 1.30% |
| **CONL** | `CONL` | +2x | GraniteShares 2x Long COIN Daily ETF | GraniteShares | Coinbase (COIN) | $614.6M | 1.04% |
| **COTG** | `COTG` | +2x | Leverage Shares 2X Long COST Daily ETF | Leverage Shares | Costco (COST) | $10.6M | 0.77% |
| **FBL** | `FBL` | +2x | GraniteShares 2x Long META Daily ETF | GraniteShares | Meta Platforms (META) | $188.2M | 1.09% |
| **GEVX** | `GEVX` | +2x | Tradr 2X Long GEV Daily ETF | AXS Investments | GE Vernova (GEV) | $36.8M | 1.30% |
| **GGLL** | `GGLL` | +2x | Direxion Daily GOOGL Bull 2X Shares | Direxion | Alphabet (GOOGL) | $1,035.5M | 0.96% |
| **GOOX** | `GOOX` | +2x | T-Rex 2X Long Alphabet Daily Target ETF | REX Shares | Alphabet (GOOGL) | $64.7M | 1.05% |
| **HOOG** | `HOOG` | +2x | Leverage Shares 2X Long HOOD Daily ETF | Leverage Shares | Robinhood (HOOD) | $74.5M | 0.85% |
| **IREX** | `IREX` | +2x | Tradr 2X Long IREN Daily ETF | AXS Investments | IREN (IREN) | $65.2M | 1.30% |
| **LLYX** | `LLYX` | +2x | Defiance Daily Target 2x Long LLY ETF | Defiance ETFs | Eli Lilly (LLY) | $45.0M | 1.49% |
| **LULG** | `LULG` | +2x | Leverage Shares 2X Long LULU Daily ETF | Leverage Shares | Lululemon (LULU) | $13.0M | 0.75% |
| **METU** | `XNMS:METU` | +2x | Direxion Daily META Bull 2X Shares | Direxion | Meta Platforms (META) | $510.3M | 1.02% |
| **MSFL** | `MSFL` | +2x | GraniteShares 2x Long MSFT Daily ETF | GraniteShares | Microsoft (MSFT) | $63.8M | 1.30% |
| **MSFU** | `XNMS:MSFU` | +2x | Direxion Daily MSFT Bull 2X Shares | Direxion | Microsoft (MSFT) | $520.4M | 0.98% |
| **MSTU** | `BATS:MSTU` | +2x | T-Rex 2X Long MSTR Daily Target ETF - USD | Tuttle Capital Management | MicroStrategy (MSTR) | $896.1M | 1.05% |
| **MSTX** | `MSTX` | +2x | Defiance Daily Target 2X Long MSTR ETF | Defiance ETFs | MicroStrategy (MSTR) | $403.6M | 1.31% |
| **NFXL** | `NFXL` | +2x | Direxion Daily NFLX Bull 2X Shares | Direxion | Netflix | $42.0M | 0.95% |
| **SHPU** | `XNMS:SHPU` | +2x | Direxion Daily SHOP Bull 2X ETF | Direxion | Shopify (SHOP) | $10.9M | 2.07% |
| **SMST** | `SMST` | +2x | T-Rex 2X Long MSTR Daily Target ETF | T-Rex | MicroStrategy | $210.0M | 1.05% |
| **SMU** | `SMU` | +2x | Tradr 2X Long SMR Daily ETF | AXS Investments | NuScale Power (SMR) | $59.3M | 1.30% |
| **SOFX** | `SOFX` | +2x | Defiance Daily Target 2X Long SOFI ETF | Defiance ETFs | SoFi Technologies (SOFI) | $55.2M | 1.33% |
| **TSLL** | `TSLL` | +2x | Direxion Daily TSLA Bull 2X Shares | Direxion | Tesla (TSLA) | $4,008.0M | 0.95% |
| **TSLR** | `TSLR` | +2x | GraniteShares 2x Long TSLA Daily ETF | GraniteShares | Tesla | $85.0M | 1.15% |
| **TSLT** | `TSLT` | +2x | T-REX 2X Long Tesla Daily Target ETF | Tuttle Capital Management | Tesla (TSLA) | $151.5M | 1.05% |
| **UBRL** | `UBRL` | +2x | GraniteShares 2x Long UBER Daily ETF | GraniteShares | Uber Technologies (UBER) | $21.0M | 1.35% |
| **UPSX** | `UPSX` | +2x | Tradr 2X Long UPST Daily ETF | AXS Investments | Upstart Holdings (UPST) | $17.9M | 1.30% |
| **WULX** | `WULX` | +2x | Tradr 2X Long WULF Daily ETF | AXS Investments | TeraWulf (WULF) | $24.1M | 1.30% |

## 6. Commodities, Energy & Volatility (Bull) (9)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **AGQ** | `AGQ` | +2x | ProShares Ultra SILVER ETF | ProShares | Silver | $1,454.2M | 1.29% |
| **GDXU** | `GDXU` | +3x | MicroSectors Gold Miners 3X Leveraged ETN | BMO | Gold Miners | $380.0M | 0.95% |
| **JNUG** | `JNUG` | +2x | Direxion Daily Junior Gold Miners Index Bull 2X Shares ETF | Direxion | Junior Gold Miners | $480.0M | 1.03% |
| **NUGT** | `ARCX:NUGT` | +2x | Direxion Daily Gold Miners Index Bull 2X ETF | Direxion | Gold Miners | $1,155.8M | 1.13% |
| **SVIX** | `SVIX` | -1x | Volatility Shares -1x Short VIX Futures ETF | Volatility Shares | S&P 500 VIX (Long Vol Compression) | $134.4M | 3.93% |
| **SVXY** | `SVXY` | -0.5x | ProShares Short VIX Short-Term Futures ETF | ProShares | S&P 500 VIX (Long Vol Compression) | $258.7M | 1.01% |
| **UCO** | `ARCX:UCO` | +2x | ProShares Ultra Bloomberg Crude Oil ETF | ProShares | Crude Oil | $427.6M | 1.47% |
| **UGL** | `UGL` | +2x | ProShares Ultra GOLD ETF | ProShares | Gold | $852.6M | 1.19% |
| **UVXY** | `BATS:UVXY` | +1.5x | ProShares Ultra VIX Short-Term Futures ETF - USD | ProShares | S&P 500 VIX | $284.6M | 1.23% |

## 7. Fixed Income, Currencies & Crypto (Bull) (7)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **BITU** | `BITU` | +2x | ProShares Ultra Bitcoin ETF | ProShares | Bitcoin | $642.9M | 0.98% |
| **BITX** | `BITX` | +2x | 2x Bitcoin ETF | Volatility Shares | Bitcoin | $1,394.0M | 2.75% |
| **ETHU** | `ETHU` | +2x | Volatility Shares 2x Ether ETF - USD | Volatility Shares | Ether | $1,466.5M | 2.97% |
| **TMF** | `TMF` | +3x | Direxion Daily 20+ Year Treasury Bull 3X Shares ETF - USD | Direxion | 20+ Year Treasury | $2,180.7M | 1.01% |
| **TYD** | `TYD` | +3x | Direxion Daily 7-10 Year Treasury Bull 3X Shares ETF | Direxion | 7-10 Year Treasury | $27.9M | 1.08% |
| **UBT** | `UBT` | +2x | ProShares Ultra 20+ YEAR TREASURY ETF | ProShares | 20+ Year Treasury | $59.0M | 0.97% |
| **YCL** | `YCL` | +2x | ProShares Ultra Yen ETF | ProShares | JPY/USD | $31.1M | 0.98% |

## 8. International / Country (Bull) (8)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **BRZU** | `BRZU` | +2x | Direxion Daily Brazil Bull 2X Shares ETF | Direxion | Brazil | $117.2M | 1.32% |
| **CHAU** | `CHAU` | +2x | Direxion Daily CSI 300 China A-Share Bull 2X Shares | Direxion | China A-Shares | $110.0M | 0.95% |
| **EDC** | `EDC` | +3x | Direxion Daily MSCI Emerging Markets Bull 3X Shares | Direxion | Emerging Markets | $192.8M | 1.12% |
| **EURL** | `EURL` | +3x | Direxion Daily MSCI Europe Bull 3X Shares | Direxion | Europe | $45.0M | 0.95% |
| **INDL** | `INDL` | +2x | Direxion Daily India Bull 3X Shares ETF | Direxion | India | $53.5M | 1.23% |
| **KORU** | `KORU` | +3x | Direxion Daily South Korea Bull 3X Shares ETF | Direxion | South Korea | $1,452.0M | 1.32% |
| **MEXX** | `MEXX` | +3x | Direxion Daily MSCI Mexico Bull 3X Shares | Direxion | Mexico | $32.0M | 0.95% |
| **YINN** | `YINN` | +3x | Direxion Daily China Bull 3x Shares ETF - USD | Direxion | China 50 | $617.9M | 1.34% |

## 9. Selective Benchmark Hedging / Tactical Shorts (Pruned to Key Anchors Only) (14)

| Ticker | Trackinsight Key | Leverage | Fund Name | Issuer | Underlying | AUM ($M) | TER |
|---|---|---|---|---|---|---|---|
| **CONI** | `CONI` | -2x | GraniteShares 2x Short COIN Daily ETF | GraniteShares | Coinbase Global | $38.0M | 1.15% |
| **MSTZ** | `BATS:MSTZ` | -2x | T-Rex 2X Inverse MSTR Daily Target ETF | Tuttle Capital Management | MicroStrategy (Bitcoin Pullback Hedge) | $77.4M | 1.05% |
| **NVD** | `NVD` | -2x | GraniteShares 2x Short NVDA Daily ETF | GraniteShares | NVIDIA (Single-Stock Tactical Short) | $54.9M | 1.35% |
| **NVDS** | `NVDS` | -1.25x | AXS 1.25X NVDA Bear Daily ETF | AXS Investments | NVIDIA | $64.2M | 1.15% |
| **PLTD** | `PLTD` | -1x | Direxion Daily PLTR Bear 1X Shares | Direxion | Palantir Technologies | $85.0M | 0.95% |
| **SCO** | `ARCX:SCO` | -2x | ProShares UltraShort Bloomberg Crude Oil ETF | ProShares | Crude Oil (Bearish Commodity) | $1,075.8M | 1.10% |
| **SOXS** | `ARCX:SOXS` | -3x | Direxion Daily Semiconductor Bear 3X Shares ETF - USD | Direxion | Semiconductors (Cycle Downturn) | $1,672.8M | 1.00% |
| **SPXS** | `ARCX:SPXS` | -3x | Direxion Daily S&P 500 Bear 3X Shares | Direxion | S&P 500 (Primary Market Hedge) | $346.1M | 1.04% |
| **SQQQ** | `XNMS:SQQQ` | -3x | ProShares UltraPro Short QQQ ETF - USD | ProShares | NASDAQ-100 (Primary Tech Hedge) | $1,863.0M | 0.99% |
| **TMV** | `TMV` | -3x | Direxion Daily 20 Year Plus Treasury Bear 3X Shares ETF | Direxion | 20+ Year Treasury (Yield Spike / Inflation) | $211.7M | 0.97% |
| **TSLQ** | `TSLQ` | -1x | AXS TSLA Bear 1X Daily ETF | AXS Investments | Tesla | $32.4M | 1.15% |
| **TSLZ** | `TSLZ` | -2x | T-REX 2X Inverse Tesla Daily Target ETF | Tuttle Capital Management | Tesla (Single-Stock Tactical Short) | $27.9M | 1.05% |
| **TZA** | `TZA` | -3x | Direxion Daily Small Cap Bear 3x Shares ETF - USD | Direxion | Russell 2000 (Small-Cap Risk-Off) | $249.1M | 0.99% |
| **YANG** | `YANG` | -3x | Direxion Daily China Bear 3x Shares ETF | Direxion | China 50 (Geopolitical / China Risk) | $103.5M | 1.03% |

