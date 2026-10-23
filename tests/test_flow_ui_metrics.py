import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FLOW_MODULE = "./docs/flow-ui.js"


def run_metrics(expression):
    script = f"const m=require('{FLOW_MODULE}');console.log(JSON.stringify({expression}));"
    result = subprocess.run(
        ["node", "-e", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_money_format_preserves_null_and_neutral_zero():
    values = run_metrics("[m.formatMoney(null),m.formatMoney(undefined),m.formatMoney(0),m.formatMoney(-0),m.formatMoney(1500000)]")
    assert values == ["—", "—", "$0", "$0", "+$1.5M"]


def test_complete_rolling_windows_never_fill_missing_values():
    rolling = run_metrics("m.completeRollingSums([{flow:1},{flow:null},{flow:3},{flow:4},{flow:5},{flow:6}],3)")
    assert rolling == [None, None, None, None, 12, 15]
    means = run_metrics("m.completeRollingMeans([{flow:1},{flow:null},{flow:3},{flow:4}],2)")
    assert means == [None, None, None, 3.5]


def test_percentile_uses_midrank_for_ties_and_complete_distribution():
    values = run_metrics("[m.empiricalPercentile([1,2,2,4],2),m.empiricalPercentile([1,2,2,4],3),m.empiricalPercentile([5,5,5],5),m.empiricalPercentile([],5),m.empiricalPercentile([1,2],null)]")
    assert values == [50, 75, 50, None, None]


def test_prior_only_z_score_excludes_current_observation():
    values = run_metrics("(()=>{const prior=[1,2,3,4,4];const mean=prior.reduce((a,b)=>a+b,0)/prior.length;const sd=Math.sqrt(prior.reduce((a,b)=>a+(b-mean)**2,0)/prior.length);return [m.priorOnlyZScore([{flow:1},{flow:2},{flow:3},{flow:4},{flow:4},{flow:100}],5,30),(100-mean)/sd,m.priorOnlyZScore([{flow:1},{flow:2},{flow:3},{flow:4},{flow:4},{flow:200}],5,30),(200-mean)/sd,m.priorOnlyZScore([{flow:0},{flow:0},{flow:0},{flow:0},{flow:0},{flow:1}],5,30),m.priorOnlyZScore([{flow:1},{flow:2}],1,30)];})()")
    assert abs(values[0] - values[1]) < 1e-12
    assert abs(values[2] - values[3]) < 1e-12
    assert values[4] is None
    assert values[5] is None


def test_selected_window_cumulative_carries_across_missing_without_creating_zero_flow():
    result = run_metrics("(()=>{const x=m.buildWindowMetrics([{date:'2026-01-01',flow:10},{date:'2026-01-02',flow:null},{date:'2026-01-03',flow:0},{date:'2026-01-04',flow:5}],0,3);return {rows:x.selected.map(r=>[r.flow,r.selectedCumulative]),percentile:x.latest.percentile10};})()")
    assert result["rows"] == [[10, 10], [None, 10], [0, 10], [5, 15]]
    assert result["percentile"] is None
    missing = run_metrics("m.buildWindowMetrics([{date:'2026-01-01',flow:null},{date:'2026-01-02',flow:null}],0,1).selected.map(r=>r.selectedCumulative)")
    assert missing == [None, None]


def test_v1_and_v2_payload_normalization_preserves_states_and_nulls():
    result = run_metrics("(()=>{const item={ticker:'TEST',fund_name:'Test',category:'long_only',alternatives:[],source:'Trackinsight',flow_currency:'USD',nav_currency:'USD'};const v1=m.normalizeFlowPayload({ticker:'TEST',updated:'2026-09-24',count:3,data:[{date:'2026-09-22',usd_flow:0,nav:10},{date:'2026-09-23',usd_flow:null,nav:null},{date:'2026-09-24',usd_flow:5,nav:11}]},item,null);const v2=m.normalizeFlowPayload({schema_version:2,catalog_version:'test',ticker:'TEST',data_status:'stale',source_asof:'2020-01-01',updated:'2020-01-01',count:1,data:[{date:'2020-01-01',usd_flow:null,nav:12}]},item,null);return {v1:{state:v1.state,flows:v1.records.map(r=>r.flow)},v2:{state:v2.state,flows:v2.records.map(r=>r.flow)},different:v1.revision!==v2.revision};})()")
    assert result["v1"] == {"state": "legacy", "flows": [0, None, 5]}
    assert result["v2"] == {"state": "stale", "flows": [None]}
    assert result["different"] is True


def test_normalization_rejects_identity_count_date_and_currency_mismatches():
    result = run_metrics("(()=>{const item={ticker:'TEST',fund_name:'Test',category:'long_only',alternatives:[],source:'Trackinsight',flow_currency:'USD',nav_currency:'USD'};const base={ticker:'TEST',count:1,data:[{date:'2026-01-01',usd_flow:1,nav:10}]};const cases=[{...base,ticker:'OTHER'},{...base,count:2},{...base,count:2,data:[...base.data,...base.data]},{...base,flow_currency:'EUR'}];return cases.map(value=>{try{m.normalizeFlowPayload(value,item,null);return 'accepted'}catch(error){return error.message}});})()")
    assert "curated catalog" in result[0]
    assert "count" in result[1]
    assert "duplicate date" in result[2]
    assert "USD" in result[3]


def test_cache_key_is_ticker_and_revision_scoped():
    values = run_metrics("[m.cacheKey('spy','r1'),m.cacheKey('SPY','r2'),m.cacheKey('QQQ','r1')]")
    assert values == ["SPY@r1", "SPY@r2", "QQQ@r1"]


def test_search_scopes_cover_all_primary_and_featured_rows_without_watch_tier():
    result = run_metrics(
        "(()=>{const catalog=require('./docs/data/flows/catalog.json');const app=m.flowResearchApp();app.flowCatalog=catalog;app._flowEntryMap=Object.fromEntries(app.flowAllEntries.map(item=>[item.ticker,item]));app.flowResolvedStates=Object.fromEntries(app.flowAllEntries.map(item=>[item.ticker,'pending']));app.flowManifest={schema_version:2,etfs:{}};const primary=app.flowSearchResults.map(result=>result.item);app.flowTierFilter='featured';const featured=app.flowSearchResults.map(result=>result.item);app.flowTierFilter='watch';const watch=app.flowSearchResults.map(result=>result.item);return {defaultCount:primary.length,defaultPrimary:primary.every(item=>item.tier==='primary'),featuredCount:featured.length,featuredPrimary:featured.every(item=>item.tier==='primary'&&item.featured),watchTiers:[...new Set(watch.map(item=>item.tier))],hasWatchTierKey:'watch_tier' in catalog,counts:catalog.counts};})()"
    )
    assert result["defaultCount"] == 160
    assert result["defaultPrimary"] is True
    assert result["featuredCount"] == 24
    assert result["featuredPrimary"] is True
    assert result["watchTiers"] == ["primary"]
    assert result["hasWatchTierKey"] is False
    assert result["counts"] == {"featured": 24, "instruments": 160, "rows": 214988}


def test_history_reconciliation_updates_flow_state_and_clears_missing_selection():
    result = run_metrics(
        "(()=>{const calls=[];let aborted=0;const app={flowTicker:'SPY',flowRangePreset:'1y',flowPriceEnabled:false,flowSearchOpen:true,flowSearchActiveIndex:4,flowData:null,flowDataState:'loading',flowDataError:'old',flowAbortController:null,flowRequestId:2,_flowMetricCacheKey:'old',_flowMetricCache:{},selectFlowTicker(ticker,options){calls.push([ticker,options]);},_flowApplyUrlRange(){}};m.reconcileFlowHistory(app,'?tab=flows&flow=qqq&range=max&price=1');const selected={ticker:app.flowTicker,range:app.flowRangePreset,price:app.flowPriceEnabled,calls};app.flowData={ticker:'QQQ'};app.flowAbortController={abort(){aborted+=1;}};m.reconcileFlowHistory(app,'?tab=flows');return {selected,cleared:{ticker:app.flowTicker,state:app.flowDataState,error:app.flowDataError,aborted,requestId:app.flowRequestId},marketTab:m.marketHistoryTab('?tab=flows'),invalidTab:m.marketHistoryTab('?tab=bad')};})()"
    )
    assert result["selected"] == {
        "ticker": "QQQ",
        "range": "max",
        "price": True,
        "calls": [["QQQ", {"writeUrl": False, "push": False}]],
    }
    assert result["cleared"] == {
        "ticker": "",
        "state": "pending",
        "error": "",
        "aborted": 1,
        "requestId": 3,
    }
    assert result["marketTab"] == "flows"
    assert result["invalidTab"] == "matrix"
    assert run_metrics("m.marketHistoryTab('')") == "matrix"


def test_chart_tabs_switch_by_click_and_keyboard_and_expose_data_tooltip():
    result = run_metrics(
        "(()=>{const app=m.flowResearchApp();app.flowData={revision:'tooltips',records:Array.from({length:12},(_,i)=>({date:`2026-09-${String(i+1).padStart(2,'0')}`,flow:(i+1)*1000,nav:10+i}))};app.flowStartIndex=0;app.flowEndIndex=11;const events=[];events.push(app.flowSetChartTab('cumulative'));const right={key:'ArrowRight',preventDefault(){events.push('right')}};app.flowChartTabKeydown(right,1);const tabAfterRight=app.flowChartTab;app.flowChartTabKeydown({key:'End',preventDefault(){}},2);const tabAfterEnd=app.flowChartTab;app.flowChartPointerMove({type:'mousemove',clientX:50,currentTarget:{querySelector(){return {getBoundingClientRect(){return {left:0,width:100}}}}}});return {events,tabAfterRight,tabAfterEnd,visible:app.flowChartTooltip.visible,tooltip:app.flowChartTooltipText,priceAvailable:app.flowChartTabAvailable('price')};})()"
    )
    assert result["events"] == [True, "right"]
    assert result["tabAfterRight"] == "percentile"
    assert result["tabAfterEnd"] == "price"
    assert result["visible"] is True
    assert "2026-09-07" in result["tooltip"]
    assert "Daily flow" in result["tooltip"]
    assert result["priceAvailable"] is True


def test_chart_geometry_keeps_null_observations_as_gaps():
    result = run_metrics(
        "(()=>{const app=m.flowResearchApp();app.flowData={revision:'gaps',records:Array.from({length:12},(_,i)=>({date:`2026-09-${String(i+1).padStart(2,'0')}`,flow:i===0?null:i,nav:null}))};app.flowStartIndex=0;app.flowEndIndex=11;return {path:m.linePath([{x:0,y:10},{x:1,y:null},{x:2,y:20}]),percentile:app.flowPercentileChartSvg,intensity:app.flowIntensityChartSvg,price:(()=>{app.flowPriceEnabled=true;return app.flowPriceChartSvg;})()};})()"
    )
    assert result["path"] == "M 0.0 10.0 M 2.0 20.0"
    for key in ("percentile", "intensity", "price"):
        assert "NaN" not in result[key]
        assert "null" not in result[key]


def test_all_null_chart_inputs_render_explicit_unavailable_states():
    charts = run_metrics(
        "(()=>{const app=m.flowResearchApp();app.flowData={revision:'all-null',records:Array.from({length:12},(_,i)=>({date:`2026-09-${String(i+1).padStart(2,'0')}`,flow:null,nav:null}))};app.flowStartIndex=0;app.flowEndIndex=11;app.flowPriceEnabled=true;return {daily:app.flowDailyChartSvg,cumulative:app.flowCumulativeChartSvg,percentile:app.flowPercentileChartSvg,intensity:app.flowIntensityChartSvg,price:app.flowPriceChartSvg};})()"
    )
    assert set(charts) == {"daily", "cumulative", "percentile", "intensity", "price"}
    assert all("unavailable" in chart.lower() for chart in charts.values())
    assert all("NaN" not in chart and "null" not in chart for chart in charts.values())


def test_flow_history_restores_ticker_range_and_chart_after_remount():
    result = run_metrics(
        "(()=>{const calls=[];const app={flowTicker:'',flowRangePreset:'1y',flowPriceEnabled:false,flowChartTab:'daily',flowChartMessage:'',flowData:null,flowDataState:'pending',flowDataError:'',flowAbortController:null,flowRequestId:0,_flowMetricCacheKey:'',_flowMetricCache:null,selectFlowTicker(ticker,options){calls.push([ticker,options]);},flowEnsureChartTab(){}};m.reconcileFlowHistory(app,'?tab=flows&flow=VTI&range=max&chart=percentile');return {ticker:app.flowTicker,range:app.flowRangePreset,chart:app.flowChartTab,calls};})()"
    )
    assert result["ticker"] == "VTI"
    assert result["range"] == "max"
    assert result["chart"] == "percentile"
    assert result["calls"] == [["VTI", {"writeUrl": False, "push": False}]]


def test_missing_price_selection_falls_back_without_disabling_focusable_tab():
    result = run_metrics(
        "(()=>{const app=m.flowResearchApp();app.flowData={revision:'no-price',records:Array.from({length:12},(_,i)=>({date:`2026-09-${String(i+1).padStart(2,'0')}`,flow:i+1,nav:null}))};app.flowStartIndex=0;app.flowEndIndex=11;app.flowChartTab='price';app.flowEnsureChartTab();const afterEnsure={tab:app.flowChartTab,message:app.flowChartMessage,available:app.flowChartTabAvailable('price')};const attempted=app.flowSetChartTab('price',false);return {afterEnsure,attempted,message:app.flowChartMessage,tab:app.flowChartTab};})()"
    )
    assert result["afterEnsure"] == {
        "tab": "daily",
        "message": "Price and flow is unavailable for this selected window.",
        "available": False,
    }
    assert result["attempted"] is False
    assert result["tab"] == "daily"
    assert "unavailable" in result["message"]


def test_flow_history_uses_push_for_selection_and_replace_for_range():
    result = run_metrics(
        "(()=>{const previous=global.window;const calls=[];global.window={location:{href:'https://example.test/markets.html?tab=flows&flow=SPY'},history:{pushState(){calls.push(['push',String(arguments[2])])},replaceState(){calls.push(['replace',String(arguments[2])])}}};try{const app=m.flowResearchApp();app.flowTicker='SPY';app.flowData={records:[{date:'2026-09-24',flow:1,nav:10}]};app.flowRangePreset='max';app.flowStartIndex=0;app.flowEndIndex=0;app.flowPriceEnabled=false;app.flowChartTab='percentile';app._flowWriteUrl(true);app._flowWriteUrl(false);return calls;}finally{global.window=previous;}})()"
    )
    assert [call[0] for call in result] == ["push", "replace"]
    assert all("tab=flows" in call[1] and "flow=SPY" in call[1] and "chart=percentile" in call[1] for call in result)


def test_percentile_chart_labels_dollar_left_axis_and_percentile_right_axes():
    svg = run_metrics(
        "(()=>{const app=m.flowResearchApp();app.flowViewportWidth=1000;app.flowData={revision:'test',records:Array.from({length:12},(_,index)=>({date:`2026-09-${String(index+1).padStart(2,'0')}`,flow:(index+1)*100000,nav:10+index}))};app.flowStartIndex=0;app.flowEndIndex=11;return app.flowPercentileChartSvg;})()"
    )
    assert "left axis shows complete trailing 10-observation" in svg
    assert "right axis shows the tie-aware empirical percentile" in svg
    assert ">$" in svg
    assert ">100%<" in svg
    assert ">50%<" in svg
    assert ">0%<" in svg


def test_category_heatmap_relative_scaling_and_universe_totals():
    result = run_metrics(
        "(()=>{const app=m.flowResearchApp();const cat=require('./docs/data/flows/catalog.json');app.flowCatalog=cat;const rows=app.flowCategoryHeatmapRows;const totals=app.flowCategoryUniverseTotals;return {rowCount:rows.length,hasStyles:rows.every(r=>r.style1d&&r.style20d&&r.styleCum),totalsCount:totals.count,totalsAum:totals.aum_m>0,totals1d:typeof totals.flow_1d==='number',totals20d:typeof totals.flow_20d==='number'};})()"
    )
    assert result["rowCount"] == 10
    assert result["hasStyles"] is True
    assert result["totalsCount"] == 160
    assert result["totalsAum"] is True
    assert result["totals1d"] is True
    assert result["totals20d"] is True


def test_underlying_battle_rows_normalization_and_paired_filter():
    result = run_metrics(
        "(()=>{const app=m.flowResearchApp();const cat=require('./docs/data/flows/catalog.json');app.flowCatalog=cat;app.flowBattleMode='paired';const paired=app.flowUnderlyingBattleRows;const nvda=paired.find(r=>r.underlying==='NVIDIA');const mstr=paired.find(r=>r.underlying==='MicroStrategy');const tsla=paired.find(r=>r.underlying==='Tesla');return {pairedCount:paired.length,nvdaBull:nvda?.bullTickers,nvdaBear:nvda?.bearTickers,mstrBull:mstr?.bullTickers,mstrBear:mstr?.bearTickers,tslaBull:tsla?.bullTickers,tslaBear:tsla?.bearTickers,nvdaBullShare:nvda?.bullSharePct};})()"
    )
    assert result["pairedCount"] == 13
    assert "NVDL" in result["nvdaBull"] and "NVD" in result["nvdaBear"]
    assert "MSTU" in result["mstrBull"] and "MSTZ" in result["mstrBear"]
    assert "TSLL" in result["tslaBull"] and ("TSLQ" in result["tslaBear"] or "TSLZ" in result["tslaBear"])
    assert 0 <= result["nvdaBullShare"] <= 100


def test_flow_empirical_edge_ledger_metrics_and_meters():
    result = run_metrics(
        "(()=>{const app=m.flowResearchApp();const cat=require('./docs/data/flows/catalog.json');app.flowCatalog=cat;const tqqqItem=cat.instruments.find(i=>i.ticker==='TQQQ');app.flowTicker='TQQQ';app._flowEntryMap=Object.fromEntries(cat.instruments.map(i=>[i.ticker,i]));const raw=require('./docs/data/flows/TQQQ.json');app.flowData=m.normalizeFlowPayload(raw,tqqqItem);const ledger=app.flowEmpiricalEdgeLedger;return {hasLedger:!!ledger,shockBadge:ledger.shockBadge,shockStatLine:ledger.shockStatLine,hasShockMeter:ledger.shockMeterSvg.includes('<svg')&&ledger.shockMeterSvg.includes('−1.5σ'),hasThrustMeter:ledger.thrustMeterSvg.includes('<svg')&&ledger.thrustMeterSvg.includes('NAV 5D'),thrustBadge:ledger.thrustBadge,thrustStatLine:ledger.thrustStatLine,hasSparkline:ledger.rangeSparklineSvg.includes('<svg')};})()"
    )
    assert result["hasLedger"] is True
    assert "%" not in result["shockStatLine"] or "beat typical" in result["shockStatLine"]
    assert "NORMAL" in result["shockBadge"] or "INFLOW" in result["shockBadge"] or "OUTFLOW" in result["shockBadge"]
    assert result["hasShockMeter"] is True
    assert result["hasThrustMeter"] is True
    assert result["hasSparkline"] is True
    assert result["thrustBadge"] in [
        "UP WITH INFLOWS",
        "UP WITH OUTFLOWS",
        "DOWN WITH INFLOWS",
        "DOWN WITH OUTFLOWS",
        "FLAT, INFLOWS",
        "FLAT, OUTFLOWS",
        "NO NET FLOW",
    ]


def test_flow_ui_edge_cases_and_chart_synchronization():
    result = run_metrics(
        "(()=>{const app=m.flowResearchApp();const cat=require('./docs/data/flows/catalog.json');app.flowCatalog=cat;app._flowEntryMap=Object.fromEntries(cat.instruments.map(i=>[i.ticker,i]));const counts=app.flowBattleCounts;const scatterSvg=app.flowScatterSvg;const tqqqItem=cat.instruments.find(i=>i.ticker==='TQQQ');app.flowTicker='TQQQ';const raw=require('./docs/data/flows/TQQQ.json');app.flowData=m.normalizeFlowPayload(raw,tqqqItem);app.flowStartIndex=0;app.flowEndIndex=app.flowMaxRecordIndex;const impulseSvg=app.flowMultiHorizonImpulseChartSvg;const intensitySvg=app.flowIntensityChartSvg;const summary=app.flowWorkbenchSummary;app.flowMeasureActive=true;app.flowMeasureStartIdx=5;app.flowMeasureCurrentIdx=15;app._flowComputeMeasurement();const measure=app.flowMeasureResult;return {counts,hasScatterHitGroup:scatterSvg.includes('<g data-ticker=')&&scatterSvg.includes('r=\"12\" fill=\"transparent\"'),hasImpulseSvg:impulseSvg.includes('<svg')&&impulseSvg.includes('data-chart-width'),hasZoneBg:intensitySvg.includes('rgba(34,211,238,0.06)'),hasSummaryNav:summary.navRetPct!==null,measureBars:measure.sessions,measureHasNav:measure.priceReturnPct!==null};})()"
    )
    assert result["counts"]["paired"] == 13
    assert result["counts"]["major"] >= 13
    assert result["hasScatterHitGroup"] is True
    assert result["hasImpulseSvg"] is True
    assert result["hasZoneBg"] is True
    assert result["hasSummaryNav"] is True
    assert result["measureBars"] == 11
    assert result["measureHasNav"] is True


def test_multi_model_zscores_and_workbench_velocity():
    result = run_metrics(
        "(()=>{const app=m.flowResearchApp();const cat=require('./docs/data/flows/catalog.json');app.flowCatalog=cat;const tqqqItem=cat.instruments.find(i=>i.ticker==='TQQQ');app.flowTicker='TQQQ';app._flowEntryMap=Object.fromEntries(cat.instruments.map(i=>[i.ticker,i]));const raw=require('./docs/data/flows/TQQQ.json');app.flowData=m.normalizeFlowPayload(raw,tqqqItem);app.flowStartIndex=0;app.flowEndIndex=app.flowMaxRecordIndex;const rows=app.flowSelectedRows;const lastRow=rows[rows.length-1];const stdZ=lastRow.priorOnlyZScore;const robZ=lastRow.robustZScore;const volZ=lastRow.volumeZScore;const momZ=lastRow.momentumZScore5D;const initialModel=app.flowZModel;app.flowCycleZModel();const nextModel=app.flowZModel;const wbSvg=app.flowWorkbenchSvg;const hasTier4=wbSvg.includes('flow-tier-4');const hasVelocityHeader=wbSvg.includes('FLOW VELOCITY (20D vs 60D BASELINE)');const hasPace=wbSvg.includes('20D Pace');const hasBase=wbSvg.includes('60D Base');const hasAccel=wbSvg.includes('Accel');const summary=app.flowWorkbenchSummary;return {hasStdZ:stdZ!==null,hasRobZ:robZ!==null,hasVolZ:volZ!==null,hasMomZ:momZ!==null,initialModel,nextModel,hasTier4,hasVelocityHeader,hasPace,hasBase,hasAccel,velocity20:summary.velocity20!==null,acceleration:summary.acceleration!==null};})()"
    )
    assert result["hasStdZ"] is True
    assert result["hasRobZ"] is True
    assert result["hasVolZ"] is True
    assert result["hasMomZ"] is True
    assert result["initialModel"] == "standard"
    assert result["nextModel"] == "robust"
    assert result["hasTier4"] is True
    assert result["hasVelocityHeader"] is True
    assert result["hasPace"] is True
    assert result["hasBase"] is True
    assert result["hasAccel"] is True
    assert result["velocity20"] is True
    assert result["acceleration"] is True


def test_flow_peer_radar_filters_and_metrics():
    result = run_metrics(
        "(()=>{const app=m.flowResearchApp();const cat=require('./docs/data/flows/catalog.json');app.flowCatalog=cat;app._flowEntryMap=Object.fromEntries(cat.instruments.map(i=>[i.ticker,i]));app.flowTicker='TQQQ';const allPeers=app.flowPeerComparisonRows;app.flowPeerFilter='direct';const directPeers=app.flowPeerComparisonRows;app.flowPeerFilter='cat';const catPeers=app.flowPeerComparisonRows;return {allCount:allPeers.length,directCount:directPeers.length,catCount:catPeers.length,allHaveSparkline:allPeers.every(p=>p.sparklineSvg.includes('<svg')),firstHasPctAum:allPeers[0]?.flow_pct_aum!==undefined};})()"
    )
    assert result["allCount"] > 0
    assert result["directCount"] > 0
    assert result["allHaveSparkline"] is True
    assert result["firstHasPctAum"] is True


def test_workbench_svg_path_optimization_and_advanced_zmodels():
    result = run_metrics(
        "(()=>{const app=m.flowResearchApp();const cat=require('./docs/data/flows/catalog.json');app.flowCatalog=cat;const tqqqItem=cat.instruments.find(i=>i.ticker==='TQQQ');app.flowTicker='TQQQ';app._flowEntryMap=Object.fromEntries(cat.instruments.map(i=>[i.ticker,i]));const raw=require('./docs/data/flows/TQQQ.json');app.flowData=m.normalizeFlowPayload(raw,tqqqItem);app.flowStartIndex=0;app.flowEndIndex=app.flowMaxRecordIndex;const svg=app.flowWorkbenchSvg;const rectCount=(svg.match(/<rect /g)||[]).length;const pathCount=(svg.match(/<path /g)||[]).length;const last=app.flowSelectedRows[app.flowSelectedRows.length-1];const robScore=m.robustPriorZScore([{flow:8},{flow:10},{flow:12},{flow:9},{flow:11},{flow:25}],5,30);const zeroVolScore=m.volumeAdjustedZScore([{flow:10,nav:50},{flow:10,nav:50},{flow:10,nav:50},{flow:10,nav:50},{flow:10,nav:50},{flow:20,nav:50}],5,[0,0,0,0,0,0],30);return {rectCount,pathCount,hasTier2Path:svg.includes('flow-tier-2')&&svg.includes('stroke-linecap=\"butt\"'),hasTier3Path:svg.includes('flow-tier-3')&&svg.includes('stroke-linecap=\"butt\"'),hasTier4Path:svg.includes('flow-tier-4')&&svg.includes('stroke-linecap=\"butt\"'),hasTableOpenState:app.flowTableOpen===false,robValid:robScore!==null,zeroVolScore};})()"
    )
    # Optimized SVG combines thousands of rects into single stroked path segments
    assert result["rectCount"] < 200
    assert result["pathCount"] > 5
    assert result["hasTier2Path"] is True
    assert result["hasTier3Path"] is True
    assert result["hasTier4Path"] is True
    assert result["hasTableOpenState"] is True
    assert result["robValid"] is True
    assert result["zeroVolScore"] is None


def test_battle_liquidation_and_peer_radar_zero_flows():
    result = run_metrics(
        "(()=>{const app=m.flowResearchApp();const cat=require('./docs/data/flows/catalog.json');app.flowCatalog=cat;app._flowEntryMap=Object.fromEntries(cat.instruments.map(i=>[i.ticker,i]));app.flowTicker='TQQQ';const battleRows=app._flowAllBattleRows;const firstBattle=battleRows[0];return {hasBattleRows:battleRows.length>0,firstBattleFound:!!firstBattle,bullShareValid:firstBattle?firstBattle.bullSharePct>=0&&firstBattle.bullSharePct<=100:false};})()"
    )
    assert result["hasBattleRows"] is True
    assert result["firstBattleFound"] is True
    assert result["bullShareValid"] is True


def test_dual_variance_scatter_and_continuous_behavior_scanner():
    result = run_metrics(
        """(()=>{
            const app = m.flowResearchApp();
            const cat = require('./docs/data/flows/catalog.json');
            app.flowCatalog = cat;
            app._flowEntryMap = Object.fromEntries(cat.instruments.map(i => [i.ticker, i]));
            app.flowTicker = 'TQQQ';

            // 1. Nominal Scatter Mode
            app.flowScatterMode = 'nominal';
            const nominalSvg = app.flowScatterSvg;
            const nominalCounts = app.flowScatterCounts;

            // 2. Volatility-Adjusted Scatter Mode
            app.flowScatterMode = 'vol_adjusted';
            const volSvg = app.flowScatterSvg;
            const volCounts = app.flowScatterCounts;

            // 3. Flow setups scanner
            const anomAll = app.flowActiveCycleAnomalies;

            const sampleCand = anomAll.dipAccum[0] || anomAll.climaxInflowTop[0] || anomAll.distributionTop[0] || anomAll.exhaustionLow[0];

            return {
                nominalSvgHasRetAxis: nominalSvg.includes('20-SESSION NAV PRICE RETURN (%)'),
                nominalSvgHasZAxis: nominalSvg.includes('DAILY FLOW Z-SCORE (σ)'),
                volSvgHasPriceZAxis: volSvg.includes('VOLATILITY-STANDARDIZED PRICE DISPLACEMENT (Z_PRICE σ)'),
                volSvgHasFlowZAxis: volSvg.includes('5D FLOW THRUST VARIANCE (Z_FLOW 5D σ)'),
                volSvgHasDataTicker: volSvg.includes('<g data-ticker='),
                volSvgHasHitTargets: volSvg.includes('r="12" fill="transparent"'),
                nominalTotal: nominalCounts.all,
                volTotal: volCounts.all,
                hasAnomaliesAll: anomAll.totalAnomalies >= 0,
                candHasRecord: sampleCand ? typeof app.flowSetupRecord(sampleCand, 'dip').text === 'string' : true,
                noFakeRates: cat.instruments.every(i => Object.entries(i.behavior_profiles.all).every(([k, v]) => !k.endsWith('_beat_rate') || v === null || i.behavior_profiles.all[k.replace('_beat_rate', '_episodes')] >= 8)),
                catalogEnriched: cat.instruments.every(i => i.z_price_20d !== undefined && i.z_flow_5d !== undefined && i.behavior_profiles !== undefined)
            };
        })()"""
    )
    assert result["nominalSvgHasRetAxis"] is True
    assert result["nominalSvgHasZAxis"] is True
    assert result["volSvgHasPriceZAxis"] is True
    assert result["volSvgHasFlowZAxis"] is True
    assert result["volSvgHasDataTicker"] is True
    assert result["volSvgHasHitTargets"] is True
    assert result["nominalTotal"] == 160
    assert result["volTotal"] == 160
    assert result["hasAnomaliesAll"] is True
    assert result["candHasRecord"] is True
    assert result["noFakeRates"] is True
    assert result["catalogEnriched"] is True






