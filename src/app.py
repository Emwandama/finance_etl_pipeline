import os, warnings
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots

warnings.filterwarnings("ignore")
_env = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=_env, override=True)

DB_USER = os.getenv("user","postgres"); DB_PASS = os.getenv("password","")
DB_HOST = os.getenv("host","localhost"); DB_PORT = os.getenv("port","5432")
DB_NAME = os.getenv("dbname","postgres")
os.environ.pop("host",None); os.environ.pop("port",None)
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

def get_engine():
    try:
        e = create_engine(DB_URL, pool_pre_ping=True)
        with e.connect() as c: c.execute(text("SELECT 1"))
        return e
    except: return None

def load_data(eng):
    if eng:
        try:
            q = """SELECT dp.trade_date AS timestamp, s.ticker AS symbol,
                   dp.open_price AS open, dp.high_price AS high,
                   dp.low_price AS low, dp.close_price AS close, dp.volume,
                   m.daily_return_pct AS daily_return, m.price_range,
                   m.vwap_approx AS vwap_proxy, m.ma_7, m.sma_20d AS ma_20,
                   m.volatility_7, m.volume_zscore
                   FROM public.daily_prices dp
                   JOIN public.symbols s ON s.symbol_id=dp.symbol_id
                   LEFT JOIN public.daily_price_metrics m
                     ON m.symbol_id=dp.symbol_id AND m.trade_date=dp.trade_date
                   ORDER BY dp.trade_date ASC"""
            with eng.connect() as c: df = pd.read_sql(text(q),c)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df
        except: pass
    np.random.seed(42); n=500
    dates = pd.bdate_range(end=date.today(),periods=n)
    cl = np.clip(130+np.cumsum(np.random.normal(0.05,1.2,n)),80,300)
    sp = np.abs(np.random.normal(1.5,0.5,n))
    hi=cl+sp; lo=cl-sp; op=lo+np.random.uniform(0,1,n)*(hi-lo)
    vol=np.random.randint(2_000_000,8_000_000,n).astype(float)
    df=pd.DataFrame({"timestamp":dates,"symbol":"IBM","open":op.round(4),"high":hi.round(4),
                     "low":lo.round(4),"close":cl.round(4),"volume":vol.astype(int)})
    df["daily_return"]=df["close"].pct_change()*100
    df["price_range"]=(df["high"]-df["low"]).round(4)
    df["vwap_proxy"]=((df["high"]+df["low"]+df["close"])/3).round(4)
    df["ma_7"]=df["close"].rolling(7,min_periods=1).mean().round(4)
    df["ma_20"]=df["close"].rolling(20,min_periods=1).mean().round(4)
    df["volatility_7"]=df["daily_return"].rolling(7,min_periods=2).std().round(4)
    vm=df["volume"].rolling(20,min_periods=1).mean()
    vs=df["volume"].rolling(20,min_periods=2).std().replace(0,np.nan)
    df["volume_zscore"]=((df["volume"]-vm)/vs).round(4)
    return df

DF=load_data(get_engine())
SYMS=sorted(DF["symbol"].unique().tolist())
MIN_D=DF["timestamp"].min().date(); MAX_D=DF["timestamp"].max().date()

C={"bg":"#0F1117","s1":"#1A1D2E","s2":"#20243A","bdr":"#353A5E",
   "cy":"#38BDF8","gr":"#34D399","rd":"#F87171","pu":"#A78BFA",
   "yw":"#FBBF24","or":"#FB923C","mt":"#94A3B8","wh":"#E2E8F0","grd":"#252A40"}

PL=dict(paper_bgcolor=C["bg"],plot_bgcolor="rgba(20,23,40,0.95)",
        font=dict(family="monospace",color=C["wh"],size=12),
        xaxis=dict(gridcolor=C["grd"],linecolor=C["bdr"],tickfont=dict(color=C["mt"]),zeroline=False),
        yaxis=dict(gridcolor=C["grd"],linecolor=C["bdr"],tickfont=dict(color=C["mt"]),zeroline=False),
        legend=dict(bgcolor="rgba(18,18,42,0.9)",bordercolor=C["bdr"],borderwidth=1,font=dict(color=C["mt"])),
        margin=dict(l=50,r=20,t=40,b=40),hovermode="x unified",
        hoverlabel=dict(bgcolor=C["s2"],bordercolor=C["cy"],font=dict(color=C["wh"])))

def card(icon,lbl,val,sub,ac):
    return html.Div([
        html.Div(icon,style={"fontSize":"22px","marginBottom":"8px"}),
        html.Div(lbl, style={"fontSize":"10px","color":C["mt"],"textTransform":"uppercase",
                             "letterSpacing":"1.5px","marginBottom":"4px","fontFamily":"monospace"}),
        html.Div(val, style={"fontSize":"24px","fontWeight":"700","color":ac,
                             "fontFamily":"monospace","lineHeight":"1.2"}),
        html.Div(sub, style={"fontSize":"11px","color":C["mt"],"marginTop":"4px","fontFamily":"monospace"}),
    ], style={"background":f"linear-gradient(135deg,{C['s1']},{C['s2']})","border":f"1px solid {C['bdr']}",
              "borderTop":f"3px solid {ac}","borderRadius":"14px","padding":"20px"})

def ccrd(title,gid,h):
    return html.Div([
        html.Div(title,style={"fontSize":"11px","fontWeight":"600","color":C["cy"],
                              "textTransform":"uppercase","letterSpacing":"2px",
                              "marginBottom":"12px","fontFamily":"monospace"}),
        dcc.Graph(id=gid,style={"height":f"{h}px"},config={"displayModeBar":False}),
    ],style={"background":f"linear-gradient(135deg,{C['s1']},#0F0F28)","border":f"1px solid {C['bdr']}",
             "borderRadius":"14px","padding":"20px","marginBottom":"18px",
             "boxShadow":"0 2px 16px rgba(0,0,0,0.25)"})

LBL={"fontSize":"10px","color":C["cy"],"textTransform":"uppercase",
     "letterSpacing":"2px","fontFamily":"monospace","marginBottom":"5px"}

app=dash.Dash(__name__,title="RT Finance",
              meta_tags=[{"name":"viewport","content":"width=device-width,initial-scale=1"}])
server=app.server

app.index_string = """<!DOCTYPE html>
<html>
<head>
{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<style>
/* ── Dropdown dark theme ── */
.Select-control{background:#0A2744 !important;border:1px solid #1E4976 !important;border-radius:8px !important;color:#E2E8F0 !important;box-shadow:none !important}
.Select-control:hover{border-color:#38BDF8 !important}
.Select-menu-outer{background:#0D2137 !important;border:1px solid #1E4976 !important;border-radius:8px !important;z-index:9999}
.Select-option{background:#0D2137 !important;color:#E2E8F0 !important;padding:8px 12px}
.Select-option:hover,.Select-option.is-focused{background:#0A2744 !important;color:#38BDF8 !important}
.Select-option.is-selected{background:#0A2744 !important;color:#38BDF8 !important}
.Select-value-label{color:#E2E8F0 !important}
.Select-arrow{border-top-color:#94A3B8 !important}
.Select-placeholder{color:#94A3B8 !important}
.Select--single>.Select-control .Select-value{color:#E2E8F0 !important}
/* ── Date picker dark theme ── */
.DateRangePickerInput{background:#0A2744 !important;border:1px solid #1E4976 !important;border-radius:8px !important;display:flex;align-items:center;padding:0 8px}
.DateRangePickerInput:hover{border-color:#38BDF8 !important}
.DateInput{background:transparent !important;width:100px}
.DateInput_input{background:transparent !important;border:none !important;color:#E2E8F0 !important;font-size:13px !important;font-family:monospace !important;padding:6px 4px !important;text-align:center}
.DateInput_input::placeholder{color:#94A3B8 !important}
.DateRangePickerInput_arrow{color:#94A3B8;margin:0 4px}
.DateRangePickerInput_arrow_svg{fill:#94A3B8}
.DayPicker{background:#0D2137 !important;border:1px solid #1E4976 !important;border-radius:10px;box-shadow:0 8px 32px rgba(0,0,0,0.5)}
.CalendarMonth{background:#0D2137 !important}
.CalendarMonthGrid{background:#0D2137 !important}
.DayPickerNavigation_button{border-color:#1E4976 !important;background:#0A2744 !important}
.DayPickerNavigation_svg__horizontal{fill:#94A3B8}
.CalendarMonth_caption{color:#E2E8F0 !important}
.DayPicker_weekHeader_li{color:#94A3B8}
.CalendarDay__default{background:#0D2137 !important;color:#E2E8F0 !important;border-color:#1E4976 !important}
.CalendarDay__default:hover{background:#0A2744 !important;color:#38BDF8 !important;border-color:#353A5E !important}
.CalendarDay__selected,.CalendarDay__selected:hover{background:#38BDF8 !important;color:#0F1117 !important;border-color:#38BDF8 !important}
.CalendarDay__selected_span{background:#1E3A5F !important;color:#E2E8F0 !important;border-color:#252A40 !important}
.CalendarDay__hovered_span{background:#1E3A5F !important;color:#38BDF8 !important}
/* ── Checkbox ── */
.dash-checklist label{color:#E2E8F0 !important}
</style>
</head>
<body>
{%app_entry%}
<footer>{%config%}{%scripts%}{%renderer%}</footer>
</body>
</html>"""

app.layout=html.Div(style={"backgroundColor":"#0F1117","minHeight":"100vh","color":"#E2E8F0"},children=[

    # ── Header ──────────────────────────────────────────────────────────────
    html.Div(style={"background":f"linear-gradient(90deg,{C['s1']},{C['s2']},{C['s1']})",
                    "borderBottom":f"1px solid {C['cy']}33","padding":"14px 32px",
                    "display":"flex","alignItems":"center","gap":"14px",
                    "boxShadow":f"0 2px 24px {C['cy']}22"},children=[
        html.Div("RT",style={"width":"40px","height":"40px",
                              "background":f"linear-gradient(135deg,{C['cy']},{C['pu']})",
                              "borderRadius":"10px","display":"flex","alignItems":"center",
                              "justifyContent":"center","fontWeight":"700",
                              "color":C["bg"],"fontSize":"15px",
                              "boxShadow":f"0 0 16px {C['cy']}66"}),
        html.Div([
            html.Div("RT Finance Dashboard",style={"fontSize":"18px","fontWeight":"700",
                                                   "color":C["cy"],"fontFamily":"monospace"}),
            html.Div("Real-Time Financial Market Analytics",style={"fontSize":"11px","color":C["mt"],"fontFamily":"monospace"}),
        ]),
        html.Div("● LIVE",style={"marginLeft":"auto","background":f"{C['gr']}15",
                                  "border":f"1px solid {C['gr']}","color":C["gr"],
                                  "padding":"5px 14px","borderRadius":"20px",
                                  "fontSize":"11px","fontFamily":"monospace","letterSpacing":"1.5px"}),
    ]),

    # ── Content ─────────────────────────────────────────────────────────────
    html.Div(style={"padding":"24px 32px","maxWidth":"1380px","margin":"0 auto"},children=[

        # Controls
        html.Div(style={"background":f"linear-gradient(135deg,{C['s1']},{C['s2']})","border":f"1px solid {C['bdr']}",
                        "borderRadius":"14px","padding":"18px 24px","display":"flex","flexWrap":"wrap",
                        "gap":"24px","alignItems":"flex-end","marginBottom":"24px",
                        "boxShadow":"0 4px 20px rgba(0,0,0,0.3)"},children=[
            html.Div([html.Div("TICKER",style=LBL),
                      dcc.Dropdown(id="sym",options=[{"label":s,"value":s} for s in SYMS],
                                   value=SYMS[0],clearable=False,
                                   style={"width":"150px","backgroundColor":"#20243A",
                                          "color":"#E2E8F0","border":f"1px solid #353A5E",
                                          "borderRadius":"8px"})],
                     style={"display":"flex","flexDirection":"column","gap":"4px"}),
            html.Div([html.Div("DATE RANGE",style=LBL),
                      dcc.DatePickerRange(id="dates",min_date_allowed=MIN_D,max_date_allowed=MAX_D,
                                          start_date=MAX_D-timedelta(days=365),end_date=MAX_D,
                                          display_format="YYYY-MM-DD")],
                     style={"display":"flex","flexDirection":"column","gap":"4px"}),
            html.Div([html.Div("CHART TYPE",style=LBL),
                      dcc.Dropdown(id="ctype",options=[{"label":"Candlestick","value":"candle"},
                                                        {"label":"Line","value":"line"},
                                                        {"label":"OHLC","value":"ohlc"}],
                                   value="candle",clearable=False,
                                   style={"width":"160px","backgroundColor":"#20243A",
                                          "color":"#E2E8F0","border":"1px solid #353A5E",
                                          "borderRadius":"8px"})],
                     style={"display":"flex","flexDirection":"column","gap":"4px"}),
            html.Div([html.Div("MA OVERLAYS",style=LBL),
                      dcc.Checklist(id="mas",options=[{"label":" MA-7","value":"ma_7"},
                                                       {"label":" MA-20","value":"ma_20"}],
                                    value=["ma_7","ma_20"],inline=True,
                                    style={"color":C["wh"],"fontFamily":"monospace","fontSize":"13px"})],
                     style={"display":"flex","flexDirection":"column","gap":"4px"}),
        ]),

        # KPIs
        html.Div(id="kpis",style={"display":"grid",
                                   "gridTemplateColumns":"repeat(auto-fit,minmax(190px,1fr))",
                                   "gap":"16px","marginBottom":"24px"}),

        # Charts
        ccrd("📈  Price History & Moving Averages","price",420),
        html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr","gap":"18px","marginBottom":"18px"},children=[
            ccrd("📊  Daily Return Distribution","retd",300),
            ccrd("〰️  7-Day Rolling Volatility","vol7",300),
        ]),
        ccrd("📉  Volume & Z-Score Anomaly","volb",320),

        html.Div("RT Finance API  ·  FINANCE API Course  ·  Alpha Vantage  ·  PostgreSQL",
                 style={"textAlign":"center","padding":"20px","color":C["mt"],"fontFamily":"monospace",
                        "fontSize":"11px","borderTop":f"1px solid {C['bdr']}","marginTop":"8px"}),
    ]),
])

@app.callback(
    Output("kpis","children"),Output("price","figure"),
    Output("retd","figure"),Output("vol7","figure"),Output("volb","figure"),
    Input("sym","value"),Input("dates","start_date"),Input("dates","end_date"),
    Input("ctype","value"),Input("mas","value"),
)
def update(sym,start,end,ctype,mas):
    ef=lambda: go.Figure(layout=go.Layout(**PL))
    if not sym or not start or not end: return [],ef(),ef(),ef(),ef()
    m=(DF["symbol"]==sym)&(DF["timestamp"]>=pd.Timestamp(start))&(DF["timestamp"]<=pd.Timestamp(end))
    df=DF[m].copy()
    if df.empty: return [html.Div("No data.",style={"color":C["mt"],"padding":"20px"})],ef(),ef(),ef(),ef()

    lat=df.iloc[-1]; prv=df.iloc[-2] if len(df)>1 else lat
    cl=float(lat["close"]) if pd.notna(lat["close"]) else 0
    pc=float(prv["close"]) if pd.notna(prv["close"]) else cl
    chg=((cl-pc)/pc*100) if pc else 0
    av=df["volume"].mean()
    dr=df["daily_return"].dropna()
    av7=df["volatility_7"].dropna().mean() if df["volatility_7"].dropna().size else 0

    kpis=[
        card("📈","Latest Close",f"${cl:.2f}",f"{'▲' if chg>=0 else '▼'} {abs(chg):.2f}% vs prev",C["cy"]),
        card("🔺","52-Wk High",f"${df['high'].max():.2f}",f"Low: ${df['low'].min():.2f}",C["pu"]),
        card("📊","Avg Volume",f"{av/1e6:.2f}M","shares / day",C["yw"]),
        card("🚀","Best Return",f"+{dr.max():.2f}%" if dr.size else "N/A","max daily gain",C["gr"]),
        card("⚠️","Worst Return",f"{dr.min():.2f}%" if dr.size else "N/A","max daily loss",C["rd"]),
        card("〰️","Avg Volatility",f"{av7:.2f}%","7-day rolling σ",C["or"]),
    ]

    pf=go.Figure()
    if ctype=="candle":
        pf.add_trace(go.Candlestick(x=df["timestamp"],open=df["open"],high=df["high"],low=df["low"],close=df["close"],
            name=sym,increasing=dict(line=dict(color=C["gr"]),fillcolor="rgba(0,255,136,0.4)"),
            decreasing=dict(line=dict(color=C["rd"]),fillcolor="rgba(255,107,107,0.4)")))
    elif ctype=="ohlc":
        pf.add_trace(go.Ohlc(x=df["timestamp"],open=df["open"],high=df["high"],low=df["low"],close=df["close"],
            name=sym,increasing_line_color=C["gr"],decreasing_line_color=C["rd"]))
    else:
        pf.add_trace(go.Scatter(x=df["timestamp"],y=df["close"],name="Close",
            line=dict(color=C["cy"],width=2.5),fill="tozeroy",fillcolor="rgba(0,212,255,0.07)"))
    if mas and "ma_7"  in mas: pf.add_trace(go.Scatter(x=df["timestamp"],y=df["ma_7"], name="MA-7", line=dict(color=C["yw"],width=1.5,dash="dot")))
    if mas and "ma_20" in mas: pf.add_trace(go.Scatter(x=df["timestamp"],y=df["ma_20"],name="MA-20",line=dict(color=C["or"],width=1.5,dash="dash")))
    pf.update_layout(**PL,xaxis_rangeslider_visible=False,yaxis_title="Price (USD)")

    ret=df["daily_return"].dropna()
    rf=go.Figure()
    rf.add_trace(go.Histogram(x=ret,nbinsx=60,
        marker=dict(color=[C["gr"] if v>=0 else C["rd"] for v in ret],opacity=0.85)))
    rf.add_vline(x=0,line_color=C["mt"],line_dash="dash",line_width=1)
    if ret.size: rf.add_vline(x=ret.mean(),line_color=C["cy"],line_dash="dash",line_width=2,
        annotation_text=f"μ={ret.mean():.2f}%",annotation_font_color=C["cy"])
    rf.update_layout(**PL,showlegend=False,xaxis_title="Return (%)",yaxis_title="Count")

    vd=df.dropna(subset=["volatility_7"])
    vf=go.Figure()
    vf.add_trace(go.Scatter(x=vd["timestamp"],y=vd["volatility_7"],name="7D Vol",
        fill="tozeroy",fillcolor="rgba(191,95,255,0.18)",line=dict(color=C["pu"],width=2.5)))
    vf.update_layout(**PL,showlegend=False,yaxis_title="Volatility (%)")

    sub=make_subplots(rows=2,cols=1,shared_xaxes=True,row_heights=[0.65,0.35],vertical_spacing=0.05)
    bc=["#00FF88" if c>=o else "#FF6B6B" for c,o in zip(df["close"],df["open"])]
    sub.add_trace(go.Bar(x=df["timestamp"],y=df["volume"],marker_color=bc,opacity=0.85,name="Volume"),row=1,col=1)
    zd=df.dropna(subset=["volume_zscore"])
    sub.add_trace(go.Scatter(x=zd["timestamp"],y=zd["volume_zscore"],
        line=dict(color=C["cy"],width=2),fill="tozeroy",fillcolor="rgba(0,212,255,0.08)",name="Z-Score"),row=2,col=1)
    sub.add_hline(y=3, line_color=C["rd"],line_dash="dot",line_width=1.5,row=2,col=1)
    sub.add_hline(y=-3,line_color=C["rd"],line_dash="dot",line_width=1.5,row=2,col=1)
    sub.add_hline(y=0, line_color=C["mt"],line_dash="dash",line_width=1,  row=2,col=1)
    pl2={k:v for k,v in PL.items() if k!="margin"}
    sub.update_layout(**pl2,showlegend=True,margin=dict(l=50,r=20,t=10,b=40))
    sub.update_yaxes(title_text="Volume", gridcolor=C["grd"],tickfont_color=C["mt"],row=1,col=1)
    sub.update_yaxes(title_text="Z-Score",gridcolor=C["grd"],tickfont_color=C["mt"],row=2,col=1)
    sub.update_xaxes(gridcolor=C["grd"],tickfont_color=C["mt"])

    return kpis,pf,rf,vf,sub

if __name__=="__main__":
    app.run(host="127.0.0.1",debug=True,port=8050)
