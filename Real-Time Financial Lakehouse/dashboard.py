import streamlit as st
import pandas as pd
import s3fs
import time
import plotly.express as px
import plotly.graph_objects as go
import uuid

MINIO_URL = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "market-data"
SILVER_PATH = "silver_layer_delta"

st.set_page_config(page_title="Crypto LakeHouse", layout="wide")

def calculate_rsi(data, window=14):
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


fs = s3fs.S3FileSystem(key=ACCESS_KEY, secret=SECRET_KEY, client_kwargs={'endpoint_url': MINIO_URL},
                       use_listings_cache=False)
def get_all_data():
    try:
        all_files = fs.glob(f"s3://{BUCKET_NAME}/{SILVER_PATH}/**/*.parquet")

        data_files = [f for f in all_files if "_delta_log" not in f and "part-" in f]

        total_files = len(data_files)
        if not data_files: return pd.DataFrame(), 0

        dfs = []
        for file in data_files[-20:]:
            with fs.open(file) as f:
                dfs.append(pd.read_parquet(f))

        if dfs:
            df = pd.concat(dfs, ignore_index=True)
            df['processed_time'] = pd.to_datetime(df['processed_time']) + pd.Timedelta(hours=3)
            df = df.sort_values('processed_time')

            df['SMA'] = df['average_price'].rolling(window=5).mean()
            df['Diff'] = df['average_price'] - df['predicted_price']
            df['RSI'] = calculate_rsi(df['average_price'])

            return df, total_files
        return pd.DataFrame(), total_files
    except Exception as e:
        print(f"Hata: {e}")
        return pd.DataFrame(), 0

with st.sidebar:
    st.header("Ayarlar")
    refresh_rate = st.slider("Hız", 1, 10, 2)
    st.divider()
    stat1 = st.empty()
    dl_placeholder = st.empty()

st.title("Crypto Lakehouse")

c1, c2, c3, c4, c5 = st.columns(5)
m1 = c1.empty()
m2 = c2.empty()
m3 = c3.empty()
m4 = c4.empty()
m5 = c5.empty()

tab1, tab2 = st.tabs(["Ana Analiz", "Model Performansı"])

with tab1:
    chart1 = st.empty()
    chart_rsi = st.empty()

with tab2:
    chart2 = st.empty()

while True:
    df, total_files = get_all_data()
    stat1.metric("Toplam Dosya", f"{total_files}")

    if not df.empty:
        csv = df.to_csv(index=False).encode('utf-8')
        dl_placeholder.download_button("CSV İndir", csv, "crypto.csv", "text/csv", key=f"dl_{uuid.uuid4()}")

        last = df.iloc[-1]

        if len(df) > 1:
            diff = last['average_price'] - df.iloc[-2]['average_price']
        else:
            diff = 0

        m1.metric("Fiyat", f"${last['average_price']:,.2f}", f"{diff:.2f}")

        pred_diff = last['predicted_price'] - last['average_price']
        m2.metric("Tahmin", f"${last['predicted_price']:,.2f}", f"{pred_diff:.2f}", delta_color="off")

        rsi_val = last['RSI'] if not pd.isna(last['RSI']) else 0
        m3.metric("RSI", f"{rsi_val:.2f}")

        if last['predicted_price'] > last['average_price']:
            m4.success("AL")
        else:
            m4.error("SAT")

        m5.metric("Saat", last['processed_time'].strftime('%H:%M:%S'))

        with chart1.container():
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df['processed_time'], y=df['average_price'], mode='lines', name='Fiyat',
                                     line=dict(color='#00F0FF')))
            fig.add_trace(go.Scatter(x=df['processed_time'], y=df['predicted_price'], mode='lines', name='AI Tahmin',
                                     line=dict(dash='dot', color='orange')))
            fig.update_layout(template="plotly_dark", height=400, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True, key=f"p_{uuid.uuid4()}")

        with chart_rsi.container():
            fig_rsi = px.line(df, x='processed_time', y='RSI', title="RSI")
            fig_rsi.add_hline(y=70, line_dash="dash", line_color="red")
            fig_rsi.add_hline(y=30, line_dash="dash", line_color="green")
            fig_rsi.update_layout(template="plotly_dark", height=250, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig_rsi, use_container_width=True, key=f"r_{uuid.uuid4()}")

        with chart2.container():
            fig_err = px.bar(df, x='processed_time', y='Diff', title="Hata Payı", color='Diff')
            fig_err.update_layout(template="plotly_dark", height=350)
            st.plotly_chart(fig_err, use_container_width=True, key=f"e_{uuid.uuid4()}")

    else:
        m1.warning(f"Delta Lake verisi taranıyor... ({total_files} dosya)")

    time.sleep(refresh_rate)