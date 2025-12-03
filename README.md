# Real-Time-Financial-Lakehouse
End-to-End Real-Time Financial Lakehouse &amp; ML Inference Pipeline. Built with Apache Kafka, Spark Structured Streaming, Delta Lake (MinIO), dbt, and Streamlit.
# 🌊 QuantFlow AI: Real-Time Financial Lakehouse & AI Inference

**QuantFlow AI**, Binance üzerinden akan canlı finansal verileri (BTC/USDT) milisaniyeler içinde yakalayan, işleyen, **Delta Lake** mimarisinde saklayan ve **Makine Öğrenmesi** ile anlık fiyat tahmini yapan uçtan uca (End-to-End) bir veri mühendisliği projesidir.

![Dashboard Ekran Görüntüsü Buraya Gelecek](dashboard_screenshot.png)

## 🏗️ Mimari ve Akış

Sistem, **Modern Data Stack** prensiplerine uygun olarak **Docker** üzerinde mikro-servis mimarisiyle kurgulanmıştır.

```mermaid
graph LR
    Binance -->|WebSocket| Producer
    Producer -->|JSON| Kafka
    Kafka -->|Stream| Spark[Spark Streaming]
    Spark -->|ML Inference| Spark
    Spark -->|Delta Lake| MinIO
    MinIO -->|Analytics| dbt
    MinIO -->|Visualize| Streamlit
