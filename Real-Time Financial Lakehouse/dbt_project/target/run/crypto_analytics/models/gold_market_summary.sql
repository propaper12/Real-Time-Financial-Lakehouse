
  
    
    

    create  table
      "memory"."main"."gold_market_summary__dbt_tmp"
  
    as (
      

SELECT
    symbol,
    date_trunc('minute', processed_time) as minute_window,

    AVG(average_price) as avg_price,
    MAX(average_price) as max_price,
    MIN(average_price) as min_price,
    AVG(volatility) as avg_volatility,
    COUNT(*) as transaction_count

FROM read_parquet('s3://market-data/silver_layer_delta/*.parquet')

GROUP BY 1, 2
ORDER BY 2 DESC
    );
  
  