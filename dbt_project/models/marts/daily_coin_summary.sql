select
    date_trunc('day', window_start) as date,
    coin,
    avg(avg_price) as daily_avg_price,
    min(min_price) as daily_min_price,
    max(max_price) as daily_max_price,
    max(max_price) - min(min_price) as volatility
from {{ ref('stg_crypto_prices') }}
group by 1, 2