select
    coin,
    window_start,
    window_end,
    cast(avg_price as numeric(12,4)) as avg_price,
    cast(min_price as numeric(12,4)) as min_price,
    cast(max_price as numeric(12,4)) as max_price
from {{ source('raw', 'crypto_windowed_prices') }}
where avg_price is not null