
-- all lfg addr
lfg_addresses AS (
  SELECT
  	block_timestamp, event_attributes:recipient as address, tx_id
  FROM terra.msg_events
  WHERE event_type = 'transfer'
  	AND event_attributes:sender = 'terra1gr0xesnseevzt3h4nxr64sh5gk4dwrwgszx3nw'
  ORDER BY block_timestamp DESC
)
------------------------------------------------------------------------------------------------------------
-- mint/burn txn tbl
txn_tbl as (
  select
    trader,
    date(block_timestamp) as block_date,
    case when swap_pair = 'LUNA to UST' then 'mint ust' else 'burn ust' end as mint_burn,
    sum(token_0_amount) as token_0_amount,
    sum(token_0_amount_usd) as token_0_amount_usd,
    sum(token_1_amount) as token_1_amount,
    sum(token_1_amount_usd) as token_1_amount_usd,
    sum(token_1_amount)/sum(token_0_amount) as avg_luna_price
  from terra.swaps
  where swap_pair in ('LUNA to UST')
    and tx_status = 'SUCCEEDED'
    and date(block_timestamp) >= '2021-11-01'
  group by
    trader,
    date(block_timestamp),
    case when swap_pair = 'LUNA to UST' then 'mint ust' else 'burn ust' end
  -- having sum(token_1_amount) > 100000
  order by token_1_amount_usd desc
),
------------------------------------------------------------------------------------------------------------
-- mint/burn total wallet tbl
wallet_tbl as (
  select
    trader,
    case when sum(token_1_amount) >= 10000000 then TRUE else FALSE end as is_gt_10m, --10m
    case when swap_pair = 'LUNA to UST' then 'mint ust' else 'burn ust' end as mint_burn,
    sum(token_0_amount) as token_0_amount,
    sum(token_0_amount_usd) as token_0_amount_usd,
    sum(token_1_amount) as token_1_amount,
    sum(token_1_amount_usd) as token_1_amount_usd,
    sum(token_1_amount)/sum(token_0_amount) as avg_luna_price
  from terra.swaps
  where swap_pair in ('LUNA to UST')
    and tx_status = 'SUCCEEDED'
    and date(block_timestamp) >= '2021-11-01'
  	and (trader not in (select distinct address from lfg_addresses) or trader != 'terra10kjnhhsgm4jfakr85673and3aw2y4a03598e0m') --LFG and TFL for ozone and TFL for 3crv
  group by
    trader,
    case when swap_pair = 'LUNA to UST' then TRUE else FALSE end
  order by token_1_amount_usd desc
),
------------------------------------------------------------------------------------------------------------
-- mint burn split by different entity
total_mint_burn as (
  select
  	case
  		when trader = 'terra13h0qevzm8r5q0yknaamlq6m3k8kuluce7yf5lj' then 'LFG capitalising Anchor reserve'
  		when trader = 'terra10kjnhhsgm4jfakr85673and3aw2y4a03598e0m' then 'TFL for Ozone (Prop 44)'
  		when trader in ('terra1qy36laaky2ns9n98naha2r0nvt3j7q3fpxfs2e', 'terra1cymh5ywgn4azak74h4gsrnakqgel4y9ssersvx') then 'TFL for 3crv boostrapping'
  	else 'Others' end as entities,
    swap_pair,
    sum(token_1_amount) as ust_amt
  from terra.swaps
  where swap_pair in ('LUNA to UST', 'UST to LUNA')
    and tx_status = 'SUCCEEDED'
  group by
  	swap_pair,
  	case 
  		when trader = 'terra13h0qevzm8r5q0yknaamlq6m3k8kuluce7yf5lj' then 'LFG capitalising Anchor reserve'
  		when trader = 'terra10kjnhhsgm4jfakr85673and3aw2y4a03598e0m' then 'TFL for Ozone (Prop 44)'
  		when trader in ('terra1qy36laaky2ns9n98naha2r0nvt3j7q3fpxfs2e', 'terra1cymh5ywgn4azak74h4gsrnakqgel4y9ssersvx') then 'TFL for 3crv boostrapping'
  	else 'Others' end
  order by swap_pair, ust_amt asc
)
,
------------------------------------------------------------------------------------------------------------
-- TerraBridge
terra_bridges AS (
  SELECT
  	date(block_timestamp) as block_date,
  	case
  		when event_attributes:recipient = 'terra13yxhrk08qvdf5zdc9ss5mwsg5sf7zva9xrgwgc' then 'eth_transfer_in'
    	when event_attributes:sender = 'terra13yxhrk08qvdf5zdc9ss5mwsg5sf7zva9xrgwgc' then 'eth_transfer_out'
  		when event_attributes:recipient = 'terra1rtn03a9l3qsc0a9verxwj00afs93mlm0yr7chk' then 'one_transfer_in'
    	when event_attributes:sender = 'terra1rtn03a9l3qsc0a9verxwj00afs93mlm0yr7chk' then 'one_transfer_out'
  		when event_attributes:recipient = 'terra1g6llg3zed35nd3mh9zx6n64tfw3z67w2c48tn2' then 'bsc_transfer_in'
    	when event_attributes:sender = 'terra1g6llg3zed35nd3mh9zx6n64tfw3z67w2c48tn2' then 'bsc_transfer_out'
  	else 'others' end as bridge_transfer,
  	event_attributes:amount[0]:denom as currency,
  	sum(event_attributes:amount[0]:amount/1e7) as pv,
  	count(*) as txn
  FROM terra.msg_events
  WHERE event_type = 'transfer' and tx_status = 'SUCCEEDED' and event_attributes:amount[0]:denom in ('uusd', 'uluna')
  	and (
  	event_attributes:recipient in ('terra13yxhrk08qvdf5zdc9ss5mwsg5sf7zva9xrgwgc', 'terra1rtn03a9l3qsc0a9verxwj00afs93mlm0yr7chk', 'terra1g6llg3zed35nd3mh9zx6n64tfw3z67w2c48tn2')
  	or event_attributes:sender in ('terra13yxhrk08qvdf5zdc9ss5mwsg5sf7zva9xrgwgc', 'terra1rtn03a9l3qsc0a9verxwj00afs93mlm0yr7chk', 'terra1g6llg3zed35nd3mh9zx6n64tfw3z67w2c48tn2')
  )
  group by
  	date(block_timestamp),
  	case
  		when event_attributes:recipient = 'terra13yxhrk08qvdf5zdc9ss5mwsg5sf7zva9xrgwgc' then 'eth_transfer_in'
    	when event_attributes:sender = 'terra13yxhrk08qvdf5zdc9ss5mwsg5sf7zva9xrgwgc' then 'eth_transfer_out'
  		when event_attributes:recipient = 'terra1rtn03a9l3qsc0a9verxwj00afs93mlm0yr7chk' then 'one_transfer_in'
    	when event_attributes:sender = 'terra1rtn03a9l3qsc0a9verxwj00afs93mlm0yr7chk' then 'one_transfer_out'
  		when event_attributes:recipient = 'terra1g6llg3zed35nd3mh9zx6n64tfw3z67w2c48tn2' then 'bsc_transfer_in'
    	when event_attributes:sender = 'terra1g6llg3zed35nd3mh9zx6n64tfw3z67w2c48tn2' then 'bsc_transfer_out'
  	else 'others' end,
  	event_attributes:amount[0]:denom
  ORDER BY block_date DESC
)
