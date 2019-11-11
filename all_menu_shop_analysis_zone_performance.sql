with shop_type_change as (
    SELECT sub.ops_shop_id,
           sub.name,
           sub.shop_type,
           sub.midlag,
           sub.previous,
           sub.pst_date_time,
           zone_code
    FROM (SELECT ops_shop_id,
                 name,
                 shop_type,
                 pst_date_time,
                 lag(shop_type) OVER (PARTITION BY ops_shop_id
                     ORDER BY pst_date_time)                    AS midlag,
                 CASE
                     WHEN midlag = shop_type THEN 'stays_same'
                     WHEN midlag <> shop_type THEN 'changes'
                     WHEN midlag IS null THEN 'first_value' end as previous
          FROM prod.fact_shop_master
         ) AS sub
             left join prod.shop_info si
                       on sub.ops_shop_id = si.shop_id
    WHERE sub.previous <> 'stays_same'
    order by pst_date_time
),

     daily_changes as (
         select pst_date_time,
                zone_code,
                case when shop_type = 1 then count(ops_shop_id) end          as new_partner_shops,
                case when shop_type = 2 then count(ops_shop_id) end          as new_all_menu_shops,
                case when shop_type in (3, 5, 6) then count(ops_shop_id) end as shops_exit_slice,
                case when shop_type = 4 then count(ops_shop_id) end          as new_fremium_shops
         from shop_type_change
         group by pst_date_time, shop_type, zone_code
     ),


     orders as (
         select date_trunc('day',date_purchased_pst) as date,
                zone_code,
                sum(cached_total)              as daily_gmv,
                count(orders_id)               as daily_orders,
                count(distinct customer)       as daily_customers,
                sum(mypizza_fees + cc_fees)    as daily_revenue
         from prod.all_orders as ao
                  left join prod.shop_info si
                            on ao.shop_id = si.shop_id
         where date_purchased_pst > '2019-01-01'
         group by date, zone_code
     )

     select date,
            orders.zone_code,
            new_partner_shops,
            new_all_menu_shops,
            shops_exit_slice,
            new_fremium_shops,
            daily_gmv,
            daily_orders,
            daily_customers,
            daily_revenue
         from orders
             left join daily_changes as dc
             on to_char(orders.date,'YYYY-MM-DD')=to_char(dc.pst_date_time,'YYYY-MM-DD')
             and dc.zone_code=orders.zone_code
             order by zone_code, date