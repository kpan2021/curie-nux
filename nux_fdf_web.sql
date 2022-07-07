WITH exposure AS
(SELECT DISTINCT tag
               , result
               , replace(lower(CASE WHEN bucket_key like 'dx_%' then bucket_key
                    else 'dx_'||bucket_key end), '-') AS dd_device_id_filtered
               , min(convert_timezone('UTC','America/Los_Angeles',exposure_time)) AS first_exposure_time
FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
WHERE experiment_name = {{experiment_name}}
AND experiment_version in {{experiment_versions | inclause}}
AND convert_timezone('UTC','America/Los_Angeles',exposure_time)::date BETWEEN {{start_date}} and {{end_date}}
AND tag in ('treatment','control')
GROUP BY 1,2,3
)

--=================================================================================================================
--================ Remove device with more than one bucket
--=================================================================================================================

, experiment_bucket_dedup as (
   with double_bucket as (
     select dd_device_id_filtered, count(distinct result) as num_buckets
    from exposure
     where result not in ('RESERVED',   'reserve')
    group by 1)
   
SELECT  tag, result, dd_device_id_filtered, first_exposure_time
from exposure
where dd_device_id_filtered  in (select dd_device_id_filtered from double_bucket where num_buckets = 1)
)

--=================================================================================================================
--================ Pull eligible device on store and order cart pages
--=================================================================================================================
,eligible_user AS
(SELECT replace(lower(CASE WHEN dd_device_id like 'dx_%' then dd_device_id
                    else 'dx_'||dd_device_id end), '-') AS dd_device_id_filtered
 , min(convert_timezone('UTC','America/Los_Angeles',timestamp)) AS min_timestamp
 , max(convert_timezone('UTC','America/Los_Angeles',timestamp)) AS max_timestamp
 FROM segment_events_raw.consumer_production.benefit_fdf_should_display_store
 WHERE convert_timezone('UTC','America/Los_Angeles',timestamp)::date BETWEEN {{start_date}} and {{end_date}}
 GROUP BY 1
UNION ALL
SELECT replace(lower(CASE WHEN dd_device_id like 'dx_%' then dd_device_id
                    else 'dx_'||dd_device_id end), '-') AS dd_device_id_filtered
 , min(convert_timezone('UTC','America/Los_Angeles',timestamp)) AS min_timestamp
 , max(convert_timezone('UTC','America/Los_Angeles',timestamp)) AS max_timestamp
 FROM segment_events_raw.consumer_production.benefit_fdf_should_display_checkout
  WHERE convert_timezone('UTC','America/Los_Angeles',timestamp)::date BETWEEN {{start_date}} and {{end_date}}
GROUP BY 1
)

,exposure_final AS
(
SELECT 
    DISTINCT
    e1.tag
   , e1.result
   , e1.dd_device_id_filtered
    ,e1.first_exposure_time
FROM experiment_bucket_dedup e1
JOIN eligible_user e2
ON e1.dd_device_id_filtered = e2.dd_device_id_filtered and (e1.first_exposure_time::date between e2.min_timestamp::date and  e2.max_timestamp::date)
)


, deliv_metrics AS
(SELECT
     replace(lower(CASE WHEN dd_device_id like 'dx_%' then dd_device_id
            else 'dx_'||dd_device_id end), '-') AS dd_device_id_filtered
     ,dd.subtotal AS subtotal
     ,dd.gov AS gov
     ,dd.is_first_ordercart_dd
     ,dd.active_date
    ,a.order_uuid
FROM
    segment_events_raw.consumer_production.m_checkout_page_system_checkout_success a
 JOIN public.dimension_deliveries dd
    ON a.order_uuid = dd.order_cart_uuid
WHERE dd.is_filtered_core and dd.is_caviar = 0 
    and convert_timezone('UTC','America/Los_Angeles',a.timestamp) BETWEEN {{start_date}} and {{end_date}}
    and convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN {{start_date}} and {{end_date}}
            
  UNION ALL

  SELECT
     replace(lower(CASE WHEN dd_device_id like 'dx_%' then dd_device_id
            else 'dx_'||dd_device_id end), '-') AS dd_device_id_filtered
     ,dd.subtotal AS subtotal
     ,dd.gov AS gov
     ,dd.is_first_ordercart_dd
     ,dd.active_date
     ,a.order_uuid
  FROM
    segment_events_raw.consumer_production.system_checkout_success a
  JOIN public.dimension_deliveries dd
    ON a.order_uuid = dd.order_cart_uuid
WHERE dd.is_filtered_core and dd.is_caviar = 0 
     and convert_timezone('UTC','America/Los_Angeles',a.timestamp) BETWEEN {{start_date}} and {{end_date}}
    and convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN {{start_date}} and {{end_date}}
)

, orders AS 
(SELECT DISTINCT
         replace(lower(CASE WHEN device_id like 'dx_%' then device_id
            else 'dx_'||device_id end), '-') AS dd_device_id_filtered 
        , active_date
        , delivery_id
FROM public.fact_delivery_attribution_1d
WHERE experience = 'doordash' AND active_date >= {{start_date}}
)

,new_cx_order AS
(SELECT
    n.dd_device_id_filtered
    ,n.active_date as first_order_date
    ,COUNT(DISTINCT CASE WHEN o.active_date >= dateadd('day',1, n.active_date) and o.active_date <= dateadd('day',7, n.active_date) then order_uuid end) as num_orders_d7
FROM deliv_metrics n
LEFT JOIN orders o ON n.dd_device_id_filtered = o.dd_device_id_filtered
WHERE n.is_first_ordercart_dd
GROUP BY 1,2)

SELECT 
    DISTINCT 
         e.tag
        , e.dd_device_id_filtered
        , coalesce(n.num_orders_d7,0) AS num_orders_d7
        , max(CASE WHEN first_order_date IS NOT NULL THEN 1 ELSE 0 END) AS new_cx_flag
        , max(CASE WHEN e.first_exposure_time::date = d.active_date THEN 1 ELSE 0 END) AS same_day_conv_flag
        , sum(CASE WHEN e.first_exposure_time::date <= d.active_date THEN d.subtotal ELSE 0 END) AS subtotal
        , sum(CASE WHEN e.first_exposure_time::date <= d.active_date THEN d.gov ELSE 0 END) AS gov
        , count(DISTINCT CASE WHEN e.first_exposure_time::date <= d.active_date THEN d.order_uuid END) AS delivery_cnt

FROM exposure_final e
LEFT JOIN deliv_metrics d
    ON e.dd_device_id_filtered = d.dd_device_id_filtered
LEFT JOIN new_cx_order n
    ON e.dd_device_id_filtered = n.dd_device_id_filtered
GROUP BY 1,2,3



