with
    leads as (select * from {{ ref("stg_leads") }}),

    aggregated as (select source, count(*) as total_leads from leads group by source),

    overall as (select count(*) as total_leads from leads)

select
    a.source,
    a.total_leads,
    o.total_leads as overall_leads,
    safe_divide(a.total_leads, o.total_leads) as pct_of_total

from aggregated a
cross join overall o
order by total_leads desc
