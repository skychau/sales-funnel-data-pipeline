with
    source as (

        select lead_id, email, source, created_at
        from `sales-funnel-data-pipeline.analytics.raw_leads`

    ),

    cleaned as (

        select
            s.lead_id,

            lower(s.email) as email,

            split(s.email, '@')[safe_offset(1)] as email_domain,

            coalesce(lower(s.source), 'unknown') as source,

            date(s.created_at) as created_date

        from source s

    ),

    deduped as (

        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by email order by created_date desc
                    ) as rn
                from cleaned
            )
        where rn = 1

    )

select *
from deduped
