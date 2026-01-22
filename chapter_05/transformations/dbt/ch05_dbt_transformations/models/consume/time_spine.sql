with days as (
    -- Generates a list of days for the next 50 years
    {{ dbt.date_spine(
        'day',
        "cast('2000-01-01' as date)",
        "cast('2050-01-01' as date)"
    ) }}
),

final as (
    select cast(date_day as date) as date_day
    from days
)

select * from final