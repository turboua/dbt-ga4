select *
from {{source('raw_data','raw_deals') }} d
left join {{source('raw_data', 'raw_refunds')}} r on d.user_id = r.user_id