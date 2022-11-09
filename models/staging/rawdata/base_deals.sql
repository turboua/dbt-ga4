select *
from {{source('raw_db','raw_deals') }} d
left join {{source('raw_db', 'raw_refunds')}} r on d.user_id = r.user_id