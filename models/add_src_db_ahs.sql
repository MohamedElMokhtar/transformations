select
    *,
    'AHS' as ABH
from {{ source('public', 'ahs_factures') }}
