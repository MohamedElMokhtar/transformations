select
    *,
    'CSM' as ABH
from {{ source('public', 'csm_factures') }}
