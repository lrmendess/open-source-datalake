with ipca as (
    select num_ano, num_mes from {{ ref('ipca_movimentacoes') }}
)

select
    num_ano,
    num_mes,
    count() as num_registros
from
    ipca
group by
    num_ano, num_mes
having
    count() > 1
