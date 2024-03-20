{{ config(alias="tb_ipca_movimentacoes_ano") }}

with ipca_deflacao as (
    select num_ano, cls_movimento from {{ ref('ipca_movimentacoes') }}
)

select
    num_ano,
    cls_movimento,
    count(cls_movimento) as num_frequencia
from
    ipca_deflacao
group by
    num_ano, cls_movimento
