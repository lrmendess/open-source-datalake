{{ config(alias="tb_ipca_movimentacoes") }}

with ipca as (
    select * from hive.trusted.tb_ipca_hist
)

select
    *,
    case when pct_mes < 0.00 then 'Deflação' else 'Inflação' end as cls_movimento
from
    ipca
