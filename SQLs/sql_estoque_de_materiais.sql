with tabela_ano as (select cod_material,
cod_almoxarifado,
ano,
mes,
quantidade

from (select cod_material, 
cod_almoxarifado, 
ano,
mes, 
quantidade, 
max(ANO) over (partition by cod_material) max_ano

from material.estoque

where cod_almoxarifado = 8
and cod_grupoempresa = 1
and cod_empresa = 1
and cod_filial = 1)

where ano = max_ano),

tabela_estoque as (select cod_material,
cod_almoxarifado,
ano,
mes,
quantidade

from (select cod_material,
cod_almoxarifado,
ano,
mes, 
quantidade,
max(mes) over (partition by cod_material) max_mes

from tabela_ano)

where mes = max_mes),

tabela_preco as (select cod_material,
data_entrada,
valor_unitario

from (select cod_material,
data_entrada,
valor_unitario,
max(data_entrada) over (partition by cod_material) ultima_data

from material.material_ultimaentrada)

where data_entrada = ultima_data)

select tabela_estoque.cod_material,
material.cod_familia,
material.cod_grupomaterial,
material.descricao,
material.cod_unidade,
coalesce(tabela_estoque.quantidade,0) as quantidade,
coalesce(tabela_preco.valor_unitario,0) as valor_unitario

from tabela_estoque,
tabela_preco,
material.material

where tabela_preco.cod_material (+)= tabela_estoque.cod_material and
material.cod_material (+)= tabela_estoque.cod_material

order by material.descricao