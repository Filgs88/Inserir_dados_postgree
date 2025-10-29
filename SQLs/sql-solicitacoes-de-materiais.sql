select solicitacaocompra.nr_solicitacao as solicitacao,
solicitacaocompra.data,
aprovacaosolicitacaocompra.dataaprovacao as apr_solicitacao,
aprovacaoparacompra.dataaprovacao as apr_compra,
solicitacaocompra.cod_material,
material.descricao,
coalesce(solicitacaocompra.qtdesolicitada,0) as qtd,
material.cod_unidade,
solicitacaocompra.observacao,
solicitacaocompra.informacao_fornecedor,
solicitacaocompra.cod_almoxarifado,
solicitacaocompra.situacao

from material.solicitacaocompra,
material.aprovacaoparacompra,
material.aprovacaosolicitacaocompra,
material.material

where solicitacaocompra.data >= to_date('01/03/2025','dd/mm/yyyy') and
solicitacaocompra.cod_almoxarifado in ('8','27') and
aprovacaoparacompra.nr_solicitacao (+)= solicitacaocompra.nr_solicitacao and
aprovacaosolicitacaocompra.nr_solicitacao (+)= solicitacaocompra.nr_solicitacao and
solicitacaocompra.cod_material (+)= material.cod_material

order by solicitacaocompra.data desc