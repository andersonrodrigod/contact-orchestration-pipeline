# README_AJUSTES_NORMALIZACAO

## Objetivo
Consolidar os ajustes aplicados para reduzir falhas de encoding/acentuacao em comparacoes de texto (HSM, status e mensagens), e registrar o que ficou para continuacao.

## Ajustes aplicados

1. Refatoracao da normalizacao canonica em `src/services/normalizacao_services.py`
- `_chave_canonica_texto`: gera chave simplificada para comparacao (minusculo, sem acento, apenas caracteres relevantes).
- `_normalizar_fragmentos_quebrados_chave`: reconstrucao de fragmentos quebrados por encoding ruim (ex.: `n mero`, `usu rio`, `n o`, `complica es`).
- `_normalizar_frases_canonicas`: mapeamento canonico deterministico para frases criticas e fallback controlado.
- `corrigir_texto_bugado`: centraliza trocas legadas + tentativa de redecodificacao + normalizacao canonica.

2. Remocao de fallback ad-hoc no filtro de HSM em `src/services/integracao_service.py`
- Mantido filtro por normalizacao de texto (`simplificar_texto`) sem heuristica ampla por `contains`.

3. Testes adicionados
- `tests/test_normalizacao_frases.py` cobrindo casos criticos:
  - `Pesquisa Complica��es Cirurgicas`
  - `N�mero � parte de um experimento`
  - `Usu�rio decidiu não receber MKT messages`
  - `N�o`

## Validacao executada
- `python -m unittest discover -s tests -p "test_*.py"`
- Resultado: `OK (16 testes)`

- Execucao real:
  - `python main.py --modo complicacao`
  - Resultado: `OK=True` (pipeline finalizado com sucesso)

## Pendencias para amanha
1. Revisar e reduzir o dicionario de trocas fixas em `corrigir_texto_bugado` para manter apenas o necessario.
2. Padronizar regra de normalizacao por dominio (HSM, Status, Resposta) para evitar divergencia futura.
3. Adicionar observabilidade opcional de valores nao mapeados (log/contador) para facilitar manutencao.
4. Decidir se a camada de exibicao deve preservar acentos em todos os campos ou somente nos campos visiveis ao usuario.

## Observacao de seguranca
- A estrategia atual privilegia robustez de comparacao sem alterar a estrutura do pipeline.
- Qualquer nova refatoracao deve continuar validando:
  - testes automatizados,
  - modo `complicacao`,
  - e filtros de HSM.
