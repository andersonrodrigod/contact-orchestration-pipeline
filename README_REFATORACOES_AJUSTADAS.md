# Refatoracoes Ajustadas - Execucao Inicial

Este arquivo registra o que foi efetivamente implementado da trilha de refatoracao recomendada.

## Objetivo desta etapa

1. Iniciar pela opcao mais recomendada: pipeline de pre-flight.
2. Definir um contrato padrao de retorno para esta nova etapa.
3. Validar funcionamento com os arquivos locais reais.

## O que foi implementado

### 1) Contrato de retorno para preflight

Arquivo:
1. `src/contracts/preflight_contracts.py`

Implementado:
1. Builder unico `build_preflight_result(...)`.
2. Estrutura padrao com:
   - `ok`
   - `mensagens`
   - `metricas`
   - `dados.contexto`
   - `dados.bloqueios`
   - `dados.avisos`
   - `dados.detalhes`

### 2) Novo pipeline de preflight

Arquivo:
1. `src/pipelines/preflight_pipeline.py`

Implementado:
1. `run_preflight_pipeline(...)` generico.
2. `run_preflight_complicacao(...)`.
3. `run_preflight_internacao_eletivo(...)`.
4. Validacoes aplicadas:
   - existencia de arquivos
   - validacao de colunas de origem (`status` + `status_resposta`)
   - validacao de colunas obrigatorias de dataset
   - qualidade de datas por limiar (`limiar_nat_data`, default 30.0)

### 3) Fallback para internacao/eletivo sem arquivo unificado

Implementado no `run_preflight_internacao_eletivo(...)`:
1. Se `status_resposta_eletivo_internacao.csv` existir, usa ele diretamente.
2. Se nao existir, concatena em memoria:
   - `status_resposta_eletivo.csv`
   - `status_resposta_internacao.csv`
3. Executa as mesmas validacoes em cima do dataframe combinado.

### 4) Integracao com CLI principal

Arquivo:
1. `src/cli/modos_principais.py`

Novos modos:
1. `preflight_complicacao`
2. `preflight_internacao_eletivo`

## Testes executados

### Teste A - Preflight complicacao
Comando:
1. `python main.py --modo preflight_complicacao`

Resultado:
1. `OK: True`
2. `bloqueios=0`
3. `avisos=0`

### Teste B - Preflight internacao/eletivo
Comando:
1. `python main.py --modo preflight_internacao_eletivo`

Resultado:
1. `OK: True`
2. `bloqueios=0`
3. `avisos=0`

### Teste C - Regressao funcional de pipeline existente
Comando:
1. `python main.py --modo complicacao_gerar_status_dataset`

Resultado:
1. `OK: True`
2. `Total status: 33243`
3. `Total dataset: 11873`
4. `Com match: 22523`
5. `Sem match: 10720`
6. Mensagens de descartes por data invalida permanecem consistentes.

### Teste D - Verificacao de warnings de parse de data
Contexto:
1. Durante a rodada anterior apareceu `UserWarning` do pandas por parse ISO com `dayfirst=True`.

Ajuste aplicado:
1. Parser de datas separado por tipo:
   - ISO (`yyyy-mm-dd` e variacoes com hora) parseado com `dayfirst=False`.
   - Demais formatos parseados com `dayfirst=True`.
   - Fallback final para formatos alternativos.

Resultado:
1. Warning eliminado nos testes.
2. Metricas de saida preservadas (`33243 / 22523 / 10720 / 11873` no modo `complicacao_gerar_status_dataset`).

## Andamento geral por fase

### Fase 1 - Contrato e preflight
Status: `CONCLUIDA`
1. Contrato de preflight implementado.
2. Pipeline de preflight implementado.
3. Modos CLI de preflight adicionados.

### Fase 2 - Unificar pipelines por contexto
Status: `CONCLUIDA`
Concluido:
1. Contextos de integracao extraidos (`HSMs` e colunas de limpeza).
2. Core comum de pipeline por contexto implementado.
3. Pipelines principais (`complicacao` e `internacao_eletivo`) migrados para core comum.
4. Status pipelines migrados para usar contexto central de defaults/loggers.
5. Modos individuais migrados para consumir defaults via `src/contexts/pipeline_contextos.py`.
6. Nomenclatura de logger consolidada por contexto no modulo central.

Validacao final da fase 2:
1. `python main.py --modo preflight_internacao_eletivo` -> `OK=True`
2. `python main.py --modo complicacao` -> `OK=True` (`33243 / 22523 / 10720 / 11873`)
3. `python main.py --modo internacao_eletivo_gerar_status_dataset` -> `OK=True` (`126261 / 12235 / 114026 / 29206`)

### Fase 3 - Endurecimento e observabilidade
Status: `EM ANDAMENTO`
Concluido nesta rodada:
1. Persistencia historica de execucao em `logs/historico_execucoes.jsonl`.
2. Padronizacao inicial de codigos de erro (`core/error_codes.py`) com classificacao automatica no resultado final.
3. Governanca de limiar de data via ambiente:
   - Variavel: `LIMIAR_NAT_DATA_PERCENT`
   - Default preservado: `30.0`
   - Aplicado em ingestao e preflight.

Pendente para fechar a fase 3:
1. Persistir tambem historico de metricas de qualidade por etapa (nao apenas resumo final por modo).
2. Expandir codigos de erro para retornos internos especificos (sem depender apenas de inferencia por mensagem).
3. Definir perfil de limiar por contexto no app (complicacao vs internacao/eletivo), com override controlado.

## Pendencias registradas (nao resolver agora)

1. Concorrencia de escrita em `xlsx` ao executar pipelines simultaneos pode gerar `BadZipFile`.
2. Decisao atual: manter como pendencia ate fechamento do ciclo de refatoracao.
3. Item ja documentado no `README_ERROS_GERAL.md` para tratativa posterior.

## Testes da rodada de fase 3

1. `python main.py --modo preflight_complicacao` -> `OK=True`
2. `python main.py --modo preflight_internacao_eletivo` -> `OK=True`
3. `python main.py --modo complicacao` -> `OK=True` (`33243 / 22523 / 10720 / 11873`)
4. `python main.py --modo individual_ingestao_complicacao` -> `OK=False`, `codigo_erro=E001` (modo desabilitado, comportamento esperado)
5. Historico atualizado em `logs/historico_execucoes.jsonl` com `modo`, `ok`, `codigo_erro` e metricas-chave.
