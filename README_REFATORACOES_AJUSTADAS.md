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
1. `run_preflight_pipeline(...)` genérico.
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
6. Mensagens de descartes por data inválida permanecem consistentes.

## Ajustes pendentes (proxima rodada)

1. Expandir contrato comum para outros pipelines (alem de preflight).
2. Criar `preflight` para uso via app com parametros de arquivo customizados.
3. Adicionar testes automatizados (unitarios) para cenarios de bloqueio/aviso.
4. Tratar warning de parse de data (`dayfirst=True` com formato ISO) de forma deterministica.
