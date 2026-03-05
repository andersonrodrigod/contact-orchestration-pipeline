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

## Fase 3 — Observabilidade, Governança e Erros Estruturados

Esta fase endurece o comportamento operacional do pipeline sem mudar o contrato funcional principal de saida. O foco foi tornar a execucao mais rastreavel, mais previsivel em falha e mais governavel em ambientes diferentes.

### 1) O que e a Fase 3

A Fase 3 consolida tres frentes complementares:

1. Observabilidade por etapa.
2. Codigos de erro explicitos no fluxo interno.
3. Governanca de limiar de qualidade de data por contexto, com override controlado.

Objetivo pratico:

1. Saber onde o pipeline falhou sem depender apenas de log textual.
2. Registrar historico com granularidade suficiente para auditoria operacional.
3. Permitir configuracao mais segura de limiar entre `complicacao` e `internacao_eletivo`.

### 2) O que foi implementado

Arquivos centrais desta rodada:

1. `src/services/observabilidade_service.py`
2. `core/error_codes.py`
3. `src/config/governanca_config.py`
4. `src/services/ingestao_service.py`
5. `src/pipelines/preflight_pipeline.py`
6. `src/pipelines/contexto_pipeline_core.py`
7. `src/pipelines/complicacao_status_pipeline.py`
8. `src/pipelines/internacao_eletivo_status_pipeline.py`
9. `src/pipelines/orquestracao_base_pipeline.py`

Implementado:

1. Persistencia de `metricas_por_etapa` no resultado e no historico.
2. Persistencia de `qualidade_data` com estrutura mais previsivel.
3. Codigos de erro atribuidos diretamente em validacao, ingestao, integracao, concatenacao, criacao de dataset, orquestracao e preflight.
4. Resolucao de limiar por contexto:
   - `complicacao`
   - `internacao_eletivo`
5. Suporte a override controlado por ambiente para chamadas que passam `limiar_nat_data`.

### 3) Como funciona a observabilidade por etapa

Cada pipeline principal passa a carregar um bloco `metricas_por_etapa` com os indicadores mais importantes da sua execucao. A ideia nao e substituir log, e sim registrar um resumo estruturado e reutilizavel.

Exemplo conceitual:

```json
{
  "metricas_por_etapa": {
    "normalizacao_padronizacao": {
      "nat_data_agendamento": 0,
      "pct_nat_data_agendamento": 0.0,
      "nat_dt_atendimento": 0,
      "pct_nat_dt_atendimento": 0.0,
      "limiar_nat_data_em_uso": 30.0
    },
    "integracao_status_resposta": {
      "total_status": 33243,
      "com_match": 22523,
      "sem_match": 10720,
      "descartados_status_data_invalida": 0,
      "descartados_resposta_data_invalida": 0
    },
    "criacao_dataset_status": {
      "total_linhas": 11873
    },
    "orquestracao": {
      "total_usuarios": 3764,
      "total_usuarios_resolvidos": 8109
    }
  }
}
```

Interpretacao:

1. `normalizacao_padronizacao` mostra qualidade de data e limiar aplicado.
2. `integracao_status_resposta` mostra volume processado e perdas por descarte.
3. `criacao_dataset_status` mostra o total de linhas do dataset consolidado.
4. `orquestracao` mostra o comportamento final do dataset orquestrado.
5. No preflight, a etapa registrada e `preflight`, com contagem de linhas e limiar utilizado.

### 4) Como funcionam os codigos de erro

Antes desta fase, boa parte da classificacao dependia de inferencia por mensagem. Com a Fase 3, os retornos internos passaram a preencher `codigo_erro` diretamente sempre que a falha e conhecida.

Taxonomia atual:

1. `E001` - modo bloqueado.
2. `E101` - validacao de arquivos.
3. `E102` - validacao de colunas.
4. `E201` - qualidade de data.
5. `E301` - ingestao.
6. `E302` - integracao.
7. `E303` - concatenacao.
8. `E401` - criacao de dataset.
9. `E501` - orquestracao.
10. `E701` - concorrencia/erro de `xlsx`.
11. `E999` - erro desconhecido quando nao houve classificacao explicita.

Exemplo:

```json
{
  "ok": false,
  "codigo_erro": "E102",
  "mensagens": [
    "Colunas obrigatorias do dataset de complicacao nao foram encontradas."
  ]
}
```

Regra operacional:

1. Se a etapa souber a causa da falha, ela devolve `codigo_erro` explicito.
2. Se a etapa nao preencher o codigo, o `main.py` ainda pode inferir o valor como fallback.
3. Isso reduz dependencia de parsing textual e melhora a estabilidade do historico.

### 5) Como funciona o limiar por contexto

O limiar de qualidade de data continua com default `30.0`, mas a resolucao agora considera contexto e politica de override.

Variaveis suportadas:

1. `LIMIAR_NAT_DATA_PERCENT`
2. `LIMIAR_NAT_DATA_PERCENT_COMPLICACAO`
3. `LIMIAR_NAT_DATA_PERCENT_INTERNACAO_ELETIVO`
4. `PERMITIR_OVERRIDE_LIMIAR_NAT_DATA`

Ordem de resolucao:

1. Se a chamada passar `limiar_nat_data` e o override estiver permitido, esse valor vence.
2. Se existir variavel por contexto, ela vence para aquele contexto.
3. Se nao existir variavel por contexto, a variavel global e usada.
4. Se nada estiver configurado, vale o default do contexto.
5. Se o override por parametro estiver bloqueado, o sistema ignora o parametro e usa a governanca do ambiente.

Exemplos:

```env
LIMIAR_NAT_DATA_PERCENT=30
LIMIAR_NAT_DATA_PERCENT_COMPLICACAO=20
LIMIAR_NAT_DATA_PERCENT_INTERNACAO_ELETIVO=35
PERMITIR_OVERRIDE_LIMIAR_NAT_DATA=true
```

Leitura do exemplo:

1. `complicacao` usa `20`.
2. `internacao_eletivo` usa `35`.
3. Qualquer contexto sem regra especifica pode usar o global `30`.
4. Se uma chamada explicita passar `limiar_nat_data`, o valor pode sobrescrever a regra se o override estiver habilitado.

### 6) Como interpretar os registros no `historico_execucoes.jsonl`

Cada linha do arquivo `logs/historico_execucoes.jsonl` representa uma execucao independente em formato JSON Lines. Isso facilita consulta, auditoria e ingestao futura por ferramentas externas.

Campos principais:

1. `timestamp`: horario da execucao.
2. `modo`: modo chamado no CLI.
3. `ok`: sucesso ou falha.
4. `codigo_erro`: codigo estruturado quando houver falha.
5. `metricas`: resumo principal da execucao.
6. `qualidade_data`: bloco resumido de qualidade das colunas de data.
7. `metricas_por_etapa`: visao granular por etapa.
8. `resultados`: presente quando ha composicao de pipelines filhos.

Exemplo realista simplificado:

```json
{
  "timestamp": "2026-03-05T14:14:04.968939",
  "modo": "complicacao",
  "ok": true,
  "codigo_erro": null,
  "metricas": {
    "total_status": 33243,
    "total_linhas": 11873,
    "com_match": 22523,
    "sem_match": 10720,
    "limiar_nat_data_em_uso": 30.0
  },
  "qualidade_data": {
    "data_agendamento": {
      "nat": 0,
      "pct_nat": 0.0,
      "limiar": 30.0
    },
    "dt_atendimento": {
      "nat": 0,
      "pct_nat": 0.0,
      "limiar": 30.0
    }
  },
  "metricas_por_etapa": {
    "normalizacao_padronizacao": {
      "nat_data_agendamento": 0,
      "pct_nat_data_agendamento": 0.0,
      "nat_dt_atendimento": 0,
      "pct_nat_dt_atendimento": 0.0,
      "limiar_nat_data_em_uso": 30.0
    },
    "integracao_status_resposta": {
      "total_status": 33243,
      "com_match": 22523,
      "sem_match": 10720,
      "descartados_status_data_invalida": 0,
      "descartados_resposta_data_invalida": 0
    }
  }
}
```

Leitura operacional recomendada:

1. Comecar por `ok` e `codigo_erro`.
2. Em seguida olhar `metricas` para validar regressao de volume.
3. Depois abrir `qualidade_data` para entender pressao de NaT e limiar aplicado.
4. Por fim analisar `metricas_por_etapa` para localizar onde houve degradacao ou perda de volume.
