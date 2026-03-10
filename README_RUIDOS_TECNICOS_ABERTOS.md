# README_RUIDOS_TECNICOS_ABERTOS

## Objetivo
Mapear ruidos tecnicos observados no projeto para decisao incremental de padronizacao, remocao de ambiguidade, limpeza de inutilidades e refatoracao.

## Regra deste backlog
Todos os itens abaixo estao com **STATUS: ABERTO** por solicitacao, para voce decidir a ordem de execucao.

## Itens abertos

### RT-001 - Ambiguidade de schema de resposta
- Categoria: padronizacao / ambiguidade
- Evidencia:
  - canonicacao de aliases para `resposta` em [schema_resposta_service.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/services/schema_resposta_service.py:20)
  - aplicacao na padronizacao de entrada em [padronizacao_service.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/services/padronizacao_service.py:41)
  - aplicacao na borda de integracao em [status_service.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/services/status_service.py:35)
  - aplicacao na leitura de metricas em [dataset_metricas_service.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/services/dataset_metricas_service.py:89)
  - `RESPOSTA` em [dataset_service.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/services/dataset_service.py:69)
- Risco: comportamentos diferentes por etapa, necessidade de vários fallbacks e risco de regressao silenciosa.
- Sugestao: definir contrato unico interno (ex.: `resposta_raw` na ingestao e `RESPOSTA` apenas na camada final).
- Progresso:
  - Fase A (contrato canonico interno `resposta`): CONCLUIDA.
  - Fase B (adapter de borda para `Resposta`/`RESPOSTA`): CONCLUIDA.
  - Fases C/D/E: ABERTAS.
- STATUS: EM_EXECUCAO

### RT-002 - Ambiguidade de nome de data de atendimento
- Categoria: padronizacao / ambiguidade
- Evidencia:
  - compatibilidade `DT_ATENDIMENTO` e `dat_atendimento` em [validacao_service.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/services/validacao_service.py:12)
  - regra equivalente em [preflight_pipeline.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/pipelines/preflight_pipeline.py:23)
  - rename em [padronizacao_service.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/services/padronizacao_service.py:36)
- Risco: lógica espalhada em varios pontos para o mesmo problema.
- Sugestao: centralizar resolucao de alias de colunas em um unico utilitario de schema.
- STATUS: ABERTO

### RT-003 - Compatibilidade com typo historico de coluna
- Categoria: padronizacao / legado
- Evidencia:
  - fallback para `LIDA_REPOSTA_SIM` e `LIDA_REPOSTA_NAO` em [orquestracao_service.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/services/orquestracao_service.py:109)
- Risco: perpetuacao de typo no dominio e no contrato de dados.
- Sugestao: manter compatibilidade apenas na borda de entrada e registrar warning quando typo aparecer.
- STATUS: ABERTO

### RT-004 - Duplicacao alta entre pipelines de contexto
- Categoria: refatoracao
- Evidencia:
  - similaridade aproximada de 82.8% entre:
    - `src/pipelines/complicacao_status_pipeline.py`
    - `src/pipelines/internacao_eletivo_status_pipeline.py`
- Risco: custo alto de manutencao e risco de drift entre fluxos teoricamente equivalentes.
- Sugestao: extrair pipeline parametrizado por contexto (defaults, logger, arquivos e regras).
- STATUS: ABERTO

### RT-005 - Funcoes monoliticas longas
- Categoria: refatoracao
- Evidencia:
  - [ingestao_service.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/services/ingestao_service.py:60) `executar_normalizacao_padronizacao` (~230 linhas)
  - [ingestao_service.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/services/ingestao_service.py:489) `executar_ingestao_unificar` (~144 linhas)
- Risco: baixa legibilidade, testes mais dificeis e maior chance de regressao.
- Sugestao: quebrar por etapas puras (leitura, padronizacao, validacao, persistencia).
- STATUS: ABERTO

### RT-006 - Modulo de compatibilidade com wildcard import
- Categoria: inutilidade / padronizacao
- Evidencia:
  - [finalizacao_service.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/services/finalizacao_service.py:6) usa `import *`
- Risco: namespace opaco, baixa rastreabilidade e comportamento implicito.
- Sugestao: deprecar formalmente o modulo ou substituir por reexports explicitos.
- STATUS: ABERTO

### RT-007 - Duplicacao em `.gitignore`
- Categoria: padronizacao / limpeza
- Evidencia:
  - entradas repetidas em [.gitignore](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/.gitignore:1) e [.gitignore](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/.gitignore:8)
- Risco: ruido de manutencao e dificuldade de revisar regras reais de ignore.
- Sugestao: consolidar em bloco unico sem duplicatas.
- STATUS: ABERTO

### RT-008 - Constante de schema sem uso aparente
- Categoria: inutilidade / padronizacao
- Evidencia:
  - [schemas.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/config/schemas.py:11) `COLUNAS_STATUS_RESPOSTA_OBRIGATORIAS_PADRONIZACAO` sem referencias no codigo.
- Risco: falsa sensacao de contrato ativo.
- Sugestao: remover ou passar a usar explicitamente nas validacoes.
- STATUS: ABERTO

### RT-009 - Historico de execucao sem politica de retencao
- Categoria: operacao / observabilidade
- Evidencia:
  - append continuo em [observabilidade_service.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/services/observabilidade_service.py:133)
- Risco: crescimento indefinido do arquivo de historico.
- Sugestao: implementar rotacao por tamanho ou por data.
- STATUS: ABERTO

### RT-010 - Estrategia de limpeza textual ainda centrada em `apply`
- Categoria: performance / refatoracao
- Evidencia:
  - aplicacao por celula em [normalizacao_services.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/services/normalizacao_services.py:210)
- Risco: custo alto em volume grande (ja mitigado por escopo de colunas, mas nao resolvido estruturalmente).
- Sugestao: avaliar cache por valor unico ou pipeline vetorizado para trocas comuns.
- STATUS: ABERTO

### RT-011 - Resultado de testes de normalizacao indica acoplamento com encoding historico
- Categoria: ambiguidade / qualidade
- Evidencia:
  - asserts com textos mojibake em [test_normalizacao_frases.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/tests/test_normalizacao_frases.py:8)
- Risco: manter comportamento "quebrado esperado" e dificultar evolucao para texto canonicamente correto.
- Sugestao: separar teste de compatibilidade legado de teste de saida canonica alvo.
- STATUS: ABERTO

### RT-012 - Funcoes irmas com logs e tratamento de erro quase iguais
- Categoria: refatoracao
- Evidencia:
  - `_run_unificar_status_resposta_pipeline` e `_run_status_somente_pipeline` em [join_status_resposta_pipeline.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/pipelines/join_status_resposta_pipeline.py:13) e [join_status_resposta_pipeline.py](c:/Users/anderson.dossantos/Desktop/dev/contact-orchestration-pipeline/src/pipelines/join_status_resposta_pipeline.py:101)
- Risco: repeticao de comportamento transversal (log, mensagens, codigo_erro).
- Sugestao: extrair helper unico para execucao protegida e padrao de logging.
- STATUS: ABERTO

## Proposta de priorizacao (sugestao)
1. RT-001, RT-002, RT-003 (contrato de schema e ambiguidades)
2. RT-004, RT-005, RT-012 (refatoracao estrutural)
3. RT-010 (performance)
4. RT-006, RT-007, RT-008, RT-009, RT-011 (higiene tecnica)

## Como iremos conduzir
Quando voce escolher um item, eu atualizo este README mudando o status de `ABERTO` para:
- `EM_ANALISE`
- `EM_EXECUCAO`
- `CONCLUIDO`
- `DESCARTADO`
