# Refatoracao - Ideias, Estrategia e Proximos Passos

Este documento consolida propostas de refatoracao para reduzir acoplamento, facilitar manutencao e permitir evolucao do pipeline com menor risco operacional.

## Objetivos de refatoracao

1. Separar responsabilidades (ingestao, validacao, integracao, dataset, orquestracao).
2. Reduzir dependencia cruzada entre pipelines de `complicacao` e `internacao_eletivo`.
3. Padronizar contratos de entrada/saida entre servicos.
4. Facilitar testes isolados por etapa.
5. Preparar o projeto para configuracao via app (limiares, modos, regras).

## Diagnostico atual (pontos de dor)

1. Alguns servicos misturam normalizacao, regra de negocio e logica de I/O no mesmo fluxo.
2. Pipelines compartilham funcoes com nomes de contexto legado (`criar_dataset_complicacao`) mesmo sendo usadas em multiplos contextos.
3. Existem regras de negocio que dependem de colunas com contrato implicito, dificultando rastreabilidade.
4. A variacao CSV/XLSX repete partes do fluxo.
5. Observabilidade ainda depende muito de mensagens livres em log, sem um contrato unico de metricas por etapa.

## Linha mestra de arquitetura (alvo)

Separar o fluxo em camadas com contratos explicitos:

1. `adapters` (entrada/saida)
- Leitura e escrita de arquivos.
- Conversao de formatos.

2. `normalization`
- Padronizacao de nomes de colunas.
- Conversao de tipos.
- Sanitizacao de texto.

3. `validation`
- Regras de schema.
- Regras de qualidade.
- Relatorio estruturado de erros/warnings.

4. `domain` (regra de negocio)
- Integracao status/resposta.
- Enriquecimento de dataset.
- Classificacao de processo/acao.

5. `orchestration` (pipeline runner)
- Encadeamento de etapas.
- Controle de falha por etapa.
- Consolidacao de metricas e mensagens.

## Separacao de dependencias (proposta pratica)

### 1) Criar pacote de contrato comum

Novo modulo sugerido: `src/contracts/`

Arquivos sugeridos:
1. `result_contracts.py`
2. `schema_contracts.py`
3. `metric_contracts.py`

Objetivo:
1. Unificar estrutura de retorno (`ok`, `mensagens`, `metricas`, `arquivos`, `erros`).
2. Definir campos obrigatorios de metricas por etapa.
3. Evitar divergencia de chave de retorno entre pipelines.

### 2) Isolar dependencia de contexto (`complicacao` vs `internacao_eletivo`)

Novo modulo sugerido: `src/contexts/`

Arquivos sugeridos:
1. `complicacao_context.py`
2. `internacao_eletivo_context.py`

Conteudo:
1. HSMs permitidos.
2. Colunas a limpar.
3. Paths default.
4. Regras especificas de contexto.

Beneficio:
1. Pipeline core passa a ser unico e recebe `context_config`.
2. Evita duplicacao de pipeline por contexto com logica quase igual.

### 3) Extrair validacoes em componentes independentes

Novo modulo sugerido: `src/validators/`

Componentes:
1. `schema_validator.py` (faltantes, duplicadas, mascaradas).
2. `data_quality_validator.py` (%NaT, valores invalidos).
3. `business_preconditions_validator.py` (campos exigidos por regra de negocio).

Beneficio:
1. Reuso simples entre CLI, app e pipeline batch.
2. Erros mais previsiveis e testaveis.

## Novo pipeline recomendado para separar outra dependencia

### Proposta: pipeline de pre-flight (antes da ingestao principal)

Nome sugerido:
1. `src/pipelines/preflight_pipeline.py`

Responsabilidade:
1. Receber arquivos de entrada.
2. Executar apenas validacao de estrutura e qualidade.
3. Retornar relatorio detalhado de bloqueios e avisos.
4. Nao gerar dataset final, nao executar orquestracao.

Entradas:
1. arquivo status
2. arquivo status_resposta (ou unificado)
3. arquivo origem de dataset
4. contexto (`complicacao` ou `internacao_eletivo`)

Saida:
1. `ok` geral.
2. lista de bloqueios.
3. lista de avisos.
4. metricas de qualidade por coluna.
5. recomendacoes de correcao para usuario.

Motivo:
1. Separa dependencia de qualidade de dados da dependencia de regra de negocio.
2. Permite ao app mostrar erro amigavel antes de iniciar processamento pesado.
3. Reduz tentativas de execucao quebrada.

## Estrategia de implementacao (fases)

### Fase 1 - Contrato e pre-flight
1. Definir contrato unico de retorno.
2. Implementar `preflight_pipeline`.
3. Adicionar comando CLI para preflight.

### Fase 2 - Unificar pipelines por contexto
1. Extrair configuracoes para `contexts`.
2. Criar pipeline generico parametrizado por contexto.
3. Manter wrappers atuais por compatibilidade.

### Fase 3 - Endurecimento e observabilidade
1. Persistir metricas de qualidade.
2. Adicionar limites configuraveis por ambiente.
3. Padronizar codigos de erro e taxonomia.

## Testabilidade (criterios)

1. Cada validator deve ter teste unitario isolado.
2. Cada etapa de pipeline deve ter teste com mocks de I/O.
3. Deve existir teste de regressao com massa real controlada.
4. Deve existir teste de compatibilidade de contrato para retorno de funcoes publicas.

## Riscos e mitigacoes

1. Risco: refatoracao quebrar contratos existentes.
- Mitigacao: manter wrappers antigos com deprecacao gradual.

2. Risco: aumento inicial de complexidade estrutural.
- Mitigacao: migrar em fases curtas, com rollback facil por etapa.

3. Risco: divergencia entre CSV e XLSX.
- Mitigacao: centralizar parser e regras de normalizacao em modulo unico.

## Decisoes recomendadas para iniciar

1. Implementar primeiro o `preflight_pipeline` (maior ganho operacional).
2. Definir contrato unico de retorno antes de novas features.
3. Congelar adicao de regra nova de negocio ate a camada de validacao estar estabilizada.
