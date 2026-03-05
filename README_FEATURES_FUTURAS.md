# Features Futuras - Validação e Padronização de Entrada

Este documento descreve evoluções planejadas para reduzir quebra de execução por qualidade de dados na origem.

## 1) Validador Pré-Processo (MVP)

Objetivo: validar arquivos antes de iniciar a orquestração completa.

Escopo inicial:
1. Diagnóstico de colunas obrigatórias faltantes.
2. Diagnóstico de colunas duplicadas e duplicadas mascaradas (`.<numero>`).
3. Diagnóstico de qualidade por coluna crítica:
   - percentual de vazio
   - percentual parseável para data
   - amostras de valores inválidos

Saída esperada:
1. Relatório técnico em JSON para o app consumir.
2. Relatório legível para usuário com ação recomendada por erro.

## 2) Fase de Padronização Total Antes do Processo

Objetivo: normalizar estrutura e tipos antes de qualquer regra de negócio.

Itens previstos:
1. Saneamento de cabeçalhos:
   - trim
   - unificação de maiúsculas/minúsculas
   - remoção de variações de acentuação
2. Dicionário de formatos de data por coluna:
   - `dd/mm/yyyy`
   - `yyyy-mm-dd`
   - regras explícitas de fallback
3. Normalização de tipos:
   - telefones
   - ids
   - campos categóricos

## 3) Roadmap de Implementação

### Fase 1 - MVP de Diagnóstico
1. Comando dedicado para validar arquivos de entrada.
2. Erros bloqueantes de layout + warnings de qualidade.
3. Exportação de relatório para integração com app.

### Fase 2 - Observabilidade
1. Métricas históricas por execução (percentual inválido por coluna).
2. Comparação entre lotes para detectar degradação da origem.
3. Dashboard de saúde de dados.

### Fase 3 - Endurecimento Configurável
1. Thresholds por ambiente (dev/hml/prod).
2. Regras de falha por contexto (ingestão, integração, dataset).
3. Modo estrito/tolerante configurável no app.
