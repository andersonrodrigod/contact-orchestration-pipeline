# README_AJUSTES_NORMALIZACAO

## Objetivo
Padronizar o projeto para comparacoes textuais sem acento/til (chave canonica), reduzindo falhas por encoding e variacao de escrita, sem quebrar os fluxos atuais.

## Diretriz central
1. Regras internas de negocio/comparacao devem usar texto canonico sem acento/til.
2. Exibicao final pode manter acento, mas nao deve dirigir regras de processamento.

## Plano de execucao seguro

### Etapa 1 - Definir padrao oficial
1. Estabelecer contrato de normalizacao textual para toda comparacao:
   - limpar ruido
   - minusculo
   - sem acento/til
   - espacos normalizados
2. Documentar esse contrato e evitar comparacao com texto bruto.

### Etapa 2 - Centralizar normalizacao
1. Manter uma unica funcao central para chave canonica em `normalizacao_services`/`texto_service`.
2. Garantir que filtros e mapas dependam dessa camada unica.
3. Evitar regras duplicadas espalhadas em modulos diferentes.

### Etapa 3 - Mapear pontos de aplicacao
Aplicar a chave canonica, no minimo, em:
1. Filtros de HSM na integracao.
2. Mapeamento de `STATUS_COLUNAS`.
3. Regras de `RESPOSTA` (sim/nao/sem resposta).
4. Regras de orquestracao que dependem de texto.

### Etapa 4 - Criar baseline antes da mudanca
Gerar referencia numerica para comparacao em testes:
1. Distribuicao de `Status` (bruto e normalizado).
2. Distribuicao de `RESPOSTA` (bruto e normalizado).
3. `total_status`, `com_match`, `sem_match`.
4. Totais finais por `PROCESSO` e `ACAO`.

Salvar baseline em arquivos dedicados (exemplo):
- `tests/baseline/status_baseline.json`
- `tests/baseline/status_resposta_baseline.json`

### Etapa 5 - Implementar em blocos pequenos
1. Bloco A: normalizacao central + testes unitarios.
2. Bloco B: integracao com `dataset_metricas_service`.
3. Bloco C: integracao com filtro de HSM e resposta.
4. Bloco D: validacao de pipelines principais.

### Etapa 6 - Testes obrigatorios de regressao
1. Unitarios: casos de texto quebrado e variacoes de acento/til.
2. Integracao: mapeamento de status/resposta.
3. Smoke: modos principais (`complicacao`, `internacao_eletivo`).
4. Regra de aceite: diferenca de metrica so e aceita com explicacao documentada.

## Critérios de aceite
1. Nenhum modo principal deve quebrar apos a migracao.
2. Contagens chave devem manter consistencia com baseline (salvo mudanca esperada).
3. Logs devem permitir rastrear valores nao mapeados.

## Proxima acao recomendada
1. Gerar baseline atual de `Status` e `RESPOSTA` antes de qualquer nova refatoracao.
2. Iniciar Bloco A com testes automatizados primeiro.
