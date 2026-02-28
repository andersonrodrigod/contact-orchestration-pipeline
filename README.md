# Contact Orchestration Pipeline

## Visao geral
Pipeline em duas etapas:
- `ingestao`: leitura, validacao, normalizacao e limpeza dos arquivos.
- `integracao`: filtro por HSM e merge entre status e status_resposta.

O projeto roda por `main.py` e escreve logs em `logs/`.

## Modos de execucao
- `auto` (padrao):
  - usa `complicacao` se existir arquivo de complicacao com dados.
  - senao, usa `unificar` se existirem eletivo e internacao com dados.
  - fallback final: `complicacao`.
- `complicacao`: usa apenas `status_resposta_complicacao`.
- `unificar`: concatena `eletivo + internacao` e segue pipeline.

## Entradas padrao
- `src/data/status.csv`
- `src/data/status_resposta_complicacao.csv`
- `src/data/status_respostas_eletivo.csv`
- `src/data/status_resposta_internacao.csv`

## Saidas padrao
- `src/data/arquivo_limpo/status_limpo.csv`
- `src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv`
- `src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv`
- `src/data/arquivo_limpo/status_complicacao_integrado.csv`
- `src/data/arquivo_limpo/status_unificado_integrado.csv`

## Execucao
```bash
python main.py
```

Forcar modo:
```bash
python main.py --modo complicacao
python main.py --modo unificar
```

## Regras principais de dados
- `Data agendamento` permanece no status.
- `DT ENVIO` e criado a partir de `Data agendamento` (somente data).
- `dat_atendimento` e padronizado para `DT_ATENDIMENTO`.
- `DT_ATENDIMENTO` e formatado em `dd/mm/yyyy`.
- merge na integracao usa `Contato + DT ENVIO` com `nom_contato + DT_ATENDIMENTO`.
- `RESPOSTA` recebe `"Sem resposta"` quando vier vazia.

## Logs
- Saida de execucao em `logs/<nome_pipeline>_<timestamp>.txt`.
- O terminal tambem mostra resumo final (`OK`, arquivo final e totais de match).

## Estrutura
- `main.py`: orquestracao dos modos e resumo final.
- `src/pipelines/ingestao_pipeline.py`: ingestao.
- `src/pipelines/integracao_pipeline.py`: integracao.
- `src/services/`: regras de schema, normalizacao, validacao, integracao e dataset.
- `core/logger.py`: logger de execucao.
