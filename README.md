# Contact Orchestration Pipeline

## Entrada de dados

Arquivos usados no pipeline de ingestao:

- `src/data/status.csv`
- `src/data/status_resposta_complicacao.csv`

## Como executar

Executar pelo arquivo principal:

```bash
python main.py
```

O `main.py` chama:

- `src/pipelines/ingestao_pipeline.py`
- funcao `run_ingestao_pipeline`

## Padronizacao aplicada

No pipeline de ingestao, as colunas de data sao padronizadas assim:

- `Data agendamento` -> `DT_ENVIO` (arquivo status)
- `dat_atendimento` -> `DT_ATENDIMENTO` (arquivo status_resposta_complicacao)

Depois da padronizacao:

- colunas de texto sao normalizadas para corrigir caracteres quebrados
- colunas de data permanecem como data (`datetime`)

## Validacoes de schema (datas)

Antes da padronizacao, o pipeline valida se as colunas obrigatorias existem:

- `Data agendamento` no arquivo status
- `dat_atendimento` no arquivo status_resposta_complicacao

Se faltar alguma coluna, o pipeline gera output para consumo futuro na interface CTkinter:

- `src/data/arquivo_limpo/output_validacao_datas.txt`

Formato do output:

```txt
OK=True|False
- mensagem 1
- mensagem 2
```

## Saidas geradas

- `src/data/arquivo_limpo/status_limpo.csv`
- `src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv`
- `src/data/arquivo_limpo/output_validacao_datas.txt`

## Sugestao de commit para esta etapa

```txt
feat(ingestao): padroniza colunas de data e valida schema de status/status_resposta
```
