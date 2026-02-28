# Contact Orchestration Pipeline

## Visao geral
Pipeline em duas etapas:
- `ingestao`: leitura, validacao, normalizacao e limpeza dos arquivos.
- `integracao`: filtro por HSM e merge entre status e status_resposta.
- `criacao_dataset`: prepara o arquivo final para relatorio.

O projeto roda por `main.py` e escreve logs em `logs/`.

## Modos de execucao
- `ambos` (padrao): executa `complicacao` e `internacao_eletivo` no mesmo run.
- `complicacao`: ingestao/integracao com `status_resposta_complicacao.csv` e criacao de dataset usando `complicacao.xlsx`.
- `internacao_eletivo`: concatena `eletivo + internacao` e segue pipeline.

## Entradas padrao
- `src/data/status.csv`
- `src/data/status_resposta_complicacao.csv`
- `src/data/complicacao.xlsx` (origem da criacao de dataset)
- `src/data/status_respostas_eletivo.csv`
- `src/data/status_resposta_internacao.csv`

## Saidas padrao
- `src/data/arquivo_limpo/status_complicacao_limpo.csv`
- `src/data/arquivo_limpo/status_internacao_eletivo_limpo.csv`
- `src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv`
- `src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv`
- `src/data/arquivo_limpo/status_complicacao.csv`
- `src/data/arquivo_limpo/status_internacao_eletivo.csv`
- `src/data/arquivo_limpo/dataset_complicacao.xlsx`
- `src/data/arquivo_limpo/dataset_internacao_eletivo.xlsx`

No modo `ambos`, cada fluxo usa arquivo intermediario proprio para evitar sobrescrita entre dependencias.

## Execucao
```bash
python main.py
```

Forcar modo:
```bash
python main.py --modo complicacao
python main.py --modo internacao_eletivo
python main.py --modo ambos
```

Os caminhos padrao de cada fluxo ficam definidos nos respectivos arquivos:
- `src/pipelines/complicacao_pipeline.py`
- `src/pipelines/internacao_eletivo_pipeline.py`

## Regras principais de dados
- `Data agendamento` permanece no status.
- `DT ENVIO` e criado a partir de `Data agendamento` (somente data).
- `dat_atendimento` e padronizado para `DT_ATENDIMENTO`.
- `DT_ATENDIMENTO` e formatado em `dd/mm/yyyy`.
- merge na integracao usa `Contato + DT ENVIO` com `nom_contato + DT_ATENDIMENTO`.
- `RESPOSTA` recebe `"Sem resposta"` quando vier vazia.
- no dataset final, `DT INTERNACAO` e `DT ENVIO` ficam como data; o resto vira texto.
- telefones (`Telefone`, `num_telefone`) sao normalizados removendo `.0` e caracteres nao numericos.
- no modo `complicacao`, o dataset final segue o padrao do legado com abas:
  `usuarios`, `usuarios_lidos`, `usuarios_respondidos`, `usuarios_duplicados`, `usuarios_resolvidos`.
- na criacao de dataset, o logger valida e informa se todas as colunas obrigatorias
  do mapeamento foram encontradas no arquivo de origem.

## Logs
- Saida de execucao em `logs/<nome_pipeline>_<timestamp>.txt`.
- O terminal tambem mostra resumo final (`OK`, arquivo final e totais de match).

## Estrutura
- `main.py`: orquestracao dos modos e resumo final.
- `src/pipelines/complicacao_pipeline.py`: dependencia exclusiva da complicacao.
- `src/pipelines/internacao_eletivo_pipeline.py`: dependencia exclusiva de internacao+eletivo.
- `src/pipelines/ingestao_pipeline.py`: ingestao.
- `src/pipelines/integracao_pipeline.py`: integracao.
- `src/pipelines/criacao_dataset_pipeline.py`: execucao da criacao de dataset (ativo no modo complicacao).
- `src/services/integracao_service.py`: regras de negocio da integracao (filtro + merge).
- `src/services/`: regras de schema, normalizacao, validacao, integracao e dataset.
- `core/logger.py`: logger de execucao.
