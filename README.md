# Contact Orchestration Pipeline

## Visao geral
Pipeline focado no fluxo de complicacao:
- `ingestao`: leitura, validacao, normalizacao e limpeza dos arquivos.
- `integracao`: filtro por HSM e merge entre status e status_resposta.
- `criacao_dataset`: prepara o arquivo final para relatorio.
- `orquestracao`: aplica classificacao (`PROCESSO`/`ACAO`) e orquestracao entre abas.

O projeto roda por `main.py` e escreve logs em `logs/`.

## Modos de execucao
- `complicacao_com_resposta` (padrao).
- `complicacao_gerar_status_dataset`.
- `complicacao_orquestracao`.
- `preflight_complicacao`.
- `complicacao_ingestao`.
- `complicacao_integrar_status_resposta`.
- `complicacao_criar_dataset_status`.
- `complicacao_gerar_dataset_status`.
- `complicacao_orquestrar`.

## Entradas padrao
- `src/data/status.csv`
- `src/data/status_resposta.csv`
- `src/data/complicacao.xlsx`

## Saidas padrao
- `src/data/arquivo_limpo/status_complicacao_limpo.csv`
- `src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv`
- `src/data/arquivo_limpo/status_complicacao.csv`
- `src/data/arquivo_limpo/complicacao_status.xlsx`
- `src/data/arquivo_limpo/complicacao_final.xlsx`

## Execucao
```bash
python main.py
```

Forcar modo:
```bash
python main.py --modo complicacao_com_resposta
python main.py --modo complicacao_gerar_status_dataset
python main.py --modo complicacao_orquestracao
python main.py --modo preflight_complicacao
python main.py --modo complicacao_ingestao
python main.py --modo complicacao_integrar_status_resposta
python main.py --modo complicacao_criar_dataset_status
python main.py --modo complicacao_gerar_dataset_status
python main.py --modo complicacao_orquestrar
```

## Documentacao da UI
- Guia de uso e manutencao da interface: `README_UI_PASSO_A_PASSO.md`

## Regras principais de dados
- `Data agendamento` permanece no status.
- `DT ENVIO` e criado a partir de `Data agendamento` (somente data).
- `dat_atendimento` e padronizado para `DT_ATENDIMENTO`.
- `DT_ATENDIMENTO` e formatado em `dd/mm/yyyy`.
- merge na integracao usa `Contato + DT ENVIO` com `nom_contato + DT_ATENDIMENTO`.
- `RESPOSTA` recebe `"Sem resposta"` quando vier vazia.
- no dataset final, `DT INTERNACAO` e `DT ENVIO` ficam como data; o resto vira texto.
- telefones (`Telefone`, `num_telefone`) sao normalizados removendo `.0` e caracteres nao numericos.
- o dataset final segue o padrao do legado com abas:
  `usuarios`, `usuarios_respondidos`, `usuarios_duplicados`, `usuarios_resolvidos`.
- na criacao de dataset, o logger valida e informa se todas as colunas obrigatorias
  do mapeamento foram encontradas no arquivo de origem.

## Logs
- Saida de execucao em `logs/<nome_pipeline>_<timestamp>.txt`.
- O terminal tambem mostra resumo final (`OK`, arquivo final e totais de match).

## Estrutura
- `main.py`: orquestracao dos modos e resumo final.
- `src/pipelines/complicacao_pipeline.py`: fluxo principal de complicacao.
- `src/pipelines/complicacao_status_pipeline.py`: ingestao + envio status + criacao do dataset_status.
- `src/pipelines/complicacao_orquestracao_pipeline.py`: orquestracao.
- `src/pipelines/join_status_resposta_pipeline.py`: unifica status + status_resposta e versao somente status.
- `src/services/ingestao_service.py`: regras de ingestao.
- `src/services/integracao_service.py`: regras de negocio da integracao.
- `src/services/dataset_metricas_service.py`: contagens de status e agregados por chave/telefone.
- `src/services/orquestracao_service.py`: regras da etapa final.
- `src/services/padronizacao_service.py`: padronizacao de nomes de colunas das fontes.
- `src/services/texto_service.py`: normalizacao e limpeza textual compartilhada.
- `core/logger.py`: logger de execucao.
