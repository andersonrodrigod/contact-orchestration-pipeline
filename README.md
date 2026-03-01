# Contact Orchestration Pipeline

## Visao geral
Pipeline em duas etapas:
- `ingestao`: leitura, validacao, normalizacao e limpeza dos arquivos.
- `integracao`: filtro por HSM e merge entre status e status_resposta.
- `criacao_dataset`: prepara o arquivo final para relatorio.
- `orquestracao`: aplica classificacao (`PROCESSO`/`ACAO`) e orquestracao entre abas.

O projeto roda por `main.py` e escreve logs em `logs/`.

## Modos de execucao
- `ambos_com_resposta` (padrao): executa os dois fluxos com status_resposta.
- `ambos_somente_status`: executa os dois fluxos sem status_resposta.
- `complicacao_com_resposta` e `complicacao_somente_status`.
- `internacao_eletivo_com_resposta` e `internacao_eletivo_somente_status`.

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
- `src/data/arquivo_limpo/complicacao_final.xlsx`
- `src/data/arquivo_limpo/internacao_final.xlsx`

No modo `ambos`, cada fluxo usa arquivo intermediario proprio para evitar sobrescrita entre dependencias.

## Execucao
```bash
python main.py
```

Forcar modo:
```bash
python main.py --modo complicacao_com_resposta
python main.py --modo complicacao_somente_status
python main.py --modo internacao_eletivo_com_resposta
python main.py --modo internacao_eletivo_somente_status
python main.py --modo ambos_com_resposta
python main.py --modo ambos_somente_status
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
  `usuarios`, `usuarios_respondidos`, `usuarios_duplicados`, `usuarios_resolvidos`.
- na criacao de dataset, o logger valida e informa se todas as colunas obrigatorias
  do mapeamento foram encontradas no arquivo de origem.

## Logs
- Saida de execucao em `logs/<nome_pipeline>_<timestamp>.txt`.
- O terminal tambem mostra resumo final (`OK`, arquivo final e totais de match).

## Estrutura
- `main.py`: orquestracao dos modos e resumo final.
- `src/pipelines/complicacao_pipeline.py`: dependencia exclusiva da complicacao.
- `src/pipelines/internacao_eletivo_pipeline.py`: dependencia exclusiva de internacao+eletivo.
- `src/pipelines/join_status_resposta_pipeline.py`: unifica status + status_resposta (e versao somente status).
- `src/pipelines/concatenar_status_respostas_pipeline.py`: unifica status_respostas (eletivo + internacao).
- `src/pipelines/complicacao_status_pipeline.py`: ingestao + envio status + criacao do dataset_status de complicacao.
- `src/pipelines/internacao_eletivo_status_pipeline.py`: ingestao + envio status + criacao do dataset_status de internacao/eletivo.
- `src/pipelines/complicacao_orquestracao_pipeline.py`: orquestracao de complicacao.
- `src/pipelines/internacao_eletivo_orquestracao_pipeline.py`: orquestracao de internacao/eletivo.
- `src/services/ingestao_service.py`: regras de ingestao (status e status_resposta).
- `src/services/integracao_service.py`: regras de negocio da integracao (filtro + merge).
- `src/services/dataset_metricas_service.py`: contagens de status e agregados por chave/telefone.
- `src/services/orquestracao_service.py`: regras da etapa final (processo/acao e movimentacao para resolvidos).
- `src/services/padronizacao_service.py`: padronizacao de nomes de colunas das fontes.
- `src/services/texto_service.py`: normalizacao e limpeza textual compartilhada.
- `src/services/`: regras de padronizacao, normalizacao, validacao, integracao e dataset.
- `core/logger.py`: logger de execucao.
