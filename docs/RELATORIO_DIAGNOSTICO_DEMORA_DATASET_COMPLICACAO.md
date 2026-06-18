# Relatorio de diagnostico: demora na geracao do dataset de complicacao

Data da investigacao: 18/06/2026

## Objetivo

Documentar o problema encontrado durante a geracao do dataset de complicacao,
registrando evidencias, causa provavel, impacto, correcao aplicada e pontos de
atencao para proximos arquivos.

Este documento tambem separa a discussao sobre `CHAVE_SENHA`, porque ela foi um
segundo problema encontrado na mesma investigacao e precisa ser entendida como
parte diferente do gargalo de performance.

## Sintoma observado

A geracao do dataset pela interface ficou em execucao por aproximadamente 30
minutos.

O comportamento esperado era a geracao do arquivo
`src/data/arquivo_limpo/complicacao_status.xlsx` em poucos minutos, considerando
que a base real tinha pouco mais de 11 mil registros uteis.

## Primeiro aviso observado

Antes da investigacao de performance, apareceu o seguinte aviso do pandas:

```text
UserWarning: Could not infer format, so each element will be parsed individually,
falling back to dateutil. To ensure parsing is consistent and as-expected,
please specify a format.
```

Esse aviso vinha de `src/services/normalizacao_services.py`, na conversao de
colunas de data.

Esse aviso nao foi a causa principal da demora de 30 minutos. Ele indica que
alguma coluna de data pode ter formatos misturados ou ambiguos, mas a demora
principal estava ligada ao tamanho efetivo que o Excel informava para a planilha.

## Evidencias coletadas

Foi criado um script temporario de diagnostico em:

```text
scripts/diagnosticar_dataset_perf.py
```

Ele mede as etapas da geracao do dataset:

- leitura do Excel;
- validacao de colunas;
- carregamento do status;
- preparacao das contagens;
- montagem da aba `usuarios`;
- enriquecimento da aba principal;
- montagem e enriquecimento de `usuarios_respondidos`;
- persistencia do XLSX final.

Na primeira leitura do arquivo `src/data/complicacao.xlsx`, o pandas carregou:

```text
linhas=1048575
colunas=65
```

Esse numero e muito relevante: `1.048.575` linhas e praticamente o limite maximo
de linhas de uma planilha Excel.

Depois foi verificado quantas linhas realmente tinham algum valor preenchido:

```text
linhas_total=1048575
linhas_com_algum_valor=11722
```

Ou seja:

- o Excel parecia ter mais de 1 milhao de linhas;
- mas apenas 11.722 linhas tinham conteudo real;
- havia linhas vazias ou quase vazias estendendo a area usada da planilha.

Tambem foi encontrada uma linha no fim da planilha com apenas um valor:

```text
DATA DO CONTATO = 2026-06-17 00:00:00
```

Isso indica que a planilha provavelmente ficou com lixo, formatacao, celula
editada ou valor isolado muito abaixo da base real.

## Causa raiz da demora

A causa principal da demora foi o arquivo Excel estar com a area usada expandida
ate quase o limite maximo da planilha.

Mesmo que a maioria das linhas estivesse vazia, o `pandas.read_excel`, usando o
motor `openpyxl`, precisou carregar a planilha considerando essa area enorme.

Depois disso, o pipeline tentava processar e gravar dados derivados dessa leitura.
Isso aumenta muito o tempo de:

- leitura do arquivo Excel;
- normalizacao de colunas;
- aplicacao de regras por coluna;
- gravacao do arquivo XLSX final.

## Impacto pratico

Antes da correcao, a geracao podia ficar aparentemente travada por muito tempo.

O problema era especialmente perigoso porque a interface nao mostrava claramente
qual etapa estava demorando. Para quem esta operando o processo, parece que o
sistema simplesmente congelou.

Esse tipo de problema pode acontecer novamente se outro arquivo Excel vier com:

- linhas vazias formatadas ate o final da planilha;
- celula preenchida isolada muito abaixo da base real;
- colunas ou linhas "fantasmas";
- area usada do Excel muito maior que a area de dados real;
- arquivo exportado por outro sistema com sujeira no fim.

## Correcao aplicada na leitura de Excel

Foi alterado o utilitario de leitura em:

```text
src/utils/arquivos.py
```

A funcao `ler_arquivo_csv` ja removia colunas vazias de Excel. Foi adicionada
tambem a remocao de linhas totalmente vazias.

Com isso, ao ler um `.xlsx`, o fluxo agora:

1. le o arquivo com `pandas.read_excel`;
2. remove colunas vazias;
3. remove linhas totalmente vazias;
4. devolve apenas o DataFrame util para o restante do pipeline.

Depois da correcao, a mesma base passou a ser considerada com:

```text
linhas=11722
colunas=65
```

## Resultado apos correcao

A geracao oficial foi executada com sucesso.

Arquivo gerado:

```text
src/data/arquivo_limpo/complicacao_status.xlsx
```

Tempo observado:

```text
94,68 segundos
```

Abas validadas:

```text
usuarios: 11722 linhas, 52 colunas
usuarios_respondidos: 4482 linhas, 52 colunas
usuarios_resolvidos: 0 linhas, 52 colunas
```

Indicadores retornados pela execucao:

```text
total_dataset=11722
total_match=6453
total_sem_match=5269
qtd_identificacao=6453
qtd_resposta=6453
```

## Parte separada: problema com CHAVE_SENHA

Durante a mesma investigacao, apareceu um segundo problema, separado da demora
do Excel.

O arquivo de status tinha a coluna `Contato` preenchida, mas a coluna
`CHAVE_SENHA` ficava vazia dentro do lookup usado pelo dataset.

Exemplo de origem no status:

```text
Contato = MURILO LOCIR MACHERALDI_HOSPITAL RIBEIRAO PRETO_..._AB7943476
```

O esperado era extrair:

```text
CHAVE_SENHA = AB7943476
```

Mas a funcao que adicionava `CHAVE_SENHA` escolhia a primeira coluna existente
da lista de origens. Como `CHAVE_SENHA` ja existia, mesmo vazia, ela era usada
como origem e impedia o fallback para `Contato`.

Na pratica:

- a coluna `CHAVE_SENHA` existia;
- estava vazia;
- a funcao aceitava essa coluna vazia;
- nao tentava extrair a senha de `Contato`;
- as contagens por `CHAVE_SENHA` falhavam.

## Correcao aplicada na CHAVE_SENHA

Foi alterado:

```text
src/services/schema_chave_service.py
```

Antes, a funcao escolhia uma unica coluna de origem e preenchia `CHAVE_SENHA`
com base nela.

Agora, a funcao:

1. preserva `CHAVE_SENHA` quando ela ja esta preenchida;
2. se `CHAVE_SENHA` estiver vazia, tenta preencher usando as proximas origens;
3. permite cair para `Contato`, `CHAVE` ou outra coluna configurada;
4. extrai a senha pelo ultimo trecho apos `_`.

Isso evita perder a chave quando uma coluna existe, mas esta vazia.

## Testes adicionados

Foram adicionados testes para proteger os dois problemas.

Arquivo:

```text
tests/test_schema_chave_service.py
```

Casos cobertos:

- preencher `CHAVE_SENHA` vazia usando `Contato`;
- preservar `CHAVE_SENHA` ja existente;
- manter os comportamentos anteriores de extracao.

Arquivo:

```text
tests/test_arquivos_utils.py
```

Caso coberto:

- remover linhas totalmente vazias de uma base lida como Excel.

Comando executado:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_schema_chave_service tests.test_arquivos_utils tests.test_dataset_metricas_service
```

Resultado:

```text
Ran 7 tests in 0.124s
OK
```

## Como averiguar se acontecer novamente

Quando outro arquivo demorar muito para gerar dataset, verificar primeiro:

1. Quantas linhas o pandas esta lendo.
2. Quantas linhas realmente tem algum valor.
3. Se existe alguma linha preenchida muito distante do fim real da base.
4. Se o arquivo esta no limite do Excel.
5. Se `CHAVE_SENHA` esta preenchida depois da ingestao/status.
6. Se o arquivo final esta demorando na leitura, processamento ou escrita.

O script de diagnostico pode ser usado assim:

```powershell
.\.venv\Scripts\python.exe scripts\diagnosticar_dataset_perf.py --origem src/data/complicacao.xlsx
```

Ele mostra a duracao de cada etapa. O objetivo e evitar ficar muitos minutos sem
saber se o problema esta na leitura, no enriquecimento ou na gravacao.

## Licoes aprendidas

1. Um arquivo Excel pequeno visualmente pode estar enorme para o pandas se a area
   usada da planilha estiver expandida.
2. Linhas totalmente vazias precisam ser removidas logo na entrada do pipeline.
3. `CHAVE_SENHA` deve ser tratada como contrato principal, mas coluna existente
   e vazia nao pode impedir fallback para outra origem confiavel.
4. Diagnostico por etapas e essencial para descobrir gargalo real.
5. A interface deveria, futuramente, mostrar progresso por etapa para evitar a
   sensacao de travamento.

## Melhorias futuras sugeridas

- Mostrar na UI a etapa atual da geracao do dataset.
- Alertar quando o arquivo Excel tiver muitas linhas vazias removidas.
- Registrar no log a quantidade de linhas antes e depois da limpeza.
- Considerar uma validacao preflight para detectar planilhas no limite do Excel.
- Avaliar exportar ou processar bases grandes em CSV quando possivel, porque CSV
  costuma ser mais rapido que XLSX para leitura em massa.

