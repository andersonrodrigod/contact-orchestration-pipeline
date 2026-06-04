# Relatorio de leitura - ingestao_service.py

Arquivo analisado:

- `src/services/ingestao_service.py`

Objetivo deste relatorio:

- Mostrar a ordem em que o arquivo trabalha.
- Explicar o que cada parte faz.
- Marcar pontos candidatos a limpeza/refatoracao.
- Servir como guia para ajustar o arquivo por partes, sem mexer no fluxo inteiro de uma vez.

## Visao geral

O `ingestao_service.py` e a primeira etapa que realmente mexe nos dados do fluxo de complicacao.

Antes dele, os arquivos principais apenas escolhem modo e coordenam chamada. Dentro dele comeca a leitura dos arquivos, validacao, padronizacao, normalizacao, limpeza de texto, criacao de coluna derivada e salvamento.

Hoje o arquivo mistura responsabilidades diferentes:

- configuracao de regras de governanca;
- controle de logger;
- leitura de arquivo;
- validacao de estrutura;
- padronizacao de colunas;
- normalizacao de datas;
- limpeza de texto;
- controle de qualidade de dados;
- montagem de retorno;
- salvamento em disco;
- tratamento de erro;
- modo com `status_resposta`;
- modo `somente_status`.

Por isso ele parece grande: ele nao faz apenas ingestao, ele tambem coordena varias regras ao redor da ingestao.

## Ordem de leitura recomendada

Para entender e refatorar com seguranca, leia nesta ordem:

1. Constantes e estado inicial.
2. Helpers pequenos.
3. `executar_ingestao_complicacao`.
4. `executar_normalizacao_padronizacao`.
5. Funcoes privadas chamadas por `executar_normalizacao_padronizacao`.
6. `executar_ingestao_somente_status`.
7. Funcoes importadas de outros services.

Essa ordem e melhor que ler de cima para baixo porque a funcao publica mostra o objetivo, e depois voce entra nos detalhes.

## 1. Imports

Parte inicial do arquivo.

O que faz:

- importa `PipelineLogger`;
- importa codigos de erro;
- importa resolucao de governanca;
- importa normalizacao de texto/data;
- importa padronizacao;
- importa contrato de resposta;
- importa validacao;
- importa leitura e escrita de arquivos.

Por que importa tanta coisa:

- este arquivo e um coordenador de ingestao;
- ele conhece muitos detalhes de outras camadas.

Ponto de atencao:

- muitos imports indicam acoplamento alto;
- isso nao quebra o codigo, mas dificulta teste e manutencao;
- no futuro, parte dessas dependencias pode ser movida para services menores.

## 2. `_mensagem_alerta_nat`

O que faz:

- monta uma mensagem padrao quando uma coluna de data tem muitos valores invalidos;
- recebe coluna, percentual, quantidade e total;
- retorna texto como alerta de qualidade.

Uso atual:

- usada para `Data agendamento`;
- usada para `DT_ATENDIMENTO`;
- usada no fluxo `somente_status`.

Ponto de atencao:

- essa funcao esta ok;
- pode continuar existindo;
- ela e pequena e evita repeticao de mensagem.

## 3. Constantes de limpeza de texto

Constantes:

- `COLUNAS_TEXTO_ALVO_STATUS`
- `COLUNAS_TEXTO_ALVO_STATUS_RESPOSTA`

O que fazem:

- definem quais colunas passam por limpeza de texto;
- no status, tenta limpar `HSM`, `Status`, `Respondido`, `RESPOSTA`;
- no status_resposta, tenta limpar `HSM`, `Status`, `Respondido`, `resposta`.

Comportamento importante:

- a limpeza so acontece se a coluna existir;
- se `RESPOSTA` nao existir no status, nada acontece com ela;
- se `resposta` nao existir no status_resposta depois da padronizacao, o contrato deve falhar antes.

Ponto de ajuste possivel:

- avaliar se `RESPOSTA` ainda precisa estar em `COLUNAS_TEXTO_ALVO_STATUS`;
- se o arquivo `status.csv` original nao usa mais essa coluna, pode ser legado;
- nao remover antes de confirmar com dados reais ou teste.

## 4. `_inicializar_estado_normalizacao`

O que faz:

- cria um dicionario com metricas iniciais;
- guarda alertas;
- guarda erros de qualidade de data;
- guarda quantidade e percentual de `NaT` no status;
- guarda quantidade e percentual de `NaT` no status_resposta.

Campos principais:

- `alertas_data`;
- `erros_qualidade_data`;
- `nat_status`;
- `pct_nat_status`;
- `nat_resposta`;
- `pct_nat_resposta`.

Ponto de ajuste possivel:

- esse dicionario poderia virar uma estrutura mais clara;
- hoje os nomes internos sao diferentes dos nomes finais do resultado;
- ainda assim, ele e util para concentrar estado da execucao.

## 5. `_ler_arquivos_status`

O que faz:

- loga leitura do arquivo status;
- chama `ler_arquivo_csv(arquivo_status)`;
- loga linhas e colunas do status;
- loga leitura do arquivo status_resposta;
- chama `ler_arquivo_csv(arquivo_status_resposta)`;
- loga linhas e colunas do status_resposta;
- retorna `df_status` e `df_status_resposta`.

Ordem:

1. ler status;
2. medir tamanho do status;
3. ler status_resposta;
4. medir tamanho do status_resposta;
5. devolver os dois DataFrames.

Ponto de ajuste possivel:

- esta funcao esta simples;
- nao parece ser prioridade de refatoracao;
- poderia no futuro virar uma funcao generica de leitura com logging.

## 6. `_validar_colunas_origem_normalizacao`

O que faz:

- valida os DataFrames antes da padronizacao;
- chama `validar_colunas_origem_para_padronizacao`;
- considera modo estrito de alias da resposta;
- considera janela de corte de alias da resposta;
- loga se a validacao foi ok e quais mensagens gerou;
- retorna o resultado da validacao.

Por que existe antes da padronizacao:

- porque precisa saber se o arquivo veio com colunas originais esperadas;
- tambem diagnostica aliases legados como `Resposta` e `RESPOSTA`.

Ponto de ajuste possivel:

- esta funcao e uma boa fronteira;
- pode continuar;
- talvez so simplificar nome no futuro.

## 7. `_aplicar_padronizacao_status_e_resposta`

O que faz:

- chama `padronizar_colunas_status`;
- chama `padronizar_colunas_status_resposta`;
- chama `garantir_contrato_resposta_canonica`;
- retorna os dois DataFrames padronizados.

Resultado esperado:

- status fica com colunas no formato esperado;
- status_resposta fica com a coluna canonica `resposta`;
- aliases legados de resposta sao removidos;
- se o contrato de resposta estiver invalido, gera erro.

Ponto de ajuste possivel:

- esta parte esta bem separada;
- pode ser mantida;
- importante nao misturar com limpeza de texto.

## 8. `_normalizar_tipos_e_coletar_qualidade_data`

O que faz:

- converte `Data agendamento` para datetime;
- converte `DT_ATENDIMENTO` para datetime;
- loga os tipos resultantes;
- conta quantos valores viraram `NaT`;
- calcula percentual de `NaT`;
- compara com `limiar_nat_data`;
- se passar do limiar, registra erro de qualidade.

Ordem interna:

1. normalizar tipos do status;
2. normalizar tipos do status_resposta;
3. logar tipo de `Data agendamento`;
4. logar tipo de `DT_ATENDIMENTO`;
5. medir qualidade de `Data agendamento`;
6. medir qualidade de `DT_ATENDIMENTO`;
7. retornar DataFrames normalizados.

Ponto de ajuste importante:

- existe repeticao entre `Data agendamento` e `DT_ATENDIMENTO`;
- a regra de contar `NaT`, calcular percentual e comparar limiar aparece duas vezes;
- esta e uma das primeiras partes boas para reduzir codigo.

Refatoracao sugerida:

- criar uma funcao tipo `_avaliar_qualidade_data(df, coluna, limiar, logger)`;
- ela retornaria `nat`, `pct_nat` e lista de erros;
- depois status e resposta usariam a mesma funcao.

## 8.1. `DT ENVIO` ainda esta na ingestao por compatibilidade

Situacao atual:

- a coluna `DT ENVIO` e criada dentro do `ingestao_service.py`;
- ela nasce a partir de `Data agendamento`;
- isso acontece antes do salvamento do arquivo limpo.

Decisao:

- por regra de responsabilidade, `DT ENVIO` nao deveria ser criada nesta fase;
- a ingestao deveria apenas validar, padronizar, converter tipos e limpar dados;
- criacao de coluna derivada deveria ficar em uma etapa posterior do pipeline.

Por que ainda nao remover:

- outras etapas esperam `DT ENVIO` no arquivo limpo;
- remover agora pode quebrar integracao, analises, graficos ou orquestracao;
- por enquanto fica na ingestao para manter compatibilidade.

Plano futuro:

1. mapear todos os pontos que usam `DT ENVIO`;
2. escolher a etapa correta para criar essa coluna derivada;
3. criar teste garantindo que `DT ENVIO` existe antes da primeira etapa que precisa dela;
4. mover a criacao para essa etapa;
5. remover a criacao de dentro da ingestao.

## 9. `_montar_resultado_normalizacao`

O que faz:

- monta o retorno padrao da normalizacao;
- decide `ok`;
- junta mensagens iniciais, mensagens de validacao, alertas e erros;
- inclui metricas de data;
- inclui configuracoes usadas;
- monta `qualidade_data`;
- monta `metricas_por_etapa`.

Ponto de ajuste importante:

- ha duplicacao de metricas;
- os mesmos valores aparecem no topo do resultado, em `qualidade_data` e em `metricas_por_etapa`;
- isso pode ser necessario para compatibilidade com outras partes, mas deixa a funcao grande.

Refatoracao sugerida:

- primeiro manter o formato externo igual;
- criar helpers internos para montar:
  - metricas de data;
  - qualidade de data;
  - metricas por etapa.

## 10. `executar_normalizacao_padronizacao`

Esta e a funcao principal do fluxo com `status` e `status_resposta`.

O que ela faz:

1. prepara mensagens iniciais;
2. cria logger se nenhum logger foi recebido;
3. loga parametros de entrada e saida;
4. cria estado inicial;
5. le status e status_resposta;
6. valida colunas de origem;
7. se a validacao falhar, retorna erro sem salvar;
8. padroniza status e status_resposta;
9. normaliza tipos;
10. limpa texto nas colunas alvo;
11. cria `DT ENVIO`;
15. valida colunas de data depois das transformacoes;
16. monta resultado final;
17. se resultado nao for ok, bloqueia salvamento;
18. formata `DT_ATENDIMENTO` para padrao BR;
19. salva status limpo;
20. salva status_resposta limpo;
21. finaliza logger;
22. retorna resultado.

O que ela controla:

- fluxo;
- configuracao;
- logger;
- validacao;
- transformacao;
- qualidade;
- escrita;
- erro inesperado.

Ponto de ajuste principal:

- esta funcao e grande demais porque coordena tudo;
- no primeiro momento, nao precisa quebrar tudo;
- o melhor e extrair partes repetidas sem mudar a assinatura.

Primeiros ajustes sugeridos:

1. extrair avaliacao de qualidade de data;
2. extrair montagem de metricas de data;
3. extrair bloco de bloqueio de saida;
4. depois pensar em separar leitura/processamento/salvamento.

## 11. `executar_ingestao_complicacao`

O que faz:

- e a fachada publica usada pela pipeline de complicacao;
- cria logger `ingestao_complicacao` se necessario;
- loga modo de complicacao;
- chama `executar_normalizacao_padronizacao`;
- passa caminhos de status e status_resposta;
- injeta mensagem inicial `Modo complicacao selecionado.`;
- retorna o resultado.

Situacao atual:

- o fluxo adicional XLSX foi removido;
- o parametro `executar_xlsx_adicional` ainda existe para compatibilidade;
- ele nao aciona mais processamento dentro deste service.

Ponto de ajuste possivel:

- em uma etapa futura, remover o parametro `executar_xlsx_adicional` dos callers;
- antes disso, localizar chamadas em `src/cli/modos_individuais.py` e `src/pipelines/complicacao_status_pipeline.py`;
- remover com teste para garantir que nenhum modo quebre.

## 12. `executar_ingestao_somente_status`

O que faz:

1. cria logger;
2. resolve limiar de data;
3. loga entrada e saida;
4. le status;
5. padroniza colunas do status;
6. normaliza `Data agendamento`;
7. calcula `NaT` e percentual;
8. se passar do limiar, registra erro;
9. limpa texto;
10. cria `DT ENVIO`;
11. se teve erro de qualidade, bloqueia salvamento;
12. salva status limpo;
13. retorna metricas;
14. se der excecao, retorna erro de ingestao.

Ponto de ajuste importante:

- repete parte da logica de `executar_normalizacao_padronizacao`;
- principalmente qualidade de `Data agendamento`;
- tambem repete montagem de retorno de qualidade.

Refatoracao sugerida:

- reaproveitar a futura funcao `_avaliar_qualidade_data`;
- reaproveitar helper de montagem de qualidade de data;
- manter esta funcao como fachada do modo somente status.

## Ordem sugerida de ajustes dentro deste arquivo

### Passo 1 - Limpar legado ja removido

Status atual:

- helpers XLSX removidos;
- bloco XLSX dentro da ingestao removido;
- parametro `executar_xlsx_adicional` mantido por compatibilidade.

Proximo ajuste possivel:

- remover callers que passam `executar_xlsx_adicional=True`;
- depois remover o parametro da assinatura.

### Passo 2 - Reduzir repeticao de qualidade de data

Alvo:

- `_normalizar_tipos_e_coletar_qualidade_data`;
- `executar_ingestao_somente_status`.

Criar helper:

```python
def _avaliar_qualidade_data(df, coluna, limiar_nat_data, logger):
    ...
```

Resultado esperado:

- uma regra unica para contar `NaT`;
- menos codigo duplicado;
- menor risco de status e resposta divergirem.

### Passo 3 - Isolar processamento do status

Alvo:

- bloco de status dentro do fluxo completo;
- bloco do `somente_status`.

Ideia:

```python
def _processar_status(df_status, limiar_nat_data, logger):
    ...
```

Deve fazer:

- padronizar colunas;
- normalizar `Data agendamento`;
- limpar texto;
- criar `DT ENVIO`;
- devolver DataFrame e metricas.

### Passo 4 - Isolar processamento do status_resposta

Ideia:

```python
def _processar_status_resposta(df_status_resposta, limiar_nat_data, logger):
    ...
```

Deve fazer:

- padronizar colunas;
- garantir contrato canonico `resposta`;
- normalizar `DT_ATENDIMENTO`;
- limpar texto;
- devolver DataFrame e metricas.

### Passo 5 - Simplificar retorno

Alvo:

- `_montar_resultado_normalizacao`;
- retorno de `executar_ingestao_somente_status`.

Ideia:

- criar helper para metricas de qualidade;
- manter as chaves atuais para nao quebrar pipeline;
- reduzir repeticao visual do codigo.

### Passo 6 - Separar leitura e escrita

Alvo:

- `executar_normalizacao_padronizacao`;
- `executar_ingestao_somente_status`.

Ideia:

- deixar leitura e salvamento nas funcoes publicas;
- deixar transformacoes em helpers puros;
- isso facilita teste sem arquivo real.

## O que eu nao mexeria primeiro

- `validar_colunas_origem_para_padronizacao`;
- `padronizar_colunas_status`;
- `padronizar_colunas_status_resposta`;
- `garantir_contrato_resposta_canonica`;
- `normalizar_tipos_dataframe`.

Motivo:

- essas funcoes estao fora do arquivo;
- primeiro vale reduzir a coordenacao e duplicacao dentro da ingestao;
- depois a gente decide se os services auxiliares tambem precisam limpeza.

## Checklist para cada ajuste

Depois de cada mudanca pequena, rodar:

```powershell
python -m compileall -q src\services\ingestao_service.py
python -m unittest discover -s tests -p "test*.py"
python main.py --modo complicacao_com_resposta
```

Se o ajuste mexer apenas em `somente_status`, rodar tambem:

```powershell
python main.py --modo complicacao_somente_status
```

## Primeira decisao recomendada

O proximo ajuste mais seguro e reduzir a repeticao da qualidade de data.

Comecaria por extrair a regra:

- contar `NaT`;
- calcular percentual;
- gerar mensagem se passar do limiar;
- retornar metricas.

Isso diminui codigo sem mudar entrada, saida ou comportamento da pipeline.
