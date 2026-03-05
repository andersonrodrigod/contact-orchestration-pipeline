# Relatorio de Bugs e Riscos - Escalas (Revisao 2026-03-05)

Data: 2026-03-05  
Branch: chore/relatorio-bugs-escala-novo  
Escopo: codigo em `src/`  
Metodo: auditoria estatica + validacoes pontuais de comportamento

## Escala 1 (urgente - pode comprometer regra principal)

### 1) Regra `SEGUNDO_ENVIO_LIDO` pode ficar morta por falta da coluna `SEGUNDO_ENVIO`
- Onde: `src/services/orquestracao_service.py:83`, `src/services/orquestracao_service.py:102`, `src/config/schemas.py:43-95`
- Como o erro acontece:
  - A classificacao usa `s_lida_envio = _serie_numerica(df, 'SEGUNDO_ENVIO')`.
  - Se a coluna nao existe, `_serie_numerica` devolve zeros para todos os registros.
  - Com isso, a regra `s_lida_envio == 1` nunca dispara.
- Sinal no resultado:
  - Processo `SEGUNDO_ENVIO_LIDO` tende a nao aparecer nunca, mesmo em cenario esperado.
- Aviso para o usuario nao quebrar:
  - Antes de rodar orquestracao, valide se o arquivo tem a coluna `SEGUNDO_ENVIO` preenchida.
- Sugestao tecnica:
  - Ou incluir `SEGUNDO_ENVIO` no contrato do dataset final e no pipeline de contagens.
  - Ou remover essa regra se ela nao fizer mais parte da estrategia.
  - Ou falhar explicitamente quando a regra exigir coluna inexistente.

### 2) Duplicidade de coluna pode passar mascarada como `.1` e nao ser barrada
- Onde: `src/utils/arquivos.py:39-45`, `src/services/validacao_service.py:123-131`
- Como o erro acontece:
  - `pandas.read_csv` renomeia automaticamente duplicadas para sufixos como `P1.1`.
  - A validacao atual detecta duplicidade apenas por nome exato (`P1` repetido).
  - Resultado: pode entrar arquivo com duplicidade logica sem bloqueio.
- Sinal no resultado:
  - Coluna “quase igual” (`.1`, `.2`) aparece no dataframe, mas sem erro de duplicidade.
- Aviso para o usuario nao quebrar:
  - Se aparecer coluna com sufixo `.1/.2`, tratar como erro de layout e corrigir na origem.
- Sugestao tecnica:
  - Normalizar nomes removendo sufixo `.<numero>` antes da checagem de duplicidade.
  - Falhar com mensagem: “coluna duplicada mascarada detectada”.

## Escala 2 (medio impacto - pode degradar resultado sem parar fluxo)

### 3) Integracao descarta linhas com data invalida sem falhar quando nao e 100%
- Onde: `src/services/status_service.py:10-16`, `src/services/status_service.py:19-23`, `src/services/status_service.py:53-60`
- Como o erro acontece:
  - Linhas com `DT ENVIO`/`DT_ATENDIMENTO` invalidas sao filtradas fora.
  - A funcao so falha se 100% das linhas forem invalidas.
  - Se invalidas forem parciais, o pipeline segue com perda silenciosa de base.
- Sinal no resultado:
  - Queda em `com_match` e aumento de `sem_match` sem falha clara da etapa.
- Aviso para o usuario nao quebrar:
  - Monitorar volume de descartes de datas antes da integracao (na origem).
- Sugestao tecnica:
  - Expor contador de descartadas no retorno e no log final.
  - Definir limiar de falha configuravel (ex.: >5% invalido).

### 4) Limiar de 30% invalido agora bloqueia ingestao; risco operacional se fonte oscilar
- Onde: `src/services/ingestao_service.py:20`, `src/services/ingestao_service.py:130-148`, `src/services/ingestao_service.py:171-176`
- Como o erro acontece:
  - Se `NaT >= 30%` em `Data agendamento` ou `DT_ATENDIMENTO`, `ok=False`.
  - A etapa quebra mesmo que o restante do arquivo esteja consistente.
- Sinal no resultado:
  - Retorno com `ok=False` e mensagem de “Alerta de qualidade... NaT em XX%”.
- Aviso para o usuario nao quebrar:
  - Validar formato de data na extração (antes de gerar CSV/XLSX) e evitar campos vazios/texto.
- Sugestao tecnica:
  - Tornar limiar configuravel por ambiente (`dev/hml/prod`) e por fonte.
  - Emitir mensagem com amostra dos valores invalidos para facilitar saneamento.

### 5) Contrato de entrada do dataset e estrito; qualquer ausencia de coluna critica bloqueia lote
- Onde: `src/config/schemas.py:23-40`, `src/services/validacao_service.py:149-158`
- Como o erro acontece:
  - Falta de uma unica coluna obrigatoria (`P1`, `STATUS`, etc.) interrompe criacao do dataset.
- Sinal no resultado:
  - `ok=False` com lista de `colunas_faltando`.
- Aviso para o usuario nao quebrar:
  - Fazer checklist de colunas obrigatorias no arquivo de origem antes da execucao.
- Sugestao tecnica:
  - Adicionar etapa “pre-flight” separada para validar layout e retornar erro amigavel antes de processar.

## Escala 3 (baixo impacto - qualidade/manutenibilidade)

### 6) Fallback de parse de data usa inferencia elemento-a-elemento (mais lento e sujeito a ambiguidade)
- Onde: `src/services/normalizacao_services.py:86-90`
- Como o risco aparece:
  - No fallback, o pandas pode interpretar formatos ambigos de forma nao uniforme.
  - Em massa grande, o parse fica mais custoso.
- Sinal no resultado:
  - Possiveis warnings de inferencia de formato; tempo maior de normalizacao.
- Aviso para o usuario nao quebrar:
  - Padronizar datas na origem para `dd/mm/yyyy` ou `yyyy-mm-dd` sem misturar no mesmo campo.
- Sugestao tecnica:
  - Definir parser por regex de formato e aplicar conversao deterministica por bloco.

## Checklist rapido para operador (evitar quebra antes de rodar)
1. Verificar se todas as colunas obrigatorias existem exatamente com os nomes esperados.
2. Verificar se nao existem colunas duplicadas visiveis ou mascaradas (`.1`, `.2`).
3. Verificar amostra de datas das colunas criticas (`Data agendamento`, `DT_ATENDIMENTO`, `DT ENVIO`).
4. Confirmar que `% de data invalida` esta abaixo do limiar operacional definido.
5. Confirmar se a coluna usada por regra de negocio (`SEGUNDO_ENVIO`) existe quando a regra estiver ativa.
