# README - Falhas Criticas e Plano de Correcao

## Objetivo
Documentar falhas criticas encontradas no projeto e definir solucoes praticas para reduzir risco de quebra em producao.

## 1) NQA calculado incorretamente (complicacao e internacao)

### Erro
- A comparacao usa `nao_quis`, mas a normalizacao gera `nao quis`.
- Resultado: contagem de NQA tende a `0` mesmo quando ha registros validos.

### Onde ocorre
- `src/services/resumo_complicacao_service.py`
- `src/services/resumo_internacao_service.py`

### Impacto
- KPI de NQA fica errado no CSV e em todos os graficos/tabelas dependentes.

### Solucao
- Padronizar comparacao para `nao quis` (ou usar funcao utilitaria unica para status canonico).
- Adicionar teste de regressao validando que registros "Nao quis" entram em `NQA`.

---

## 2) Regra de video depende de texto quebrado por encoding

### Erro
- A logica de video compara `RP1 == 'NÃ£o quis'`.
- Quando o texto vier corrigido/normalizado, a contagem nao bate.

### Onde ocorre
- `src/services/resumo_complicacao_service.py`

### Impacto
- `TOTAL_VIDEO_SIM_NQA` e `TOTAL_VIDEO_RESPONDIDO_SIM` podem ficar incorretos.

### Solucao
- Comparar valor normalizado (`simplificar_texto`) com `nao quis`.
- Evitar comparacoes por literal com acento/encoding.
- Cobrir com teste de 3 cenarios:
1. `Nao quis` correto
2. `NÃ£o quis` quebrado
3. vazio/nulo

---

## 3) Falha silenciosa na geracao de tabela/grafico

### Erro
- Quando `matplotlib` falha/indisponivel, alguns servicos retornam `ok=True` com arquivos vazios ou "ignorados".

### Onde ocorre
- `src/services/tabela_resumo_complicacao_service.py`
- `src/services/graficos_status_enviado_service.py`
- `src/services/graficos_uniao_status_resposta_service.py`
- `src/services/graficos_orquestracao_service.py`

### Impacto
- Pipeline aparenta sucesso, mas a entrega visual nao foi gerada.

### Solucao
- Regra de severidade:
1. Se etapa de grafico for obrigatoria: retornar `ok=False`.
2. Se opcional: manter `ok=True`, mas retornar flag explicita `degradado=True`.
- Registrar no resultado principal contadores minimos esperados vs gerados.

---

## 4) Colunas obrigatorias ausentes nao interrompem resumo

### Erro
- Se faltar coluna, codigo preenche vazio e continua calculo.
- Hoje a ausencia vira apenas mensagem.

### Onde ocorre
- `src/services/resumo_complicacao_service.py`
- `src/services/resumo_internacao_service.py`

### Impacto
- CSV final pode ser gerado com metricas incorretas sem bloquear a execucao.

### Solucao
- Definir colunas obrigatorias por contexto.
- Se faltarem colunas obrigatorias: abortar com `ok=False` e erro objetivo.
- Manter apenas colunas opcionais como warning.

---

## 5) Datas invalidas no arquivo origem

### Erro
- Warning recorrente de serial de data invalido no Excel (openpyxl).

### Impacto
- Filtros por data podem desconsiderar linhas ou interpretar datas incorretamente.

### Solucao
- Criar validacao preflight para datas:
1. percentual de datas invalidas por coluna critica
2. exemplos de linhas invalidas
3. bloqueio se ultrapassar limite configurado

---

## Plano de execucao recomendado (ordem)
1. Corrigir `NQA` e regra de `Nao quis` no video.
2. Implementar validacao hard-fail para colunas obrigatorias.
3. Ajustar politica de erro em graficos (`ok=False` ou `degradado=True`).
4. Criar testes de regressao para resumo e video.
5. Adicionar preflight de qualidade de datas.

## Criterios de aceite
- `NQA` diferente de zero quando houver `Nao quis` no dataset.
- Contagens de video batendo com amostra manual.
- Pipeline nao pode finalizar "sucesso" sem artefatos obrigatorios.
- Falta de coluna obrigatoria deve interromper execucao com mensagem clara.

## Observacao final
Este documento descreve falhas de alta prioridade operacional. O ideal e aplicar correcoes em patches pequenos e validados por teste para reduzir risco de regressao.
