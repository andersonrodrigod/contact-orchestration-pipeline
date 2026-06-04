# Plano de refatoracao ponta a ponta - pipeline de complicacao

Este plano serve como trilha de trabalho para refatorar o projeto sem perder a ordem real de execucao. A ideia e mexer primeiro nos arquivos que reagem primeiro, criar protecao de testes em volta deles e so depois abrir os servicos mais pesados.

## Objetivo

- Manter o fluxo atual de complicacao funcionando.
- Reduzir acoplamento entre CLI, UI, pipelines e servicos.
- Separar leitura, normalizacao, integracao, dataset, analises, graficos e orquestracao em contratos mais claros.
- Facilitar testes por etapa, sem depender sempre de uma execucao completa.

## Mapa de reacao da execucao CLI

Comando principal esperado:

```powershell
python main.py --modo complicacao_com_resposta
```

Ordem em que os arquivos reagem:

1. `main.py`
   - Monta os modos disponiveis.
   - Recebe `--modo`.
   - Chama `run_pipeline(modo)`.
   - No fim, anexa codigo de erro, registra historico e imprime resumo.

2. `src/cli/modos_principais.py`
   - Define o mapa principal de comandos.
   - `complicacao_com_resposta` aponta para `run_pipeline_complicacao_com_resposta`.

3. `src/cli/modos_individuais.py`
   - Registra comandos menores de etapa, como ingestao, integracao, dataset e orquestracao.
   - E importante porque expande a superficie publica da CLI.

4. `src/pipelines/complicacao_pipeline.py`
   - Primeiro coordenador real do dominio.
   - Busca defaults em `CONTEXTO_PIPELINE_COMPLICACAO`.
   - Executa a etapa de status/dataset e depois a orquestracao.

5. `src/contexts/pipeline_contextos.py`
   - Guarda os caminhos padrao e nomes de loggers do contexto de complicacao.
   - Deve virar a fonte unica de configuracao do fluxo.

6. `src/pipelines/complicacao_status_pipeline.py`
   - Primeiro arquivo grande do fluxo de dados.
   - Orquestra ingestao, uniao status/resposta, dataset, resumo, tabelas, analises e graficos.

7. `src/services/ingestao_service.py`
   - Primeira leitura real dos arquivos.
   - Le `status.csv` e `status_resposta.csv`.
   - Valida colunas, padroniza nomes, normaliza tipos, limpa texto e salva arquivos limpos.

8. `src/utils/arquivos.py`
   - Faz a leitura e escrita fisica de CSV/XLSX.
   - Deve ser protegido cedo porque qualquer erro aqui afeta todas as etapas.

9. `src/services/validacao_service.py`
    - Confere contrato minimo das colunas de entrada.

10. `src/services/padronizacao_service.py`
    - Padroniza colunas de status e resposta.

11. `src/services/schema_resposta_service.py`
    - Garante o contrato canonico da resposta.

13. `src/services/normalizacao_services.py`
    - Normaliza datas, textos e campos criticos.

14. `src/pipelines/join_status_resposta_pipeline.py`
    - Une status com status_resposta.
    - Gera a base integrada `status_complicacao.csv`.

15. `src/services/integracao_service.py`
    - Contem a regra de integracao/match entre status e resposta.

16. `src/pipelines/contexto_status_pipeline_base.py`
    - Base generica para criar dataset com status.

17. `src/services/dataset_service.py`
    - Aplica status no dataset de complicacao.
    - Gera `complicacao_status.xlsx`.

18. `src/services/resumo_complicacao_service.py`
    - Gera resumo consolidado.

19. `src/services/tabela_resumo_complicacao_service.py`
    - Gera tabelas resumo por dia e filtros especificos.

20. `src/services/analise_dados_fase1_service.py`
    - Analise logo apos uniao de status/resposta.

21. `src/services/analise_dados_fase2_service.py`
    - Analise apos criacao do dataset com status.

22. `src/services/graficos_uniao_status_resposta_service.py`
    - Graficos da fase de uniao.

23. `src/services/graficos_status_enviado_service.py`
    - Graficos da fase de dataset/status enviado.

24. `src/pipelines/complicacao_orquestracao_pipeline.py`
    - Entra depois que o dataset com status esta pronto.

25. `src/pipelines/orquestracao_base_pipeline.py`
    - Base generica da orquestracao.

26. `src/services/orquestracao_service.py`
    - Aplica regra final de resolucao/orquestracao.

27. `src/services/analise_dados_fase3_orquestracao_service.py`
    - Analise apos orquestracao.

28. `src/services/graficos_orquestracao_service.py`
    - Graficos finais da orquestracao.

29. `src/services/observabilidade_service.py`
    - Registra historico da execucao.

30. `src/utils/resumo_execucao.py`
    - Imprime o resumo final no terminal.

## Mapa de reacao da UI

Comando principal esperado:

```powershell
python run_ui.py
```

Ordem inicial:

1. `run_ui.py`
   - Cria `App` e inicia `mainloop`.

2. `src/ui/app.py`
   - Monta a janela principal.
   - Controla selecao de arquivos, execucao em thread e exibicao de progresso.

3. `src/ui/views/menu_view.py`
   - Mostra as opcoes de fluxo.

4. `src/ui/views/complicacao_view.py`
   - Tela do fluxo de complicacao.

5. `src/ui/controllers/fluxo_partes_controller.py`
   - Controla execucao por partes quando o usuario nao roda o fluxo completo.

6. `src/ui/app.py`
   - Ao executar complicacao, chama `run_complicacao_pipeline_gerar_status_dataset`, `run_complicacao_pipeline_gerar_status_dataset_somente_status` ou `run_complicacao_pipeline_orquestrar`.

Depois desse ponto, a UI cai nos mesmos arquivos de pipeline e servicos usados pela CLI.

## Por onde comecar

Primeiro arquivo para entender a execucao:

- `main.py`

Primeiro arquivo para refatorar com impacto controlado:

- `src/pipelines/complicacao_pipeline.py`

Primeiro arquivo grande que precisa ser quebrado com cuidado:

- `src/pipelines/complicacao_status_pipeline.py`

Primeiro servico de negocio que merece isolamento por testes:

- `src/services/ingestao_service.py`

## Ordem recomendada de refatoracao

### Fase 0 - Protecao antes de mexer

Arquivos:

- `tests/`
- `requirements.txt`
- `docs/PLANO_REFATORACAO_PIPELINE_COMPLICACAO.md`

Tarefas:

- Instalar dependencias, incluindo `pytest`.
- Rodar a suite atual.
- Registrar quais arquivos de entrada sao fixtures confiaveis.
- Separar dados gerados de dados de teste.

Validacao:

```powershell
python -m pytest
python -m unittest discover -s tests -p "test*.py"
```

### Fase 1 - Entrada da aplicacao e registro de modos

Arquivos:

- `main.py`
- `src/cli/modos_principais.py`
- `src/cli/modos_individuais.py`
- `src/cli/acoes_app.py`

Tarefas:

- Criar uma funcao unica para montar registro de comandos.
- Evitar duplicidade entre modos principais e comandos de etapa.
- Garantir que cada modo retorne sempre o mesmo formato de resultado.
- Testar cada modo sem executar a pipeline completa, usando stubs.

Resultado esperado:

- CLI mais facil de testar.
- Menos importacao circular e menos decisao espalhada.

### Fase 2 - Contexto e configuracao

Arquivos:

- `src/contexts/pipeline_contextos.py`
- `src/config/paths.py`
- `src/config/governanca_config.py`

Tarefas:

- Centralizar todos os caminhos default de complicacao.
- Remover defaults duplicados dentro das funcoes de pipeline.
- Separar configuracao de caminho, regra de governanca e nome de logger.
- Criar testes para resolver defaults sem tocar no disco.

Resultado esperado:

- Uma unica fonte de verdade para entradas e saidas.

### Fase 3 - Coordenadores de pipeline

Arquivos:

- `src/pipelines/complicacao_pipeline.py`

Tarefas:

- Manter a sequencia status/dataset -> orquestracao no coordenador principal.
- Padronizar retorno de erro entre status/dataset e orquestracao.
- Evitar agregacao de metricas antigas no resultado final do fluxo completo.
- Testar a ordem de chamada sem ler arquivos reais.

Resultado esperado:

- O fluxo completo fica legivel em poucas linhas.

### Fase 4 - Quebrar `complicacao_status_pipeline.py`

Arquivo principal:

- `src/pipelines/complicacao_status_pipeline.py`

Novos alvos sugeridos:

- `src/pipelines/status_ingestao_pipeline.py`
- `src/pipelines/status_integracao_pipeline.py`
- `src/pipelines/status_dataset_pipeline.py`
- `src/pipelines/status_relatorios_pipeline.py`

Tarefas:

- Separar a etapa de ingestao da etapa de uniao.
- Separar criacao de dataset das analises/graficos.
- Manter funcoes publicas antigas como fachada temporaria.
- Escrever testes por etapa antes de remover a fachada.

Resultado esperado:

- Arquivo menor.
- Cada fase pode ser rodada e testada isoladamente.

### Fase 5 - Ingestao e normalizacao

Arquivos:

- `src/services/ingestao_service.py`
- `src/services/validacao_service.py`
- `src/services/padronizacao_service.py`
- `src/services/schema_resposta_service.py`
- `src/services/normalizacao_services.py`
- `src/utils/arquivos.py`

Tarefas:

- Separar leitura de arquivo da transformacao de DataFrame.
- Criar funcoes puras para normalizacao.
- Deixar salvamento como ultima camada.
- Testar casos de coluna faltante, data invalida, texto quebrado e resposta canonica.

Resultado esperado:

- A primeira fase real de dados passa a ser previsivel e facil de depurar.

### Fase 6 - Integracao status/resposta

Arquivos:

- `src/pipelines/join_status_resposta_pipeline.py`
- `src/services/integracao_service.py`

Tarefas:

- Documentar chave de match e prioridade das regras.
- Separar preparacao dos dados, match e metricas.
- Testar cenarios com match, sem match e duplicidade.

Resultado esperado:

- A uniao deixa de ser uma caixa preta.

### Fase 7 - Dataset com status

Arquivos:

- `src/pipelines/contexto_status_pipeline_base.py`
- `src/services/dataset_service.py`
- `src/services/dataset_metricas_service.py`

Tarefas:

- Separar leitura XLSX, aplicacao de status e escrita XLSX.
- Padronizar metricas de linhas alteradas, ignoradas e sem chave.
- Testar regra de aplicacao de status em DataFrame pequeno.

Resultado esperado:

- Criacao de `complicacao_status.xlsx` fica testavel sem arquivo grande.

### Fase 8 - Resumos, analises e graficos

Arquivos:

- `src/services/resumo_complicacao_service.py`
- `src/services/tabela_resumo_complicacao_service.py`
- `src/services/analise_dados_fase1_service.py`
- `src/services/analise_dados_fase2_service.py`
- `src/services/analise_dados_fase3_orquestracao_service.py`
- `src/services/graficos_uniao_status_resposta_service.py`
- `src/services/graficos_status_enviado_service.py`
- `src/services/graficos_orquestracao_service.py`

Tarefas:

- Separar calculo de metricas da gravacao de arquivos.
- Criar testes para agregacoes principais.
- Manter geracao de graficos como camada final.

Resultado esperado:

- Relatorios e graficos deixam de bloquear a refatoracao do core.

### Fase 8.1 - Mover criacao de DT ENVIO para fora da ingestao

Situacao atual:

- `DT ENVIO` ainda e criado dentro de `src/services/ingestao_service.py`;
- isso foi mantido temporariamente para nao quebrar os arquivos limpos e as etapas seguintes;
- a ingestao nao deveria criar coluna derivada de negocio, apenas validar, padronizar, converter tipos e limpar dados.

Decisao:

- nao remover agora;
- deixar explicito que essa coluna esta na ingestao apenas por compatibilidade;
- mover essa criacao para uma fase posterior quando soubermos exatamente qual etapa deve ser dona dela.

Tarefas futuras:

- mapear todos os arquivos que leem `DT ENVIO`;
- descobrir qual e a primeira etapa que realmente precisa dessa coluna;
- criar teste garantindo que `DT ENVIO` existe antes dessa etapa;
- mover `criar_coluna_dt_envio_por_data_agendamento` para o ponto correto;
- remover a criacao de `DT ENVIO` da ingestao.

Resultado esperado:

- ingestao fica limitada a limpeza e validacao estrutural;
- coluna derivada passa a nascer no lugar correto do pipeline.

### Fase 8.2 - Criar CLIs proprias para status e status_resposta

Situacao atual:

- as funcoes antigas de `somente_status` foram removidas da ingestao e dos modos principais;
- o fluxo oficial de complicacao fica, por enquanto, com `status + status_resposta`;
- a execucao isolada de `status` e `status_resposta` deve voltar como etapa propria, nao como uma variacao escondida dentro da ingestao.

Decisao:

- nao manter `executar_ingestao_somente_status` dentro de `ingestao_service.py`;
- criar comandos explicitos de CLI para cada entrada;
- deixar a CLI/pipeline decidir qual etapa rodar.

CLIs futuras sugeridas:

- `normalizar_status_complicacao`
- `normalizar_status_resposta_complicacao`
- `normalizar_status_e_status_resposta_complicacao`

Tarefas futuras:

- extrair uma funcao pequena para normalizar apenas `status`;
- extrair uma funcao pequena para normalizar apenas `status_resposta`;
- criar comandos no registro de CLI para cada etapa;
- adicionar testes unitarios para cada comando;
- reativar UI/fluxos parciais chamando essas CLIs/etapas, nao uma funcao `somente_status` dentro da ingestao.

Resultado esperado:

- ingestao fica reutilizavel;
- modo de execucao fica explicito na CLI;
- `status` e `status_resposta` podem ser rodados separadamente sem misturar regra de fluxo com limpeza.

### Fase 9 - Orquestracao final

Arquivos:

- `src/pipelines/complicacao_orquestracao_pipeline.py`
- `src/pipelines/orquestracao_base_pipeline.py`
- `src/services/orquestracao_service.py`

Tarefas:

- Definir contrato de entrada do dataset com status.
- Separar regra de resolucao da leitura/escrita de XLSX.
- Testar usuarios resolvidos, pendentes e sem dados suficientes.

Resultado esperado:

- Etapa final fica independente da preparacao do dataset.

### Fase 10 - UI desacoplada

Arquivos:

- `run_ui.py`
- `src/ui/app.py`
- `src/ui/views/*.py`
- `src/ui/controllers/*.py`

Tarefas:

- Criar uma camada de caso de uso que a UI chama, em vez de chamar pipelines diretamente.
- Padronizar mensagens de progresso.
- Remover regra de negocio de dentro da tela.
- Testar controller sem abrir janela.

Resultado esperado:

- UI vira interface, nao dona do fluxo.

### Fase 11 - Observabilidade e resultado padrao

Arquivos:

- `core/pipeline_result.py`
- `core/error_codes.py`
- `src/services/observabilidade_service.py`
- `src/utils/resumo_execucao.py`

Tarefas:

- Padronizar `ok`, `mensagens`, `metricas`, `arquivos` e `dados`.
- Garantir codigo de erro consistente.
- Registrar historico sem depender de detalhe interno da pipeline.

Resultado esperado:

- Erro, log, historico e resumo final passam a falar a mesma lingua.

## Sequencia de commits sugerida

1. `test: adicionar pytest e fixtures base`
2. `refactor: centralizar registro de modos`
3. `refactor: centralizar contexto de complicacao`
4. `refactor: simplificar coordenador da pipeline`
5. `refactor: separar ingestao de status e resposta`
6. `refactor: isolar integracao status resposta`
7. `refactor: isolar criacao de dataset com status`
8. `refactor: separar relatorios e graficos`
9. `refactor: isolar orquestracao final`
10. `refactor: desacoplar ui das pipelines`
11. `docs: atualizar mapa de execucao`

## Regra de ouro durante a refatoracao

Mexer em uma fase por vez. Para cada fase:

1. Criar ou ajustar teste da fase.
2. Refatorar mantendo a assinatura publica antiga.
3. Rodar testes.
4. So depois remover duplicidade ou fachada antiga.

## Checklist de validacao continua

- `python -m pytest`
- `python -m unittest discover -s tests -p "test*.py"`
- `python main.py --modo complicacao_com_resposta`
- `python main.py --modo complicacao_ingestao`
- `python main.py --modo complicacao_integrar_status_resposta`
- `python main.py --modo complicacao_criar_dataset_status`
- `python main.py --modo complicacao_orquestracao`

## Decisao pratica para o proximo passo

Comecar pela Fase 1, mas sem abrir ainda os servicos grandes. O primeiro PR de refatoracao deve apenas organizar `main.py`, `src/cli/modos_principais.py` e `src/cli/modos_individuais.py`, com testes garantindo que os comandos continuam apontando para as mesmas funcoes.
