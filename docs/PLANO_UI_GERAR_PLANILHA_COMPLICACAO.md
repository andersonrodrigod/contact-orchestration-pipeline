# Plano - UI Gerar Planilha Complicacao

## Objetivo

Adicionar ao `run_ui.py` um novo frame para executar o fluxo de
`scripts/gerar_planilha_complicacao.py` pela interface grafica.

Esse frame sera separado do frame atual de disparo. A ideia e manter duas etapas
claras:

- **Gerar Planilha Complicacao**: prepara a planilha base `complicacao.xlsx`.
- **Gerar Disparo Complicacao**: usa os arquivos de status/resposta/dataset para
  gerar o disparo final.

## Tela Nova

Nome sugerido do botao no menu:

```text
Gerar Planilha Complicacao
```

Campos esperados no frame:

- Planilha complicacao base (`.xlsx`, `.xls`)
- Telefones CSV (`.csv`)
- Utilidade (`.xlsx`, `.xls`)
- Pasta de saida

Arquivos gerados:

- `complicacao.xlsx`

## Arquitetura Proposta

Criar os arquivos:

- `src/ui/views/gerar_planilha_complicacao_view.py`
- `src/ui/controllers/gerar_planilha_complicacao_controller.py`

Alterar:

- `src/ui/app.py`
- `src/ui/views/menu_view.py`

Fluxo de execucao:

1. Usuario seleciona os tres arquivos e a pasta de saida.
2. Controller valida existencia, extensao e campos obrigatorios.
3. App abre modal de progresso.
4. Worker em thread chama:

```python
executar_pipeline(
    arquivo=arquivo_base,
    arquivo_telefones=arquivo_telefones,
    arquivo_utilidade=arquivo_utilidade,
    arquivo_saida=pasta_saida / "complicacao.xlsx",
)
```

5. UI exibe sucesso ou erro resumido.

## Tratamento de Erros

Validacoes iniciais recomendadas:

- Arquivo base foi selecionado e existe.
- Arquivo base tem extensao Excel.
- CSV de telefones foi selecionado e existe.
- CSV de telefones tem extensao `.csv`.
- Arquivo de utilidade foi selecionado e existe.
- Utilidade tem extensao Excel.
- Pasta de saida foi selecionada.
- Pasta de saida pode ser criada ou escrita.

Validacoes de conteudo recomendadas:

- Base contem pelo menos as colunas realmente essenciais para o processamento.
- CSV de telefones contem as colunas obrigatorias.
- Utilidade contem abas e colunas esperadas.

## Comportamento Atual das Colunas

Hoje o pipeline ja cria automaticamente as colunas listadas em
`COLUNAS_PRINCIPAIS` quando elas nao existem na base:

```python
for coluna in COLUNAS_PRINCIPAIS:
    if coluna not in df.columns:
        df[coluna] = ""
```

Isso evita erro para varias colunas de saida, mas nao e uma validacao amigavel.

Pontos importantes:

- `COD USUARIO` e usado antes desse preenchimento, logo se faltar na base o
  codigo tende a quebrar com erro tecnico.
- `PROCEDIMENTO`, `PRESTADOR`, `IDADE`, `DT INTERNACAO`, `SENHA` e outras
  colunas principais sao criadas vazias se faltarem, entao o fluxo pode rodar,
  mas pode gerar uma planilha pobre ou incorreta.
- O CSV de telefones ja tem uma checagem de colunas obrigatorias e retorna
  resumo quando faltam colunas, sem quebrar a execucao.
- A utilidade ainda pode quebrar com erro tecnico se faltar uma aba ou coluna
  esperada.

## Melhorias Necessarias Antes de Expor na UI

Para a UI ficar segura, vale acrescentar uma camada de validacao antes de chamar
o pipeline:

- Validar colunas minimas da base e avisar quais faltam.
- Separar colunas obrigatorias de colunas opcionais/autopreenchidas.
- Validar abas/colunas da utilidade antes de processar.
- Transformar erros tecnicos comuns em mensagens claras para o usuario.

Mensagem desejada quando faltar coluna:

```text
A planilha base nao tem as colunas obrigatorias:
COD USUARIO, PROCEDIMENTO

Revise o arquivo selecionado e tente novamente.
```

## Decisao Recomendada

Antes de implementar o frame, usar a funcao de validacao criada no pacote
`src/features/gerar_planilha_complicacao`:

```python
validar_colunas_base(df)
```

Essa funcao retorna um dicionario padronizado:

```python
{
    "ok": True,
    "mensagens": [
        "A planilha base nao tem algumas colunas padrao. Elas serao criadas vazias: ...",
    ],
}
```

ou:

```python
{
    "ok": False,
    "mensagens": [
        "A planilha base nao tem a coluna obrigatoria COD USUARIO.",
    ],
}
```

Assim o controller da UI consegue mostrar o erro antes de iniciar a execucao.

Status atual:

- Falta de `COD USUARIO` bloqueia a execucao com mensagem amigavel.
- Falta de outras colunas de `COLUNAS_PRINCIPAIS` gera aviso, mas a execucao
  continua criando essas colunas vazias.
