# UI - Passo a Passo

Guia rapido para usar e manter a interface em `CustomTkinter`.

## 1) Como abrir a UI

```bash
python run_ui.py
```

## 2) Arquitetura da UI

- `src/ui/app.py`: composicao geral, navegacao entre telas e integracao com controllers.
- `src/ui/views/`: telas (somente layout/comportamento visual).
- `src/ui/controllers/`: validacao e regras de execucao por tela.
- `src/ui/components/file_selector_row.py`: linha reutilizavel de selecao de arquivo/pasta.
- `src/ui/services/pipeline_runner.py`: execucao assincroma de pipelines para nao travar a UI.

## 3) Mapa de telas do menu

- `Modo Ambos Complicacao e Internacao`
- `Modo Complicacao`
- `Modo Internacao`
- `Concatenar Arquivos`
- `Uniao de Status e Flow de Respostas`
- `Execucao em Partes`
- `Limpeza de Dados` (placeholder)
- `Utilitario` (placeholder)
- `Configuracoes` (placeholder)

## 4) Fluxo rapido por tela

### 4.1 Modo Ambos

1. Preencha obrigatorios: Complicacao, Internacao, Status.
2. Flows: ou informa os 3 arquivos de flow, ou nao informa nenhum.
3. Clique `Executar ETL`.
4. Se executar sem flows, aparece aviso de confirmacao sobre perda de respostas.

### 4.2 Modo Complicacao

1. Obrigatorios: Base Complicacao + Status.
2. Flow de resposta de complicacao e opcional.
3. Sem flow, roda em `somente_status` (com aviso de confirmacao).

### 4.3 Modo Internacao

1. Obrigatorios: Base Internacao + Status.
2. Flows Eletivo e Urgencia: ou os 2 juntos, ou nenhum.
3. Sem flows, roda em `somente_status` (com aviso de confirmacao).

### 4.4 Concatenar Arquivos

Modos:
- `Status`
- `Status Resposta`
- `Livre`

Passos:
1. Selecione os dois arquivos de entrada.
2. Em `Salvar`, selecione uma pasta (nao arquivo).
3. A UI monta o nome padrao automaticamente.
4. Clique em executar.

### 4.5 Uniao de Status e Flow de Respostas

Modos:
- `Complicacao`
- `Internacao`

Passos:
1. Selecione `Status Limpo` + `Flow Limpo`.
2. Em `Salvar arquivo unificado`, escolha pasta.
3. Execute a uniao.

### 4.6 Execucao em Partes

Contextos:
- `Complicacao`
- `Internacao`

Etapas:
1. `Ingestao`
2. `Uniao de Status`
3. `Criar Dataset`
4. `Enviar Status`
5. `Orquestrar`

Regra:
- Cada etapa valida campos obrigatorios antes de executar.
- Campos `Salvar ...` usam selecao de pasta e nome de arquivo padrao.

### 4.7 Ingestao (Tela dedicada)

Contextos:
- `Complicacao`
- `Internacao`

Comportamento:
- Permite executar somente status, somente status_resposta, ou ambos.
- Se informou entrada de status, precisa de saida de status.
- Se informou entrada de status_resposta, precisa de saida de status_resposta.

## 5) Modal de progresso

- Usado para execucoes ETL principais.
- Mostra:
  - barra de progresso
  - percentual
  - etapa atual
  - botao cancelar

## 6) Status da refatoracao (UI)

Ja aplicado:
- Separacao de `views` por tela.
- Separacao de `controllers` por dominio de tela.
- Reuso de componente de linha de arquivo.
- Fluxos de `Execucao em Partes` e `Ingestao` com controllers dedicados.

Pendencias recomendadas (proxima fase):
- Extrair estilos repetidos de botoes/cards para factories.
- Padronizar textos e encoding (evitar caracteres quebrados).
- Adicionar testes de controller (validacao de entrada e plano de execucao).

## 7) Convencoes atuais importantes

- `Selecionar` para entrada abre arquivo (`askopenfilename`).
- `Salvar` abre pasta (`askdirectory`) e gera nome final padrao.
- Mensagens de erro/sucesso ficam no rodape do card da etapa/tela.
