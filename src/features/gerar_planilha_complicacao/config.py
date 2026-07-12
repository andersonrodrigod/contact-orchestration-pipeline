COLUNAS_PRINCIPAIS = [
    "COD FILIAL",  # A
    "FILIAL",  # B
    "BASE",  # C
    "SIGLA",  # D
    "ESTADO",  # E
    "SENHA",  # F
    "COMPLICACAO",  # G
    "OBITO",  # H
    "COD USUARIO",  # I
    "USUARIO",  # J
    "TELEFONE OPERACIONAL",  # K
    "IDADE",  # L
    "IDADE T",  # M
    "EMPRESA",  # N
    "COD PLANO",  # O
    "PLANO",  # P
    "DT ADESAO",  # Q
    "TEMPO PLANO",  # R
    "DIAS CARENCIA",  # S
    "TP ATENDIMENTO",  # T
    "TRATAMENTO",  # U
    "PRESTADOR",  # V
    "SOLICITANTE",  # W
    "COD PROCEDIMENTO",  # X
    "PROCEDIMENTO",  # Y
    "DT AUTORIZACAO",  # Z
    "DT INTERNACAO",  # AA
    "DT ENVIO",  # AB
    "DIARIAS",  # AC
    "UTI",  # AD
    "OBSERVACAO",  # AE
    "CHAVE",  # AF
    "OPERADOR",  # AG
    "CONTATO",  # AH
    "DT ENVIO MANUAL",  # AI
    "DATA DO CONTATO",  # AJ
    "LIDA",  # AK
    "RESPOSTA",  # AL
    "STATUS",  # AM
    "DATA DE ENVIO",  # AN
    "P1",  # AO
    "P2",  # AP
    "P3",  # AQ
    "P4",  # AR
    "OBSERVACAO DO CLIENTE",  # AS
    "OPERADOR RP1",  # AT
    "CONTATO RP1",  # AU
    "DATA CONTATO RP1",  # AV
    "OBSERVAÇÕES DO CLIENTE RP1",  # AW
    "RP1 Nº",  # AX
    "RP1",  # AY
    "LIGACAO EFETIVADA",  # AZ
    "ESPECIALISTA",  # BA
    "TIPO",  # BB
    "UF",  # BC
    "DISTRITO",  # BD
    "TELEFONE 1",  # BE
    "TELEFONE 2",  # BF
    "TELEFONE 3",  # BG
    "TELEFONE 4",  # BH
    "TELEFONE 5",  # BI
    "CD_PESSOA",  # BJ
    "DUPLICADO",  # BK
    "STATUS ENVIADO"  # BL
]

COLUNAS_PESQUISA = [
    "Data",  # A
    "Nome",  # B
    "Telefone",  # C
    "CPF/CNPJ",  # D
    "Resposta",  # E
    "Opção",  # F
    "Protocolo",  # G
    "Cod.",  # H
    "Número externo",  # I
    "Agente",  # J
    "Canal",  # K
    "Conta",  # L
    "Serviço",  # M
    "UF",  # N
    "Classificação",  # O
    "Entrada",  # P
    "Classificação de IA",  # Q
    "SENHA",  # R
]

ABAS_AJUSTADAS = {
    "STATUS": [
        "Conta",  # A
        "HSM",  # B
        "Mensagem",  # C
        "Categoria",  # D
        "Template",  # E
        "Data do envio",  # F
        "Status",  # G
        "Respondido",  # H
        "protocolo",  # I
        "Agendamento",  # J
        "Data agendamento",  # K
        "Status agendamento",  # L
        "Campanha",  # M
        "Agente",  # N
        "Contato",  # O
        "Telefone",  # P
        "ID_Mailing",  # Q
        "DT ENVIO",  # R
        "RESPOSTA",  # S
        "NOME_MANIPULADO",  # T
        "SENHA"  # U
    ],
    "P1": COLUNAS_PESQUISA,
    "P2": COLUNAS_PESQUISA,
    "P3": COLUNAS_PESQUISA,
    "P4": COLUNAS_PESQUISA,
    "CHAVE ERRO": [
        "CHAVE ERRADA",  # A
        "P1",  # B
        "P2",  # C
        "P3",  # D
        "P4",  # E
        "CHAVE CERTA",  # F
    ],
    "RESUMO": [],
}

PALAVRAS_PRIORITARIAS = [
    # Prioridades originais
    "CESARIANA",
    "PARTO",
    # Primeiras palavras da coluna PROC_1 (fixas no codigo)
    "ABLACAO",
    "AMIGDALECTOMIA",
    "AMPUTACAO",
    "ANEURISMA",
    "ANGIOPLASTIA",
    "ANOMALIA",
    "APENDICECTOMIA",
    "ARTRITE",
    "ARTRODESE",
    "ARTROPLASTIA",
    "ARTROTOMIA",
    "BRONCOSCOPIA",
    "COLECISTECTOMIA",
    "COLECTOMIA",
    "COLPOPLASTIA",
    "DILATACAO",
    "ENXERTO",
    "EPISTAXE",
    "FACECTOMIA",
    "FISTULECTOMIA",
    "GASTROPLASTIA",
    "HERNIA",
    "HERNIORRAFIA",
    "HISTERECTOMIA",
    "HISTEROSCOPIA",
    "LAPAROTOMIA",
    "LIPOASPIRACAO",
    "MAMOPLASTIA",
    "ORQUIDOPEXIA",
    "OSTEOPLASTIAS",
    "PTERIGIO",
    "QUADRANTECTOMIA",
    "QUIMIOEMBOLIZACAO",
    "RECONSTRUCAO",
    "RUPTURA",
    "SEPTOPLASTIA",
    "TROCA",
    "URETERORRENOLITOTRIPSIA",
]
