ERRO_DESCONHECIDO = 'E999'
ERRO_MODO_BLOQUEADO = 'E001'
ERRO_VALIDACAO_ARQUIVOS = 'E101'
ERRO_VALIDACAO_COLUNAS = 'E102'
ERRO_QUALIDADE_DATA = 'E201'
ERRO_INGESTAO = 'E301'
ERRO_INTEGRACAO = 'E302'
ERRO_CONCATENACAO = 'E303'
ERRO_CRIACAO_DATASET = 'E401'
ERRO_ORQUESTRACAO = 'E501'
ERRO_XLSX_CONCORRENCIA = 'E701'


def inferir_codigo_erro_por_mensagens(mensagens):
    if not mensagens:
        return ERRO_DESCONHECIDO

    texto = ' '.join(str(m).lower() for m in mensagens)

    if 'badzipfile' in texto or 'bad crc-32' in texto:
        return ERRO_XLSX_CONCORRENCIA
    if 'desabilitado' in texto and 'modo' in texto:
        return ERRO_MODO_BLOQUEADO
    if 'validacao de colunas' in texto or 'coluna' in texto and 'obrigatoria' in texto:
        return ERRO_VALIDACAO_COLUNAS
    if 'arquivo' in texto and ('nao encontrado' in texto or 'faltando' in texto):
        return ERRO_VALIDACAO_ARQUIVOS
    if 'qualidade de data' in texto or 'nat' in texto:
        return ERRO_QUALIDADE_DATA
    if 'concatenacao' in texto:
        return ERRO_CONCATENACAO
    if 'integracao' in texto:
        return ERRO_INTEGRACAO
    if 'dataset' in texto:
        return ERRO_CRIACAO_DATASET
    if 'orquestracao' in texto:
        return ERRO_ORQUESTRACAO
    if 'ingestao' in texto:
        return ERRO_INGESTAO
    return ERRO_DESCONHECIDO


def anexar_codigo_erro(resultado):
    if not isinstance(resultado, dict):
        return resultado
    if resultado.get('ok'):
        return resultado
    if resultado.get('codigo_erro'):
        return resultado
    mensagens = resultado.get('mensagens', [])
    resultado['codigo_erro'] = inferir_codigo_erro_por_mensagens(mensagens)
    return resultado
