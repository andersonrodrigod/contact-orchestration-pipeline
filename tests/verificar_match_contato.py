import pandas as pd
import unicodedata


def normalizar_texto(valor):
    texto = str(valor).strip().lower()
    texto = unicodedata.normalize('NFKD', texto)
    return ''.join(ch for ch in texto if not unicodedata.combining(ch))


def executar_verificacao_match_contato():
    arquivo_status = 'src/data/arquivo_limpo/status_limpo.csv'
    arquivo_resposta = 'src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv'

    df_status = pd.read_csv(arquivo_status, sep=';', dtype=str, encoding='utf-8-sig', keep_default_na=False)
    df_resposta = pd.read_csv(arquivo_resposta, sep=';', dtype=str, encoding='utf-8-sig', keep_default_na=False)

    df_status['Contato'] = df_status['Contato'].astype(str).str.strip()
    df_resposta['nom_contato'] = df_resposta['nom_contato'].astype(str).str.strip()

    hsm_normalizado = df_status['HSM'].astype(str).apply(normalizar_texto)
    filtro_hsm = hsm_normalizado == normalizar_texto('Pesquisa Complicacoes Cirurgicas')
    df_status_filtrado = df_status[filtro_hsm].copy()

    contatos_resposta = set(df_resposta['nom_contato'].tolist())
    tem_match = df_status_filtrado['Contato'].isin(contatos_resposta)

    total = len(df_status_filtrado)
    total_match = int(tem_match.sum())
    total_sem_match = int((~tem_match).sum())

    print('=== VERIFICACAO MATCH CONTATO ===')
    print('HSM filtrado: Pesquisa Complicacoes Cirurgicas')
    print(f'Total de registros no status (filtrado): {total}')
    print(f'Quantidade COM match entre Contato e nom_contato: {total_match}')
    print(f'Quantidade SEM match entre Contato e nom_contato: {total_sem_match}')

    

if __name__ == '__main__':
    executar_verificacao_match_contato()
