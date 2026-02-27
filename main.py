








def processo_0_ajustar_normalizar_status():
    pass


def processo_1_criar_pipeline_planilha():
    pass


def processo_2_enviar_status_orquestra():
    pass


def processo_3_orquestrar_usuarios():
    pass

def processo_6_merge_telefones_data_sql():
    pass


def executar_camada_1():
    processo_0_ajustar_normalizar_status()


def executar_camada_2(executar_6=False):
    if executar_6:
        processo_6_merge_telefones_data_sql()
    processo_1_criar_pipeline_planilha()


def executar_camada_3():
    processo_2_enviar_status_orquestra()


def executar_camada_4():
    processo_3_orquestrar_usuarios()


def executar_pipeline_completo(executar_6=False):
    executar_camada_1()
    executar_camada_2(executar_6=executar_6)
    executar_camada_3()
    executar_camada_4()


if __name__ == "__main__":
    executar_pipeline_completo()
