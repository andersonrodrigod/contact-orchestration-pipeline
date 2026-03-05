from core.pipeline_result import PipelineResult


def build_preflight_result(
    ok,
    contexto,
    bloqueios=None,
    avisos=None,
    metricas=None,
    detalhes=None,
):
    bloqueios = bloqueios or []
    avisos = avisos or []
    metricas = metricas or {}
    detalhes = detalhes or {}

    mensagens = [
        f'Preflight contexto={contexto}',
        f'bloqueios={len(bloqueios)}',
        f'avisos={len(avisos)}',
    ]
    mensagens.extend([f'BLOQUEIO: {b}' for b in bloqueios])
    mensagens.extend([f'AVISO: {a}' for a in avisos])

    return PipelineResult(
        ok=ok,
        mensagens=mensagens,
        metricas=metricas,
        dados={
            'contexto': contexto,
            'bloqueios': bloqueios,
            'avisos': avisos,
            'detalhes': detalhes,
        },
    ).to_dict()
