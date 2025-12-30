import pandas as pd
# ... (funções de limpeza permanecem as mesmas)

def gerar_relatorios_finais_v4(file_path):
    # O motor processa todo o histórico...
    
    reconciliacao_custodia = [] # Novo foco do Arquivo 3

    for ts, group in df.groupby('Timestamp'):
        # 1. ENTRADAS EXTERNAS (Depósitos de Cripto)
        deps = group[(group['Categoria'].str.contains('Depósito')) & (group['Moeda'] != 'Real Brasileiro')]
        for _, d in deps.iterrows():
            reconciliacao_custodia.append({
                'Data': d['Data'],
                'Hora': d['Hora'],
                'Sentido': 'ENTRADA (Inbound)',
                'Moeda': d['Moeda'],
                'Quantidade': abs(d['Val_Numeric']),
                'Tipo': 'Depósito Externo',
                'Match_Pendente': 'Sim' # Para você marcar manualmente após cruzar com a Binance
            })

        # 2. SAÍDAS EXTERNAS (Retiradas de Cripto)
        rets = group[(group['Categoria'].str.contains('Retirada')) & (group['Moeda'] != 'Real Brasileiro')]
        for _, r in rets.iterrows():
            reconciliacao_custodia.append({
                'Data': r['Data'],
                'Hora': r['Hora'],
                'Sentido': 'SAÍDA (Outbound)',
                'Moeda': r['Moeda'],
                'Quantidade': abs(r['Val_Numeric']),
                'Tipo': 'Retirada para Carteira',
                'Match_Pendente': 'Sim'
            })

    # Exportação dos arquivos atualizados
    pd.DataFrame(reconciliacao_custodia).to_csv('Relatorio_3_Reconciliacao.csv', index=False, sep=';', decimal=',')
    # ... (os outros dois arquivos seguem a lógica anterior)
