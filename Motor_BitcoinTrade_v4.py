import pandas as pd
import re
from datetime import datetime

def clean_val(val_str):
    if pd.isna(val_str): return 0.0
    s = re.sub(r'[^\d,\.-]', '', str(val_str))
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try: return float(s)
    except: return 0.0

def processar_motor_portugal_v4(file_path):
    df = pd.read_csv(file_path, sep=';')
    df['Val_Numeric'] = df['Quantidade'].apply(clean_val)
    # Garante que a data seja lida corretamente
    df['Timestamp'] = pd.to_datetime(df['Data'] + ' ' + df['Hora'], dayfirst=True)
    df = df.sort_values(['Timestamp', 'Categoria'])

    inventory = {} 
    fiat_list = ['Real Brasileiro', 'BRL', 'Euro', 'EUR', 'US Dollar', 'USD']
    saidas_finais = []

    for ts, group in df.groupby('Timestamp'):
        data_atual = ts
        
        # 1. PROCESSAR ENTRADAS (Compras e Depósitos)
        entradas = group[(group['Val_Numeric'] > 0) & (~group['Moeda'].isin(fiat_list))]
        for _, row in entradas.iterrows():
            moeda = row['Moeda']
            qtd = abs(row['Val_Numeric'])
            
            # Identificar custo e se é origem externa
            custo_total = 0.0
            origem_ext = "Não"
            
            if "Depósito" in row['Categoria']:
                origem_ext = "Sim"
                custo_total = 0.0
            else:
                # Se for compra, busca o valor pago em FIAT no mesmo timestamp
                pago = group[(group['Val_Numeric'] < 0) & (group['Moeda'].isin(fiat_list))]
                custo_total = abs(pago['Val_Numeric'].sum()) if not pago.empty else 0.0

            if moeda not in inventory: inventory[moeda] = []
            inventory[moeda].append({
                'qtd': qtd, 
                'custo': custo_total, 
                'data_acq': data_atual, 
                'ext': origem_ext
            })

        # 2. PROCESSAR SAÍDAS (Vendas para FIAT)
        vendas = group[(group['Categoria'].str.contains('Venda', na=False))]
        for _, row_v in vendas.iterrows():
            moeda_v = row_v['Moeda']
            qtd_v = abs(row_v['Val_Numeric'])
            
            # Descobrir o valor recebido (Contraparte Fiat)
            recebido = group[(group['Val_Numeric'] > 0) & (group['Moeda'].isin(fiat_list))]
            valor_venda_total = recebido['Val_Numeric'].sum() if not recebido.empty else 0.0
            moeda_recebida = recebido['Moeda'].iloc[0] if not recebido.empty else "N/A"

            if moeda_v in inventory:
                restante = qtd_v
                while restante > 0 and inventory[moeda_v]:
                    lote = inventory[moeda_v][0]
                    qtd_a_retirar = min(lote['qtd'], restante)
                    
                    # Proporcionalidade do custo e do valor de venda
                    proporcao_lote = qtd_a_retirar / lote['qtd']
                    proporcao_venda = qtd_a_retirar / qtd_v
                    
                    custo_lote = lote['custo'] * proporcao_lote
                    valor_venda_lote = valor_venda_total * proporcao_venda
                    
                    # Cálculo de Dias e Isenção
                    dias = (data_atual - lote['data_acq']).days
                    if lote['ext'] == "Sim":
                        isento_status = "TBD"
                    else:
                        isento_status = f"{'SIM' if dias > 365 else 'NÃO'} ({dias} dias)"

                    saidas_finais.append({
                        'Data_Venda': data_atual.strftime('%Y-%m-%d'),
                        'Ativo': moeda_v,
                        'Moeda_Venda': moeda_recebida,
                        'Valor_Venda': round(valor_venda_lote, 2),
                        'Data_Aquisicao': lote['data_acq'].strftime('%Y-%m-%d'),
                        'Custo_Aquisicao_USD': round(custo_lote, 2),
                        'Origem_Externa': lote['ext'],
                        'Resultado': round(valor_venda_lote - custo_lote, 2),
                        'Isento_365d': isento_status
                    })

                    # Atualiza inventário
                    if lote['qtd'] <= restante:
                        restante -= lote['qtd']
                        inventory[moeda_v].pop(0)
                    else:
                        lote['qtd'] -= restante
                        lote['custo'] -= custo_lote
                        restante = 0

    return pd.DataFrame(saidas_finais)

# Execução e salvamento
# res = processar_motor_portugal_v4('BitcoinTrade_statement.csv')
# res.to_csv('Relatorio_IRS_Portugal.csv', index=False, sep=';')
