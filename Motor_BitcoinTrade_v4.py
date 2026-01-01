import pandas as pd
import re
import os

def clean_val(val_str):
    if pd.isna(val_str): return 0.0
    s = re.sub(r'[^\d,\.-]', '', str(val_str))
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try: return float(s)
    except: return 0.0

def processar_motor_final_triplo(file_path):
    if not os.path.exists(file_path):
        print(f"Erro: Arquivo {file_path} não encontrado!")
        return

    df = pd.read_csv(file_path, sep=';')
    df['Val_Numeric'] = df['Quantidade'].apply(clean_val)
    df['Timestamp'] = pd.to_datetime(df['Data'] + ' ' + df['Hora'], dayfirst=True)
    df = df.sort_values(['Timestamp', 'Categoria'])

    inventory = {} 
    fiat_list = ['Real Brasileiro', 'BRL', 'Euro', 'EUR', 'US Dollar', 'USD']
    
    log_irs = []    # Arquivo 1
    log_swaps = []  # Arquivo 2
    log_recon = []  # Arquivo 3

    for ts, group in df.groupby('Timestamp'):
        data_atual = ts
        
        # --- 1. ENTRADAS (Build Inventory) ---
        entradas = group[(group['Val_Numeric'] > 0) & (~group['Moeda'].isin(fiat_list))]
        for _, row in entradas.iterrows():
            moeda = row['Moeda']
            qtd = abs(row['Val_Numeric'])
            custo_total = 0.0
            origem_ext = "Não"
            
            if "Depósito" in row['Categoria']:
                origem_ext = "Sim"
                custo_total = 0.0
                log_recon.append({'Data': data_atual.strftime('%Y-%m-%d'), 'Moeda': moeda, 'Qtd': qtd, 'Tipo': 'Depósito', 'Status': 'Origem Externa (Custo 0)'})
            else:
                pago = group[(group['Val_Numeric'] < 0) & (group['Moeda'].isin(fiat_list) | (group['Moeda'] != moeda))]
                custo_total = abs(pago['Val_Numeric'].sum()) if not pago.empty else 0.0

            if moeda not in inventory: inventory[moeda] = []
            inventory[moeda].append({'qtd': qtd, 'custo': custo_total, 'data_acq': data_atual, 'ext': origem_ext})

        # --- 2. SAÍDAS (Consume Inventory) ---
        saidas = group[group['Categoria'].str.contains('Venda|Retirada', na=False)]
        for _, row_s in saidas.iterrows():
            moeda_v = row_s['Moeda']
            qtd_v = abs(row_s['Val_Numeric'])
            if moeda_v in fiat_list: continue

            recebido = group[(group['Val_Numeric'] > 0) & (group['Moeda'] != moeda_v)]
            moeda_recebida = recebido['Moeda'].iloc[0] if not recebido.empty else "Carteira Externa"
            valor_recebido_total = recebido['Val_Numeric'].sum() if not recebido.empty else 0.0

            if moeda_v in inventory:
                restante = qtd_v
                while restante > 1e-9 and inventory[moeda_v]:
                    lote = inventory[moeda_v][0]
                    qtd_a_retirar = min(lote['qtd'], restante)
                    
                    prop_lote = qtd_a_retirar / lote['qtd']
                    prop_venda = qtd_a_retirar / qtd_v if qtd_v > 0 else 0
                    
                    custo_lote = lote['custo'] * prop_lote
                    valor_venda_lote = valor_recebido_total * prop_venda
                    
                    dias = (data_atual - lote['data_acq']).days
                    isento_status = "TBD" if lote['ext'] == "Sim" else f"{'SIM' if dias > 365 else 'NÃO'} ({dias} dias)"

                    linha = {
                        'Data_Venda': data_atual.strftime('%Y-%m-%d'),
                        'Ativo': moeda_v,
                        'Moeda_Venda': moeda_recebida,
                        'Valor_Venda': round(valor_venda_lote, 2),
                        'Data_Aquisicao': lote['data_acq'].strftime('%Y-%m-%d'),
                        'Custo_Aquisicao_USD': round(custo_lote, 2),
                        'Origem_Externa': lote['ext'],
                        'Resultado': round(valor_venda_lote - custo_lote, 2),
                        'Isento_365d': isento_status
                    }

                    # Distribuição dos Relatórios
                    if "Retirada" in row_s['Categoria']:
                        log_recon.append({'Data': data_atual.strftime('%Y-%m-%d'), 'Moeda': moeda_v, 'Qtd': qtd_v, 'Tipo': 'Retirada', 'Status': 'Saída para Externa'})
                    elif moeda_recebida in fiat_list:
                        log_irs.append(linha)
                    else:
                        log_swaps.append(linha)

                    if lote['qtd'] <= restante:
                        restante -= lote['qtd']
                        inventory[moeda_v].pop(0)
                    else:
                        lote['qtd'] -= restante
                        lote['custo'] -= custo_lote
                        restante = 0

    # Salvar os 3 Arquivos
    pd.DataFrame(log_irs).to_csv('Arquivo1_IRS.csv', index=False, sep=';', encoding='utf-8-sig')
    pd.DataFrame(log_swaps).to_csv('Arquivo2_Swaps.csv', index=False, sep=';', encoding='utf-8-sig')
    pd.DataFrame(log_recon).to_csv('Arquivo3_Reconciliacao.csv', index=False, sep=';', encoding='utf-8-sig')
    
    print("✓ Sucesso! Gerados: Arquivo1_IRS.csv, Arquivo2_Swaps.csv e Arquivo3_Reconciliacao.csv")

# Executar
processar_motor_final_triplo('BitcoinTrade_statement.csv')