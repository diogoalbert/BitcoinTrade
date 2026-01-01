import pandas as pd
import re

def clean_val(val_str):
    if pd.isna(val_str): return 0.0
    s = re.sub(r'[^\d,\.-]', '', str(val_str))
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try: return float(s)
    except: return 0.0

def processar_relatorio_final_v3(file_path):
    print("Processando com colunas de contraparte...")
    df = pd.read_csv(file_path, sep=';')
    
    df['Val_Numeric'] = df['Quantidade'].apply(clean_val)
    df['Timestamp'] = pd.to_datetime(df['Data'] + ' ' + df['Hora'], dayfirst=True)
    df = df.sort_values(['Timestamp', 'Categoria'])

    inventory = {} 
    final_output = []
    
    for ts, group in df.groupby('Timestamp'):
        data_s = ts.strftime('%Y-%m-%d')
        hora_s = ts.strftime('%H:%M:%S')
        
        # --- 1. ENTRADAS FIAT ---
        depositos_brl = group[(group['Categoria'] == 'Depósito bancário') & (group['Moeda'] == 'Real Brasileiro')]
        for _, dep in depositos_brl.iterrows():
            final_output.append({
                'operação': 'Entrada Fiat', 'Data': data_s, 'hora': hora_s,
                'Moeda': 'BRL', 'quantidade': '', 
                'Valor (Custo FIFO)': abs(dep['Val_Numeric']), 
                'Ativo_Contraparte': 'Banco', 'Valor_Recebido_Contraparte': abs(dep['Val_Numeric']),
                'Fees': 0.0
            })

        # --- 2. DEPÓSITOS CRIPTO (Cust0 Zero) ---
        depositos_cripto = group[(group['Categoria'].str.contains('Depósito')) & (group['Moeda'] != 'Real Brasileiro')]
        for _, dep in depositos_cripto.iterrows():
            qtd = abs(dep['Val_Numeric'])
            if qtd > 0:
                final_output.append({
                    'operação': 'Depósito Cripto', 'Data': data_s, 'hora': hora_s,
                    'Moeda': dep['Moeda'], 'quantidade': qtd, 
                    'Valor (Custo FIFO)': 0.0, 
                    'Ativo_Contraparte': 'Carteira Externa', 'Valor_Recebido_Contraparte': qtd,
                    'Fees': 0.0
                })
                if dep['Moeda'] not in inventory: inventory[dep['Moeda']] = []
                inventory[dep['Moeda']].append({'qty': qtd, 'cost': 0.0})

        # --- 3. COMPRAS (BRL -> Cripto) ---
        compras_cripto = group[(group['Categoria'] == 'Compra') & (group['Moeda'] != 'Real Brasileiro')]
        if not compras_cripto.empty:
            brl_gasto_total = abs(group[(group['Categoria'] == 'Compra') & (group['Moeda'] == 'Real Brasileiro')]['Val_Numeric'].sum())
            fee_total = abs(group[group['Categoria'].str.contains('Taxa sobre compra')]['Val_Numeric'].sum())
            total_qtd = compras_cripto['Val_Numeric'].sum()
            
            if total_qtd > 0:
                for _, c in compras_cripto.iterrows():
                    prop = c['Val_Numeric'] / total_qtd
                    custo_lote = brl_gasto_total * prop
                    fee_lote = fee_total * prop
                    
                    final_output.append({
                        'operação': 'Compra', 'Data': data_s, 'hora': hora_s,
                        'Moeda': c['Moeda'], 'quantidade': c['Val_Numeric'],
                        'Valor (Custo FIFO)': round(custo_lote, 2), # O que pagou
                        'Ativo_Contraparte': 'BRL', 
                        'Valor_Recebido_Contraparte': round(custo_lote, 2), # Valor equivalente em BRL
                        'Fees': round(fee_lote, 8)
                    })
                    if c['Moeda'] not in inventory: inventory[c['Moeda']] = []
                    inventory[c['Moeda']].append({'qty': c['Val_Numeric'], 'cost': custo_lote})

        # --- 4. SAÍDAS (VENDA / RETIRADA / SWAP) ---
        saidas = group[
            (group['Categoria'].isin(['Retirada para carteira externa', 'Venda'])) & 
            (group['Moeda'] != 'Real Brasileiro')
        ]
        
        if not saidas.empty:
            fees_saida = abs(group[group['Categoria'].str.contains('Taxa')]['Val_Numeric'].sum())
            qtd_total_saida = abs(saidas['Val_Numeric'].sum())
            tipo_operacao = saidas['Categoria'].iloc[0]
            moeda_saida = saidas['Moeda'].iloc[0]

            # IDENTIFICAR CONTRAPARTE (O que entrou?)
            ativo_contra = ""
            valor_contra = 0.0
            
            if tipo_operacao == 'Retirada para carteira externa':
                ativo_contra = "Carteira Externa"
                valor_contra = 0.0 # Não houve recebimento financeiro, apenas transferência
            else: 
                # É VENDA ou SWAP
                # Verifica se entrou BRL (Venda simples)
                entrada_brl = group[(group['Moeda'] == 'Real Brasileiro') & (group['Val_Numeric'] > 0)]
                # Verifica se entrou outra Cripto (Swap)
                entrada_cripto = group[(group['Categoria'] == 'Compra') & (group['Moeda'] != 'Real Brasileiro') & (group['Moeda'] != moeda_saida)]
                
                if not entrada_brl.empty:
                    ativo_contra = "BRL"
                    valor_contra = entrada_brl['Val_Numeric'].sum()
                elif not entrada_cripto.empty:
                    ativo_contra = entrada_cripto['Moeda'].iloc[0] # Ex: ETH
                    valor_contra = entrada_cripto['Val_Numeric'].sum() # Ex: 1.5 ETH
                else:
                    ativo_contra = "Desconhecido"
            
            # CÁLCULO FIFO
            custo_herdado_total = 0.0
            qtd_restante = qtd_total_saida
            
            if moeda_saida in inventory:
                while qtd_restante > 1e-9 and inventory[moeda_saida]:
                    lote = inventory[moeda_saida][0]
                    if lote['qty'] <= qtd_restante:
                        custo_herdado_total += lote['cost']
                        qtd_restante -= lote['qty']
                        inventory[moeda_saida].pop(0)
                    else:
                        fracao = qtd_restante / lote['qty']
                        custo_parcial = lote['cost'] * fracao
                        custo_herdado_total += custo_parcial
                        lote['qty'] -= qtd_restante
                        lote['cost'] -= custo_parcial
                        qtd_restante = 0
            
            final_output.append({
                'operação': tipo_operacao,
                'Data': data_s, 'hora': hora_s,
                'Moeda': moeda_saida, 'quantidade': qtd_total_saida,
                'Valor (Custo FIFO)': round(custo_herdado_total, 2), # CUSTO DE AQUISIÇÃO
                'Ativo_Contraparte': ativo_contra,
                'Valor_Recebido_Contraparte': round(valor_contra, 8), # VALOR DE VENDA (Proceeds)
                'Fees': round(fees_saida, 8)
            })

    df_final = pd.DataFrame(final_output)
    arquivo = 'Relatorio_FIFO_Completo_Contraparte.csv'
    df_final.to_csv(arquivo, index=False, sep=';', decimal=',')
    print(f"Gerado: {arquivo}")
    print("Colunas chave: 'Valor (Custo FIFO)' vs 'Valor_Recebido_Contraparte'")

processar_relatorio_final_v3('BitcoinTrade_statement.csv')