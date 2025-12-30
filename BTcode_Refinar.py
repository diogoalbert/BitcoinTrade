import pandas as pd
import re

def clean_val(val_str):
    if pd.isna(val_str): return 0.0
    # Passo 1: Remover tudo que NÃO for número, vírgula, ponto ou sinal de menos
    # Isso elimina "R$", "BTC", espaços ocultos, letras, etc.
    s = re.sub(r'[^\d,\.-]', '', str(val_str))
    
    # Passo 2: Ajustar pontuação brasileira (Ex: -1.500,50 vira -1500.50)
    # Se tiver vírgula e ponto, assumimos que ponto é milhar e removemos
    if ',' in s and '.' in s:
        s = s.replace('.', '') # Remove ponto de milhar
        s = s.replace(',', '.') # Transforma vírgula em ponto decimal
    elif ',' in s:
        s = s.replace(',', '.') # Transforma vírgula em ponto decimal
        
    try:
        return float(s)
    except:
        return 0.0

def processar_relatorio_final(file_path):
    # Carregar o arquivo
    print("Lendo arquivo...")
    df = pd.read_csv(file_path, sep=';')
    
    # Aplicar a limpeza rigorosa
    df['Val_Numeric'] = df['Quantidade'].apply(clean_val)
    
    # Criar Timestamp ordenável
    df['Timestamp'] = pd.to_datetime(df['Data'] + ' ' + df['Hora'], dayfirst=True)
    df = df.sort_values(['Timestamp', 'Categoria']) # Ordena para processar na sequência certa

    inventory = {} # Estoque FIFO: { 'BTC': [{'qty': 0.5, 'cost': 1000}, ...], 'ETH': ... }
    final_output = []
    
    # Agrupar por segundo para casar as operações
    for ts, group in df.groupby('Timestamp'):
        data_s = ts.strftime('%Y-%m-%d')
        hora_s = ts.strftime('%H:%M:%S')
        
        # 1. ENTRADA DE FIAT (Depósitos)
        depositos = group[group['Categoria'] == 'Depósito bancário']
        for _, dep in depositos.iterrows():
            final_output.append({
                'operação': 'Entrada', 
                'Data': data_s, 'hora': hora_s,
                'Moeda': 'BRL', 'quantidade': '', 
                'Valor': abs(dep['Val_Numeric']), 
                'Fees': '', 'Preço unitário': ''
            })

        # 2. COMPRA (Casar BRL gasto com Cripto recebida)
        compras_cripto = group[(group['Categoria'] == 'Compra') & (group['Moeda'] != 'Real Brasileiro')]
        if not compras_cripto.empty:
            # Captura o valor total em BRL gasto neste timestamp (soma dos valores negativos de BRL)
            brl_lines = group[(group['Categoria'] == 'Compra') & (group['Moeda'] == 'Real Brasileiro')]
            valor_brl_total = abs(brl_lines['Val_Numeric'].sum())
            
            # Captura taxas (Fees) pagas neste timestamp
            fee_lines = group[group['Categoria'].str.contains('Taxa sobre compra')]
            fee_total = abs(fee_lines['Val_Numeric'].sum())
            
            total_qtd_cripto = compras_cripto['Val_Numeric'].sum()
            
            # Se houver valor BRL detectado, processa
            if total_qtd_cripto > 0:
                for _, c in compras_cripto.iterrows():
                    # Regra de 3 para distribuir o custo se houver múltiplas linhas de cripto
                    prop = c['Val_Numeric'] / total_qtd_cripto
                    custo_real = valor_brl_total * prop
                    fee_real = fee_total * prop
                    preco_unit = custo_real / c['Val_Numeric'] if c['Val_Numeric'] > 0 else 0
                    
                    final_output.append({
                        'operação': 'Compra',
                        'Data': data_s, 'hora': hora_s,
                        'Moeda': c['Moeda'], 
                        'quantidade': c['Val_Numeric'],
                        'Valor': round(custo_real, 2), # Aqui está o Custo de Aquisição
                        'Fees': round(fee_real, 8),
                        'Preço unitário': round(preco_unit, 2)
                    })
                    
                    # Adiciona ao Estoque FIFO
                    if c['Moeda'] not in inventory: inventory[c['Moeda']] = []
                    inventory[c['Moeda']].append({'qty': c['Val_Numeric'], 'cost': custo_real})

        # 3. RETIRADA (Cálculo FIFO de quanto custou esse lote que está saindo)
        retiradas = group[group['Categoria'] == 'Retirada para carteira externa']
        if not retiradas.empty:
            # Soma taxas de mineração deste timestamp
            miner_fees = abs(group[group['Categoria'].str.contains('Taxa de mineração')]['Val_Numeric'].sum())
            
            for _, r in retiradas.iterrows():
                moeda = r['Moeda']
                qtd_saida = abs(r['Val_Numeric'])
                custo_herdado_total = 0.0
                
                # Algoritmo FIFO (Consumir lotes antigos)
                qtd_restante = qtd_saida
                if moeda in inventory:
                    while qtd_restante > 1e-9 and inventory[moeda]:
                        lote = inventory[moeda][0] # Pega o lote mais antigo
                        
                        if lote['qty'] <= qtd_restante:
                            # Consome o lote todo
                            custo_herdado_total += lote['cost']
                            qtd_restante -= lote['qty']
                            inventory[moeda].pop(0) # Remove lote vazio
                        else:
                            # Consome fração do lote
                            fracao = qtd_restante / lote['qty']
                            custo_parcial = lote['cost'] * fracao
                            custo_herdado_total += custo_parcial
                            
                            # Atualiza o lote remanescente
                            lote['qty'] -= qtd_restante
                            lote['cost'] -= custo_parcial
                            qtd_restante = 0
                
                final_output.append({
                    'operação': 'Retirada para carteira externa',
                    'Data': data_s, 'hora': hora_s,
                    'Moeda': moeda, 
                    'quantidade': qtd_saida,
                    'Valor': round(custo_herdado_total, 2), # Custo de Aquisição Herdado
                    'Fees': round(miner_fees, 8),
                    'Preço unitário': '' # Não aplicável em retirada (é média ponderada implícita)
                })

    # Salvar
    df_final = pd.DataFrame(final_output)
    output_file = 'Relatorio_Corrigido_Final.csv'
    df_final.to_csv(output_file, index=False, sep=';', decimal=',')
    print(f"Sucesso! Arquivo gerado: {output_file}")
    
    # Debug: Mostrar as primeiras linhas para confirmação
    print("\n--- Amostra das primeiras 5 linhas geradas ---")
    print(df_final.head().to_string())

# Executar
processar_relatorio_final('BitcoinTrade_statement.csv')