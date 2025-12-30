import pandas as pd
import re

# 1. Configuração e Limpeza de Dados
def clean_val(val_str):
    if pd.isna(val_str): return 0.0
    # Remove siglas de moedas e símbolos, ajusta pontuação brasileira para decimal
    s = re.sub(r'[A-Z\$]', '', str(val_str)).strip()
    if not s: return 0.0
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except:
        return 0.0

def processar_fifo(file_path):
    # Carregar o CSV original
    df = pd.read_csv(file_path, sep=';')
    df['Val_Numeric'] = df['Quantidade'].apply(clean_val)
    df['Timestamp'] = pd.to_datetime(df['Data'] + ' ' + df['Hora'], dayfirst=True)
    df = df.sort_values('Timestamp')

    inventory = {} # Dicionário para gerir lotes por moeda
    results = []

    for _, row in df.iterrows():
        moeda = row['Moeda']
        cat = row['Categoria']
        val = row['Val_Numeric']
        
        # Ignorar linhas de saldo em Real (apenas rastrear o gasto na Compra)
        if moeda == 'Real Brasileiro' and cat != 'Venda':
            continue

        evento = {
            'Data': row['Data'],
            'Hora': row['Hora'],
            'Moeda': moeda,
            'Categoria': cat,
            'Quantidade': val,
            'Custo_Base_BRL': 0.0,
            'Lotes_Origem': ""
        }

        # EVENTO: COMPRA (Geração de Lote)
        if cat == 'Compra' and moeda != 'Real Brasileiro':
            # Localizar o gasto em BRL no mesmo timestamp
            banco_brl = df[(df['Timestamp'] == row['Timestamp']) & 
                           (df['Moeda'] == 'Real Brasileiro') & 
                           (df['Categoria'] == 'Compra')]
            
            total_cripto_timestamp = df[(df['Timestamp'] == row['Timestamp']) & 
                                        (df['Moeda'] == moeda) & 
                                        (df['Categoria'] == 'Compra')]['Val_Numeric'].sum()
            total_brl_timestamp = abs(banco_brl['Val_Numeric'].sum())
            
            # Cálculo do custo proporcional deste lote específico
            custo_lote = (val / total_cripto_timestamp) * total_brl_timestamp if total_cripto_timestamp > 0 else 0
            
            if moeda not in inventory: inventory[moeda] = []
            inventory[moeda].append({'qty': val, 'cost': custo_lote, 'date': row['Data'], 'hora': row['Hora']})
            
            evento['Custo_Base_BRL'] = custo_lote
            evento['Lotes_Origem'] = "AQUISIÇÃO"

        # EVENTO: RETIRADA OU VENDA (Consumo de Lote FIFO)
        elif cat in ['Retirada para carteira externa', 'Venda']:
            qty_to_consume = abs(val)
            total_cost_inherited = 0.0
            lotes_detalhe = []
            
            if moeda in inventory and inventory[moeda]:
                while qty_to_consume > 1e-9 and inventory[moeda]:
                    lote = inventory[moeda][0]
                    if lote['qty'] <= qty_to_consume:
                        # Consome lote inteiro
                        consumed_qty = lote['qty']
                        total_cost_inherited += lote['cost']
                        lotes_detalhe.append(f"{consumed_qty:.8f} de {lote['date']} {lote['hora']}")
                        qty_to_consume -= consumed_qty
                        inventory[moeda].pop(0)
                    else:
                        # Consome fração do lote
                        fraction = qty_to_consume / lote['qty']
                        cost_part = lote['cost'] * fraction
                        total_cost_inherited += cost_part
                        lotes_detalhe.append(f"{qty_to_consume:.8f} de {lote['date']} {lote['hora']}")
                        lote['qty'] -= qty_to_consume
                        lote['cost'] -= cost_part
                        qty_to_consume = 0
                
                evento['Custo_Base_BRL'] = total_cost_inherited
                evento['Lotes_Origem'] = " | ".join(lotes_detalhe)
            else:
                evento['Lotes_Origem'] = "ERRO: Lote não encontrado (verificar histórico)"

        results.append(evento)

    # Gerar DataFrame final e exportar
    df_final = pd.DataFrame(results)
    df_final.to_csv('Relatorio_FIFO_Portugal.csv', index=False, sep=';', encoding='utf-8-sig')
    print("Relatório gerado com sucesso: Relatorio_FIFO_Portugal.csv")

# Executar
processar_fifo('BitcoinTrade_statement.csv')
