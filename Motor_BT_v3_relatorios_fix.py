import pandas as pd
import re
from dataclasses import dataclass
from typing import Dict, List, Any, Optional, Tuple

FIAT = {"BRL", "Real Brasileiro", "EUR", "Euro", "USD"}

def clean_val(val_str):
    """
    Normaliza valores numéricos vindos de CSVs brasileiros/portugueses:
    - Remove símbolos e espaços
    - Converte separadores (1.234,56 -> 1234.56)
    """
    if pd.isna(val_str):
        return 0.0
    if isinstance(val_str, (float, int)):
        return float(val_str)

    s = re.sub(r"[^0-9,\.-]", "", str(val_str))

    # Casos:
    # 1) "1.234,56" -> remove milhares e troca vírgula por ponto
    # 2) "1234,56" -> troca vírgula por ponto
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def _is_crypto(symbol: str) -> bool:
    if symbol is None:
        return False
    return str(symbol).strip() not in FIAT

def _safe_str(x) -> str:
    return "" if pd.isna(x) else str(x)

def processar_bitcointrade_com_relatorios(
    file_path: str,
    out_irs: str = "BT_Arquivo_1_IRS.csv",
    out_swaps: str = "BT_Arquivo_2_Swaps.csv",
    out_reconciliacao: str = "BT_Arquivo_3_Reconciliacao.csv",
    out_full: str = "BT_Relatorio_FIFO_Completo_Contraparte.csv",
) -> Dict[str, pd.DataFrame]:
    """
    Processa extrato da BitcoinTrade (formato BT) e gera:
    Arquivo 1 (IRS): vendas contra FIAT (inclui BRL), com detalhe por lote FIFO (uma linha por lote consumido).
    Arquivo 2 (Swaps): swaps cripto-cripto no período 2022-2025, para prova de permutas.
    Arquivo 3 (Reconciliação): checklist de entradas/saídas de cripto com flag de "SEM ORIGEM" em depósitos cripto.

    Nota: este motor não faz conversões cambiais. O Arquivo 1 entrega o valor de venda na moeda de contraparte (ex.: BRL).
    """

    df = pd.read_csv(file_path, sep=";")

    # Normalização mínima esperada no layout BT
    # A BitcoinTrade tipicamente usa a coluna "Quantidade" para valores em fiat e quantidades em cripto.
    # Algumas exportações alternativas usam "Valor". Este bloco aceita ambas.
    for col in ["Data", "Hora", "Categoria", "Moeda"]:
        if col not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente no CSV: {col}")

    if "Quantidade" in df.columns:
        df["Val_Numeric"] = df["Quantidade"].apply(clean_val)
    elif "Valor" in df.columns:
        df["Val_Numeric"] = df["Valor"].apply(clean_val)
    else:
        raise ValueError('Coluna obrigatória ausente no CSV: Quantidade (ou, alternativamente, Valor)')

    df["Timestamp"] = pd.to_datetime(df["Data"].astype(str) + " " + df["Hora"].astype(str), dayfirst=True, errors="coerce")
    df = df.sort_values("Timestamp")
    inventory: Dict[str, List[Dict[str, Any]]] = {}

    # Saída completa (estilo consolidado)
    final_output: List[Dict[str, Any]] = []

    # Saída IRS por lote consumido
    report_irs: List[Dict[str, Any]] = []

    # Saída Swaps (evento agregado)
    report_swaps: List[Dict[str, Any]] = []

    # Reconciliação (entradas/saídas)
    report_recon: List[Dict[str, Any]] = []

    # Helper: adiciona lote ao inventário
    def add_lote(moeda: str, qty: float, cost_total: float, date_str: str):
        if moeda not in inventory:
            inventory[moeda] = []
        inventory[moeda].append({"qty": float(qty), "cost": float(cost_total), "date": date_str})

    # Helper: consome FIFO e retorna chunks (qtd, custo, data_origem)
    def consume_fifo(moeda: str, qty_to_consume: float) -> Tuple[float, List[Dict[str, Any]]]:
        if qty_to_consume <= 0:
            return 0.0, []
        if moeda not in inventory:
            inventory[moeda] = []

        qtd_restante = float(qty_to_consume)
        custo_total = 0.0
        chunks: List[Dict[str, Any]] = []

        while qtd_restante > 1e-12 and inventory[moeda]:
            lote = inventory[moeda][0]
            lote_qty = float(lote["qty"])
            lote_cost = float(lote["cost"])

            if lote_qty <= 0:
                inventory[moeda].pop(0)
                continue

            if lote_qty <= qtd_restante + 1e-12:
                # consome o lote inteiro
                custo_total += lote_cost
                chunks.append({"qty": lote_qty, "cost": lote_cost, "date": lote.get("date", "")})
                qtd_restante -= lote_qty
                inventory[moeda].pop(0)
            else:
                # consome parte do lote, custo proporcional
                frac = qtd_restante / lote_qty
                cost_part = lote_cost * frac
                custo_total += cost_part
                chunks.append({"qty": qtd_restante, "cost": cost_part, "date": lote.get("date", "")})

                # atualiza lote remanescente
                lote["qty"] = lote_qty - qtd_restante
                lote["cost"] = lote_cost - cost_part
                qtd_restante = 0.0

        # Se faltar inventário, mantém custo consumido e deixa o resto sem custo (para evidenciar problema)
        if qtd_restante > 1e-9:
            chunks.append({"qty": qtd_restante, "cost": 0.0, "date": "SEM INVENTÁRIO (Verificar)"})
            # não altera inventory, pois não há lotes
        return custo_total, chunks

    # Agrupamento por segundo (casamento)
    for ts, group in df.groupby("Timestamp", dropna=False):
        if pd.isna(ts):
            continue

        data_s = ts.strftime("%Y-%m-%d")
        hora_s = ts.strftime("%H:%M:%S")

        # 1) Entrada Fiat (Depósito bancário)
        depositos_fiat = group[(group["Categoria"].astype(str).str.contains("Depósito", na=False)) & (group["Moeda"] == "Real Brasileiro") & (group["Val_Numeric"] > 0)]
        for _, dep in depositos_fiat.iterrows():
            v = abs(dep["Val_Numeric"])
            final_output.append({
                "operação": "Entrada Fiat",
                "Data": data_s,
                "hora": hora_s,
                "Moeda": "BRL",
                "quantidade": "",
                "Valor (Custo FIFO)": v,
                "Ativo_Contraparte": "Banco",
                "Valor_Recebido_Contraparte": v,
                "Fees": 0.0
            })
            # Reconciliação (fiat não entra)

        # 2) Depósitos Cripto (custo zero, origem desconhecida no contexto BT)
        depositos_cripto = group[(group["Categoria"].astype(str).str.contains("Depósito", na=False)) & (group["Moeda"] != "Real Brasileiro") & (group["Val_Numeric"] > 0)]
        for _, dep in depositos_cripto.iterrows():
            moeda = _safe_str(dep["Moeda"]).strip()
            qtd = abs(dep["Val_Numeric"])
            if qtd <= 0:
                continue

            final_output.append({
                "operação": "Depósito Cripto",
                "Data": data_s,
                "hora": hora_s,
                "Moeda": moeda,
                "quantidade": qtd,
                "Valor (Custo FIFO)": 0.0,
                "Ativo_Contraparte": "Carteira Externa",
                "Valor_Recebido_Contraparte": qtd,
                "Fees": 0.0
            })
            add_lote(moeda, qtd, 0.0, data_s)

            report_recon.append({
                "Data": data_s,
                "hora": hora_s,
                "Moeda": moeda,
                "Qtd": qtd,
                "Tipo": "ENTRADA",
                "Operação": "Depósito",
                "Status": "DEPÓSITO SEM ORIGEM (Verificar)"
            })

        # 3) Compras (BRL -> Cripto): custo = BRL gasto alocado proporcionalmente por ativo
        compras_cripto = group[(group["Categoria"] == "Compra") & (group["Moeda"] != "Real Brasileiro") & (group["Val_Numeric"] > 0)]
        if not compras_cripto.empty:
            brl_gasto_total = abs(group[(group["Moeda"] == "Real Brasileiro") & (group["Val_Numeric"] < 0)]["Val_Numeric"].sum())
            fee_total = abs(group[group["Categoria"].astype(str).str.contains("Taxa sobre compra", na=False)]["Val_Numeric"].sum())

            total_qtd = compras_cripto["Val_Numeric"].sum()
            if total_qtd > 0 and brl_gasto_total > 0:
                for _, c in compras_cripto.iterrows():
                    moeda = _safe_str(c["Moeda"]).strip()
                    qtd = float(c["Val_Numeric"])
                    prop = qtd / total_qtd
                    custo = brl_gasto_total * prop
                    fee_prop = fee_total * prop

                    final_output.append({
                        "operação": "Compra",
                        "Data": data_s,
                        "hora": hora_s,
                        "Moeda": moeda,
                        "quantidade": qtd,
                        "Valor (Custo FIFO)": round(custo, 8),
                        "Ativo_Contraparte": "BRL",
                        "Valor_Recebido_Contraparte": round(custo, 8),
                        "Fees": round(fee_prop, 8)
                    })
                    add_lote(moeda, qtd, custo, data_s)

        # 4) Saídas (Venda / Retirada / Swap)
        saidas_cripto = group[(group["Val_Numeric"] < 0) & (group["Moeda"] != "Real Brasileiro")]

        if not saidas_cripto.empty:
            # Determina operação
            if group["Categoria"].astype(str).str.contains("Retirada", na=False).any():
                tipo_operacao = "Retirada para carteira externa"
            else:
                tipo_operacao = "Venda"  # engloba venda e swaps, contraparte define

            moeda_saida = _safe_str(saidas_cripto["Moeda"].iloc[0]).strip()
            qtd_total_saida = abs(saidas_cripto["Val_Numeric"].sum())
            fees_saida = abs(group[group["Categoria"].astype(str).str.contains("Taxa", na=False)]["Val_Numeric"].sum())

            # Contraparte: BRL (entrada fiat) ou cripto (entrada cripto) ou externo
            ativo_contra = ""
            valor_contra = 0.0

            if tipo_operacao == "Retirada para carteira externa":
                ativo_contra = "Carteira Externa"
                valor_contra = 0.0
            else:
                entrada_brl = group[(group["Moeda"] == "Real Brasileiro") & (group["Val_Numeric"] > 0)]
                entrada_cripto = group[(group["Val_Numeric"] > 0) & (group["Moeda"] != "Real Brasileiro") & (group["Moeda"] != moeda_saida)]

                if not entrada_brl.empty:
                    ativo_contra = "BRL"
                    valor_contra = float(entrada_brl["Val_Numeric"].sum())
                elif not entrada_cripto.empty:
                    ativo_contra = _safe_str(entrada_cripto["Moeda"].iloc[0]).strip()
                    valor_contra = float(entrada_cripto["Val_Numeric"].sum())
                else:
                    ativo_contra = "Desconhecido"
                    valor_contra = 0.0

            # FIFO
            custo_herdado_total, chunks = consume_fifo(moeda_saida, qtd_total_saida)

            # Saída completa agregada
            final_output.append({
                "operação": tipo_operacao,
                "Data": data_s,
                "hora": hora_s,
                "Moeda": moeda_saida,
                "quantidade": qtd_total_saida,
                "Valor (Custo FIFO)": round(custo_herdado_total, 8),
                "Ativo_Contraparte": ativo_contra,
                "Valor_Recebido_Contraparte": round(valor_contra, 8),
                "Fees": round(fees_saida, 8)
            })

            # Reconciliação: apenas movimentações cripto com potencial match externo
            if tipo_operacao == "Retirada para carteira externa":
                report_recon.append({
                    "Data": data_s,
                    "hora": hora_s,
                    "Moeda": moeda_saida,
                    "Qtd": qtd_total_saida,
                    "Tipo": "SAÍDA",
                    "Operação": "Retirada",
                    "Status": "RETIRADA PARA MATCH EXTERNO"
                })

            # Arquivo 1 (IRS): vendas contra FIAT, detalhado por lote FIFO
            if tipo_operacao == "Venda" and ativo_contra in FIAT:
                # alocação de proceeds proporcional à qty em cada chunk
                total_qty = sum(float(ch["qty"]) for ch in chunks if float(ch["qty"]) > 0)
                for ch in chunks:
                    ch_qty = float(ch["qty"])
                    if ch_qty <= 0:
                        continue
                    share = (ch_qty / total_qty) if total_qty > 0 else 0.0
                    proceeds_alloc = float(valor_contra) * share
                    cost_alloc = float(ch["cost"])
                    report_irs.append({
                        "Data_Venda": data_s,
                        "Moeda": moeda_saida,
                        "Quantidade": ch_qty,
                        "Data_Aquisição": ch.get("date", ""),
                        "Custo_Aquisição": round(cost_alloc, 8),
                        "Moeda_Venda": ativo_contra,
                        "Valor_Venda": round(proceeds_alloc, 8),
                        "Resultado": round(proceeds_alloc - cost_alloc, 8)
                    })

            # Arquivo 2 (Swaps): cripto-cripto, 2022-2025 (evento agregado)
            if tipo_operacao == "Venda" and _is_crypto(ativo_contra):
                if "2022-01-01" <= data_s <= "2025-12-31":
                    # data de origem FIFO: mais antiga entre chunks válidos
                    origem_dates = [ch.get("date", "") for ch in chunks if ch.get("date", "") and "SEM" not in ch.get("date", "")]
                    data_origem_min = min(origem_dates) if origem_dates else ""
                    report_swaps.append({
                        "Data": data_s,
                        "hora": hora_s,
                        "Saiu": moeda_saida,
                        "Quantidade_Saiu": qtd_total_saida,
                        "Entrou": ativo_contra,
                        "Quantidade_Entrou": valor_contra,
                        "Custo_Transferido": round(custo_herdado_total, 8),
                        "Data_Origem_FIFO": data_origem_min
                    })

    # DataFrames de saída
    df_full = pd.DataFrame(final_output)
    df_irs = pd.DataFrame(report_irs)
    df_swaps = pd.DataFrame(report_swaps)
    df_recon = pd.DataFrame(report_recon)

    # Persistência (sep ;, decimal ,) para consistência
    df_full.to_csv(out_full, index=False, sep=";", decimal=",")
    df_irs.to_csv(out_irs, index=False, sep=";", decimal=",")
    df_swaps.to_csv(out_swaps, index=False, sep=";", decimal=",")
    df_recon.to_csv(out_reconciliacao, index=False, sep=";", decimal=",")

    return {
        "full": df_full,
        "irs": df_irs,
        "swaps": df_swaps,
        "reconciliacao": df_recon,
    }

if __name__ == "__main__":
    # Ajusta aqui o nome do ficheiro BT a processar
    processar_bitcointrade_com_relatorios("BitcoinTrade_statement.csv")