#%% Libraries
import pandas as pd
import pandas_market_calendars as pmc
pd.set_option('display.max_columns', None)
from pathlib import Path
from pandas.tseries.offsets import BMonthBegin

import numpy as np
from pyacm import NominalACM
from scipy.stats import jarque_bera
from statsmodels.tsa.stattools import adfuller

import matplotlib.pyplot as plt
import seaborn as sns

from typing import Dict
import os

#%% DI1 - Import + ajusta

# Ajustes no di1
#### VENCIMENTO e Dus

def add_vencimento_e_dus(di):
    
    meses_pt = {
        'JAN': 1, 'FEV': 2, 'MAR': 3, 'ABR': 4, 'MAI': 5, 'JUN': 6,
        'JUL': 7, 'AGO': 8, 'SET': 9, 'OUT': 10, 'NOV': 11, 'DEZ': 12
    }

    meses_b3 = {
        'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
        'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
    }
    
    def _venc(row):
        code = str(row["maturity_code"]).upper().strip()
        ref = pd.Timestamp(row["refdate"])
        if code[:3] in meses_pt:
            mes, ano = meses_pt[code[:3]], int(code[3:])
        elif code[0] in meses_b3:
            mes, ano = meses_b3[code[0]], int(code[1:])
        else:
            return pd.NaT
        ano += 2000
        v = pd.Timestamp(ano, mes, 1) + BMonthBegin()
        while v < ref:
            ano += 10
            v = pd.Timestamp(ano, mes, 1) + BMonthBegin()
        return v
    
    di["refdate"] = pd.to_datetime(di["refdate"], errors="coerce")
    di["vencimento"] = di.apply(_venc, axis=1).astype("datetime64[ns]")
    
    cal = pmc.get_calendar("B3")
    start = pd.to_datetime(pd.concat([di["refdate"], di["vencimento"]]).min()).normalize() - pd.Timedelta(days=5)
    end   = pd.to_datetime(pd.concat([di["refdate"], di["vencimento"]]).max()).normalize() + pd.Timedelta(days=5)
    sch = cal.schedule(start_date=start, end_date=end)
    
    trading = pd.DatetimeIndex(sch.index).tz_localize(None).normalize().unique().sort_values()
    t_np = trading.values.astype("datetime64[D]")
    
    r = di["refdate"].dt.normalize().values.astype("datetime64[D]")
    v = di["vencimento"].dt.normalize().values.astype("datetime64[D]")
    mask = ~pd.isna(r) & ~pd.isna(v)
    out = np.full(len(di), np.nan, dtype="float")
    if mask.any():
        ir = np.searchsorted(t_np, r[mask], side="right")
        iv = np.searchsorted(t_np, v[mask], side="right")
        out[mask] = (iv - ir).astype(float)
    
    di["maturity_days"] = pd.Series(out, index=di.index).astype("Int64")
    return di

#%% ACM
def run_acm(data, di):
    
    # Read data
    df = pd.read_parquet(data)
    
    # Adjust data
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
    df = df.set_index('Date')
    maturities = [col for col in df.columns if col != 'Year']
    df[maturities] = df[maturities].astype(float)
    df[maturities] = df[maturities] / 100  # converter de percentual para decimal
    df = df.rename(columns={col: int(col.replace('M','')) for col in maturities})  # transformar colunas em int
    df = df.dropna()
    
    # Run model ACM
    acm = NominalACM(
        curve=df,
        n_factors=5,
    )
    
    # Transform term premium into DataFrame
    tp_df = pd.DataFrame(acm.tp, index=df.index, columns=df.columns)

    # Dados em semestres        
    colunas_int = [int(c) for c in tp_df.columns]
    semesters = [c for c in colunas_int if c % 6 == 0]
    tp_df = tp_df[semesters]
    
    # filtra dados para obter semanais
    tp_df.reset_index(inplace=True)
    tp_df = tp_df[tp_df['Date'].isin(di['refdate'])]
    tp_df["Date"] = pd.to_datetime(tp_df["Date"])
    tp_df = tp_df.set_index("Date")

    return acm, tp_df

#%% Normality test - Gauss

def normality_test_cumulative_data(df):
    
    alpha: float = 0.05
    
    results = []

    for column_name in df.columns:
        serie = df[column_name].dropna()

        for i in range(1, len(serie)):  # começar do segundo ponto
            if i < 30:  # só calcular quando houver pelo menos 30 observações acumuladas
                continue

            data_atual = serie.index[i]
            janela = serie.iloc[:i]  # todos os dados passados até a data atual

            if janela.std() > 0:
                jb_stat, p_val = jarque_bera(janela)
                interpretation = 'NORMAL' if p_val > alpha else 'NON_NORMAL'
            else:
                jb_stat, p_val, interpretation = np.nan, np.nan, 'Não aplicável'

            results.append({
                'Date': data_atual,
                'maturity': column_name,
                'JB_stat': jb_stat,
                'p-value': p_val,
                'interpretation': interpretation
            })

    return pd.DataFrame(results)

def normality_test_full(df):

    alpha=0.05
    results = []
    
    for column_name in df.columns:
        serie = df[column_name].dropna()

        if len(serie) < 30:  # exigir pelo menos 30 observações
            jb_stat, p_val, interpretation = np.nan, np.nan, 'Não aplicável'
        elif serie.std() == 0:  # variância zero
            jb_stat, p_val, interpretation = np.nan, np.nan, 'Não aplicável'
        else:
            jb_stat, p_val = jarque_bera(serie)
            interpretation = 'NORMAL' if p_val > alpha else 'NON_NORMAL'

        results.append({
            'maturity': column_name,
            'JB_stat': jb_stat,
            'p-value': p_val,
            'interpretation': interpretation
        })

    return pd.DataFrame(results)


def plot_normality_frequency_percent():

    cn = cumulative_normality.dropna(subset=["interpretation"]).copy()
    classes = ["NORMAL", "NON_NORMAL"]
    colors = {"NORMAL": "#2ca02c", "NON_NORMAL": "#d62728"}

    # Contagens por vértice
    freq_counts = (
        pd.crosstab(cn["maturity"], cn["interpretation"])
          .reindex(columns=classes, fill_value=0)
          .sort_index()
    )
    freq_counts["TOTAL"] = freq_counts.sum(axis=1)

    # Percentuais
    freq_pct = (
        freq_counts[classes]
          .div(freq_counts["TOTAL"], axis=0)
          .mul(100)
    )

    # Plot
    fig, ax = plt.subplots(figsize=(12, 6))
    bottom = None
    x = freq_pct.index.astype(str)

    for cls in classes:
        values = freq_pct[cls]
        ax.bar(x, values, bottom=bottom, label=cls, color=colors.get(cls), edgecolor="white")
        bottom = values if bottom is None else bottom + values

    ax.set_title("Frequência (%) de NORMAL vs NON_NORMAL por Vértice")
    ax.set_xlabel("Vértice (maturity)")
    ax.set_ylabel("Percentual (%)")
    ax.set_ylim(0, 100)
    ax.legend(title="Classe")
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    # Anotar percentuais dentro das barras
    for i, m in enumerate(x):
        cum = 0.0
        for cls in classes:
            v = float(freq_pct.iloc[i][cls])
            if v > 4:
                ax.text(i, cum + v / 2, f"{v:.0f}%", ha="center", va="center", fontsize=9, color="white")
            cum += v

    plt.tight_layout()
    plt.show()
    
#%% - Stationary test ADF

def adf_test_full_all(
    df: pd.DataFrame,
    alpha: float = 0.05,
    regression: str = "c",   # "c", "ct", "nc", "ctt"
    autolag: str = "AIC"     # "AIC", "BIC", "t-stat" ou None
) -> pd.DataFrame:
    
    results = []

    for col in df.columns:
        serie = df[col].dropna()

        # Ordena por data (segurança) sem alterar o DF original
        if not serie.index.is_monotonic_increasing:
            serie = serie.sort_index()

        # Tenta converter índice em datetime (localmente)
        try:
            if not np.issubdtype(type(serie.index.values[0]), np.datetime64):
                idx_dt = pd.to_datetime(serie.index, errors="coerce")
                if idx_dt.notna().all():
                    serie.index = idx_dt
        except Exception:
            pass

        # Verificações de sanidade
        if len(serie) == 0:
            results.append({
                "maturity": col,
                "n_obs": 0,
                "ADF_stat": np.nan,
                "ADF_pvalue": np.nan,
                "ADF_lags": np.nan,
                "ADF_nobs": np.nan,
                "interpretation": "Não aplicável"
            })
            continue

        if serie.std(ddof=1) <= 0 or np.allclose(serie.values, serie.values[0]):
            results.append({
                "maturity": col,
                "n_obs": int(len(serie)),
                "ADF_stat": np.nan,
                "ADF_pvalue": np.nan,
                "ADF_lags": np.nan,
                "ADF_nobs": np.nan,
                "interpretation": "Não aplicável"
            })
            continue

        # Executa ADF
        try:
            adf_res = adfuller(serie.values, regression=regression, autolag=autolag)
            adf_stat, adf_p, adf_lags, adf_nobs = adf_res[0], adf_res[1], adf_res[2], adf_res[3]
            interp = "STATIONARY" if (adf_p is not None and adf_p < alpha) else "NON_STATIONARY"
        except Exception:
            adf_stat = adf_p = adf_lags = adf_nobs = np.nan
            interp = "Falha no teste"

        results.append({
            "maturity": col,
            "n_obs": int(len(serie)),
            "ADF_stat": adf_stat,
            "ADF_pvalue": adf_p,
            "ADF_lags": adf_lags,
            "ADF_nobs": adf_nobs,
            "interpretation": interp
        })

    # DataFrame final
    out = pd.DataFrame(results)

    # Ordena por maturidade quando for numericamente possível, mantendo rótulos originais
    def _to_numeric_safe(x):
        try:
            return float(x)
        except Exception:
            return np.nan

    numeric_keys = out["maturity"].map(_to_numeric_safe)
    if numeric_keys.notna().any():
        # Prioriza ordenação por valor numérico quando disponível; caso contrário, por string do rótulo
        out = out.assign(_sort_key=np.where(numeric_keys.notna(), numeric_keys, np.inf))
        out = out.sort_values(["_sort_key", "maturity"]).drop(columns=["_sort_key"]).reset_index(drop=True)
    else:
        out = out.sort_values("maturity").reset_index(drop=True)

    return out


#%% HBOS

def hbos_rolling(
    df: pd.DataFrame,
    n: int,
    bins: int = 20,
    binning: str = "quantile",
    q: float = 0.99,
    epsilon: float = 1e-6,
    col_label=None
) -> pd.DataFrame:
 
    def _compute_histogram_edges(x_vals: np.ndarray, _bins: int, _binning: str) -> np.ndarray:
        x_clean = x_vals[~np.isnan(x_vals)]
        if x_clean.size == 0:
            return np.array([-np.inf, np.inf], dtype=float)

        if _binning == "uniform":
            x_min, x_max = np.min(x_clean), np.max(x_clean)
            if np.isclose(x_min, x_max):
                return np.array([x_min - 1e-12, x_max + 1e-12], dtype=float)
            return np.linspace(x_min, x_max, _bins + 1)
        elif _binning == "quantile":
            qs = np.linspace(0.0, 1.0, _bins + 1)
            edges = np.quantile(x_clean, qs, method="linear")
            edges = np.asarray(edges, dtype=float)
            eps = 1e-12
            for i in range(1, len(edges)):
                if edges[i] <= edges[i - 1]:
                    edges[i] = edges[i - 1] + eps
            return edges
        else:
            raise ValueError("binning deve ser 'quantile' ou 'uniform'.")

    def _hbos_score_last_of_window(x_win: np.ndarray, edges: np.ndarray, _epsilon: float) -> float:
        mask = ~np.isnan(x_win)
        if mask.sum() == 0:
            return np.nan

        counts, _ = np.histogram(x_win[mask], bins=edges)
        widths = np.diff(edges)
        total = counts.sum()

        dens = (counts + _epsilon) / (total + _epsilon * len(counts)) / np.maximum(widths, _epsilon)

        x_last = x_win[-1]
        if np.isnan(x_last):
            return np.nan
        bin_idx = np.searchsorted(edges, x_last, side="right") - 1
        bin_idx = np.clip(bin_idx, 0, len(dens) - 1)

        return -np.log(max(dens[bin_idx], _epsilon))

    def _process_column(s: pd.Series) -> pd.DataFrame:
        s = s.sort_index()
        x = s.values.astype(float)
        n_obs = len(x)

        scores = np.full(n_obs, np.nan, dtype=float)
        thr = np.full(n_obs, np.nan, dtype=float)
        outlier_type = np.full(n_obs, None, dtype=object)

        if n is None or n <= 1 or n > n_obs:
            return pd.DataFrame(
                {"value": s.values, "HBOS_score": scores, "threshold": thr, "is_outlier": False, "outlier_type": outlier_type},
                index=s.index
            )

        low_q = 1.0 - q
        for t in range(n, n_obs + 1):
            seg = x[t - n : t]
            if np.all(np.isnan(seg)):
                continue

            edges = _compute_histogram_edges(seg, bins, binning)
            s_last = _hbos_score_last_of_window(seg, edges, epsilon)
            scores[t - 1] = s_last

            seg_scores = np.full(seg.shape[0], np.nan, dtype=float)
            mask_seg = ~np.isnan(seg)
            if mask_seg.any():
                counts, _ = np.histogram(seg[mask_seg], bins=edges)
                widths = np.diff(edges)
                total = counts.sum()
                dens = (counts + epsilon) / (total + epsilon * len(counts)) / np.maximum(widths, epsilon)
                idx_bins = np.searchsorted(edges, seg[mask_seg], side="right") - 1
                idx_bins = np.clip(idx_bins, 0, len(dens) - 1)
                seg_scores[mask_seg] = -np.log(np.maximum(dens[idx_bins], epsilon))

            seg_valid = ~np.isnan(seg_scores)
            if seg_valid.any():
                thr[t - 1] = np.quantile(seg_scores[seg_valid], q)

            # Classificação do tipo de outlier: 'max' ou 'min' com base em quantis da janela
            x_last = seg[-1]
            if not np.isnan(x_last):
                seg_vals_valid = seg[~np.isnan(seg)]
                if seg_vals_valid.size > 0:
                    q_low = np.quantile(seg_vals_valid, low_q)
                    q_high = np.quantile(seg_vals_valid, q)
                    if x_last >= q_high:
                        outlier_type[t - 1] = "max"
                    elif x_last <= q_low:
                        outlier_type[t - 1] = "min"
                    else:
                        outlier_type[t - 1] = None

        is_out = (scores > thr) & ~np.isnan(scores) & ~np.isnan(thr)

        # Garante que o tipo só é marcado quando is_outlier=True; caso contrário, None
        outlier_type_final = np.where(is_out, outlier_type, None)

        return pd.DataFrame(
            {
                "value": s.values,
                "HBOS_score": scores,
                "threshold": thr,
                "is_outlier": is_out,
                "outlier_type": outlier_type_final
            },
            index=s.index
        )

    if col_label is None:
        frames = []
        for col in df.columns:
            temp = _process_column(df[col])
            temp["maturity"] = col
            frames.append(temp)

        result = pd.concat(frames).sort_index()

        # Remove as primeiras n observações por vértice (maturity)
        if isinstance(n, int) and n > 1:
            result["__ord__"] = result.groupby("maturity").cumcount()
            result = result[result["__ord__"] >= n].drop(columns="__ord__")

        return result

    res = _process_column(df[col_label])

    # Remove as primeiras n observações na coluna especificada
    if isinstance(n, int) and n > 1 and n <= len(res):
        res = res.iloc[n:]

    return res


#%% Função Backtest

def backtest_di(
    hbos: pd.DataFrame,
    di: pd.DataFrame,
    cdi: pd.DataFrame,
    maturity_target: int = 6,
    n_contratos: int = 1,
    stop_pct: float = 0.05,
    notional_contrato: float = 100_000.0,
    valor_corretagem: float = 20.0
) -> pd.DataFrame:

    # ------------------------------------------------------
    # 1. Initial Fiilter
    # ------------------------------------------------------
    sinal = hbos[hbos['maturity'] == maturity_target].copy()
    sinal['maturity_days'] = maturity_target * 21

    # ------------------------------------------------------
    # 2. Find the nearest contract
    # ------------------------------------------------------
    def join_sinal_di_nearest_maturity(sinal, di):
        s = sinal.reset_index().copy()
        d = di.copy()
        s['Date'] = pd.to_datetime(s['Date']).dt.normalize()
        d['refdate'] = pd.to_datetime(d['refdate']).dt.normalize()

        merged = s.merge(
            d,
            left_on='Date',
            right_on='refdate',
            how='left',
            suffixes=('', '_di')
        )
        merged['abs_diff_maturity'] = (merged['maturity_days'] - merged['maturity_days_di']).abs()
        merged['abs_diff_maturity_filled'] = merged['abs_diff_maturity'].fillna(np.inf)
        best_idx = merged.groupby('Date', sort=False)['abs_diff_maturity_filled'].idxmin()
        out = merged.loc[best_idx].copy()
        out.drop(columns=['abs_diff_maturity', 'abs_diff_maturity_filled'], inplace=True)
        out.set_index('Date', inplace=True)
        return out

    trading = join_sinal_di_nearest_maturity(sinal, di)

    # ------------------------------------------------------
    # 3. Add previous contract
    # ------------------------------------------------------
    def add_previous_contract_columns(trading, di):
        tr = trading.copy().sort_index()
        tr['refdate'] = pd.to_datetime(tr['refdate']).dt.normalize()
        d = di.copy()
        d['refdate'] = pd.to_datetime(d['refdate']).dt.normalize()

        tr['previous_symbol'] = tr['symbol'].shift(1)
        tr.loc[tr['symbol'] == tr['previous_symbol'], 'previous_symbol'] = pd.NA

        tmp = tr.reset_index()[['Date', 'refdate', 'previous_symbol']].rename(columns={'previous_symbol': 'symbol'})
        tmp = tmp[~tmp['symbol'].isna()].copy()

        prev_prices = tmp.merge(
            d[['refdate', 'symbol', 'price']],
            on=['refdate', 'symbol'],
            how='left'
        ).rename(columns={'price': 'previous_price_on_dt'})

        tr = tr.reset_index().merge(
            prev_prices[['Date', 'symbol', 'previous_price_on_dt']].rename(columns={'symbol': 'previous_symbol'}),
            on=['Date', 'previous_symbol'],
            how='left'
        ).set_index('Date')

        tr['rolagem'] = tr['previous_symbol'].notna() & tr['previous_price_on_dt'].notna()
        return tr

    trading = add_previous_contract_columns(trading, di)

    # ------------------------------------------------------
    # 4. Ajuste (R$)
    # ------------------------------------------------------
    trading['ajuste_financeiro'] = np.where(
        trading['rolagem'],
        trading['previous_price_on_dt'] - trading['price'].shift(1),
        trading['price'] - trading['price'].shift(1)
    )

    # ------------------------------------------------------
    # 5. Trading and metrics
    # ------------------------------------------------------
    df = trading.copy().sort_index()
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['ajuste_financeiro'] = pd.to_numeric(df['ajuste_financeiro'], errors='coerce').fillna(0.0)

    posicao, notional, pnl_list = [], [], []
    pnl_acum_book_list, pnl_acum_trade_list = [], []
    stop_valor_list, stop_flag_list = [], []
    trade_id_list, corretagem_list, pnl_acum_aju_list = [], [], []

    pos = 0
    pnl_acum_book = 0.0
    pnl_acum_trade = 0.0
    trade_counter = 0
    corretagem_total = 0.0
    size = int(abs(n_contratos))

    for dt, row in df.iterrows():
        price = row['price']
        ajuste = row['ajuste_financeiro']
        signal = row.get('outlier_type', None)
        rolagem_flag = bool(row.get('rolagem', False))

        # Daily PnL
        pnl_dia = -ajuste * abs(pos) if pos > 0 else ajuste * abs(pos) if pos < 0 else 0.0
        pnl_acum_book += pnl_dia

        # Cumulated PNL
        if pos != 0:
            pnl_acum_trade += pnl_dia
        else:
            pnl_acum_trade = np.nan

        # Stop 
        if pos != 0:
            stop_valor = stop_pct * notional_contrato * abs(pos)
            stop_atingido = pnl_acum_trade <= -stop_valor
        else:
            stop_valor = np.nan
            stop_atingido = False

        next_pos = pos
        stop_flag_today = False
        corretagem_dia = 0.0

        # STOP
        if stop_atingido:
            next_pos = 0
            stop_flag_today = True
            trade_counter += 1
            corretagem_dia += valor_corretagem * abs(pos)

        # Signals
        desired_pos = +size if signal == 'min' else -size if signal == 'max' else None
        if desired_pos is not None:
            if pos == 0 and next_pos == 0:
                next_pos = desired_pos
                trade_counter += 1
                corretagem_dia += valor_corretagem * abs(desired_pos)
            elif pos != 0 and np.sign(desired_pos) != np.sign(pos):
                next_pos = desired_pos
                trade_counter += 1
                corretagem_dia += valor_corretagem * abs(desired_pos)

        # Rollover
        if rolagem_flag and pos != 0:
            trade_counter += 2
            corretagem_dia += 2 * valor_corretagem * abs(size)

        corretagem_total += corretagem_dia

        # Results
        posicao.append(pos)
        notional.append(price * pos if pd.notna(price) else np.nan)
        pnl_list.append(pnl_dia)
        pnl_acum_book_list.append(pnl_acum_book)
        pnl_acum_trade_list.append(pnl_acum_trade)
        stop_valor_list.append(stop_valor)
        stop_flag_list.append(stop_flag_today)
        trade_id_list.append(trade_counter)
        corretagem_list.append(corretagem_dia)
        pnl_acum_aju_list.append(pnl_acum_book - corretagem_total)

        prev_pos = pos
        pos = next_pos

        if (prev_pos == 0 and pos != 0) or (prev_pos != 0 and pos != 0 and np.sign(prev_pos) != np.sign(pos)):
            pnl_acum_trade = 0.0


    # ------------------------------------------------------
    # 6. Final Columns
    # ------------------------------------------------------
    df['posicao'] = posicao
    df['notional'] = notional
    df['pnl'] = pnl_list
    df['pnl_acumulado'] = pnl_acum_book_list
    df['pnl_acumulado_trade'] = pnl_acum_trade_list
    df['stop_valor'] = stop_valor_list
    df['stop_flag'] = stop_flag_list
    df['trade_id'] = trade_id_list
    df['corretagem'] = corretagem_list
    df['pnl_acum_aju'] = pnl_acum_aju_list

    
    # ------------------------------------------------------
    # 7. PNL vs CDI
    # ------------------------------------------------------
    cdi = cdi.copy()

    cdi['CDI_year'] = cdi['CDI_year'] / 100
    cdi['CDI_daily'] = (1 + cdi['CDI_year']) ** (1/252) - 1
    cdi.rename(columns={'Date':'refdate'}, inplace=True)
    cdi['refdate'] = pd.to_datetime(cdi['refdate'], dayfirst=True)   

    df = pd.merge_asof(
        df.sort_values('refdate'),
        cdi.sort_values('refdate')[['refdate', 'CDI_daily']],
        on='refdate'
    )
    
    df['prev_Date'] = df['refdate'].shift(1)
    df['dias_uteis_pregao'] = df.apply(
        lambda row: len(pd.bdate_range(start=row['prev_Date'], end=row['refdate'])) - 1 
        if pd.notna(row['prev_Date']) else np.nan,
        axis=1
    )
    
    df['cdi_period'] = (1 + df['CDI_daily']) ** df['dias_uteis_pregao'] - 1
    df['pnl_cdi'] = df['cdi_period'] * abs(df['notional'])
        
    df['pnl_final'] = np.where(
    df['posicao'] > 0,
    df['pnl'] + df['pnl_cdi'],
    df['pnl'] - df['pnl_cdi']
    )

    df['pnl_final_acum'] = (df['pnl_final'].cumsum())


    cols_final = [
        'refdate', 'value', 'is_outlier', 'outlier_type',
        'maturity', 'maturity_days', 'maturity_code',
        'symbol', 'price', 'maturity_days_di', 'previous_symbol',
        'previous_price_on_dt', 'rolagem', 'ajuste_financeiro', 'posicao',
        'notional', 'pnl', 'pnl_acumulado', 'pnl_acumulado_trade',
        'stop_valor', 'stop_flag', 'trade_id', 'corretagem', 'pnl_acum_aju', 
        'pnl_cdi', 'pnl_final', 'pnl_final_acum'
        
    ]

    return df[[c for c in cols_final if c in df.columns]]



#%% Analise e eficiencia - v1 (sem cdi) --- retornos ja são ex cdi

def calcular_cotas(trading_metric, notional_contrato=100_000):
 
    df = trading_metric.copy()
     
    df['retorno_acumulado_estrategia'] = df['pnl_final_acum'] / notional_contrato
    df['cota_estrategia'] = (1 + df['retorno_acumulado_estrategia'])
    
    df['retorno_semanal'] = df['cota_estrategia'].pct_change()

    vols = []
    for i in range(len(df)):
        serie_passada = df['retorno_semanal'].iloc[1:i+1].dropna()

        if len(serie_passada) > 1:
            vol = serie_passada.std() * np.sqrt(52)
        else:
            vol = np.nan
        vols.append(vol)

    df['vol'] = vols
    
    df['sharpe'] = df['retorno_acumulado_estrategia'] / df['vol']
    return df


#%% DF_Cotas


def build_df_cotas(resultados_cotas: Dict) -> pd.DataFrame:

    def _get_date_index(df: pd.DataFrame) -> pd.Series:
        if isinstance(df.index, pd.DatetimeIndex):
            return df.index
        if 'refdate' in df.columns:
            return pd.to_datetime(df['refdate'], errors='coerce')
        for cand in ['date', 'Date', 'dt', 'data']:
            if cand in df.columns:
                return pd.to_datetime(df[cand], errors='coerce')
        raise ValueError("Não foi possível determinar a coluna de data (índice datetime ou 'refdate').")

    cdi_series = None
    estrategias = {}

    for key, df in resultados_cotas.items():
        if not isinstance(df, pd.DataFrame):
            continue

        dti = _get_date_index(df)
        dff = df.copy()
        dff = dff.assign(_dt=dti).dropna(subset=['_dt']).sort_values('_dt')
        dff = dff.drop_duplicates(subset=['_dt'], keep='last').set_index('_dt')

        for col in ['cota_cdi', 'cota_estrategia', 'maturity']:
            if col in dff.columns:
                dff[col] = pd.to_numeric(dff[col], errors='coerce')

        maturity = None
        if isinstance(key, (int, float)) and pd.notna(key):
            maturity = int(key)
        elif 'maturity' in dff.columns and dff['maturity'].notna().any():
            maturity = int(dff['maturity'].dropna().iloc[0])

        if 'cota_estrategia' in dff.columns and maturity is not None:
            estrategias[maturity] = dff['cota_estrategia'].copy()

        if 'cota_cdi' in dff.columns:
            cdi_col = dff['cota_cdi'].copy()
            cdi_series = cdi_col if cdi_series is None else cdi_series.combine_first(cdi_col)

    pieces = []

    if cdi_series is not None:
        pieces.append(cdi_series.rename('cota_cdi'))

    for m in sorted(estrategias.keys()):
        pieces.append(estrategias[m].rename(f'cota_estrategia_{m}'))

    if not pieces:
        return pd.DataFrame()

    df_cotas = pd.concat(pieces, axis=1).sort_index()

    return df_cotas


#%% Final Function (Run ALL)
def run_all_backtest(
    premia: pd.DataFrame,
    di: pd.DataFrame,
    cdi: pd.DataFrame,
    n_hbos: int = 52,
    bins: int = 20,
    q: float = 0.95,
    stop_pct: float = 0.05,
    n_contratos: int = 1,
    valor_corretagem: float = 20.0,
    notional_contrato: float = 100_000.0,
    plot: bool = False,
    path_save: str = r"path"
):
    

    # ======================================================
    # 1. HBOS rolling
    # ======================================================
    hbos = hbos_rolling(
        df=premia,
        n=n_hbos,
        bins=bins,
        binning="quantile",
        q=q,
        epsilon=1e-6,
        col_label=None
    )

    # ======================================================
    # 2. backtest to each maturity
    # ======================================================
    resultados = {}
    for maturity in premia.columns:
        maturity_int = int(maturity)
        print(f"Rodando backtest para maturidade {maturity_int} meses...")
        try:
            resultado = backtest_di(
                di=di,
                hbos=hbos,
                cdi=cdi,
                maturity_target=maturity_int,
                n_contratos=n_contratos,
                stop_pct=stop_pct,
                valor_corretagem=valor_corretagem
            )
            resultados[maturity_int] = resultado
        except Exception as e:
            print(f"❌ Erro na maturidade {maturity_int}: {e}")

    print("✅ Todos os backtests concluídos!")

    # ======================================================
    # 3. Returns
    # ======================================================
    resultados_cotas = {}
    for maturity, df_trading in resultados.items():
        try:
            resultados_cotas[maturity] = calcular_cotas(df_trading, notional_contrato)
        except Exception as e:
            print(f"❌ Erro ao calcular cotas para maturidade {maturity}: {e}")

    print("Cálculo de cotas concluído!")

    df_cotas = build_df_cotas(resultados_cotas)
    print("df_cotas consolidado com sucesso!")


    if plot:
        plt.figure(figsize=(14, 6))
        for col in df_cotas.columns:
            plt.plot(df_cotas.index, df_cotas[col], label=col)

        plt.title("Cotas CDI e Estratégias")
        plt.xlabel("Data")
        plt.ylabel("Cota")
        plt.legend(ncol=3, fontsize=8)
        plt.grid(alpha=0.3)
        plt.tight_layout()

        fig_path = f"{path_save}\\cotas_backtest.png"
        plt.savefig(fig_path, dpi=300, bbox_inches="tight")
        plt.close()

        print(f" Gráfico salvo em: {fig_path}")

    return df_cotas, resultados, resultados_cotas


#%% 1. Run All + Resultados e analise

# Imports

### DI
ArithmeticErrordi = pd.read_parquet(r"data_di1.parquet")
di.drop(columns={'price_previous', 'change', 'settlement_value'}, inplace=True)

### Curva B3
df_curves = r"curvas_b3.parquet"

### CDI
cdi = pd.read_csv(r"hist_cdi.csv", sep=';')

# RUN

di = add_vencimento_e_dus(di)

acm, premia = run_acm(df_curves, di)

cumulative_normality = normality_test_cumulative_data(premia)

adf_full_all = adf_test_full_all(
    premia,
    alpha=0.05,
    regression="c",
    autolag="AIC"
)


n_hbos_values = list(range(13, 157, 13))

resultados_df_cotas = {}
resultados_backtests = {}
resultados_cotas = {}

for n in n_hbos_values:
    print(f"\nRodando backtest com n_hbos = {n}...\n")
    try:
        df_cotas, resultados, resultados_cotas_n = run_all_backtest(
            premia=premia,
            di=di,
            cdi=cdi,
            n_hbos=n,
            plot=False  
        )

        resultados_df_cotas[n] = df_cotas
        resultados_backtests[n] = resultados
        resultados_cotas[n] = resultados_cotas_n

        print(f"✅ Concluído para n_hbos = {n}")

    except Exception as e:
        print(f"❌ Erro em n_hbos = {n}: {e}")

#%% 1.1: Data analytics: analise dos inputs curva
def plot_data(data):
    # Ler dados
    df = pd.read_parquet(data)
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
    df['Year'] = df['Date'].dt.year

    # Selecionar primeira curva de cada ano
    first_curve_each_year = df.groupby('Year').first().reset_index()

    # Todas as maturidades disponíveis
    maturities = [int(col[1:]) for col in df.columns if col.startswith('M')]

    # --- Gráfico 1: Curvas completas ---
    plt.figure(figsize=(14,7))
    cmap = plt.get_cmap('tab20')  # paleta de cores
    colors = [cmap(i % 20) for i in range(len(first_curve_each_year))]
    
    for i, (_, row) in enumerate(first_curve_each_year.iterrows()):
        rates = row[[col for col in df.columns if col.startswith('M')]].values
        plt.plot(maturities, rates, label=str(row['Year']), color=colors[i], linewidth=2)

    # Layout
    plt.title('Interest Rate Curves', fontsize=16)
    plt.xlabel('Maturity (months)', fontsize=14)
    plt.ylabel('Rate (%)', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=12)
    plt.tight_layout()
    
    # Salvar gráfico 1
    output_path1 = r"plot_curvas.png"
    Path(output_path1).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path1, dpi=300)
    plt.close()
    print(f"Gráfico completo salvo em: {output_path1}")

    # --- Gráfico 2: Evolução de maturidades específicas ---
    selected_maturities = [1, 12, 36, 60, 120]
    
    plt.figure(figsize=(14,7))
    for maturity in selected_maturities:
        col_name = f'M{maturity}'
        if col_name in df.columns:
            plt.plot(df['Date'], df[col_name], label=f'{maturity} month' if maturity == 1 else f'{maturity} months', linewidth=2)
    
    # Layout
    plt.title('Interest Rate Evolution for Selected Maturities', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Rate (%)', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(fontsize=12)
    plt.tight_layout()
    
    # Salvar gráfico 2
    output_path2 = r"plot_vertices.png"
    plt.savefig(output_path2, dpi=300)
    plt.close()
    print(f"Gráfico de maturidades selecionadas salvo em: {output_path2}")

    return df

def descriptive_stats(df, maturities=None):
    if maturities is None:
        maturities = [col for col in df.columns if col.startswith('M')]
    else:
        maturities = [f'M{m}' if isinstance(m, int) else m for m in maturities]

    stats_list = []
    for col in maturities:
        rates = df[col]
        stats_list.append({
            'Maturity (Months)': int(col[1:]),  # 'M1' = 1 mês
            'Mean': round(rates.mean(), 2),
            'Std': round(rates.std(), 2),
            'Min': round(rates.min(), 2),
            'Max': round(rates.max(), 2),
            '25%': round(rates.quantile(0.25), 2),
            '50%': round(rates.median(), 2),
            '75%': round(rates.quantile(0.75), 2)
        })

    stats_df = pd.DataFrame(stats_list)
    stats_df = stats_df.sort_values('Maturity (Months)').reset_index(drop=True)
    return stats_df

# Plot
#df = plot_data(df_curves)

# Stats 
#stats_df = descriptive_stats(df, [1, 12, 36, 60, 120])
#%% 1.2 Premia: analise dos resultados

def plot_premia(premia):
    # Garantir que Date seja datetime
    premia.index = pd.to_datetime(premia.index, dayfirst=True)
    premia['Year'] = premia.index.year

    # Selecionar primeira observação de cada ano
    first_curve_each_year = premia.groupby('Year').first().reset_index()

    # Todas as maturidades disponíveis
    maturities = [col for col in premia.columns if isinstance(col, (int, float))]

    # --- Gráfico 1: Curvas completas ---
    plt.figure(figsize=(14,7))
    cmap = plt.get_cmap('tab20')
    colors = [cmap(i % 20) for i in range(len(first_curve_each_year))]
    
    for i, (_, row) in enumerate(first_curve_each_year.iterrows()):
        rates = row[maturities].values
        plt.plot(maturities, rates, label=str(int(row['Year'])), color=colors[i], linewidth=2)


    plt.title('Term Premium Curves', fontsize=16)
    plt.xlabel('Maturity (months)', fontsize=14)
    plt.ylabel('Rate', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=12)
    plt.tight_layout()

    output_path1 = r"plot_curvas_premia.png"
    Path(output_path1).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path1, dpi=300)
    plt.close()
    print(f"Gráfico completo salvo em: {output_path1}")

    # --- Gráfico 2: Evolução de maturidades específicas ---
    selected_maturities = [6, 12, 36, 60, 120]  # adaptado para premia
    
    plt.figure(figsize=(14,7))
    for maturity in selected_maturities:
        if maturity in premia.columns:
            plt.plot(premia.index, premia[maturity], label=f'{maturity} months', linewidth=2)

    plt.title('Term Premium Evolution for Selected Maturities', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Rate', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(fontsize=12)
    plt.tight_layout()

    output_path2 = r"plot_vertices_premia.png"
    plt.savefig(output_path2, dpi=300)
    plt.close()
    print(f"Gráfico de maturidades selecionadas salvo em: {output_path2}")

    return premia

def descriptive_stats_premia(premia, maturities=None):
    if maturities is None:
        maturities = [col for col in premia.columns if isinstance(col, (int, float))]
    
    stats_list = []
    for col in maturities:
        rates = premia[col]
        stats_list.append({
            'Maturity (Months)': int(col),
            'Mean': round(rates.mean(), 4),
            'Std': round(rates.std(), 4),
            'Min': round(rates.min(), 4),
            'Max': round(rates.max(), 4),
            '25%': round(rates.quantile(0.25), 4),
            '50%': round(rates.median(), 4),
            '75%': round(rates.quantile(0.75), 4)
        })

    stats_df = pd.DataFrame(stats_list)
    stats_df = stats_df.sort_values('Maturity (Months)').reset_index(drop=True)
    return stats_df

premia = plot_premia(premia)
stats = descriptive_stats_premia(premia)
#%% 1.3: JB Normality + ADF

teste_jb = normality_test_full(premia)

teste_adf = adf_test_full_all(premia)
#%% 1.4: hbos

# Agregar resultados
dfs_agregados = []

# Percorre cada n_hbos e seus resultados
for n_hbos, resultados_dict in resultados_backtests.items():
    for nome_df, df in resultados_dict.items():
        if isinstance(df, pd.DataFrame):
            # Mantém apenas as colunas desejadas, se existirem
            colunas = ['refdate', 'value', 'outlier_type', 'maturity', 'posicao', 'notional', 'pnl_final', 'pnl_final_acum']
            colunas_existentes = [c for c in colunas if c in df.columns]

            # Cria cópia filtrada
            df_filtrado = df[colunas_existentes].copy()

            # Adiciona coluna com o n_hbos
            df_filtrado['n_hbos'] = n_hbos

            # Adiciona à lista
            dfs_agregados.append(df_filtrado)

# Concatena tudo num único DataFrame final
df_agregado = pd.concat(dfs_agregados, ignore_index=True)

print(df_agregado.head())


# Mostrar dinamica do hbos em 60 meses com 52
hbos_df = df_agregado[df_agregado['maturity'] == 60]
hbos_df = hbos_df[hbos_df['n_hbos'] == 52]


def plot_outliers_hbos(hbos_df, maturidade=None, output_dir=r'path'):
    """
    Gera e salva um gráfico mostrando os outliers (max e min) sobre a série 'value'
    para uma dada maturidade contida no DataFrame hbos_df.
    """

    os.makedirs(output_dir, exist_ok=True)

    # Se a maturidade não for especificada, usa a primeira do DataFrame
    if maturidade is None:
        maturidade = hbos_df['maturity'].iloc[0]

    # Filtra a maturidade escolhida
    df_plot = hbos_df[hbos_df['maturity'] == maturidade].copy()

    # Define parâmetros
    n_hbos = df_plot['n_hbos'].iloc[0] if 'n_hbos' in df_plot.columns else None

    # Configura o estilo
    plt.figure(figsize=(14, 7))
    sns.set_style('whitegrid')

    # Linha base da série
    plt.plot(df_plot['refdate'], df_plot['value'], color='black', linewidth=1.4, label='Term Premium')

    # Marca outliers
    df_out_max = df_plot[df_plot['outlier_type'] == 'max']
    df_out_min = df_plot[df_plot['outlier_type'] == 'min']

    plt.scatter(df_out_max['refdate'], df_out_max['value'], color='red', marker='o', s=45, label='Outlier max')
    plt.scatter(df_out_min['refdate'], df_out_min['value'], color='green', marker='o', s=45, label='Outlier min')

    # Título e eixos
    title = f'Outliers - Maturity {maturidade} months'
    if n_hbos is not None:
        title += f' (n_hbos={n_hbos})'
    plt.title(title, fontsize=14)
    plt.xlabel('Data', fontsize=12)
    plt.ylabel('Term Premium (%)', fontsize=12)
    plt.legend()
    plt.tight_layout()

    # Caminho e salvamento
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    file_name = f'outliers_m{maturidade}_nhbos{n_hbos}_{timestamp}.png' if n_hbos is not None else f'outliers_m{maturidade}_{timestamp}.png'
    file_path = os.path.join(output_dir, file_name)

    plt.savefig(file_path, dpi=300, bbox_inches='tight')
    plt.close()

    print(f'✅ Gráfico salvo em: {file_path}')
    return file_path

#plot_outliers_hbos(hbos_df, maturidade=60)

def count_outliers(df: pd.DataFrame):
    """
    Conta o número de outliers ('min' e 'max') separadamente e retorna dois DataFrames
    prontos para plotagem (maturity no eixo y, n_hbos no eixo x).

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame contendo colunas ['maturity', 'n_hbos', 'outlier_type'].

    Retorna
    -------
    tuple(pd.DataFrame, pd.DataFrame)
        Dois DataFrames: (df_min, df_max)
    """
    # Filtrar outliers
    df_outliers = df[df['outlier_type'].isin(['min', 'max'])]

    # Contar por maturity e n_hbos
    tabela = (
        df_outliers
        .groupby(['maturity', 'n_hbos', 'outlier_type'])
        .size()
        .reset_index(name='count')
    )

    # Criar tabela de min
    df_min = tabela[tabela['outlier_type'] == 'min'].pivot(
        index='maturity',
        columns='n_hbos',
        values='count'
    ).fillna(0).astype(int)

    # Criar tabela de max
    df_max = tabela[tabela['outlier_type'] == 'max'].pivot(
        index='maturity',
        columns='n_hbos',
        values='count'
    ).fillna(0).astype(int)

    return df_min, df_max

# Uso
df_min, df_max = count_outliers(df_agregado)

#%% 1.5 backtest results

# Backtest maturity = 60, hbos = 52
plot_backtest = resultados_backtests[52][60]

def plot_trades(df: pd.DataFrame,
                                    value_col: str = 'pnl_final_acum',
                                    date_col: str = 'refdate',
                                    pos_col: str = 'posicao',
                                    stop_col: str = 'stop_flag',
                                    save_path: str = r"path",
                                    filename: str = 'excess_returns_trades_stops_reversed.png'):
    """
    Plota excess returns com trades invertidos (1 ↔ -1) e stops.
    
    Compra: posicao muda de 0 → -1
    Venda: posicao muda de 0 → 1
    Stop: stop_flag == True
    """
    df[date_col] = pd.to_datetime(df[date_col])
    
    # Detectar trades invertidos
    df['prev_pos'] = df[pos_col].shift(1).fillna(0)
    buys = df[(df['prev_pos'] == 0) & (df[pos_col] == -1)]
    sells = df[(df['prev_pos'] == 0) & (df[pos_col] == 1)]
    stops = df[df[stop_col] == True]
    
    plt.figure(figsize=(14,6))
    
    # Linha de excess returns
    plt.plot(df[date_col], df[value_col], color='blue', label='Excess Returns (BRL)')
    
    # Plotar trades invertidos
    plt.scatter(buys[date_col], buys[value_col], color='green', label='Buy', zorder=5)
    plt.scatter(sells[date_col], sells[value_col], color='red', label='Sell', zorder=5)
    
    # Plotar stops
    plt.scatter(stops[date_col], stops[value_col], color='black', marker='x', s=80, label='Stop', zorder=6)
    
    # Ajustar eixo x para anos
    plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y'))
    plt.xticks(rotation=45)
    
    plt.xlabel('Year')
    plt.ylabel('Excess Returns')
    plt.title('Excess Returns with Trades and Stops (Maturity = 60 months, Outlier Rolling Window = 52 weeks)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    
    os.makedirs(save_path, exist_ok=True)
    plt.savefig(os.path.join(save_path, filename), dpi=300)
    plt.show()
    
    df.drop(columns='prev_pos', inplace=True)

    
plot_trades(plot_backtest)



##### Sharpe ratios

# Lista para armazenar os dfs meltados
dfs_melt = []

for n_hbos, df in resultados_df_cotas.items():
    # Resetar índice para manter _dt como coluna
    df_reset = df.reset_index()
    
    # Meltar o dataframe
    df_melt = df_reset.melt(id_vars='_dt', var_name='estrategia', value_name='cota')
    
    # Adicionar a coluna n_hbos
    df_melt['n_hbos'] = n_hbos
    
    # Adicionar à lista
    dfs_melt.append(df_melt)

# Concatenar todos
sharpe_df = pd.concat(dfs_melt, ignore_index=True)

sharpe_df.dropna(subset='cota', inplace=True)
sharpe_df.rename(columns={'_dt':'DT'}, inplace=True)
sharpe_df['DT'] = pd.to_datetime(sharpe_df['DT'])

sharpe_df['retorno'] = sharpe_df.groupby(['estrategia', 'n_hbos'])['cota'].pct_change()
sharpe_df['volatilidade'] = sharpe_df.groupby(['estrategia', 'n_hbos'])['retorno'].transform('std')
sharpe_df['volatilidade'] = sharpe_df['volatilidade'] * (252 ** 0.5)

sharpe_df = sharpe_df[sharpe_df['DT'] == sharpe_df['DT'].max()]

sharpe_df['excess_return'] = sharpe_df['cota'] - 1
sharpe_df['sharpe'] = sharpe_df['excess_return'] / sharpe_df['volatilidade']

sharpe_df['maturity'] = sharpe_df['estrategia'].str.replace('cota_estrategia_', '', regex=False).astype(int)
sharpe_df = sharpe_df[['maturity', 'n_hbos', 'excess_return', 'volatilidade', 'sharpe']]

pivot_sharpe = sharpe_df.pivot(
    index='n_hbos', 
    columns='maturity', 
    values='sharpe'
)

pivot_vol = sharpe_df.pivot(
    index='n_hbos', 
    columns='maturity', 
    values='volatilidade'
)

pivot_excess_return = sharpe_df.pivot(
    index='n_hbos', 
    columns='maturity', 
    values='excess_return'
)