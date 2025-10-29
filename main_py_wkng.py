#%% Libraries
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from pyacm import NominalACM
from scipy.stats import jarque_bera
import numpy as np
from scipy.stats import rankdata
from pandas.tseries.offsets import BMonthBegin
import pandas_market_calendars as pmc
pd.set_option('display.max_columns', None)


#%% Data analytics
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
    output_path1 = r"C:\Users\Bernardo Machado\OneDrive\Área de Trabalho\TCC\graficos\plot_curvas.png"
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
    output_path2 = r"C:\Users\Bernardo Machado\OneDrive\Área de Trabalho\TCC\graficos\plot_vertices.png"
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

#%% ACM
def run_acm(data):
    
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

    return acm, tp_df


#%% Run Functions - Plots, Stats and Run ACM
#Data
#df_curves = r"C:\Users\Bernardo Machado\OneDrive\Área de Trabalho\TCC\data_colection\curvas_b3.parquet"
df_curves = r"\\nas03\gestao_recursos\Pessoais\Bernardo\premia\curvas_b3.parquet"


# Plot
df = plot_data(df_curves)
# Stats 
stats_df = descriptive_stats(df, [1, 12, 36, 60, 120])

# Model ACM
acm, premia = run_acm(df_curves)


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

cumulative_normality = normality_test_cumulative_data(premia)


def plot_normality_frequency_percent():
    """
    Plota a frequência percentual de NORMAL vs NON_NORMAL por vértice (maturity)
    usando o DataFrame global cumulative_normality.
    """
    import pandas as pd
    import matplotlib.pyplot as plt

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
    
plot_normality_frequency_percent()
#%% - Stationary test ADF

# Teste de estacionariedade ADF (não cumulativo) para TODO o DataFrame 'premia'
# - Mantém o nome original do DF
# - Varre todas as colunas (vértices), qualquer que seja o tipo do rótulo (int/float/str)
# - Retorna um DataFrame longo com resultados por coluna

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller

def adf_test_full_all(
    df: pd.DataFrame,
    alpha: float = 0.05,
    regression: str = "c",   # "c", "ct", "nc", "ctt"
    autolag: str = "AIC"     # "AIC", "BIC", "t-stat" ou None
) -> pd.DataFrame:
    """
    Executa o teste ADF na SÉRIE COMPLETA (não cumulativo) para todas as colunas do DataFrame.

    Interpretação:
        p-value < alpha  -> "STATIONARY"
        p-value >= alpha -> "NON_STATIONARY"

    Parâmetros
    ----------
    df : pd.DataFrame
        Índice temporal (ex.: Date) e colunas = vértices (maturities).
    alpha : float
        Nível de significância para interpretação.
    regression : str
        Especificação da regressão no ADF ("c", "ct", "nc", "ctt").
    autolag : str
        Critério de seleção de lags no ADF.

    Retorno
    -------
    pd.DataFrame com colunas:
        - maturity          (rótulo original da coluna)
        - n_obs             (número de observações usadas)
        - ADF_stat
        - ADF_pvalue
        - ADF_lags
        - ADF_nobs
        - interpretation    ("STATIONARY", "NON_STATIONARY", "Não aplicável", "Falha no teste")
    """
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


# Exemplo de uso (mantendo nomes originais; sem criar variáveis auxiliares globais):
adf_full_all = adf_test_full_all(
    premia,
    alpha=0.05,
    regression="c",
    autolag="AIC"
)



#%% DI1

di = pd.read_parquet(r"\\nas03\gestao_recursos\Pessoais\Bernardo\premia\data_di1.parquet")
#di = pd.read_parquet(r"C:\Users\Bernardo Machado\OneDrive\Área de Trabalho\TCC\data_colection\data_di1.parquet")
di.drop(columns={'price_previous', 'change', 'settlement_value'}, inplace=True)

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

di = add_vencimento_e_dus(di)


####### !!!!!!!!!!!!!
"""FILTRO DI (SEMANAL) > PREMIO"""
premia.reset_index(inplace=True)
premia = premia[premia['Date'].isin(di['refdate'])]
premia["Date"] = pd.to_datetime(premia["Date"])
premia = premia.set_index("Date")



#%% HBOS

# HBOS (Histogram-Based Outlier Score) rolante para a coluna de 60 meses do DataFrame `premia`
# - Não executa nada automaticamente; apenas define a função solicitada.
# - Parâmetro `n` controla o número de observações na janela rolante.
# - A função retorna um DataFrame indexado por Date com as colunas:
#     ["value", "HBOS_score", "threshold", "is_outlier"]
# - Mantém o nome original do DF (`premia`) e não cria variáveis globais auxiliares.

def hbos_rolling(
    df: pd.DataFrame,
    n: int,
    bins: int = 20,
    binning: str = "quantile",
    q: float = 0.99,
    epsilon: float = 1e-6,
    col_label=None
) -> pd.DataFrame:
    """
    Calcula o HBOS (Histogram-Based Outlier Score) univariado em janela rolante e classifica o tipo
    de outlier ('max' ou 'min') com base nos quantis da janela. Remove as primeiras `n` observações
    por vértice (coluna) do DataFrame de retorno, que são naturalmente indisponíveis para o cálculo
    rolante (janelas iniciais).

    Visão geral do método:
    - Para cada janela de tamanho `n`, constrói-se um histograma dos valores válidos (não-NaN).
    - A densidade por bin é suavizada por `epsilon` e normalizada pela largura do bin.
    - O HBOS do último valor da janela é o negativo do log da densidade do bin em que ele cai.
    - O limiar (threshold) de outlier é o quantil `q` da distribuição de HBOS dentro da janela.
    - O tipo de outlier é rotulado como:
        * 'max' se o último valor da janela >= quantil `q` dos valores da janela;
        * 'min' se o último valor da janela <= quantil `1 - q` dos valores da janela;
        * None caso contrário.
    - As `n` primeiras observações por vértice são removidas do retorno para evitar linhas iniciais
      sem escore/limiar (efeito de borda da janela).

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame com índice temporal (ou ordenável) e colunas numéricas (cada coluna = um vértice).
    n : int
        Tamanho da janela rolante (ex.: 252).
    bins : int, padrão 20
        Número de bins do histograma em cada janela.
    binning : {"quantile", "uniform"}, padrão "quantile"
        Estratégia de construção dos bins:
        - "quantile": arestas dos bins pelos quantis igualmente espaçados da amostra da janela.
        - "uniform": arestas igualmente espaçadas entre min e max da amostra da janela.
    q : float, padrão 0.99
        Quantil usado para:
        - Definir o threshold do HBOS (quantil `q` do HBOS na janela).
        - Classificar outliers por valor (quantis `q` e `1 - q` dos valores da janela).
    epsilon : float, padrão 1e-6
        Suavização numérica para evitar densidades/lagaritmos nulos ou infinidades.
    col_label : hashable ou None, padrão None
        Se informado, aplica apenas à coluna especificada. Se None, aplica a todas as colunas
        e concatena os resultados com a coluna adicional 'maturity' indicando o vértice.

    Retorno
    -------
    pd.DataFrame
        Para `col_label=None`: DataFrame concatenado com todas as colunas (vértices),
        contendo:
            - value: valor original da série.
            - HBOS_score: escore HBOS do último ponto de cada janela.
            - threshold: quantil `q` do HBOS na janela (limiar de outlier).
            - is_outlier: booleano, True se HBOS_score > threshold.
            - outlier_type: "max", "min" ou None conforme classificação na janela.
            - maturity: nome da coluna (vértice).
        As primeiras `n` linhas de cada vértice são removidas do retorno.

        Para `col_label` definido: mesmo conjunto de colunas (sem 'maturity').
        As primeiras `n` linhas são removidas do retorno.

    Notas
    -----
    - O índice de `df` é ordenado antes do processamento por coluna.
    - Para binning "quantile", arestas iguais são desempatadas por um pequeno incremento (eps).
    - Quando a janela contém apenas NaN, o escore do ponto final permanece NaN.
    - Complexidade aproximada: O(T * (custo do histograma por janela)), onde T é o número de pontos.

    Exemplo
    -------
    >>> res = hbos_rolling(df, n=252, bins=20, binning="quantile", q=0.99)
    >>> res_single = hbos_rolling(df, n=252, col_label="DI1_252")
    """

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


# Exemplo de chamada para todas as colunas com janela n=126 (21*6)
hbos = hbos_rolling(
    df=premia,
    n=(52),           # tamanho da janela
    bins=20,
    binning="quantile",
    q=0.95,
    epsilon=1e-6,
    col_label=None   # None = processa todas as colunas
)

#%% Backteste # #### TESTE 1
# premio_sinal = resultado[resultado['maturity'] == 6]
# premio_sinal['maturity_days'] = premio_sinal['maturity'] * 21
# premio_sinal.dropna(subset='HBOS_score', inplace=True)


# premio_sinal.reset_index(inplace=True)
# premio_sinal.rename(columns={'Date':'refdate', }, inplace=True)

# ##### Merge di + sinal
# # garantir cópias e tipos corretos (evitar SettingWithCopyWarning)
# premio = premio_sinal.copy()
# di2 = di.copy()

# premio.loc[:, 'refdate'] = pd.to_datetime(premio['refdate'])
# di2.loc[:, 'refdate'] = pd.to_datetime(di2['refdate'])

# # índice original para recolocar depois
# premio = premio.reset_index(drop=False).rename(columns={'index': 'orig_idx'})

# # índice de dias de negociação disponíveis em di
# trading_dates = pd.DatetimeIndex(sorted(di2['refdate'].unique()))

# # mapear cada refdate do premio para a data de negociação mais próxima (com tolerância)
# tolerance = pd.Timedelta('252d')   # ajuste se quiser menor/maior
# pos = trading_dates.get_indexer(premio['refdate'], method='nearest', tolerance=tolerance)

# # pos == -1 significa sem correspondência dentro da tolerância
# premio['refdate_trading'] = pd.to_datetime([trading_dates[i] if i != -1 else pd.NaT for i in pos])

# # agora unir (many-to-many) premio com todas as linhas de di2 dessa data de negociação
# merged = premio.merge(
#     di2,
#     left_on='refdate_trading',
#     right_on='refdate',
#     how='left',
#     suffixes=('_premio', '_di')
# )

# # se quiser ver quantos ficaram sem data de negociação mapeada:
# n_no_trading = premio['refdate_trading'].isna().sum()
# print(f'Linhas sem data de negociação dentro de {tolerance}: {n_no_trading}')

# # calcular diferença absoluta de maturity_days e escolher por orig_idx a menor
# merged['maturity_diff'] = (merged['maturity_days_premio'].astype(float) - merged['maturity_days_di'].astype(float)).abs()

# # para grupos onde não há correspondência (todos NaN), vamos manter uma linha com NaN nos campos do di
# # selecionar idxmin com cuidado (ignorar grupos vazios)
# def pick_min_df(df):
#     if df['maturity_days_di'].isna().all():
#         # retorna primeira linha (com campos DI vazios) para manter a entrada do premio
#         return df.iloc[0]
#     else:
#         return df.loc[df['maturity_diff'].idxmin()]

# picked = merged.groupby('orig_idx', group_keys=False).apply(pick_min_df).reset_index(drop=True)

# # remontar DataFrame final (opcional: escolher colunas que quer manter)
# # ex: manter todas colunas do premio + colunas do di com sufixo _di
# cols_premio = [c for c in picked.columns if c.endswith('_premio') or c in ('orig_idx','refdate_premio','refdate_trading')]
# cols_di = [c for c in picked.columns if c.endswith('_di') or c in ('vencimento','maturity_days_di')]
# final = picked  # se preferir filtrar/reordenar, faça aqui

# # exemplo inspecionar discrepâncias grandes
# # calc % diferença entre maturity_days
# final['maturity_days_premio'] = final['maturity_days_premio'].astype(float)
# final['maturity_days_di'] = final['maturity_days_di'].astype(float)
# final['abs_diff'] = (final['maturity_days_premio'] - final['maturity_days_di']).abs()

# # Limpar colunas
# final = final[['refdate_premio', 'value', 'outlier_type', 'maturity', 'maturity_days_premio', 'symbol', 'price_previous', 'price', 'vencimento']]

# ### TRADING
# # Garantir ordenação temporal
# final = final.sort_values('refdate_premio').reset_index(drop=True)

# # Parâmetros
# contracts_per_trade = 10
# stop_loss_pct = 0.10  # 10%

# # Inicialização de colunas
# final['position'] = 0
# final['entry_price'] = np.nan
# final['max_pnl_since_entry'] = 0.0
# final['notional'] = 0.0
# final['pnl'] = 0.0
# final['pnl_cumsum'] = 0.0
# final['stop'] = False
# final['rollover'] = False  # nova flag
# final['symbol_prev'] = final['symbol'].shift(1)

# # Estados internos
# position = 0
# entry_price = np.nan
# pnl_cumsum = 0.0
# max_pnl_since_entry = 0.0
# symbol_atual = None

# for i in range(1, len(final)):
#     price_prev = final.loc[i - 1, 'price']
#     price_now = final.loc[i, 'price']
#     symbol_now = final.loc[i, 'symbol']
#     symbol_prev = final.loc[i - 1, 'symbol']

#     # PnL diário
#     pnl = position * (price_now - price_prev)
#     pnl_cumsum += pnl
#     final.loc[i, 'pnl'] = pnl
#     final.loc[i, 'pnl_cumsum'] = pnl_cumsum

#     # Detectar rolagem de contrato
#     if position != 0 and symbol_now != symbol_prev:
#         # PnL da rolagem = diferença entre contratos * -posição
#         pnl_roll = (price_now - price_prev) * (-position)
#         pnl_cumsum += pnl_roll
#         final.loc[i, 'pnl'] += pnl_roll  # adiciona ao PnL do dia
    
#         # Marca rolagem
#         final.loc[i, 'rollover'] = True
    
#         # Reabre no novo contrato (mesma direção)
#         entry_price = price_now
#         max_pnl_since_entry = 0.0
#         symbol_atual = symbol_now

#     # Se temos posição, verifica stop
#     if position != 0:
#         pnl_trade = (price_now - entry_price) * np.sign(position) * abs(position)
#         max_pnl_since_entry = max(max_pnl_since_entry, pnl_trade)

#         drawdown = (max_pnl_since_entry - pnl_trade) / (max_pnl_since_entry + 1e-9)
#         if drawdown >= stop_loss_pct:
#             position = 0
#             entry_price = np.nan
#             max_pnl_since_entry = 0.0
#             final.loc[i, 'stop'] = True

#     # Se não há posição, verifica sinal
#     if position == 0:
#         if final.loc[i, 'outlier_type'] == 'min':
#             position = contracts_per_trade
#             entry_price = price_now
#             symbol_atual = symbol_now
#         elif final.loc[i, 'outlier_type'] == 'max':
#             position = -contracts_per_trade
#             entry_price = price_now
#             symbol_atual = symbol_now

#     # Atualiza registros
#     final.loc[i, 'position'] = position
#     final.loc[i, 'entry_price'] = entry_price
#     final.loc[i, 'max_pnl_since_entry'] = max_pnl_since_entry
#     final.loc[i, 'notional'] = position * price_now
#     final.loc[i, 'pnl_cumsum'] = pnl_cumsum

# final.head(20)

#%% Função de trading v2


# def backtest(resultado, di, maturity_target=6, contracts_per_trade=10, stop_loss_pct=0.10, trading_tol='252d'):
#     """
#     Backtest de trading em DI usando sinais de prêmio.
    
#     Parâmetros:
#     - resultado: DataFrame com sinais (colunas: Date, value, HBOS_score, outlier_type, maturity)
#     - di: DataFrame com contratos DI (colunas: refdate, symbol, price, vencimento, maturity_days)
#     - maturity_target: maturidade em meses para filtrar prêmio
#     - contracts_per_trade: número de contratos por operação
#     - stop_loss_pct: stop em percentual
#     - trading_tol: tolerância para mapear refdate do prêmio ao DI
    
#     Retorna:
#     - final: DataFrame pronto para análise/trading
#     """
    
#     # ===== 1. Preparar sinais =====
#     premio_sinal = resultado[resultado['maturity'] == maturity_target].copy()
#     premio_sinal['maturity_days'] = premio_sinal['maturity'] * 21
#     premio_sinal.dropna(subset=['HBOS_score'], inplace=True)
    
#     if premio_sinal.empty:
#         raise ValueError(f"Nenhum sinal encontrado para maturity_target = {maturity_target}. O backtest não pode continuar.")
    
#     premio_sinal = premio_sinal.reset_index()
#     premio_sinal.rename(columns={'Date': 'refdate'}, inplace=True)
#     premio_sinal['orig_idx'] = premio_sinal.index
    
#     # ===== 2. Merge com DI =====
#     premio_sinal['refdate'] = pd.to_datetime(premio_sinal['refdate'])
#     di['refdate'] = pd.to_datetime(di['refdate'])
    
#     trading_dates = pd.DatetimeIndex(sorted(di['refdate'].unique()))
#     pos = trading_dates.get_indexer(premio_sinal['refdate'], method='nearest', tolerance=pd.Timedelta(trading_tol))
#     premio_sinal['refdate_trading'] = pd.to_datetime([trading_dates[i] if i != -1 else pd.NaT for i in pos])
    
#     merged = premio_sinal.merge(
#         di,
#         left_on='refdate_trading',
#         right_on='refdate',
#         how='left',
#         suffixes=('_premio', '_di')
#     )
    
#     merged['maturity_diff'] = (merged['maturity_days_premio'] - merged['maturity_days_di']).abs()
    
#     def pick_min_df(df):
#         if df['maturity_days_di'].isna().all():
#             return df.iloc[0]
#         else:
#             return df.loc[df['maturity_diff'].idxmin()]
    
#     picked = merged.groupby('orig_idx', group_keys=False).apply(pick_min_df).reset_index(drop=True)
    
#     picked.rename(columns={'maturity_days_di': 'maturity_days'}, inplace=True)
    
#     final_cols = ['refdate_premio', 'value', 'outlier_type', 'maturity', 'maturity_days',
#                   'symbol', 'price', 'vencimento']
#     picked.rename(columns={'refdate':'refdate_premio'}, inplace=True)
#     final = picked[final_cols].copy()
    
#     # ===== 3. Backtest =====
#     final = final.sort_values('refdate_premio').reset_index(drop=True)
    
#     # << CORREÇÃO DEFINITIVA AQUI: Preencher preços NaN com o último valor válido >>
#     final['price'].fillna(method='ffill', inplace=True)
    
#     # Inicializar colunas
#     final['position'] = 0
#     final['entry_price'] = np.nan
#     final['max_pnl_since_entry'] = 0.0
#     final['notional'] = 0.0
#     final['pnl'] = 0.0
#     final['pnl_cumsum'] = 0.0
#     final['stop'] = False
#     final['rollover'] = False
#     final['symbol_prev'] = final['symbol'].shift(1)
    
#     # Estados internos
#     position = 0
#     entry_price = np.nan
#     pnl_cumsum = 0.0
#     max_pnl_since_entry = 0.0
#     symbol_atual = None
    
#     for i in range(1, len(final)):
#         price_prev = final.loc[i - 1, 'price']
#         price_now = final.loc[i, 'price']
#         symbol_now = final.loc[i, 'symbol']
#         symbol_prev = final.loc[i - 1, 'symbol']
        
#         pnl = -position * (price_now - price_prev)
#         pnl_cumsum += pnl
        
#         if position != 0 and symbol_now != symbol_prev:
#             final.loc[i, 'rollover'] = True
#             entry_price = price_now
#             max_pnl_since_entry = 0.0
#             symbol_atual = symbol_now
            
#         if position != 0:
#             pnl_trade = -position * (price_now - entry_price)
#             max_pnl_since_entry = max(max_pnl_since_entry, pnl_trade)
            
#             if max_pnl_since_entry > 0:
#                 drawdown = (max_pnl_since_entry - pnl_trade) / max_pnl_since_entry
#                 if drawdown >= stop_loss_pct:
#                     position = 0
#                     entry_price = np.nan
#                     max_pnl_since_entry = 0.0
#                     final.loc[i, 'stop'] = True
        
#         if position == 0 and not final.loc[i, 'stop']:
#             if final.loc[i, 'outlier_type'] == 'min':
#                 position = contracts_per_trade
#                 entry_price = price_now
#                 symbol_atual = symbol_now
#             elif final.loc[i, 'outlier_type'] == 'max':
#                 position = -contracts_per_trade
#                 entry_price = price_now
#                 symbol_atual = symbol_now
        
#         final.loc[i, 'position'] = position
#         final.loc[i, 'entry_price'] = entry_price
#         final.loc[i, 'max_pnl_since_entry'] = max_pnl_since_entry
#         final.loc[i, 'notional'] = position * price_now
#         final.loc[i, 'pnl'] = pnl
#         final.loc[i, 'pnl_cumsum'] = pnl_cumsum
    
#     return final

# df_backtest = backtest(
#     resultado=resultado,  # Use seu DataFrame 'resultado' aqui
#     di=di,                # Use seu DataFrame 'di' aqui
#     maturity_target=6,
#     contracts_per_trade=10,
#     stop_loss_pct=0.10,
#     trading_tol='5d' # Reduzindo a tolerância para o exemplo
# )

#%% BACKTEST 3

"""
Backtest para o DI:
- Compra contrato no sinal Min
- Vende contrato no sinal Max
- Stop em drawdwown de 5%    
- Considerar rolagem dos contratos
"""

"Filtros Iniciais - iniciar com vertice de 6 meses"
sinal = hbos[hbos['maturity'] == 6]
sinal['maturity_days'] = 6 * 21

"MERGE DI + SINAL DE TRADING"

def join_sinal_di_nearest_maturity(sinal: pd.DataFrame, di: pd.DataFrame) -> pd.DataFrame:
    """
    Une `sinal` e `di` por data escolhendo, para cada Date de `sinal`, o contrato de `di`
    na mesma `refdate` cuja `maturity_days` é a mais próxima (diferença absoluta mínima)
    da `maturity_days` do `sinal`.

    Parâmetros
    ----------
    sinal : pd.DataFrame
        DataFrame indexado por 'Date' (DatetimeIndex) contendo, no mínimo, a coluna 'maturity_days'.
    di : pd.DataFrame
        DataFrame contendo, no mínimo, as colunas 'refdate' (datas) e 'maturity_days'.

    Retorno
    -------
    pd.DataFrame
        DataFrame indexado por 'Date' com todas as colunas originais de `sinal` e as colunas do
        contrato selecionado de `di`. Colunas com mesmo nome entre `sinal` e `di` são
        desambiguadas com sufixo `_di` no lado de `di`.
    """
    # Cópias para não alterar os objetos originais
    s = sinal.reset_index().copy()
    d = di.copy()

    # Normalização dos tipos de data (remove horário e garante datetime)
    s['Date'] = pd.to_datetime(s['Date']).dt.normalize()
    d['refdate'] = pd.to_datetime(d['refdate']).dt.normalize()

    # Junção por data (cada Date de s com todos os contratos de d na mesma refdate)
    merged = s.merge(
        d,
        left_on='Date',
        right_on='refdate',
        how='left',
        suffixes=('', '_di')
    )

    # Diferença absoluta em dias de maturidade (sinal vs di)
    # Observação: após o merge, a 'maturity_days' de `di` vira 'maturity_days_di'
    merged['abs_diff_maturity'] = (merged['maturity_days'] - merged['maturity_days_di']).abs()

    # Para datas sem match em `di`, o merge gera linha(s) com NaN; usar +inf para idxmin selecionar essa linha
    merged['abs_diff_maturity_filled'] = merged['abs_diff_maturity'].fillna(np.inf)

    # Seleciona, para cada Date, a linha com menor diferença (se houver empate, pega a primeira)
    best_idx = merged.groupby('Date', sort=False)['abs_diff_maturity_filled'].idxmin()
    out = merged.loc[best_idx].copy()

    # Limpeza de colunas auxiliares
    out.drop(columns=['abs_diff_maturity', 'abs_diff_maturity_filled'], inplace=True)

    # Define índice como 'Date'
    out.set_index('Date', inplace=True)

    return out

trading = join_sinal_di_nearest_maturity(sinal, di)


"DINAMICA DE TRADING: Compra e vende, rolagem e stop"

# Considera o contrato de rolagem
def add_previous_contract_columns(trading: pd.DataFrame, di: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona ao DataFrame `trading`:
      - 'previous_symbol': contrato antigo (imediatamente anterior) quando há troca;
      - 'previous_price_on_dt': preço do contrato antigo na mesma data (refdate == Date) da troca;
      - 'rolagem': True quando ambas as colunas acima estão preenchidas (não-NaN), False caso contrário.

    Pré-requisitos:
      * `trading` indexado por 'Date' (DatetimeIndex) e contendo: ['refdate','symbol','price'].
      * `di` contendo: ['refdate','symbol','price'].
    """
    tr = trading.copy().sort_index()
    tr['refdate'] = pd.to_datetime(tr['refdate']).dt.normalize()

    d = di.copy()
    d['refdate'] = pd.to_datetime(d['refdate']).dt.normalize()

    # Detecta a troca de contrato comparando com a linha anterior
    tr['previous_symbol'] = tr['symbol'].shift(1)

    # previous_symbol apenas quando houve troca; demais ficam NaN
    tr.loc[tr['symbol'] == tr['previous_symbol'], 'previous_symbol'] = pd.NA

    # Busca o preço do contrato anterior na mesma refdate da troca
    tmp = tr.reset_index()[['Date', 'refdate', 'previous_symbol']].rename(columns={'previous_symbol': 'symbol'})
    tmp = tmp[~tmp['symbol'].isna()].copy()

    prev_prices = tmp.merge(
        d[['refdate', 'symbol', 'price']],
        on=['refdate', 'symbol'],
        how='left'
    ).rename(columns={'price': 'previous_price_on_dt'})

    # Junta o preço anterior de volta ao trading
    tr = tr.reset_index().merge(
        prev_prices[['Date', 'symbol', 'previous_price_on_dt']].rename(columns={'symbol': 'previous_symbol'}),
        on=['Date', 'previous_symbol'],
        how='left'
    ).set_index('Date')

    # Coluna de rolagem: True se ambas as novas colunas estiverem preenchidas
    tr['rolagem'] = tr['previous_symbol'].notna() & tr['previous_price_on_dt'].notna()

    return tr
trading = add_previous_contract_columns(trading, di)

# Ajuste financeiro, considerando rolagem
trading['ajuste_financeiro'] = np.where(
    trading['rolagem'],
    trading['previous_price_on_dt'] - trading['price'].shift(1),
    trading['price'] - trading['price'].shift(1)
)


def build_trading_metrics(
    trading: pd.DataFrame,
    n_contratos: int = 1,
    stop_pct: float = 0.10
) -> pd.DataFrame:
    """
    Regras principais:
      - Sinais:
          * Compra (+n_contratos) se outlier_type == 'min'.
          * Venda  (-n_contratos) se outlier_type == 'max'.
          * Se já posicionado e vier sinal OPOSTO, vira a mão (±n -> ∓n) para o próximo dia.
          * Sinal no mesmo sentido apenas mantém (não soma contratos).
      - PnL diário (com base na posição vigente do próprio dia):
          * posicao > 0 (comprado):  pnl_dia = -ajuste_financeiro * |posicao|
          * posicao < 0 (vendido):    pnl_dia =  ajuste_financeiro * |posicao|
          * posicao == 0:             pnl_dia = 0
      - Stop por DRAWDOWN do trade:
          * pnl_acumulado_trade: soma dos pnl_dia desde a última entrada/virada (NaN quando flat).
          * peak_pnl_trade: MÁXIMO de pnl_acumulado_trade dentro do trade (reinicia a cada trade).
          * stop_alvo = peak_pnl_trade * (1 - stop_pct)  (somente se peak_pnl_trade > 0).
          * stop_flag = True no dia em que pnl_acumulado_trade <= stop_alvo.
          * O stop ajusta a posição do PRÓXIMO dia (zerando), a menos que haja sinal oposto no mesmo dia,
            caso em que a virada de mão prevalece (abre na direção do sinal no próximo dia).

    Pré-requisitos:
      * trading contém: ['price', 'ajuste_financeiro', 'outlier_type'] e índice temporal ('Date') ordenável.
    """
    df = trading.copy().sort_index()

    # Sanitização de tipos
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['ajuste_financeiro'] = pd.to_numeric(df['ajuste_financeiro'], errors='coerce').fillna(0.0)

    # Saídas
    posicao = []
    notional = []
    pnl_list = []
    pnl_acum_book_list = []
    pnl_acum_trade_list = []
    pnl_acum_trade_peak_list = []
    stop_alvo_list = []
    stop_flag_list = []

    # Estado
    pos = 0
    pnl_acum_book = 0.0

    # Estado do trade (reinicia em cada entrada/virada)
    pnl_acum_trade = 0.0
    peak_pnl_trade = 0.0

    size = int(abs(n_contratos))

    for dt, row in df.iterrows():
        price = row['price']
        ajuste = row['ajuste_financeiro']
        signal = row.get('outlier_type', None)

        # 1) PnL do dia com base na posição vigente
        if pos > 0:
            pnl_dia = -ajuste * abs(pos)
        elif pos < 0:
            pnl_dia =  ajuste * abs(pos)
        else:
            pnl_dia = 0.0

        pnl_acum_book += pnl_dia

        # 2) Atualiza métricas do trade corrente (somente quando pos != 0)
        if pos != 0:
            pnl_acum_trade += pnl_dia
            peak_pnl_trade = max(peak_pnl_trade, pnl_acum_trade)
            pnl_acum_trade_today = pnl_acum_trade
            peak_pnl_trade_today = peak_pnl_trade
        else:
            pnl_acum_trade_today = np.nan
            peak_pnl_trade_today = np.nan

        # 3) Calcula stop alvo (drawdown do trade: alvo = pico * (1 - stop_pct), apenas se pico > 0)
        if pos != 0 and peak_pnl_trade > 0:
            stop_alvo_val = peak_pnl_trade * (1.0 - float(stop_pct))
        else:
            stop_alvo_val = np.nan

        # 4) Determina a posição do PRÓXIMO dia
        next_pos = pos
        stop_flag_today = False

        # a) Regra de stop por drawdown do trade
        if pos != 0 and peak_pnl_trade > 0 and not np.isnan(pnl_acum_trade_today):
            if pnl_acum_trade_today <= stop_alvo_val:
                next_pos = 0
                stop_flag_today = True

        # b) Sinal de entrada/virada de mão
        desired_pos = None
        if signal == 'min':
            desired_pos = +size
        elif signal == 'max':
            desired_pos = -size

        if desired_pos is not None:
            if pos == 0 and next_pos == 0:
                # Estava zerado e não foi stopado -> entra no sentido do sinal
                next_pos = desired_pos
            elif pos != 0:
                # Já posicionado: se sinal oposto, vira a mão (sobrepõe stop para zero)
                if np.sign(desired_pos) != np.sign(pos):
                    next_pos = desired_pos
                # Sinal no mesmo sentido: mantém

        # 5) Registra outputs do dia (posição vigente do próprio dia)
        posicao.append(pos)
        notional.append(price * pos if pd.notna(price) else np.nan)
        pnl_list.append(pnl_dia)
        pnl_acum_book_list.append(pnl_acum_book)
        pnl_acum_trade_list.append(pnl_acum_trade_today)
        pnl_acum_trade_peak_list.append(peak_pnl_trade_today)
        stop_alvo_list.append(stop_alvo_val)
        stop_flag_list.append(bool(stop_flag_today))

        # 6) Avança estado para o próximo dia
        prev_pos = pos
        pos = next_pos

        # Novo trade começa em 0->±n ou virada de mão (±n -> ∓n) -> reseta métricas do trade
        if (prev_pos == 0 and pos != 0) or (prev_pos != 0 and pos != 0 and np.sign(prev_pos) != np.sign(pos)):
            pnl_acum_trade = 0.0
            peak_pnl_trade = 0.0
        # Encerramento para 0 (por stop ou ausência de sinal): mantém zerado até próxima entrada

    # Anexa colunas
    df['posicao'] = pd.Series(posicao, index=df.index, dtype='int64')
    df['notional'] = pd.Series(notional, index=df.index, dtype='float64')
    df['pnl'] = pd.Series(pnl_list, index=df.index, dtype='float64')
    df['pnl_acumulado'] = pd.Series(pnl_acum_book_list, index=df.index, dtype='float64')
    df['pnl_acumulado_trade'] = pd.Series(pnl_acum_trade_list, index=df.index, dtype='float64')
    df['pnl_acumulado_trade_peak'] = pd.Series(pnl_acum_trade_peak_list, index=df.index, dtype='float64')
    df['stop_alvo'] = pd.Series(stop_alvo_list, index=df.index, dtype='float64')
    df['stop_flag'] = pd.Series(stop_flag_list, index=df.index, dtype='bool')

    return df


trading_metric = build_trading_metrics(trading, 1, 0.01)
