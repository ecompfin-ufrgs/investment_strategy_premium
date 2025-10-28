#%% Libraries
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from pyacm import NominalACM
from scipy.stats import jarque_bera
import numpy as np
from scipy.stats import rankdata

#%% Functions
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

    return acm, tp_df


#%% Run Functions - Plots, Stats and Run ACM
#Data
df_curves = r"C:\Users\Bernardo Machado\OneDrive\Área de Trabalho\TCC\data_colection\curvas_b3.parquet"

# Plot
df = plot_data(df_curves)
# Stats 
stats_df = descriptive_stats(df, [1, 12, 36, 60, 120])

# Model ACM
acm, premia = run_acm(df_curves)

# Vertices semestrais
colunas_int = [int(c) for c in premia.columns]
semesters = [c for c in colunas_int if c % 6 == 0]
premia = premia[semesters]

#%% Normality test

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

#%% Percentiles

def cumulative_percentile(df):

    percentiles = pd.DataFrame(index=df.index, columns=df.columns, dtype=float)

    for col in df.columns:
        serie = df[col].dropna()
        pct_list = []

        for i in range(len(serie)):
            if i+1 < 252:  # apenas começar a calcular com pelo menos 30 observações
                pct_list.append(float('nan'))
                continue

            janela = serie.iloc[:i+1]  # todos os dados passados até a data atual
            obs = serie.iloc[i]

            # Percentil: (posição do valor na ordenação / tamanho da janela) * 100
            rank = rankdata(janela, method='average')  # rank dentro da janela
            pct = rank[-1] / len(janela) * 100  # percentil da observação atual
            pct_list.append(pct)

        percentiles[col] = pct_list

    return percentiles
 
percentis = cumulative_percentile(premia)

#%% IQR import pandas as pd

import matplotlib.pyplot as plt

# Parâmetros
col = 60         # vértice alvo
k = 1.5          # fator multiplicador do IQR
window = 756     # janela rolling (~3 anos úteis)

# Série alvo
serie = premia[col]

# Rolling quantis e IQR
q1_roll = serie.rolling(window=window, min_periods=window).quantile(0.25)
q3_roll = serie.rolling(window=window, min_periods=window).quantile(0.75)
iqr_roll = q3_roll - q1_roll
lower_roll = q1_roll - k * iqr_roll
upper_roll = q3_roll + k * iqr_roll

# Dummy 0/1 de outlier
outlier_dummy = ((serie < lower_roll) | (serie > upper_roll)).astype(int)
outlier_dummy.name = 'outlier_60'

# Juntar com a série original
premia_out = premia[[col]].copy()
premia_out['outlier_60'] = outlier_dummy

# --- Gráfico ---
plt.figure(figsize=(12, 6))
plt.plot(premia_out.index, premia_out[col], label=f'Prêmio {col} meses', color='steelblue')

# Limites IQR
plt.plot(lower_roll.index, lower_roll, color='gray', linestyle='--', linewidth=1, alpha=0.6, label='Limite inferior')
plt.plot(upper_roll.index, upper_roll, color='gray', linestyle='--', linewidth=1, alpha=0.6, label='Limite superior')

# Destacar outliers
plt.scatter(
    premia_out.index[premia_out['outlier_60'] == 1],
    premia_out[col][premia_out['outlier_60'] == 1],
    color='red', label='Outliers (IQR rolling 756)', zorder=5
)

plt.title(f'Outliers pelo IQR Rolling (756 observações) - Vértice {col} meses')
plt.xlabel('Data')
plt.ylabel('Prêmio')
plt.legend()
plt.tight_layout()

# Caminho de salvamento
path = r'C:\Users\Bernardo Machado\OneDrive\Área de Trabalho\TCC\outliers_IQR_60m.png'
plt.savefig(path, dpi=300)
plt.close()

print(f'Gráfico salvo em: {path}')


