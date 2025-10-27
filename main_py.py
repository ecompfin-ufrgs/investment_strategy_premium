#%% Libraries
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from pyacm import NominalACM


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


#%% Run Functions
#Data
df_curves = r"C:\Users\Bernardo Machado\OneDrive\Área de Trabalho\TCC\data_colection\curvas_b3.parquet"

# Plot
df = plot_data(df_curves)
# Stats 
stats_df = descriptive_stats(df, [1, 12, 36, 60, 120])

# Model ACM
acm, premia = run_acm(df_curves)
