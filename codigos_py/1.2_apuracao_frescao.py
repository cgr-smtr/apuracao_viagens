import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
import numpy as np
from datetime import datetime, timedelta
from google.cloud import bigquery
import basedosdados as bd
import zipfile
import io
from tqdm import tqdm
import warnings

warnings.filterwarnings('ignore')

# Configurações
ano_apurar = "2026"
mes_apurar = "03"

ano_gtfs = "2026"
mes_gtfs = "03"
estudo_gtfs = "05"  #ESTUDO, NÃO CONSIDERAR MAIS QUINZENA!!!!

bd.config.billing_id = "rj-smtr"
client = bigquery.Client(project="rj-smtr")

# Caminhos
endereco_gtfs = f"../../dados/gtfs/{ano_gtfs}/sppo_{ano_gtfs}-{mes_gtfs}-{estudo_gtfs}Q.zip"

def read_gtfs(path):
    """Lê arquivos essenciais do GTFS de um zip."""
    data = {}
    with zipfile.ZipFile(path, 'r') as z:
        for filename in ['shapes.txt', 'routes.txt', 'trips.txt', 'stop_times.txt', 'stops.txt', 'frequencies.txt']:
            if filename in z.namelist():
                data[filename.replace('.txt', '')] = pd.read_csv(z.open(filename), dtype={'shape_id': str, 'route_id': str, 'trip_id': str, 'stop_id': str})
    return data

gtfs = read_gtfs(endereco_gtfs)
gtfs['shapes'] = gtfs['shapes'].sort_values(['shape_id', 'shape_pt_sequence'])

# Filtro de linhas Frescão (route_type == 200)
linhas = gtfs['routes'][gtfs['routes']['route_type'].astype(str) == '200']['route_short_name'].unique()
linhas = np.sort(linhas)

data_ref = datetime.strptime(f"{ano_apurar}-{mes_apurar}-01", "%Y-%m-%d").date()

# Criar diretórios
pasta_ano = os.path.join("../../dados/viagens/frescao", ano_apurar)
os.makedirs(pasta_ano, exist_ok=True)

pasta_mes = os.path.join(pasta_ano, mes_apurar)
os.makedirs(pasta_mes, exist_ok=True)

linhas_lista = "','".join(linhas)

data_texto = data_ref.strftime("%Y-%m-%d")

if data_ref.month == 12:
    data_fim = data_ref.replace(year=data_ref.year + 1, month=1)
else:
    data_fim = data_ref.replace(month=data_ref.month + 1)

pasta_ano_gps = os.path.join("../../dados/gps/frescao", ano_apurar)
os.makedirs(pasta_ano_gps, exist_ok=True)

pasta_mes_gps = os.path.join(pasta_ano_gps, mes_apurar)
os.makedirs(pasta_mes_gps, exist_ok=True)

data_fim_limite = datetime.now().date() - timedelta(days=2)

gps_mes_list = []
current_date = data_ref
while current_date < data_fim:
    if current_date > data_fim_limite:
        break
    
    # R script indica gps_frescao_... RDS
    arquivo_gps = os.path.join(pasta_mes_gps, f"gps_frescao_{current_date}.parquet")
    
    if os.path.exists(arquivo_gps):
        registros_gps = pd.read_parquet(arquivo_gps)
        gps_mes_list.append(registros_gps)
    else:
        query_gps = f"""
            SELECT timestamp_gps, id_veiculo, servico, latitude, longitude, tipo_parada 
            FROM `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` 
            WHERE data = '{current_date}' AND servico IN ('{linhas_lista}')
        """
        registros_gps = bd.read_sql(query=query_gps, billing_project_id="rj-smtr")
        registros_gps.to_parquet(arquivo_gps)
        gps_mes_list.append(registros_gps)
    
    current_date += timedelta(days=1)

gps_mes = pd.concat(gps_mes_list, ignore_index=True) if gps_mes_list else pd.DataFrame()

# Processamento de frequências e trips (igual ao BRT)
def parse_hms(s):
    h, m, s = map(int, s.split(':'))
    return timedelta(hours=h, minutes=m, seconds=s)

viagens_freq = gtfs['frequencies'].copy()
viagens_freq['start_timedelta'] = viagens_freq['start_time'].apply(parse_hms)
viagens_freq['end_timedelta'] = viagens_freq['end_time'].apply(parse_hms)
viagens_freq['start_timedelta'] = viagens_freq['start_timedelta'].apply(lambda x: x - timedelta(hours=24) if x.total_seconds() >= 86400 else x)
viagens_freq['end_timedelta'] = viagens_freq['end_timedelta'].apply(lambda x: x - timedelta(hours=24) if x.total_seconds() >= 86400 else x)

base_date = datetime(1970, 1, 1)
viagens_freq['start_dt'] = viagens_freq['start_timedelta'].apply(lambda x: base_date + x)
viagens_freq['end_dt'] = viagens_freq['end_timedelta'].apply(lambda x: base_date + x)
viagens_freq['start_dt'] = viagens_freq['start_dt'].apply(lambda x: x + timedelta(days=1) if x.hour < 2 else x)
viagens_freq['end_dt'] = np.where(viagens_freq['end_dt'] < viagens_freq['start_dt'], viagens_freq['end_dt'] + timedelta(days=1), viagens_freq['end_dt'])

viagens_freq['duracao'] = (viagens_freq['end_dt'] - viagens_freq['start_dt']).dt.total_seconds()
viagens_freq['partidas'] = (viagens_freq['duracao'] / viagens_freq['headway_secs']).astype(int)

trips = gtfs['trips'].copy()
trips['letras'] = trips['trip_short_name'].str.extract(r'([A-Z]+)')
trips['numero'] = trips['trip_short_name'].str.extract(r'([0-9]+)')
trips['trip_short_name_clean'] = trips[['letras', 'numero']].fillna('').agg(''.join, axis=1)

trips_merged = trips.merge(viagens_freq[['trip_id', 'partidas']], on='trip_id', how='left')
trips_merged['partidas'] = trips_merged['partidas'].fillna(1)
trips_merged['ocorrencias'] = trips_merged.groupby('shape_id')['partidas'].transform('sum')

trips_manter = trips_merged.sort_values('ocorrencias', ascending=False).groupby(['route_id', 'direction_id']).head(1)
trips_manter = trips_manter.drop_duplicates(['shape_id', 'trip_short_name_clean'])
trip_ids_manter = trips_manter['trip_id'].tolist()

gtfs['trips'] = gtfs['trips'][gtfs['trips']['trip_id'].isin(trip_ids_manter)]
gtfs['stop_times'] = gtfs['stop_times'][gtfs['stop_times']['trip_id'].isin(trip_ids_manter)]

def apuracao(linha):
    try:
        trips_da_linha = gtfs['trips'][gtfs['trips']['trip_short_name'].str.contains(linha, na=False)]
        if trips_da_linha.empty: return None
        
        trips_filt = trips_da_linha.groupby('direction_id').head(1)
        trip_ids_filt = trips_filt['trip_id'].tolist()
        
        shapes_ids = trips_filt['shape_id'].tolist()
        shapes_df = gtfs['shapes'][gtfs['shapes']['shape_id'].isin(shapes_ids)].copy()
        
        geometry = shapes_df.groupby('shape_id').apply(lambda x: LineString(list(zip(x['shape_pt_lon'], x['shape_pt_lat']))))
        shapes_gdf = gpd.GeoDataFrame(geometry.rename('geometry'), crs="EPSG:4326")
        shapes_gdf = shapes_gdf.merge(trips_filt[['shape_id', 'direction_id', 'trip_short_name']], on='shape_id')
        shapes_gdf['extensao'] = shapes_gdf['geometry'].to_crs("EPSG:31983").length
        shapes_tabela = shapes_gdf.drop(columns='geometry').rename(columns={'trip_short_name': 'servico'})
        
        pontos_usar = gtfs['stop_times'][gtfs['stop_times']['trip_id'].isin(trip_ids_filt)]
        pontos_usar = pontos_usar.merge(gtfs['trips'][['trip_id', 'direction_id']], on='trip_id')
        pontos_usar = pontos_usar[pontos_usar['direction_id'].astype(str) == '0'].drop_duplicates('stop_sequence')
        
        if pontos_usar.empty: return None

        def get_buffer(seq):
            ponto = pontos_usar[pontos_usar['stop_sequence'] == seq].merge(gtfs['stops'], on='stop_id')
            if ponto.empty: return None
            gdf = gpd.GeoDataFrame(ponto, geometry=gpd.points_from_xy(ponto.stop_lon, ponto.stop_lat), crs="EPSG:4326")
            return gdf.to_crs("EPSG:31983").buffer(150).to_crs("EPSG:4326").unary_union

        primeiro_buffer = get_buffer(pontos_usar['stop_sequence'].min())
        ultimo_buffer = get_buffer(pontos_usar['stop_sequence'].max())
        
        reg_gps = gps_mes[gps_mes['servico'] == linha].copy()
        if reg_gps.empty: return None
        
        gps_gdf = gpd.GeoDataFrame(reg_gps, geometry=gpd.points_from_xy(reg_gps.longitude, reg_gps.latitude), crs="EPSG:4326")
        gps_gdf['classificacao'] = 'meio'
        if primeiro_buffer: gps_gdf.loc[gps_gdf.within(primeiro_buffer), 'classificacao'] = 'inicio'
        if ultimo_buffer: gps_gdf.loc[gps_gdf.within(ultimo_buffer), 'classificacao'] = 'final'
            
        gps_gdf = gps_gdf.sort_values(['id_veiculo', 'timestamp_gps'])
        gps_gdf['class_change'] = gps_gdf.groupby('id_veiculo')['classificacao'].shift() != gps_gdf['classificacao']
        gps_gdf['viagem'] = gps_gdf.groupby('id_veiculo')['class_change'].cumsum()
        
        gps_dt = gps_gdf.drop_duplicates(['id_veiculo', 'viagem']).copy()
        gps_dt = gps_dt.sort_values(['id_veiculo', 'timestamp_gps'])
        gps_dt['anterior'] = gps_dt.groupby('id_veiculo')['classificacao'].shift()
        gps_dt['posterior'] = gps_dt.groupby('id_veiculo')['classificacao'].shift(-1)
        
        gps_dt['direction_id_inferred'] = np.select(
            [(gps_dt['anterior'] == 'inicio') & (gps_dt['posterior'] == 'final'),
             (gps_dt['anterior'] == 'final') & (gps_dt['posterior'] == 'inicio')],
            [0, 1], default=np.nan
        )
        
        gps_gdf = gps_gdf.merge(gps_dt.dropna(subset=['direction_id_inferred'])[['id_veiculo', 'viagem', 'direction_id_inferred']], on=['id_veiculo', 'viagem'])
        gps_gdf = gps_gdf.merge(shapes_tabela, left_on='direction_id_inferred', right_on='direction_id', how='left')
        
        gps_gdf['timestamp_gps'] = pd.to_datetime(gps_gdf['timestamp_gps'])
        
        def make_id_viagem(group):
            start_time = group['timestamp_gps'].min()
            dir_label = "I" if group['direction_id_inferred'].iloc[0] == 0 else "V"
            return f"{group['id_veiculo'].iloc[0]}-{linha}-{dir_label}-{group['shape_id'].iloc[0]}-{start_time.strftime('%Y%m%d%H%M%S')}"

        ids_viagem = gps_gdf.groupby(['id_veiculo', 'viagem']).apply(make_id_viagem)
        gps_gdf = gps_gdf.merge(ids_viagem.rename('id_viagem'), on=['id_veiculo', 'viagem'])
        
        viagem_stats = gps_gdf.groupby('id_viagem').agg(
            datetime_partida=('timestamp_gps', 'min'),
            datetime_chegada=('timestamp_gps', 'max'),
            n_registros=('timestamp_gps', 'count')
        ).reset_index()
        viagem_stats['tempo_viagem_min'] = (viagem_stats['datetime_chegada'] - viagem_stats['datetime_partida']).dt.total_seconds() / 60
        
        gps_gdf = gps_gdf.merge(viagem_stats, on='id_viagem')
        gps_gdf = gps_gdf[(gps_gdf['n_registros'] > 10) & (gps_gdf['shape_id'].notna())]
        if gps_gdf.empty: return None

        gps_gdf['velocidade_media'] = (gps_gdf['extensao'] / ((gps_gdf['datetime_chegada'] - gps_gdf['datetime_partida']).dt.total_seconds())) * 3.6
        garagem = gps_gdf[gps_gdf['tipo_parada'] == 'garagem'].groupby('id_viagem').size().rename('registros_garagem')
        gps_gdf = gps_gdf.merge(garagem, on='id_viagem', how='left').fillna({'registros_garagem': 0})
        gps_gdf['perc_garagem'] = (gps_gdf['registros_garagem'] / gps_gdf['n_registros']) * 100
        
        distancia_percorrida = gps_gdf.groupby('id_viagem').apply(
            lambda g: gpd.GeoSeries([LineString(list(zip(g.geometry.x, g.geometry.y)))], crs="EPSG:4326").to_crs("EPSG:31983").length.iloc[0]
        ).rename('distancia_aferida')
        
        gps_gdf['hm'] = gps_gdf['timestamp_gps'].dt.strftime('%H:%M')
        minutos_reg = gps_gdf.groupby('id_viagem')['hm'].nunique().rename('qt_minutos_registros')
        
        buffer_shape_0 = shapes_gdf[shapes_gdf['direction_id'] == 0].to_crs("EPSG:31983").buffer(50).to_crs("EPSG:4326").unary_union
        buffer_shape_1 = shapes_gdf[shapes_gdf['direction_id'] == 1].to_crs("EPSG:31983").buffer(50).to_crs("EPSG:4326").unary_union
        
        shape_conform = gps_gdf.groupby('id_viagem').apply(
            lambda g: g.within(buffer_shape_0 if g['direction_id_inferred'].iloc[0] == 0 else buffer_shape_1).sum()
        ).rename('qt_shapes_dentro')
        
        viagens = gps_gdf.drop_duplicates('id_viagem').copy()
        viagens = viagens.merge(distancia_percorrida, on='id_viagem').merge(minutos_reg, on='id_viagem').merge(shape_conform, on='id_viagem')
        
        viagens['perc_conformidade_distancia'] = (viagens['distancia_aferida'] / viagens['extensao']) * 100
        viagens['perc_conformidade_registros'] = (viagens['qt_minutos_registros'] / viagens['tempo_viagem_min']).clip(upper=1) * 100
        viagens['perc_conformidade_shape'] = (viagens['qt_shapes_dentro'] / viagens['n_registros']) * 100
        
        viagens['viagem_valida'] = (viagens['perc_conformidade_registros'] >= 50) & (viagens['perc_conformidade_distancia'] >= 30) & (viagens['perc_garagem'] <= 10) & (viagens['perc_conformidade_shape'] >= 80)
        viagens['data'] = viagens['datetime_partida'].dt.date
        
        viagens_validas = viagens[viagens['viagem_valida']]
        if not viagens_validas.empty:
            viagens_validas['faixa_horaria'] = viagens_validas['datetime_partida'].dt.hour
            viagens_validas['tipo_dia'] = viagens_validas['datetime_partida'].dt.weekday.map(lambda w: 'D' if w == 6 else ('S' if w == 5 else 'U'))
            sumario_final = viagens_validas.groupby(['direction_id', 'faixa_horaria', 'data']).size().reset_index(name='qtd')
            sumario_final = sumario_final.merge(viagens_validas[['data', 'tipo_dia']].drop_duplicates(), on='data')
            sumario_final = sumario_final.groupby(['direction_id', 'faixa_horaria', 'tipo_dia'])['qtd'].mean().round().reset_index(name='media_viagens')
        else:
            sumario_final = pd.DataFrame()

        pasta_brutas = os.path.join(pasta_mes, "brutas")
        pasta_validas = os.path.join(pasta_mes, "validas")
        os.makedirs(pasta_brutas, exist_ok=True)
        os.makedirs(pasta_validas, exist_ok=True)
        
        viagens.to_csv(os.path.join(pasta_brutas, f"linha-{linha}_{data_texto}.csv"), index=False)
        if not viagens_validas.empty: viagens_validas.to_csv(os.path.join(pasta_validas, f"linha-{linha}_{data_texto}.csv"), index=False)
        sumario_final.to_csv(os.path.join(pasta_mes, f"sumario_linha-{linha}_{data_texto}.csv"), index=False)
        
    except Exception as e:
        print(f"Erro na linha {linha}: {e}")

for linha in tqdm(linhas):
    apuracao(linha)
