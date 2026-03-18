pacman::p_load(
  gtfstools,
  dplyr,
  data.table,
  sf,
  mapview,
  osmdata,
  Hmisc,
  geobr,
  basemapR,
  pbapply,
  lubridate,
  bigrquery,
  stringr
)

ano_apurar <- "2025"
mes_apurar <- "10"

ano_gtfs <- "2026"
mes_gtfs <- "02"
quinzena_gtfs <- "02"

basedosdados::set_billing_id("rj-smtr")

endereco_gtfs <- paste0(
  "../../dados/gtfs/", ano_gtfs, "/sppo_",
  ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q.zip"
)

gtfs <- read_gtfs(endereco_gtfs) %>%
  filter_by_weekday("monday")

gtfs$shapes <- as.data.table(gtfs$shapes) %>%
  arrange(shape_id, shape_pt_sequence)

linhas <- gtfs$routes %>%
  filter(route_type == "200") %>%
  arrange() %>%
  pull(route_short_name)
  
data_ref <- as.Date(paste(ano_apurar, mes_apurar, "01", sep = "-"))

pasta_ano <- file.path("../../dados/viagens/frescao", ano_apurar)
if (!dir.exists(pasta_ano)) {
  dir.create(pasta_ano, recursive = TRUE)
}

pasta_mes <- file.path(pasta_ano, mes_apurar)
if (!dir.exists(pasta_mes)) {
  dir.create(pasta_mes)
}

linhas_lista <- paste(linhas, collapse = "','")

data_texto <- as.character(data_ref)
data_fim <- data_ref + months(1)

pasta_ano_gps <- file.path("../../dados/gps/frescao", ano_apurar)
if (!dir.exists(pasta_ano_gps)) {
  dir.create(pasta_ano_gps, recursive = TRUE)
}

pasta_mes_gps <- file.path(pasta_ano_gps, mes_apurar)
if (!dir.exists(pasta_mes_gps)) {
  dir.create(pasta_mes_gps)
}

data_fim_limite <- Sys.Date() - 2

gps_mes <- data.frame()

data <- data_ref

dias_loop <- seq(data_ref, data_fim, by = "day")

for (data in dias_loop) {
  arquivo_gps <- file.path(pasta_mes_gps, paste0("gps_frescao_", as_date(data), ".RDS"))
  
  if (file.exists(arquivo_gps)) {
    registros_gps <- readRDS(arquivo_gps)
    gps_mes <- bind_rows(gps_mes, registros_gps)
  } else {
    query_gps <- glue::glue(
      "SELECT timestamp_gps,id_veiculo,servico,latitude,longitude,tipo_parada 
        FROM `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` 
        WHERE data = '{as_date(data)}' AND servico IN ('{linhas_lista}')"
    )
    
    registros_gps <- basedosdados::read_sql(query_gps)
    
    saveRDS(registros_gps, arquivo_gps)
    
    gps_mes <- bind_rows(gps_mes, registros_gps)
  }
  
  if (data == data_fim_limite) {
    break 
  }
}

viagens_freq <- gtfs$frequencies %>%
  mutate(start_time = if_else(lubridate::hour(hms(start_time)) >= 24,
    hms(start_time) - lubridate::hours(24),
    hms(start_time)
  )) %>%
  mutate(end_time = if_else(lubridate::hour(hms(end_time)) >= 24,
    hms(end_time) - lubridate::hours(24),
    hms(end_time)
  )) %>%
  mutate(
    start_time = paste(sprintf("%02d", lubridate::hour(start_time)),
      sprintf("%02d", lubridate::minute(start_time)),
      sprintf("%02d", lubridate::second(start_time)),
      sep = ":"
    ),
    end_time = paste(sprintf("%02d", lubridate::hour(end_time)),
      sprintf("%02d", lubridate::minute(end_time)),
      sprintf("%02d", lubridate::second(end_time)),
      sep = ":"
    )
  ) %>%
  mutate(
    start_time = as.POSIXct(start_time,
      format = "%H:%M:%S", origin =
        "1970-01-01"
    ),
    end_time = as.POSIXct(end_time,
      format = "%H:%M:%S", origin =
        "1970-01-01"
    )
  ) %>%
  mutate(start_time = if_else(as.ITime(start_time) < as.ITime("02:00:00"),
    start_time + 86400,
    start_time
  )) %>%
  mutate(end_time = if_else(end_time < start_time,
    end_time + 86400,
    end_time
  )) %>%
  mutate(
    duracao = as.integer(difftime(end_time, start_time, units = "secs")),
    partidas = as.integer(duracao / headway_secs)
  )

trips_manter <- gtfs$trips %>%
  mutate(
    letras = stringr::str_extract(trip_short_name, "[A-Z]+"),
    numero = stringr::str_extract(trip_short_name, "[0-9]+")
  ) %>%
  tidyr::unite(., trip_short_name, letras, numero, na.rm = T, sep = "") %>%
  left_join(select(viagens_freq, trip_id, partidas)) %>%
  mutate(partidas = if_else(is.na(partidas), 1, partidas)) %>%
  group_by(shape_id) %>%
  mutate(ocorrencias = sum(partidas)) %>%
  ungroup() %>%
  group_by(route_id, direction_id) %>%
  slice_max(ocorrencias, n = 1) %>%
  ungroup() %>%
  distinct(shape_id, trip_short_name, .keep_all = T) %>%
  select(trip_id, trip_short_name, shape_id, direction_id)

gtfs <- filter_by_trip_id(gtfs, trips_manter$trip_id)

apuracao <- function(linha) {
  trips_filtrar <- gtfs$trips %>%
    filter(trip_short_name == linha) %>%
    group_by(direction_id) %>%
    slice(1) %>%
    ungroup() %>%
    select(trip_id) %>%
    unlist()

  gtfs_filt <- filter_by_trip_id(gtfs, trips_filtrar)

  shapes <- convert_shapes_to_sf(gtfs_filt) %>%
    left_join(select(gtfs_filt$trips, trip_short_name, shape_id, direction_id)) %>%
    mutate(extensao = st_length(.))

  shapes_tabela <- shapes %>%
    st_drop_geometry() %>%
    rename(servico = trip_short_name)

  pontos_usar <- gtfs_filt$stop_times %>%
    left_join(gtfs$trips) %>%
    filter(trip_short_name == linha) %>%
    filter(direction_id == "0") %>%
    filter(service_id == "U") %>%
    distinct(stop_sequence, .keep_all = T)

  primeiro_ponto_metro_buffer <- pontos_usar %>%
    filter(stop_sequence == "0") %>%
    left_join(gtfs$stops) %>%
    st_as_sf(
      coords = c("stop_lon", "stop_lat"),
      remove = F
    ) %>%
    st_set_crs(4326) %>%
    st_transform(31983) %>%
    st_buffer(150) %>%
    st_transform(4326)

  ultimo_ponto_metro_buffer <- pontos_usar %>%
    filter(stop_sequence == nrow(pontos_usar) - 1) %>%
    left_join(gtfs$stops) %>%
    st_as_sf(
      coords = c("stop_lon", "stop_lat"),
      remove = F
    ) %>%
    st_set_crs(4326) %>%
    st_transform(31983) %>%
    st_buffer(150) %>%
    st_transform(4326)

  registros_gps_linha <- gps_mes %>%
    filter(servico == linha)

  if (nrow(registros_gps_linha) > 0) {
    gps_sf <- registros_gps_linha %>%
      st_as_sf(coords = c("longitude", "latitude")) %>%
      st_set_crs(4326)

    gps_sf <- gps_sf %>%
      mutate(classificacao = case_when(
        sapply(st_within(., primeiro_ponto_metro_buffer), any) ~ "inicio",
        sapply(st_within(., ultimo_ponto_metro_buffer), any) ~ "final",
        TRUE ~ "meio"
      )) %>%
      group_by(id_veiculo) %>%
      arrange(timestamp_gps) %>%
      mutate(viagem = cumsum(classificacao != lag(classificacao, default = first(classificacao)))) %>%
      ungroup()

    gps_dt <- gps_sf %>%
      group_by(id_veiculo) %>%
      select(timestamp_gps, classificacao, viagem, id_veiculo) %>%
      st_drop_geometry() %>%
      as.data.table() %>%
      arrange(timestamp_gps) %>%
      distinct(id_veiculo, viagem, .keep_all = T) %>%
      group_by(id_veiculo) %>%
      arrange(viagem) %>%
      mutate(anterior = lag(classificacao)) %>%
      mutate(posterior = lead(classificacao)) %>%
      mutate(direction_id = case_when(
        anterior == "inicio" & posterior == "final" ~ 0,
        anterior == "final" & posterior == "inicio" ~ 1,
        TRUE ~ NA
      )) %>%
      filter(!is.na(direction_id)) %>%
      ungroup() %>%
      select(id_veiculo, viagem, direction_id)

    gps_sf <- gps_sf %>%
      ungroup() %>%
      left_join(gps_dt, by = c("id_veiculo", "viagem")) %>%
      left_join(shapes_tabela) %>%
      group_by(viagem, id_veiculo) %>%
      mutate(id_viagem = paste0(
        id_veiculo, "-", servico, "-",
        if_else(direction_id == "0", "I", "V"), "-", shape_id, "-",
        format(min(timestamp_gps), "%Y%m%d%H%M%S")
      )) %>%
      tidyr::fill(id_viagem) %>%
      ungroup() %>%
      arrange(id_veiculo, timestamp_gps) %>%
      group_by(id_veiculo) %>%
      mutate(
        datetime_partida = lag(timestamp_gps, default = first(timestamp_gps)),
        datetime_partida = if_else(viagem == 1, timestamp_gps, datetime_partida)
      ) %>%
      group_by(id_viagem) %>%
      mutate(datetime_partida = min(datetime_partida)) %>%
      ungroup() %>%
      group_by(id_veiculo) %>%
      mutate(
        datetime_chegada = lead(timestamp_gps, default = last(timestamp_gps)),
        datetime_chegada = if_else(viagem == n(), timestamp_gps, datetime_chegada)
      ) %>%
      group_by(id_viagem) %>%
      mutate(datetime_chegada = max(datetime_chegada)) %>%
      ungroup() %>%
      group_by(id_viagem) %>%
      mutate(tempo_viagem = as.integer(difftime(datetime_chegada, datetime_partida, units = "min"))) %>%
      mutate(velocidade_media = round((as.integer(extensao) / as.integer(
        difftime(datetime_chegada, datetime_partida, units = "secs")
      )) * 3.6, 2)) %>%
      mutate(n_registros = n()) %>%
      filter(n_registros > 10) %>%
      filter(!is.na(shape_id)) %>%
      ungroup()

    registros_em_garagem <- gps_sf %>%
      select(id_viagem, tipo_parada) %>%
      st_drop_geometry() %>%
      group_by(id_viagem) %>%
      filter(tipo_parada == "garagem") %>%
      summarise(registros_em_garagem = n())

    gps_sf <- gps_sf %>%
      left_join(registros_em_garagem) %>%
      mutate(registros_em_garagem = if_else(is.na(registros_em_garagem), 0, registros_em_garagem)) %>%
      mutate(percentual_registros_garagem = round((registros_em_garagem / n_registros) * 100, 2))

    distancia <- gps_sf %>%
      select(id_viagem, timestamp_gps) %>%
      dplyr::group_by(id_viagem) %>%
      summarise(do_union = FALSE) %>%
      sf::st_cast("LINESTRING") %>%
      mutate(distancia_percorrida = as.integer(st_length(geometry))) %>%
      st_drop_geometry()

    conformidade_registros <- gps_sf %>%
      select(id_viagem, timestamp_gps) %>%
      st_drop_geometry() %>%
      group_by(id_viagem) %>%
      mutate(hm = format(timestamp_gps, "%H:%M")) %>%
      distinct(hm, .keep_all = T) %>%
      summarise(qt_minutos_registros = n())

    buffer_ida <- shapes %>%
      filter(direction_id == 0) %>%
      st_transform(31983) %>%
      st_buffer(50)

    buffer_volta <- shapes %>%
      filter(direction_id == 0) %>%
      st_transform(31983) %>%
      st_buffer(50)

    conformidade_shape_ida <- gps_sf %>%
      select(direction_id, id_viagem) %>%
      filter(direction_id == 0) %>%
      mutate(shape = if_else(sapply(st_within(buffer_ida), any), T, F)) %>%
      filter(shape) %>%
      group_by(id_viagem) %>%
      summarise(qt_shapes_dentro = n()) %>%
      st_drop_geometry()

    conformidade_shape_volta <- gps_sf %>%
      select(direction_id, id_viagem) %>%
      filter(direction_id == 1) %>%
      mutate(shape = if_else(sapply(st_within(buffer_volta), any), T, F)) %>%
      filter(shape) %>%
      group_by(id_viagem) %>%
      summarise(qt_shapes_dentro = n()) %>%
      st_drop_geometry()

    viagens <- gps_sf %>%
      ungroup() %>%
      st_drop_geometry() %>%
      distinct(id_viagem, .keep_all = T) %>%
      st_drop_geometry() %>%
      select(-c(timestamp_gps, tipo_parada, classificacao, viagem)) %>%
      left_join(distancia) %>%
      left_join(conformidade_registros) %>%
      left_join(conformidade_shape_ida) %>%
      left_join(conformidade_shape_volta) %>%
      mutate(perc_conformidade_distancia = round(as.numeric((distancia_percorrida / extensao) * 100), 2)) %>%
      rename(
        distancia_planejada = extensao,
        distancia_aferida = distancia_percorrida
      ) %>%
      mutate(
        distancia_planejada = as.integer(distancia_planejada),
        distancia_aferida = as.integer(distancia_aferida)
      ) %>%
      mutate(perc_conformidade_registros = round((qt_minutos_registros / tempo_viagem) * 100, 2)) %>%
      mutate(perc_conformidade_registros = if_else(perc_conformidade_registros > 100, 100, perc_conformidade_registros)) %>%
      mutate(perc_conformidade_shape = round((qt_shapes_dentro / n_registros) * 100, 2)) %>%
      mutate(viagem_valida = case_when(
        perc_conformidade_registros < 50 ~ F,
        perc_conformidade_distancia < 30 ~ F,
        percentual_registros_garagem > 10 ~ F,
        perc_conformidade_shape < 80 ~ F,
        TRUE ~ T
      )) %>%
      mutate(data = as.character(as.Date(datetime_partida)))

    viagens_validas <- viagens %>%
      filter(viagem_valida == T)

    sumario_viagens <- viagens_validas %>%
      mutate(
        faixa_horaria = lubridate::hour(datetime_partida),
        tipo_dia = case_when(
          lubridate::wday(data) == "1" ~ "D",
          lubridate::wday(data) == "7" ~ "S",
          TRUE ~ "U"
        )
      ) %>%
      group_by(direction_id, faixa_horaria, data) %>%
      mutate(quantidade_viagens = n()) %>%
      group_by(direction_id, faixa_horaria, tipo_dia) %>%
      summarise(media_viagens = round(mean(quantidade_viagens))) %>%
      arrange(desc(tipo_dia), direction_id, faixa_horaria)

    pasta_viagens_brutas <- paste0(pasta_mes, "/brutas/")

    ifelse(!dir.exists(file.path(getwd(), pasta_viagens_brutas)),
      dir.create(file.path(getwd(), pasta_viagens_brutas), recursive = T), FALSE
    )

    pasta_viagens_validas <- paste0(pasta_mes, "/validas/")

    ifelse(!dir.exists(file.path(getwd(), pasta_viagens_validas)),
      dir.create(file.path(getwd(), pasta_viagens_validas), recursive = T), FALSE
    )

    end_linha_bruto <- paste0(pasta_viagens_brutas, "/linha-", linha, "_", data_texto)
    end_linha_validas <- paste0(pasta_viagens_validas, "/linha-", linha, "_", data_texto)
    end_linha <- paste0(pasta_mes, "/sumario_linha-", linha, "_", data_texto)

    end_viagens <- paste0(end_linha_bruto, ".csv")
    end_viagens_limpo <- paste0(end_linha_validas, ".csv")
    end_sumario <- paste0(end_linha, ".csv")

    fwrite(viagens, end_viagens)
    if (nrow(viagens_validas) > 0) {
      fwrite(viagens_validas, end_viagens_limpo)
    }
    fwrite(sumario_viagens, end_sumario)
    rm(registros_gps_linha)
    gc()
  }
  return(invisible(NULL))
}

pblapply(linhas, apuracao)