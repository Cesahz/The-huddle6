import pandas as pd

df = pd.read_csv('recursos/server_logs.csv')
df['timestamp_event'] = pd.to_datetime(df['timestamp_event'])
df['is_bad'] = df['severity'].isin(['ERROR', 'CRITICAL']) | (df['status_code'] >= 500)

print("Dataset cargado:", df.shape)
print("=" * 60)


# ================================================================
# EJERCICIO 1 — groupby básico + size()
# ¿Cuántos eventos generó cada servicio?
# ================================================================
# agrupar df por 'service_name'
# contar cuántas filas hay en cada grupo con .size()
# ordenar de mayor a menor
# mostrar resultado
eventos_por_servicio = df.groupby('service_name').size().sort_values(ascending=False)

resultado_1 = eventos_por_servicio 

print("\nEJERCICIO 1 — Eventos por servicio:")
print(resultado_1)


# ================================================================
# EJERCICIO 2 — groupby + agg() con múltiples métricas
# Por cada servicio, calcular: total de eventos, latencia promedio,
# latencia máxima y cantidad de bad events
# ================================================================
# agrupar df por 'service_name'
# usar .agg() con:
#   total_eventos  → contar filas ('timestamp_event', 'count')
#   lat_promedio   → promedio de 'latency_ms'
#   lat_maxima     → máximo de 'latency_ms'
#   bad_events     → suma de 'is_bad'
# resetear índice
# ordenar por bad_events descendente

eventos_por_servicio = df.groupby('service_name').agg(
    total_eventos=('timestamp_event','count'),
    lat_promedio=('latency_ms','mean'),
    lat_maxima=('latency_ms', 'max'),
    bad_events=('is_bad','sum')
)
eventos_por_servicio.reset_index()
eventos_por_servicio = eventos_por_servicio.sort_values(by='bad_events',ascending=False)
resultado_2 = eventos_por_servicio  

print("\nEJERCICIO 2 — Métricas por servicio:")
print(resultado_2)


# ================================================================
# EJERCICIO 3 — groupby con pd.Grouper (ventanas de tiempo)
# ¿Cuántos eventos hubo por hora?
# ================================================================
# agrupar df usando pd.Grouper(key='timestamp_event', freq='H')
# contar eventos por hora con .size()
# renombrar la serie a 'total_eventos'
# mostrar solo las 5 horas con más tráfico
eventos_por_tiempo = df.groupby([pd.Grouper(key='timestamp_event',freq='h')])
total_eventos = eventos_por_tiempo.size()
top_eventos = total_eventos.sort_values(ascending=False).head(5)

resultado_3 = top_eventos  # reemplazá con tu código

print("\nEJERCICIO 3 — Top 5 horas con más tráfico:")
print(resultado_3)


# ================================================================
# EJERCICIO 4 — groupby con múltiples columnas + unstack()
# ¿Cuántos eventos bad hubo por servicio y por severidad?
# ================================================================
# filtrar df para quedarte solo con bad events (is_bad == True)
# agrupar por ['service_name', 'severity']
# contar con .size()
# usar .unstack(fill_value=0) para pivotear severity como columnas
# mostrar la tabla resultante

df_bad = df[df['is_bad']==True]
df_bad = df_bad.groupby(['service_name','severity'])
cantidad_df_bad = df_bad.size().unstack(fill_value=0)
resultado_4 = cantidad_df_bad  

print("\nEJERCICIO 4 — Bad events por servicio y severidad:")
print(resultado_4)


# ================================================================
# EJERCICIO 5 — groupby + agg() + calcular columna derivada
# Por cada endpoint, calcular bad_rate
# (solo ventanas con más de 10 eventos)
# ================================================================
# agrupar df por 'endpoint'
# agregar: total_eventos (count) y bad_events (sum de is_bad)
# resetear índice
# calcular columna 'bad_rate' = bad_events / total_eventos
# filtrar filas con total_eventos > 10
# ordenar por bad_rate descendente
# mostrar top 10

df_end = df.groupby('endpoint').agg(
    total_eventos=('timestamp_event','count'),
    bad_events=('is_bad','sum')
)
df_end = df_end.reset_index()
df_end['bad_rate'] = df_end['bad_events'] / df_end['total_eventos']
df_end = df_end.sort_values(by='bad_rate',ascending=False).head(10)

resultado_5 = df_end  # reemplazá con tu código

print("\nEJERCICIO 5 — Top 10 endpoints por bad_rate:")
print(resultado_5)


# ================================================================
# EJERCICIO 7 — groupby + Grouper de 5min + múltiples métricas
# (el que usaste en el challenge, ahora dominalo vos solo)
# Construir analisis_temporal desde cero:
# por cada ventana de 5 min → total, bad_events, avg_latency, bad_rate
# mostrar el top 5 por bad_rate (con filtro de >= 20 eventos)
# ================================================================
# agrupar df con pd.Grouper(key='timestamp_event', freq='5min')
# agregar: total_events, bad_events, avg_latency_ms
# calcular bad_rate
# filtrar total_events >= 20
# mostrar top 5 por bad_rate

bins_5m = df.groupby([pd.Grouper(key='timestamp_event',freq='5min')]).agg(
    total_events=('timestamp_event','count'),
    bad_events=('is_bad','sum'),
    avg_latency_ms=('latency_ms','mean')
)
bins_5m['bad_rate'] = bins_5m['bad_events'] / bins_5m['total_events']
bins_5m = bins_5m[bins_5m['total_events'] >= 20]
top_5 = bins_5m.sort_values(by='bad_rate', ascending=False).head(5)
resultado_7 = top_5  # reemplazá con tu código

print("\nEJERCICIO 7 — Top 5 momentos críticos:")
print(resultado_7)


# ================================================================
# REFERENCIA RÁPIDA — parámetros de groupby que usaste o vas a usar
# ================================================================
#
# df.groupby('col')                     → agrupa por una columna
# df.groupby(['col1', 'col2'])          → agrupa por múltiples columnas
# df.groupby(pd.Grouper(key='ts', freq='5min'))  → agrupa por ventana de tiempo
# df.groupby([pd.Grouper(...), 'col'])  → tiempo + columna a la vez
#
# Después del groupby:
# .size()                → cuenta filas por grupo (devuelve Serie)
# .sum()                 → suma por grupo (solo columnas numéricas)
# .mean()                → promedio por grupo
# .agg(nombre=('col', 'funcion'))  → múltiples métricas a la vez
# .agg({'col': ['mean','max']})    → múltiples funciones por columna
#
# Funciones válidas dentro de agg():
# 'count', 'sum', 'mean', 'median', 'max', 'min', 'std', 'nunique'
#
# Post-procesamiento:
# .reset_index()         → convierte el índice (grupos) en columnas normales
# .unstack(fill_value=0) → pivotea el último nivel del índice como columnas
# .sort_values(by='col', ascending=False)  → ordenar
# .head(N) / .nlargest(N, 'col')           → top N
#
# transform() — diferencia clave con agg():
# .agg()       → colapsa filas, devuelve una fila por grupo
# .transform() → mantiene el mismo tamaño del df original, una fila por evento
