# Análisis de Logs Distribuidos

Análisis exploratorio de un sistema de logging distribuido para identificar incidentes, servicios afectados y comportamiento anómalo usando Python y Pandas.

## Objetivo

Detectar el momento crítico del sistema, diagnosticar qué servicio y endpoint fueron los más afectados, y comparar el incidente contra el comportamiento base (baseline).

## Estructura

```
├── recursos/
│   └── server_logs.csv       # Dataset de logs (no incluido en el repo)
├── analisis_logs.ipynb       # Notebook principal con el análisis completo
├── requirements.txt
└── README.md
```

## Instalación

```bash
pip install -r requirements.txt
```

## Uso

Colocá el archivo `server_logs.csv` dentro de la carpeta `recursos/` y ejecutá el notebook de arriba a abajo.

## Dataset

Campos principales: `timestamp_event`, `service_name`, `severity`, `message`, `endpoint`, `status_code`, `latency_ms`, `trace_id`.

## Definiciones

- **Bad event:** `severity` es `ERROR` o `CRITICAL`, o `status_code >= 500`
- **Momento crítico:** ventana de 5 minutos con mayor `bad_rate` (mínimo 20 eventos)
- **Baseline:** todo el dataset fuera del momento crítico
