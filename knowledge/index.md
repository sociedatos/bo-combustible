---
type: dataset
title: Monitoreo y Abastecimiento de Combustible en Bolivia (ANH / YPFB)
dimensions:
  - departamento
  - distrito_comercial
  - estacion
  - producto
  - fecha
contracts:
  - type: datapackage
    path: ./datapackage.yaml
lineage:
  source:
    - name: Agencia Nacional de Hidrocarburos (ANH)
      url: https://abastecimiento.prod.anh.gob.bo
    - name: Yacimientos Petrolíferos Fiscales Bolivianos (YPFB)
      url: https://consulta.ypfb.gob.bo
  version: 2.1.0
  updated_at: '2026-10-01T00:00:00Z'
---

# Monitoreo y Abastecimiento de Combustible en Bolivia (ANH / YPFB)

Base de datos unificada sobre oferta mayorista, inventario en tanques de surtidores y logística de distribución de hidrocarburos líquidos (Gasolina Especial, Diésel Oil, Gasolina Premium, Diésel ULS) en Bolivia, integrando las fuentes oficiales de supervisión de la **Agencia Nacional de Hidrocarburos (ANH)** y de despacho de **Yacimientos Petrolíferos Fiscales Bolivianos (YPFB)**.

## Contexto y Fuentes Institucionales

La cadena de suministro de combustibles en Bolivia cuenta con dos niveles de supervisión digital:

1. **Despacho Mayorista y Logística (YPFB - `consulta.ypfb.gob.bo`):**
   - **`data_ypfb/despachos/*.csv`**: Registro detallado de cisternas despachadas desde plantas de almacenaje con placa de vehículo, volumen (L), estación receptora, timestamps y estado de confirmación SIGOPER.
   - **`data_ypfb/programacion/*.csv`**: Nominaciones y cuotas programadas por estación de servicio con bandera de cumplimiento (`atendida`).
   - **`data_ypfb/resumen/*.csv`**: Métricas agregadas por distrito comercial (programado, despachado, pendiente).
   - **Telemetría GPS**: Enlace con dispositivos telemáticos satelitales en ruta (`https://nominac.kyros-tech.com/api/gps/publico/{placa}`).

2. **Inventario Minorista y Disponibilidad en Surtidores (ANH - `abastecimiento.prod.anh.gob.bo`):**
   - **`data_abastecimiento/*.csv`**: Snapshots periódicos con volumen continuo (`saldo_litros`), clasificación cualitativa (`saldo_estado`), ventas activas (`con_venta`) y cisterna en descarga (`despacho_en_curso`).
   - **`data_discrete/*.csv`**: Registro histórico de estados discretos (semanas 33 a 39 de 2026).
   - **`data/*.csv`**: Serie histórica volumétrica semanal (marzo 2025 a agosto 2026).
   - **`stations.csv`**: Catálogo maestro georreferenciado de estaciones de servicio activas.

```ojs
const stationsData = await datamesh.query({
  resource_uri: "stations.csv",
  limit: 600
});

const deptoNames = {
  1: "Chuquisaca",
  2: "La Paz",
  3: "Cochabamba",
  4: "Oruro",
  5: "Potosí",
  6: "Tarija",
  7: "Santa Cruz",
  8: "Beni",
  9: "Pando"
};

const deptoIdx = stationsData.columns.indexOf("id_departamento");
const nombreIdx = stationsData.columns.indexOf("nombreEstacion");

const stations = stationsData.rows.map(r => ({
  departamento: deptoNames[r[deptoIdx]] || `Depto ${r[deptoIdx]}`,
  nombre: r[nombreIdx]
}));

return Plot.plot({
  title: "Distribución de Estaciones de Servicio por Departamento",
  subtitle: "Catálogo georreferenciado nacional (ANH)",
  x: { label: "Departamento", sort: { y: "-y" } },
  y: { label: "Número de Estaciones", grid: true },
  marks: [
    Plot.barY(stations, Plot.groupX({ y: "count" }, {
      x: "departamento",
      fill: "#0f9d58"
    })),
    Plot.ruleY([0])
  ]
});
```

## Estructura de Datasets

- **`data_ypfb/despachos/*.csv`**: Despachos de cisternas con placa, volumen, destino y confirmación logística.
- **`data_ypfb/programacion/*.csv`**: Cuotas asignadas por estación y verificación de entrega.
- **`data_ypfb/resumen/*.csv`**: Balances volumétricos distritales.
- **`data_abastecimiento/*.csv`**: Inventario volumétrico en tanques de surtidor, estado y geolocalización.
- **`data_discrete/*.csv`**: Niveles cualitativos semanales.
- **`data/*.csv`**: Telemedición histórica continua (2025-2026).
- **`stations.csv`**: Directorio georreferenciado de estaciones de servicio.

## Líneas de Investigación y Análisis

- **Cruce Oferta vs. Inventario:** Correlación entre despacho de cisternas de YPFB y recuperación de `saldo_litros` en la ANH.
- **Cuellos de Botella Logísticos:** Tiempos de tránsito entre salida de planta y recepción efectiva en estación.
- **Detección Temprana de Desabastecimiento:** Identificación de estaciones con nominaciones incumplidas (`atendida: false`) y saldo bajo simultáneo.
- **Monitoreo de Rutas y Flota:** Trazabilidad de cisternas mediante placas de despacho y telemetría GPS.
