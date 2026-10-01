---
type: dataset
title: Monitoreo y Saldos de Combustible en Bolivia (ANH)
dimensions:
  - departamento
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
  version: 2.0.0
  updated_at: '2026-10-01T00:00:00Z'
---

# Monitoreo y Saldos de Combustible en Bolivia (ANH)

Base de datos temporal sobre inventario, nivel de abastecimiento y eventos de venta de hidrocarburos líquidos (Gasolina Especial, Diésel Oil, Gasolina Premium, Diésel ULS) en las estaciones de servicio reguladas de Bolivia, originada en los sistemas de supervisión de la Agencia Nacional de Hidrocarburos (ANH).

## Contexto y Fuentes Institucionales

La Agencia Nacional de Hidrocarburos (ANH) recopila periódicamente telemedición y reportes de volumen de tanques mediante el sistema B-SISA (Boliviana de Sistemas de Autoidentificación) y plataformas de supervisión en línea:

1. **Portal de Abastecimiento Web (`data_abastecimiento/*.csv`):** Plataforma web pública (`abastecimiento.prod.anh.gob.bo`) que reporta saldos volumétricos continuos (`saldo_litros`), clasificación cualitativa (`saldo_estado`), banderas de ventas activas (`con_venta`), despachos de cisternas en curso (`despacho_en_curso`, `seguimiento_id`) y coordenadas geográficas.
2. **API Móvil Discreta (`data_discrete/*.csv`):** Capturas del servicio móvil v2 implementadas en el periodo de transición a variables cualitativas (bajo, medio, alto) sin reporte directo de litros exactos.
3. **Serie Histórica Continua (`data/*.csv`):** Registro de saldos continuos en litros por telemedición (Octano, BSA y Planta) capturados entre marzo de 2025 y agosto de 2026.
4. **Catálogo Maestro de Estaciones (`stations.csv`):** Directorio georreferenciado con identificadores de balance, razón social, dirección y departamento.

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
      fill: "#2563eb"
    })),
    Plot.ruleY([0])
  ]
});
```

## Estructura de Datasets

- **`data_abastecimiento/*.csv`**: Archivos semanales (`YYYYWW.csv`) generados mediante el recolector automatizado que integran saldo volumétrico en litros, nivel cualitativo, despacho de cisternas y geolocalización.
- **`data_discrete/*.csv`**: Archivos semanales con estados discretos de saldo de combustible (`alto`, `medio`, `bajo`) y estado de venta en surtidores.
- **`data/*.csv`**: Serie histórica volumétrica semanal (`202511.csv` a `202631.csv`) con telemedición desglosada por sistema.
- **`stations.csv`**: Maestro de referencia espacial y administrativo de más de 590 estaciones en los 9 departamentos.

## Líneas de Investigación y Análisis

- **Monitoreo de Desabastecimiento:** Detección de quiebres de stock en tiempo real y frecuencia de estaciones con saldo bajo o nulo por producto.
- **Logística y Cisternas:** Análisis de tiempos de reposición mediante la correlación entre `despacho_en_curso` y recuperación de `saldo_litros`.
- **Patrones de Venta y Colas:** Identificación de estaciones críticas mediante marcas temporales de `fecha_ultima_venta` y persistencia de despacho.
- **Disparidad Territorial:** Evaluación de concentración y resiliencia energética entre ejes metropolitanos y municipios rurales o fronterizos.
