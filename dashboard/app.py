"""UrbanFlow - Dashboard de movilidad urbana y predicción de demanda de taxis.

Lee los resultados que el pipeline PySpark generó en el clúster de AWS
(carpeta resultados/) y los presenta de forma interactiva.
Ejecutar: .venv\\Scripts\\python -m streamlit run dashboard\\app.py
"""
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

RAIZ = Path(__file__).resolve().parent.parent
RES = RAIZ / "resultados"
DIAS = {1: "Domingo", 2: "Lunes", 3: "Martes", 4: "Miércoles", 5: "Jueves", 6: "Viernes", 7: "Sábado"}
ORDEN_DIAS = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
COLOR = "#F5B700"  # amarillo taxi
TINTA = "#E6EDF7"  # texto claro del tema oscuro (.streamlit/config.toml)
st.set_page_config(page_title="UrbanFlow", page_icon=":material/local_taxi:", layout="wide")


@st.cache_data
def csv(nombre, **kw):
    ruta = RES / nombre
    return pd.read_csv(ruta, **kw) if ruta.exists() else None


@st.cache_data
def js(nombre):
    ruta = RES / nombre
    return json.loads(ruta.read_text(encoding="utf-8-sig")) if ruta.exists() else None


@st.cache_data
def geo():
    return json.loads((Path(__file__).parent / "taxi_zones.geojson").read_text(encoding="utf-8"))


def falta(nombre):
    st.info("Aún no hay resultados de «{}». Ejecuta el pipeline: .\\urbanflow.ps1 procesar".format(nombre))


def mapa(df, columna, titulo, escala="YlOrRd", formato=":,.1f", **kw):
    fig = px.choropleth_map(df, geojson=geo(), locations="zona_origen", color=columna,
                            hover_name="zona", hover_data={"borough": True, columna: formato, "zona_origen": False},
                            color_continuous_scale=escala, map_style="carto-darkmatter",
                            center={"lat": 40.715, "lon": -73.93}, zoom=9.4, opacity=0.78, labels={columna: titulo}, **kw)
    fig.update_layout(height=620, margin=dict(l=0, r=0, t=0, b=0))
    return fig


@st.cache_data
def centros():
    """Centro aproximado de cada zona: promedio del contorno de su polígono más grande."""
    salida = {}
    for f in geo()["features"]:
        g = f["geometry"]
        poligonos = g["coordinates"] if g["type"] == "MultiPolygon" else [g["coordinates"]]
        anillo = max((p[0] for p in poligonos), key=len)
        salida[int(f["id"])] = (sum(c[1] for c in anillo) / len(anillo), sum(c[0] for c in anillo) / len(anillo))
    return salida


def si_no(valor):
    return "Sí" if valor else "No"


kpis, etl, modelo_m = js("kpis.json"), js("etl_resumen.json"), js("metricas_modelo.json")
ejec = js("ejecucion.json") or {}
zonas = csv("zonas_total.csv")
nombres = zonas[["zona_origen", "zona", "borough"]] if zonas is not None else None

st.title(":material/local_taxi: UrbanFlow")
st.caption("Análisis de movilidad urbana y predicción de demanda de taxis en Nueva York con Apache Spark "
           "(PySpark, Spark SQL, MLlib, GraphX y Structured Streaming) sobre un clúster Hadoop/YARN en AWS.")
if kpis is None:
    falta("indicadores")
    st.stop()

t_resumen, t_patrones, t_mapa, t_pred, t_sql, t_grafo, t_spark = st.tabs(
    [":material/dashboard: Resumen", ":material/schedule: Patrones", ":material/map: Mapa de demanda",
     ":material/model_training: Predicción", ":material/database: Spark SQL", ":material/hub: Grafos (GraphX)",
     ":material/memory: Spark por dentro"])

# ---------------------------------------------------------------- Resumen
with t_resumen:
    c = st.columns(5)
    c[0].metric("Registros crudos", "{:,.0f}".format(etl["registros_crudos"]))
    c[1].metric("Viajes válidos analizados", "{:,.0f}".format(etl["registros_validos"]),
                "-{:.1f} % descartado".format(etl["porcentaje_descartado"]), delta_color="off")
    c[2].metric("Periodo", "{} días".format(kpis["dias"]), "{} → {}".format(kpis["fecha_inicial"], kpis["fecha_final"]),
                delta_color="off")
    c[3].metric("Viajes promedio por día", "{:,.0f}".format(kpis["viajes_promedio_dia"]))
    c[4].metric("Zonas con actividad", kpis["zonas_con_viajes"])
    c = st.columns(4)
    c[0].metric("Distancia media", "{:.2f} mi".format(kpis["distancia_media_mi"]))
    c[1].metric("Tarifa media", "US$ {:.2f}".format(kpis["tarifa_media_usd"]))
    c[2].metric("Duración media", "{:.1f} min".format(kpis["duracion_media_min"]))
    c[3].metric("Recaudo total", "US$ {:,.0f} M".format(kpis["recaudo_total_usd"] / 1e6))

    izq, der = st.columns(2)
    calidad = csv("calidad_datos.csv")
    if calidad is not None:
        motivos = ["fuera_del_mes", "distancia_invalida", "tarifa_invalida", "duracion_invalida", "zona_desconocida"]
        largo = calidad.melt(id_vars="mes", value_vars=motivos, var_name="motivo", value_name="registros")
        largo["motivo"] = largo["motivo"].str.replace("_", " ").str.capitalize()
        fig = px.bar(largo, x="mes", y="registros", color="motivo", barmode="group",
                     title="Calidad de datos: registros que incumplen cada regla (un viaje puede incumplir varias)")
        izq.plotly_chart(fig, use_container_width=True)
    tiempos = csv("tiempos_pipeline.csv", header=None, names=["paso", "segundos"])
    if tiempos is not None:
        tiempos = tiempos.drop_duplicates("paso", keep="last")
        fig = px.bar(tiempos, x="segundos", y="paso", orientation="h", text="segundos",
                     title="Tiempo de cada etapa del pipeline en el clúster (segundos)", color_discrete_sequence=[COLOR])
        fig.update_yaxes(categoryorder="array", categoryarray=list(tiempos["paso"])[::-1])
        der.plotly_chart(fig, use_container_width=True)
    st.markdown("**Infraestructura:** {} · región {} · meses procesados: {}".format(
        ejec.get("cluster", "-"), ejec.get("region", "-"), ", ".join(etl["meses"])))

# ---------------------------------------------------------------- Patrones
with t_patrones:
    heat = csv("heatmap_dia_hora.csv")
    if heat is not None:
        heat["dia"] = heat["dia_semana"].map(DIAS)
        matriz = heat.pivot(index="dia", columns="hora", values="viajes_promedio").reindex(ORDEN_DIAS)
        fig = px.imshow(matriz, aspect="auto", color_continuous_scale="YlOrRd",
                        labels=dict(x="Hora del día", y="", color="Viajes promedio"),
                        title="¿Cuándo hay más demanda? Viajes promedio por día de la semana y hora")
        fig.update_layout(height=420)
        st.plotly_chart(fig, use_container_width=True)
    izq, der = st.columns(2)
    dia = csv("viajes_por_dia.csv", parse_dates=["fecha"])
    if dia is not None:
        dia["media_7_dias"] = dia["viajes"].rolling(7, center=True).mean()
        fig = go.Figure([go.Scatter(x=dia["fecha"], y=dia["viajes"], name="Viajes por día", line=dict(color="#5B6B82")),
                         go.Scatter(x=dia["fecha"], y=dia["media_7_dias"], name="Media móvil 7 días", line=dict(color=COLOR, width=3))])
        fig.update_layout(title="Viajes por día", height=380, legend=dict(orientation="h", y=-0.2))
        izq.plotly_chart(fig, use_container_width=True)
    bh = csv("borough_hora.csv")
    if bh is not None:
        bh = bh[~bh["borough"].isin(["Unknown", "N/A", "EWR"])]
        fig = px.line(bh, x="hora", y="viajes", color="borough", markers=True, title="Viajes por hora en cada borough")
        fig.update_layout(height=380, legend=dict(orientation="h", y=-0.25))
        der.plotly_chart(fig, use_container_width=True)
    vn = csv("viernes_noche_top.csv")
    if vn is not None:
        fig = px.bar(vn.sort_values("promedio_por_viernes"), x="promedio_por_viernes", y="zona", orientation="h",
                     color="borough", text="promedio_por_viernes",
                     title="¿Dónde se piden más taxis los viernes por la noche? (20:00–23:59, promedio por viernes)")
        fig.update_layout(height=520)
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------- Mapa
with t_mapa:
    opcion = st.radio("Mostrar en el mapa", ["Viajes promedio por día", "Pronóstico para mañana a una hora"], horizontal=True)
    if opcion == "Viajes promedio por día":
        st.plotly_chart(mapa(zonas, "viajes_promedio_dia", "Viajes/día"), use_container_width=True)
        st.caption("Las zonas de Manhattan y los aeropuertos (JFK, LaGuardia) concentran la mayor parte de los viajes.")
    else:
        man = csv("prediccion_manana.csv")
        if man is None:
            falta("predicción")
        else:
            hora = st.slider("Hora", 0, 23, 8, format="%d:00")
            st.plotly_chart(mapa(man[man["hora"] == hora], "viajes_esperados", "Viajes esperados", "Viridis"),
                            use_container_width=True)
            st.caption("Viajes que el modelo espera el {} a las {}:00 en cada zona.".format(man["fecha"].iloc[0], hora))

# ---------------------------------------------------------------- Predicción
with t_pred:
    if modelo_m is None:
        falta("modelo")
    else:
        st.markdown("**Modelo:** {algoritmo} · {arboles} árboles · profundidad {profundidad} · {zonas_activas} zonas · "
                    "entrenamiento {filas_entrenamiento:,} filas hasta {entrenamiento_hasta} · prueba {filas_prueba:,} filas "
                    "({prueba_desde} → {prueba_hasta}) · entrenado en {segundos_entrenamiento:.0f} s".format(**modelo_m))
        tabla = pd.DataFrame(modelo_m["metricas"]).T.rename(columns={"rmse": "RMSE", "mae": "MAE", "r2": "R²"})
        izq, der = st.columns([1, 1])
        izq.markdown("**Evaluación en los 14 días de prueba (datos que el modelo nunca vio)**")
        mejor_celda = "rgba(245, 183, 0, 0.28)"  # amarillo translúcido: legible con texto claro
        izq.dataframe(tabla.style.format("{:.3f}").highlight_min(subset=["RMSE", "MAE"], color=mejor_celda)
                      .highlight_max(subset=["R²"], color=mejor_celda), use_container_width=True)
        mejor = modelo_m.get("mejor_modelo", "Random Forest (MLlib)")
        rf, base = modelo_m["metricas"][mejor], modelo_m["metricas"]["Línea base: promedio histórico"]
        mejora = 100 * (1 - rf["rmse"] / base["rmse"])
        if mejora > 0:
            izq.success("{} reduce el error (RMSE) un {:.0f} % frente al promedio histórico y explica el {:.0f} % "
                        "de la variación de la demanda (R²).".format(mejor, mejora, 100 * rf["r2"]))
        else:
            izq.warning("{} no supera al promedio histórico en esta ejecución (RMSE {:.0f} % mayor).".format(mejor, -mejora))
        imp = csv("importancia_variables.csv")
        if imp is not None:
            fig = px.bar(imp.sort_values("importancia"), x="importancia", y="variable", orientation="h",
                         title="¿Qué variables usa más el modelo?", color_discrete_sequence=[COLOR])
            der.plotly_chart(fig, use_container_width=True)

        pred = csv("predicciones_prueba.csv", parse_dates=["fecha"])
        if pred is not None:
            pred["momento"] = pred["fecha"] + pd.to_timedelta(pred["hora"], unit="h")
            pred = pred.merge(nombres, on="zona_origen", how="left")
            col_m = "pred_modelo" if "pred_modelo" in pred.columns else "pred_rf"
            nombre_m = mejor.replace(" (MLlib)", "")
            ciudad = pred.groupby("momento")[["viajes", col_m, "pred_perfil"]].sum().reset_index()
            fig = go.Figure([go.Scatter(x=ciudad["momento"], y=ciudad["viajes"], name="Real", line=dict(color=TINTA)),
                             go.Scatter(x=ciudad["momento"], y=ciudad[col_m], name=nombre_m, line=dict(color=COLOR, width=2)),
                             go.Scatter(x=ciudad["momento"], y=ciudad["pred_perfil"], name="Promedio histórico", line=dict(color="#7aa6c2", dash="dot"))])
            fig.update_layout(title="Toda la ciudad: viajes reales vs predichos por hora (periodo de prueba)", height=400,
                              legend=dict(orientation="h", y=-0.2))
            st.plotly_chart(fig, use_container_width=True)
            opciones = (pred.groupby(["zona_origen", "zona", "borough"])["viajes"].sum().sort_values(ascending=False)
                        .reset_index())
            etiqueta = opciones.apply(lambda r: "{} ({})".format(r["zona"], r["borough"]), axis=1).tolist()
            elegida = st.selectbox("Zona", etiqueta, index=0)
            zid = int(opciones.iloc[etiqueta.index(elegida)]["zona_origen"])
            z = pred[pred["zona_origen"] == zid]
            fig = go.Figure([go.Scatter(x=z["momento"], y=z["viajes"], name="Real", line=dict(color=TINTA)),
                             go.Scatter(x=z["momento"], y=z[col_m], name=nombre_m, line=dict(color=COLOR, width=2))])
            fig.update_layout(title="{}: real vs predicho".format(elegida), height=360, legend=dict(orientation="h", y=-0.25))
            st.plotly_chart(fig, use_container_width=True)

        man = csv("prediccion_manana.csv")
        if man is not None:
            st.subheader("¿Cuántos viajes habrá mañana ({}) en una zona y hora?".format(man["fecha"].iloc[0]))
            a, b = st.columns([2, 1])
            zonas_m = sorted(man["zona"].dropna().unique())
            zona_q = a.selectbox("Zona de origen", zonas_m, index=zonas_m.index("JFK Airport") if "JFK Airport" in zonas_m else 0)
            hora_q = b.select_slider("Hora", options=list(range(24)), value=8, format_func=lambda h: "{:02d}:00".format(h))
            dz = man[man["zona"] == zona_q]
            valor = float(dz[dz["hora"] == hora_q]["viajes_esperados"].iloc[0])
            a.metric("Viajes esperados en {} a las {:02d}:00".format(zona_q, hora_q), "≈ {:.0f}".format(valor))
            fig = px.bar(dz, x="hora", y="viajes_esperados", title="Perfil previsto de {} para mañana".format(zona_q),
                         color_discrete_sequence=[COLOR])
            fig.add_vline(x=hora_q, line_dash="dot")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Las predicciones representan viajes registrados esperados, no la demanda insatisfecha.")

# ---------------------------------------------------------------- Spark SQL
with t_sql:
    sq = js("spark_sql.json")
    if sq is None:
        falta("Spark SQL")
    else:
        cons = sq["consultas"]
        st.markdown("Los Parquet de HDFS se registran como **vistas temporales** (`viajes`, `zonas`) en el catálogo de "
                    "Spark SQL y se consultan con SQL estándar sobre los {:,} viajes. Catalyst optimiza el SQL igual que un "
                    "DataFrame.".format(etl["registros_validos"]))
        top = csv("sql_top_zonas_por_hora.csv")
        if "top_zonas_por_hora" in cons and top is not None:
            c = cons["top_zonas_por_hora"]
            st.subheader("Función de ventana: ¿qué zona lidera cada hora del día?")
            izq, der = st.columns([3, 2])
            fig = px.bar(top[top["puesto"] == 1], x="hora", y="viajes", color="zona", hover_data=["borough"],
                         title="Zona n.º 1 de cada hora (viajes en todo el periodo)")
            fig.update_layout(height=430, legend=dict(orientation="h", y=-0.25))
            izq.plotly_chart(fig, use_container_width=True)
            der.code(c["sql"], language="sql")
            der.caption("{} filas en {} s en el clúster.".format(c["filas"], c["segundos"]))
            with st.expander("Las 3 primeras zonas de cada hora (resultado completo)"):
                st.dataframe(top, hide_index=True, use_container_width=True)
        cor = csv("sql_corredores.csv")
        if "corredores" in cons and cor is not None:
            c = cons["corredores"]
            st.subheader("JOIN doble: los corredores origen → destino más usados")
            izq, der = st.columns([3, 2])
            cor["corredor"] = cor["origen"] + " → " + cor["destino"]
            fig = px.bar(cor.sort_values("viajes"), x="viajes", y="corredor", orientation="h", color="velocidad_mph",
                         color_continuous_scale="RdYlGn",
                         hover_data=["distancia_media_mi", "duracion_media_min", "tarifa_media_usd"],
                         title="Los 15 corredores con más viajes (color: velocidad media, mph)")
            fig.update_layout(height=520)
            izq.plotly_chart(fig, use_container_width=True)
            der.code(c["sql"], language="sql")
            der.caption("{} filas en {} s en el clúster.".format(c["filas"], c["segundos"]))
        fr = csv("sql_franjas.csv")
        if "franjas" in cons and fr is not None:
            c = cons["franjas"]
            st.subheader("CASE: ¿a qué hora se mueve más lento la ciudad?")
            fr["franja"] = fr["franja"].str[2:]
            izq, der = st.columns([3, 2])
            fig = px.bar(fr, x="franja", y="velocidad_mph", color="tipo_dia", barmode="group", text="velocidad_mph",
                         hover_data=["viajes", "tarifa_media_usd", "distancia_media_mi"],
                         color_discrete_sequence=["#9aa5b1", COLOR], labels={"franja": "", "tipo_dia": ""},
                         title="Velocidad media de los taxis por franja horaria (mph)")
            fig.update_layout(height=430, legend=dict(orientation="h", y=-0.2))
            izq.plotly_chart(fig, use_container_width=True)
            der.code(c["sql"], language="sql")
        eq = sq.get("sql_vs_dataframe")
        if eq:
            st.subheader("SQL y DataFrame: dos lenguajes, un mismo plan")
            izq, der = st.columns(2)
            izq.markdown("**Spark SQL**")
            izq.code(eq["sql"], language="sql")
            der.markdown("**API de DataFrames (PySpark)**")
            der.code(eq["dataframe"], language="python")
            c = st.columns(4)
            c[0].metric("Mismo plan optimizado (Catalyst)", si_no(eq["mismo_plan_optimizado"]))
            c[1].metric("Mismo plan físico", si_no(eq["mismo_plan_fisico"]))
            c[2].metric("Mismo resultado", si_no(eq["mismo_resultado"]), "{} filas".format(eq["filas"]), delta_color="off")
            c[3].metric("Tiempo SQL / DataFrame", "{} s / {} s".format(min(eq["tiempos_sql_s"]), min(eq["tiempos_dataframe_s"])))
            if eq["mismo_plan_optimizado"]:
                st.caption("Catalyst traduce las dos versiones al mismo plan, así que rinden igual: elegir SQL o DataFrame "
                           "es cuestión de legibilidad, no de velocidad.")
            plan = RES / "plan_spark_sql.txt"
            if plan.exists():
                with st.expander("Plan completo de la consulta SQL (analizado, optimizado y físico)"):
                    st.code(plan.read_text(encoding="utf-8"), language="text")

# ---------------------------------------------------------------- Grafos
with t_grafo:
    gr, gz = js("grafo_resumen.json"), csv("grafo_zonas.csv")
    if gr is None or gz is None:
        falta("grafos")
    else:
        st.markdown("La ciudad como **grafo dirigido**: cada zona es un vértice y cada par origen → destino con viajes es "
                    "una arista con peso. **GraphX** solo tiene API en Scala/Java; **GraphFrames** lo expone en Python sobre "
                    "DataFrames, y su PageRank y su Label Propagation ejecutan los algoritmos de `org.apache.spark.graphx.lib`.")
        c = st.columns(5)
        c[0].metric("Vértices (zonas)", gr["vertices"])
        c[1].metric("Aristas origen → destino", "{:,}".format(gr["aristas"]),
                    "densidad {:.0%}".format(gr["densidad"]), delta_color="off")
        c[2].metric("Corredores fuertes", "{:,}".format(gr["corredores_fuertes"]),
                    "≥ {} viajes (1 al día)".format(gr["umbral_corredor_fuerte"]), delta_color="off")
        c[3].metric("Viajes en corredores fuertes", "{:.1f} %".format(gr["porcentaje_viajes_en_corredores_fuertes"]))
        c[4].metric("Comunidades (Label Propagation)", len(gr["comunidades"]))
        if "troncal_k" in gr:
            st.caption("PageRank y Label Propagation de GraphX no usan pesos, y cada zona tiene en promedio unos {:.0f} "
                       "corredores fuertes; por eso se ejecutan sobre el grafo troncal: los {} destinos principales de cada "
                       "zona ({:,} aristas). PageRank responde: ¿a qué zonas van, como destino principal, las zonas "
                       "importantes?".format(gr["corredores_fuertes"] / gr["zonas_con_corredor_fuerte"], gr["troncal_k"],
                                             gr["troncal_aristas"]))

        gz = gz.rename(columns={"id": "zona_origen"})
        nombre_com = {x["comunidad"]: "Comunidad {}".format(i + 1) for i, x in enumerate(gr["comunidades"])}
        vista = st.radio("Colorear el mapa por", ["PageRank (zonas centrales)", "Balance: llegan − salen",
                                                  "Comunidades (Label Propagation)"], horizontal=True)
        if vista.startswith("PageRank"):
            fig = mapa(gz.dropna(subset=["pagerank"]), "pagerank", "PageRank", formato=":.3f")
        elif vista.startswith("Balance"):
            limite = float(gz["balance"].abs().quantile(0.95))
            fig = mapa(gz, "balance", "Llegan − salen", "RdBu", formato=":,.0f",
                       color_continuous_midpoint=0, range_color=(-limite, limite))
        else:
            # Colores distintos solo para las 8 comunidades más grandes; el resto, en gris.
            gc = gz.dropna(subset=["comunidad"]).copy()
            grandes = {x["comunidad"]: "{} ({} zonas)".format(nombre_com[x["comunidad"]], x["zonas"])
                       for x in gr["comunidades"][:8]}
            otras = "Otras comunidades (más pequeñas)"
            gc["grupo"] = gc["comunidad"].astype("int64").map(grandes).fillna(otras)
            orden = list(grandes.values()) + [otras]
            fig = mapa(gc, "grupo", "Comunidad", formato=True, category_orders={"grupo": orden},
                       color_discrete_map=dict(zip(orden, px.colors.qualitative.Bold[:len(grandes)] + ["#4A5568"])))
        corr = csv("grafo_corredores.csv")
        if corr is not None and st.checkbox("Dibujar los 20 corredores con más viajes", value=True):
            cen, top20 = centros(), corr.head(20)
            maximo = top20["viajes"].max()
            for _, r in top20.iterrows():
                a, b = cen.get(int(r["src"])), cen.get(int(r["dst"]))
                if a and b:
                    fig.add_trace(go.Scattermap(lat=[a[0], b[0]], lon=[a[1], b[1]], mode="lines", showlegend=False,
                                                line=dict(width=1 + 7 * r["viajes"] / maximo, color="rgba(255,255,255,0.85)"),
                                                hoverinfo="text", text="{} → {}: {:,} viajes".format(
                                                    r["zona_origen"], r["zona_destino"], int(r["viajes"]))))
        st.plotly_chart(fig, use_container_width=True)
        if vista.startswith("Balance"):
            st.caption("Azul: llegan más viajes de los que salen (destinos). Rojo: salen más de los que llegan (orígenes). "
                       "Calculado con aggregateMessages: cada arista envía su número de viajes a su origen y a su destino.")

        izq, der = st.columns(2)
        top_pr = gz.dropna(subset=["pagerank"]).nlargest(12, "pagerank")
        fig = px.bar(top_pr.sort_values("pagerank"), x="pagerank", y="zona", color="borough", orientation="h",
                     title="PageRank (GraphX): las 12 zonas más centrales de la red")
        # Con color por borough cada borough es una serie; sin esto, el orden se agrupa por serie.
        fig.update_yaxes(categoryorder="total ascending")
        fig.update_layout(height=430, legend=dict(orientation="h", y=-0.2))
        izq.plotly_chart(fig, use_container_width=True)
        bal = pd.concat([gz.nlargest(6, "balance"), gz.nsmallest(6, "balance")])
        fig = px.bar(bal.sort_values("balance"), x="balance", y="zona", orientation="h", color="balance",
                     color_continuous_scale="RdBu", color_continuous_midpoint=0,
                     title="aggregateMessages: zonas que más reciben (+) y más emiten (−)")
        fig.update_layout(height=430)
        der.plotly_chart(fig, use_container_width=True)

        iv = csv("grafo_ida_vuelta.csv")
        if iv is not None:
            st.subheader("Búsqueda de patrones: (a)-[ida]->(b); (b)-[vuelta]->(a)")
            st.markdown("**{:,}** pares de zonas tienen un corredor fuerte en los dos sentidos. Los más usados:".format(
                gr["pares_ida_y_vuelta"]))
            st.dataframe(iv.rename(columns={"zona_a": "Zona A", "borough_a": "Borough A", "zona_b": "Zona B",
                                            "borough_b": "Borough B", "viajes_a_b": "A → B", "viajes_b_a": "B → A",
                                            "viajes_total": "Total", "asimetria_pct": "Asimetría %"}),
                         hide_index=True, use_container_width=True)
        com = pd.DataFrame(gr["comunidades"])
        if not com.empty:
            st.subheader("Comunidades detectadas con Label Propagation (GraphX)")
            com.insert(0, "Comunidad", [nombre_com[x] for x in com["comunidad"]])
            com["Composición"] = com["por_borough"].apply(
                lambda d: ", ".join("{} {}".format(v, k) for k, v in sorted(d.items(), key=lambda x: -x[1])))
            st.dataframe(com.head(10)[["Comunidad", "zonas", "borough_principal", "Composición"]].rename(
                columns={"zonas": "Zonas", "borough_principal": "Borough principal"}), hide_index=True, use_container_width=True)
            st.caption("Las 10 comunidades más grandes de {}. Label Propagation no conoce los boroughs: agrupa las zonas "
                       "solo por cómo las conectan los viajes.".format(len(com)))
        st.caption("Tiempos en el clúster: grafo {construccion_s} s · aggregateMessages {aggregate_messages_s} s · "
                   "PageRank {pagerank_s} s · patrones {motifs_s} s · Label Propagation {label_propagation_s} s.".format(
                       **gr["tiempos"]))

# ---------------------------------------------------------------- Spark por dentro
with t_spark:
    comp = js("comparacion_rdd_df.json")
    if comp is not None:
        st.subheader("RDD vs DataFrame: la misma pregunta, dos formas de pedírsela a Spark")
        izq, der = st.columns([1, 1])
        fig = px.bar(x=["RDD (lambdas)", "DataFrame (API estructurada)"], y=[comp["mejor_rdd_s"], comp["mejor_dataframe_s"]],
                     text=[comp["mejor_rdd_s"], comp["mejor_dataframe_s"]], color=["RDD", "DataFrame"],
                     color_discrete_sequence=["#9aa5b1", COLOR], labels={"x": "", "y": "segundos"},
                     title="{} sobre {:,} viajes".format(comp["consulta"], comp["registros"]))
        fig.update_layout(showlegend=False)
        izq.plotly_chart(fig, use_container_width=True)
        der.markdown(
            "**Resultado:** el DataFrame fue **{:.1f}× más rápido** y ambos dieron el mismo resultado ({}).\n\n"
            "- Con **RDD**, Spark solo recibe lambdas de Python: no sabe que es un promedio, no puede optimizar y "
            "cada fila viaja serializada como objeto Python entre la JVM y Python.\n"
            "- Con **DataFrame**, Spark entiende la intención (`groupBy` + `avg`): **Catalyst** optimiza el plan y "
            "**Tungsten** ejecuta código generado sobre datos binarios, sin salir de la JVM.".format(
                comp["aceleracion"], "sí" if comp["mismo_resultado"] else "no"))
        tph = pd.DataFrame({"hora": [int(h) for h in comp["tarifa_por_hora"]], "tarifa": list(comp["tarifa_por_hora"].values())})
        der.plotly_chart(px.line(tph, x="hora", y="tarifa", markers=True, title="Tarifa promedio por hora (resultado)",
                                 color_discrete_sequence=[COLOR]).update_layout(height=260), use_container_width=True)
    plan = (RES / "plan_catalyst.txt")
    if plan.exists():
        st.subheader("Optimizador Catalyst: el filtro se escribió después del join y Spark lo movió antes")
        texto = plan.read_text(encoding="utf-8")
        partes = {}
        for titulo in ["== Analyzed Logical Plan ==", "== Optimized Logical Plan ==", "== Physical Plan =="]:
            if titulo in texto:
                partes[titulo] = texto.split(titulo)[1].split("\n== ")[0].strip()
        izq, der = st.columns(2)
        izq.markdown("**Plan lógico analizado** (como se escribió)")
        izq.code(partes.get("== Analyzed Logical Plan ==", "")[:3000], language="text")
        der.markdown("**Plan lógico optimizado** (filtro empujado hacia la lectura)")
        der.code(partes.get("== Optimized Logical Plan ==", "")[:3000], language="text")
        with st.expander("Plan físico completo (busca PushedFilters en la lectura del Parquet)"):
            st.code(partes.get("== Physical Plan ==", ""), language="text")
    prog = js("streaming_progreso.json")
    if prog is not None:
        st.subheader("Structured Streaming: ventanas de tiempo de evento con watermark")
        st.markdown("Se simuló la llegada en vivo de los viajes del **viernes {}** (16:00–23:59): un archivo por hora, "
                    "ventanas de **{}** y watermark de **{}**. Al final llegan dos lotes tardíos: uno dentro del watermark "
                    "(actualiza ventanas) y otro muy atrasado (Spark lo descarta).".format(
                        prog["dia_simulado"], prog["ventana"], prog["watermark"]))
        dfp = pd.DataFrame(prog["progreso"])
        if not dfp.empty:
            dfp.insert(1, "archivo", (prog["archivos"] + [""] * len(dfp))[:len(dfp)])
            st.dataframe(dfp.rename(columns={"lote": "micro-lote", "filas_entrada": "filas que llegaron",
                                             "filas_por_segundo": "filas/s", "ventanas_en_estado": "ventanas en memoria",
                                             "filas_descartadas_por_watermark": "grupos descartados por watermark",
                                             "duracion_ms": "duración (ms)"}),
                         use_container_width=True, hide_index=True)
            st.caption("Spark agrega parcialmente cada lote antes del operador con estado; por eso el descarte se cuenta en "
                       "grupos (ventana × borough), no en filas. El lote muy atrasado no actualizó ninguna ventana.")
        ven = csv("streaming_ventanas.csv", parse_dates=["ventana_inicio"])
        if ven is not None:
            final = ven.sort_values("lote").groupby(["ventana_inicio", "borough"]).tail(1)
            final = final[~final["borough"].isin(["Unknown", "N/A"])]
            fig = px.bar(final, x="ventana_inicio", y="viajes", color="borough", barmode="group",
                         title="Conteo final por ventana de 1 hora (cada 30 min) y borough")
            st.plotly_chart(fig, use_container_width=True)
