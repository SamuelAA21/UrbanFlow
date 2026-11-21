"""Convierte el shapefile oficial de zonas de taxi de NYC TLC a GeoJSON (lat/lon).

El shapefile viene en EPSG:2263 (NAD83 / New York Long Island, pies US). Aquí se
implementa la proyección inversa Lambert Conformal Conic de 2 paralelos (EPSG 9802)
para no depender de librerías GIS pesadas. Solo se ejecuta una vez.
"""
import io
import json
import math
import os
import urllib.request
import zipfile

import shapefile  # pyshp

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
SALIDA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "taxi_zones.geojson")

# Elipsoide GRS80 y parámetros de EPSG:2263
A = 6378137.0
F_ = 1 / 298.257222101
E = math.sqrt(2 * F_ - F_ ** 2)
PIE_US = 1200 / 3937
FI1, FI2, FIF = map(math.radians, (41 + 2 / 60, 40 + 40 / 60, 40 + 10 / 60))
LAMBDA_F = math.radians(-74.0)
ESTE_F = 300000.0  # metros (984250 pies US)


def _m(fi):
    return math.cos(fi) / math.sqrt(1 - E ** 2 * math.sin(fi) ** 2)


def _t(fi):
    return math.tan(math.pi / 4 - fi / 2) / ((1 - E * math.sin(fi)) / (1 + E * math.sin(fi))) ** (E / 2)


N = (math.log(_m(FI1)) - math.log(_m(FI2))) / (math.log(_t(FI1)) - math.log(_t(FI2)))
FF = _m(FI1) / (N * _t(FI1) ** N)
R_F = A * FF * _t(FIF) ** N


def a_lonlat(x_pies, y_pies):
    x, y = x_pies * PIE_US - ESTE_F, y_pies * PIE_US
    r = math.copysign(math.hypot(x, R_F - y), N)
    theta = math.atan2(x, R_F - y)
    t = (r / (A * FF)) ** (1 / N)
    chi = math.pi / 2 - 2 * math.atan(t)
    e2, e4, e6, e8 = E ** 2, E ** 4, E ** 6, E ** 8
    fi = (chi + (e2 / 2 + 5 * e4 / 24 + e6 / 12 + 13 * e8 / 360) * math.sin(2 * chi)
          + (7 * e4 / 48 + 29 * e6 / 240 + 811 * e8 / 11520) * math.sin(4 * chi)
          + (7 * e6 / 120 + 81 * e8 / 1120) * math.sin(6 * chi)
          + (4279 * e8 / 161280) * math.sin(8 * chi))
    return round(math.degrees(theta / N + LAMBDA_F), 5), round(math.degrees(fi), 5)


def anillo(puntos, paso):
    coords = [a_lonlat(x, y) for x, y in puntos[::paso]]
    if coords[0] != coords[-1]:
        coords.append(coords[0])
    return coords if len(coords) >= 4 else [a_lonlat(x, y) for x, y in puntos]


def main():
    datos = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(URL, timeout=60).read()))
    nombre = [n for n in datos.namelist() if n.endswith(".shp")][0][:-4]
    lector = shapefile.Reader(shp=io.BytesIO(datos.read(nombre + ".shp")),
                              dbf=io.BytesIO(datos.read(nombre + ".dbf")),
                              shx=io.BytesIO(datos.read(nombre + ".shx")))
    campos = [f[0] for f in lector.fields[1:]]
    features = []
    for registro in lector.shapeRecords():
        atrib = dict(zip(campos, registro.record))
        forma = registro.shape
        partes = list(forma.parts) + [len(forma.points)]
        paso = 2 if len(forma.points) > 400 else 1  # simplificación ligera
        anillos = [anillo(forma.points[partes[i]:partes[i + 1]], paso) for i in range(len(partes) - 1)]
        features.append({
            "type": "Feature",
            "id": int(atrib["LocationID"]),
            "properties": {"zona_id": int(atrib["LocationID"]), "zona": atrib["zone"], "borough": atrib["borough"]},
            # Cada anillo como polígono propio: suficiente para un mapa de calor por zona.
            "geometry": {"type": "MultiPolygon", "coordinates": [[a] for a in anillos]},
        })
    with open(SALIDA, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, separators=(",", ":"))
    lon, lat = a_lonlat(988000, 211000)  # Times Square aprox. en pies US
    print("Zonas: {} | archivo: {} KB | control Times Square: {}, {}".format(
        len(features), os.path.getsize(SALIDA) // 1024, lat, lon))


if __name__ == "__main__":
    main()
