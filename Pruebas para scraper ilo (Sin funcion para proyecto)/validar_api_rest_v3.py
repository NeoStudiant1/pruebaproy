# -*- coding: utf-8 -*-

import requests
import json
import re

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

BASE = "https://labordoc.ilo.org"
DOCID = "alma995339593202676"
ILS_API_ID = "12120616820002676"  
VID = "41ILO_INST:41ILO_V2"
VID_PATH = "41ILO_INST"  
REFERER = (
    f"{BASE}/discovery/fulldisplay?docid={DOCID}"
    f"&context=L&vid={VID}&lang=en&search_scope=ALL_ILO&adaptor=Local"
)


def obtener_jwt():
    r = requests.get(
        f"{BASE}/primaws/rest/pub/institution/41ILO_INST/guestJwt"
        "?isGuest=true&lang=en&viewId=41ILO_INST:41ILO_V2",
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
        },
        timeout=(10, 30),
    )
    r.raise_for_status()
    return json.loads(r.text)


def probar_url(jwt, url, etiqueta, descripcion=""):
    print(f"\n--- VARIANTE {etiqueta} ---")
    if descripcion:
        print(f"Descripcion: {descripcion}")
    print(f"GET {url}")
    try:
        r = requests.get(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Authorization": f"Bearer {jwt}",
                "Accept": "application/pdf,*/*",
                "Referer": REFERER,
            },
            timeout=(10, 60),
            allow_redirects=True,
            stream=True,
        )
        ct = r.headers.get("Content-Type", "?")
        print(f"  Status final: {r.status_code}")
        print(f"  URL final: {r.url[:140]}{'...' if len(r.url) > 140 else ''}")
        print(f"  Content-Type: {ct}")
        print(f"  Content-Length: {r.headers.get('Content-Length', '?')}")
        if r.history:
            print(f"  Redirects ({len(r.history)}):")
            for hop in r.history:
                hu = hop.url[:100] + ("..." if len(hop.url) > 100 else "")
                print(f"    {hop.status_code} -> {hu}")
        primeros = b""
        for chunk in r.iter_content(chunk_size=8192):
            primeros = chunk[:32]
            break
        r.close()
        print(f"  Primeros bytes: {primeros!r}")
        es_pdf = (
            r.status_code == 200
            and "pdf" in ct.lower()
            and primeros.startswith(b"%PDF")
        )
        if es_pdf:
            print(f"  >>> {etiqueta} SIRVE PDF REAL <<<")
        return {
            "es_pdf": es_pdf,
            "status": r.status_code,
            "url_final": r.url,
            "ct": ct,
            "primeros": primeros,
        }
    except Exception as e:
        print(f"  Excepcion: {type(e).__name__}: {e}")
        return None


def explorar_html_visor(jwt, url):
    print(f"\n--- EXPLORACION HTML del visor ---")
    print(f"GET {url} (sin stream para leer body completo)")
    r = requests.get(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Authorization": f"Bearer {jwt}",
            "Accept": "text/html,*/*",
            "Referer": REFERER,
        },
        timeout=(10, 30),
        allow_redirects=True,
    )
    print(f"  Status: {r.status_code}, body length: {len(r.text)}")
    candidatos = re.findall(
        r'https?://[^\s"\'<>]+(?:\.pdf|/view/delivery/[^\s"\'<>]+|/media/\d+/download)',
        r.text
    )
    candidatos += re.findall(
        r'(?:src|href)="(/view/delivery/[^"]+)"',
        r.text
    )
    candidatos += re.findall(
        r'(?:src|href)="(https?://[^"]+\.pdf[^"]*)"',
        r.text
    )
    unicos = list(dict.fromkeys(candidatos))
    print(f"  Candidatos a URL de PDF en el HTML: {len(unicos)}")
    for u in unicos[:10]:
        print(f"    {u[:140]}")
    return unicos


def main():
    print("Obteniendo JWT...")
    jwt = obtener_jwt()
    print(f"JWT OK ({len(jwt)} chars)")

    print()
    print("=" * 70)
    print("BUSCANDO LA URL CORRECTA DE DESCARGA DE PDF")
    print("=" * 70)
    print(f"docid: {DOCID}")
    print(f"ilsApiId del primer servicio: {ILS_API_ID}")

    res_a = probar_url(
        jwt,
        f"{BASE}/discovery/delivery/{VID}/{ILS_API_ID}",
        "A: /discovery/delivery/{vid}/{ilsApiId}",
        descripcion="Lo que devuelve la API REST. Ya sabemos que da HTML."
    )

    res_b = probar_url(
        jwt,
        f"{BASE}/view/delivery/{VID_PATH}/{ILS_API_ID}",
        "B: /view/delivery/{vid_sin_v2}/{ilsApiId}",
        descripcion="Formato corto del visor de delivery."
    )
    res_c = probar_url(
        jwt,
        f"{BASE}/view/delivery/{VID}/{ILS_API_ID}",
        "C: /view/delivery/{vid_completo}/{ilsApiId}",
        descripcion="Por si el endpoint quiere el VID con :V2."
    )

    encontro_pdf = any(
        r and r.get("es_pdf")
        for r in [res_a, res_b, res_c]
    )

    if not encontro_pdf:
        print()
        print("=" * 70)
        print("Ninguna variante de URL sirvio PDF directo.")
        print("Exploramos el HTML del visor para encontrar el PDF real.")
        print("=" * 70)
        candidatos = explorar_html_visor(
            jwt,
            f"{BASE}/discovery/delivery/{VID}/{ILS_API_ID}"
        )
        if candidatos:
            for c in candidatos[:3]:
                if not c.startswith("http"):
                    c = BASE + c
                probar_url(
                    jwt, c,
                    f"CANDIDATO_HTML: {c[:60]}...",
                    descripcion="URL extraida del HTML del visor"
                )

    print()
    print("=" * 70)
    print("RESUMEN")
    print("=" * 70)
    for nombre, res in [("A discovery/delivery", res_a),
                        ("B view/delivery (sin :V2)", res_b),
                        ("C view/delivery (con :V2)", res_c)]:
        if res is None:
            print(f"  {nombre}: ERROR")
        elif res["es_pdf"]:
            print(f"  {nombre}: SIRVE PDF (status {res['status']}, ct {res['ct']})")
        else:
            print(f"  {nombre}: NO sirve PDF (status {res['status']}, "
                  f"ct {res['ct']}, bytes {res['primeros'][:8]!r}...)")


if __name__ == "__main__":
    main()
