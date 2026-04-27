# -*- coding: utf-8 -*-

import json
import re
import time
import requests
from datetime import datetime

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

BASE = "https://labordoc.ilo.org"
DOCID = "alma995339593202676"
VID = "41ILO_INST:41ILO_V2"
REFERER_FULLDISPLAY = (
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
            "Referer": REFERER_FULLDISPLAY,
        },
        timeout=(10, 30),
    )
    r.raise_for_status()
    return json.loads(r.text)


def obtener_service_urls(jwt_token):
    url = (
        f"{BASE}/primaws/rest/pub/edelivery/{DOCID}"
        f"?vid={VID}&lang=en&googleScholar=false"
    )
    r = requests.post(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Referer": REFERER_FULLDISPLAY,
        },
        data="{}",
        timeout=(10, 30),
    )
    r.raise_for_status()
    return r.json().get("electronicServices", [])


def main():
    print("=" * 70)
    print("OPCION E: explorar el visor con Playwright")
    print("=" * 70)

    print("\nObteniendo JWT...")
    jwt_token = obtener_jwt()
    print(f"JWT OK ({len(jwt_token)} chars)")

    print("\nObteniendo serviceUrl del primer PDF...")
    servicios = obtener_service_urls(jwt_token)
    if not servicios:
        print("[FALLO] No hay servicios.")
        return
    pdf = next(
        (s for s in servicios
         if s.get("fileType") == "pdf"
         and s.get("serviceType") == "DIGITAL"
         and s.get("hasAccess")),
        None
    )
    if not pdf:
        print("[FALLO] No hay PDF descargable.")
        return
    visor_url = f"{BASE}{pdf['serviceUrl']}"
    print(f"Visor URL a explorar: {visor_url}")
    print(f"  packageName: {pdf['packageName']}")
    print(f"  firstFileSize: {pdf['firstFileSize']}")

    print()
    print("=" * 70)
    print("LANZANDO PLAYWRIGHT PARA CARGAR EL VISOR Y CAPTURAR XHRS")
    print("=" * 70)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[FALLO] Playwright no instalado.")
        return

    capturas = []  
    pdf_url_detectada = None

    def on_response(response):
        nonlocal pdf_url_detectada
        try:
            url = response.url
            ct = response.headers.get("content-type", "")
            es_relevante = (
                "/primaws/" in url
                or "/view/delivery/" in url
                or "/discovery/delivery/" in url
                or ".pdf" in url.lower()
                or "amazonaws.com" in url
                or "exlibrisgroup.com" in url
                or "alma" in url.lower()
                or "pdf" in ct.lower()
                or "octet-stream" in ct.lower()
            )
            es_asset = any(
                e in url.lower()
                for e in [".woff", ".woff2", ".ttf", ".css", ".js?", ".js#",
                         ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico",
                         "/translations/", "/cslfiles/"]
            )
            if not es_relevante or es_asset:
                return

            cap = {
                "url": url,
                "metodo": response.request.method,
                "status": response.status,
                "ct": ct,
                "request_headers": dict(response.request.headers),
                "response_headers": dict(response.headers),
                "body": "",
                "body_total_chars": 0,
                "body_es_binario": False,
            }
            try:
                if "pdf" in ct.lower() or "octet-stream" in ct.lower():
                    cap["body_es_binario"] = True
                    cap["body"] = "[binario, no se lee]"
                    pdf_url_detectada = url
                else:
                    body_text = response.text()
                    cap["body_total_chars"] = len(body_text)
                    cap["body"] = body_text[:1500]
            except Exception as e:
                cap["body"] = f"[error: {e}]"
            capturas.append(cap)
        except Exception as e:
            print(f"  Error en handler: {e}")

    with sync_playwright() as pw:
        navegador = pw.chromium.launch(headless=True)
        contexto = navegador.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 900},
            extra_http_headers={"Referer": REFERER_FULLDISPLAY},
        )
        pagina = contexto.new_page()
        pagina.on("response", on_response)

        print(f"\nCargando: {visor_url}")
        pagina.goto(visor_url, wait_until="domcontentloaded", timeout=30000)

        print("Esperando 10s para que el visor cargue completamente...")
        time.sleep(10)

        try:
            pagina.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
        except Exception:
            pass

        html_final = pagina.content()

        candidatos_html = re.findall(
            r'(?:src|href|data-url)="([^"]*(?:\.pdf|/delivery/|amazonaws|exlibris)[^"]*)"',
            html_final
        )
        candidatos_html += re.findall(
            r'https?://[^\s"\'<>]+(?:\.pdf|amazonaws[^\s"\'<>]+|/view/delivery/[^\s"\'<>]+)',
            html_final
        )
        candidatos_html = list(dict.fromkeys(candidatos_html))

        try:
            iframes = pagina.query_selector_all("iframe")
            iframes_info = []
            for f in iframes:
                src = f.get_attribute("src") or ""
                if src:
                    iframes_info.append(src)
        except Exception:
            iframes_info = []

        navegador.close()

    print()
    print("=" * 70)
    print(f"DUMP DE {len(capturas)} XHR/RESPONSES RELEVANTES")
    print("=" * 70)

    for i, cap in enumerate(capturas):
        url_corta = cap["url"][:130] + ("..." if len(cap["url"]) > 130 else "")
        print(f"\n[{i}] {cap['metodo']} {url_corta}")
        print(f"    status: {cap['status']}  content-type: {cap['ct']}")
        rh = {
            k.lower(): v[:80] for k, v in cap["request_headers"].items()
            if k.lower() in (
                "accept", "origin", "referer", "authorization",
                "cookie", "content-type",
            ) or k.lower().startswith("x-")
        }
        if rh:
            print(f"    req headers clave:")
            for k, v in rh.items():
                v_short = (v[:60] + "...") if len(v) > 60 else v
                print(f"      {k}: {v_short}")
        resp_h = {
            k.lower(): v for k, v in cap["response_headers"].items()
            if k.lower() in (
                "location", "content-disposition", "content-length",
                "set-cookie",
            )
        }
        if resp_h:
            print(f"    resp headers clave:")
            for k, v in resp_h.items():
                v_short = (v[:120] + "...") if len(v) > 120 else v
                print(f"      {k}: {v_short}")
        if cap["body_es_binario"]:
            print(f"    body: [BINARIO PDF detectado]")
        elif cap["body"]:
            body_short = cap["body"][:600].replace("\n", " ").replace("\r", " ")
            print(f"    body_chars: {cap['body_total_chars']}")
            print(f"    body[:600]: {body_short}")

    print()
    print("=" * 70)
    print("URLS CANDIDATAS EN EL HTML FINAL DEL VISOR")
    print("=" * 70)
    for c in candidatos_html[:15]:
        print(f"  {c[:160]}")

    print()
    print("=" * 70)
    print("IFRAMES EN EL VISOR")
    print("=" * 70)
    for src in iframes_info:
        print(f"  src={src[:160]}")

    print()
    print("=" * 70)
    print("DETECCION DE URL S3/PDF")
    print("=" * 70)
    if pdf_url_detectada:
        print(f"PDF URL detectada en captura: {pdf_url_detectada}")
    else:
        print("No se detecto request directo a PDF binario en las capturas.")
        print("Buscar en URLS CANDIDATAS arriba.")


if __name__ == "__main__":
    main()
