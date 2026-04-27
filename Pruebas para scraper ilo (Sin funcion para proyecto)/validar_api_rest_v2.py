# -*- coding: utf-8 -*-

import requests
import json
import base64

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def obtener_jwt():
    url_jwt = (
        "https://labordoc.ilo.org/primaws/rest/pub/institution/41ILO_INST/guestJwt"
        "?isGuest=true&lang=en&viewId=41ILO_INST:41ILO_V2"
    )
    r = requests.get(
        url_jwt,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://labordoc.ilo.org/discovery/search?vid=41ILO_INST:41ILO_V2",
        },
        timeout=(10, 30),
    )
    r.raise_for_status()
    return json.loads(r.text)


def probar_edelivery(jwt_token, docid, content_type, body, etiqueta):
    print(f"\n--- VARIANTE {etiqueta} ---")
    print(f"Content-Type request: {content_type!r}")
    print(f"Body request: {body!r}")
    url_edelivery = (
        f"https://labordoc.ilo.org/primaws/rest/pub/edelivery/{docid}"
        f"?vid=41ILO_INST:41ILO_V2&lang=en&googleScholar=false"
    )
    headers = {
        "User-Agent": USER_AGENT,
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/json, text/plain, */*",
        "Referer": (
            "https://labordoc.ilo.org/discovery/fulldisplay?"
            f"docid={docid}&context=L&vid=41ILO_INST:41ILO_V2"
            "&lang=en&search_scope=ALL_ILO&adaptor=Local"
        ),
    }
    if content_type:
        headers["Content-Type"] = content_type
    try:
        r = requests.post(
            url_edelivery,
            headers=headers,
            data=body,
            timeout=(10, 30),
        )
        print(f"Status: {r.status_code}")
        print(f"Content-Type response: {r.headers.get('Content-Type', '?')}")
        if r.status_code == 200:
            try:
                data = r.json()
                servicios = data.get("electronicServices", [])
                print(f"electronicServices: {len(servicios)} servicios")
                return r, data
            except Exception as e:
                print(f"No es JSON valido: {e}")
                print(f"Body preview: {r.text[:200]}")
                return r, None
        else:
            print(f"Body preview: {r.text[:300]}")
            return r, None
    except Exception as e:
        print(f"Excepcion: {type(e).__name__}: {e}")
        return None, None


def probar_descarga(jwt_token, service_url_path, docid):
    print()
    print("=" * 70)
    print("HIPOTESIS 1: serviceUrl sirve PDF real")
    print("=" * 70)
    service_url_completa = f"https://labordoc.ilo.org{service_url_path}"
    print(f"GET {service_url_completa}")
    r = requests.get(
        service_url_completa,
        headers={
            "User-Agent": USER_AGENT,
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/pdf,*/*",
            "Referer": (
                "https://labordoc.ilo.org/discovery/fulldisplay?"
                f"docid={docid}&context=L&vid=41ILO_INST:41ILO_V2"
                "&lang=en&search_scope=ALL_ILO&adaptor=Local"
            ),
        },
        timeout=(10, 60),
        allow_redirects=True,
        stream=True,
    )
    print(f"Status final: {r.status_code}")
    url_final_corta = r.url[:140] + ("..." if len(r.url) > 140 else "")
    print(f"URL final: {url_final_corta}")
    print(f"Content-Type: {r.headers.get('Content-Type', '?')}")
    print(f"Content-Length: {r.headers.get('Content-Length', '?')}")
    if r.history:
        print(f"Redirects ({len(r.history)}):")
        for hop in r.history:
            hop_url = hop.url[:100] + ("..." if len(hop.url) > 100 else "")
            print(f"  {hop.status_code} -> {hop_url}")
    primeros_bytes = b""
    for chunk in r.iter_content(chunk_size=8192):
        primeros_bytes = chunk[:16]
        break
    r.close()
    print(f"Primeros bytes: {primeros_bytes!r}")
    criterios = {
        "status==200": r.status_code == 200,
        "ct incluye pdf": "pdf" in r.headers.get("Content-Type", "").lower(),
        "magic %PDF": primeros_bytes.startswith(b"%PDF"),
    }
    for k, v in criterios.items():
        print(f"  {k}: {v}")
    return all(criterios.values())


def main():
    print("Obteniendo JWT...")
    jwt_token = obtener_jwt()
    print(f"JWT OK ({len(jwt_token)} chars)")

    docid = "alma995339593202676"

    print()
    print("=" * 70)
    print("HIPOTESIS 1.0: /primaws/edelivery responde con electronicServices")
    print("=" * 70)
    print("Probando 2 variantes para resolver el 415 anterior:")

    variantes = [
        ("Content-Type=application/json + body='{}'", "application/json", "{}"),
        ("Content-Type=application/json + body=''",   "application/json", ""),
    ]

    data_buena = None
    for etiqueta, ct, body in variantes:
        r, data = probar_edelivery(jwt_token, docid, ct, body, etiqueta)
        if data and data.get("electronicServices"):
            print(f">> VARIANTE {etiqueta!r} FUNCIONA")
            data_buena = data
            break
        else:
            print(f">> VARIANTE {etiqueta!r} no funciono, probando siguiente")

    if not data_buena:
        print("\n[H1.0 FALLO] Ninguna variante funciono. Pasame el output a Claude.")
        return

    servicios = data_buena["electronicServices"]
    print(f"\nServicios completos ({len(servicios)}):")
    primer_pdf_path = None
    for i, s in enumerate(servicios):
        print(f"  #{i}: packageName={s.get('packageName')!r}, "
              f"serviceType={s.get('serviceType')!r}, "
              f"fileType={s.get('fileType')!r}, "
              f"hasAccess={s.get('hasAccess')!r}, "
              f"firstFileSize={s.get('firstFileSize')!r}")
        print(f"       serviceUrl={s.get('serviceUrl')!r}")
        if (primer_pdf_path is None
                and s.get("fileType") == "pdf"
                and s.get("serviceType") == "DIGITAL"
                and s.get("hasAccess")):
            primer_pdf_path = s.get("serviceUrl")

    if not primer_pdf_path:
        print("\n[H1 SKIP] Ningun servicio cumple criterios pdf+DIGITAL+hasAccess.")
        return

    ok = probar_descarga(jwt_token, primer_pdf_path, docid)
    print()
    print("=" * 70)
    print("RESULTADO FINAL")
    print("=" * 70)
    if ok:
        print("Las dos hipotesis pasaron. Luz verde para Opcion B.")
    else:
        print("H1 fallo. Pasale el output a Claude.")


if __name__ == "__main__":
    main()
