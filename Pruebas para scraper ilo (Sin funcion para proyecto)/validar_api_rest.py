# -*- coding: utf-8 -*-

import requests
import json
import base64
from datetime import datetime

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def main():
    print("=" * 70)
    print("HIPOTESIS 2: JWT guest desde esta IP")
    print("=" * 70)

    url_jwt = (
        "https://labordoc.ilo.org/primaws/rest/pub/institution/41ILO_INST/guestJwt"
        "?isGuest=true&lang=en&viewId=41ILO_INST:41ILO_V2"
    )
    print(f"GET {url_jwt}")

    try:
        r = requests.get(
            url_jwt,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://labordoc.ilo.org/discovery/search?vid=41ILO_INST:41ILO_V2",
            },
            timeout=(10, 30),
        )
        print(f"Status: {r.status_code}")
        print(f"Content-Type: {r.headers.get('Content-Type', '?')}")
        print(f"Body length: {len(r.text)}")
        print(f"Body preview: {r.text[:120]}...")

        if r.status_code != 200:
            print("\n[H2 FALLO] Status no es 200. Abortando.")
            return False

        jwt_token = json.loads(r.text)
        print(f"\nJWT obtenido (largo {len(jwt_token)} chars):")
        print(f"  {jwt_token[:80]}...")

        parts = jwt_token.split(".")
        payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        print(f"\nPayload del JWT:")
        print(f"  userName:      {payload.get('userName')}")
        print(f"  userGroup:     {payload.get('userGroup')}")
        print(f"  userIp:        {payload.get('userIp')}")
        print(f"  institution:   {payload.get('institution')}")
        print(f"  exp:           {payload.get('exp')}")
        if payload.get("exp"):
            exp_dt = datetime.fromtimestamp(payload["exp"])
            print(f"  exp (legible): {exp_dt}")
        print("\n[H2 OK] JWT obtenido correctamente desde esta IP.")

    except Exception as e:
        print(f"\n[H2 FALLO] Excepcion: {type(e).__name__}: {e}")
        return False

    print()
    print("=" * 70)
    print("HIPOTESIS 1.0: /primaws/edelivery responde con electronicServices")
    print("=" * 70)

    docid = "alma995339593202676"
    url_edelivery = (
        f"https://labordoc.ilo.org/primaws/rest/pub/edelivery/{docid}"
        f"?vid=41ILO_INST:41ILO_V2&lang=en&googleScholar=false"
    )
    print(f"POST {url_edelivery}")

    try:
        r = requests.post(
            url_edelivery,
            headers={
                "User-Agent": USER_AGENT,
                "Authorization": f"Bearer {jwt_token}",
                "Accept": "application/json, text/plain, */*",
                "Referer": f"https://labordoc.ilo.org/discovery/fulldisplay?docid={docid}&context=L&vid=41ILO_INST:41ILO_V2&lang=en&search_scope=ALL_ILO&adaptor=Local",
            },
            timeout=(10, 30),
        )
        print(f"Status: {r.status_code}")
        print(f"Content-Type: {r.headers.get('Content-Type', '?')}")

        if r.status_code != 200:
            print(f"\n[H1.0 FALLO] Status no es 200. Body preview: {r.text[:300]}")
            return False

        data = r.json()
        servicios = data.get("electronicServices", [])
        print(f"\nelectronicServices encontrados: {len(servicios)}")

        if not servicios:
            print(f"[H1.0 ALERTA] electronicServices vacio. Body: {r.text[:500]}")
            return False

        primer_pdf_path = None
        for i, s in enumerate(servicios):
            print(f"  Servicio #{i}:")
            print(f"    packageName:    {s.get('packageName')}")
            print(f"    serviceType:    {s.get('serviceType')}")
            print(f"    fileType:       {s.get('fileType')}")
            print(f"    hasAccess:      {s.get('hasAccess')}")
            print(f"    firstFileSize:  {s.get('firstFileSize')}")
            print(f"    serviceUrl:     {s.get('serviceUrl')}")
            if (primer_pdf_path is None
                    and s.get("fileType") == "pdf"
                    and s.get("serviceType") == "DIGITAL"
                    and s.get("hasAccess")):
                primer_pdf_path = s.get("serviceUrl")

        print(f"\n[H1.0 OK] {len(servicios)} servicios devueltos.")
        if not primer_pdf_path:
            print("[H1 SKIP] No hay servicio que cumpla "
                  "fileType=pdf + serviceType=DIGITAL + hasAccess. "
                  "Cambia el docid de prueba.")
            return False

    except Exception as e:
        print(f"\n[H1.0 FALLO] Excepcion: {type(e).__name__}: {e}")
        return False

    print()
    print("=" * 70)
    print("HIPOTESIS 1: serviceUrl sirve PDF real")
    print("=" * 70)

    service_url_completa = f"https://labordoc.ilo.org{primer_pdf_path}"
    print(f"GET {service_url_completa}")

    try:
        r = requests.get(
            service_url_completa,
            headers={
                "User-Agent": USER_AGENT,
                "Authorization": f"Bearer {jwt_token}",
                "Accept": "application/pdf,*/*",
                "Referer": f"https://labordoc.ilo.org/discovery/fulldisplay?docid={docid}&context=L&vid=41ILO_INST:41ILO_V2&lang=en&search_scope=ALL_ILO&adaptor=Local",
            },
            timeout=(10, 60),
            allow_redirects=True,
            stream=True,
        )

        print(f"\nResultado tras seguir redirects:")
        print(f"  Status final: {r.status_code}")
        url_final_corta = r.url[:140] + ("..." if len(r.url) > 140 else "")
        print(f"  URL final:    {url_final_corta}")
        print(f"  Content-Type: {r.headers.get('Content-Type', '?')}")
        print(f"  Content-Length: {r.headers.get('Content-Length', '?')}")
        if r.history:
            print(f"  Redirects encadenados ({len(r.history)}):")
            for hop in r.history:
                hop_url = hop.url[:100] + ("..." if len(hop.url) > 100 else "")
                print(f"    {hop.status_code} -> {hop_url}")

        primeros_bytes = b""
        for chunk in r.iter_content(chunk_size=8192):
            primeros_bytes = chunk[:16]
            break
        r.close()
        print(f"  Primeros bytes: {primeros_bytes!r}")

        criterios = {
            "status==200": r.status_code == 200,
            "content-type incluye pdf": "pdf" in r.headers.get("Content-Type", "").lower(),
            "magic bytes %PDF": primeros_bytes.startswith(b"%PDF"),
        }
        print(f"\nCriterios:")
        for k, v in criterios.items():
            print(f"  {k}: {v}")

        if all(criterios.values()):
            print("\n[H1 OK] serviceUrl sirve PDF real.")
        else:
            print("\n[H1 PARCIAL/FALLO] Algun criterio no se cumplio.")
            return False

    except Exception as e:
        print(f"\n[H1 FALLO] Excepcion: {type(e).__name__}: {e}")
        return False

    print()
    print("=" * 70)
    print("RESULTADO FINAL")
    print("=" * 70)
    print("Las dos hipotesis pasaron. Luz verde para el refactor de Opcion B.")
    return True


if __name__ == "__main__":
    main()
