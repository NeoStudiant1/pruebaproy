# -*- coding: utf-8 -*-

import json
import requests

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

BASE = "https://labordoc.ilo.org"
DOCID = "alma995339593202676"
PID = "12120616820002676"  
VID = "41ILO_INST:41ILO_V2"
REFERER = (
    f"{BASE}/discovery/fulldisplay?docid={DOCID}"
    f"&context=L&vid={VID}&lang=en&search_scope=ALL_ILO&adaptor=Local"
)


def main():
    print("Obteniendo JWT...")
    r = requests.get(
        f"{BASE}/primaws/rest/pub/institution/41ILO_INST/guestJwt"
        "?isGuest=true&lang=en&viewId=41ILO_INST:41ILO_V2",
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Referer": REFERER,
        },
        timeout=(10, 30),
    )
    jwt = json.loads(r.text)
    print(f"JWT OK ({len(jwt)} chars)")

    url = (
        f"{BASE}/primaws/rest/priv/delivery/representationInfo"
        f"?inst=41ILO_INST&lang=en&mmsId=&pid={PID}"
    )
    print(f"\nGET {url}")
    r = requests.get(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Authorization": f"Bearer {jwt}",
            "Accept": "application/json, text/plain, */*",
            "Referer": REFERER,
        },
        timeout=(10, 30),
    )
    print(f"Status: {r.status_code}")
    print(f"Content-Type: {r.headers.get('Content-Type')}")
    print(f"Body length: {len(r.text)}")
    print()

    try:
        data = r.json()
    except Exception as e:
        print(f"No es JSON: {e}")
        print(r.text[:2000])
        return

    pretty = json.dumps(data, indent=2, ensure_ascii=False)
    print("JSON COMPLETO:")
    print("=" * 70)
    if len(pretty) > 8000:
        print(pretty[:8000])
        print("...")
        print(f"[truncado, total {len(pretty)} chars]")
    else:
        print(pretty)

    print()
    print("=" * 70)
    print("URLs S3/exlibris detectadas en el JSON:")
    print("=" * 70)
    import re
    urls = re.findall(
        r'https?://[^\s"\'<>]+(?:\.pdf[^\s"\'<>]*|amazonaws[^\s"\'<>]+|exlibrisgroup[^\s"\'<>]+)',
        r.text
    )
    unicas = list(dict.fromkeys(urls))
    for u in unicas:
        print(f"  {u[:200]}")

    print()
    print("=" * 70)
    print("Estructura de keys (top level y data):")
    print("=" * 70)
    print(f"Top-level keys: {list(data.keys())}")
    if "data" in data and isinstance(data["data"], dict):
        print(f"data keys: {list(data['data'].keys())}")
        for k, v in data["data"].items():
            tipo = type(v).__name__
            if tipo == "list":
                desc = f"list[{len(v)}]"
                if v and isinstance(v[0], dict):
                    desc += f" elementos con keys {list(v[0].keys())}"
            elif tipo == "str":
                desc = f"str ({len(v)} chars)"
            else:
                desc = tipo
            print(f"  data['{k}']: {desc}")


if __name__ == "__main__":
    main()
