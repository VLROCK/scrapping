"""
Script de diagnóstico — rode localmente para inspecionar a estrutura
de data e JSON que o trafilatura extrai de uma página do autores.com.br

Uso:
    python debug_data_autores.py
"""

import asyncio
import re
import json
import aiohttp
from charset_normalizer import from_bytes
import trafilatura
from bs4 import BeautifulSoup

# Troque por qualquer URL que apareceu no seu log de "[Salvo]" ou "[Batendo na porta]"
URLS_TESTE = [
    "https://autores.com.br/publicacoes-artigos2/143-literatura/rondel/67531-quando-no-espelho-eu-olhar.html",
    "https://autores.com.br/publicacoes-artigos2/14-literatura-popular/frases-e-proverbios/99339-primeira-caridade.html",
    "https://autores.com.br/publicacoes-artigos2/41-literatura-juvenil/contos/99561-a-professora-sophie-scholl-parte-4.html",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
}


async def inspecionar_url(session: aiohttp.ClientSession, url: str):
    print(f"\n{'='*70}")
    print(f"URL: {url}")
    print('='*70)

    async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as r:
        print(f"HTTP Status: {r.status}")
        if r.status != 200:
            print("Falhou — pulando.")
            return

        html_bytes = await r.read()
        detected   = from_bytes(html_bytes).best()
        html_str   = str(detected) if detected else html_bytes.decode("utf-8", errors="replace")
        print(f"HTML size: {len(html_str):,} chars | Encoding detectado: {detected.encoding if detected else 'utf-8 fallback'}")

    # ── 1. O que o trafilatura extrai ────────────────────────────────────────
    print("\n── TRAFILATURA bare_extraction ──")
    result = trafilatura.bare_extraction(
        html_str, url=url,
        include_comments=False, include_tables=False, include_links=False
    )
    if result:
        d = result.as_dict()
        for k, v in d.items():
            if k not in ('body', 'commentsbody'):
                print(f"  {k:20s}: {repr(v)[:150]}")
    else:
        print("  RETORNOU NONE — trafilatura não conseguiu extrair nada.")

    # ── 2. Meta tags de data ─────────────────────────────────────────────────
    print("\n── META TAGS relacionadas a data ──")
    soup  = BeautifulSoup(html_str, 'html.parser')
    metas = soup.find_all('meta')
    for m in metas:
        name    = (m.get('name') or m.get('property') or '').lower()
        content = m.get('content', '')
        if any(k in name for k in ['date', 'time', 'publish', 'modif', 'creat', 'article']):
            print(f"  name/property='{name}' | content='{content}'")

    # ── 3. JSON-LD ───────────────────────────────────────────────────────────
    print("\n── JSON-LD (structured data) ──")
    scripts = soup.find_all('script', type='application/ld+json')
    if scripts:
        for s in scripts:
            try:
                data = json.loads(s.string or '{}')
                print(f"  @type: {data.get('@type')}")
                for key in ['datePublished', 'dateModified', 'dateCreated', 'name', 'headline']:
                    if key in data:
                        print(f"  {key}: {data[key]}")
            except Exception:
                print(f"  JSON inválido: {(s.string or '')[:100]}")
    else:
        print("  Nenhum bloco JSON-LD encontrado.")

    # ── 4. Tags <time> ───────────────────────────────────────────────────────
    print("\n── TAGS <time> ──")
    times = soup.find_all('time')
    if times:
        for t in times:
            print(f"  datetime='{t.get('datetime')}' | texto='{t.get_text(strip=True)[:80]}'")
    else:
        print("  Nenhuma tag <time> encontrada.")

    # ── 5. Padrões de data no HTML bruto ────────────────────────────────────
    print("\n── PADRÕES DE DATA no HTML bruto ──")
    padroes = re.findall(
        r'\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4}|\d{4}[\/\-]\d{2}[\/\-]\d{2}',
        html_str[:30000]
    )
    print(f"  Encontrados: {list(dict.fromkeys(padroes))[:15]}")  # únicos, ordem preservada

    # ── 6. Contexto da palavra "publicad" e "data" ───────────────────────────
    print("\n── CONTEXTO 'publicad' e 'criado' no HTML ──")
    for termo in ['publicad', 'criado em', 'data:', 'created', 'postado']:
        idx = html_str.lower().find(termo)
        if idx > 0:
            trecho = html_str[max(0, idx-80):idx+200].replace('\n', ' ').strip()
            print(f"  [{termo}] ...{trecho}...")
        else:
            print(f"  [{termo}] não encontrado.")

    # ── 7. Texto extraído ────────────────────────────────────────────────────
    print("\n── TEXTO EXTRAÍDO (primeiros 500 chars) ──")
    if result and result.text:
        print(repr(result.text[:500]))
    else:
        # fallback manual
        texto_manual = trafilatura.extract(html_str, url=url)
        print(f"  trafilatura.extract(): {repr(texto_manual[:300]) if texto_manual else 'None'}")


async def main():
    async with aiohttp.ClientSession() as session:
        for url in URLS_TESTE:
            await inspecionar_url(session, url)
            await asyncio.sleep(1)  # pausa entre requests


if __name__ == "__main__":
    asyncio.run(main())