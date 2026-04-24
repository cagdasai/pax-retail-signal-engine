"""
PAX Retail Signal Engine V2 — Sektörlü takip listesi
----------------------------------------------------
- takip_listesi.json: kategori + isim + sektör okur.
- kaynak_listesi.json: RSS/Web kaynaklarını okur.
- GitHub Issue açar.
- Issue içine 4 katmanlı mail formatı ekler.
- gorulen_haberler.json ile aynı haberi tekrar bildirmez.
"""

import os
import re
import json
import time
import html
import hashlib
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urljoin, urlparse

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
GITHUB_REPO = os.environ.get("GITHUB_REPOSITORY") or "cagdasai/pax-retail-signal-engine"

TAKIP_DOSYA = "takip_listesi.json"
KAYNAK_DOSYA = "kaynak_listesi.json"
GORULEN_DOSYA = "gorulen_haberler.json"

KAYNAK_ARASI_BEKLEME = 1
GORULDU_GUN = 30

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, text/html, */*",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
}

HASSAS_TERIMLER = {
    "File", "Civil", "Mars", "Gusto", "Elle", "Jumbo", "Eker", "Mondi",
    "Efes", "Ekomini", "Flo", "BAT", "Dagi", "Avva", "Loya", "SPX",
    "Chakra", "Emsan", "Jacobs", "Mado", "Namet", "Eti", "Subway",
    "Aroma", "Porland", "Pepsi", "Newal", "Bunge", "Şok", "Çilek",
    "İçim", "Mudo", "Panço", "Logo", "NCR", "QNB", "BJK", "LCW"
}


def normalize(text):
    if not text:
        return ""
    text = html.unescape(str(text)).lower()
    table = str.maketrans("ıİğĞüÜşŞöÖçÇ", "iiggüüssööcc")
    text = text.translate(table)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def json_oku(path, default):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"{path} okunamadı:", e)
    return default


def json_yaz(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def haber_id(link, title):
    raw = normalize((link or "") + (title or ""))
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def url_oku(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=25) as response:
        return response.read()


def text_from_element(el):
    if el is None or el.text is None:
        return ""
    return html.unescape(el.text).strip()


def rss_tara(kaynak):
    isim = kaynak.get("isim", "Bilinmeyen")
    rss_url = (kaynak.get("rss") or "").strip()

    if not rss_url:
        return [], "RSS yok"

    try:
        content = url_oku(rss_url)
        root = ET.fromstring(content)
        haberler = []

        for item in root.findall(".//item"):
            title = text_from_element(item.find("title"))
            link = text_from_element(item.find("link"))
            desc = text_from_element(item.find("description"))

            if title and link:
                haberler.append({
                    "kaynak": isim,
                    "baslik": title,
                    "link": link,
                    "ozet": desc
                })

        ns = {"atom": "http://www.w3.org/2005/Atom"}

        for entry in root.findall(".//atom:entry", ns):
            title_el = entry.find("atom:title", ns)
            link_el = entry.find("atom:link", ns)
            summary_el = entry.find("atom:summary", ns)

            title = text_from_element(title_el)
            link = link_el.attrib.get("href", "").strip() if link_el is not None else ""
            desc = text_from_element(summary_el)

            if title and link:
                haberler.append({
                    "kaynak": isim,
                    "baslik": title,
                    "link": link,
                    "ozet": desc
                })

        return haberler, None

    except Exception as e:
        return [], str(e)


def web_tara(kaynak):
    isim = kaynak.get("isim", "Bilinmeyen")
    web_url = (kaynak.get("web") or "").strip()

    if not web_url:
        return [], "Web URL yok"

    try:
        raw = url_oku(web_url)
        text = raw.decode("utf-8", errors="ignore")
        base = f"{urlparse(web_url).scheme}://{urlparse(web_url).netloc}"

        pattern = re.compile(
            r'<a\s+[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
            re.I | re.S
        )

        haberler = []
        seen = set()

        for href, inner in pattern.findall(text):
            title = re.sub(r"<[^>]+>", " ", inner)
            title = html.unescape(re.sub(r"\s+", " ", title)).strip()

            if len(title) < 15:
                continue

            link = urljoin(base, href.strip())
            key = normalize(link + title)

            if key in seen:
                continue

            seen.add(key)

            haberler.append({
                "kaynak": isim,
                "baslik": title,
                "link": link,
                "ozet": ""
            })

        return haberler, None

    except Exception as e:
        return [], str(e)


def kaynak_tara(kaynak):
    isim = kaynak.get("isim", "Bilinmeyen")

    if (kaynak.get("rss") or "").strip():
        haberler, hata = rss_tara(kaynak)

        if haberler:
            print(f"✅ {isim}: RSS ile {len(haberler)} haber")
            return haberler, None, "rss"

        print(f"⚠️ {isim}: RSS başarısız/boş: {hata}")

    haberler, hata = web_tara(kaynak)

    if haberler:
        print(f"✅ {isim}: Web ile {len(haberler)} link")
        return haberler, None, "web"

    print(f"❌ {isim}: kaynak okunamadı: {hata}")
    return [], hata, "hata"


def takip_map_hazirla(takip_listesi):
    items = []

    for kategori, kayitlar in takip_listesi.items():
        if not isinstance(kayitlar, list):
            continue

        for kayit in kayitlar:
            if isinstance(kayit, str):
                isim = kayit
                sektor = ""
            elif isinstance(kayit, dict):
                isim = kayit.get("isim") or kayit.get("firma") or ""
                sektor = kayit.get("sektor") or ""
            else:
                continue

            isim = str(isim).strip()
            sektor = str(sektor).strip()

            if not isim:
                continue

            items.append({
                "kategori": kategori,
                "firma": isim,
                "sektor": sektor,
                "firma_norm": normalize(isim),
                "hassas": isim in HASSAS_TERIMLER
            })

    return items


def eslesme_var_mi(text_norm, firma_norm, hassas):
    if not firma_norm:
        return False

    if hassas or len(firma_norm) <= 4:
        pattern = r"(?<![a-z0-9])" + re.escape(firma_norm) + r"(?![a-z0-9])"
        return bool(re.search(pattern, text_norm))

    return firma_norm in text_norm


def haberleri_eslestir(haberler, takip_listesi):
    takip_items = takip_map_hazirla(takip_listesi)
    eslesen = []

    for haber in haberler:
        combined = normalize(
            (haber.get("baslik") or "") + " " + (haber.get("ozet") or "")
        )

        for item in takip_items:
            if eslesme_var_mi(combined, item["firma_norm"], item["hassas"]):
                h = haber.copy()
                h["kategori"] = item["kategori"]
                h["firma"] = item["firma"]
                h["sektor"] = item["sektor"]
                eslesen.append(h)
                break

    return eslesen


def format_mail(results):
    mail = "PAX Retail Signal | Günlük Intel Raporu\n\n"

    sections = {
        "Müşteriler": "🟢 MÜŞTERİLER",
        "KasaPOS Firmaları": "🟡 KASAPOS FİRMALARI",
        "Rakipler": "🔴 RAKİPLER",
        "Fintech & Bankalar": "🔵 FINTECH & BANKALAR"
    }

    toplam = 0

    for key, title in sections.items():
        kayitlar = results.get(key, [])
        toplam += len(kayitlar)

        if kayitlar:
            mail += f"\n{title}\n\n"

            for r in kayitlar:
                sektor = f" — {r.get('sektor')}" if r.get("sektor") else ""
                mail += f"• {r.get('isim')}{sektor}\n"
                mail += f"  → {r.get('baslik')}\n"
                if r.get("kaynak"):
                    mail += f"  Kaynak: {r.get('kaynak')}\n"
                mail += "\n"

    if toplam == 0:
        mail += "Bugün yeni haber bulunamadı. Sistem çalıştı.\n"

    mail += f"\nToplam yeni haber: {toplam}\n"

    return mail


def mail_results_olustur(yeni_haberler):
    results = {
        "Müşteriler": [],
        "KasaPOS Firmaları": [],
        "Rakipler": [],
        "Fintech & Bankalar": []
    }

    for h in yeni_haberler:
        kategori = h.get("kategori")

        if kategori not in results:
            continue

        results[kategori].append({
            "isim": h.get("firma", ""),
            "sektor": h.get("sektor", ""),
            "baslik": h.get("baslik", ""),
            "kaynak": h.get("kaynak", "")
        })

    return results


def issue_body_olustur(
    yeni_haberler,
    tum_haber_sayisi,
    toplam_eslesen,
    sorunlu_kaynaklar,
    takip_listesi,
    kaynak_listesi
):
    tarih = datetime.now().strftime("%d.%m.%Y %H:%M")

    sektor_sayim = {}

    for h in yeni_haberler:
        sektor = h.get("sektor") or "Sektör yok"
        sektor_sayim[sektor] = sektor_sayim.get(sektor, 0) + 1

    body = f"""# PAX Retail Signal Engine — Günlük Rapor

**Tarih:** {tarih}

## Özet

- Taranan kaynak sayısı: {len(kaynak_listesi)}
- Taranan haber/link sayısı: {tum_haber_sayisi}
- Firma eşleşen toplam haber: {toplam_eslesen}
- Yeni bildirilecek haber: {len(yeni_haberler)}
- Sorunlu kaynak: {len(sorunlu_kaynaklar)}

"""

    if sektor_sayim:
        body += "## Sektör Dağılımı\n\n"

        for sektor, adet in sorted(sektor_sayim.items(), key=lambda x: x[1], reverse=True):
            body += f"- **{sektor}:** {adet} haber\n"

        body += "\n"

    kategori_sirasi = [
        "Müşteriler",
        "KasaPOS Firmaları",
        "Rakipler",
        "Fintech & Bankalar"
    ]

    if yeni_haberler:
        body += "## Yeni Haberler\n\n"

        for kategori in kategori_sirasi:
            grup = [h for h in yeni_haberler if h.get("kategori") == kategori]

            if not grup:
                continue

            body += f"### {kategori} ({len(grup)})\n\n"

            for h in grup:
                sektor = f" — {h.get('sektor')}" if h.get("sektor") else ""
                body += f"- **{h.get('firma')}**{sektor} — [{h.get('baslik')}]({h.get('link')})\n"
                body += f"  - Kaynak: {h.get('kaynak')}\n"

            body += "\n"
    else:
        body += "Bugün yeni haber bulunamadı. Sistem kontrol amaçlı rapor oluşturdu.\n\n"

    if sorunlu_kaynaklar:
        body += "---\n\n## Sorunlu Kaynaklar\n\n"

        for s in sorunlu_kaynaklar:
            body += f"- **{s.get('isim')}** ({s.get('tip')}) — `{str(s.get('hata'))[:180]}`\n"

        body += "\n"

    body += "---\n\n## Takip Kapsamı\n\n"

    for kategori in kategori_sirasi:
        kayitlar = takip_listesi.get(kategori, [])
        body += f"- **{kategori}:** {len(kayitlar)} kayıt\n"

    body += f"- **Kaynak siteler:** {len(kaynak_listesi)} kayıt\n"

    body += "\n---\n\n## Mail Formatı\n\n"
    body += "```text\n"
    body += format_mail(mail_results_olustur(yeni_haberler))
    body += "\n```\n"

    body += "\n---\nBu issue GitHub Actions tarafından otomatik oluşturuldu.\n"

    return body


def issue_ac(
    yeni_haberler,
    tum_haber_sayisi,
    toplam_eslesen,
    sorunlu_kaynaklar,
    takip_listesi,
    kaynak_listesi
):
    tarih = datetime.now().strftime("%d.%m.%Y %H:%M")

    title = (
        f"📰 PAX Retail Signal Engine — {len(yeni_haberler)} yeni haber — {tarih}"
        if yeni_haberler
        else f"✅ PAX Retail Signal Engine — sistem çalıştı — {tarih}"
    )

    body = issue_body_olustur(
        yeni_haberler,
        tum_haber_sayisi,
        toplam_eslesen,
        sorunlu_kaynaklar,
        takip_listesi,
        kaynak_listesi
    )

    if not GITHUB_TOKEN:
        print("GITHUB_TOKEN bulunamadı. Issue açılamadı.")
        print(body[:1000])
        return

    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/issues"

    payload = json.dumps({
        "title": title,
        "body": body,
        "labels": ["pazar-takip", "otomatik", "signal-engine"]
    }).encode("utf-8")

    req = urllib.request.Request(
        api_url,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )

    with urllib.request.urlopen(req, timeout=25) as response:
        result = response.read().decode("utf-8")
        print("✅ Issue açıldı:", result[:300])


def main():
    print("🔍 PAX Retail Signal Engine V2 başladı")

    takip_listesi = json_oku(TAKIP_DOSYA, {})
    kaynak_listesi = json_oku(KAYNAK_DOSYA, [])

    if not takip_listesi:
        raise RuntimeError(f"{TAKIP_DOSYA} boş veya okunamadı.")

    if not kaynak_listesi:
        raise RuntimeError(f"{KAYNAK_DOSYA} boş veya okunamadı.")

    gorulen = json_oku(GORULEN_DOSYA, {})
    tum_haberler = []
    sorunlu = []

    print(f"Takip kategorisi: {len(takip_listesi)}")
    print(f"Kaynak sayısı: {len(kaynak_listesi)}")

    for i, kaynak in enumerate(kaynak_listesi, 1):
        print(f"[{i}/{len(kaynak_listesi)}] {kaynak.get('isim')}")

        haberler, hata, tip = kaynak_tara(kaynak)

        if hata and not haberler:
            sorunlu.append({
                "isim": kaynak.get("isim"),
                "hata": hata,
                "tip": tip
            })

        tum_haberler.extend(haberler)

        if i < len(kaynak_listesi):
            time.sleep(KAYNAK_ARASI_BEKLEME)

    eslesen = haberleri_eslestir(tum_haberler, takip_listesi)

    yeni = []
    now = time.time()
    threshold = now - (GORULDU_GUN * 24 * 3600)

    temiz_gorulen = {}

    for k, v in gorulen.items():
        if isinstance(v, (int, float)) and v > threshold:
            temiz_gorulen[k] = v

    for h in eslesen:
        hid = haber_id(h.get("link", ""), h.get("baslik", ""))

        if hid not in temiz_gorulen:
            yeni.append(h)
            temiz_gorulen[hid] = now

    print("=== ÖZET ===")
    print("Toplam haber/link:", len(tum_haberler))
    print("Firma eşleşen toplam:", len(eslesen))
    print("Yeni bildirilecek:", len(yeni))
    print("Sorunlu kaynak:", len(sorunlu))

    issue_ac(
        yeni,
        len(tum_haberler),
        len(eslesen),
        sorunlu,
        takip_listesi,
        kaynak_listesi
    )

    json_yaz(GORULEN_DOSYA, temiz_gorulen)

    print("✅ Tamamlandı")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("GENEL HATA:", str(e))
        import traceback
        traceback.print_exc()
        raise
