#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Synthetic benchmark corpus with KNOWN ground truth.

Every fixture is generated here from literal strings / payloads, so the
ground truth is independent of pipeline output (never derived from it).
"""
import gzip
import io
import tarfile
import zipfile
from dataclasses import dataclass, field
from typing import List


@dataclass
class CorpusItem:
    name: str
    data: bytes
    mimetype: str
    filename: str
    ground_truth: str = ""          # expected extracted text (may be substring-checked)
    qr_payloads: List[str] = field(default_factory=list)
    category: str = ""              # pdf|text|image|office|archive|eml
    exact: bool = False             # ground_truth must match exactly (normalised)


def _pdf(pages, metadata=None):
    import fitz
    doc = fitz.open()
    for text in pages:
        page = doc.new_page()
        page.insert_text((72, 72), text)
    if metadata:
        doc.set_metadata(metadata)
    return doc.tobytes()


def _qr_png(payload):
    import qrcode
    buf = io.BytesIO()
    qrcode.make(payload).save(buf)
    return buf.getvalue()


def _text_image(text="Hello OCR World 123"):
    from PIL import Image, ImageDraw
    img = Image.new("RGB", (640, 120), "white")
    ImageDraw.Draw(img).text((10, 10), text, fill="black")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _docx(paras):
    body = "".join(
        f"<w:p>{''.join(f'<w:r><w:t>{p}</w:t></w:r>' for p in [para])}</w:p>"
        for para in paras)
    xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
           f"<w:body>{body}</w:body></w:document>")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("word/document.xml", xml)
    return buf.getvalue()


def _xlsx(strings):
    si = "".join(f"<si><t>{s}</t></si>" for s in strings)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("xl/sharedStrings.xml",
                   f'<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">{si}</sst>')
        z.writestr("xl/worksheets/sheet1.xml",
                   '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                   "<sheetData><row><c><v>42</v></c></row></sheetData></worksheet>")
    return buf.getvalue()


def build_corpus():
    items = []
    pdf_text_1 = "The quick brown fox jumps over the lazy dog"
    items.append(CorpusItem("pdf-single", _pdf([pdf_text_1], {"title": "Fox Doc", "author": "Jane Doe"}),
                            "application/pdf", "fox.pdf", pdf_text_1, category="pdf"))
    items.append(CorpusItem("pdf-multi", _pdf(["First page alpha content", "Second page beta content"]),
                            "application/pdf", "multi.pdf",
                            "First page alpha content\nSecond page beta content", category="pdf"))
    items.append(CorpusItem("pdf-numeric", _pdf(["Invoice 1042 total 999 euros due 2024"]),
                            "application/pdf", "inv.pdf", "Invoice 1042 total 999 euros due 2024", category="pdf"))
    items.append(CorpusItem("text-plain", b"Plain text ground truth sentence.",
                            "text/plain", "note.txt", "Plain text ground truth sentence.", category="text", exact=True))
    fr = "Bonjour le monde ceci est un texte francais de reference"
    items.append(CorpusItem("text-french", fr.encode(), "text/plain", "fr.txt", fr, category="text", exact=True))
    long_text = " ".join(["benchmark"] * 50)
    items.append(CorpusItem("text-long", long_text.encode(), "text/plain", "long.txt", long_text, category="text", exact=True))

    for i, payload in enumerate([
            "https://example.org/alpha",
            "WIFI:T:WPA;S:TestNet;P:secret;;",
            "BEGIN:VCARD\nVERSION:3.0\nFN:John Doe\nEND:VCARD",
            "plain text payload 12345",
            "https://example.org/" + "x" * 80]):
        items.append(CorpusItem(f"qr-{i}", _qr_png(payload), "image/png", f"qr{i}.png",
                                "", qr_payloads=[payload], category="image"))

    items.append(CorpusItem("image-rendered-text", _text_image(), "image/png", "rendered.png",
                            "Hello OCR World 123", category="image"))
    items.append(CorpusItem("docx-1", _docx(["Hello docx ground truth", "Second paragraph here"]),
                            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                            "a.docx", "Hello docx ground truth\nSecond paragraph here", category="office"))
    items.append(CorpusItem("xlsx-1", _xlsx(["AlphaCell", "BetaCell"]),
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            "a.xlsx", "AlphaCell", category="office"))

    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, "w") as z:
        z.writestr("a.txt", "zip alpha text")
        z.writestr("b.txt", "zip beta text")
    items.append(CorpusItem("zip-1", zbuf.getvalue(), "application/zip", "a.zip",
                            "zip alpha text", category="archive"))
    tbuf = io.BytesIO()
    with tarfile.open(fileobj=tbuf, mode="w:gz") as tf:
        data = b"tar gamma text"
        info = tarfile.TarInfo("c.txt"); info.size = len(data)
        tf.addfile(info, io.BytesIO(data))
    items.append(CorpusItem("targz-1", tbuf.getvalue(), "application/x-tar", "a.tar.gz",
                            "tar gamma text", category="archive"))
    items.append(CorpusItem("gzip-1", gzip.compress(b"gzip delta text"), "application/gzip",
                            "d.txt.gz", "gzip delta text", category="archive"))
    eml = b"Subject: Corpus Subject\r\nFrom: a@example.org\r\nContent-Type: text/plain\r\n\r\neml body ground truth"
    items.append(CorpusItem("eml-1", eml, "message/rfc822", "a.eml", "eml body ground truth", category="eml"))
    return items


# --- duplicate / near-duplicate ground-truth pairs (independent literals) ---
DEDUP_BASE = "the quick brown fox jumps over the lazy dog and runs through the deep forest " * 4
DEDUP_PAIRS = [
    ("exact", DEDUP_BASE, DEDUP_BASE, True),
    ("near-word-swap", DEDUP_BASE, DEDUP_BASE.replace("forest", "woods", 1), True),
    ("near-truncated", DEDUP_BASE, DEDUP_BASE[: len(DEDUP_BASE) - 20], True),
    ("unrelated", DEDUP_BASE, "quantum physics lecture notes about electrons and protons and neutrons " * 4, False),
    ("unrelated-short", DEDUP_BASE, "cooking recipes for pasta and tomato sauce with basil", False),
]


# --- text metrics (independent implementations) ---
def _lev(a, b):
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def cer(ref, hyp):
    ref = ref or ""
    if not ref:
        return 0.0 if not hyp else 1.0
    return round(_lev(ref, hyp or "") / len(ref), 4)


def wer(ref, hyp):
    r, h = (ref or "").split(), (hyp or "").split()
    if not r:
        return 0.0 if not h else 1.0
    return round(_lev(r, h) / len(r), 4)


def norm(s):
    return " ".join((s or "").split())
