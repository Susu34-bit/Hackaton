"""
convert_txt_to_csv.py
---------------------
Lit le fichier CSV d'entrée (colonne "file") contenant des URLs,
télécharge chaque fichier, vérifie que c'est bien un fichier texte (TXT),
et le convertit en CSV dans le dossier de sortie.

Usage :
    python convert_txt_to_csv.py                          # utilise les chemins par défaut
    python convert_txt_to_csv.py input.csv output_dir/   # chemins personnalisés
"""

import csv
import io
import os
import sys
import requests

# ── Configuration par défaut ──────────────────────────────────────────────────
DEFAULT_INPUT_CSV = "reseau-lio.csv"
DEFAULT_OUTPUT_DIR = "output_csv"
TIMEOUT = 30  # secondes par requête

# Types MIME acceptés comme "texte"
TEXT_MIME_PREFIXES = ("text/plain", "text/csv", "text/tab-separated-values")


def is_text_content(content_type: str) -> bool:
    """Retourne True si le Content-Type indique un fichier texte brut."""
    ct = content_type.lower().split(";")[0].strip()
    return any(ct.startswith(prefix) for prefix in TEXT_MIME_PREFIXES)


def detect_delimiter(sample: str) -> str:
    """Devine le séparateur (tabulation, point-virgule, virgule) d'après un échantillon."""
    for delimiter in ("\t", ";", ","):
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=delimiter)
            return dialect.delimiter
        except csv.Error:
            continue
    return ","  # fallback


def txt_to_csv(content: str, output_path: str) -> None:
    """Convertit le contenu texte en CSV propre et l'écrit sur le disque."""
    sample = content[:4096]
    delimiter = detect_delimiter(sample)

    reader = csv.reader(io.StringIO(content), delimiter=delimiter)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in reader:
            writer.writerow(row)


def process_urls(input_csv: str, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    with open(input_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        urls = [row["file"].strip() for row in reader if row.get("file", "").strip()]

    print(f"→ {len(urls)} URL(s) trouvée(s) dans « {input_csv} »\n")

    stats = {"converted": 0, "skipped": 0, "error": 0}

    for i, url in enumerate(urls, 1):
        file_id = url.rstrip("/").split("/")[-1]
        print(f"[{i:>3}/{len(urls)}] {file_id}", end=" … ")

        try:
            response = requests.get(url, timeout=TIMEOUT)
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "")

            if not is_text_content(content_type):
                print(f"⏭  ignoré  (Content-Type: {content_type.split(';')[0].strip()})")
                stats["skipped"] += 1
                continue

            # Décode le contenu (UTF-8 par défaut, latin-1 en fallback)
            try:
                text = response.content.decode("utf-8")
            except UnicodeDecodeError:
                text = response.content.decode("latin-1")

            output_path = os.path.join(output_dir, f"{file_id}.csv")
            txt_to_csv(text, output_path)
            print(f"✅ converti  → {output_path}")
            stats["converted"] += 1

        except requests.RequestException as e:
            print(f"❌ erreur    ({e})")
            stats["error"] += 1

    print(f"""
╔══════════════════════════════╗
  ✅ Convertis  : {stats['converted']:>4}
  ⏭  Ignorés   : {stats['skipped']:>4}
  ❌ Erreurs    : {stats['error']:>4}
  📁 Dossier   : {output_dir}
╚══════════════════════════════╝""")


if __name__ == "__main__":
    input_csv = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT_CSV
    output_dir = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT_DIR
    process_urls(input_csv, output_dir)
