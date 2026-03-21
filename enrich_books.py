#!/usr/bin/env python3
"""
enrich_books.py — Enrich Obsidian book markdown files with metadata from Open Library / Google Books.

Usage:
  Single file:   python enrich_books.py path/to/Book.md
  Batch folder:  python enrich_books.py --batch path/to/folder/
  New file:      python enrich_books.py --new 9780358447849 --output path/to/folder/
"""

import argparse
import os
import re
import sys
from pathlib import Path

import requests
import frontmatter
import keyring

# ---------------------------------------------------------------------------
# Optional Google Books API key from environment
# ---------------------------------------------------------------------------
GOOGLE_BOOKS_API_KEY = keyring.get_password("enrich_books", "google_books_api_key")

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def query_open_library(isbn: str) -> dict | None:
    """Query Open Library for book metadata. Returns a normalised dict or None."""
    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"    [Open Library] Request failed: {e}")
        return None

    key = f"ISBN:{isbn}"
    if key not in data:
        return None

    book = data[key]

    # Authors
    authors = ", ".join(a.get("name", "") for a in book.get("authors", []))

    # Publisher & year
    publishers = book.get("publishers", [])
    publisher = publishers[0].get("name", "") if publishers else ""
    publish_date = book.get("publish_date", "")
    year = re.search(r"\d{4}", publish_date).group(0) if re.search(r"\d{4}", publish_date) else publish_date

    # Subjects / genres
    subjects = [s.get("name", s) if isinstance(s, dict) else s for s in book.get("subjects", [])]
    genres = subjects[:8]  # cap at 8

    # Description
    desc = book.get("notes", "") or book.get("description", "")
    if isinstance(desc, dict):
        desc = desc.get("value", "")

    # Cover image
    covers = book.get("cover", {})
    cover_url = covers.get("large") or covers.get("medium") or covers.get("small") or ""

    return {
        "title": book.get("title", ""),
        "author": authors,
        "publisher": publisher,
        "year": year,
        "pages": book.get("number_of_pages", ""),
        "genres": genres,
        "description": desc.strip(),
        "cover_url": cover_url,
    }


def query_google_books(isbn: str) -> dict | None:
    """Query Google Books for book metadata. Returns a normalised dict or None."""
    params = {"q": f"isbn:{isbn}"}
    if GOOGLE_BOOKS_API_KEY:
        params["key"] = GOOGLE_BOOKS_API_KEY
    try:
        resp = requests.get("https://www.googleapis.com/books/v1/volumes", params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"    [Google Books] Request failed: {e}")
        return None

    items = data.get("items", [])
    if not items:
        return None

    info = items[0].get("volumeInfo", {})

    authors = ", ".join(info.get("authors", []))
    publisher = info.get("publisher", "")
    published = info.get("publishedDate", "")
    year = published[:4] if published else ""
    genres = info.get("categories", [])[:8]
    desc = info.get("description", "")

    # Cover
    image_links = info.get("imageLinks", {})
    cover_url = image_links.get("thumbnail", "").replace("http://", "https://")

    return {
        "title": info.get("title", ""),
        "author": authors,
        "publisher": publisher,
        "year": year,
        "pages": info.get("pageCount", ""),
        "genres": genres,
        "description": desc.strip(),
        "cover_url": cover_url,
    }


def fetch_book_data(isbn: str) -> dict | None:
    """Try Open Library first, fall back to Google Books."""
    print(f"    Querying Open Library...")
    data = query_open_library(isbn)
    if data and data.get("title"):
        print(f"    Open Library: found '{data['title']}'")
        return data

    print(f"    Open Library returned no results, trying Google Books...")
    data = query_google_books(isbn)
    if data and data.get("title"):
        print(f"    Google Books: found '{data['title']}'")
        return data

    return None


# ---------------------------------------------------------------------------
# Markdown helpers
# ---------------------------------------------------------------------------

def extract_isbn_from_body(content: str) -> str | None:
    """Extract ISBN from the markdown body (e.g. 'ISBN: 9780358447849')."""
    match = re.search(r"ISBN:\s*([0-9X\-]+)", content, re.IGNORECASE)
    return match.group(1).replace("-", "") if match else None


def remove_isbn_from_body(content: str) -> str:
    """Remove the ISBN line from the body."""
    return re.sub(r"\n?ISBN:\s*[0-9X\-]+\n?", "\n", content, flags=re.IGNORECASE).strip()


def insert_description(content: str, description: str) -> str:
    """Insert a Description section before the Review section, if not already present."""
    if not description:
        return content
    if "## Description" in content:
        return content  # already exists, don't overwrite

    desc_block = f"\n## Description\n{description}\n"

    # Insert before ## Review if it exists
    if "## Review" in content:
        return content.replace("## Review", f"{desc_block}\n## Review")

    # Otherwise append
    return content.rstrip() + f"\n{desc_block}"


def ensure_review_section(content: str) -> str:
    """Add a Review section at the end if not present."""
    if "## Review" not in content:
        content = content.rstrip() + "\n\n## Review\n"
    return content


def enrich_frontmatter(post: frontmatter.Post, book: dict, isbn: str) -> bool:
    """
    Write enriched fields into frontmatter.
    Only fills empty / missing fields. Returns True if any changes were made.
    """
    changed = False

    fields = {
        "ISBN": isbn,
        "Title": book.get("title", ""),
        "Author": book.get("author", ""),
        "Publisher": book.get("publisher", ""),
        "Year": book.get("year", ""),
        "Pages": book.get("pages", ""),
        "Genres": book.get("genres", []),
    }

    # Cover image — only set Base Image if empty
    if book.get("cover_url") and not post.metadata.get("Base Image"):
        post.metadata["Base Image"] = book["cover_url"]
        changed = True

    for key, value in fields.items():
        if value and not post.metadata.get(key):
            post.metadata[key] = value
            changed = True

    return changed


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def process_file(filepath: Path) -> str:
    """
    Process a single markdown file. Returns a status string for the summary.
    Possible return values: 'enriched', 'skipped (no isbn)', 'skipped (no data)', 'skipped (no changes)'
    """
    print(f"\n  Processing: {filepath.name}")

    raw = filepath.read_text(encoding="utf-8")
    post = frontmatter.loads(raw)

    # 1. Find ISBN — frontmatter first, then body
    isbn = str(post.metadata.get("ISBN", "")).replace("-", "") or extract_isbn_from_body(post.content)

    if not isbn:
        print(f"    No ISBN found — skipping.")
        return "skipped (no isbn)"

    print(f"    ISBN: {isbn}")

    # 2. Fetch data
    book = fetch_book_data(isbn)
    if not book:
        print(f"    No data returned from either API — skipping.")
        return "skipped (no data)"

    # 3. Migrate ISBN from body to frontmatter (if it was in the body)
    if not post.metadata.get("ISBN"):
        post.content = remove_isbn_from_body(post.content)

    # 4. Enrich frontmatter
    fm_changed = enrich_frontmatter(post, book, isbn)

    # 5. Insert description into body
    original_content = post.content
    post.content = insert_description(post.content, book.get("description", ""))
    post.content = ensure_review_section(post.content)
    body_changed = post.content != original_content

    if not fm_changed and not body_changed:
        print(f"    All fields already populated — no changes made.")
        return "skipped (no changes)"

    # 6. Write back
    filepath.write_text(frontmatter.dumps(post), encoding="utf-8")
    print(f"    Done.")
    return "enriched"


def create_new_file(isbn: str, output_dir: Path) -> str:
    """
    Create a brand-new markdown file for the given ISBN.
    Returns a status string.
    """
    print(f"\n  Creating new file for ISBN: {isbn}")

    book = fetch_book_data(isbn)
    if not book:
        print(f"    No data returned from either API — cannot create file.")
        return "failed (no data)"

    title = book.get("title", f"Unknown_{isbn}")
    safe_title = re.sub(r'[<>:"/\\|?*]', "", title).strip()
    filename = output_dir / f"{safe_title}.md"

    if filename.exists():
        print(f"    File already exists: {filename.name} — skipping.")
        return "skipped (file exists)"

    cover_url = book.get("cover_url", "")
    cover_image = f"![{title}|200]({cover_url})" if cover_url else ""
    description = book.get("description", "")
    desc_block = f"\n## Description\n{description}\n" if description else ""

    metadata = {
        "Base Image": cover_url,
        "Reading Status": "Wishlist",
        "Rating": "",
        "ISBN": isbn,
        "Title": title,
        "Author": book.get("author", ""),
        "Publisher": book.get("publisher", ""),
        "Year": book.get("year", ""),
        "Pages": book.get("pages", ""),
        "Genres": book.get("genres", []),
    }

    post = frontmatter.Post(
        content=f"\n{cover_image}\n{desc_block}\n## Review\n",
        **metadata,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    filename.write_text(frontmatter.dumps(post), encoding="utf-8")
    print(f"    Created: {filename.name}")
    return "created"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Enrich Obsidian book markdown files with metadata from Open Library / Google Books."
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("file", nargs="?", help="Path to a single markdown file to enrich.")
    group.add_argument("--batch", metavar="FOLDER", help="Path to a folder — enriches all .md files inside.")
    group.add_argument("--new", metavar="ISBN", help="Create a new markdown file for the given ISBN.")

    parser.add_argument(
        "--output",
        metavar="FOLDER",
        default=".",
        help="Output folder for --new mode (default: current directory).",
    )

    args = parser.parse_args()

    # --- Single file mode ---
    if args.file:
        path = Path(args.file)
        if not path.is_file():
            print(f"Error: '{path}' is not a file.")
            sys.exit(1)
        result = process_file(path)
        print(f"\nResult: {path.name} — {result}")

    # --- Batch mode ---
    elif args.batch:
        folder = Path(args.batch)
        if not folder.is_dir():
            print(f"Error: '{folder}' is not a directory.")
            sys.exit(1)

        md_files = sorted(folder.glob("*.md"))
        if not md_files:
            print(f"No .md files found in '{folder}'.")
            sys.exit(0)

        print(f"Found {len(md_files)} markdown file(s) in '{folder}'.\n")
        results = {}
        for md in md_files:
            results[md.name] = process_file(md)

        # Summary
        print("\n" + "─" * 50)
        print("Summary:")
        for name, status in results.items():
            icon = "✓" if status == "enriched" else "—"
            print(f"  {icon}  {name}: {status}")

        enriched = sum(1 for s in results.values() if s == "enriched")
        print(f"\n{enriched}/{len(md_files)} file(s) enriched.")

    # --- New file mode ---
    elif args.new:
        output_dir = Path(args.output)
        result = create_new_file(args.new, output_dir)
        print(f"\nResult: {result}")


if __name__ == "__main__":
    main()
