"""
Build the USDA FoodData Central branded-foods corpus for KNN enrichment.

Downloads, processes, embeds, and writes:
  corpus/faiss_index.bin       — FAISS IndexFlatIP, dim=384, L2-normalized
  corpus/corpus_metadata.json  — list of {"category": str, "product_name": str}
  corpus/corpus_summary.json   — human-readable build stats

Run once before any pipeline run, or to refresh the corpus.

Usage:
  python scripts/build_corpus.py
  python scripts/build_corpus.py --limit 10000
  python scripts/build_corpus.py --help
"""

import argparse
import json
import sys
import traceback
import zipfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths (all relative to the ETL project root, i.e. parent of this script)
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent

RAW_DIR = ROOT / "data" / "usda_raw"
ZIP_PATH = RAW_DIR / "branded_food_download.zip"

DOWNLOAD_URL = (
    "https://fdc.nal.usda.gov/fdc-datasets/"
    "FoodData_Central_branded_food_csv_2024-10-31.zip"
)

EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
EMBED_BATCH_SIZE = 256

# ---------------------------------------------------------------------------
# Taxonomy mapping  USDA branded_food_category → pipeline primary_category
# ---------------------------------------------------------------------------
USDA_TO_PIPELINE_CATEGORY = {
    # Dairy
    "Cheese": "Dairy",
    "Cheese Substitutes": "Dairy",
    "Dairy Based Snack": "Dairy",
    "Ice Cream & Frozen Dairy Desserts": "Dairy",
    "Milk": "Dairy",
    "Milk Based Beverages": "Dairy",
    "Yogurt": "Dairy",
    # Meat & Poultry
    "Beef Products": "Meat & Poultry",
    "Chicken": "Meat & Poultry",
    "Luncheon Meats/Cold Cuts": "Meat & Poultry",
    "Meat": "Meat & Poultry",
    "Pork": "Meat & Poultry",
    "Poultry": "Meat & Poultry",
    "Sausage": "Meat & Poultry",
    "Turkey": "Meat & Poultry",
    # Seafood
    "Fish": "Seafood",
    "Seafood": "Seafood",
    "Seafood Products": "Seafood",
    "Shellfish": "Seafood",
    # Bakery
    "Bagels/English Muffins": "Bakery",
    "Baked Products": "Bakery",
    "Bread & Bakery Products": "Bakery",
    "Breads & Buns": "Bakery",
    "Cakes, Cupcakes, Snack Cakes": "Bakery",
    "Cookies & Biscuits": "Bakery",
    "Crackers": "Bakery",
    "Muffins": "Bakery",
    "Pies & Tarts": "Bakery",
    "Rolls & Buns": "Bakery",
    "Tortillas & Wraps": "Bakery",
    # Breakfast Cereals
    "Breakfast Cereals": "Breakfast Cereals",
    "Hot Cereals": "Breakfast Cereals",
    "Ready-to-Eat Cereal": "Breakfast Cereals",
    # Beverages
    "Beverages": "Beverages",
    "Carbonated Soft Drink": "Beverages",
    "Coffee & Tea": "Beverages",
    "Energy Drinks": "Beverages",
    "Fruit & Vegetable Juice": "Beverages",
    "Fruit Drinks": "Beverages",
    "Sports & Energy Drinks": "Beverages",
    "Water": "Beverages",
    # Snacks
    "Chips, Pretzels & Snacks": "Snacks",
    "Crackers & Pretzels": "Snacks",
    "Fruit Snacks": "Snacks",
    "Granola Bars": "Snacks",
    "Popcorn, Peanuts, Seeds & Related Snacks": "Snacks",
    "Rice Cakes": "Snacks",
    "Snack/Granola Bars": "Snacks",
    "Trail Mix": "Snacks",
    # Condiments
    "Condiments": "Condiments",
    "Dips, Spreads, Seasonings & Dressings": "Condiments",
    "Dressings & Salad Dressings": "Condiments",
    "Gravy": "Condiments",
    "Hot Sauce & Salsa": "Condiments",
    "Ketchup": "Condiments",
    "Mayonnaise": "Condiments",
    "Mustard & Mayonnaise": "Condiments",
    "Pickles, Olives, Peppers & Relishes": "Condiments",
    "Salsa & Dips": "Condiments",
    "Sauces & Gravies": "Condiments",
    "Soy Sauce & Condiments": "Condiments",
    "Vinegar & Cooking Wine": "Condiments",
    # Frozen Foods
    "Frozen Appetizers & Snacks": "Frozen Foods",
    "Frozen Breakfast": "Frozen Foods",
    "Frozen Dinners": "Frozen Foods",
    "Frozen Entrees": "Frozen Foods",
    "Frozen Meals": "Frozen Foods",
    "Frozen Pizza": "Frozen Foods",
    "Frozen Potatoes": "Frozen Foods",
    "Frozen Vegetables": "Frozen Foods",
    # Fruits
    "Fresh Fruits": "Fruits",
    "Fruit": "Fruits",
    "Fruit Butters, Jams & Jellies": "Fruits",
    # Vegetables
    "Fresh Vegetables": "Vegetables",
    "Vegetable Products": "Vegetables",
    "Vegetables": "Vegetables",
    # Pasta & Grains
    "Dry Pasta, Rice & Beans": "Pasta & Grains",
    "Grain Products": "Pasta & Grains",
    "Pasta": "Pasta & Grains",
    "Rice": "Pasta & Grains",
    "Rice & Grain Products": "Pasta & Grains",
    # Soups
    "Broth & Stocks": "Soups",
    "Soups": "Soups",
    "Soups & Broths": "Soups",
    # Canned Foods
    "Canned Beans": "Canned Foods",
    "Canned Fish & Seafood": "Canned Foods",
    "Canned Fruits": "Canned Foods",
    "Canned Meat & Poultry": "Canned Foods",
    "Canned Tomatoes": "Canned Foods",
    "Canned Vegetables": "Canned Foods",
    # Baby Food
    "Baby Food": "Baby Food",
    "Baby Foods": "Baby Food",
    "Infant & Toddler Foods": "Baby Food",
    "Infant Formula": "Baby Food",
    # Confectionery
    "Candy": "Confectionery",
    "Candy & Gum": "Confectionery",
    "Chocolate": "Confectionery",
    "Gum": "Confectionery",
    "Hard Candy": "Confectionery",
    # Supplements
    "Dietary Supplements": "Supplements",
    "Nutrition Bars": "Supplements",
    "Protein Bars": "Supplements",
    "Protein Supplements": "Supplements",
    "Vitamins & Minerals": "Supplements",
    # Deli
    "Deli Meats": "Deli",
    "Deli Salads": "Deli",
    "Prepared Salads": "Deli",
    # Pet Food
    "Cat Food": "Pet Food",
    "Dog Food": "Pet Food",
    "Pet Food": "Pet Food",
    "Pet Snacks & Treats": "Pet Food",
}


# ---------------------------------------------------------------------------
# Dependency guards
# ---------------------------------------------------------------------------

def _require_faiss():
    try:
        import faiss  # noqa: F401
    except ImportError:
        print(
            "faiss-cpu not installed. Run: pip install faiss-cpu",
            file=sys.stderr,
        )
        sys.exit(1)


def _require_sentence_transformers():
    try:
        import sentence_transformers  # noqa: F401
    except ImportError:
        print(
            "sentence-transformers not installed. "
            "Run: pip install sentence-transformers",
            file=sys.stderr,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Step 1: Download
# ---------------------------------------------------------------------------

def download_raw(skip_if_exists: bool = True) -> None:
    import urllib.request

    try:
        from tqdm import tqdm
        _has_tqdm = True
    except ImportError:
        _has_tqdm = False

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if skip_if_exists and ZIP_PATH.exists():
        print(f"Zip already exists at {ZIP_PATH}, skipping download.")
    else:
        print(f"Downloading USDA FoodData Central from:\n  {DOWNLOAD_URL}")

        try:
            response = urllib.request.urlopen(DOWNLOAD_URL)
        except Exception as e:
            code = getattr(e, "code", None)
            if code == 404:
                print(
                    "USDA download failed (404). Check "
                    "https://fdc.nal.usda.gov/download-datasets/ "
                    "for the current filename and update the URL in this script.",
                    file=sys.stderr,
                )
                sys.exit(1)
            raise

        total = int(response.headers.get("Content-Length", 0))
        downloaded = 0
        last_reported_mb = 0
        chunk_size = 64 * 1024  # 64 KB

        if _has_tqdm and total:
            bar = tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="Downloading",
            )
        else:
            bar = None

        with open(ZIP_PATH, "wb") as f:
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if bar:
                    bar.update(len(chunk))
                else:
                    mb = downloaded // (10 * 1024 * 1024)
                    if mb > last_reported_mb:
                        print(
                            f"  Downloaded {downloaded / 1e6:.0f} MB"
                            + (f" / {total / 1e6:.0f} MB" if total else ""),
                        )
                        last_reported_mb = mb

        if bar:
            bar.close()
        print(f"Saved to {ZIP_PATH}")

    # Extract only the two CSVs we need
    needed = {"branded_food.csv", "food.csv"}
    already_extracted = all((RAW_DIR / name).exists() for name in needed)
    if already_extracted:
        print("CSV files already extracted, skipping extraction.")
        return

    print("Extracting branded_food.csv and food.csv …")
    try:
        with zipfile.ZipFile(ZIP_PATH) as zf:
            members = zf.namelist()
            for member in members:
                basename = Path(member).name
                if basename in needed:
                    # Extract flat into RAW_DIR
                    data = zf.read(member)
                    (RAW_DIR / basename).write_bytes(data)
                    print(f"  Extracted {basename}")
    except zipfile.BadZipFile as e:
        print(
            f"Zip file appears corrupt: {e}\n"
            "Delete data/usda_raw/ and re-run to download fresh copies.",
            file=sys.stderr,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Step 2: Load and join
# ---------------------------------------------------------------------------

def load_and_join() -> "pd.DataFrame":
    import pandas as pd

    food_path = RAW_DIR / "food.csv"
    branded_path = RAW_DIR / "branded_food.csv"

    try:
        food = pd.read_csv(food_path, low_memory=False)
    except Exception as e:
        print(
            f"Could not read {food_path}: {e}\n"
            "Delete data/usda_raw/ and re-run.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        branded = pd.read_csv(branded_path, low_memory=False)
    except Exception as e:
        print(
            f"Could not read {branded_path}: {e}\n"
            "Delete data/usda_raw/ and re-run.",
            file=sys.stderr,
        )
        sys.exit(1)

    food = food[food["data_type"] == "branded_food"][["fdc_id", "description"]]
    branded = branded[[
        "fdc_id",
        "brand_owner",
        "brand_name",
        "branded_food_category",
        "gtin_upc",
        "ingredients",
        "modified_date",
    ]]

    df = food.merge(branded, on="fdc_id", how="inner")
    print(f"Loaded: {len(df):,} rows")
    return df


# ---------------------------------------------------------------------------
# Step 3: Filter
# ---------------------------------------------------------------------------

def filter_and_map(df: "pd.DataFrame", min_text_len: int) -> tuple["pd.DataFrame", int, int]:
    import pandas as pd

    # Deduplicate by gtin_upc: keep most recent record per barcode.
    # Rows without a gtin_upc are kept unconditionally.
    df["modified_date"] = pd.to_datetime(df["modified_date"], errors="coerce")
    has_upc = df["gtin_upc"].notna() & (df["gtin_upc"].astype(str).str.strip() != "")
    with_upc = (
        df[has_upc]
        .sort_values("modified_date", ascending=False)
        .drop_duplicates(subset="gtin_upc", keep="first")
    )
    without_upc = df[~has_upc]
    df = pd.concat([with_upc, without_upc], ignore_index=True)
    print(f"After dedup:              {len(df):,} rows")

    # Drop rows with null/empty branded_food_category
    df = df[df["branded_food_category"].notna() & (df["branded_food_category"].str.strip() != "")]
    print(f"After category filter:    {len(df):,} rows")

    # Map USDA category → pipeline taxonomy
    before_map = len(df)
    df["pipeline_category"] = df["branded_food_category"].map(USDA_TO_PIPELINE_CATEGORY)
    df = df[df["pipeline_category"].notna()]
    rows_dropped_unmapped = before_map - len(df)
    print(
        f"After taxonomy mapping:   {len(df):,} rows "
        f"(dropped {rows_dropped_unmapped:,} unmapped)"
    )

    # Build combined text
    def build_text(row):
        parts = []
        for col in ["description", "brand_owner", "ingredients"]:
            val = row[col]
            if pd.notna(val) and str(val).strip():
                parts.append(str(val).strip())
        return " ".join(parts)

    df["combined_text"] = df.apply(build_text, axis=1)

    # Apply min text length filter
    before_short = len(df)
    df = df[df["combined_text"].astype(str).str.len() >= min_text_len]
    rows_dropped_short = before_short - len(df)
    if rows_dropped_short:
        print(f"After min-text-len filter: {len(df):,} rows (dropped {rows_dropped_short:,} short)")

    return df, rows_dropped_unmapped, rows_dropped_short


# ---------------------------------------------------------------------------
# Step 5: Embed
# ---------------------------------------------------------------------------

def embed(texts: list[str]) -> "np.ndarray":
    import numpy as np
    import faiss
    from sentence_transformers import SentenceTransformer

    try:
        from tqdm import tqdm as _tqdm  # noqa: F401
        show_progress = True
    except ImportError:
        show_progress = False

    n = len(texts)
    mem_mb = n * EMBEDDING_DIM * 4 / 1e6
    print(f"Embedding {n:,} rows. Estimated memory: ~{mem_mb:.0f} MB")

    if n > 200_000:
        print(
            f"Large corpus ({n:,} rows). This will take several minutes "
            f"and ~{mem_mb:.0f} MB RAM.\n"
            "Use --limit 50000 to test with a subset first."
        )

    model = SentenceTransformer(EMBEDDING_MODEL)
    embeddings = model.encode(
        texts,
        batch_size=EMBED_BATCH_SIZE,
        show_progress_bar=show_progress,
        convert_to_numpy=True,
    )
    embeddings = embeddings.astype(np.float32)
    faiss.normalize_L2(embeddings)
    return embeddings


# ---------------------------------------------------------------------------
# Step 6: Build FAISS index and save
# ---------------------------------------------------------------------------

def build_and_save(
    df: "pd.DataFrame",
    embeddings: "np.ndarray",
    output_dir: Path,
    min_text_len: int,
    rows_dropped_unmapped: int,
    rows_dropped_short: int,
) -> None:
    import faiss
    import numpy as np

    output_dir.mkdir(parents=True, exist_ok=True)

    index = faiss.IndexFlatIP(EMBEDDING_DIM)
    index.add(embeddings.astype(np.float32))

    metadata = [
        {"category": row["pipeline_category"], "product_name": str(row["description"])}
        for _, row in df.iterrows()
    ]

    index_path = output_dir / "faiss_index.bin"
    meta_path = output_dir / "corpus_metadata.json"
    summary_path = output_dir / "corpus_summary.json"

    faiss.write_index(index, str(index_path))
    with open(meta_path, "w") as f:
        json.dump(metadata, f)

    # Category distribution
    cat_dist = df["pipeline_category"].value_counts().to_dict()

    summary = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "source": "USDA FoodData Central Branded Foods",
        "total_vectors": int(index.ntotal),
        "category_distribution": {k: int(v) for k, v in cat_dist.items()},
        "embedding_model": EMBEDDING_MODEL,
        "min_text_len": min_text_len,
        "rows_dropped_unmapped": rows_dropped_unmapped,
        "rows_dropped_short_text": rows_dropped_short,
    }
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    # Print summary
    print(f"\nCorpus built: {index.ntotal:,} vectors across {len(cat_dist)} categories")
    print("Category distribution:")
    for cat, count in sorted(cat_dist.items(), key=lambda x: -x[1]):
        print(f"  {cat:<30} {count:>10,}")

    return index, metadata


# ---------------------------------------------------------------------------
# Step 7: Validate
# ---------------------------------------------------------------------------

def validate(df: "pd.DataFrame", embeddings: "np.ndarray", index, metadata: list[dict]) -> None:
    import numpy as np
    import faiss

    n = len(df)
    check_indices = [0, n // 2, n - 1]
    df_reset = df.reset_index(drop=True)

    print("\nSpot checks:")
    all_passed = True
    for i in check_indices:
        row = df_reset.iloc[i]
        q = embeddings[i : i + 1].astype(np.float32)
        sims, idxs = index.search(q, 3)
        top_idx = int(idxs[0][0])
        top_cat = metadata[top_idx]["category"]
        top_sim = float(sims[0][0])
        product = str(row["description"])[:60]
        expected = row["pipeline_category"]
        passed = top_cat == expected
        if not passed:
            all_passed = False
        status = "" if passed else " [MISMATCH]"
        print(
            f"  Spot check [{i}]: '{product}' → "
            f"top match: '{top_cat}' (sim={top_sim:.3f}){status}"
        )

    if not all_passed:
        print(
            "Warning: one or more spot checks had a category mismatch. "
            "The index is saved; review the taxonomy mapping if this is unexpected.",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build USDA FoodData Central corpus for KNN enrichment.",
        epilog="""
Examples:
  # First time setup
  python scripts/build_corpus.py

  # Test with 10k rows
  python scripts/build_corpus.py --limit 10000

  # Rebuild with fresh download
  rm -rf data/usda_raw/ corpus/
  python scripts/build_corpus.py
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only process N rows (for testing, default: no limit)",
    )
    parser.add_argument(
        "--min-text-len",
        type=int,
        default=10,
        metavar="N",
        help="Drop rows where combined text is shorter than N chars (default: 10)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "corpus",
        metavar="DIR",
        help="Where to write corpus files (default: corpus/)",
    )
    return parser.parse_args()


def main() -> None:
    _require_faiss()
    _require_sentence_transformers()

    args = parse_args()

    # Step 1: Download
    download_raw()

    # Step 2: Load and join
    df = load_and_join()

    # Optional row limit (applied before filtering so counts are honest)
    if args.limit is not None:
        df = df.head(args.limit)
        print(f"[--limit] Capped to {len(df):,} rows for testing.")

    # Step 3: Filter and map
    df, rows_dropped_unmapped, rows_dropped_short = filter_and_map(df, args.min_text_len)

    if df.empty:
        print("No rows remain after filtering. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Step 5: Embed
    texts = df["combined_text"].tolist()
    embeddings = embed(texts)

    # Step 6: Build FAISS index and save
    index, metadata = build_and_save(
        df,
        embeddings,
        args.output_dir,
        args.min_text_len,
        rows_dropped_unmapped,
        rows_dropped_short,
    )

    # Step 7: Validate
    validate(df, embeddings, index, metadata)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        print("\nUnexpected error:", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
