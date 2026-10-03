"""Load the Microsoft AI Diffusion CSV into a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Column mapping from CSV headers to clean short names.
COLUMN_RENAMES = {
    "Economy": "economy",
    "H1 2025 AI Diffusion": "ai_diffusion_h1_2025",
    "H2 2025 AI Diffusion": "ai_diffusion_h2_2025",
    "Q1 2026 AI Diffusion": "ai_diffusion_q1_2026",
}


def run() -> None:
    snap = paths.load_snapshot("ai_diffusion_msft.csv")
    tb = snap.read_csv(encoding="latin-1")

    # Rename columns.
    tb = tb.rename(columns=COLUMN_RENAMES, errors="raise")

    # Strip "%" signs and convert to float.
    for col in ["ai_diffusion_h1_2025", "ai_diffusion_h2_2025", "ai_diffusion_q1_2026"]:
        tb[col] = tb[col].astype(str).str.replace("%", "", regex=False).astype(float)

    assert len(tb) > 100, f"Expected >100 economies, got {len(tb)}"

    # Check for duplicates.
    assert not tb.duplicated(subset=["economy"]).any(), "Duplicate economies found"

    # Check value ranges.
    for col in ["ai_diffusion_h1_2025", "ai_diffusion_h2_2025", "ai_diffusion_q1_2026"]:
        assert tb[col].between(0, 100).all(), f"Out-of-range values in {col}"

    tb = tb.format(["economy"])

    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
