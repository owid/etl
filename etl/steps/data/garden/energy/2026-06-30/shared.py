"""Constants shared by the energy garden steps."""

# Provider regions dropped from the reader-facing tables. They are either residual buckets of the
# Energy Institute's own table layout ("Other Western Africa") or regional slices with no definition
# in our regions dataset, so a reader gets no explanation of what they contain. The Statistical
# Review garden keeps them all: they are inputs to our continent aggregates.
# Deliberately kept in the outputs: the defined (EI)/(Ember) regions (they carry a definition on
# hover) and self-explanatory organizations (OPEC, OECD, EU, G7, G20, ASEAN).
EXCLUDED_PROVIDER_REGIONS = [
    "Central America (EI)",
    "Eastern Africa (EI)",
    "Middle Africa (EI)",
    "Middle East and Africa (EI)",
    "Non-OECD (EI)",
    "Non-OPEC (EI)",
    "Other Africa (EI)",
    "Other Asia Pacific (EI)",
    "Other CIS (EI)",
    "Other Caribbean (EI)",
    "Other Eastern Africa (EI)",
    "Other Europe (EI)",
    "Other Middle Africa (EI)",
    "Other Middle East (EI)",
    "Other North America (EI)",
    "Other Northern Africa (EI)",
    "Other South America (EI)",
    "Other South and Central America (EI)",
    "Other Southern Africa (EI)",
    "Other Western Africa (EI)",
    "Rest of World (EI)",
    "Western Africa (EI)",
]
