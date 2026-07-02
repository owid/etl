from etl.catalog_jsonld.sitemap import SitemapEntry, sitemap_xml


def test_sitemap_xml_includes_lastmod_for_date_shaped_versions() -> None:
    xml = sitemap_xml(
        [
            SitemapEntry(url="https://catalog.ourworldindata.org/emissions/owid_co2/", lastmod="2025-12-04"),
        ]
    )

    assert "<loc>https://catalog.ourworldindata.org/emissions/owid_co2/</loc>" in xml
    assert "<lastmod>2025-12-04</lastmod>" in xml


def test_sitemap_xml_omits_lastmod_when_not_date_shaped() -> None:
    xml = sitemap_xml(
        [
            SitemapEntry(url="https://catalog.ourworldindata.org/open_numbers/gapminder/", lastmod="latest"),
            SitemapEntry(url="https://catalog.ourworldindata.org/wb/no_lastmod/", lastmod=None),
        ]
    )

    assert "<lastmod>" not in xml
    assert "<loc>https://catalog.ourworldindata.org/open_numbers/gapminder/</loc>" in xml
    assert "<loc>https://catalog.ourworldindata.org/wb/no_lastmod/</loc>" in xml


def test_sitemap_xml_sorts_entries_by_url() -> None:
    xml = sitemap_xml(
        [
            SitemapEntry(url="https://catalog.ourworldindata.org/wb/world_bank_pip/", lastmod="2026-03-24"),
            SitemapEntry(url="https://catalog.ourworldindata.org/emissions/owid_co2/", lastmod="2025-12-04"),
        ]
    )

    first_loc = xml.index("emissions/owid_co2")
    second_loc = xml.index("wb/world_bank_pip")
    assert first_loc < second_loc


def test_sitemap_xml_empty_list_produces_valid_empty_urlset() -> None:
    xml = sitemap_xml([])

    assert "<urlset" in xml
    assert "<url>" not in xml
