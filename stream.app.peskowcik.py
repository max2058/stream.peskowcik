"""
Streamlit app to display sorbischsprachige Folgen von "Unser Sandmännchen".

Dieses Skript ruft die MediathekViewWeb‑API auf und filtert nach
Folgen, bei denen das Thema "Unser Sandmännchen" ist und deren Titel
den Begriff "sorbisch" enthalten. Die Ergebnisse werden in einer
tabelle angezeigt und können als einfacher RSS‑Feed heruntergeladen
werden.

Die App ist so gestaltet, dass sie sich auf streamlit.io hosten lässt.
Die API ist öffentlich zugänglich, daher benötigt der Aufruf keine
Authentifizierung.
"""

import json
from datetime import datetime, timezone
from typing import List, Dict, Any

import requests
import streamlit as st
import pandas as pd


def build_query(topic: str = "Unser Sandmännchen", title_filter: str = "sorbisch", *, size: int = 20, offset: int = 0) -> str:
    """Construct a JSON query for the MediathekViewWeb API.

    Args:
        topic: The topic (Sendung) to search for.
        title_filter: A term that must appear in the title.
        size: Number of results to fetch.
        offset: Offset for pagination.

    Returns:
        A JSON‑formatted string for the query parameter.
    """
    query: Dict[str, Any] = {
        "queries": [
            {"fields": ["topic"], "query": topic},
            {"fields": ["title"], "query": title_filter},
        ],
        "sortBy": "timestamp",
        "sortOrder": "desc",
        "future": False,
        "offset": offset,
        "size": size,
    }
    return json.dumps(query)


def fetch_results(query_json: str) -> List[Dict[str, Any]]:
    """Call the MediathekViewWeb API and return the results list.

    Args:
        query_json: The JSON query string to include in the URL.

    Returns:
        A list of result dictionaries. Returns an empty list on error.
    """
    base_url = "https://mediathekviewweb.de/api/query"
    params = {"query": query_json}
    try:
        resp = requests.get(base_url, params=params, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        st.error(f"Fehler beim Abrufen der API: {exc}")
        return []
    try:
        data = resp.json()
    except json.JSONDecodeError:
        st.error("Konnte die API‑Antwort nicht als JSON interpretieren.")
        return []
    # API returns a dict with keys: result, err
    results = data.get("result", {}).get("results", [])
    return results


def convert_timestamp(ts: int) -> str:
    """Convert Unix timestamp (seconds) to RFC822 formatted string."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%a, %d %b %Y %H:%M:%S %z")


def build_rss(results: List[Dict[str, Any]]) -> str:
    """Generate a minimal RSS feed from the results.

    Args:
        results: List of API result dicts.

    Returns:
        A string containing RSS XML.
    """
    from xml.sax.saxutils import escape

    channel_title = "Unser Sandmännchen – sorbische Folgen"
    channel_link = "https://www.sandmann.de"
    channel_description = "RSS‑Feed mit sorbischsprachigen Folgen aus der MediathekViewWeb‑API"

    items_xml = []
    for entry in results:
        title = escape(entry.get("title", ""))
        description = escape(entry.get("description", ""))
        pub_date = convert_timestamp(entry.get("timestamp", 0))
        link = escape(entry.get("url_website", ""))
        enclosure_url = escape(entry.get("url_video", ""))
        duration = entry.get("duration")
        item_xml = f"""
        <item>
            <title>{title}</title>
            <description>{description}</description>
            <link>{link}</link>
            <guid>{link}</guid>
            <pubDate>{pub_date}</pubDate>
            <enclosure url="{enclosure_url}" length="{duration}" type="video/mp4" />
        </item>
        """
        items_xml.append(item_xml.strip())

    rss_xml = f"""
    <?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
      <channel>
        <title>{channel_title}</title>
        <link>{channel_link}</link>
        <description>{channel_description}</description>
        {''.join(items_xml)}
      </channel>
    </rss>
    """
    return rss_xml.strip()


def main() -> None:
    st.set_page_config(page_title="Sandmännchen Sorbisch", layout="wide")
    st.image(
        "https://www.mdr.de/sandmann/sandmann824-resimage_v-variantBig24x9_w-2560.jpg?version=55897",
        use_container_width=True,
    )
    st.title("Unser Sandmännchen – Sorbische Folgen")
    st.write(
        "Um sich nicht mit der KiKA- oder ARD-Mediathek herumärgern zu müssen und die wenigen aktuell verfügbaren sorbischen Folgen schnell griffbereit zu haben, gibt es diese App."
    )
    st.write(
        "Diese App nutzt die offene MediathekViewWeb‑API, um sorbischsprachige Sandmännchen‑Folgen zu finden und anzuzeigen. https://github.com/max2058/stream.peskowcik"
    )
    
    # API Query and Fetch
    with st.spinner("Lade Daten von der Mediathek…"):
        query_json = build_query()
        results = fetch_results(query_json)

    if not results:
        st.warning("Es wurden keine passenden Einträge gefunden.")
        return

    # Transform results for display
    table_rows = []
    for entry in results:
        row = {
            "Titel": entry.get("title"),
            "Beschreibung": entry.get("description"),
            "Datum": datetime.fromtimestamp(entry.get("timestamp", 0)).strftime("%d.%m.%Y"),
            "Video": entry.get("url_video"),
            "Website": entry.get("url_website"),
        }
        table_rows.append(row)

    st.subheader("Folgen abspielen")
    for row in table_rows:
        with st.expander(f"{row['Titel']} ({row['Datum']})"):
            st.write(row["Beschreibung"])
            st.video(row["Video"])

    st.subheader("Gefundene Folgen")
    df = pd.DataFrame(table_rows)
    df_display = df.drop(columns=["Video"])
    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Beschreibung": st.column_config.TextColumn("Beschreibung", width="medium"),
            "Website": st.column_config.LinkColumn("Website", display_text="zur Seite"),
        },
    )

    # Provide a download button for RSS
    rss_xml = build_rss(results)
    st.download_button(
        label="RSS‑Feed herunterladen",
        data=rss_xml,
        file_name="sandmaennchen_sorbisch.xml",
        mime="application/rss+xml",
    )


if __name__ == "__main__":
    main()
