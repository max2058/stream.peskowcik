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
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import re
import requests
import streamlit as st
import pandas as pd


def build_query(
    *, topic: Optional[str] = "Unser Sandmännchen",
    title_filter: Optional[str] = None,
    size: int = 50,
    offset: int = 0,
) -> str:
    """Construct a JSON query for the MediathekViewWeb API.

    The search can be limited to a specific topic.  A title_filter can be
    provided to restrict results by a search term in the title; if
    ``title_filter`` is None or an empty string, no title filter is applied.

    Args:
        topic: The topic (Sendung) to search for. If None, no topic filter
            will be applied.
        title_filter: A term that must appear in the title (optional).
        size: Number of results to fetch. Larger sizes may be necessary to
            capture older episodes.
        offset: Offset for pagination.

    Returns:
        A JSON‑formatted string for the query parameter.
    """
    queries: List[Dict[str, Any]] = []
    if topic:
        queries.append({"fields": ["topic"], "query": topic})
    if title_filter:
        queries.append({"fields": ["title"], "query": title_filter})
    query: Dict[str, Any] = {
        "queries": queries,
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


def extract_base64_id(url: str) -> Optional[str]:
    """Extract the base64‑encoded publication ID from a MediathekViewWeb url.

    The MediathekViewWeb API returns the ``url_website`` field which often
    ends with a base64‑encoded identifier (e.g. ``.../Y3JpZDovL3JiYl84ZGU4...``).
    This helper returns that identifier if present.

    Args:
        url: The website url returned by MediathekViewWeb.

    Returns:
        The base64 string if one could be extracted, otherwise ``None``.
    """
    if not url:
        return None
    # The ID is the last path segment after the last slash, and it
    # consists of base64 characters (letters, digits, +, /, =)
    parts = url.rstrip("/").split("/")
    candidate = parts[-1]
    # Heuristically check if it looks like base64 (no dots or dashes)
    if re.fullmatch(r"[A-Za-z0-9_\-]+", candidate):
        return candidate
    return None


def is_sorbian_episode(entry: Dict[str, Any]) -> bool:
    """Heuristically determine whether a MediathekViewWeb entry is sorbischsprachig.

    Because calling the ARD API for every entry introduces long loading
    times and may hit network timeouts, this function relies solely on
    pattern matching in the ``title`` and ``description`` fields.

    Known sorbische Folgen typically include the words "sorbisch" or
    "Pěskowčik" (oder "Peskowcik") in ihrem Titel.  Die Folge
    "Fuchs und Elster: Gestörte Angelfreuden" enthält zwar kein
    "sorbisch", hat aber einen einzigartigen Folgentitel.  Daher
    ergänzen wir eine Liste von Schlüsselbegriffen, die auf sorbische
    Inhalte hinweisen.

    Args:
        entry: A result dict from the MediathekViewWeb API.

    Returns:
        True if the entry appears to be sorbischsprachig, otherwise False.
    """
    title = (entry.get("title") or "").lower()
    description = (entry.get("description") or "").lower()
    # simple keywords that almost always mark sorbian episodes
    keywords = [
        "sorbisch",
        "peskowcik",
        "pěskowčik",
        "gestörte angelfreuden",
        "gestoerte angelfreuden",
        "suwa",
        "spewaca",
        "mróčele",
        "mrocele",
    ]
    for kw in keywords:
        if kw in title or kw in description:
            return True
    return False


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
        # We fetch a larger window of results so that recently
        # veröffentlichte sorbische Episoden ohne "sorbisch" im Titel
        # nicht untergehen.  Adjust size if necessary.
        # Fetch the first page of results for "Unser Sandmännchen".  We start
        # with 200 items which correspond to roughly 100 Sendetage (ca. zwei
        # Folgen pro Tag).  We fetch the second page only if needed later.
        query_json = build_query(topic="Unser Sandmännchen", title_filter=None, size=200, offset=0)
        results = fetch_results(query_json)

    if not results:
        st.warning("Es wurden keine passenden Einträge gefunden.")
        return

    # Filter for sorbian episodes.  To avoid long load times, we examine
    # only a subset of the most recent entries and fetch subsequent
    # pages only if necessary.  Checking each entry may trigger an
    # additional network request to the ARD API, so we limit the number
    # of checks.
    sorbian_entries: List[Dict[str, Any]] = []
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=120)  # limit to last 120 Tage
    max_checks = 200  # maximum number of entries to examine per page
    max_results = 15  # maximum number of sorbian episodes to collect
    offset = 0
    while len(sorbian_entries) < max_results:
        # If not the first iteration, fetch the next page
        if offset > 0:
            query_json_page = build_query(topic="Unser Sandmännchen", title_filter=None, size=200, offset=offset)
            page_results = fetch_results(query_json_page)
            if not page_results:
                break
            results_page = page_results
        else:
            results_page = results
        checked = 0
        cutoff_reached = False
        for entry in results_page:
            if checked >= max_checks:
                break
            ts = entry.get("timestamp", 0)
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            if dt < cutoff_date:
                # break early once we reach the cutoff date
                cutoff_reached = True
                break
            if is_sorbian_episode(entry):
                sorbian_entries.append(entry)
                if len(sorbian_entries) >= max_results:
                    break
            checked += 1
        # Determine whether to fetch another page
        if cutoff_reached or len(sorbian_entries) >= max_results:
            break
        # If we examined fewer entries than max_checks it means the page
        # contained fewer than max_checks items, so there is no need to
        # request further pages.
        if checked < max_checks:
            break
        offset += 200

    if not sorbian_entries:
        st.warning("Derzeit sind keine sorbischsprachigen Sandmännchen‑Folgen verfügbar.")
        return

    # Transform sorbian_entries for display
    table_rows: List[Dict[str, Any]] = []
    for entry in sorted(sorbian_entries, key=lambda e: e.get("timestamp", 0), reverse=True):
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
            # Some entries may not have a direct video url (e.g. if geoblocked).  Use the
            # website as fallback when url_video is missing.
            video_url = row["Video"] or row["Website"]
            st.video(video_url)

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
    rss_xml = build_rss(sorbian_entries)
    st.download_button(
        label="RSS‑Feed herunterladen",
        data=rss_xml,
        file_name="sandmaennchen_sorbisch.xml",
        mime="application/rss+xml",
    )


if __name__ == "__main__":
    main()
