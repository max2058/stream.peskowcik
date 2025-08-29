"""
Eine Streamlit App für sorbischsprachige Folgen vom Sandmann – Pěskowčik.

Dieses Skript ruft die MediathekViewWeb‑API auf und filtert nach entsprechenden
Folgen. 
Die Ergebnisse werden in einer Tabelle angezeigt und können als einfacher RSS‑Feed heruntergeladen
werden.

Die App ist so gestaltet, dass sie sich auf streamlit.io hosten lässt.
Die API ist öffentlich zugänglich, daher benötigt der Aufruf keine
Authentifizierung.
"""

import json
from html import escape as html_escape
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import re
import base64
import requests
import streamlit as st
import pandas as pd
from pathlib import Path
import streamlit.components.v1 as components


def _load_default_thumbnail_bytes() -> bytes:
    """Load thumbnail bytes from common locations or fall back to a tiny PNG.

    Tries `assets/images/sandmann_preview.png` relative to this file first,
    then the legacy location next to the script. If neither exists, returns a
    1x1 transparent PNG so the app can still render without crashing.
    """
    candidates = [
        Path(__file__).parent / "assets" / "images" / "sandmann_preview.png",
        Path(__file__).with_name("sandmann_preview.png"),
    ]
    for p in candidates:
        try:
            if p.exists():
                return p.read_bytes()
        except Exception:
            # ignore and try next
            pass
    # 1x1 transparent PNG (base64)
    pixel_b64 = (
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGNgYAAAAAMAASsJTYQAAAAASUVORK5CYII="
    )
    return base64.b64decode(pixel_b64)


DEFAULT_THUMBNAIL = _load_default_thumbnail_bytes()
# Data URL for use as <video poster="...">
THUMBNAIL_DATA_URL = "data:image/png;base64," + base64.b64encode(DEFAULT_THUMBNAIL).decode("ascii")


def build_query(
    *, topic: Optional[str] = "Unser Sandmännchen",
    title_filter: Optional[str] = None,
    size: int = 50,
    offset: int = 0,
) -> str:
    """Construct a JSON query for the MediathekViewWeb API.

    The search can be limited to a specific topic. A title_filter can be
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
    # The ID is the last path segment after the last slash.
    parts = url.rstrip("/").split("/")
    candidate = parts[-1]
    # Only return IDs that look like ARD's base64-encoded CRIDs.  These
    # identifiers always start with "Y3Jp" (the base64 encoding of "crid").
    # We avoid returning shorter slugs (e.g. Kika URLs), because those
    # would defeat our deduplication logic.  If the candidate does not
    # start with this prefix, we consider it not to be a base64 ID.
    if candidate.startswith("Y3Jp"):
        return candidate
    return None


def resolve_base64_from_url(source_url: str) -> Optional[str]:
    """Try to resolve a base64 ARD publication ID from a URL.

    - If the URL already points to ARD Mediathek with a trailing base64 segment,
      reuse :func:`extract_base64_id`.
    - If the URL is an MDR page, download the HTML and scan for an embedded
      ARD base64 ID (pattern starting with ``Y3Jp``).

    Returns the base64 ID if found, otherwise ``None``.
    """
    try:
        # Fast path: extract directly if present in the URL
        direct = extract_base64_id(source_url)
        if direct:
            return direct
        # Fallback: fetch HTML and search for a base64 CRID token
        if source_url.startswith("http"):
            resp = requests.get(source_url, timeout=10)
            resp.raise_for_status()
            html = resp.text
            m = re.search(r"(Y3JpZDovL[^'\"<>\s]+)", html)
            if m:
                return m.group(1)
    except Exception:
        return None
    return None


def fetch_mdr_episode(url: str) -> Optional[Dict[str, Any]]:
    """Try to extract video and metadata from an MDR video page.

    Heuristics:
    - Prefer direct MP4 links; otherwise accept HLS (m3u8).
    - Read OpenGraph meta for title/description if present.
    Returns a minimal entry dict or None on failure.
    """
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        html = resp.text
    except Exception:
        return None

    # Extract candidate video URLs (mp4 preferred, m3u8 fallback)
    urls = []
    try:
        # Common patterns in <source>, JSON, data attributes
        urls = re.findall(r"https?://[^'\"\s>]+\.(?:mp4|m3u8)(?:\?[^'\"\s<]*)?", html, flags=re.I)
    except Exception:
        urls = []
    # Deduplicate preserving order
    seen = set()
    cand_urls: List[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            cand_urls.append(u)

    def _pick_best(candidates: List[str]) -> Optional[str]:
        if not candidates:
            return None
        # Prefer mp4; otherwise first m3u8
        for u in candidates:
            if u.lower().endswith(".mp4"):
                return u
        return candidates[0]

    video_url = _pick_best(cand_urls)

    # Second attempt: look for an explicit embed URL and parse it
    if not video_url:
        # Try <meta property="og:video" ...>, twitter:player or obvious embed paths
        embed_url = None
        for prop in ["video", "video:url"]:
            m = re.search(rf'<meta[^>]+property=["\']og:{prop}["\'][^>]+content=["\']([^"\']+)["\']', html, flags=re.I)
            if m:
                embed_url = m.group(1)
                break
        if not embed_url:
            # Derive from numeric id in URL
            m = re.search(r"video-(\d+)\.html", url)
            if m:
                embed_url = f"https://www.mdr.de/mediathek/embed/video-{m.group(1)}.html"
        if embed_url:
            try:
                eresp = requests.get(embed_url, timeout=10)
                eresp.raise_for_status()
                ehtml = eresp.text
                eurls = re.findall(r"https?://[^'\"\s>]+\.(?:mp4|m3u8)(?:\?[^'\"\s<]*)?", ehtml, flags=re.I)
                video_url = _pick_best(eurls)
                # If still nothing, scan for JSON with src fields
                if not video_url:
                    m2 = re.search(r"src\s*:\s*['\"](https?://[^'\"]+\.(?:mp4|m3u8)[^'\"]*)['\"]", ehtml)
                    if m2:
                        video_url = m2.group(1)
            except Exception:
                pass

    # Extract OpenGraph title/description if available
    def _meta(name: str) -> Optional[str]:
        m = re.search(rf'<meta[^>]+property=["\']og:{name}["\'][^>]+content=["\']([^"\']+)["\']', html, flags=re.I)
        if m:
            return m.group(1)
        return None

    title = _meta("title") or ""
    description = _meta("description") or ""

    # Optionally parse date from the page if present (dd.mm.yyyy)
    # We look for patterns like " 22.08.2021" close to "So"/"Mo" etc.
    ts = 0
    m_date = re.search(r"(\b\d{2}\.\d{2}\.\d{4}\b)", html)
    if m_date:
        ts = _parse_de_date_to_ts(m_date.group(1))

    if not (title or description or video_url):
        return None

    return {
        "channel": "MDR",
        "topic": "Unser Sandmännchen",
        "title": title,
        "description": description,
        "timestamp": ts,
        "duration": None,
        "size": None,
        "url_website": url,
        "url_video": video_url,
    }


def is_sorbian_episode(entry: Dict[str, Any]) -> bool:
    """Heuristically determine whether a MediathekViewWeb entry is sorbischsprachig.

    Because calling the ARD API for every entry introduces long loading
    times and may hit network timeouts, this function relies solely on
    pattern matching in the ``title`` and ``description`` fields.

    Known sorbische Folgen typically include the words "sorbisch" or
    "Pěskowčik" (oder "Peskowcik") in ihrem Titel. Die Folge
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
        "jablucina",
        "jabłucina",
        "liska",
        "sroka",
    ]
    for kw in keywords:
        if kw in title or kw in description:
            return True
    return False

def sorbian_score(entry: Dict[str, Any]) -> int:
    """Return a score indicating how likely the entry is to be sorbischsprachig.

    The score is based on the presence of strong keywords ("sorbisch" or
    variations of "Pěskowčik"), the appearance of Sorbian diacritic
    characters in the title or description, and the word "sorbisch" in
    the URL.  A higher score means the entry is more likely to be sorbisch.

    Args:
        entry: A result dict from the MediathekViewWeb API.

    Returns:
        An integer score (0 or higher).
    """
    if not entry:
        return 0
    title = (entry.get("title") or "").lower()
    description = (entry.get("description") or "").lower()
    url_web = (entry.get("url_website") or "").lower()
    url_vid = (entry.get("url_video") or "").lower()
    score = 0
    # Strong keywords give highest weight
    if any(kw in title or kw in description for kw in ["sorbisch", "peskowcik", "pěskowčik"]):
        score += 3
    # Detect Sorbian diacritics (characters outside basic ASCII range)
    if re.search(r"[ěščžćłńáóśźżĚŠČŽĆŁŃÁÓŚŹŻ]", title + description):
        score += 2
    # 'sorbisch' in the URL adds one point
    if "sorbisch" in url_web or "sorbisch" in url_vid:
        score += 1
    return score

# IDs of episodes known to be sorbischsprachig but which may not yet be listed
# in the MediathekView database.  Each ID corresponds to the base64
# publication identifier in the ARD Mediathek URL.
MANUAL_EPISODES: List[str] = [
    # Pěskowčik: Plumps: Suwa mróčele | 29.06.2025
    "Y3JpZDovL3JiYl8wMjk5OTNlZS1kOTI4LTRmNjUtYTMzNy00Y2U0MzA4ZDBjMjRfcHVibGljYXRpb24",
    # Pěskowčik: Liška a sroka: Špewaca lisca wopus | 06.07.2025
    "Y3JpZDovL3JiYl8xNDYwZDFhZS1hYTBkLTQ5YjctYTRlYy1kZDZiOWVmNjI1OWRfcHVibGljYXRpb24",

    # Fuchs und Elster: Gestörte Angelfreuden (sorbisch) | 24.08.2025
    "Y3JpZDovL3JiYl80MDE1ZGU4MS01ZjQwLTRhOWItYjdlNi1kZTQ3ZGU2M2Y5MTVfcHVibGljYXRpb24",
]

# Optional additional sources (URLs) that might not show up in MediathekView yet.
# These can be ARD Mediathek links (containing a base64 ID) or MDR video pages.
# We attempt to resolve MDR pages to ARD base64 IDs at runtime.
MANUAL_EPISODE_URLS: List[str] = [
    # Provided by user: MDR pages
    "https://www.mdr.de/sandmann/video-536936.html",
    "https://www.mdr.de/sandmann/video-529286.html",
    "https://www.mdr.de/sandmann/video-529344.html",
    # Provided by user: ARD Mediathek direct link (already contains base64 ID)
    "https://www.ardmediathek.de/video/unser-sandmaennchen/peskowcik-liska-a-sroka-jablucina-oder-unser-sandmaennchen-sorbisch-oder-17-08-2025/rbb/Y3JpZDovL3JiYl9iNmY2MWU1ZC02NDdkLTQ2ZjQtYjYzNC0wY2JkOTM5NzYwOTdfcHVibGljYXRpb24",
]

# Rich metadata for specific MDR links provided by the user.
# Dates are given as "dd.mm.yyyy" and will be parsed to a UTC timestamp.
MANUAL_EPISODE_METADATA: Dict[str, Dict[str, str]] = {
    "https://www.mdr.de/sandmann/video-536936.html": {
        "title": "Pěskowčik: Kalli chce być myška",
        "description": (
            "Kalli njemóže sej zaso raz wusnyć! Tónraz stanie so z myšku, "
            "dokelž tak rady twarožk rymza."
        ),
        "date": "22.08.2021",
    },
    "https://www.mdr.de/sandmann/video-529286.html": {
        "title": "Pěskowčik: Pirat Kalli so hněwa",
        "description": (
            "Kalli chce pirat być, tola Mareike je jemu tutu ideju skazyła. "
            "Naraz stanie so wón samo z kapitanom wulkeje łódźe a hižo wubědźowanje startuje."
        ),
        "date": "25.07.2021",
    },
    "https://www.mdr.de/sandmann/video-529344.html": {
        "title": "Pěskowčik: Kalli a wobraz za Mareiku",
        "description": (
            "Kalli njemóže sej zaso raz wusnyć. Tónraz je módry krokodil namolował "
            "a chce so ze seršćowcom stać. Kalli chce mjenujcy swět pisaniši sčinić."
        ),
        "date": "11.07.2021",
    },
}

def _parse_de_date_to_ts(date_str: str) -> int:
    """Parse a date in format dd.mm.yyyy to a UTC timestamp (12:00).

    Returns 0 on failure.
    """
    try:
        # remove weekday prefixes like "So " if present and trailing punctuation
        cleaned = date_str.strip()
        cleaned = re.sub(r"^[A-Za-zÀ-ÿ]{2,3}\s+", "", cleaned)  # drop short weekday like So, Mo
        cleaned = cleaned.rstrip(".")
        dt = datetime.strptime(cleaned, "%d.%m.%Y").replace(tzinfo=timezone.utc)
        # set to noon to avoid timezone edge cases
        dt = dt.replace(hour=12, minute=0, second=0, microsecond=0)
        return int(dt.timestamp())
    except Exception:
        return 0


def fetch_ard_episode(base64_id: str) -> Optional[Dict[str, Any]]:
    """Fetch episode details directly from the ARD page‑gateway API.

    This function retrieves the metadata for a given episode ID and extracts
    a minimal set of fields (title, description, timestamp, url_video,
    url_website) that mirror the structure returned by MediathekView.  If
    anything goes wrong (network error, unexpected JSON), None is returned.

    Args:
        base64_id: The base64‑encoded publication ID found in ARD URLs.

    Returns:
        A dict with the same keys as MediathekView entries or None on error.
    """
    api_url = f"https://api.ardmediathek.de/page-gateway/pages/ard/item/{base64_id}"
    try:
        resp = requests.get(api_url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return None
    # Attempt to locate an episode widget
    widgets = data.get("widgets", [])
    for widget in widgets:
        # ensure this is the episode we want (EPISODE and has mediaCollection)
        if not widget.get("mediaCollection"):
            continue
        # Extract titles
        title = widget.get("longTitle") or widget.get("mediumTitle") or widget.get("title") or ""
        description = widget.get("longSynopsis") or widget.get("synopsis") or ""
        # Extract timestamp from broadcastedOn
        ts_str = widget.get("broadcastedOn")
        if ts_str:
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                timestamp = int(dt.timestamp())
            except Exception:
                timestamp = 0
        else:
            timestamp = 0
        # Extract video URL (pick first available mp4)
        url_video = None
        try:
            media_array = widget["mediaCollection"]["embedded"]["_mediaArray"]
            # _mediaArray is a list; take first element
            if media_array:
                stream_array = media_array[0]["_mediaStreamArray"]
                # choose the stream with 720p or fallback to the first
                chosen = None
                for s in stream_array:
                    # Some entries use numeric quality keys; others use string
                    if s.get("_height") == 720 or s.get("_quality") in ("avc720", 3):
                        chosen = s
                        break
                if chosen is None and stream_array:
                    chosen = stream_array[0]
                if chosen:
                    url_video = chosen.get("_stream")
        except Exception:
            url_video = None
        # Build url_website (the ARD public url)
        url_website = f"https://www.ardmediathek.de/video/{base64_id}"
        return {
            "channel": widget.get("publisher", {}).get("name", "RBB"),
            "topic": "Unser Sandmännchen",
            "title": title,
            "description": description,
            "timestamp": timestamp,
            "duration": widget.get("duration"),
            "size": None,
            "url_website": url_website,
            "url_video": url_video,
        }
    return None


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

    channel_title = "Pěskowčik – Stream Now!"
    channel_link = "https://www.sandmann.de"
    channel_description = "RSS‑Feed mit sorbischsprachigen Folgen aus der MediathekViewWeb‑API"

    items_xml = []
    for entry in results:
        # Ensure we always pass strings to escape(); handle None values robustly.
        title = escape((entry.get("title") or ""))
        description = escape((entry.get("description") or ""))
        pub_date = convert_timestamp(int(entry.get("timestamp", 0) or 0))
        link = escape((entry.get("url_website") or ""))
        enclosure_url = escape((entry.get("url_video") or ""))
        # Build enclosure only if a video URL is present; infer MIME type.
        if enclosure_url:
            lower = enclosure_url.lower()
            if lower.endswith('.m3u8'):
                enclosure_type = 'application/x-mpegURL'
            else:
                enclosure_type = 'video/mp4'
            enclosure_xml = f"<enclosure url=\"{enclosure_url}\" length=\"0\" type=\"{enclosure_type}\" />"
        else:
            enclosure_xml = ""
        item_xml = f"""
        <item>
            <title>{title}</title>
            <description>{description}</description>
            <link>{link}</link>
            <guid>{link}</guid>
            <pubDate>{pub_date}</pubDate>
            {enclosure_xml}
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
    st.title("Pěskowčik – Stream Now!")
    # Lightweight UI tweaks for clearer episode cards (theme-agnostic)
    st.markdown(
        """
        <style>
        /* Episode card: make titles prominent and descriptions subtler */
        .episode-title { font-size: 1.15rem; font-weight: 700; margin: 0 0 .35rem 0; }
        .episode-meta { font-size: .9rem; opacity: .8; margin: -.2rem 0 .35rem 0; }
        /* Always show exactly two lines height for description, theme-agnostic */
        .desc-wrap { position: relative; }
        .episode-desc { font-size: .95rem; line-height: 1.45; opacity: .9; margin: 0 0 .15rem 0;
            display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden;
            min-height: calc(1.45em * 2);
        }
        .desc-toggle { display: none; }
        .desc-toggle:checked ~ .episode-desc { display: block; overflow: visible; -webkit-line-clamp: initial; -webkit-box-orient: initial; }
        .readmore { display: inline-block; font-size: .85rem; opacity: .8; text-decoration: underline; cursor: pointer; background: none; border: 0; padding: 0; float: right; }
        .readmore:hover { opacity: 1; }
        .readmore.less { display: none; }
        .desc-toggle:checked ~ .readmore.more { display: none; }
        .desc-toggle:checked ~ .readmore.less { display: inline-block; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.info("Diese App befindet sich noch im Aufbau und in der Entwicklung")
    st.markdown(
        """
        Um sich nicht mit den Mediatheken oder Google herumärgern zu müssen und um die wenigen aktuell verfügbaren sorbischen Folgen schnell griffbereit zu haben, habe ich diese App entwickelt.
        
        Bei der Entwicklung musste ich leider feststellen:
        """
    )

    with st.expander("Mehr lesen"):
        st.markdown(
            """
            1. Nicht immer bekommt eine sorbische Episode auch einen sorbischsprachigen Titel und Beschreibung – manchmal ist alles nur auf Deutsch.
            2. Es kam schon vor, dass im Titel **Plumps** steht, dich aber in der Episode **Fuchs und Elster** begrüßen.
            Das liegt nicht an der API oder dieser App, sondern direkt an der ARD-Mediathek!
            """
        )

    st.markdown(
        """
        Diese App nutzt die offene MediathekViewWeb‑API, um sorbischsprachige Sandmännchen‑Folgen zu finden und anzuzeigen. 
        https://github.com/max2058/stream.peskowcik
        """
    )
    
    # API Query and Fetch
    with st.spinner("Lade Daten von der Mediathek…"):
        # veröffentlichte sorbische Episoden ohne "sorbisch" im Titel
        # nicht untergehen. 
        query_json = build_query(topic="Unser Sandmännchen", title_filter=None, size=200, offset=0)
        results = fetch_results(query_json)

    if not results:
        st.warning("Es wurden keine passenden Einträge gefunden.")
        return

    sorbian_entries: List[Dict[str, Any]] = []
    sorbian_map: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=180)  # limit to last 180 Tage
    max_checks = 200  # maximum number of entries to examine per page
    max_results = 30  # maximum number of sorbian episodes to collect
    offset = 0
    while len(sorbian_map) < max_results:
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
                # Skip german duplicate of Gestörte Angelfreuden if it lacks a base64 ID
                title_lower = (entry.get("title") or "").strip().lower()
                if "gestörte angelfreuden" in title_lower or "gestoerte angelfreuden" in title_lower:
                    # Only include the sorbisch version; require (sorbisch) in the title
                    if "(sorbisch)" not in title_lower:
                        checked += 1
                        continue
                # Normalize key components
                title_norm = title_lower
                desc_norm = (entry.get("description") or "").strip().lower()
                ts_int = int(entry.get("timestamp", 0))
                date_str = datetime.fromtimestamp(ts_int, tz=timezone.utc).strftime("%Y-%m-%d") if ts_int else ""
                key = (title_norm, desc_norm, date_str)
                existing = sorbian_map.get(key)
                # Update the map if this entry has a higher sorbian score
                if existing is None or sorbian_score(entry) > sorbian_score(existing):
                    sorbian_map[key] = entry
                    if len(sorbian_map) >= max_results:
                        break
            checked += 1
        # Determine whether to fetch another page
        if cutoff_reached or len(sorbian_map) >= max_results:
            break
        # If we examined fewer entries than max_checks it means the page
        # contained fewer than max_checks items, so there is no need to
        # request further pages.
        if checked < max_checks:
            break
        offset += 200

    # Optionally include manually specified episodes that might not yet
    # appear in the MediathekView database.  These are considered in
    # the same deduplication logic as the API entries.
    def _add_entry_to_map(ep: Optional[Dict[str, Any]]):
        if not ep:
            return
        title_norm = (ep.get("title") or "").strip().lower()
        desc_norm = (ep.get("description") or "").strip().lower()
        ts_int = int(ep.get("timestamp", 0))
        date_str = datetime.fromtimestamp(ts_int, tz=timezone.utc).strftime("%Y-%m-%d") if ts_int else ""
        key = (title_norm, desc_norm, date_str)
        existing = sorbian_map.get(key)
        if existing is None or sorbian_score(ep) > sorbian_score(existing):
            sorbian_map[key] = ep

    # From known base64 IDs
    for base64_id in MANUAL_EPISODES:
        try:
            ep = fetch_ard_episode(base64_id)
        except Exception:
            ep = None
        _add_entry_to_map(ep)

    # From external URLs (ARD or MDR); try to resolve to ARD base64 first
    for src in MANUAL_EPISODE_URLS:
        base64_id = None
        if src.startswith("http"):
            base64_id = resolve_base64_from_url(src)

        meta = MANUAL_EPISODE_METADATA.get(src)
        fetched = None
        if base64_id:
            try:
                fetched = fetch_ard_episode(base64_id)
            except Exception:
                fetched = None
            # If ARD didn't provide a playable video, try MDR extraction
            if fetched and not fetched.get("url_video"):
                try:
                    mdr_ep = fetch_mdr_episode(src)
                except Exception:
                    mdr_ep = None
                if mdr_ep and mdr_ep.get("url_video"):
                    fetched = {**fetched, "url_video": mdr_ep.get("url_video")}
            if meta:
                # Merge fetched video (if any) with manual title/desc/date and keep MDR link
                _add_entry_to_map(
                    {
                        "channel": (fetched or {}).get("channel", "MDR"),
                        "topic": "Unser Sandmännchen",
                        "title": meta.get("title", (fetched or {}).get("title")),
                        "description": meta.get("description", (fetched or {}).get("description")),
                        "timestamp": _parse_de_date_to_ts(meta.get("date", "")) or (fetched or {}).get("timestamp", 0),
                        "duration": (fetched or {}).get("duration"),
                        "size": None,
                        "url_website": src,
                        "url_video": (fetched or {}).get("url_video"),
                    }
                )
            else:
                _add_entry_to_map(fetched)
        else:
            # Fallback: build a manual entry using provided metadata if available
            if meta:
                # Try MDR extraction first to get a playable stream
                try:
                    mdr_ep = fetch_mdr_episode(src)
                except Exception:
                    mdr_ep = None
                if mdr_ep and mdr_ep.get("url_video"):
                    _add_entry_to_map(
                        {
                            "channel": "MDR",
                            "topic": "Unser Sandmännchen",
                            "title": meta.get("title", mdr_ep.get("title", "Pěskowčik (MDR)")),
                            "description": meta.get("description", mdr_ep.get("description", "")),
                            "timestamp": _parse_de_date_to_ts(meta.get("date", "")) or mdr_ep.get("timestamp", 0),
                            "duration": None,
                            "size": None,
                            "url_website": src,
                            "url_video": mdr_ep.get("url_video"),
                        }
                    )
                else:
                    _add_entry_to_map(
                        {
                            "channel": "MDR",
                            "topic": "Unser Sandmännchen",
                            "title": meta.get("title", "Pěskowčik (MDR)"),
                            "description": meta.get("description", "Manuell hinzugefügt – externen Link öffnen."),
                            "timestamp": _parse_de_date_to_ts(meta.get("date", "")),
                            "duration": None,
                            "size": None,
                            "url_website": src,
                            "url_video": None,
                        }
                    )
            else:
                # Minimal fallback
                m = re.search(r"video-(\d+)", src)
                short = m.group(1) if m else src.rsplit("/", 1)[-1]
                _add_entry_to_map(
                    {
                        "channel": "MDR",
                        "topic": "Unser Sandmännchen",
                        "title": f"Pěskowčik (MDR) – {short}",
                        "description": "Manuell hinzugefügt – externen Link öffnen.",
                        "timestamp": 0,
                        "duration": None,
                        "size": None,
                        "url_website": src,
                        "url_video": None,
                    }
                )

    # Convert the sorbian_map to a list for further processing
    sorbian_entries = list(sorbian_map.values())
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
            # Einheitliches Vorschaubild für alle Videos
            # "Vorschau": DEFAULT_THUMBNAIL,
            "Video": entry.get("url_video"),
            "Website": entry.get("url_website"),
        }
        table_rows.append(row)

    st.subheader("Aktuelle Folgen jetzt Streamen")

    # No JS toggles or theme-specific button styles to keep things snappy and consistent
    cols = st.columns(3)
    for idx, row in enumerate(table_rows):
        col = cols[idx % 3]
        with col:
            # Prominent title + smaller meta date line
            st.markdown(
                f"<div class='episode-title'>{html_escape(str(row['Titel'] or ''))}</div>"
                f"<div class='episode-meta'>{html_escape(str(row['Datum'] or ''))}</div>",
                unsafe_allow_html=True,
            )

            # Vorschau: immer 2 Zeilen (CSS line‑clamp) mit rein CSS-basiertem Toggle ohne Rerun/JS
            desc = html_escape(str(row.get("Beschreibung") or ""))
            toggle_id = f"toggle-{idx}"
            html_block = (
                f"<div class='desc-wrap'>"
                f"<input type='checkbox' id='{toggle_id}' class='desc-toggle'>"
                f"<div class='episode-desc'>{desc}</div>"
                f"<label for='{toggle_id}' class='readmore more'>Mehr lesen</label>"
                f"<label for='{toggle_id}' class='readmore less'>Weniger</label>"
                f"</div>"
            )
            st.markdown(html_block, unsafe_allow_html=True)
            # Some entries may not have a direct video url (e.g. if geoblocked). Use the
            # website as fallback when url_video is missing.
            video_url = row["Video"]
            if video_url:
                vu = str(video_url)
                if vu.lower().endswith(".m3u8"):
                    # Use hls.js for cross‑browser HLS playback
                    player_id = f"vid-{idx}"
                    video_html = f'''
<div>
  <video id="{player_id}" controls preload="none" playsinline style="width: 100%; height: auto;" poster="{THUMBNAIL_DATA_URL}"></video>
  <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
  <script>
    (function() {{
      var url = {json.dumps(vu)};
      var video = document.getElementById({json.dumps(player_id)});
      if (video.canPlayType('application/vnd.apple.mpegURL')) {{
        video.src = url;
      }} else if (window.Hls && window.Hls.isSupported()) {{
        var hls = new Hls();
        hls.loadSource(url);
        hls.attachMedia(video);
      }} else {{
        // Fallback link if HLS unsupported
        var a = document.createElement('a');
        a.href = url;
        a.innerText = 'Video öffnen';
        a.target = '_blank';
        video.parentNode.appendChild(a);
      }}
    }})();
  </script>
</div>
'''
                    components.html(video_html, height=320)
                else:
                    video_html = f'''
<video controls preload="none" playsinline style="width: 100%; height: auto;" poster="{THUMBNAIL_DATA_URL}">
  <source src="{vu}" type="video/mp4">
  Dein Browser unterstützt das Video-Tag nicht.
</video>
'''
                    components.html(video_html, height=300)
            else:
                # Fallback: verlinktes Vorschaubild zur Website
                website = row["Website"]
                preview_link_html = f'<a href="{website}" target="_blank"><img src="{THUMBNAIL_DATA_URL}" style="width:100%; height:auto; border:0;"/></a>'
                components.html(preview_link_html, height=300)

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
            "Vorschau": st.column_config.ImageColumn("Vorschau", width="small"),
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
