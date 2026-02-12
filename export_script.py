import os
import re
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import pandas as pd
import praw

SUBREDDIT = "watchexchange"
MONTHS_BACK = 6

# Heuristic parsing helpers
PRICE_RE = re.compile(r"""
(?:
    (?:\$\s?)(?P<usd1>\d{2,6}(?:[.,]\d{2})?)      # $1234 or $1,234.56
  | (?P<usd2>\d{2,6}(?:[.,]\d{2})?)\s?(?:usd|USD) # 1234 USD
  | (?P<eur>\d{2,6}(?:[.,]\d{2})?)\s?(?:eur|EUR|€)
  | (?:€\s?)(?P<eur2>\d{2,6}(?:[.,]\d{2})?)
)
""", re.VERBOSE)

# Common patterns in Watchexchange titles:
# [WTS] [USA-CA] Brand Model ...
# [WTS] [CAN/CONUS] Brand Model ...
TITLE_TAG_RE = re.compile(r"\[(?P<tag>[^\]]+)\]")

LOCATION_RE = re.compile(r"""
\[(?:USA|US|CONUS|CAN|EU|UK|AUS|NZ|INTL)[-\s]?
(?P<loc>[A-Z]{2}|[A-Za-z]{2,20}(?:\s?[A-Za-z]{2,20})?)\]
""", re.VERBOSE)

SHIP_DEST_HINTS = [
    ("CONUS", re.compile(r"\bCONUS\b", re.I)),
    ("USA", re.compile(r"\bUSA\b|\bUS only\b", re.I)),
    ("CANADA", re.compile(r"\bCanada\b|\bCAN\b", re.I)),
    ("EU", re.compile(r"\bEU\b|\bEurope\b", re.I)),
    ("UK", re.compile(r"\bUK\b|\bUnited Kingdom\b", re.I)),
    ("WORLDWIDE", re.compile(r"\bworldwide\b|\bWW shipping\b|\binternational\b", re.I)),
]

LABEL_YES_PATTERNS = [
    re.compile(r"\bbuyer (?:provides|supplies|sends) (?:a )?label\b", re.I),
    re.compile(r"\bbuyer['’]s label\b", re.I),
]
LABEL_NO_PATTERNS = [
    re.compile(r"\bseller (?:provides|supplies) (?:a )?label\b", re.I),
    re.compile(r"\bshipping included\b", re.I),
    re.compile(r"\bI will ship\b", re.I),
]

def extract_price(text: str):
    if not text:
        return None
    m = PRICE_RE.search(text)
    if not m:
        return None
    # Determine currency and normalize
    if m.group("usd1") or m.group("usd2"):
        val = m.group("usd1") or m.group("usd2")
        currency = "USD"
    else:
        val = m.group("eur") or m.group("eur2")
        currency = "EUR"
    val = val.replace(",", "")
    return f"{currency} {val}"

def extract_location_from_title(title: str):
    # Try to find something like [USA-CA] or [USA CA] or [CA]
    # Many posts use bracket tags. We take the 2nd bracket group if it looks like location.
    tags = TITLE_TAG_RE.findall(title or "")
    # Example tags: WTS, USA-CA, CAN/CONUS
    for t in tags:
        tt = t.strip()
        if any(x in tt.upper() for x in ["USA", "US", "CAN", "EU", "UK", "AUS", "NZ"]):
            return tt
        # Sometimes just state code like [CA] after [WTS]
        if re.fullmatch(r"[A-Z]{2}", tt):
            return tt
    return None

def extract_ship_dests(text: str):
    if not text:
        return None
    found = []
    for label, rx in SHIP_DEST_HINTS:
        if rx.search(text):
            found.append(label)
    if not found:
        return None
    # De-dup preserve order
    out = []
    for x in found:
        if x not in out:
            out.append(x)
    return ", ".join(out)

def infer_buyer_label(text: str):
    if not text:
        return None
    for rx in LABEL_YES_PATTERNS:
        if rx.search(text):
            return "yes"
    for rx in LABEL_NO_PATTERNS:
        if rx.search(text):
            return "no"
    return None  # unknown

def extract_brand_model(title: str):
    # Heuristic: remove bracket tags, then split the remaining first chunk
    if not title:
        return (None, None)
    cleaned = re.sub(r"\[[^\]]+\]\s*", "", title).strip()
    # Many titles start with Brand then Model, but not always.
    parts = cleaned.split()
    if len(parts) < 2:
        return (cleaned or None, None)
    brand = parts[0]
    model = " ".join(parts[1:])[:200]
    return (brand, model)

def main():
    reddit = praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=os.environ.get("REDDIT_USER_AGENT", "watchexchange-parser/1.0"),
    )

    cutoff = datetime.now(timezone.utc) - relativedelta(months=MONTHS_BACK)

    rows = []
    # Pull more than you need, then filter by date.
    # Increase limit if your 6 months coverage is incomplete.
    for post in reddit.subreddit(SUBREDDIT).new(limit=5000):
        created = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
        if created < cutoff:
            break

        title = post.title or ""
        body = post.selftext or ""
        combined = f"{title}\n{body}"

        brand, model = extract_brand_model(title)
        price = extract_price(combined)
        loc = extract_location_from_title(title) or extract_location_from_title(body)
        ship_dests = extract_ship_dests(combined)
        buyer_label = infer_buyer_label(combined)

        rows.append({
            "Watch Brand": brand,
            "Watch Model": model,
            "Sale Price": price,
            "Buyers Shipping Label (yes or no)": buyer_label,
            "Location of Seller": loc,
            "Possible Shipping Destinations": ship_dests,
            "Username of Poster": f"u/{post.author.name}" if post.author else None,
            "Date Listed": created.astimezone(timezone.utc).strftime("%Y-%m-%d"),
        })

    df = pd.DataFrame(rows)

    # Your requested sort: Date Listed descending
    df["Date Listed"] = pd.to_datetime(df["Date Listed"], errors="coerce")
    df = df.sort_values("Date Listed", ascending=False)

    # Save CSV
    out = f"watchexchange_last_{MONTHS_BACK}_months.csv"
    df.to_csv(out, index=False)
    print(f"Wrote {len(df)} rows to {out}")

if __name__ == "__main__":
    main()
