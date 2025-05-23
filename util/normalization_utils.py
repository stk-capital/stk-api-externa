def normalize_name(name: str) -> str:
    """Return Title Case name preserving short all-caps siglas (<=4 chars)."""
    if not name:
        return ""
    cleaned = " ".join(name.strip().split())
    parts = []
    for p in cleaned.split():
        if len(p) <= 4 and p.isupper():
            parts.append(p)
        else:
            parts.append(p.title())
    return " ".join(parts)


def normalize_ticker(ticker) -> str:
    """Return upper-case ticker; fallback to 'PRIVATE' when empty/None."""
    if not ticker:
        return "PRIVATE"
    return str(ticker).strip().upper() 