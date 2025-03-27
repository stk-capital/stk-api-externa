


def format_followers(count: int) -> str:
    """Format follower count for display"""
    if count >= 1_000_000:
        return f"{count/1_000_000:.1f}M"
    elif count >= 1000:
        return f"{count/1000:.1f}K"
    return str(count)
