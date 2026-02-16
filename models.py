from dataclasses import dataclass

@dataclass
class RawPost:
    source_name: str
    source_category: str
    source_group: str
    platform: str
    post_url: str
    posted_at: str
    text: str
