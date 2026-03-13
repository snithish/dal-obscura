from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from dal_obscura.policy.models import MaskRule


@dataclass(frozen=True)
class AccessDecision:
    allowed_columns: List[str]
    masks: Dict[str, MaskRule]
    row_filter: Optional[str]
    policy_version: int
