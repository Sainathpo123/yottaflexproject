"""Collapse several records that share a key inside one file."""

from __future__ import annotations

from ..contracts.models import Candidate, RejectDecision, freeze_strings
from ..contracts.reasons import CONFLICTING_VERSION, DUPLICATE_KEY


def _reject(candidate: Candidate, reason_code: str, detail: str) -> RejectDecision:
    return RejectDecision(
        source_line=candidate.source_line,
        raw=freeze_strings(candidate.raw),
        record_key=candidate.key,
        reason_code=reason_code,
        reason_detail=detail,
    )


def collapse(candidates: list[Candidate]) -> tuple[list[Candidate], list[RejectDecision]]:
    """Keep the latest valid version of each key.

    An exact extra copy of that version is a duplicate. An older copy is
    superseded and quarantined, not published. If two different payloads share
    the newest timestamp, none of the key is published.
    """
    groups: dict[str, list[Candidate]] = {}
    order: list[str] = []
    for candidate in candidates:
        if candidate.key not in groups:
            order.append(candidate.key)
            groups[candidate.key] = []
        groups[candidate.key].append(candidate)

    winners: list[Candidate] = []
    rejects: list[RejectDecision] = []
    for key in order:
        group = groups[key]
        if len(group) == 1:
            winners.append(group[0])
            continue
        newest = max(item.version_at for item in group)
        top = [item for item in group if item.version_at == newest]
        if len({item.record_hash for item in top}) > 1:
            for item in group:
                rejects.append(
                    _reject(
                        item,
                        CONFLICTING_VERSION,
                        "same_version_timestamp_different_payload_in_batch",
                    )
                )
            continue
        winner = min(top, key=lambda item: item.source_line)
        winners.append(winner)
        for item in group:
            if item is winner:
                continue
            if item.record_hash == winner.record_hash and item.version_at == winner.version_at:
                detail = "exact_duplicate_in_batch"
            else:
                detail = "superseded_by_later_record_in_batch"
            rejects.append(_reject(item, DUPLICATE_KEY, detail))
    winners.sort(key=lambda item: item.source_line)
    return winners, rejects
