#!/usr/bin/env python3
import argparse
import json
import os
import pathlib
import sys

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
COPY_TRADE_DIR = PROJECT_ROOT.parent / "Copy Trade"

if COPY_TRADE_DIR.exists():
    sys.path.insert(0, str(COPY_TRADE_DIR))

try:
    from gmgn.client import gmgn  # type: ignore
except Exception as exc:  # pragma: no cover - helper script
    print(f"[gmgn_fetch] unable to import gmgn client: {exc}", file=sys.stderr)
    sys.exit(2)


def _coerce_price(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = "".join(ch for ch in value if ch.isdigit() or ch in {".", "-", "e", "E"})
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _extract_price(payload):
    if isinstance(payload, dict):
        for key in ("price", "price_usd", "priceUsd", "usd_price", "priceInUsd"):
            if key in payload:
                price = _coerce_price(payload.get(key))
                if price is not None:
                    return price
        for value in payload.values():
            price = _extract_price(value)
            if price is not None:
                return price
    elif isinstance(payload, list):
        for value in payload:
            price = _extract_price(value)
            if price is not None:
                return price
    else:
        return _coerce_price(payload)
    return None


def _normalize_address(address: str) -> str:
    if not isinstance(address, str):
        return ""
    return address.strip().lower()


def _entry_matches(entry: dict, target: str) -> bool:
    if not isinstance(entry, dict):
        return False
    target = _normalize_address(target)
    if not target:
        return True
    candidates = []
    for key in ("address", "token_address", "base_address", "pool_address", "contract", "a", "ba", "pa"):
        value = entry.get(key)
        if isinstance(value, str):
            candidates.append(_normalize_address(value))
    base_info = entry.get("base_token_info") or entry.get("bti")
    if isinstance(base_info, dict):
        for key in ("address", "token_address", "a"):
            value = base_info.get(key)
            if isinstance(value, str):
                candidates.append(_normalize_address(value))
    return target in candidates or not candidates


def _inject_price(payload: dict, address: str, price: float) -> dict:
    if not payload or price is None:
        return payload

    def assign(entry: dict):
        if not isinstance(entry, dict):
            return
        entry["price_usd"] = price
        entry["p"] = price
        base_info = entry.get("base_token_info") or entry.get("bti")
        if isinstance(base_info, dict):
            base_info["price_usd"] = price
            base_info["p"] = price

    target = payload.get("data", payload)
    normalized = _normalize_address(address)
    if isinstance(target, dict):
        for key, value in target.items():
            if isinstance(value, dict) and (_normalize_address(key) == normalized or _entry_matches(value, normalized)):
                assign(value)
    elif isinstance(target, list):
        for entry in target:
            if _entry_matches(entry, normalized):
                assign(entry)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Lightweight GMGN fetch helper.")
    parser.add_argument("--chain", default="bsc")
    parser.add_argument("--address", required=True)
    args = parser.parse_args()

    client = gmgn(chain=args.chain)
    client.randomiseRequest()
    session = getattr(client, "sendRequest", None)
    headers = getattr(client, "headers", None)
    if session is None or headers is None:
        print("[gmgn_fetch] helper failed to initialize session", file=sys.stderr)
        return 3
    url = f"{client.BASE_URL}/api/v1/mutil_window_token_info"
    payload = {
        "chain": client.chain,
        "addresses": [args.address],
    }
    response = session.post(url, json=payload, headers=headers)
    if not (200 <= response.status_code < 300):
        print(
            f"[gmgn_fetch] request failed ({response.status_code}): "
            f"{response.text[:200]}",
            file=sys.stderr,
        )
        return 4
    data = response.json()
    price_value = None
    try:
        price_payload = client.getTokenUsdPrice(args.address)
        price_value = _extract_price(price_payload)
    except Exception as exc:  # pragma: no cover - helper script
        print(f"[gmgn_fetch] price lookup failed: {exc}", file=sys.stderr)

    if price_value is not None:
        data = _inject_price(data, args.address, price_value)
    json.dump(data, sys.stdout, ensure_ascii=False)
    return 0


if __name__ == "__main__":  # pragma: no cover - helper script
    raise SystemExit(main())
