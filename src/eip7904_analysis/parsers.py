from __future__ import annotations

import ast
import json
import re
from typing import Any


HEX_RE = r"0x[a-fA-F0-9]+"
SELECTOR_RE = re.compile(r"Some\(\[([0-9,\s]+)\]\)")


def parse_json_maybe(value: str | None) -> Any | None:
    if value is None:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def parse_operation_counts(value: str | None) -> dict[str, Any] | None:
    parsed = parse_json_maybe(value)
    return parsed if isinstance(parsed, dict) else None


def parse_rust_debug_divergence_location(value: str | None) -> dict[str, Any] | None:
    if not value:
        return None

    contract = re.search(r"contract:\s*(" + HEX_RE + ")", value)
    pc = re.search(r"pc:\s*(\d+)", value)
    call_depth = re.search(r"call_depth:\s*(\d+)", value)
    opcode = re.search(r"opcode:\s*(\d+)", value)
    opcode_name = re.search(r'opcode_name:\s*"([^"]+)"', value)
    selectors = SELECTOR_RE.findall(value)

    return {
        "contract": contract.group(1).lower() if contract else None,
        "pc": int(pc.group(1)) if pc else None,
        "call_depth": int(call_depth.group(1)) if call_depth else None,
        "opcode": int(opcode.group(1)) if opcode else None,
        "opcode_name": opcode_name.group(1) if opcode_name else None,
        "function_selectors": [
            "0x" + bytes(int(part.strip()) for part in match.split(",")).hex() for match in selectors
        ],
        "raw": value,
    }


def parse_rust_debug_oog_info(value: str | None) -> dict[str, Any] | None:
    if not value:
        return None

    contract = re.search(r"contract:\s*(" + HEX_RE + ")", value)
    pc = re.search(r"pc:\s*(\d+)", value)
    call_depth = re.search(r"call_depth:\s*(\d+)", value)
    opcode = re.search(r"opcode:\s*(\d+)", value)
    opcode_name = re.search(r'opcode_name:\s*"([^"]+)"', value)
    gas_remaining = re.search(r"gas_remaining:\s*(\d+)", value)
    pattern = re.search(r"pattern:\s*([A-Za-z]+)", value)

    return {
        "contract": contract.group(1).lower() if contract else None,
        "pc": int(pc.group(1)) if pc else None,
        "call_depth": int(call_depth.group(1)) if call_depth else None,
        "opcode": int(opcode.group(1)) if opcode else None,
        "opcode_name": opcode_name.group(1) if opcode_name else None,
        "gas_remaining": int(gas_remaining.group(1)) if gas_remaining else None,
        "pattern": pattern.group(1).lower() if pattern else None,
        "raw": value,
    }


def parse_call_frames(value: str | None) -> list[dict[str, Any]] | None:
    parsed = parse_json_maybe(value)
    return parsed if isinstance(parsed, list) else None


def try_literal_eval(value: str | None) -> Any | None:
    if value is None:
        return None
    try:
        return ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return None
