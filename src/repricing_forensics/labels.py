from __future__ import annotations

from typing import Optional


ADDRESS_PROJECT_LABELS = {
    "0xdac17f958d2ee523a2206206994597c13d831ec7": "Tether USDT",
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "Circle USDC",
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH",
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "Uniswap V2 Router",
    "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": "SushiSwap Router",
    "0x00005ea00ac477b1030ce78506496e8c2de24bf5": "Uniswap Universal Router",
    "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789": "ERC-4337 EntryPoint",
    "0x000000000004444c5dc75cb358380d2e3de08a90": "1inch Aggregation Router",
    "0x0000000000000068f116a894984e2db1123eb395": "Uniswap Permit2",
    "0x6b175474e89094c44da98b954eedeac495271d0f": "Maker DAI",
    "0x514910771af9ca656af840dff83e8264ecf986ca": "Chainlink LINK",
    "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce": "SHIB",
    "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2": "Maker MKR",
    "0x881d40237659c251811cec9c364ef91dc08d300c": "Metamask Swap Router",
    "0xe592427a0aece92de3edee1f18e0157c05861564": "Uniswap V3 Router",
    "0x111111125421ca6dc452d289314280a0f8842a65": "1inch Aggregation Router",
    "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb": "0x Settler / Aggregation",
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": "Uniswap V3 Router 2",
    "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae": "LI.FI / Socket Bridge",
    "0x4337084d9e255ff0702461cf8895ce9e3b5ff108": "ERC-4337 EntryPoint",
}


def normalize_address(address: Optional[str]) -> Optional[str]:
    if address is None:
        return None
    return address.lower()


def infer_project_label(
    address: Optional[str],
    compiled_name: Optional[str] = None,
    classification: Optional[str] = None,
    source_hint: Optional[str] = None,
) -> str:
    norm = normalize_address(address)
    if norm in ADDRESS_PROJECT_LABELS:
        return ADDRESS_PROJECT_LABELS[norm]

    name = " ".join(filter(None, [(compiled_name or "").lower(), (source_hint or "").lower()]))
    if "entrypoint" in name:
        return "ERC-4337 EntryPoint"
    if "uniswap" in name:
        return "Uniswap"
    if "sushi" in name:
        return "SushiSwap"
    if "permit2" in name:
        return "Uniswap Permit2"
    if "aggregationrouter" in name:
        return "1inch Aggregation Router"
    if "universalrouter" in name:
        return "Uniswap Universal Router"
    if "entrypoint" in name:
        return "ERC-4337 EntryPoint"
    if "safe" in name or "gnosis" in name:
        return "Safe"
    if "proxy" in name and classification:
        return f"Proxy ({classification})"
    if classification == "wallet_or_safe":
        return "Wallet / Safe"
    if classification == "proxy":
        return "Proxy"
    if classification == "upgradeable":
        return "Upgradeable Contract"
    return norm or "unknown"
