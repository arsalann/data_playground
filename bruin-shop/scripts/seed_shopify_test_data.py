#!/usr/bin/env python3
"""Seed a Shopify development store with realistic synthetic ecommerce data.

This script is intentionally not a Bruin asset. It writes to Shopify so the
`*_test` ingestr assets can prove real API extraction into BigQuery.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any


API_VERSION = "2026-04"

PRODUCT_TEMPLATES = [
    ("Trail Bottle", "Hydration", "Northstar Supply", "outdoor,bestseller", Decimal("34.00")),
    ("Everyday Tote", "Bags", "Northstar Supply", "commute", Decimal("64.00")),
    ("Merino Tee", "Apparel", "Blue Finch", "apparel", Decimal("48.00")),
    ("Rain Shell", "Apparel", "Blue Finch", "outerwear", Decimal("128.00")),
    ("Packing Cubes", "Travel", "Waypoint Goods", "travel", Decimal("39.00")),
    ("Desk Lamp", "Home", "Studio Vale", "home", Decimal("88.00")),
    ("Ceramic Mug", "Home", "Studio Vale", "home", Decimal("22.00")),
    ("Travel Charger", "Electronics", "Waypoint Goods", "electronics", Decimal("52.00")),
    ("Canvas Cap", "Accessories", "Blue Finch", "accessories", Decimal("28.00")),
    ("Field Notebook", "Stationery", "Studio Vale", "stationery", Decimal("16.00")),
]

FIRST_NAMES = [
    "Avery",
    "Maya",
    "Noah",
    "Sofia",
    "Ethan",
    "Lina",
    "Marcus",
    "Priya",
    "Iris",
    "Julian",
    "Nora",
    "Caleb",
    "Tara",
    "Owen",
    "Leah",
]

LAST_NAMES = [
    "Rivera",
    "Chen",
    "Patel",
    "Morgan",
    "Kim",
    "Singh",
    "Garcia",
    "Nguyen",
    "Brown",
    "Khan",
]

STATES = [
    ("San Francisco", "California", "CA", "94107"),
    ("Austin", "Texas", "TX", "78701"),
    ("Chicago", "Illinois", "IL", "60607"),
    ("Seattle", "Washington", "WA", "98101"),
    ("Denver", "Colorado", "CO", "80202"),
]


@dataclass(frozen=True)
class ShopifyConfig:
    shop: str
    client_id: str
    client_secret: str


def env_config() -> ShopifyConfig:
    missing = [
        name
        for name in [
            "BRUIN_SHOPIFY_STORE_URL",
            "BRUIN_SHOPIFY_CLIENT_ID",
            "BRUIN_SHOPIFY_CLIENT_SECRET",
        ]
        if not os.environ.get(name)
    ]
    if missing:
        raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")

    shop = os.environ["BRUIN_SHOPIFY_STORE_URL"].replace("https://", "").rstrip("/")
    return ShopifyConfig(
        shop=shop,
        client_id=os.environ["BRUIN_SHOPIFY_CLIENT_ID"],
        client_secret=os.environ["BRUIN_SHOPIFY_CLIENT_SECRET"],
    )


def request_json(
    url: str,
    method: str,
    headers: dict[str, str],
    body: dict[str, Any] | None = None,
    retries: int = 4,
) -> dict[str, Any]:
    data = json.dumps(body).encode() if body is not None else None
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                raw = resp.read().decode()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode()
            if exc.code == 429 and attempt < retries:
                retry_after = exc.headers.get("Retry-After")
                delay = int(retry_after) if retry_after and retry_after.isdigit() else min(65, 10 * (attempt + 1))
                print(f"Rate limited by Shopify; sleeping {delay}s before retry {attempt + 1}/{retries}.")
                time.sleep(delay)
                continue
            raise RuntimeError(f"{method} {url} failed with {exc.code}: {detail}") from exc
    raise RuntimeError(f"{method} {url} failed after retries")


def post_form(url: str, data: dict[str, str]) -> dict[str, Any]:
    encoded = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(
        url,
        data=encoded,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()
        raise RuntimeError(f"POST {url} failed with {exc.code}: {detail}") from exc


def get_access_token(config: ShopifyConfig) -> str:
    payload = post_form(
        f"https://{config.shop}/admin/oauth/access_token",
        {
            "grant_type": "client_credentials",
            "client_id": config.client_id,
            "client_secret": config.client_secret,
        },
    )
    scopes = set(payload.get("scope", "").split(","))
    required = {"write_products", "write_customers", "write_orders"}
    missing = sorted(required - scopes)
    if missing:
        raise SystemExit(f"Active Shopify token is missing scopes: {', '.join(missing)}")
    print("Active Shopify scopes include required write access.")
    return payload["access_token"]


def admin_url(config: ShopifyConfig, path: str) -> str:
    return f"https://{config.shop}/admin/api/{API_VERSION}/{path.lstrip('/')}"


def auth_headers(token: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
    }


def get_json(config: ShopifyConfig, token: str, path: str) -> dict[str, Any]:
    return request_json(admin_url(config, path), "GET", auth_headers(token), None)


def load_seeded_products(config: ShopifyConfig, token: str, suffix: str) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []
    page = get_json(config, token, "products.json?limit=250")
    for product in page.get("products", []):
        tags = product.get("tags", "")
        if "bruin-test" not in tags or suffix not in tags:
            continue
        variants = product.get("variants", [])
        if not variants:
            continue
        variant = variants[0]
        products.append(
            {
                "id": product["id"],
                "title": product["title"],
                "variant_id": variant["id"],
                "price": Decimal(str(variant["price"])),
                "sku": variant.get("sku"),
            }
        )
    return products


def load_seeded_customers(config: ShopifyConfig, token: str, suffix: str) -> list[dict[str, Any]]:
    customers: list[dict[str, Any]] = []
    page = get_json(config, token, "customers.json?limit=250")
    for customer in page.get("customers", []):
        tags = customer.get("tags", "")
        if "bruin-test" not in tags or suffix not in tags:
            continue
        customers.append(
            {
                "id": customer["id"],
                "email": customer["email"],
                "first_name": customer.get("first_name"),
                "last_name": customer.get("last_name"),
            }
        )
    return customers


def create_products(config: ShopifyConfig, token: str, count: int, suffix: str, rng: random.Random) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []
    for index in range(count):
        title, product_type, vendor, tags, price = PRODUCT_TEMPLATES[index % len(PRODUCT_TEMPLATES)]
        inventory = rng.randint(25, 120)
        sku = f"BRUIN-{suffix}-{index + 1:03d}"
        payload = {
            "product": {
                "title": f"{title} {suffix}",
                "vendor": vendor,
                "product_type": product_type,
                "status": "active",
                "tags": f"{tags},bruin-test,synthetic,{suffix}",
                "variants": [
                    {
                        "option1": "Default Title",
                        "price": str(price),
                        "sku": sku,
                        "inventory_management": "shopify",
                        "inventory_quantity": inventory,
                        "taxable": True,
                    }
                ],
            }
        }
        result = request_json(admin_url(config, "products.json"), "POST", auth_headers(token), payload)
        product = result["product"]
        variant = product["variants"][0]
        products.append(
            {
                "id": product["id"],
                "title": product["title"],
                "variant_id": variant["id"],
                "price": Decimal(str(variant["price"])),
                "sku": variant["sku"],
            }
        )
    return products


def create_customers(config: ShopifyConfig, token: str, count: int, suffix: str, rng: random.Random) -> list[dict[str, Any]]:
    customers: list[dict[str, Any]] = []
    for index in range(count):
        first_name = FIRST_NAMES[index % len(FIRST_NAMES)]
        last_name = LAST_NAMES[(index * 3) % len(LAST_NAMES)]
        city, province, province_code, zip_code = STATES[index % len(STATES)]
        email = f"bruin-test-{index + 1:03d}-{suffix}@example.com"
        tags = "bruin-test,synthetic"
        if index % 5 == 0:
            tags += ",vip,repeat"
        payload = {
            "customer": {
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "verified_email": True,
                "tags": f"{tags},{suffix}",
                "send_email_welcome": False,
                "addresses": [
                    {
                        "first_name": first_name,
                        "last_name": last_name,
                        "address1": f"{100 + rng.randint(1, 899)} Market Street",
                        "city": city,
                        "province": province,
                        "province_code": province_code,
                        "country": "United States",
                        "country_code": "US",
                        "zip": zip_code,
                    }
                ],
            }
        }
        result = request_json(admin_url(config, "customers.json"), "POST", auth_headers(token), payload)
        customer = result["customer"]
        customers.append(
            {
                "id": customer["id"],
                "email": customer["email"],
                "first_name": customer["first_name"],
                "last_name": customer["last_name"],
            }
        )
    return customers


def create_orders(
    config: ShopifyConfig,
    token: str,
    count: int,
    suffix: str,
    products: list[dict[str, Any]],
    customers: list[dict[str, Any]],
    rng: random.Random,
    order_delay_seconds: float,
    order_dates: list[date] | None = None,
) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    for index in range(count):
        customer = customers[index % len(customers)]
        line_count = 1 + (1 if index % 4 == 0 else 0)
        sampled_products = rng.sample(products, k=min(line_count, len(products)))
        line_items = [
            {
                "variant_id": product["variant_id"],
                "quantity": 1 + (1 if rng.random() < 0.2 else 0),
            }
            for product in sampled_products
        ]
        financial_status = "paid" if index % 11 else "pending"
        discount_codes = []
        if index % 7 == 0:
            discount_codes.append({"code": "BRUIN10", "amount": "5.00", "type": "fixed_amount"})
        processed_at = None
        if order_dates:
            order_date = order_dates[index % len(order_dates)]
            hour = 9 + (index % 10)
            minute = (index * 7) % 60
            processed_at = datetime(order_date.year, order_date.month, order_date.day, hour, minute, tzinfo=UTC)
        payload = {
            "order": {
                "email": customer["email"],
                "customer": {"id": customer["id"]},
                "financial_status": financial_status,
                "fulfillment_status": "fulfilled" if index % 5 else "partial",
                "send_receipt": False,
                "send_fulfillment_receipt": False,
                "currency": "USD",
                "tags": f"bruin-test,synthetic,{suffix}",
                "line_items": line_items,
                "discount_codes": discount_codes,
                "note": "Synthetic order created for Bruin ingestr validation.",
            }
        }
        if processed_at:
            payload["order"]["processed_at"] = processed_at.isoformat().replace("+00:00", "Z")
        result = request_json(admin_url(config, "orders.json"), "POST", auth_headers(token), payload)
        order = result["order"]
        orders.append(
            {
                "id": order["id"],
                "order_number": order.get("order_number"),
                "email": order.get("email"),
                "total_price": order.get("total_price"),
                "financial_status": order.get("financial_status"),
            }
        )
        if order_delay_seconds and index < count - 1:
            time.sleep(order_delay_seconds)
    return orders


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--products", type=int, default=10)
    parser.add_argument("--customers", type=int, default=30)
    parser.add_argument("--orders", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--suffix", default=datetime.now(UTC).strftime("%Y%m%d-%H%M%S"))
    parser.add_argument("--reuse-suffix", help="Reuse existing bruin-test products/customers with this suffix.")
    parser.add_argument("--data-start-date", help="Business start date for generated Shopify orders, YYYY-MM-DD.")
    parser.add_argument("--data-end-date", help="Business end date for generated Shopify orders, YYYY-MM-DD.")
    parser.add_argument(
        "--order-delay-seconds",
        type=float,
        default=65,
        help="Delay between order creates; trial stores may only allow about one order per minute.",
    )
    args = parser.parse_args()

    if args.products < 1 or args.customers < 1:
        raise SystemExit("--products and --customers must be at least 1")
    if args.orders < 0:
        raise SystemExit("--orders cannot be negative")

    summary = seed_shopify_data(
        products=args.products,
        customers=args.customers,
        orders=args.orders,
        seed=args.seed,
        suffix=args.suffix,
        reuse_suffix=args.reuse_suffix,
        order_delay_seconds=args.order_delay_seconds,
        data_start_date=date.fromisoformat(args.data_start_date) if args.data_start_date else None,
        data_end_date=date.fromisoformat(args.data_end_date) if args.data_end_date else None,
    )
    print(json.dumps(summary, indent=2, default=str))
    return 0


def seed_shopify_data(
    products: int = 10,
    customers: int = 30,
    orders: int = 5,
    seed: int = 42,
    suffix: str | None = None,
    reuse_suffix: str | None = None,
    order_delay_seconds: float = 65,
    data_start_date: date | None = None,
    data_end_date: date | None = None,
) -> dict[str, Any]:
    if products < 1 or customers < 1:
        raise ValueError("products and customers must be at least 1")
    if orders < 0:
        raise ValueError("orders cannot be negative")

    config = env_config()
    token = get_access_token(config)
    rng = random.Random(seed)

    run_suffix = reuse_suffix or suffix or datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    print(f"Seeding Shopify store {config.shop} with suffix {run_suffix}")
    if reuse_suffix:
        created_products = load_seeded_products(config, token, run_suffix)
        created_customers = load_seeded_customers(config, token, run_suffix)
        if not created_products or not created_customers:
            raise ValueError(f"No reusable products/customers found for suffix {run_suffix}")
        print(f"Reusing {len(created_products)} products and {len(created_customers)} customers.")
    else:
        created_products = create_products(config, token, products, run_suffix, rng)
        print(f"Created {len(created_products)} products.")
        created_customers = create_customers(config, token, customers, run_suffix, rng)
        print(f"Created {len(created_customers)} customers.")
    order_dates = None
    if data_start_date and data_end_date:
        day_count = (data_end_date - data_start_date).days + 1
        if day_count < 1:
            raise ValueError("data_end_date must be on or after data_start_date")
        order_dates = [data_start_date + timedelta(days=offset) for offset in range(day_count)]

    created_orders = create_orders(
        config,
        token,
        orders,
        run_suffix,
        created_products,
        created_customers,
        rng,
        order_delay_seconds,
        order_dates,
    )
    print(f"Created {len(created_orders)} orders.")

    return {
        "shop": config.shop,
        "suffix": run_suffix,
        "data_start_date": data_start_date.isoformat() if data_start_date else None,
        "data_end_date": data_end_date.isoformat() if data_end_date else None,
        "products": created_products,
        "customers": created_customers,
        "orders": created_orders,
    }


if __name__ == "__main__":
    sys.exit(main())
