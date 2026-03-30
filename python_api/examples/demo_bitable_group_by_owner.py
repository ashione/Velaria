import os

from velaria import BitableClient, group_rows_count_by_field


def main():
    app_id = os.getenv("FEISHU_BITABLE_APP_ID")
    app_secret = os.getenv("FEISHU_BITABLE_APP_SECRET")
    if not app_id or not app_secret:
        raise RuntimeError(
            "FEISHU_BITABLE_APP_ID and FEISHU_BITABLE_APP_SECRET are required for this demo"
        )

    bitable_url = os.getenv("FEISHU_BITABLE_BASE_URL")
    owner_field = os.getenv("FEISHU_BITABLE_OWNER_FIELD")
    if not bitable_url:
        raise RuntimeError("FEISHU_BITABLE_BASE_URL is required for this demo")
    if not owner_field:
        raise RuntimeError("FEISHU_BITABLE_OWNER_FIELD is required for this demo")

    client = BitableClient(app_id=app_id, app_secret=app_secret)
    rows = client.list_records_from_url(bitable_url)
    grouped = group_rows_count_by_field(rows, owner_field)

    for owner, count in sorted(grouped.items()):
        print(f"{owner}: {count}")


if __name__ == "__main__":
    main()
