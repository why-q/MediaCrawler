import argparse
import asyncio
import json
import sys
from pathlib import Path

import aiofiles

# Usage:
# python parse_json.py --json_paths json_path1 json_path2 ... --output_path [output_path]
# or
# python parse_json.py --json_dir [json_dir] --output_path [output_path]


async def parse_cmd():
    parser = argparse.ArgumentParser(description="Json Parser")
    parser.add_argument(
        "--json_paths", type=str, help="json file path list", nargs="+", default=[]
    )
    parser.add_argument(
        "--output_path",
        type=str,
        help="output file path",
        default="./data/output/output.txt",
    )
    parser.add_argument("--json_dir", type=str, help="json file directory", default="")
    parser.add_argument(
        "--platform", type=str, help="which platform to download", default="xhs"
    )
    parser.add_argument(
        "--trans_dir",
        type=str,
        help="where to store json files transformed from jsonl",
        default="./data/trans",
    )

    args = parser.parse_args()
    return args


async def get_links_xhs(json_path: str) -> list:
    links = []

    with open(json_path, "r", encoding="utf-8") as file:
        data = json.load(file)
        for item in data:
            image_str = item.get("image_list", "")
            image_list = image_str.split(",")
            for image in image_list:
                links.append(image)

    return links


async def get_links_weibo(json_path: str) -> list:
    links = []

    with open(json_path, "r", encoding="utf-8") as file:
        data = json.load(file)
        for item in data:
            if int(item.get("pic_num", 0)) == 0:
                continue

            pic_urls = item.get("pic_urls", None)
            assert pic_urls is not None

            for pic_url in pic_urls:
                links.append(pic_url)

    return links


async def jsonl_to_json(jsonl_file_path, json_file_path):
    async def process_line(line):
        return json.loads(line)

    datas = []
    async with aiofiles.open(jsonl_file_path, "r", encoding="utf-8") as jsonl_file:
        async for line in jsonl_file:
            datas.append(await process_line(line))

    async with aiofiles.open(json_file_path, "w", encoding="utf-8") as json_file:
        await json_file.write(json.dumps(datas, indent=4))

    print(f"Transformed [{jsonl_file_path}] to [{json_file_path}]")


async def main():
    args = await parse_cmd()

    json_paths = args.json_paths
    json_dir = args.json_dir
    output_path = args.output_path
    platform = args.platform

    if platform == "xhs":
        get_links = get_links_xhs
    elif platform == "weibo":
        get_links = get_links_weibo
        trans_dir = Path(args.trans_dir)
        trans_dir.mkdir(parents=True, exist_ok=True)

    links = []
    if json_dir != "":
        json_paths = [
            path
            for path in Path(json_dir).glob(
                "*.json" if platform != "weibo" else "*.jsonl"
            )
            if path.is_file()
        ]

    for json_path in json_paths:
        if not Path(json_path).exists():
            print(f"{str(json_path)} does not exist")
            continue

        if json_path.suffix == ".jsonl":
            await jsonl_to_json(json_path, trans_dir / f"{json_path.stem}.json")
            json_path = trans_dir / f"{json_path.stem}.json"

        links += await get_links(json_path)

    unique_links = list(set(links))
    print(f"Total unique links: {len(unique_links)}")

    if not Path(output_path).parent.exists():
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as file:
        for link in unique_links:
            file.write(link + "\n")
    print(f"Output file saved to {output_path}")


if __name__ == "__main__":
    try:
        asyncio.get_event_loop().run_until_complete(main())
    except KeyboardInterrupt:
        sys.exit()
