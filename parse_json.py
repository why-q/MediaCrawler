import argparse
import asyncio
import json
import sys
from pathlib import Path

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

    args = parser.parse_args()
    return args


async def main():
    args = await parse_cmd()

    json_paths = args.json_paths
    json_dir = args.json_dir
    output_path = args.output_path

    links = []
    if json_dir != "":
        json_paths = [
            str(path) for path in Path(json_dir).glob("*.json") if path.is_file()
        ]

    for json_path in json_paths:
        if not Path(json_path).exists():
            print(f"{json_path} does not exist")
            continue

        with open(json_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            for item in data:
                image_str = item.get("image_list", "")
                image_list = image_str.split(",")
                for image in image_list:
                    links.append(image)

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
