import argparse
import asyncio
import sys
import time
from io import BytesIO
from pathlib import Path

import aiohttp
import aiohttp.client_exceptions
import pillow_heif
from aiohttp import ClientError
from PIL import Image

# Usage:
# python download_img.py --txt_paths txt_path1 txt_path2 ... --output_dir [output_dir]
# or
# python download_img.py --txt_dir [txt_dir] --output_dir [output_dir]


async def parse_cmd():
    parser = argparse.ArgumentParser(description="Json Parser")
    parser.add_argument(
        "--platform", type=str, help="which platform to download", default="xhs"
    )
    parser.add_argument(
        "--txt_paths", type=str, help="json file path list", nargs="+", default=[]
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        help="output directory",
        default="./data/img/",
    )
    parser.add_argument("--txt_dir", type=str, help="json file directory", default="")
    parser.add_argument(
        "--max_retries", type=int, help="max retries to download", default=3
    )
    parser.add_argument(
        "--max_concurrent", type=int, help="max concurrent downloads", default=10
    )

    args = parser.parse_args()
    return args


async def download_image_weibo(session, url, output_dir, max_retries=3):
    img_path = Path(output_dir) / f"{url.split('/')[-1]}.png"
    if img_path.exists():
        print(f"Skipping existing img: {url}")
        return

    for attempt in range(max_retries):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    image_data = await response.read()
                    try:
                        image = Image.open(BytesIO(image_data))
                        image = image.convert("RGBA")
                        image.save(img_path, "PNG")
                        print(f"Downloaded and converted: {url}")
                        return
                    except Image.UnidentifiedImageError:
                        print(f"Failed to convert to PNG: {url}")
                else:
                    print(f"Wrong response status of {url}: {response.status}")
                    return
        except (ClientError, asyncio.TimeoutError) as e:
            if attempt < max_retries - 1:
                print(
                    f"Error downloading {url}: {e}. Retrying... (Attempt {attempt + 1})"
                )
                await asyncio.sleep(5)
            else:
                print(f"Failed to download {url} after {max_retries} attempts: {e}")
                return


async def download_images_weibo(txt_file, output_dir, max_concurrent=10, max_retries=3):
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_download(url):
            async with semaphore:
                await download_image_weibo(session, url, output_dir, max_retries)

        with open(txt_file, "r") as file:
            image_urls = file.readlines()

        tasks = [
            asyncio.create_task(bounded_download(url.strip())) for url in image_urls
        ]
        await asyncio.gather(*tasks)


async def download_image_xhs(session, url, output_dir, max_retries=3):
    img_path = Path(output_dir) / f"{url.split('/')[-1]}.png"
    if img_path.exists():
        print(f"Skipping existing img: {url}")
        return

    for attempt in range(max_retries):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    image_data = await response.read()
                    try:
                        image = Image.open(BytesIO(image_data))
                        image = image.convert("RGBA")
                        image.save(img_path, "PNG")
                        print(f"Downloaded and converted: {url}")
                        return
                    except Image.UnidentifiedImageError:
                        heif_file = pillow_heif.read_heif(BytesIO(image_data))
                        image = Image.frombytes(
                            heif_file.mode,
                            heif_file.size,
                            heif_file.data,
                            "raw",
                            heif_file.mode,
                            heif_file.stride,
                        )
                        image = image.convert("RGBA")
                        image.save(img_path, "PNG")
                        print(f"Downloaded and converted: {url}")
                        return
                else:
                    print(f"Wrong response status of {url}: {response.status}")
                    return
        except (ClientError, asyncio.TimeoutError) as e:
            if attempt < max_retries - 1:
                print(
                    f"Error downloading {url}: {e}. Retrying... (Attempt {attempt + 1})"
                )
                await asyncio.sleep(5)
            else:
                print(f"Failed to download {url} after {max_retries} attempts: {e}")
                return


async def download_images_xhs(txt_file, output_dir, max_concurrent=10, max_retries=3):
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_download(url):
            async with semaphore:
                await download_image_xhs(session, url, output_dir, max_retries)

        with open(txt_file, "r") as file:
            image_urls = file.readlines()

        tasks = [
            asyncio.create_task(bounded_download(url.strip())) for url in image_urls
        ]
        await asyncio.gather(*tasks)


async def download_image_pexels(session, url, id_, output_dir, max_retries=3):
    img_path = Path(output_dir) / f"{id_}.png"
    if img_path.exists():
        print(f"Skipping existing img: {url}")
        return

    for attempt in range(max_retries):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    image_data = await response.read()
                    image = Image.open(BytesIO(image_data))
                    image = image.convert("RGBA")
                    image.save(img_path, "PNG")
                    print(f"Downloaded and converted: {url}")
                    return
                else:
                    print(f"Wrong response status of {url}: {response.status}")
                    return
        except (ClientError, asyncio.TimeoutError) as e:
            if attempt < max_retries - 1:
                print(
                    f"Error downloading {url}: {e}. Retrying... (Attempt {attempt + 1})"
                )
                await asyncio.sleep(5)
            else:
                print(f"Failed to download {url} after {max_retries} attempts: {e}")


async def download_images_pexels(
    txt_file, output_dir, max_concurrent=10, max_retries=3
):
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_download(url, id_):
            async with semaphore:
                await download_image_pexels(session, url, id_, output_dir, max_retries)

        with open(txt_file, "r") as file:
            image_urls, image_ids = [], []
            for line in file.readlines():
                url, id_ = line.split(" ")
                image_urls.append(url)
                image_ids.append(id_)
            print(f"Found {len(image_urls)} images to download")

        tasks = [
            asyncio.create_task(bounded_download(url.strip(), id_.strip()))
            for (url, id_) in zip(image_urls, image_ids)
        ]

        await asyncio.gather(*tasks)


async def main():
    args = await parse_cmd()

    txt_paths = args.txt_paths
    txt_dir = args.txt_dir
    output_dir = args.output_dir
    max_concurrent = args.max_concurrent
    max_retries = args.max_retries
    platform = args.platform

    if not Path(output_dir).exists():
        Path(output_dir).mkdir(parents=True, exist_ok=True)

    if txt_dir != "":
        if not Path(txt_dir).exists():
            raise FileNotFoundError(f"{txt_dir} does not exist")

        txt_paths = [
            str(path) for path in Path(txt_dir).glob("*.txt") if path.is_file()
        ]

    if platform == "xhs":
        download_images = download_images_xhs
    elif platform == "pexels":
        download_images = download_images_pexels
    elif platform == "unsplash":
        download_images = download_images_pexels
    elif platform == "huaban":
        download_images = download_images_pexels
    elif platform == "weibo":
        download_images = download_images_weibo
    else:
        print(f"Platform not supported: {platform}")
        return

    for txt_path in txt_paths:
        if not Path(txt_path).exists():
            print(f"{txt_path} does not exist")
            continue

        for _ in range(max_retries):
            try:
                await download_images(txt_path, output_dir, max_concurrent, max_retries)
                print(f"Finished downloading {txt_path}")
                break
            except aiohttp.client_exceptions.ClientConnectionError:
                print("Connection error, retrying in 30 seconds...")
                time.sleep(30)


if __name__ == "__main__":
    try:
        asyncio.get_event_loop().run_until_complete(main())
    except KeyboardInterrupt:
        sys.exit()
