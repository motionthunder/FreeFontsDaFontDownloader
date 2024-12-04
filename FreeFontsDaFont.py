import asyncio
import aiohttp
import aiofiles
import os
import zipfile
import py7zr
import patoolib
import rarfile
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from typing import Set, List, Dict
import hashlib
import logging
from tqdm import tqdm
import shutil
from dataclasses import dataclass
from asyncio import Semaphore
import time
import re

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

@dataclass
class FontArchive:
    url: str
    filename: str
    category: int

class DaFontDownloader:
    def __init__(self, output_dir: str, max_concurrent_downloads: int = 10):
        self.output_dir = output_dir
        self.temp_dir = os.path.join(output_dir, '_temp')
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.download_semaphore = Semaphore(max_concurrent_downloads)
        self.seen_fonts = set()
        self.category_names = {}  # Кэш для названий категорий
        self.setup_directories()
        self.setup_logging()

    def setup_directories(self):
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)

    def setup_logging(self):
        logging.basicConfig(
            filename='font_downloader.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    async def get_category_name(self, session: aiohttp.ClientSession, category: int) -> str:
        """Получаем название категории"""
        if category in self.category_names:
            return self.category_names[category]

        url = f'https://www.dafont.com/theme.php?cat={category}&l[]=10&l[]=1'
        try:
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    text = await response.text(encoding='latin-1')
                    soup = BeautifulSoup(text, 'html.parser')
                    # Ищем название категории в хлебных крошках или заголовке
                    category_title = soup.find('title').text.split('-')[0].strip()
                    # Очищаем название от недопустимых символов
                    clean_title = re.sub(r'[^\w\s-]', '', category_title)
                    self.category_names[category] = clean_title
                    return clean_title
        except Exception as e:
            logging.error(f"Error getting category name for {category}: {e}")
        
        fallback_name = f"Category_{category}"
        self.category_names[category] = fallback_name
        return fallback_name

    async def get_category_pages(self, session: aiohttp.ClientSession, category: int) -> List[FontArchive]:
        fonts = []
        page = 1
        seen_fonts_in_category = set()
        repeated_pages = 0
        
        while True:
            url = f'https://www.dafont.com/theme.php?cat={category}&page={page}&l[]=10&l[]=1'
            try:
                async with session.get(url, headers=self.headers) as response:
                    if response.status != 200:
                        break
                    
                    text = await response.text(encoding='latin-1')
                    soup = BeautifulSoup(text, 'html.parser')
                    previews = soup.find_all("div", {"class": "preview"})
                    
                    if not previews:
                        break
                    
                    new_fonts_count = 0
                    for div in previews:
                        try:
                            poster = div["style"].replace("background-image:url(/", "").replace(")", "")
                            down = poster.replace(".png", '').rsplit('/', 1)[-1][:-1]
                            
                            if down not in seen_fonts_in_category:
                                new_fonts_count += 1
                                seen_fonts_in_category.add(down)
                                fonts.append(FontArchive(
                                    url=f"https://dl.dafont.com/dl/?f={down}",
                                    filename=f"{down}.zip",
                                    category=category
                                ))
                        except Exception as e:
                            logging.error(f"Error processing font preview: {e}")
                    
                    print(f"Category {category}: Found {new_fonts_count} new fonts on page {page}")
                    
                    if new_fonts_count == 0:
                        repeated_pages += 1
                        if repeated_pages >= 2:
                            print(f"Category {category}: No new fonts found for 2 pages, stopping")
                            break
                    else:
                        repeated_pages = 0
                    
                    page += 1
                    await asyncio.sleep(0.1)
                    
            except Exception as e:
                logging.error(f"Error processing category {category} page {page}: {e}")
                break
        
        print(f"Category {category}: Total unique fonts found: {len(fonts)}")
        return fonts

    async def download_font(self, session: aiohttp.ClientSession, font: FontArchive) -> tuple[bool, str]:
        async with self.download_semaphore:
            try:
                async with session.get(font.url, headers=self.headers) as response:
                    if response.status != 200:
                        return False, ""
                    
                    filename = response.headers.get('Content-Disposition', '').split('filename=')[-1].strip('"\'')
                    if not filename:
                        filename = font.filename
                    
                    filepath = os.path.join(self.temp_dir, filename)
                    async with aiofiles.open(filepath, 'wb') as f:
                        await f.write(await response.read())
                    return True, filepath
            except Exception as e:
                logging.error(f"Error downloading {font.url}: {e}")
                return False, ""

    def process_archive(self, filepath: str, category_dir: str) -> List[str]:
        try:
            if not os.path.exists(filepath):
                return []

            temp_extract_dir = filepath + "_extracted"
            os.makedirs(temp_extract_dir, exist_ok=True)
            os.makedirs(category_dir, exist_ok=True)

            try:
                if filepath.lower().endswith('.zip'):
                    with zipfile.ZipFile(filepath, 'r') as zip_ref:
                        zip_ref.extractall(temp_extract_dir)
                elif filepath.lower().endswith('.7z'):
                    with py7zr.SevenZipFile(filepath, 'r') as sz_ref:
                        sz_ref.extractall(temp_extract_dir)
                elif filepath.lower().endswith('.rar'):
                    with rarfile.RarFile(filepath, 'r') as rar_ref:
                        rar_ref.extractall(temp_extract_dir)
                else:
                    patoolib.extract_archive(filepath, outdir=temp_extract_dir)
            except Exception as e:
                logging.error(f"Error extracting {filepath}: {e}")
                return []

            processed_files = []
            for root, _, files in os.walk(temp_extract_dir):
                ttf_files = [f for f in files if f.lower().endswith('.ttf')]
                otf_files = [f for f in files if f.lower().endswith('.otf')]
                
                font_files = ttf_files if ttf_files else otf_files
                
                for file in font_files:
                    src_path = os.path.join(root, file)
                    font_hash = self.get_file_hash(src_path)
                    
                    if font_hash not in self.seen_fonts:
                        self.seen_fonts.add(font_hash)
                        dest_path = os.path.join(category_dir, file)
                        shutil.move(src_path, dest_path)
                        processed_files.append(dest_path)

            # Очистка
            try:
                os.remove(filepath)
                shutil.rmtree(temp_extract_dir)
            except Exception as e:
                logging.error(f"Error cleaning up {filepath}: {e}")

            return processed_files
        except Exception as e:
            logging.error(f"Error processing archive {filepath}: {e}")
            return []

    @staticmethod
    def get_file_hash(filepath: str) -> str:
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            buf = f.read(65536)
            while len(buf) > 0:
                hasher.update(buf)
                buf = f.read(65536)
        return hasher.hexdigest()

    async def process_category(self, session: aiohttp.ClientSession, category: int):
        # Получаем название категории
        category_name = await self.get_category_name(session, category)
        category_dir = os.path.join(self.output_dir, f"{category}_{category_name}")
        os.makedirs(category_dir, exist_ok=True)

        fonts = await self.get_category_pages(session, category)
        if not fonts:
            return

        print(f"\nProcessing category {category} ({category_name}): {len(fonts)} fonts found")
        
        # Скачиваем шрифты асинхронно
        download_tasks = [self.download_font(session, font) for font in fonts]
        download_results = await asyncio.gather(*download_tasks)
        
        # Обрабатываем успешно скачанные архивы
        with ThreadPoolExecutor() as executor:
            futures = []
            for success, filepath in download_results:
                if success and filepath:
                    futures.append(
                        executor.submit(self.process_archive, filepath, category_dir)
                    )
            
            for future in futures:
                future.result()  # Ждем завершения обработки

    async def run(self, start_category: int = 101, end_category: int = 805):
        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            for category in range(start_category, end_category + 1):
                try:
                    await self.process_category(session, category)
                    print(f"Completed category {category}")
                    print(f"Total unique fonts processed so far: {len(self.seen_fonts)}")
                except Exception as e:
                    logging.error(f"Error processing category {category}: {e}")
                    continue

def main():
    output_dir = "D:/FONTS/"
    downloader = DaFontDownloader(output_dir)
    
    start_time = time.time()
    asyncio.run(downloader.run())
    end_time = time.time()
    
    print("\nDownload and processing complete!")
    print(f"Total unique fonts processed: {len(downloader.seen_fonts)}")
    print(f"Total time: {(end_time - start_time) / 60:.2f} minutes")

if __name__ == "__main__":
    main()