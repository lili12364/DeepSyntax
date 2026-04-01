import asyncio
import logging
from typing import Dict, List
import os
import shutil
from datetime import datetime
from collections import defaultdict

from .scraper import BoutiqaatBrandScraper
from .s3_uploader import S3Uploader
from .excel_generator import ExcelGenerator
from config import TEMP_DIR, S3_EXCEL_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Hardcoded brand URLs for group 62 (URLs 1221–1240)
BRAND_URLS = [
    "https://www.boutiqaat.com/ar-kw/men/level-1/br/",
    "https://www.boutiqaat.com/ar-kw/men/levelo-1/br/",
    "https://www.boutiqaat.com/ar-kw/men/lexon-1/br/",
    "https://www.boutiqaat.com/ar-kw/men/lecoxen-1/br/",
    "https://www.boutiqaat.com/ar-kw/men/liquides-imaginaires/br/",
    "https://www.boutiqaat.com/ar-kw/men/laila-alyafie/br/",
    "https://www.boutiqaat.com/ar-kw/men/lignestbarth/br/",
    "https://www.boutiqaat.com/ar-kw/men/lena/br/",
    "https://www.boutiqaat.com/ar-kw/men/lenscare/br/",
    "https://www.boutiqaat.com/ar-kw/men/lens-revive-1/br/",
    "https://www.boutiqaat.com/ar-kw/men/lengling-munich/br/",
    "https://www.boutiqaat.com/ar-kw/men/liu-jo/br/",
    "https://www.boutiqaat.com/ar-kw/men/leo-eyewear-1/br/",
    "https://www.boutiqaat.com/ar-kw/men/matchit-1/br/",
    "https://www.boutiqaat.com/ar-kw/men/majda-bekkali-1/br/",
    "https://www.boutiqaat.com/ar-kw/men/majouri/br/",
    "https://www.boutiqaat.com/ar-kw/men/magic-pouch-1/br/",
    "https://www.boutiqaat.com/ar-kw/men/matriskin-1/br/",
    "https://www.boutiqaat.com/ar-kw/men/martiderm/br/",
    "https://www.boutiqaat.com/ar-kw/men/merveille-perfume-1/br/",
]


class BoutiqaatBrandPipeline:
    """Scrape, process and upload products for a batch of brands."""

    def __init__(self):
        self.uploader = S3Uploader()
        self.excel_generator = ExcelGenerator()

    # ------------------------------------------------------------------
    # Async layer
    # ------------------------------------------------------------------

    async def _process_brand_async(
        self, semaphore: asyncio.Semaphore, url: str
    ) -> bool:
        """Acquire semaphore slot and process one brand URL in a thread."""
        async with semaphore:
            slug = url.rstrip("/").split("/")[-2]
            logger.info(f"[Slot acquired] Starting brand: {slug}")
            scraper = BoutiqaatBrandScraper()
            try:
                return await asyncio.to_thread(self._process_brand, scraper, url)
            except Exception as exc:
                logger.error(f"Error processing {slug}: {exc}")
                return False

    def run(self) -> bool:
        logger.info("=" * 80)
        logger.info("Starting Men's Brand Pipeline (Async – Semaphore=4)")
        logger.info("=" * 80)
        try:
            if not self.uploader.test_connection():
                logger.error("S3 connection failed. Exiting.")
                return False

            logger.info(
                f"Processing {len(BRAND_URLS)} brands (max 4 concurrent)"
            )
            semaphore = asyncio.Semaphore(4)

            async def _gather_all():
                return await asyncio.gather(
                    *[
                        self._process_brand_async(semaphore, url)
                        for url in BRAND_URLS
                    ],
                    return_exceptions=True,
                )

            results = asyncio.run(_gather_all())

            successful = sum(1 for r in results if r is True)
            failed = len(results) - successful
            logger.info("=" * 80)
            logger.info(f"Pipeline Complete: {successful} successful, {failed} failed")
            logger.info("=" * 80)
            return True
        except Exception as exc:
            logger.error(f"Pipeline failed: {exc}")
            return False
        finally:
            import shutil as _shutil
            if os.path.exists(TEMP_DIR):
                try:
                    _shutil.rmtree(TEMP_DIR)
                    logger.info("Cleaned up temporary files")
                except Exception as exc:
                    logger.warning(f"Failed to cleanup temp files: {exc}")

    # ------------------------------------------------------------------
    # Core processing (runs inside asyncio.to_thread)
    # ------------------------------------------------------------------

    def _process_brand(
        self, scraper: "BoutiqaatBrandScraper", brand_url: str
    ) -> bool:
        slug = brand_url.rstrip("/").split("/")[-2]
        brand_name = slug.replace("-", " ").title()

        try:
            products = scraper.get_brand_products(brand_url)
            if not products:
                logger.info(f"Skipping {brand_name} – no products available")
                return True

            logger.info(
                f"Found {len(products)} products for brand: {brand_name}"
            )

            for idx, product in enumerate(products, 1):
                logger.info(
                    f"  [{idx}/{len(products)}] Processing: {product.get('name', 'Unknown')}"
                )
                try:
                    full = scraper.get_product_full_details(product["url"])
                    if full:
                        product.update(full)
                    if product.get("image_url"):
                        product["s3_image_path"] = self._upload_product_image(
                            product, brand_name
                        )
                    else:
                        product["s3_image_path"] = "No image available"
                except Exception as exc:
                    logger.warning(f"    Error processing product: {exc}")
                    continue

            subcategories_data = defaultdict(list)
            for product in products:
                key = product.get("subcategory", brand_name)
                subcategories_data[key].append(product)

            excel_file = self.excel_generator.create_category_workbook(
                brand_name, subcategories_data
            )
            self._upload_excel_file(excel_file, brand_name)

            logger.info(f"\u2713 Completed brand: {brand_name}")
            return True
        except Exception as exc:
            logger.error(f"\u2717 Failed brand {brand_name}: {exc}")
            return False

    # ------------------------------------------------------------------
    # S3 helpers
    # ------------------------------------------------------------------

    def _upload_product_image(self, product: Dict, brand_name: str) -> str:
        try:
            image_url = product.get("image_url")
            sku = product.get("sku", "unknown")
            if not image_url:
                return "No image URL"
            safe = (
                "".join(c for c in brand_name if c.isalnum() or c in " _-")
                .rstrip()
                .replace(" ", "_")
            )
            s3_path = (
                f"boutiqaat-data/year={datetime.now().strftime('%Y')}/"
                f"month={datetime.now().strftime('%m')}/"
                f"day={datetime.now().strftime('%d')}/men/brands/images/{safe}"
            )
            s3_key = self.uploader.upload_image_from_url(image_url, f"{sku}_image.jpg", s3_path)
            return s3_key if s3_key else "Upload failed"
        except Exception as exc:
            logger.warning(f"Error uploading image for {product.get('name')}: {exc}")
            return "Error"

    def _upload_excel_file(self, local_path: str, brand_name: str) -> bool:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe = (
                "".join(c for c in brand_name if c.isalnum() or c in " _-")
                .rstrip()
                .replace(" ", "_")
            )
            s3_path = (
                f"boutiqaat-data/year={datetime.now().strftime('%Y')}/"
                f"month={datetime.now().strftime('%m')}/"
                f"day={datetime.now().strftime('%d')}/men/brands/excel-files"
            )
            s3_key = self.uploader.upload_local_file(
                local_path, s3_path, f"{safe}_{timestamp}.xlsx"
            )
            if s3_key:
                logger.info(f"Excel uploaded: {s3_key}")
                return True
            logger.error(f"Failed to upload Excel: {local_path}")
            return False
        except Exception as exc:
            logger.error(f"Error uploading Excel: {exc}")
            return False


if __name__ == "__main__":
    pipeline = BoutiqaatBrandPipeline()
    success = pipeline.run()
    exit(0 if success else 1)
