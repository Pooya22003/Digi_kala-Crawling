from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time
import pandas as pd
import os
import uuid

# Couchbase
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException

# --- تنظیمات Couchbase ---
COUCHBASE_HOST = "couchbase://127.0.0.1"
COUCHBASE_USER = "pooya"
COUCHBASE_PASS = "pooya10000"  # ← رمز عبور خود را وارد کنید
BUCKET_NAME = "mobile_products"

cluster = Cluster(
    COUCHBASE_HOST,
    ClusterOptions(PasswordAuthenticator(COUCHBASE_USER, COUCHBASE_PASS)),
)
bucket = cluster.bucket(BUCKET_NAME)
collection = bucket.default_collection()

# --- Selenium ---
service = Service("chromedriver.exe")
driver = webdriver.Chrome(service=service)

file_name = os.path.join(
    os.path.expanduser("~"), "Desktop", "mobile-phone-custom-pages.csv"
)
all_data = []

MAX_PAGES = 10  # تعداد صفحات برای پردازش

for page in range(1, MAX_PAGES + 1):
    if page == 1:
        url = "https://www.digikala.com/search/category-mobile-phone/"
    else:
        url = f"https://www.digikala.com/search/category-mobile-phone/?page={page}"

    driver.get(url)
    time.sleep(2)

    products = driver.find_elements(
        By.CSS_SELECTOR, "div.product-list_ProductList__item__LiiNI"
    )
    if not products:
        print(f"❌ صفحه {page} محصولی نداشت")
        break

    print(f"✅ صفحه {page}، تعداد محصولات: {len(products)}")

    for product in products:
        product_dict = {}
        # عنوان
        try:
            title_el = product.find_element(By.CSS_SELECTOR, "h3")
            product_dict["عنوان"] = title_el.text
            product_url = title_el.find_element(By.XPATH, "..").get_attribute("href")
        except:
            product_dict["عنوان"] = "نامشخص"
            product_url = None

        # قیمت
        try:
            price_raw = product.find_element(
                By.CSS_SELECTOR, 'span[data-testid="price-final"]'
            ).text
            digits = "".join(filter(str.isdigit, price_raw))
            price_digits = int(digits) if digits else 0
            product_dict["قیمت ریال"] = price_digits
            million = price_digits // 1_000_000
            thousand = (price_digits % 1_000_000) // 1_000
            if million == 0 and thousand == 0:
                product_dict["قیمت"] = "نامشخص"
            elif thousand == 0:
                product_dict["قیمت"] = f"{million} میلیون تومان"
            else:
                product_dict["قیمت"] = f"{million} میلیون و {thousand} هزار تومان"
        except:
            product_dict["قیمت"] = "نامشخص"
            product_dict["قیمت ریال"] = 0

        # وضعیت ارسال
        try:
            product_dict["وضعیت ارسال"] = product.find_element(
                By.CSS_SELECTOR, "p.text-caption.text-neutral-600"
            ).text
        except:
            product_dict["وضعیت ارسال"] = "نامشخص"

        # جزئیات صفحه محصول
        if product_url:
            try:
                driver.execute_script("window.open(arguments[0]);", product_url)
                driver.switch_to.window(driver.window_handles[1])
                time.sleep(2)
                trs = driver.find_elements(By.CSS_SELECTOR, "tr")
                for tr in trs:
                    try:
                        th_text = tr.find_element(By.TAG_NAME, "th").text.strip()
                        td_text = tr.find_element(By.TAG_NAME, "td").text.strip()
                        if th_text:
                            product_dict[th_text] = td_text
                    except:
                        continue
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
            except:
                driver.switch_to.window(driver.window_handles[0])

        all_data.append(product_dict)

        # ذخیره در Couchbase
        try:
            doc_id = str(uuid.uuid4())
            collection.upsert(doc_id, product_dict)
        except CouchbaseException as e:
            print(f"❌ خطا در ذخیره محصول در Couchbase: {e}")

driver.quit()

# ذخیره در CSV
df = pd.DataFrame(all_data)
df.to_csv(file_name, index=False, encoding="utf-8-sig")
print(
    f"✅ تمام داده‌ها در فایل {file_name} ذخیره شدند. تعداد کل محصولات: {len(all_data)}"
)
