from selenium import webdriver 
from selenium.webdriver.remote.webdriver import WebDriver, WebElement
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from typing import NamedTuple, List, Optional
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import multiprocessing
from multiprocessing import Manager, Queue
from datetime import datetime
import pandas as pd
import time
import os

class Deal(NamedTuple):
    track_id: str | None
    category: str
    image_url: str
    current_temperature: float
    expiration_date: datetime | None
    url: str
    title: str
    price: float | None
    initial_price: float | None
    shipping: float | None
    merchant: str
    description: str
    comments_count: int

def install_chrome_driver(headless=True) -> WebDriver:
    """ 
    Installation de Chrome pour Selenium

    See: https://www.digitalcitizen.life/fastest-web-browser/
    """
    options = webdriver.ChromeOptions()
    if headless:
        # options pour éviter la détection du mode headless
        options.add_argument("--headless")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.6723.116 Safari/537.3') # important, les elements ne sont pas chargés sans
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_experimental_option("prefs", { "profile.managed_default_content_settings.images": 2 })
    return webdriver.Chrome(options=options)

class Tools:
    @staticmethod
    def safe(func: callable, default=None) -> any:
        try:
            return func()
        except:
            return default() if callable(default) else default
        
    @staticmethod
    def __unsafe_extract_number(text: str) -> float:
        """ ___@internal___ Extrait un nombre d'une chaîne de caractères. """
        text = text.replace(" ", "").replace(",", ".")
        sign = -1 if "-" in text else 1
        number = float("".join([char for char in text if char.isdigit() or char == "."]))
        return sign * number
    
    @staticmethod
    def extract_number(text: str, default:float=0.0) -> float:
        """ Extrait un nombre d'une chaîne de caractères. """
        return Tools.safe(lambda: Tools.__unsafe_extract_number(text), default)
    
    @staticmethod
    def __unsafe_clean_text(text: str) -> str:
        """ ___@internal___ Nettoie une chaîne de caractères. """
        return text.replace("\n", "").replace("\r", "").replace("\xa0", " ").strip()
    
    @staticmethod
    def clean_text(text: str, default:str="") -> str:
        """ Nettoie une chaîne de caractères. """
        return Tools.safe(lambda: Tools.__unsafe_clean_text(text), default)
    
    @staticmethod
    def __unsafe_extract_date(text: str) -> datetime:
        import re
        """ ___@internal___ Extrait une date d'une chaîne de caractères. """
        match = re.search(r"\d{2}/\d{2}/\d{4}.*?(\d{2}:\d{2})", text)

        if match:
            date_str = re.search(r"\d{2}/\d{2}/\d{4}", text).group()
            time_str = match.group(1)
            return datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
        else:
            raise ValueError("Aucune date trouvée dans le texte.")
    
    @staticmethod
    def extract_date(text: str, default:datetime=None) -> datetime:
        """ Extrait une date d'une chaîne de caractères. """
        return Tools.safe(lambda: Tools.__unsafe_extract_date(text), default)
    
    @staticmethod
    def create_batches(data: List[any], batch_size: int) -> List[List[any]]:
        """ Crée des lots de données """
        return [data[i:i+batch_size] for i in range(0, len(data), batch_size)]

class SeleniumTools:
    @staticmethod
    def find(element: WebElement, selector: str) -> WebElement | None:
        return Tools.safe(lambda: element.find_element(By.CSS_SELECTOR, selector))
    
    @staticmethod
    def get_text(element: WebElement) -> str | None:
        return SeleniumTools.get_attribute(element, "textContent") # alternative plus fiable à element.text
    
    @staticmethod
    def get_attribute(element: WebElement, attribute: str) -> str | None:
        return Tools.safe(lambda: element.get_attribute(attribute))
    
    @staticmethod
    def get_attributes(elements: List[WebElement], attribute: str) -> List[str]:
        attributes = [SeleniumTools.get_attribute(element, attribute) for element in elements]
        return [attribute for attribute in attributes if attribute is not None]
    
    @staticmethod
    def wait_element(driver: WebDriver, selector: str, timeout: float = 10.0) -> WebElement | None:
        return Tools.safe(lambda: WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector))))
    
    @staticmethod
    def wait_elements(driver: WebDriver, selector: str, timeout: float = 10.0) -> List[WebElement]:
        return Tools.safe(lambda: WebDriverWait(driver, timeout).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))), default=[])
    
class SortBy:
    NEW = "new"
    HIGHEST_PRICE = "highest_price"
    LOWEST_PRICE = "lowest_price"
    TEMPERATURE = "temp"
    DISCUSSIONS = "discussion"

class TimeFrame:
    ALL = 0
    WEEK = 7
    MONTH = 30
    YEAR = 365

class CategoryFilters(NamedTuple):
    hide_expired: bool = True
    hide_local: bool = True
    time_frame: TimeFrame = TimeFrame.WEEK
    sort_by: SortBy = SortBy.NEW
    page: int = 1

class ScraperOptions(NamedTuple):
    hubs_url = "https://www.dealabs.com/groupe"
    headless: bool = True
    logs: bool = False
    wait_timeout: float = 0.5
    max_pages_per_category: Optional[int] = None
    max_categories: Optional[int] = None
    max_hubs: Optional[int] = None
    max_threads: int = 4
    max_workers: int = multiprocessing.cpu_count()
    driver: Optional[WebDriver] = None

def log_to_file(message: str) -> None:
    with open("logs.txt", "a") as file:
        file.write(f"{message}\n")

def append_to_csv(data: List[Deal], filename: str) -> None:
    df = pd.DataFrame(data)
    df.to_csv(filename, mode='a', header=False, index=False)

class DealabsCategoryScraper:
    def __init__(self, category_url: str, options=ScraperOptions()):
        self.category_url = category_url
        self.options = options
        self.driver = options.driver

    def __debug(self, *values) -> None:
        if self.options.logs:
            print(*values)

    def build_url(self, filters: CategoryFilters) -> str:
        """ Construit l'URL de la catégorie en fonction des options """
        return f"{self.category_url}?hide_expired={filters.hide_expired}&hide_local={filters.hide_local}&time_frame={filters.time_frame}&sort={filters.sort_by}&page={filters.page}"

    def get_category(self) -> str:
        """ Récupère le nom de la catégorie à partir de l'URL """
        return self.category_url.split("/")[-1]

    def __start_driver(self) -> None:
        """ Initialise un WebDriver scopé à la catégorie """
        if not self.driver:
            self.driver = install_chrome_driver(headless=self.options.headless)

    def __stop_driver(self) -> None:
        """ Ferme le WebDriver scopé à la catégorie """
        if self.driver is not self.options.driver:
            self.driver.quit()

    # def __clear_parasites(self) -> None:
    #     """ Supprime les popups parasites """
    #     timeout = self.options.wait_timeout
    #     Tools.safe(SeleniumTools.wait_element(self.driver, "[data-t=continueWithoutAccepting]", timeout=timeout).click(), lambda: self.__debug("Pas de popup cookie."))
    #     Tools.safe(SeleniumTools.wait_element(self.driver, "div.popover-content button.button--type-secondary", timeout=timeout).click(), lambda:  self.__debug("Pas de popup parasite."))

    def __get_total_pages_count(self) -> int:
        """ Récupère le nombre total de pages à partir de la pagination """
        total_page_count = SeleniumTools.wait_element(self.driver, 'button[aria-label="Dernière page"]', timeout=self.options.wait_timeout)
        return int(Tools.extract_number(SeleniumTools.get_text(total_page_count), default=1))
    
    def __get_pages_range(self, start_from=2) -> range:
        """ Récupère la plage des pages à parcourir """
        total_pages_count = self.__get_total_pages_count()
        if self.options.max_pages_per_category:
            total_pages_count = min(total_pages_count, self.options.max_pages_per_category)
        return range(start_from, total_pages_count + 1)
    
    def __extract_deal(self, element: WebElement) -> Deal:
        """ Extrait les informations d'un deal """
        
        # identifiant du deal
        track_id = SeleniumTools.get_attribute(element, "id")
        # illustration du deal
        image_url = SeleniumTools.get_attribute(SeleniumTools.find(element, "div.threadGrid-image span.imgFrame img"), "src")
        # température format -0° (négatif) ou 0° (positif)
        current_temperature = SeleniumTools.get_text(SeleniumTools.find(element, "div.threadGrid-headerMeta button.vote-temp"))
        # date d'expiration format JJ/MM/AAAA (suffix optionnel: à HH:MM)
        expiration_date = SeleniumTools.get_text(SeleniumTools.find(element, "div.threadGrid-headerMeta span.metaRibbon svg.icon--hourglass + span"))
        # lien absolu vers le deal
        deal_url = SeleniumTools.get_attribute(SeleniumTools.find(element, "strong.thread-title a"), "href")
        # titre du deal
        deal_title = SeleniumTools.get_text(SeleniumTools.find(element, "strong.thread-title"))
        # container des caractéristiques du deal
        deal_characteristics = SeleniumTools.find(element, "div.threadGrid-title > span.overflow--fade")
        # prix du deal
        deal_price = SeleniumTools.get_text(SeleniumTools.find(deal_characteristics, "span.thread-price"))
        # prix initial du deal
        deal_initial_price = SeleniumTools.get_text(SeleniumTools.find(deal_characteristics, "span.text--lineThrough"))
        # prix de la livraison
        deal_shipping = SeleniumTools.get_text(SeleniumTools.find(deal_characteristics, "span:has(svg.icon--truck) + span"))
        # marchand du deal
        deal_merchant = SeleniumTools.get_text(SeleniumTools.find(deal_characteristics, "a[data-t=merchantLink]"))
        # description du deal
        deal_description = SeleniumTools.get_text(SeleniumTools.find(element, "div.threadGrid-body"))
        # nombre de commentaires
        comments_count = SeleniumTools.get_text(SeleniumTools.find(element, "div.threadGrid-footerMeta a[title=Commentaires]"))

        return Deal(
            track_id=track_id,
            category=self.get_category(),
            image_url=Tools.clean_text(image_url),
            current_temperature=Tools.extract_number(current_temperature, 0),
            expiration_date=Tools.extract_date(expiration_date),
            url=Tools.clean_text(deal_url),
            title=Tools.clean_text(deal_title),
            price=Tools.extract_number(deal_price) if deal_price else None,
            initial_price=Tools.extract_number(deal_initial_price) if deal_initial_price else None,
            shipping=Tools.extract_number(deal_shipping) if deal_shipping else None,
            merchant=Tools.clean_text(deal_merchant),
            description=Tools.clean_text(deal_description),
            comments_count=Tools.extract_number(comments_count, 0)
        )

    def __get_deals(self) -> List[Deal]:
        """  Récupère les deals de la page """
        deals = self.driver.find_elements(By.CSS_SELECTOR, 'article.thread--deal')
        deals = [Tools.safe(lambda: self.__extract_deal(deal)) for deal in deals]
        return [deal for deal in deals if deal is not None]

    def scrape_all_deals(self, filters=CategoryFilters()) -> List[Deal]:
        """ Récupère tous les deals de la catégorie """
        url = self.build_url(filters)

        self.__start_driver()
        self.driver.get(url)
        # self.__clear_parasites()
        
        deals = self.__get_deals()

        for page in self.__get_pages_range():
            url = self.build_url(filters._replace(page=page))
            self.driver.get(url)
            # self.__clear_parasites()
            deals += self.__get_deals()

        self.__stop_driver()
        return deals
    
class DealabsCategoryBatchScraper:
    def __init__(self, urls: List[str], options=ScraperOptions()) -> None:
        self.urls = urls
        self.options = options
    
    @staticmethod
    def scrape_category(url: str, options:ScraperOptions) -> List[Deal]:
        """ Fonction indépendante pour scraper une catégorie """
        scraper = DealabsCategoryScraper(url, options)
        return scraper.scrape_all_deals()
    
    def scrape_all_categories(self) -> List[Deal]:
        """ Récupère tous les deals de la catégories en parallèle """
        results: List[Deal] = []
        categories = self.urls
        categories = categories[:self.options.max_categories] if self.options.max_categories else categories # restriction du nombre de catégories (si défini)
        
        with ThreadPoolExecutor(self.options.max_threads) as executor:
            futures = {executor.submit(DealabsCategoryBatchScraper.scrape_category, url, self.options): url for url in categories}
            for future in as_completed(futures):
                try:
                    deals = future.result()
                    results += deals
                except Exception as e:
                    log_to_file(f"[scrape_all_categories] Erreur lors du scraping de {futures[future]} : {e}")

        return results
    
class DealabsScraper:
    def __init__(self, options=ScraperOptions()) -> None:
        self.options = options

    def __debug(self, *values) -> None:
        if self.options.logs:
            print(*values)

    def __extract_categories(self, driver: WebDriver, url: str) -> List[str]:
        """ Récupère les liens des catégories à scraper """
        driver.get(url)
        return SeleniumTools.get_attributes(driver.find_elements(By.CSS_SELECTOR, 'div#pageContent div.listLayout-main a'), 'href')

    def fetch_category_links(self) -> List[str]:
        """ Récupère séquentiellement les liens de toutes les catégories à scraper """
        driver = install_chrome_driver(headless=self.options.headless)
        driver.get(self.options.hubs_url)
        hub_links = SeleniumTools.get_attributes(driver.find_elements(By.CSS_SELECTOR, 'div.listLayout-main a.button--type-secondary'), 'href')
        category_links = [self.__extract_categories(driver, hub) for hub in hub_links]
        driver.quit()
        return [link for links in category_links for link in links]
    
    @staticmethod
    def scrape_category_batch(urls: List[str], options:ScraperOptions, results_queue:Queue) -> List[dict]:
        """ Fonction indépendante pour scraper un batch de catégories avec une instance unique de WebDriver par processus """
        print(f"[PID {os.getpid()}] Initialisation d'un nouveau driver chrome")
        driver = install_chrome_driver(headless=options.headless)
        scraper = DealabsCategoryBatchScraper(urls, options._replace(driver=driver))
        deals = scraper.scrape_all_categories()
        # Ajouter les résultats au gestionnaire de queue pour synchronisation
        results_queue.put(deals)
        driver.quit()
        print(f"[PID {os.getpid()}] Fermeture du driver chrome")
    
    def scrape_all_hubs(self) -> List[Deal]:
        """ Récupère tous les deals de toutes les catégories de tous les hubs en parallèle """
        start = time.time()
        category_links = self.fetch_category_links()
        results: List[Deal] = []

        
        category_links = category_links[:self.options.max_categories] if self.options.max_hubs else category_links # restreindre le nombre de categories (si défini)
        category_batches = Tools.create_batches(category_links, 10)


        with Manager() as manager:
            results_queue: Queue = manager.Queue()
            results: List[Deal] = []
        
            num_processes = min(self.options.max_workers, multiprocessing.cpu_count()) # Nombre de processus maximum
            with ProcessPoolExecutor(max_workers=num_processes) as executor:
                futures = {executor.submit(DealabsScraper.scrape_category_batch, batch, self.options, results_queue): batch for batch in category_batches}

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        import traceback
                        log_to_file(f"Erreur lors du scraping de {futures[future]} : {e}")
                        log_to_file(traceback.format_exc())
            
            while not results_queue.empty():
                results.extend(results_queue.get())

        end = time.time()
        self.__debug(f"[scrape_all_hubs] Temps d'exécution: {end - start} secondes.")
        return results
    
if __name__ == "__main__":
    # options = ScraperOptions(max_hubs=1, max_categories=10, max_pages_per_category=None, max_workers=4, logs=True)
    options = ScraperOptions(max_workers=4, logs=True)
    scraper = DealabsScraper(options=options)
    deals = scraper.scrape_all_hubs()
    if len(deals) > 0:
        df = pd.DataFrame(deals)
        df.to_csv("deals_complete.csv", index=False)