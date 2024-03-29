import asyncio
import os
import json
from datetime import datetime
import sys
from bs4 import BeautifulSoup
import csv
import yadisk
import aiohttp
import requests
import seleniumwire.undetected_chromedriver as UC
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

def SetLogg(logg_text: str, type: int=1, newline: str = "\n") -> str:
    logg_info_types: dict = {-1: f"Error! {logg_text}", 0: f"Warning! {logg_text}", 1: logg_text}
    
    now = datetime.now()
    hour = f"0{now.hour}" if now.hour < 10 else now.hour
    minute = f"0{now.minute}" if now.minute < 10 else now.minute
    second = f"0{now.second}" if now.second < 10 else now.second
    
    return f"[{hour}:{minute}:{second}]: {logg_info_types[type]}{newline}"

def DoLogg(logg_text: str, type: int=1):
    print(SetLogg(logg_text, type=type, newline=""))
    
    if LOGG_FILE != None:
        LOGG_FILE.write(SetLogg(logg_text, type=type))

async def WaitUntilInternetConnectionIsAvailable(times: int, timeout: int) -> bool:
    for _ in range(times):
        if await GetIsInternetConnectionAvailable(timeout=timeout) == True:
            return True
        else:
            await asyncio.sleep(5)
            continue
        
    return False

async def GetIsInternetConnectionAvailable(timeout: int, url: str="https://ya.ru/") -> bool: 
    try:
        _ = requests.head(url=url, timeout=timeout)
        return True
    except requests.ConnectionError:
        return False

async def GetInternetConnectionStatus():
    is_internet_connection_available = False
    
    if TRY_RECONNECT == True:
        is_internet_connection_available = await WaitUntilInternetConnectionIsAvailable(times=RECONNECTION_TIMES, timeout=CONNECTION_TIMEOUT)
    else:
        is_internet_connection_available = await GetIsInternetConnectionAvailable(timeout=CONNECTION_TIMEOUT)
        
    return is_internet_connection_available

async def PauseProgramUntilInternetConnectionIsAvailable() -> None:
    DoLogg("Выполняется проверка подключения к интернету")
    
    while await GetInternetConnectionStatus() != True:
        if LOGG_FILE != None:
            LOGG_FILE.write(SetLogg("Подключение к интернету не установлено, ожидание ввода пользователя"))
        
        print(SetLogg("Подключение к интернету не установлено, попытаться установить подключение повторно? [y/n]: ", newline=""), end="")
        
        action = input()
        while action not in ["y", "n"]:
            print(SetLogg("Неверный формат выбора, пожалуйста введите ваш выбор из [y/n]: ", newline=""), end="")
            action = input()
            
        if LOGG_FILE != None:
            if action == "y":
                LOGG_FILE.write(SetLogg('Пользователь ввёл "y", выполняется повторная попытка подключения к интернету'))
            else:
                LOGG_FILE.write(SetLogg('Пользователь ввёл "n", парсер завершил работу'))
                
        if action == "y":
            continue
        else:
            LOGG_FILE.close()
            sys.exit()
    
    DoLogg("Подключение к интернету установлено")

def GetConfig():
    BASE_CONFIG_SETTINGS = {"internet_reconnection": 0,
                            "internet_reconnection_attempts": 10,
                            "internet_connection_timeout": 5,
                            "request_delay": 5,
                            "logging": 1,
                            "ya_disk_token": '"empty"',
                            "ya_disk_save_folder": '"empty"',
                            "sender_mail_address": '"empty"',
                            "sender_mail_password": '"empty"',
                            "recipient_mail_address": '"empty"',
                            "save_file_after_parsing": 1}
    
    CONFIG_SETTING_TYPES = {"internet_reconnection": int,
                            "internet_reconnection_attempts": int,
                            "internet_connection_timeout": int,
                            "request_delay": int,
                            "logging": int,
                            "ya_disk_token": str,
                            "ya_disk_save_folder": str,
                            "sender_mail_address": str,
                            "sender_mail_password": str,
                            "recipient_mail_address": str,
                            "save_file_after_parsing": int}
    
    def SetConfigFileWithBaseSettings() -> None:
        with open("src//config.txt", "w+", encoding="UTF-8") as config_file:
            for setting in BASE_CONFIG_SETTINGS:
                config_file.write(f'"{setting}": {BASE_CONFIG_SETTINGS[setting]}\n')

    def ReadConfigFile() -> str:
        config_setting_params = open("src//config.txt", "r", encoding="UTF-8").readlines()
        config_string = '{"settings": {'

        for param in config_setting_params:
            config_string  += param.replace("\n", "") + ","
            
        config_string = config_string [0:len(config_string) - 1] + "}}"
        return config_string
    
    def GetConfigSettings() -> dict:
        config_string = ReadConfigFile()
        config_settings = dict()
        
        try:
            config_object = json.loads(config_string)
            if len(config_object["settings"]) != len(BASE_CONFIG_SETTINGS):
                raise Exception("Config file pass failed")       
        except:
            SetConfigFileWithBaseSettings()
            config_object = json.loads(ReadConfigFile())
        finally:         
            for setting in BASE_CONFIG_SETTINGS:
                if setting in config_object["settings"]:
                    config_setting = config_object["settings"][setting]
                    
                    if type(config_setting) == CONFIG_SETTING_TYPES[setting]:
                        
                        if setting in ["internet_reconnection", "logging", "save_file_after_parsing"]:
                            if config_setting in [0, 1]:
                                config_settings.update({setting: config_setting})
                            else:
                                SetConfigFileWithBaseSettings()
                                config_settings.update({setting: BASE_CONFIG_SETTINGS[setting]})
                                
                        elif setting == "internet_connection_timeout":
                            if (config_setting > 0) and (config_setting < 300):
                                config_settings.update({setting: config_setting})
                            else:
                                SetConfigFileWithBaseSettings()
                                config_settings.update({setting: BASE_CONFIG_SETTINGS[setting]})
                                
                        elif setting == "internet_reconnection_attempts":
                            if (config_setting > 0) and (config_setting < 1200):
                                config_settings.update({setting: config_setting})
                            else:
                                SetConfigFileWithBaseSettings()
                                config_settings.update({setting: BASE_CONFIG_SETTINGS[setting]})
                                
                        elif setting == "request_delay":
                            if (config_setting >= 5) and (config_setting < 120):
                                config_settings.update({setting: config_setting})
                            else:
                                SetConfigFileWithBaseSettings()
                                config_settings.update({setting: BASE_CONFIG_SETTINGS[setting]})
                        
                        else:
                            config_settings.update({setting: config_setting})
                                       
                    else:
                        SetConfigFileWithBaseSettings()
                        config_settings.update({setting: BASE_CONFIG_SETTINGS[setting]})
            
            return config_settings
    
    if os.path.exists("src") == True:
        if os.path.isfile("src//config.txt") != True:
            SetConfigFileWithBaseSettings()              
    else:
        os.mkdir("src")
        SetConfigFileWithBaseSettings() 
        
    return GetConfigSettings()

class Driver():
    def __init__(self):
        self.driver: UC.Chrome
    
    async def WaitElementBy(self, type: str, xpath: str, timeout: int = 10):
        waiting_types = {
                "click": EC.element_to_be_clickable((By.XPATH, xpath)), 
                "single": EC.presence_of_element_located((By.XPATH, xpath)), 
                "all": EC.presence_of_all_elements_located((By.XPATH, xpath)), 
                "all_visibility": EC.visibility_of_all_elements_located((By.XPATH, xpath)), 
                "single_visibility": EC.visibility_of_element_located((By.XPATH, xpath)),
            }
            
        try:    
            element = WebDriverWait(self.driver, timeout).until(
                waiting_types[type]
            )
        except:
            element = None
        finally:
            return element
    
    async def GetUrl(self, url: str) -> bool:
        is_url_opened = -1
        
        try:
            self.driver.get(url=url)
        except:
            pass
        else:
            is_url_opened = 1
        finally:
            return is_url_opened
    
    async def Start(self, proxy: str) -> int:
        driver_start_status = -1
        
        try:
            DoLogg("Запуск драйвера")
                        
            seleniumwire_options = {
                'proxy': {
                    'http': f"http://{proxy}",
                    'https': f"https://{proxy}",
                }
            }
            
            chrome_options = UC.ChromeOptions()
            chrome_options.page_load_strategy = "eager"
            
            for option in ["--headless=new", "--blink-settings=imagesEnabled=false", "--disable-logging", "--log-level=3", "--disable-crash-reporter"]:
                chrome_options.add_argument(option)
            
            if proxy == "":
                self.driver = UC.Chrome(options=chrome_options)
            else:
                self.driver = UC.Chrome(seleniumwire_options=seleniumwire_options, options=chrome_options)
                   
        except Exception as e:
            error_message = str(e)
            
            DoLogg(f"Критическая ошибка! Запуск драйвера невозможен, причина: {error_message}", -1)
            DoLogg("Парсер завершил работу")
            sys.exit()
            
        else:
            self.driver.set_page_load_timeout(30)
            driver_start_status = 1
            
            DoLogg("Драйвер запущен")
        finally:
            return driver_start_status
        
    async def Close(self):
        self.driver.close()
        self.driver.quit()
        
class Parser(Driver):
    def __init__(self, ya_token: str, ya_foldername: str, sender_mail: str, sender_password: str, recipient_mail: str):
        self.base_url = "Website is classified"
        self.ya_token = ya_token
        self.ya_foldername = ya_foldername
        self.sender_mail = sender_mail
        self.sender_password = sender_password
        self.recipient_mail = recipient_mail
    
    async def WaitUntilDriverStart(self, proxy: str):
        while await self.Start(proxy=proxy) != 1:
            continue
    
    async def CheckProxiesIsValidForParsing(self, proxies: str) -> bool:
        await PauseProgramUntilInternetConnectionIsAvailable()
        
        try:
            conn = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=conn) as session:
                async with session.get(url=self.base_url, proxy=f"http://{proxies}", timeout=CONNECTION_TIMEOUT) as response:
                    if response.status == 200:
                        return True     
        except:
            pass
        
        return False

    async def GetValidProxies(self) -> str:
        proxies = ""
        
        while await self.CheckProxiesIsValidForParsing(proxies=proxies) != True:
            if proxies == "": 
                DoLogg('IP адрес не подходит для парсинга по причне "Удалённый сервер не может обработать запрос адреса из-за перегрузки", попробуйте использовать прокси', -1)
                print(SetLogg('Введите адрес прокси сервера в одном из форматов: "логин:пароль@адрес:порт"/"адрес:порт": ', newline=""), end="")
            else:
                DoLogg(f'Прокси сервер {proxies} не подходит для парсинга, попробуйте его изменить', -1)
                print(SetLogg('Введите адрес прокси сервера в одном из форматов: "логин:пароль@адрес:порт"/"адрес:порт": ', newline=""), end="")
                
            proxies = input()
            
        return proxies
    
    async def GetSubcategoryPageData(self, url: str) -> list:
        page_data = []
         
        while await self.GetUrl(url) != 1:
            if await GetInternetConnectionStatus() == True:
                await asyncio.sleep(5)
                continue
            else:       
                DoLogg(f"Соединение с интернетом разорвано при попытке открыть ссылку {url}")
                
                await PauseProgramUntilInternetConnectionIsAvailable()
                continue
            
        page_html = self.driver.page_source
        page_parser = BeautifulSoup(page_html, "html.parser")
        
        products_list = page_parser.find("div", {"class": "sale reviewed block-view flexible-title active"})
        if products_list != None:
            product_blocks = products_list.find_all("div", {"class": "sale-text-box"})
            
            if len(product_blocks) > 0:
                for product_block in product_blocks:

                    product_code = ""
                    product_name = ""
                    product_price = ""
                    product_url = ""
                    product_stock = ""
                        
                    product_name_block = product_block.find("strong", {"class": "sale-title"})
                    if product_name_block != None:
                        product_name_description = product_name_block.find("a")
                        
                        if product_name_description != None:
                            raw_product_name = product_name_description.text.replace("\n", "")
                                                         
                            for i in range(0, len(raw_product_name) - 1):
                                if (raw_product_name[i] == " ") and (raw_product_name[i + 1] == " "):
                                    continue
                                else:
                                    product_name += raw_product_name[i]
                                    
                            product_url = self.base_url + product_name_description["href"]                      
                            product_code = product_name_description["href"][::-1]
                            product_code = product_code[0:product_code.index("-")][::-1]
                        
                    product_price_text_block = product_block.find("div", {"class": "price-text"})
                    if product_price_text_block != None:
                        try:
                            raw_product_price = product_price_text_block.find("strong").find("span").text
                            for char in raw_product_price:
                                if char.isdigit():
                                    product_price += char
                        except:
                            pass
                        
                        product_price_other_info = product_price_text_block.find_all("em")
                        raw_product_stock = product_price_other_info[len(product_price_other_info) - 1].text.lower()
                        
                        if "под заказ" in raw_product_stock:
                            product_stock = "Под заказ"
                        elif "нет в наличии" in raw_product_stock:
                            product_stock = "Нет в наличии"
                        elif "ожидается" in raw_product_stock:
                            product_stock = "Ожидается"                  
                        else:
                            product_stock = "В наличии"
                            
                    product_data = {"Product code": product_code, "Product name": product_name, "Product price": product_price, "Product stock": product_stock, "Product url": product_url}
                    page_data.append(product_data)
                      
        return page_data
    
    async def GetSubcategoryPageUrls(self, url: str) -> tuple:
        category_page_urls = set()
        category_name = ""
        
        while await self.GetUrl(url) != 1:
            if await GetInternetConnectionStatus() == True:
                DoLogg(f"Ошибка при попытке открыть ссылку {url}, повторная попытка", -1)
                await asyncio.sleep(REQUEST_DELAY)
                continue
            else:       
                DoLogg(f"Соединение с интернетом разорвано при попытке открыть ссылку {url}", -1)
                
                await PauseProgramUntilInternetConnectionIsAvailable()
                continue
                
        page_html = self.driver.page_source
        page_parser = BeautifulSoup(page_html, "html.parser")
        
        category_main_block = page_parser.find("div", {"id": "main"})
        if category_main_block != None:
            category_name = category_main_block.find("h1").text
        
        paging_block = page_parser.find("ul", {"class": "paging"})
        if paging_block != None:               
            page_blocks = paging_block.find_all("li")
            
            if len(page_blocks) > 0:
                for page_block in page_blocks:     
                    page_url_block = page_block.find("a")
                    
                    if page_url_block != None:
                        category_page_urls.add(self.base_url + page_url_block["href"])
        else:
            category_page_urls.add(url)
            
        return (category_page_urls, category_name)
                  
    async def GetSubcategoryUrls(self) -> set:
        category_for_parsing_urls = []
        
        page_html = self.driver.page_source
        page_parser = BeautifulSoup(page_html, "html.parser")
     
        dropbox = page_parser.find("div", {"class": "dropbox-holder"})
        if dropbox != None:
            categories_tabset = dropbox.find("ul", {"id": "tabset"})
            if categories_tabset != None:
                subcategories_tabset = dropbox.find_all("div", {"data-mcs-theme": "dark"})                          
                categories = categories_tabset.find_all("li")
                
                if ((len(categories) > 0) and (len(subcategories_tabset) > 0)) and (len(categories) == len(subcategories_tabset)):
                    for i in range(len(categories)):
                        category = categories[i].text.replace("   ", "")
                        category = category[1:len(category)]
                        
                        if category in NECESSARY_CATEGORIES:
                            subcategories = subcategories_tabset[i].find_all("a")
                            
                            for subcategory in subcategories:         
                                category_for_parsing_urls.append(self.base_url + subcategory["href"])
                 
        return set(category_for_parsing_urls)
    
    async def SaveDataToYandexDisk(self):
        
        await PauseProgramUntilInternetConnectionIsAvailable()
    
        disk = yadisk.YaDisk(token=self.ya_token)
        while True:
            try:
                assert disk.check_token() == True
            except:
                await PauseProgramUntilInternetConnectionIsAvailable()
            
                print(SetLogg("Яндекс токен недействителен, пожалуйста, введите действительный Яндекс токен: ", newline="", type=0), end="")
                if LOGG_FILE != None:
                    LOGG_FILE.write(SetLogg("Яндекс токен недействителен, ожидание ввода пользователем действительного Яндекс токена", 0))
                    
                self.ya_token = input()
                disk = yadisk.YaDisk(token=self.ya_token)
            else:
                break
              
        if disk.is_dir(self.ya_foldername) != True:
            disk.mkdir(self.ya_foldername)
            
        disk.upload(CSV_FILENAME, f"{self.ya_foldername}/{CSV_FILENAME}")
        DoLogg("Отправил файл на яндекс диск")
        
    async def SaveDataToMail(self):
        
        message = MIMEMultipart()
        message["From"] = self.sender_mail
        message["To"] = self.recipient_mail
        message["Subject"] = CSV_FILENAME
        
        csv_file = open(CSV_FILENAME, "rb")
        
        csv_file_attachment = MIMEBase('application', 'octet-stream')
        csv_file_attachment.set_payload(csv_file.read())
        encoders.encode_base64(csv_file_attachment)
        csv_file_attachment.add_header('Content-Disposition', f'attachment; filename={CSV_FILENAME}')
        message.attach(csv_file_attachment)
        
        while True:
            await PauseProgramUntilInternetConnectionIsAvailable()
            
            try:
                smtp_server = smtplib.SMTP_SSL('smtp.yandex.com')
                smtp_server.ehlo(self.sender_mail)
                smtp_server.login(self.sender_mail, self.sender_password)
                smtp_server.auth_plain()
                smtp_server.sendmail(self.sender_mail, self.recipient_mail, message.as_string())
            except:
                
                DoLogg("Имя почты отправителя или пароль неверные")
                self.sender_mail = input("Введите имя почты отправителя: ")
                self.sender_password = input("Введите пароль почты отправителя: ")
                continue
            
            else:
                smtp_server.quit()
                csv_file.close()
                
                DoLogg("Отправил файл на почту")
                break
           
    async def Parse(self):
        
        proxy = await self.GetValidProxies()     
        await self.WaitUntilDriverStart(proxy=proxy)
        
        DoLogg(f"Открытие ссылки {self.base_url}")
        
        while await self.GetUrl(self.base_url) != 1:
            if GetInternetConnectionStatus() == True:
                f"Ошибка при попытке открыть ссылку {self.base_url}, повторная попытка"
                await asyncio.sleep(REQUEST_DELAY)
                continue
            else:       
                DoLogg(f"Соединение с интернетом разорвано при попытке открыть ссылку {self.base_url}", -1)
                
                await PauseProgramUntilInternetConnectionIsAvailable()
                continue
            
        DoLogg(f"Ссылка {self.base_url} успешно открыта")
                
        pop_up_buttons = await self.WaitElementBy("all", "//div[@class='ordering-form']//a[@class='btn btn-green popup-link']")
        if pop_up_buttons != None:  
            await asyncio.sleep(2)
            pop_up_buttons[2].click()
            await asyncio.sleep(3)
        
        change_shop_element = await self.WaitElementBy("single", "//label[@onclick='onChangeShop_1();']")
        if change_shop_element != None:
            await asyncio.sleep(2)
            change_shop_element.click()
            await asyncio.sleep(3)
        
        csv_headers = ["Product code", "Product name", "Product price", "Product stock", "Product url"]
        csv_file = open(CSV_FILENAME, "w+", encoding="cp1251", newline="")
        csv_writer = csv.DictWriter(csv_file, delimiter=";", dialect="excel", fieldnames=csv_headers)
        
        csv_writer.writeheader()
        
        DoLogg(f"Создан файл {CSV_FILENAME}")
        DoLogg(f"Получение ссылок на подкатегории для парсинга")
           
        category_for_parsing_urls = await self.GetSubcategoryUrls()                
        if len(category_for_parsing_urls) > 0:
            DoLogg(f"Ссылки на подкатегории для парсинга получены")
            DoLogg(f"Начало парсинга")
            
            for category_url in category_for_parsing_urls:
                category_page_data = await self.GetSubcategoryPageUrls(url=category_url)
                
                category_page_urls = category_page_data[0]
                category_name = category_page_data[1]
                
                DoLogg(f"Парсю категорию {category_name}")
                
                if len(category_page_urls) > 0:
                    for category_page_url in category_page_urls:
                        
                        category_page_data = await self.GetSubcategoryPageData(url=category_page_url)
                        if len(category_page_data) > 0:
                            for row in category_page_data:
                                csv_writer.writerow(row)
                        
                        await asyncio.sleep(REQUEST_DELAY)
                        
                DoLogg(f"Спарсил категорию {category_name}")
        
        csv_file.close()
                    
async def main():
    global CONNECTION_TIMEOUT
    global TRY_RECONNECT
    global RECONNECTION_TIMES
    global NECESSARY_CATEGORIES 
    global LOGG_FILE
    global TODAY
    global CSV_FILENAME
    global REQUEST_DELAY
    
    CONFIG = GetConfig()
    """
    internet_reconnection - выполнять переподключения к интернету (0 - нет, 1 - да)
    internet_reconnection_attempts - количество переподключений к интернету
    internet_connection_timeout - тайм-аут подключения к интернету и для запросов (в секундах)
    request_delay - задержка между запросами
    logging - логировать процесс парсинга (0 - нет, 1 - да)
    sender_mail_address - электронная почта отправителя
    sender_mail_password - пароль приложения от электронной почты отправителя
    recipient_mail_address - электронная почта получателя
    ya_disk_token - токен яндекс ID
    ya_disk_save_folder - папка в Яндекс диске для сохранения файлов
    save_file_after_parsing - сохранить файл после парсинга (0 - нет, 1 - да)
    """
    
    if CONFIG["logging"] == 1:
        LOGG_FILE = open("logg.txt", "a", encoding="UTF-8")
    else:
        LOGG_FILE = None   

    TODAY = str(datetime.today())
    TODAY = TODAY[0:TODAY.index(" ")].split("-")
    
    DoLogg(f"Работа парсера на {TODAY[2]}.{TODAY[1]}.{TODAY[0]}")
    DoLogg("Парсер запущен")
    
    CSV_FILENAME = f"{TODAY[2]}.{TODAY[1]}_Website is classified.csv"
         
    NECESSARY_CATEGORIES = [
        "Телевизоры, проекторы, игровые консоли, гаджеты, аксессуары",
        "Планшеты, смартфоны и аксессуары",
        "Ноутбуки, моноблоки, неттопы и аксессуары",
        "Комплектующие, мониторы и аксессуары",
        "Компьютерные и офисные аксессуары",
        "Сетевое оборудование и аксессуары",
        "Периферийное оборудование, оргтехника и РМ"]
    
    CONNECTION_TIMEOUT = CONFIG["internet_connection_timeout"]
    TRY_RECONNECT = True if CONFIG["internet_reconnection"] == 1 else False
    RECONNECTION_TIMES = CONFIG["internet_reconnection_attempts"]
    REQUEST_DELAY = CONFIG["request_delay"]
    
    parser = Parser(ya_token=CONFIG["ya_disk_token"], ya_foldername=CONFIG["ya_disk_save_folder"], 
                    sender_mail=CONFIG["sender_mail_address"], sender_password=CONFIG["sender_mail_password"],
                    recipient_mail=CONFIG["recipient_mail_address"]
                )
    await parser.Parse()
    
    DoLogg("Парсинг окончен")  
    DoLogg("Отправляю файл на яндекс диск")  
    await parser.SaveDataToYandexDisk()
    
    DoLogg("Отправляю файл на почту")
    await parser.SaveDataToMail()
    
    DoLogg("Парсер закончил работу")
    
    if LOGG_FILE != None:
        LOGG_FILE.close()
        
    if CONFIG["save_file_after_parsing"] == 0:
        os.remove(CSV_FILENAME)
        
    sys.exit()

if __name__ == "__main__":
    asyncio.run(main())