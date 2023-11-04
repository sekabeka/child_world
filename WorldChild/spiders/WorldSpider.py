import scrapy
from bs4 import BeautifulSoup
import lxml
import pandas as pd
import re

class ChildWorld(scrapy.Spider):
    name = 'ChildWorld'
    unique = 0

    def start_requests(self):
        p = pd.read_excel('E:\proga\world-child\WorldChild\WorldChild\Ссылки детмир.xlsx').to_dict('list')
        start_urls = p['Ссылки на категории товаров']
        roots_categories = p['Корневая']
        add_categories = p['Подкатегория 1']
        placements = p['Размещение на сайте']
        prefixs = p['Префиксы']
        for url, root, add, pref, place in zip(start_urls, roots_categories, add_categories, prefixs, placements):
            kwargs = {
                'root_category' : root,
                'add_category' : add,
                'prefix' : pref,
                'placement' : place,
                'page' : 1,
                'domain' : url
            }
            yield scrapy.Request(url, cb_kwargs=kwargs)
    


    def ReceiveInfo(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        brand = soup.find('span', attrs={'data-testid' : 'brandName'}).text.strip()
        title = soup.find('h1', attrs={'data-testid' : 'productTitle'}).text.strip()
        div_contain_sections = soup.find('div', attrs={'data-testid' : 'productSections'})
        for count, section in enumerate(div_contain_sections.find_all('section', recursive=False)):
            match count:
                case 0:
                    pictures = section.find_all("picture")
                    images = ' '.join([i.source['srcset'] for i in pictures])
                    images = re.sub(r'(\d+x) | (\d+x,)', '', images)
                case 1:
                    ul = section.find('ul')
                    if ul:
                        ul = ul.find_all('li')
                        markers = ' '.join([li.text for li in ul])
                    else:
                        markers = None
                case 2:
                    if section.find('p', attrs={'data-testid' : 'price'}):
                        price = re.sub(r'[^,\.0-9]','',section.find('p', attrs={'data-testid' : 'price'}).text)
                        if '%' in section.find('p', attrs={'data-testid' : 'price'}).find_next().text:
                            sale_size = re.sub('\D', '', section.find('p', attrs={'data-testid' : 'price'}).find_next().text)
                        else:
                            sale_size = None
                    else:
                        price = 'Нет в наличии'
                        sale_size = None
                case 3:
                    description = section.find('section', attrs={'data-testid' : 'descriptionBlock'})
                    if description:
                        description = re.sub(r'\xa0', ' ', description.div.text.strip())
                    else:
                        description = None
                    characteristic = section.find('section', attrs={'data-testid' : 'characteristicBlock'})
                    tmp = {}
                    if characteristic:
                        table = characteristic.table
                        for it in table.find_all('tr'):
                            match it.th.text.strip().lower():
                                case 'артикул':
                                    article = it.td.text.strip()
                                    continue
                                case 'страна производства':
                                    name, prop = (f'Параметр: Страна-производитель', it.td.text.strip())
                                case 'продавец':
                                    continue
                                case _ :
                                    name, prop = (f'Параметр: {it.th.text.strip()}', it.td.text.strip())
                            tmp[name] = prop
                    else:
                        pass
        self.logger.info(f'we have {self.unique} items')
        
        return {
            'Категория' : kwargs.pop('root_category'),
            'Подкатегория' : kwargs.pop('add_category'),
            'Артикул' : kwargs.pop('prefix') + article,
            'Название товара или услуги' : title,
            'Размещение на сайте' : kwargs.pop('placement'),
            'Описание товара' : description,
            'Ссылка на товар' : response.url,
            'Цена продажи' : None,
            'Старая цена' : format(float((1 + int(sale_size) / 100) * 2 * float(price.replace(',', '.'))), '.2f').replace('.', ',') if price != 'Нет в наличии' and sale_size != None and sale_size else None,
            'Цена закупки' : price.replace('.', ','),
            'Изображения' : images,
            'Параметр: Бренд' : brand,
            'Параметр: Производитель' : brand,
            'Параметр: Артикул поставщика' : article,
            'Параметр: Размер скидки' : sale_size,
            'Параметр: Метки' : markers,
            **kwargs, **tmp
        }
        
    def handler(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        self.unique += 1
        if 'page' and 'domain' in kwargs.keys():
            kwargs.pop('page')
            kwargs.pop('domain')
        if soup.find('div', attrs={'data-testid' : 'variantsBlock'}):
            if 'zoozavr' in response.url:
                variants = [('https://www.zoozavr.ru' + i['href'], i.text.strip()) for i in soup.find('div', attrs={'data-testid' : 'variantsBlock'}).find_all('a', attrs={'data-testid' : 'variantsItem'})]
            else:
                variants = [('https://www.detmir.ru' + i['href'], i.text.strip()) for i in soup.find('div', attrs={'data-testid' : 'variantsBlock'}).find_all('a', attrs={'data-testid' : 'variantsItem'})]
            for url, var in variants:
                if url != response.url:
                    kwargs["Свойство: вариант"] = var
                    yield scrapy.Request(url, callback=self.ReceiveInfo, cb_kwargs=kwargs)
                else:
                    kwargs["Свойство: вариант"] = var
                    yield self.ReceiveInfo(response=response, **kwargs)
        else:
            yield self.ReceiveInfo(response=response, **kwargs)
            
    def parse(self, response, **kwargs):
        self.logger.info(f'we go {response.url}')
        soup = BeautifulSoup(response.text, 'lxml')  
        domain, page = kwargs['domain'], kwargs['page']  
        if 'zoo' in domain:
            products = soup.find_all('section', id=re.compile(r'product-\d+'))
            for prod in products:
                if prod.find(string=re.compile(r'Товар закончился|Только в розничных магазинах')):
                    self.logger.error(f'We have no available products {response.url, products.index(prod)}')
                    return
                link = prod.find('a')['href']
                yield scrapy.Request(link, callback=self.handler, cb_kwargs=kwargs)
        else:
            products = soup.find_all('section', id=re.compile(r'\d+'))
            for prod in products:
                link = prod.find(href=re.compile(r'.*?www\.detmir\.ru.*'))['href']
                if prod.find(string=re.compile(r'Товар закончился|Только в розничных магазинах')):
                    self.logger.error(f'We have no available products {response.url, products.index(prod)}')
                    break
                yield scrapy.Request(link, callback=self.handler, cb_kwargs=kwargs)
        
        if soup.find(string=re.compile(r"показать ещё", flags=re.I)):
            new_url = domain + f'page/{page + 1}'
            kwargs['page'] += 1
            yield scrapy.Request(new_url, callback=self.parse, cb_kwargs=kwargs)  
       
    
            


    def closed(self, reason):
        import json
        import pandas as pd

        with open('E:\proga\world-child\WorldChild\child.jsonl', 'r', encoding='utf-8') as file:
            s = file.readlines()
        result = [json.loads(item) for item in s]
        roots = set([i['Категория'] for i in result])
        p = pd.DataFrame(result)
        with pd.ExcelWriter('child.xlsx', engine='xlsxwriter', engine_kwargs={'options' : {'strings_to_urls': False}}) as writer:
            p.to_excel(writer, index=False, sheet_name='products')
            p = p.drop_duplicates(['Параметр: Артикул поставщика'])
            p.to_excel(writer, index=False, sheet_name='unique_products')
            for name in roots:
                tmp = []
                for prod in result:
                    if prod['Категория'] == name:
                        tmp.append(prod)
                df = pd.DataFrame(tmp)
                df.to_excel(writer, index=False, sheet_name=name)
          