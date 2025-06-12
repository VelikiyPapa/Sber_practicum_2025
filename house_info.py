import asyncio
import pandas as pd
import re
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm

EXCEL_INPUT = "hous_hunter_urls.xlsx"
EXCEL_OUTPUT = "house_info.xlsx"
N_WORKERS = 10

BASE_URL = "https://xn--80az8a.xn--d1aqf.xn--p1ai/сервисы/единый-реестр-застройщиков/застройщик/"

BANKS_TAB_SELECTOR = 'li.Tabs__NewTabsItem-sc-jsq7op-3:has-text("Уполномоченные банки")'
BANK_CELL_SELECTOR = 'div.BaseCell__Cell-sc-7809tj-0.gUdgAY'
STATS_SELECTOR = 'p[class^="BuilderCardStatisticsData__Number-sc"]'
GROUP_LINK_SELECTOR = 'a[class^="BuilderCardHeader__GroupLink-sc"]'
HOUSES_LINK_SELECTOR = 'span[class^="BuilderCardHousesLinks__ButtonText-sc"]'

def chunk_list(lst, n):
    """Разделяет список lst на n примерно равных кусков."""
    return [lst[i::n] for i in range(n)]

async def process_company(page, company_id):
    url = BASE_URL + str(company_id)
    try:
        await page.goto(url, timeout=40000)
        await page.wait_for_timeout(1800)

        banks_tab = await page.query_selector(BANKS_TAB_SELECTOR)
        if banks_tab:
            await banks_tab.click()
            await page.wait_for_timeout(1200)
        bank_cells = await page.query_selector_all(BANK_CELL_SELECTOR)
        bank_cells_values = [await cell.inner_text() for cell in bank_cells] if bank_cells else ["-"]
        banks_first = bank_cells_values[0].split(";")[0] if bank_cells_values and bank_cells_values[0] != "-" else "-"

        stats_map = {
            "домов": "-",
            "строятся с задержкой": "-",
            "квартир": "-",
            "тыс. м² жилой площади": "-"
        }
        container_selector = 'div[class^="BuilderCardStatisticsData__Container-sc"]'
        stat_containers = await page.query_selector_all(container_selector)
        for container in stat_containers:
            number_el = await container.query_selector('p[class^="BuilderCardStatisticsData__Number-sc"]')
            text_el = await container.query_selector('p[class^="BuilderCardStatisticsData__Text-sc"]')
            if number_el and text_el:
                number = await number_el.inner_text()
                text = (await text_el.inner_text()).strip().lower()
                for key in stats_map:
                    if key in text:
                        stats_map[key] = number
        stats_final = [
            stats_map["домов"],
            stats_map["строятся с задержкой"],
            stats_map["квартир"],
            stats_map["тыс. м² жилой площади"]
        ]

        group_links = await page.query_selector_all(GROUP_LINK_SELECTOR)
        group_links_values = [await link.inner_text() for link in group_links] if group_links else ["-"]

        houses_links = await page.query_selector_all(HOUSES_LINK_SELECTOR)
        houses_links_values = [await link.inner_text() for link in houses_links] if houses_links else ["-"]
        house_numbers = []
        for val in houses_links_values:
            found = re.findall(r'\d+', val)
            house_numbers.extend(found)
        houses_links_str = ";".join(house_numbers) if house_numbers else "-"

        return {
            "ID": company_id,
            "Banks": banks_first,
            "Stats_1": stats_final[0],
            "Stats_2": stats_final[1],
            "Stats_3": stats_final[2],
            "Stats_4": stats_final[3],
            "GroupLinks": "; ".join(group_links_values),
            "HousesLinks": houses_links_str,
        }
    except Exception as e:
        return {
            "ID": company_id,
            "Banks": "ERROR",
            "Stats_1": "ERROR",
            "Stats_2": "ERROR",
            "Stats_3": "ERROR",
            "Stats_4": "ERROR",
            "GroupLinks": str(e),
            "HousesLinks": "ERROR"
        }

async def worker(companies, results, pbar, worker_id):
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False) # Всегда False, иначе жесть
        context = await browser.new_context()
        page = await context.new_page()
        for company_id in companies:
            res = await process_company(page, company_id)
            results.append(res)
            pbar.update(1)
        await browser.close()

async def main():
    df = pd.read_excel(EXCEL_INPUT)
    company_ids = df["ID"].dropna().astype(int).tolist()
    # company_ids = company_ids[:100] # Для теста!!!
    total = len(company_ids)
    pools = chunk_list(company_ids, N_WORKERS)
    manager = asyncio.Manager() if hasattr(asyncio, 'Manager') else None

    results = []
    pbar = tqdm(total=total, desc="Парсинг компаний", ncols=100)

    tasks = [worker(pools[i], results, pbar, i) for i in range(N_WORKERS)]
    await asyncio.gather(*tasks)
    pbar.close()

    out_df = pd.DataFrame(results)
    out_df.to_excel(EXCEL_OUTPUT, index=False)
    print(f"\nДанные по {len(results)} компаниям сохранены в {EXCEL_OUTPUT}")

if __name__ == "__main__":
    asyncio.run(main())
