import asyncio
from playwright.async_api import async_playwright
import pandas as pd

URL = "https://xn--80az8a.xn--d1aqf.xn--p1ai/сервисы/единый-реестр-застройщиков?objStatus=0"
API_PATH = "/api/erz/main/filter"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        companies = []

        async def handle_response(response):
            if API_PATH in response.url and response.status == 200:
                try:
                    data = await response.json()
                    for item in data["data"]["developers"]:
                        companies.append(item)
                except Exception as ex:
                    print("Ошибка обработки XHR:", ex)

        page.on("response", handle_response)
        await page.goto(URL)
        await page.wait_for_timeout(2000)

        clicks = 0
        while clicks < 500:
            try:
                show_more = await page.query_selector('button.ButtonMore__Button-sc-sabgzz-0')
                if show_more:
                    await show_more.click()
                    await page.wait_for_timeout(700)
                    clicks += 1
                    print(f"Клик №{clicks} выполнен")
                else:
                    print("Кнопка 'Показать ещё' не найдена, выходим")
                    break
            except Exception as e:
                print("Ошибка:", e)
                break

        await page.wait_for_timeout(2000)
        await browser.close()

        unique_ids = set()
        results = []
        for item in companies:
            if item["devId"] not in unique_ids:
                unique_ids.add(item["devId"])
                results.append({
                    "ID": item.get("devId", ""),
                    "Name": item.get("devShortNm", ""),
                    "FullName": item.get("devFullCleanNm", ""),
                    "INN": item.get("devInn", ""),
                    "OGRN": item.get("devOgrn", ""),
                    "Phone": item.get("devPhoneNum", ""),
                    "Email": item.get("devEmail", ""),
                    "Region": item.get("regRegionDesc", ""),
                    "Address": item.get("devLegalAddr", ""),
                })

        df = pd.DataFrame(results)
        df.to_excel("hous_hunter_urls.xlsx", index=False)
        print("Собрано:", len(df), "компаний.")

if __name__ == "__main__":
    asyncio.run(main())
