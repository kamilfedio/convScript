import os
from dotenv import load_dotenv
import tabula
from pandas import DataFrame
from pathlib import Path

from estimate import NormaExportEstimateCleaner
from forecast import ForecastCleaner


class ConvertPDF:
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
    PATH = os.getenv('PATH_TO_FILES')

    @classmethod
    def drop_unnamed(cls, page: DataFrame) -> DataFrame:
        cols = []
        for col in page.columns:
            if "Unnamed" not in col:
                cols.append(col)

        return page[cols]

    @classmethod
    def check(cls, page: DataFrame) -> bool:

        if page.empty:
            return False
        if len(page.columns) < 3:
            return False
        if page.isnull().sum().sum() / (len(page) * len(page.columns)) >= .7 and len(page.columns) <= 3:
            return False
        if page[1:].isnull().sum().sum() / (len(page) * len(page.columns)) >= .7:
            return False
        if len(cls.drop_unnamed(page).columns) < 2:
            return False

        return True

    @classmethod
    def clear_pages(cls, filename: str) -> list[DataFrame]:
        if not filename.endswith('.pdf'):
            filename += '.pdf'
        reader = tabula.read_pdf(Path(Path(cls.PATH) / filename), pages='all')
        pages = [cls.drop_unnamed(page) for page in reader if cls.check(page)]

        return pages


filename = 'file-1'
if __name__ == '__main__':
    pages: list[DataFrame] = ConvertPDF.clear_pages(filename)

    # res: DataFrame = NormaExportEstimateCleaner(pages, True).clean() #przedmiar
    res: DataFrame = ForecastCleaner(pages, True).clean() #kosztorys inwestorski
    res.to_csv('./file.csv', sep=';', encoding='utf-8', index=False, columns=res.columns)
    print(res.head(5))
