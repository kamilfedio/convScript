import numpy as np
import pandas as pd

from base import CleanerBaseClass

class ConfigBase:
    LP = 'Lp.'
    DESCRIPTION = 'Opis'
    QUANTITY = 'Ilość'
    ANALOGY = 'analogia'

class ConfigEstimate(ConfigBase):
    BASE = 'Podstawa'
    UNIT = 'Jedn. przedm.'
    UNIT_PRICE = 'Cena jedn.'
    VALUE = 'Wartość'

class ConfigEstimatePlus(ConfigBase):
    BASE = 'Podstawa wyceny'
    UNIT = 'Jedn. miary'
    UNIT_PRICE = 'Cena zł'
    VALUE = 'Wartość zł (5x6)'


class ForecastCleaner(CleanerBaseClass):
    def clean(self) -> pd.DataFrame:
        """
        process all tables, concat them and return as dataframe
        :return: dataframe
        """
        tables: list[pd.DataFrame] = []
        

        for df in self._group_by_columns(self.tables):
            if ConfigBase.DESCRIPTION in df.columns:
                global Config
                if ConfigEstimate.BASE in df.columns:
                    Config = ConfigEstimate
                else:
                    Config = ConfigEstimatePlus
                _df = self._clean_dataframe(df, self.remove_newlines)
                tables.append(_df)

        return tables[0]

        
    @staticmethod
    def _group_by_columns(tables: list[pd.DataFrame]) -> list[pd.DataFrame]:
        """
        concat tabels by columns match
        :param tables: list of df's
        :return: list of df
        """
        grouped_tabels: dict[str, list[pd.DataFrame]] = {}
        for df in tables:
            key = str(df.columns.to_list())
            if key in grouped_tabels.keys():
                grouped_tabels[key].append(df)
            else:
                grouped_tabels[key] = [df]

        return [pd.concat(v) for k, v in grouped_tabels.items()]
    

    def _clean_dataframe(cls, table: pd.DataFrame, remove_newlines: bool) -> pd.DataFrame:
        """
        Converts df's to json type
        :param table: dataframe with table data
        :return: dataframe
        """

        pipeline: list = [
            cls._unify_column_names,
            cls._clean_n_lines_base,
            cls._clean_n_lines_lp,
            cls._move_from_base,
            cls._clean_n_lines_description,
            cls._drop_full_na,
            cls._replace_problematic_characters,
        ]
        for step in pipeline:
            table = step(table)
        
        return table
    
    @staticmethod
    def _unify_column_names(table: pd.DataFrame) -> pd.DataFrame:
        """
        :param table: dataframe
        :return: dataframe
        """
        unified_columns = [
            Config.LP,
            Config.BASE,
            Config.DESCRIPTION,
            Config.UNIT,
            Config.QUANTITY,
            Config.UNIT_PRICE,
            Config.VALUE
        ]

        table.columns = unified_columns
        return table
    
    @staticmethod
    def _move_from_base(table: pd.DataFrame) -> pd.DataFrame:
        """
            move from base column to description
        Args:
            table (pd.DataFrame): _description_

        Returns:
            pd.DataFrame: _description_
        """
        _base = table.columns.get_loc(Config.BASE)
        _desc = table.columns.get_loc(Config.DESCRIPTION)
        for idx in range(len(table)):
            current_value = table.iloc[idx, _base]
            if pd.notna(current_value) and current_value.startswith('Krotność'):
                table.iloc[idx, _desc] = current_value
                table.iloc[idx, _base] = np.nan
        
        return table
    
    @staticmethod
    def _clean_n_lines_base(table: pd.DataFrame) -> pd.DataFrame:
        """
        Merge rows to avoid something like (n0 - K.0.2, n1 - abc) and make - (n0 - K.0.2 abc)
        :param table: dataframe
        :return: cleaned dataframe
        """
        _base = table.columns.get_loc(Config.BASE)
        _lp = table.columns.get_loc(Config.LP)

        for idx in range(len(table) - 1):
            try:
                current_value = table.iloc[idx, _base]
                if pd.isna(current_value):
                    continue
                next_values = []
                for _idx in range(idx+1, len(table)):
                    next_value = table.iloc[_idx, _base]
                    next_lp = table.iloc[_idx, _lp]

                    if pd.notna(next_value) and (pd.isna(next_lp) or 'd' in next_lp):
                        next_values.append(next_value)
                        table.iloc[_idx, _base] = np.nan
                    else:
                        break
                table.iloc[idx, _base] = f'{current_value}{" ".join(next_values)}'
            except KeyError as e:
                continue

        return table
    
    @staticmethod
    def _clean_n_lines_lp(table: pd.DataFrame) -> pd.DataFrame:
        """
        cleans lp column
        """
        _base = table.columns.get_loc(Config.BASE)
        _lp = table.columns.get_loc(Config.LP)
        for idx in range(len(table) - 1):
            try:
                current_value = table.iloc[idx, _lp]
                if pd.isna(current_value) or str(current_value).strip()[-1] != '.':
                    continue
                next_values = []
                for _idx in range(idx+1, len(table) -1):
                    next_lp = table.iloc[_idx, _lp]
                    next_base = table.iloc[_idx, _base]
                    if pd.notna(next_lp) and pd.isna(next_base):
                        next_values.append(next_lp)
                        table.iloc[_idx, _lp] = np.nan
                        if not str(next_lp).endswith('.'):
                            break
                    else:
                        break
                table.iloc[idx, _lp] = f'{current_value}{"".join(next_values)}'
            except KeyError as e:
                pass


        return table

    @staticmethod
    def _clean_n_lines_description(table: pd.DataFrame) -> pd.DataFrame:
        """
        if description ends with '-' merge it with next row description
        :param table: dataframe
        :return: dataframe
        """
        _desc = table.columns.get_loc(Config.DESCRIPTION)
        _base = table.columns.get_loc(Config.BASE)
        _lp = table.columns.get_loc(Config.LP)

        for idx in range(len(table) - 1):
            try:
                current_value = table.iloc[idx, _desc]
                if pd.isna(current_value):
                    continue
                next_values = []
                for _idx in range(idx + 1, len(table)):
                    next_value = table.iloc[_idx, _desc]
                    next_base = table.iloc[_idx, _base]
                    next_lp = table.iloc[_idx, _lp]
                    if pd.notna(next_value) and pd.isna(next_base) and (pd.isna(next_lp) or 'd' in next_lp):
                        if next_value.endswith('-'):
                            next_value = next_value[:-1]
                        next_values.append(next_value)
                        table.iloc[_idx, _desc] = np.nan
                    else:
                        break
                table.iloc[idx, _desc] = f'{current_value[:-1] if current_value.endswith("-") else current_value}{" ".join(next_values)}'
            except KeyError as e:
                continue

        return table

    @staticmethod
    def _drop_full_na(table: pd.DataFrame) -> pd.DataFrame:
        return table.dropna(how='all')
    
    @staticmethod
    def _replace_problematic_characters(table: pd.DataFrame) -> pd.DataFrame:
        """
        return better chars
        """
        def replace(x):
            if isinstance(x, str):
                return x.replace('Ŝ', 'ż')
            return x
        table.applymap(replace)

        return table