import re
import numpy as np
import pandas as pd

from base import CleanerBaseClass


class CleanerConfig:
    DESCRIPTION_CALCS = 'Opis i wyliczenia'
    DESCRIPTION = 'Opis'
    NUMBER_ROW = 'Lp.'
    BASE = 'Podstawa'
    CALCULATIONS = 'Wyliczenia'
    UNIT = 'j.m.'
    EACH = 'Poszcz'
    TOTAL = 'Razem'
    ANALOGY = 'analogia'
    KNR = 'KNR'
    NR_SPEC = 'Nr spec. Technicznej'


class NormaExportEstimateCleaner(CleanerBaseClass):
    def clean(self) -> pd.DataFrame:
        """
        process all tables, concat them and return as dataframe
        :return:
        list
        """
        tables: list[pd.DataFrame] = []
        for df in self._group_by_columns():
            if CleanerConfig.DESCRIPTION_CALCS in df.columns:
                _df = self._clean_dataframe(df, self.remove_newlines)
                tables.append(_df)
        return tables[0]

    def _group_by_columns(self) -> list[pd.DataFrame]:
        """
        concat tabels by columns match
        :return: list of df's
        """
        grouped_tabels: dict[str, list[pd.DataFrame]] = {}
        for df in self.tables:
            key = str(df.columns.to_list())
            if key in grouped_tabels.keys():
                grouped_tabels[key].append(df)
            else:
                grouped_tabels[key] = [df]

        return [pd.concat(v) for k, v in grouped_tabels.items()]
    

    @classmethod
    def _clean_dataframe(cls, table: pd.DataFrame, remove_newlines: bool) -> pd.DataFrame:
        """
        Converts df's to json type
        :param table: dataframe with table data
        :return: dataframe
        """

        pipeline = [
            cls._unify_columns_names,
            cls._clean_n_lines_base,
            cls._clean_n_lines_description,
            cls._move_from_bad_column,
            cls._move_calculations,
        ]
        columns_mapped = {
            CleanerConfig.DESCRIPTION_CALCS: CleanerConfig.DESCRIPTION,
        }
        columns_order = [
            CleanerConfig.NUMBER_ROW,
            CleanerConfig.BASE,
            CleanerConfig.DESCRIPTION,
            CleanerConfig.CALCULATIONS,
            CleanerConfig.UNIT,
            CleanerConfig.EACH,
            CleanerConfig.TOTAL
        ]
        if len(table.columns) == 7:
            columns_order.insert(1, CleanerConfig.NR_SPEC)

        for step in pipeline:
            table = step(table)

        table = cls._rename_column_name(table, columns_mapped)
        table = cls._change_columns_order(table, columns_order)
        return table

    @staticmethod
    def _unify_columns_names(table: pd.DataFrame) -> pd.DataFrame:
        """
        Unify columns names
        :param table: dataframe
        :return: dataframe
        """
        unified_columns = [
            CleanerConfig.NUMBER_ROW,
            CleanerConfig.BASE,
            CleanerConfig.DESCRIPTION_CALCS,
            CleanerConfig.UNIT,
            CleanerConfig.EACH,
            CleanerConfig.TOTAL
        ]
        if len(table.columns) == 7:
            unified_columns.insert(1, CleanerConfig.NR_SPEC)

        table.columns = unified_columns
        return table

    @staticmethod
    def _clean_n_lines_base(table: pd.DataFrame) -> pd.DataFrame:
        """
        Merge rows to avoid something like (n0 - K.0.2, n1 - abc) and make - (n0 - K.0.2 abc)
        :param table: dataframe
        :return: cleaned dataframe
        """
        _base = table.columns.get_loc(CleanerConfig.BASE)

        for idx in range(len(table) - 1):
            current_value = table.iloc[idx, _base]
            next_value = table.iloc[idx + 1, _base]

            if pd.notna(current_value) and pd.notna(next_value):
                if idx + 2 < len(table):
                    next_next_value = table.iloc[idx + 2, _base]
                    if next_next_value == CleanerConfig.ANALOGY:
                        next_value += f' {next_next_value}'
                        table.iloc[idx + 2, _base] = np.nan
                    elif next_value.endswith('-'):
                        next_value += next_next_value
                        table.iloc[idx + 2, _base] = np.nan
                table.iloc[idx, _base] = f"{current_value} {next_value}"
                table.iloc[idx + 1, _base] = np.nan

        return table

    @staticmethod
    def _clean_n_lines_description(table: pd.DataFrame) -> pd.DataFrame:
        """
        if description ends with '-' merge it with next row description
        :param table: dataframe
        :return: dataframe
        """
        _desc = table.columns.get_loc(CleanerConfig.DESCRIPTION_CALCS)

        for idx in range(len(table) - 1):
            try:
                current_value = table.iloc[idx, _desc]
                next_value = table.iloc[idx + 1, _desc]

                if pd.notna(current_value) and pd.notna(next_value) and current_value.endswith('-'):
                    table.iloc[idx, _desc] = f'{current_value[:-1]}{next_value}'
                    table.iloc[idx + 1, _desc] = np.nan

            except KeyError as e:
                continue

        return table

    @staticmethod
    def _move_from_bad_column(table: pd.DataFrame) -> pd.DataFrame:
        """
        move bad values from lp and base columns
        :param table:
        :return:
        """
        _base = table.columns.get_loc(CleanerConfig.BASE)
        _desc = table.columns.get_loc(CleanerConfig.DESCRIPTION_CALCS)
        _lp = table.columns.get_loc(CleanerConfig.NUMBER_ROW)
        _jm = table.columns.get_loc(CleanerConfig.UNIT)
        _posz = table.columns.get_loc(CleanerConfig.EACH)
        pattern = r'^[a-zA-Z]\.\d+$'
        for idx in range(len(table) - 1):
            try:
                base_value = table.iloc[idx, _base]
                desc_value = table.iloc[idx, _desc]
                lp_value = table.iloc[idx, _lp]
                jm_value = table.iloc[idx, _jm]
                posz_value = table.iloc[idx, _posz]

                if CleanerConfig.KNR in str(desc_value):
                    table.iloc[idx, _base] = desc_value
                    table.iloc[idx, _desc] = np.nan
                if CleanerConfig.KNR in str(base_value):
                    continue
                if isinstance(jm_value, (float, int)):
                    if pd.notna(posz_value):
                        table.iloc[idx, _posz] = jm_value
                    table.iloc[idx, _jm] = np.nan
                if isinstance(desc_value, str) and len(desc_value) <= 2:
                    if isinstance(jm_value, (float, int)):
                        table.iloc[idx, _jm] = desc_value
                        table.iloc[idx, _posz] = jm_value
                        table.iloc[idx, _desc] = np.nan
                if (isinstance(base_value, (float, int)) and (isinstance(desc_value, str)
                                                              and len(desc_value) < 4)):
                    table.iloc[idx, _jm] = desc_value
                    table.iloc[idx, _desc] = base_value
                    table.iloc[idx, _base] = np.nan
                if pd.notna(base_value) and pd.isna(desc_value):
                    table.iloc[idx, _desc] = base_value
                    table.iloc[idx, _base] = np.nan
                if pd.notna(lp_value) and pd.isna(desc_value):
                    if re.match(pattern, lp_value):
                        continue
                    table.iloc[idx, _desc] = lp_value
                    table.iloc[idx, _lp] = np.nan
                if isinstance(lp_value, (float, int)):
                    table.iloc[idx, _base] = np.nan

            except KeyError as e:
                continue

        return table

    @staticmethod
    def _move_calculations(table: pd.DataFrame) -> pd.DataFrame:
        """
        move calculations from description to new column
        :param table: dataframe of table
        :return: dataframe
        """
        pattern = r'^[\d\.\s\+\-\*\/\(\)]+$'
        table[CleanerConfig.CALCULATIONS] = np.nan
        table[CleanerConfig.CALCULATIONS] = table[CleanerConfig.CALCULATIONS].astype(object)
        _calc = table.columns.get_loc(CleanerConfig.CALCULATIONS)
        _desc = table.columns.get_loc(CleanerConfig.DESCRIPTION_CALCS)
        _lp = table.columns.get_loc(CleanerConfig.NUMBER_ROW)

        for idx in range(len(table)):
            current_row = table.iloc[idx, _desc]
            lp_row = table.iloc[idx, _lp]
            if isinstance(current_row, str) and re.match(pattern, current_row):
                table.iloc[idx, _calc] = str(current_row)
                table.iloc[idx, _desc] = np.nan
            if isinstance(lp_row, str) and re.match(pattern, lp_row):
                table.iloc[idx, _calc] = lp_row
                table.iloc[idx, _lp] = np.nan

        return table

    @staticmethod
    def _rename_column_name(table: pd.DataFrame, mapped_columns: dict[str, str], cols: bool = True) -> pd.DataFrame:
        """
        rename given columns
        :param table: dataframe
        :param mapped_columns: dict of mapped columns
        :param cols: is cols or index names of columns to rename
        :return: renamed dataframe
        """
        if cols:
            table.rename(columns=mapped_columns, inplace=True)
        else:
            table.rename(index=mapped_columns, inplace=True)
        return table

    @staticmethod
    def _change_columns_order(table: pd.DataFrame, columns_order: list[str]) -> pd.DataFrame:
        """
        change columns names order
        :param table:
        :param columns_order:
        :return:
        """
        return table[columns_order]