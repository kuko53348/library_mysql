"""
Complete facade over library_sqlite.

Exposes ALL public methods of module_sqlite, with the same names and the
same parameters. No added logic, no extra validation, no changed
signatures: it only delegates. This is the single import point.

Usage:
    from modules.wraper_sqlite import Sql

    sql = Sql("inventory.db")
    sql.create_table("register", {...})
    sql.insert_dict("register", {...})
    sql.read_table_values_where_many("register", filters=[...])
    sql.update_table_values_where_many("register", by_data={...}, filters=[...])
    sql.remove_table_values_where_many("register", filters=[...])
"""

from .library_sqlite import module_sqlite


class Sql:
    """
    Complete facade over module_sqlite.

    Every method returns (bool, data), same as the underlying library.
    Parameters are identical to module_sqlite's: nothing is omitted,
    nothing is renamed, nothing gets extra validation.
    """

    def __init__(self, path_db: str = "database.db", timeout: float = 5.0):
        self.path_db = path_db
        self.timeout = float(timeout)
        self.db = module_sqlite(path_db=path_db, timeout=timeout)

    # =================================================================
    # CHECK
    # =================================================================
    def check_table_name(self, table_name: str = "message") -> tuple:
        return self.db.check_table_name(table_name)

    def check_table_name_structure(self, table_name: str = "message") -> tuple:
        return self.db.check_table_name_structure(table_name)

    def check_column(
        self, table_name: str = "message", column_name: str = "column_name"
    ) -> tuple:
        return self.db.check_column(table_name, column_name)

    def check_column_and_value_name(
        self, table_name: str = "message", column_data: tuple = ()
    ) -> tuple:
        return self.db.check_column_and_value_name(table_name, column_data)

    # =================================================================
    # CREATE
    # =================================================================
    def create_table(
        self, table_name: str = "message", column_data: dict | None = None
    ) -> tuple:
        return self.db.create_table(table_name, column_data)

    def create_new_column_in_table(
        self,
        table_name: str = "message",
        column_name: str = "colum_name",
        sql_data_type: str = "TEXT",
    ) -> tuple:
        return self.db.create_new_column_in_table(
            table_name, column_name, sql_data_type
        )

    def create_values_in_table_where(
        self, table_name: str = "message", column_data: tuple = ()
    ) -> tuple:
        return self.db.create_values_in_table_where(table_name, column_data)

    def insert_dict(
        self,
        table_name: str = "message",
        data: dict | None = None,
        defaults: dict | None = None,
        fill_missing: str | int = "",
    ) -> tuple:
        return self.db.insert_dict(
            table_name, data=data, defaults=defaults, fill_missing=fill_missing
        )

    # =================================================================
    # RENAME
    # =================================================================
    def rename_table(
        self,
        old_table_name: str = "message",
        new_table_name: str = "message",
    ) -> tuple:
        return self.db.rename_table(old_table_name, new_table_name)

    def rename_value_in_table(
        self,
        table_name: str = "message",
        old_column_name: str = "message",
        new_column_name: str = "new_data",
    ) -> tuple:
        return self.db.rename_value_in_table(
            table_name, old_column_name, new_column_name
        )

    # =================================================================
    # UPDATE
    # =================================================================
    def update_table_values_where(
        self,
        table_name: str = "message",
        by_data: tuple = (),
        change_data_where: tuple = (),
    ) -> tuple:
        return self.db.update_table_values_where(
            table_name, by_data=by_data, change_data_where=change_data_where
        )

    def update_table_values_where_and_match(
        self,
        table_name: str = "message",
        by_data: tuple = (),
        change_data_where: tuple = (),
        match_with: tuple = (),
    ) -> tuple:
        return self.db.update_table_values_where_and_match(
            table_name,
            by_data=by_data,
            change_data_where=change_data_where,
            match_with=match_with,
        )

    def update_multiple_params_where_and_match(
        self,
        table_name: str = "table_name",
        by_data: dict | None = None,
        change_data_where: list | None = None,
    ) -> tuple:
        return self.db.update_multiple_params_where_and_match(
            table_name, by_data=by_data, change_data_where=change_data_where
        )

    def update_table_values_where_many(
        self,
        table_name: str = "message",
        by_data: dict | None = None,
        filters: list | None = None,
    ) -> tuple:
        return self.db.update_table_values_where_many(
            table_name, by_data=by_data, filters=filters
        )

    def update_where_advanced(
        self,
        table_name: str = "message",
        by_data: dict | None = None,
        filters: list | None = None,
        conjunction: str = "AND",
    ) -> tuple:
        return self.db.update_where_advanced(
            table_name,
            by_data=by_data,
            filters=filters,
            conjunction=conjunction,
        )

    def set_column_all_rows(
        self,
        table_name: str = "message",
        column_name: str = "column_name",
        value_to_set: str | int = "NULL",
    ) -> tuple:
        return self.db.set_column_all_rows(table_name, column_name, value_to_set)

    # =================================================================
    # REMOVE
    # =================================================================
    def remove_table(self, table_name: str = "message") -> tuple:
        return self.db.remove_table(table_name)

    def remove_all_data_from_table(self, table_name: str = "message") -> tuple:
        return self.db.remove_all_data_from_table(table_name)

    def remove_table_values_where(
        self, table_name: str = "message", remove_data_where: tuple = ()
    ) -> tuple:
        return self.db.remove_table_values_where(table_name, remove_data_where)

    def remove_table_values_where_and_match(
        self,
        table_name: str = "message",
        remove_data_where: tuple = (),
        match_with: tuple = (),
    ) -> tuple:
        return self.db.remove_table_values_where_and_match(
            table_name,
            remove_data_where=remove_data_where,
            match_with=match_with,
        )

    def remove_table_values_where_many(
        self, table_name: str = "message", filters: list | None = None
    ) -> tuple:
        return self.db.remove_table_values_where_many(table_name, filters=filters)

    def remove_where_advanced(
        self,
        table_name: str = "message",
        filters: list | None = None,
        conjunction: str = "AND",
    ) -> tuple:
        return self.db.remove_where_advanced(
            table_name, filters=filters, conjunction=conjunction
        )

    def delete_values_in_table_where(
        self, table_name: str = "message", row_id: int = 0
    ) -> tuple:
        return self.db.delete_values_in_table_where(table_name, row_id)

    def delete_all_values_in_table(self, table_name: str = "message") -> tuple:
        return self.db.delete_all_values_in_table(table_name)

    # =================================================================
    # READ
    # =================================================================
    def read_tables_from_database(self) -> tuple:
        return self.db.read_tables_from_database()

    def read_all_table_and_columns(
        self,
        table_name: str = "message",
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        return self.db.read_all_table_and_columns(
            table_name,
            fetch_one=fetch_one,
            limit=limit,
            offset=offset,
            order_by=order_by,
        )

    def read_only_last_column_nums(
        self,
        table_name: str = "message",
        column_name: str = "column_name",
        show_last_num_of_values: int = 10,
        fetch_one: bool = False,
    ) -> tuple:
        return self.db.read_only_last_column_nums(
            table_name,
            column_name=column_name,
            show_last_num_of_values=show_last_num_of_values,
            fetch_one=fetch_one,
        )

    def read_by_id(
        self, table_name: str = "message", row_id: int = 1
    ) -> tuple:
        return self.db.read_by_id(table_name, row_id)

    def read_all_table_and_columns_where(
        self, table_name: str = "message", row_id: int = 0
    ) -> tuple:
        return self.db.read_all_table_and_columns_where(table_name, row_id)

    def read_table_values_where(
        self,
        table_name: str = "message",
        read_data_where: tuple = (),
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        return self.db.read_table_values_where(
            table_name,
            read_data_where=read_data_where,
            fetch_one=fetch_one,
            limit=limit,
            offset=offset,
            order_by=order_by,
        )

    def read_table_values_where_and_match(
        self,
        table_name: str = "message",
        read_data_where: tuple = (),
        match_with: tuple = (),
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        return self.db.read_table_values_where_and_match(
            table_name,
            read_data_where=read_data_where,
            match_with=match_with,
            fetch_one=fetch_one,
            limit=limit,
            offset=offset,
            order_by=order_by,
        )

    def read_table_values_where_many(
        self,
        table_name: str = "message",
        filters: list | None = None,
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        return self.db.read_table_values_where_many(
            table_name,
            filters=filters,
            fetch_one=fetch_one,
            limit=limit,
            offset=offset,
            order_by=order_by,
        )

    def read_all_columns_by_table(
        self,
        table_name: str = "message",
        filters: list | None = None,
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        return self.db.read_all_columns_by_table(
            table_name,
            filters=filters,
            fetch_one=fetch_one,
            limit=limit,
            offset=offset,
            order_by=order_by,
        )

    def read_columns_where(
        self,
        table_name: str = "message",
        columns: list | None = None,
        filters: list | None = None,
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        return self.db.read_columns_where(
            table_name,
            columns=columns,
            filters=filters,
            fetch_one=fetch_one,
            limit=limit,
            offset=offset,
            order_by=order_by,
        )

    def read_where_advanced(
        self,
        table_name: str = "message",
        filters: list | None = None,
        conjunction: str = "AND",
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        return self.db.read_where_advanced(
            table_name,
            filters=filters,
            conjunction=conjunction,
            fetch_one=fetch_one,
            limit=limit,
            offset=offset,
            order_by=order_by,
        )

if __name__ == "__main__":

    # ---------------------------------------------------------------------------
    # Usage:
    #     from modules.wraper_sqlite import Sql
    #     sql = Sql("inventory.db")
    #     sql.insert_dict("register", {...})
    # ---------------------------------------------------------------------------
    # Demo: local instance just to test the file.
    # NOTE: ":memory:" does not work here. Writes go through the single
    # _WriteQueue connection, but every read opens its own new connection
    # (sqlite3.connect(path_db)); with a real file both see the same file on
    # disk, but with ":memory:" every connection is a separate, empty
    # in-RAM database, so any read sees an empty database. That's why the
    # demo uses a real temporary file instead.
    import tempfile

    demo_path = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
    demo = Sql(path_db=demo_path)
    demo.create_table("t", {"a": "TEXT"})
    print(demo.insert_dict("t", {"a": "hello"}))
    print(demo.read_all_table_and_columns("t"))