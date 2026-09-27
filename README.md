# library_sqlite — examples

One real case per method. If a method has no example here, it doesn't exist.

*[Versión en español](./docs/README-ES.md)*

---

## Create database and table

```python
from library_sqlite import module_sqlite

db = module_sqlite(path_db="app.db")

db.create_table("message", {
    "user_name":    "TEXT",
    "drag_screen":  "TEXT",
    "time_created": "TEXT NOT NULL DEFAULT (datetime('now'))",
})
# (True, 'Table created: [message]')

db.create_table("stock", {
    "id":         "INTEGER PRIMARY KEY AUTOINCREMENT",
    "sku":        "TEXT NOT NULL",
    "nombre":     "TEXT NOT NULL",
    "cantidad":   "INTEGER NOT NULL DEFAULT 0",
    "created_at": "TEXT NOT NULL DEFAULT (datetime('now'))",
})
# (True, 'Table created: [stock]')
```

---

## Insert

By position:

```python
db.create_values_in_table_where("message", ("javier", "HOME", "2026-09-22"))
# (True, 'Row inserted in table [message]: (...)')
```

By column name (recommended):

```python
db.insert_dict("message", {"user_name": "javier", "drag_screen": "HOME"})
# (True, 1)
```

With defaults:

```python
db.insert_dict(
    "stock",
    data={"sku": "P-1", "nombre": "Cilindro 5kg"},
    defaults={"cantidad": 0},
)
# (True, 1)
```

With an explicit id:

```python
db.insert_dict("stock", {"id": 50, "sku": "P-2", "nombre": "Cilindro 11kg"})
# (True, 50)
```

---

## Read

The whole table:

```python
db.read_all_table_and_columns("message")
# (True, [('javier', 'HOME', '2026-09-22')])
```

With limit and order:

```python
db.read_all_table_and_columns("message", limit=10, offset=0, order_by="user_name ASC")
# (True, [('javier', 'HOME', '2026-09-22')])
```

First row only:

```python
db.read_all_table_and_columns("message", fetch_one=True)
# (True, ('javier', 'HOME', '2026-09-22'))
```

By one column:

```python
db.read_table_values_where("message", ("user_name", "javier"))
# (True, [('javier', 'HOME', '2026-09-22')])
```

With `fetch_one`:

```python
db.read_table_values_where("message", ("user_name", "javier"), fetch_one=True)
# (True, ('javier', 'HOME', '2026-09-22'))
```

No match:

```python
db.read_table_values_where("message", ("user_name", "nobody"), fetch_one=True)
# (True, None)
```

By two columns:

```python
db.read_table_values_where_and_match(
    "message",
    read_data_where=("user_name", "javier"),
    match_with=("drag_screen", "HOME"),
)
# (True, [('javier', 'HOME', '2026-09-22')])
```

By N columns:

```python
db.read_table_values_where_many(
    "stock",
    filters=[("sku", "P-1"), ("nombre", "Cilindro 5kg"), ("cantidad", 0)],
    fetch_one=True,
)
# (True, (1, 'P-1', 'Cilindro 5kg', 0, '2026-09-22 10:00:00'))
```

Only some columns:

```python
db.read_columns_where(
    "stock",
    columns=["sku", "nombre", "cantidad"],
    filters=[("cantidad", 0)],
)
# (True, [('P-1', 'Cilindro 5kg', 0), ('P-2', 'Cilindro 11kg', 0)])

db.read_columns_where(
    "stock",
    columns=["sku", "cantidad"],
    filters=[("sku", "P-1")],
    fetch_one=True,
)
# (True, ('P-1', 10))

db.read_columns_where(
    "stock",
    columns=["nombre", "cantidad"],
    filters=[("cantidad", 0)],
    order_by="nombre ASC",
    limit=5,
)
# (True, [('Cilindro 11kg', 0), ('Cilindro 5kg', 0)])
# note: ASC sorts as TEXT, not numerically -> '1' < '5', so 11kg comes before 5kg

db.read_columns_where(
    "stock",
    columns=["sku"],
    filters=[("cantidad", 999)],
)
# (True, [])

db.read_columns_where(
    "stock",
    columns=["no_existe"],
    filters=[("sku", "P-1")],
)
# (False, 'Column does not exist: table [stock] column [no_existe]')

db.read_columns_where(
    "stock",
    columns=[],
    filters=[("sku", "P-1")],
)
# (False, 'columns must be a non-empty list')

db.read_columns_where(
    "stock",
    columns=["sku"],
    filters=[],
)
# (False, 'filters must be a non-empty list of (column, value) tuples')
```

With operators:

```python
db.read_where_advanced(
    "stock",
    filters=[("cantidad", "BETWEEN", (5, 20)), ("nombre", "LIKE", "%Cilindro%")],
)
# (True, [(1, 'P-1', 'Cilindro 5kg', 10, ...)])

db.read_where_advanced("stock", filters=[("cantidad", ">", 10)])
# (True, [...])

db.read_where_advanced("stock", filters=[("sku", "IN", ["P-1", "P-2"])])
# (True, [...])

db.read_where_advanced("stock", filters=[("nombre", "IS NOT NULL", None)])
# (True, [...])

db.read_where_advanced(
    "stock",
    filters=[("sku", "=", "P-1"), ("cantidad", "=", 0)],
    conjunction="OR",
)
# (True, [...])
```

By rowid:

```python
db.insert_dict("stock", {"sku": "P-3", "nombre": "Cilindro 15kg"})
# (True, 2)

db.read_by_id("stock", 2)
# (True, (2, 2, 'P-3', 'Cilindro 15kg', 0, '2026-09-22 10:00:00'))

db.read_all_table_and_columns_where("stock", 2)
# (True, (2, 2, 'P-3', 'Cilindro 15kg', 0, '2026-09-22 10:00:00'))

db.read_by_id("stock", 9999)
# (False, 'No row found with id 9999 in table [stock]')
```

Last N rows:

```python
db.read_only_last_column_nums("stock", show_last_num_of_values=3)
# (True, [(2, 2, 'P-3', ...), (1, 1, 'P-1', ...)])
```

List tables:

```python
db.read_tables_from_database()
# (True, ['message', 'stock'])
```

---

## Update

By one column:

```python
db.update_table_values_where(
    "message",
    by_data=("user_name", "xavier"),
    change_data_where=("user_name", "javier"),
)
# (True, 'Table [message] updated: ...')
```

By two columns:

```python
db.update_table_values_where_and_match(
    "message",
    by_data=("drag_screen", "WORK"),
    change_data_where=("user_name", "xavier"),
    match_with=("drag_screen", "HOME"),
)
# (True, 'Table [message] updated: ...')
```

Several columns at once:

```python
db.update_multiple_params_where_and_match(
    "stock",
    by_data={"nombre": "Cilindro 5kg premium", "cantidad": 25},
    change_data_where=[("sku", "P-1")],
)
# (True, 'Table [stock] updated multiple columns')

db.update_table_values_where_many(
    "stock",
    by_data={"nombre": "Cilindro 5kg premium", "cantidad": 25},
    filters=[("sku", "P-1")],
)
# (True, 'Table [stock] updated multiple columns')
```

With operators:

```python
db.update_where_advanced(
    "stock",
    by_data={"cantidad": 0},
    filters=[("cantidad", "<", 5)],
)
# (True, 'Table [stock] updated where "cantidad" < ?')
```

Whole column:

```python
db.set_column_all_rows("stock", "cantidad", 0)
# (True, 'Column [cantidad] in table [stock] set to 0 in all rows')

db.set_column_all_rows("stock", "nombre", "sin nombre")
# (True, 'Column [nombre] in table [stock] set to sin nombre in all rows')
```

By rowid:

```python
db.update_table_values_where_many(
    "stock",
    by_data={"cantidad": 99},
    filters=[("id", 1)],
)
# (True, 'Table [stock] updated multiple columns')
```

---

## Delete

By one column:

```python
db.remove_table_values_where("message", ("user_name", "javier"))
# (True, 'Deleted from table [message] where user_name=javier')
```

By two columns:

```python
db.remove_table_values_where_and_match(
    "message",
    remove_data_where=("user_name", "javier"),
    match_with=("drag_screen", "HOME"),
)
# (True, 'Deleted from table [message] where ...')
```

By N columns:

```python
db.remove_table_values_where_many(
    "stock",
    filters=[("sku", "P-1"), ("cantidad", 0)],
)
# (True, "Deleted from table [stock] where [('sku', 'P-1'), ('cantidad', 0)]")
```

With operators:

```python
db.remove_where_advanced("stock", filters=[("cantidad", "<", 5)])
# (True, 'Deleted from table [stock] where "cantidad" < ?')
```

By id:

```python
db.delete_values_in_table_where("stock", 1)
# (True, 'Deleted from table [stock] where id=1')
```

Empty a table:

```python
db.delete_all_values_in_table("stock")
# (True, 'Table [stock] emptied')

db.remove_all_data_from_table("stock")
# (True, 'Table [stock] emptied')
```

Drop a table:

```python
db.remove_table("message")
# (True, 'Table [message] dropped')
```

---

## Check

```python
db.check_table_name("stock")
# (True, 'Table exists: [stock]')

db.check_table_name("no_existe")
# (False, 'Table does not exist: [no_existe]')

db.check_table_name_structure("stock")
# (True, [(0, 'id', 'INTEGER', 0, None, 1), (1, 'sku', 'TEXT', 1, None, 0), ...])

db.check_column("stock", "sku")
# (True, 'Column exists: table [stock] column [sku]')

db.check_column("stock", "no_existe")
# (False, 'Column does not exist: table [stock] column [no_existe]')

db.check_column_and_value_name("stock", ("sku", "P-1"))
# (True, 'Value exists: column [sku] value [P-1]')

db.check_column_and_value_name("stock", ("sku", "no_existe"))
# (False, 'Value does not exist: column [sku] value [no_existe]')
```

---

## Rename

```python
db.rename_table("message", "whatsapp")
# (True, 'Table [message] renamed to [whatsapp]')

db.rename_value_in_table("whatsapp", "user_name", "username")
# (True, 'Column [user_name] renamed to [username]')
```

---

## "Read or create" pattern

```python
ok, row = db.read_table_values_where("users", ("email", email), fetch_one=True)
if not ok:
    return {"error": row}
if row is None:
    ok, new_id = db.insert_dict("users", {"email": email, "name": name})
    if not ok:
        return {"error": new_id}
    ok, row = db.read_by_id("users", new_id)
return row
```

---

## Natural key pattern

```python
KEY = [("date", "2024-08-26"), ("type", "bodega"), ("name", "local")]

# Read
ok, row = db.read_table_values_where_many("register", filters=KEY, fetch_one=True)

# Update
db.update_table_values_where_many("register", by_data={"llenos_5kg": 10}, filters=KEY)

# Delete
db.remove_table_values_where_many("register", filters=KEY)
```

---

## Specific-columns pattern

```python
# A table with 47 columns, only need three
ok, rows = db.read_columns_where(
    "register",
    columns=["type", "name", "llenos_5kg"],
    filters=[("date", "2024-08-26")],
)
# (True, [('bodega', 'local', 5), ('camion', 'pepe', 30)])

# With order and limit
ok, rows = db.read_columns_where(
    "register",
    columns=["name", "llenos_5kg"],
    filters=[("date", "2024-08-26")],
    order_by="llenos_5kg DESC",
    limit=10,
)
# (True, [('pepe', 30), ('local', 5)])
```

---

## Pagination pattern

```python
def list_page(page: int, size: int = 50):
    ok, rows = db.read_all_table_and_columns(
        "records",
        limit=size,
        offset=page * size,
        order_by="id DESC",
    )
    return rows if ok else []

list_page(0)   # first page
list_page(1)   # second page
```

---

## Upsert pattern

```python
ok, existing = db.read_table_values_where("stock", ("sku", sku), fetch_one=True)
if existing:
    db.update_table_values_where_many(
        "stock", by_data={"cantidad": new_quantity}, filters=[("sku", sku)]
    )
else:
    db.insert_dict("stock", {"sku": sku, "cantidad": new_quantity})
```

---

## Dump-to-JSON pattern

```python
ok, rows = db.read_all_table_and_columns("stock", limit=1000)
columns = [r[1] for r in db.check_table_name_structure("stock")[1]]
stock = [dict(zip(columns, row)) for row in rows]
# [{'id': 1, 'sku': 'P-1', 'nombre': 'Cilindro 5kg', 'cantidad': 10, ...}, ...]
```

---

## Error handling

```python
db.create_table("", {})
# (False, 'column_data cannot be empty')

db.create_table("stock", {})
# (False, 'column_data cannot be empty')

db.create_table("bad name!", {"a": "TEXT"})
# (False, "Unsafe SQLite identifier: 'bad name!'")

db.read_all_table_and_columns("no_existe")
# (False, 'Table does not exist: [no_existe]')

db.read_table_values_where("stock", ("no_existe", "P-1"))
# (False, 'Column does not exist: table [stock] column [no_existe]')

db.insert_dict("stock", {"sku_que_no_existe": "P-1"})
# (False, 'Table [stock] has no column(s) [...]; create them first, its columns are [...]')

db.read_where_advanced("stock", filters=[("sku", "SUPERLIKE", "P-1")])
# (False, "Unsupported operator: 'SUPERLIKE'")

db.read_where_advanced("stock", filters=[("sku", "IN", [])])
# (False, "IN expects a non-empty list or tuple, got: []")

db.read_where_advanced("stock", filters=[("cantidad", "BETWEEN", 5)])
# (False, "BETWEEN expects a tuple (min, max), got: 5")

db.read_columns_where("stock", columns=["no_existe"], filters=[("sku", "P-1")])
# (False, 'Column does not exist: table [stock] column [no_existe]')

db.read_columns_where("stock", columns=[], filters=[("sku", "P-1")])
# (False, 'columns must be a non-empty list')

db.read_columns_where("stock", columns=["sku"], filters=[])
# (False, 'filters must be a non-empty list of (column, value) tuples')
```

---

## SQL injection

```python
db.create_table("notes", {"text": "TEXT"})
# (True, 'Table created: [notes]')

db.create_values_in_table_where("notes", ("it's mine; DROP TABLE notes;",))
# (True, 'Row inserted in table [notes]: ...')

db.read_all_table_and_columns("notes")
# (True, [("it's mine; DROP TABLE notes;",)])
```

---

## Concurrency

```python
import threading

db = module_sqlite(path_db="app.db")
db.create_table("hits", {"payload": "TEXT"})

def worker(n):
    for i in range(100):
        db.create_values_in_table_where("hits", (f"{n}-{i}",))

threads = [threading.Thread(target=worker, args=(n,)) for n in range(20)]
for t in threads: t.start()
for t in threads: t.join()

ok, rows = db.read_all_table_and_columns("hits")
# (True, [...]) with 2000 rows, zero errors
```

---

## `:memory:` does not work here

Writes always go through the single `_WriteQueue` writer connection, but
every read opens a brand-new connection (`sqlite3.connect(path_db)`). With
a real file, both see the same file on disk. With `":memory:"`, every
connection is its own separate, empty in-RAM database — so a write is
never visible to a later read:

```python
db = module_sqlite(path_db=":memory:")
db.create_table("t", {"a": "TEXT"})
db.insert_dict("t", {"a": "hello"})          # (True, 1) — goes through the writer
db.read_all_table_and_columns("t")           # (False, 'Table does not exist: [t]')
```

Always use a real file path (or `tempfile` for throwaway databases).

---

## Reference table

| Method | What it does |
|---|---|
| `create_table` | Create a table |
| `create_new_column_in_table` | Add a column |
| `create_values_in_table_where` | Insert by position |
| `insert_dict` | Insert by column name |
| `rename_table` | Rename a table |
| `rename_value_in_table` | Rename a column |
| `set_column_all_rows` | Set a value/NULL for the whole column |
| `check_table_name` | Does the table exist? |
| `check_table_name_structure` | Table structure |
| `check_column` | Does the column exist? |
| `check_column_and_value_name` | Is there a row with that value? |
| `read_all_table_and_columns` | Read everything |
| `read_table_values_where` | Read with 1 condition |
| `read_table_values_where_and_match` | Read with 2 conditions |
| `read_table_values_where_many` | Read with N conditions |
| `read_columns_where` | Read specific columns with N conditions |
| `read_where_advanced` | Read with operators |
| `read_by_id` | Read by rowid |
| `read_all_table_and_columns_where` | Read by rowid (alias) |
| `read_only_last_column_nums` | Last N rows |
| `read_tables_from_database` | List tables |
| `update_table_values_where` | Update with 1 condition |
| `update_table_values_where_and_match` | Update with 2 conditions |
| `update_multiple_params_where_and_match` | Update several columns |
| `update_table_values_where_many` | Update with N conditions |
| `update_where_advanced` | Update with operators |
| `remove_table` | DROP TABLE |
| `remove_all_data_from_table` | Empty a table |
| `remove_table_values_where` | Delete with 1 condition |
| `remove_table_values_where_and_match` | Delete with 2 conditions |
| `remove_table_values_where_many` | Delete with N conditions |
| `remove_where_advanced` | Delete with operators |
| `delete_values_in_table_where` | Delete by id (alias) |
| `delete_all_values_in_table` | Empty a table (alias) |