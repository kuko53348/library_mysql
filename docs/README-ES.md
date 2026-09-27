# library_sqlite — ejemplos

Cada método con un caso real. Los que no tienen ejemplo, no existen.

*[Versión en Ingles](../README.md)*

---

## Crear base y tabla

```
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

## Insertar

Por posición:

```
db.create_values_in_table_where("message", ("javier", "HOME", "2026-09-22"))
# (True, 'Row inserted in table [message]: (...)')
```

Por nombre de columna (recomendado):

```
db.insert_dict("message", {"user_name": "javier", "drag_screen": "HOME"})
# (True, 1)
```

Con defaults:

```
db.insert_dict(
    "stock",
    data={"sku": "P-1", "nombre": "Cilindro 5kg"},
    defaults={"cantidad": 0},
)
# (True, 1)
```

Con id explícito:

```
db.insert_dict("stock", {"id": 50, "sku": "P-2", "nombre": "Cilindro 11kg"})
# (True, 50)
```

---

## Leer

Toda la tabla:

```
db.read_all_table_and_columns("message")
# (True, [('javier', 'HOME', '2026-09-22')])
```

Con límite y orden:

```
db.read_all_table_and_columns("message", limit=10, offset=0, order_by="user_name ASC")
# (True, [('javier', 'HOME', '2026-09-22')])
```

Primera fila:

```
db.read_all_table_and_columns("message", fetch_one=True)
# (True, ('javier', 'HOME', '2026-09-22'))
```

Por una columna:

```
db.read_table_values_where("message", ("user_name", "javier"))
# (True, [('javier', 'HOME', '2026-09-22')])
```

Con `fetch_one`:

```
db.read_table_values_where("message", ("user_name", "javier"), fetch_one=True)
# (True, ('javier', 'HOME', '2026-09-22'))
```

Sin coincidencia:

```
db.read_table_values_where("message", ("user_name", "nadie"), fetch_one=True)
# (True, None)
```

Por dos columnas:

```
db.read_table_values_where_and_match(
    "message",
    read_data_where=("user_name", "javier"),
    match_with=("drag_screen", "HOME"),
)
# (True, [('javier', 'HOME', '2026-09-22')])
```

Por N columnas:

```
db.read_table_values_where_many(
    "stock",
    filters=[("sku", "P-1"), ("nombre", "Cilindro 5kg"), ("cantidad", 0)],
    fetch_one=True,
)
# (True, (1, 'P-1', 'Cilindro 5kg', 0, '2026-09-22 10:00:00'))
```

Solo algunas columnas:

```
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
# (True, [('Cilindro 5kg', 0), ('Cilindro 11kg', 0)])

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

Con operadores:

```
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

Por rowid:

```
db.insert_dict("stock", {"sku": "P-3", "nombre": "Cilindro 15kg"})
# (True, 2)

db.read_by_id("stock", 2)
# (True, (2, 2, 'P-3', 'Cilindro 15kg', 0, '2026-09-22 10:00:00'))

db.read_all_table_and_columns_where("stock", 2)
# (True, (2, 2, 'P-3', 'Cilindro 15kg', 0, '2026-09-22 10:00:00'))

db.read_by_id("stock", 9999)
# (False, 'No row found with id 9999 in table [stock]')
```

Últimas N filas:

```
db.read_only_last_column_nums("stock", show_last_num_of_values=3)
# (True, [(2, 2, 'P-3', ...), (1, 1, 'P-1', ...)])
```

Listar tablas:

```
db.read_tables_from_database()
# (True, ['message', 'stock'])
```

---

## Actualizar

Por una columna:

```
db.update_table_values_where(
    "message",
    by_data=("user_name", "xavier"),
    change_data_where=("user_name", "javier"),
)
# (True, 'Table [message] updated: ...')
```

Por dos columnas:

```
db.update_table_values_where_and_match(
    "message",
    by_data=("drag_screen", "WORK"),
    change_data_where=("user_name", "xavier"),
    match_with=("drag_screen", "HOME"),
)
# (True, 'Table [message] updated: ...')
```

Varias columnas a la vez:

```
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

Con operadores:

```
db.update_where_advanced(
    "stock",
    by_data={"cantidad": 0},
    filters=[("cantidad", "<", 5)],
)
# (True, 'Table [stock] updated where "cantidad" < ?')
```

Toda la columna:

```
db.set_column_all_rows("stock", "cantidad", 0)
# (True, 'Column [cantidad] in table [stock] set to 0 in all rows')

db.set_column_all_rows("stock", "nombre", "sin nombre")
# (True, 'Column [nombre] in table [stock] set to sin nombre in all rows')
```

Por rowid:

```
db.update_table_values_where_many(
    "stock",
    by_data={"cantidad": 99},
    filters=[("id", 1)],
)
# (True, 'Table [stock] updated multiple columns')
```

---

## Borrar

Por una columna:

```
db.remove_table_values_where("message", ("user_name", "javier"))
# (True, 'Deleted from table [message] where user_name=javier')
```

Por dos columnas:

```
db.remove_table_values_where_and_match(
    "message",
    remove_data_where=("user_name", "javier"),
    match_with=("drag_screen", "HOME"),
)
# (True, 'Deleted from table [message] where ...')
```

Por N columnas:

```
db.remove_table_values_where_many(
    "stock",
    filters=[("sku", "P-1"), ("cantidad", 0)],
)
# (True, "Deleted from table [stock] where [('sku', 'P-1'), ('cantidad', 0)]")
```

Con operadores:

```
db.remove_where_advanced("stock", filters=[("cantidad", "<", 5)])
# (True, 'Deleted from table [stock] where "cantidad" < ?')
```

Por id:

```
db.delete_values_in_table_where("stock", 1)
# (True, 'Deleted from table [stock] where id=1')
```

Vaciar tabla:

```
db.delete_all_values_in_table("stock")
# (True, 'Table [stock] emptied')

db.remove_all_data_from_table("stock")
# (True, 'Table [stock] emptied')
```

Eliminar tabla:

```
db.remove_table("message")
# (True, 'Table [message] dropped')
```

---

## Comprobar

```
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

## Renombrar

```
db.rename_table("message", "whatsapp")
# (True, 'Table [message] renamed to [whatsapp]')

db.rename_value_in_table("whatsapp", "user_name", "username")
# (True, 'Column [user_name] renamed to [username]')
```

---

## Patrón "leer o crear"

```
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

## Patrón clave natural

```
KEY = [("date", "2024-08-26"), ("type", "bodega"), ("name", "local")]

# Leer
ok, row = db.read_table_values_where_many("register", filters=KEY, fetch_one=True)

# Actualizar
db.update_table_values_where_many("register", by_data={"llenos_5kg": 10}, filters=KEY)

# Borrar
db.remove_table_values_where_many("register", filters=KEY)
```

---

## Patrón columnas específicas

```
# Una tabla con 47 columnas, solo necesito tres
ok, rows = db.read_columns_where(
    "register",
    columns=["type", "name", "llenos_5kg"],
    filters=[("date", "2024-08-26")],
)
# (True, [('bodega', 'local', 5), ('camion', 'pepe', 30)])

# Con orden y límite
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

## Patrón paginación

```
def list_page(page: int, size: int = 50):
    ok, rows = db.read_all_table_and_columns(
        "records",
        limit=size,
        offset=page * size,
        order_by="id DESC",
    )
    return rows if ok else []

list_page(0)   # primera página
list_page(1)   # segunda página
```

---

## Patrón upsert

```
ok, existing = db.read_table_values_where("stock", ("sku", sku), fetch_one=True)
if existing:
    db.update_table_values_where_many(
        "stock", by_data={"cantidad": nueva_cantidad}, filters=[("sku", sku)]
    )
else:
    db.insert_dict("stock", {"sku": sku, "cantidad": nueva_cantidad})
```

---

## Patrón dump a JSON

```
ok, rows = db.read_all_table_and_columns("stock", limit=1000)
columns = [r[1] for r in db.check_table_name_structure("stock")[1]]
stock = [dict(zip(columns, row)) for row in rows]
# [{'id': 1, 'sku': 'P-1', 'nombre': 'Cilindro 5kg', 'cantidad': 10, ...}, ...]
```

---

## Manejo de errores

```
db.create_table("", {})
# (False, 'Unsafe SQLite identifier: ...')

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

## Inyección SQL

```
db.create_table("notes", {"text": "TEXT"})
# (True, 'Table created: [notes]')

db.create_values_in_table_where("notes", ("it's mine; DROP TABLE notes;",))
# (True, 'Row inserted in table [notes]: ...')

db.read_all_table_and_columns("notes")
# (True, [("it's mine; DROP TABLE notes;",)])
```

---

## Concurrencia

```
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
# (True, [...]) con 2000 filas, cero errores
```

---

## Tabla de referencia

| Método | Qué hace |
|---|---|
| `create_table` | Crear tabla |
| `create_new_column_in_table` | Añadir columna |
| `create_values_in_table_where` | Insertar por posición |
| `insert_dict` | Insertar por nombre |
| `rename_table` | Renombrar tabla |
| `rename_value_in_table` | Renombrar columna |
| `set_column_all_rows` | Poner valor/NULL en toda la columna |
| `check_table_name` | ¿Existe la tabla? |
| `check_table_name_structure` | Estructura de la tabla |
| `check_column` | ¿Existe la columna? |
| `check_column_and_value_name` | ¿Hay fila con ese valor? |
| `read_all_table_and_columns` | Leer todo |
| `read_table_values_where` | Leer con 1 condición |
| `read_table_values_where_and_match` | Leer con 2 condiciones |
| `read_table_values_where_many` | Leer con N condiciones |
| `read_columns_where` | Leer columnas específicas con N condiciones |
| `read_where_advanced` | Leer con operadores |
| `read_by_id` | Leer por rowid |
| `read_all_table_and_columns_where` | Leer por rowid (alias) |
| `read_only_last_column_nums` | Últimas N filas |
| `read_tables_from_database` | Listar tablas |
| `update_table_values_where` | Update con 1 condición |
| `update_table_values_where_and_match` | Update con 2 condiciones |
| `update_multiple_params_where_and_match` | Update de varias columnas |
| `update_table_values_where_many` | Update con N condiciones |
| `update_where_advanced` | Update con operadores |
| `remove_table` | DROP TABLE |
| `remove_all_data_from_table` | Vaciar tabla |
| `remove_table_values_where` | Delete con 1 condición |
| `remove_table_values_where_and_match` | Delete con 2 condiciones |
| `remove_table_values_where_many` | Delete con N condiciones |
| `remove_where_advanced` | Delete con operadores |
| `delete_values_in_table_where` | Delete por id (alias) |
| `delete_all_values_in_table` | Vaciar tabla (alias) |