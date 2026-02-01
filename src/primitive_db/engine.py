import shlex
import re
from src.primitive_db.core import (
    create_table, drop_table, list_tables, info_table,
    insert, select, update, delete
)
from src.primitive_db.utils import load_metadata, save_metadata


def print_help():
    """Выводит справочную информацию о доступных командах."""
    print("\n***Операции с данными***")
    print("Функции:")
    print(
        "<command> insert into <имя_таблицы> values (<значение1>, "
        "<значение2>, ...) - создать запись"
    )
    print(
        "<command> select from <имя_таблицы> where <столбец> = <значение> "
        "- прочитать записи по условию"
    )
    print(
        "<command> select from <имя_таблицы> - прочитать все записи"
    )
    print(
        "<command> update <имя_таблицы> set <столбец> = <значение> "
        "where <столбец> = <значение> - обновить запись"
    )
    print(
        "<command> delete from <имя_таблицы> where <столбец> = <значение> "
        "- удалить запись"
    )
    print(
        "<command> info <имя_таблицы> - вывести информацию о таблице"
    )
    print(
        "<command> create_table <имя_таблицы> <столбец1:тип> ... "
        "- создать таблицу"
    )
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация")


def _parse_values(values_str):
    """Парсит строку значений вида ("Sergei", 28, true) в список."""
    values_str = values_str.strip("() ")
    parts = []
    current = ""
    in_quotes = False

    for char in values_str:
        if char == '"':
            in_quotes = not in_quotes
        elif char == "," and not in_quotes:
            parts.append(current.strip())
            current = ""
        else:
            current += char
    if current:
        parts.append(current.strip())

    return [p.strip('"').strip("'") for p in parts]


def _parse_condition(cond_str):
    """Парсит условие вида 'age = 28' в кортеж (столбец, значение)."""
    match = re.match(r'^(\w+)\s*=\s*(.+)$', cond_str.strip())
    if not match:
        return None, None
    col, val = match.groups()
    return col.strip(), val.strip('"').strip("'")


def run():
    """Основной цикл работы с базой данных."""
    print("\n***База данных***")
    print_help()

    while True:
        try:
            user_input = input("\n>>> Введите команду: ").strip()
            if not user_input:
                continue

            metadata = load_metadata()
            parts = shlex.split(user_input)
            cmd = parts[0].lower() if parts else ""

            if cmd == "exit":
                break
            if cmd == "help":
                print_help()
            elif cmd == "list_tables":
                success, msg = list_tables(metadata)
                print(msg)
            elif cmd == "create_table":
                if len(parts) < 3:
                    print(
                        'Некорректное значение: недостаточно аргументов. '
                        'Пример: create_table users name:str age:int'
                    )
                    continue
                success, msg = create_table(metadata, parts[1], parts[2:])
                print(msg)
                if success:
                    save_metadata(metadata)
            elif cmd == "drop_table":
                if len(parts) != 2:
                    print(
                        'Некорректное значение: требуется имя таблицы. '
                        'Пример: drop_table users'
                    )
                    continue
                success, msg = drop_table(metadata, parts[1])
                print(msg)
                if success:
                    save_metadata(metadata)
            elif cmd == "info":
                if len(parts) != 2:
                    print(
                        'Некорректное значение: требуется имя таблицы. '
                        'Пример: info users'
                    )
                    continue
                success, msg = info_table(metadata, parts[1])
                print(msg)
            elif (
                cmd == "insert" and len(parts) >= 4
                and parts[1].lower() == "into"
                and parts[3].lower() == "values"
            ):
                table_name = parts[2]
                values_str = user_input.split("values", 1)[1].strip()
                values = _parse_values(values_str)
                success, msg = insert(metadata, table_name, values)
                print(msg)
            elif cmd == "select" and len(parts) >= 3 and parts[1].lower() == "from":
                table_name = parts[2]
                where_clause = None
                if "where" in user_input.lower():
                    where_part = user_input.lower().split("where", 1)[1].strip()
                    col, val = _parse_condition(where_part)
                    if col and val:
                        where_clause = (col, val)
                success, msg = select(metadata, table_name, where_clause)
                print(msg)
            elif (
                cmd == "update" and len(parts) >= 6
                and "set" in parts and "where" in parts
            ):
                table_name = parts[1]
                set_idx = parts.index("set")
                where_idx = parts.index("where")
                set_clause = (parts[set_idx + 1], parts[set_idx + 3])
                where_clause = (parts[where_idx + 1], parts[where_idx + 3])
                success, msg = update(metadata, table_name, set_clause, where_clause)
                print(msg)
            elif (
                cmd == "delete" and len(parts) >= 4
                and parts[1].lower() == "from" and "where" in parts
            ):
                table_name = parts[2]
                where_part = user_input.lower().split("where", 1)[1].strip()
                col, val = _parse_condition(where_part)
                if col and val:
                    success, msg = delete(metadata, table_name, (col, val))
                    print(msg)
                else:
                    print("Некорректное условие WHERE")
            else:
                print(f'Функции "{cmd}" нет. Попробуйте снова.')
        except KeyboardInterrupt:
            print("\nВыход...")
            break
        except EOFError:
            print("\nВыход...")
            break
