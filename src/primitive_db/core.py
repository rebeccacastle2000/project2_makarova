from prettytable import PrettyTable
from src.primitive_db.utils import load_table_data, save_table_data
from src.primitive_db.decorators import handle_db_errors, confirm_action, log_time
from src.primitive_db.cache import create_cacher
from src.primitive_db.constants import ALLOWED_TYPES

query_cache = create_cacher()


def _convert_value(value_str, target_type):
    try:
        if target_type == "int":
            return int(value_str)
        if target_type == "bool":
            return value_str.lower() in ("true", "1", "yes")
        if target_type == "str":
            return value_str
    except ValueError:
        raise ValueError(f"Невозможно преобразовать '{value_str}' в тип {target_type}")


@handle_db_errors
def create_table(metadata, table_name, columns):
    if table_name in metadata:
        return False, f'Ошибка: Таблица "{table_name}" уже существует.'

    validated = [("ID", "int")]
    for col in columns:
        if ":" not in col:
            raise ValueError(
                f'Некорректное значение: {col}. Ожидается формат "имя:тип".'
            )
        name, dtype = col.split(":", 1)
        if dtype not in ALLOWED_TYPES:
            raise ValueError(
                f'Некорректный тип данных: {dtype}. '
                f'Допустимые: {", ".join(ALLOWED_TYPES)}'
            )
        validated.append((name, dtype))

    metadata[table_name] = validated
    cols_str = ", ".join(f"{n}:{t}" for n, t in validated)
    return True, f'Таблица "{table_name}" успешно создана со столбцами: {cols_str}'


@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata, table_name):
    if table_name not in metadata:
        raise KeyError(table_name)
    del metadata[table_name]
    return True, f'Таблица "{table_name}" успешно удалена.'


@handle_db_errors
def list_tables(metadata):
    if not metadata:
        return True, "Нет созданных таблиц."
    return True, "\n".join(f"- {name}" for name in sorted(metadata.keys()))


@handle_db_errors
def info_table(metadata, table_name):
    if table_name not in metadata:
        raise KeyError(table_name)
    columns = metadata[table_name]
    cols_str = ", ".join(f"{n}:{t}" for n, t in columns)
    return True, f"Таблица: {table_name}\nСтолбцы: {cols_str}"


@handle_db_errors
@log_time
def insert(metadata, table_name, values):
    if table_name not in metadata:
        raise KeyError(table_name)

    columns = metadata[table_name]
    expected_count = len(columns) - 1

    if len(values) != expected_count:
        raise ValueError(
            f'Ожидается {expected_count} значения(й), получено {len(values)}.'
        )

    table_data = load_table_data(table_name)
    record = {"ID": 1}

    for (col_name, col_type), val in zip(columns[1:], values):
        record[col_name] = _convert_value(val, col_type)

    if table_data:
        record["ID"] = max(r["ID"] for r in table_data) + 1

    table_data.append(record)
    save_table_data(table_name, table_data)
    return True, (
        f'Запись с ID={record["ID"]} успешно добавлена '
        f'в таблицу "{table_name}".'
    )


def _match_condition(record, column, value, metadata, table_name):
    if column not in record:
        return False
    expected_type = next(
        (t for n, t in metadata[table_name] if n == column), None
    )
    if expected_type:
        try:
            value = _convert_value(value, expected_type)
        except Exception:
            return False
    return record[column] == value


@handle_db_errors
@log_time
def select(metadata, table_name, where_clause=None):
    if table_name not in metadata:
        raise KeyError(table_name)

    cache_key = f"{table_name}:{str(where_clause)}"

    def fetch_data():
        table_data = load_table_data(table_name)
        columns = [col[0] for col in metadata[table_name]]

        if where_clause:
            column, value = where_clause
            filtered = [
                r for r in table_data
                if _match_condition(r, column, value, metadata, table_name)
            ]
        else:
            filtered = table_data

        if not filtered:
            return True, "Нет записей."

        table = PrettyTable()
        table.field_names = columns
        for row in filtered:
            table.add_row([row.get(col, "") for col in columns])
        return True, str(table)

    return query_cache(cache_key, fetch_data)


@handle_db_errors
@log_time
def update(metadata, table_name, set_clause, where_clause):
    if table_name not in metadata:
        raise KeyError(table_name)

    table_data = load_table_data(table_name)
    column_set, value_set = set_clause
    column_where, value_where = where_clause

    updated = []
    for record in table_data:
        if _match_condition(
            record, column_where, value_where, metadata, table_name
        ):
            expected_type = next(
                (t for n, t in metadata[table_name] if n == column_set), None
            )
            if expected_type:
                record[column_set] = _convert_value(value_set, expected_type)
                updated.append(record["ID"])

    if not updated:
        raise ValueError("Не найдено записей для обновления.")

    save_table_data(table_name, table_data)
    return True, (
        f"Записи с ID={', '.join(map(str, updated))} в таблице "
        f'"{table_name}" успешно обновлены.'
    )


@handle_db_errors
@confirm_action("удаление записей")
def delete(metadata, table_name, where_clause):
    if table_name not in metadata:
        raise KeyError(table_name)

    table_data = load_table_data(table_name)
    column, value = where_clause

    before_count = len(table_data)
    table_data = [
        r for r in table_data
        if not _match_condition(r, column, value, metadata, table_name)
    ]
    after_count = len(table_data)

    if before_count == after_count:
        raise ValueError("Не найдено записей для удаления.")

    deleted_count = before_count - after_count
    save_table_data(table_name, table_data)
    return True, (
        f"{deleted_count} запись(ей) успешно удалена(ы) "
        f'из таблицы "{table_name}".'
    )
