from prettytable import PrettyTable
from src.primitive_db.utils import load_table_data, save_table_data  

ALLOWED_TYPES = {"int", "str", "bool"}


def _convert_value(value_str, target_type):
    try:
        if target_type == "int":
            return int(value_str)
        elif target_type == "bool":
            return value_str.lower() in ("true", "1", "yes")
        elif target_type == "str":
            return value_str
    except ValueError:
        raise ValueError(f"Невозможно преобразовать '{value_str}' в тип {target_type}")


def create_table(metadata, table_name, columns):
    if table_name in metadata:
        return False, f'Ошибка: Таблица "{table_name}" уже существует.'
    
    validated = [("ID", "int")]
    for col in columns:
        if ":" not in col:
            return False, f'Некорректное значение: {col}. Ожидается формат "имя:тип".'
        name, dtype = col.split(":", 1)
        if dtype not in ALLOWED_TYPES:
            return False, f'Некорректный тип данных: {dtype}. Допустимые: {", ".join(ALLOWED_TYPES)}'
        validated.append((name, dtype))
    
    metadata[table_name] = validated
    cols_str = ", ".join(f"{n}:{t}" for n, t in validated)
    return True, f'Таблица "{table_name}" успешно создана со столбцами: {cols_str}'


def drop_table(metadata, table_name):
    if table_name not in metadata:
        return False, f'Ошибка: Таблица "{table_name}" не существует.'
    del metadata[table_name]
    return True, f'Таблица "{table_name}" успешно удалена.'


def list_tables(metadata):
    if not metadata:
        return "Нет созданных таблиц."
    return "\n".join(f"- {name}" for name in sorted(metadata.keys()))


def info_table(metadata, table_name):
    if table_name not in metadata:
        return f'Ошибка: Таблица "{table_name}" не существует.'
    columns = metadata[table_name]
    cols_str = ", ".join(f"{n}:{t}" for n, t in columns)
    return f"Таблица: {table_name}\nСтолбцы: {cols_str}"


def insert(metadata, table_name, values):
    if table_name not in metadata:
        return False, f'Ошибка: Таблица "{table_name}" не существует.'
    
    columns = metadata[table_name]
    expected_count = len(columns) - 1  
    
    if len(values) != expected_count:
        return False, f'Ошибка: Ожидается {expected_count} значения(й), получено {len(values)}.'
    
    # Загружаем существующие данные
    table_data = load_table_data(table_name)
    
    # Валидация типов
    record = {"ID": 1}
    try:
        for (col_name, col_type), val in zip(columns[1:], values):
            record[col_name] = _convert_value(val, col_type)
    except ValueError as e:
        return False, f"Ошибка валидации: {e}"
    
    # Генерация ID
    if table_data:
        record["ID"] = max(r["ID"] for r in table_data) + 1
    
    table_data.append(record)
    save_table_data(table_name, table_data)
    return True, f'Запись с ID={record["ID"]} успешно добавлена в таблицу "{table_name}".'


def _match_condition(record, column, value, metadata, table_name):
    if column not in record:
        return False
    expected_type = next((t for n, t in metadata[table_name] if n == column), None)
    if expected_type:
        try:
            value = _convert_value(value, expected_type)
        except:
            return False
    return record[column] == value


def select(metadata, table_name, where_clause=None):
    if table_name not in metadata:
        return False, f'Ошибка: Таблица "{table_name}" не существует.'
    
    # Загружаем данные таблицы
    table_data = load_table_data(table_name)
    columns = [col[0] for col in metadata[table_name]]
    
    if where_clause:
        column, value = where_clause
        filtered = [r for r in table_data if _match_condition(r, column, value, metadata, table_name)]
    else:
        filtered = table_data
    
    if not filtered:
        return True, "Нет записей."
    
    table = PrettyTable()
    table.field_names = columns
    for row in filtered:
        table.add_row([row.get(col, "") for col in columns])
    return True, str(table)


def update(metadata, table_name, set_clause, where_clause):
    if table_name not in metadata:
        return False, f'Ошибка: Таблица "{table_name}" не существует.'
    
    # Загружаем данные
    table_data = load_table_data(table_name)
    column_set, value_set = set_clause
    column_where, value_where = where_clause
    
    updated = []
    for record in table_data:
        if _match_condition(record, column_where, value_where, metadata, table_name):
            expected_type = next((t for n, t in metadata[table_name] if n == column_set), None)
            if expected_type:
                try:
                    record[column_set] = _convert_value(value_set, expected_type)
                    updated.append(record["ID"])
                except ValueError as e:
                    return False, f"Ошибка валидации: {e}"
    
    if not updated:
        return False, "Не найдено записей для обновления."
    
    save_table_data(table_name, table_data)
    return True, f"Записи с ID={', '.join(map(str, updated))} в таблице \"{table_name}\" успешно обновлены."


def delete(metadata, table_name, where_clause):
    if table_name not in metadata:
        return False, f'Ошибка: Таблица "{table_name}" не существует.'
    
    # Загружаем данные
    table_data = load_table_data(table_name)
    column, value = where_clause
    
    before_count = len(table_data)
    table_data = [r for r in table_data if not _match_condition(r, column, value, metadata, table_name)]
    after_count = len(table_data)
    
    if before_count == after_count:
        return False, "Не найдено записей для удаления."
    
    deleted_count = before_count - after_count
    save_table_data(table_name, table_data)
    return True, f"{deleted_count} запись(ей) успешно удалена(ы) из таблицы \"{table_name}\"."