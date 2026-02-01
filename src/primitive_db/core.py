ALLOWED_TYPES = {"int", "str", "bool"}


def create_table(metadata, table_name, columns):
    if table_name in metadata:
        return False, f'Ошибка: Таблица "{table_name}" уже существует.'
    
    # Валидация столбцов
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