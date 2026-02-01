import shlex
from src.primitive_db.core import create_table, drop_table, list_tables
from src.primitive_db.utils import load_metadata, save_metadata


def print_help():
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация")


def run():
    print("\n***База данных***")
    print_help()
    
    while True:
        try:
            user_input = input("\n>>>Введите команду: ").strip()
            if not user_input:
                continue
            
            parts = shlex.split(user_input)
            cmd = parts[0] if parts else ""
            args = parts[1:]
            
            metadata = load_metadata()
            
            if cmd == "exit":
                break
            elif cmd == "help":
                print_help()
            elif cmd == "list_tables":
                print(list_tables(metadata))
            elif cmd == "create_table":
                if len(args) < 2:
                    print('Некорректное значение: недостаточно аргументов. Пример: create_table users name:str age:int')
                    continue
                table_name = args[0]
                columns = args[1:]
                success, msg = create_table(metadata, table_name, columns)
                print(msg)
                if success:
                    save_metadata(metadata)
            elif cmd == "drop_table":
                if len(args) != 1:
                    print('Некорректное значение: требуется имя таблицы. Пример: drop_table users')
                    continue
                success, msg = drop_table(metadata, args[0])
                print(msg)
                if success:
                    save_metadata(metadata)
            else:
                print(f'Функции "{cmd}" нет. Попробуйте снова.')
        except KeyboardInterrupt:
            print("\nВыход...")
            break
        except Exception as e:
            print(f"Ошибка: {e}")