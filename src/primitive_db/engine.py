def welcome():
    print("DB project is running!")
    print("<command> exit - выйти из программы")
    print("<command> help - справочная информация")

    while True:
        cmd = input("Введите команду: ").strip()
        if cmd == "exit":
            break
        elif cmd == "help":
            print("<command> exit - выйти из программы")
            print("<command> help - справочная информация")
