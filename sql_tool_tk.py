#!python3
# -*- coding: utf-8 -*-

from tkinter import *
from tkinter import ttk
import os, csv, time

# для тестирования GUI без модуля драйвера базы данных
try:
    import cx_Oracle as DB  # pyodbc для MS SQL Server
    INFO_TEXT = "Строка статуса"
except Exception as e:
    INFO_TEXT = str(e)
    DB = None

# Строка соединения с БД
DATABASE_URI = 'user/password@IP:port/db_name'  # 'DRIVER={SQL Server};SERVER=tcp:IP,port;DATABASE=db_name;UID=user;PWD=password' для MS SQL Server

# Переменные окружения (при необходимости)
os.environ["NLS_LANG"] = "RUSSIAN_CIS.CL8MSWIN1251"

# Путь для сохранения CSV
dirname = os.path.expanduser("~\\Desktop")  # ссылка на рабочий стол

# Имя файла CSV
filename = os.path.join(dirname, "export.csv")

# Разделитель строк в файле CSV
end_line = "\n"

# Разделитель полей в файле CSV
separator = ";"

# Кодировка текста
text_codec = "cp1251"


class DatabaseError(Exception):
    """Пользовательский класс исключения для базы данных!"""
    pass


class UseDatabase:
    """Класс диспетчера контекста для соединения с базой данных!"""

    def __init__(self, config: str) -> None:
        self.configuration = config

    def __enter__(self) -> "cursor":
        try:
            self.conn = DB.connect(self.configuration)  # соединение с базой данных
            self.cursor1 = self.conn.cursor()
            self.cursor2 = self.conn.cursor()
            return self.cursor1, self.cursor2  # возвращаем два курсора (для ситуации когда курсор исп-ся в самом запросе)
        except Exception as err:
            raise DatabaseError(err)

    def __exit__(self, exc_type, exc_value, exc_trace) -> None:
        self.conn.commit()
        self.cursor1.close()
        self.cursor2.close()
        self.conn.close()
        if exc_type:
            raise DatabaseError(exc_value)  # если ошибка в SQL-запросе


class SQLToolsGUI:
    def __init__(self, root):
        self.root = root
        root.title("SQL tools")
        root.minsize(width=600, height=375)
        root.geometry("920x575-10+10")

        self.dataset = None
        self.headers = None

        # create a menu
        self.popup_label = Menu(root, tearoff=0)
        self.popup_label.add_command(label="Копировать", command=self.copy_label)
        self.popup_tree = Menu(root, tearoff=0)
        self.popup_tree.add_command(label="Копировать", command=self.copy_tree)
        self.popup_text = Menu(root, tearoff=0)
        self.popup_text.add_command(label="Копировать", command=self.copy_text)
        self.popup_text.add_separator()
        self.popup_text.add_command(label="Вставить", command=self.paste_text)

        self.labelframe_style = ttk.Style()
        self.labelframe_style.configure("Gray.TLabelframe.Label", font="Consolas 10", foreground="#808080")
        self.tree_style = ttk.Style()
        self.tree_style.configure("Gray.Treeview", font="Consolas 12", foreground="#333333")
        self.btn_style = ttk.Style()
        self.btn_style.configure("Gray.TButton", font="Consolas 12", foreground="#333333")

        self.content = ttk.Frame(root, padding=(10, 10, 10, 0))
        self.pw = ttk.Panedwindow(self.content, orient=VERTICAL, height=50)
        self.sqlFrame = ttk.Labelframe(self.pw, text="SQL-запрос | :cr для курсора",
                                       style="Gray.TLabelframe", padding=(10, 10, 10, 0))
        self.sqlText = Text(self.sqlFrame, height=20, font="Consolas 12", foreground="#333333")
        self.sqlText.bind("<Control-Key>", self.selectText)
        self.sqlText.bind("<Button-3>", self.do_popup_text)
        self.sqlText["wrap"] = "none"
        self.sbX = Scrollbar(self.sqlFrame, orient=HORIZONTAL, command=self.sqlText.xview)
        self.sqlText.configure(xscrollcommand=self.sbX.set)
        self.sbY = Scrollbar(self.sqlFrame, orient=VERTICAL, command=self.sqlText.yview)
        self.sqlText.configure(yscrollcommand=self.sbY.set)
        self.sqlButton = ttk.Button(self.sqlFrame, text="Выполнить запрос", style="Gray.TButton", command=self.beginSQL)
        self.dataFrame = ttk.Labelframe(self.pw, text="Данные результата запроса", style="Gray.TLabelframe",
                                        padding=(10, 10, 10, 0))
        self.sqlResult = ttk.Treeview(self.dataFrame, height=25, style="Gray.Treeview")
        self.sqlResult.bind("<Control-Key>", self.selectTree)
        self.sqlResult.bind("<Button-3>", self.do_popup_tree)
        self.sbXR = Scrollbar(self.dataFrame, orient=HORIZONTAL, command=self.sqlResult.xview)
        self.sqlResult.configure(xscrollcommand=self.sbXR.set)
        self.sbYR = Scrollbar(self.dataFrame, orient=VERTICAL, command=self.sqlResult.yview)
        self.sqlResult.configure(yscrollcommand=self.sbYR.set)
        self.csvButton = ttk.Button(self.dataFrame, text="Экспортировать данные", style="Gray.TButton", command=self.beginCSV)
        self.footer = ttk.Label(self.content, text=INFO_TEXT, font="Consolas 10", justify="right", foreground="#808080")
        self.footer.bind("<Button-3>", self.do_popup_label)
        self.pw.add(self.sqlFrame, weight=1)
        self.pw.add(self.dataFrame, weight=1)

        self.content.grid(column=0, row=0, sticky=(N, S, E, W))
        self.pw.grid(column=0, row=0, sticky=(N, S, E, W))
        self.sqlText.grid(column=0, row=0, sticky=(N, S, E, W))
        self.sbX.grid(column=0, row=1, columnspan=2, sticky=(E, W))
        self.sbY.grid(column=1, row=0, sticky=(N, S))
        self.sqlButton.grid(column=0, row=2, columnspan=2, sticky=(E, W), pady=10)
        self.sqlResult.grid(column=0, row=0, sticky=(N, S, E, W))
        self.sbXR.grid(column=0, row=1, columnspan=2, sticky=(E, W))
        self.sbYR.grid(column=1, row=0, sticky=(N, S))
        self.csvButton.grid(column=0, row=2, columnspan=2, sticky=(E, W), pady=10)
        self.footer.grid(column=0, row=1, sticky=(E,), pady=5)

        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)
        self.content.columnconfigure(0, weight=1)
        self.content.rowconfigure(0, weight=1)
        self.sqlFrame.columnconfigure(0, weight=1)
        self.sqlFrame.rowconfigure(0, weight=1)
        self.dataFrame.columnconfigure(0, weight=1)
        self.dataFrame.rowconfigure(0, weight=1)

        self.csvButton["state"] = "disabled"
        self.sqlResult.heading("#0", text="№")
        self.sqlText.insert(1.0, "SELECT SYSDATE FROM DUAL")  # "SELECT CONVERT(VARCHAR, GETDATE(), 20) AS SYSDATE" для MS SQL Server
        self.sqlText.focus()
        
        if INFO_TEXT != "Строка статуса":  # копирование ошибки в буфер обмена
            self.root.clipboard_clear()
            self.root.clipboard_append(INFO_TEXT)
            self.root.update()

    def do_popup_label(self, event):
        # display the popup menu
        try:
            self.popup_label.tk_popup(event.x_root, event.y_root, 0)
        finally:
            # make sure to release the grab (Tk 8.0a1 only)
            self.popup_label.grab_release()

    def copy_label(self):
        self.root.clipboard_clear()
        self.root.clipboard_append(self.footer["text"])
        self.root.update()

    def do_popup_tree(self, event):
        # display the popup menu
        try:
            self.popup_tree.tk_popup(event.x_root, event.y_root, 0)
        finally:
            # make sure to release the grab (Tk 8.0a1 only)
            self.popup_tree.grab_release()

    def copy_tree(self):
        try:
            text_copy = [self.sqlResult.item(x)["values"] for x in self.sqlResult.selection()]
        except:
            text_copy = ""
        finally:
            self.root.clipboard_clear()
            self.root.clipboard_append(text_copy)
            self.root.update()

    def do_popup_text(self, event):
        # display the popup menu
        try:
            self.popup_text.tk_popup(event.x_root, event.y_root, 0)
        finally:
            # make sure to release the grab (Tk 8.0a1 only)
            self.popup_text.grab_release()

    def copy_text(self):
        try:
            text_copy = self.sqlText.selection_get()
        except:
            text_copy = ""
        finally:
            self.root.clipboard_clear()
            self.root.clipboard_append(text_copy)
            self.root.update()

    def paste_text(self):
        self.sqlText.insert("insert", self.root.clipboard_get())

    def selectText(self, event):  # для русской раскладки (для английской и так работает)
        if event.keycode == 67 and event.keysym == "??":  # копирование выделенного текста в буфер обмена
            try:
                text_copy = self.sqlText.selection_get()
            except:
                text_copy = ""
            finally:
                self.root.clipboard_clear()
                self.root.clipboard_append(text_copy)
                self.root.update()
        elif event.keycode == 86 and event.keysym == "??":  # вставка текста из буфера обмена
            self.sqlText.insert("insert", self.root.clipboard_get())

    def selectTree(self, event):  # для любой раскладки
        if event.keysym == "c" or (event.keycode == 67 and event.keysym == "??"):  # копирование выделенных строк в буфер обмена
            try:
                text_copy = [self.sqlResult.item(x)["values"] for x in self.sqlResult.selection()]
            except:
                text_copy = ""
            finally:
                self.root.clipboard_clear()
                self.root.clipboard_append(text_copy)
                self.root.update()

    def beginSQL(self):
        self.dataset = None
        self.headers = None
        self.csvButton["state"] = "disabled"
        self.footer["text"] = "Ожидание ..."  # self.footer.update()
        self.root.config(cursor="wait")  # self.root.update()

        # очистка таблицы
        for item in self.sqlResult.get_children():
            self.sqlResult.delete(item)
        self.sqlResult["columns"] = ()

        self.root.after(150, self.execSQL)  # обновляется GUI до запуска следующей задачи

    def execSQL(self):
        _SQL = self.sqlText.get(1.0, "end")
        is_cursor = True if ':cr' in _SQL else False  # Признак курсора в SQL-запросе
        is_query = True  # Признак выборки
        time1 = time.time()  # время начала запроса

        try:  # Запрос к БД
            with UseDatabase(DATABASE_URI) as cursor:
                if is_cursor:  # SQL-запрос с курсором
                    cursor[0].execute(_SQL, cr=cursor[1])
                    try:
                        self.dataset = cursor[1].fetchall()
                        self.headers = [desc[0].upper() for desc in cursor[1].description]
                    except:
                        is_query = False
                else:  # Чистый SQL-запрос
                    cursor[0].execute(_SQL)
                    try:
                        self.dataset = cursor[0].fetchall()
                        self.headers = [desc[0].upper() for desc in cursor[0].description]
                    except:
                        is_query = False
        except DatabaseError as err:
            self.footer["text"] = str(err)
            self.root.clipboard_clear()
            self.root.clipboard_append(str(err))
            self.root.update()  # остается в буфере обмена после закрытия приложения
        else:
            time2 = time.time()  # время окончания запроса
            delta_time = str(round(time2 - time1, 3))  # время запроса (округление до трех знаков после запятой)
            self.footer[
                "text"] = "Успешно (time = " + delta_time + ")"  # для SQL-запроса не на выборку (вставка, изменение, удаление)

            # Вывод результата SQL-запроса
            if is_query:
                max_first_width = 50  # ширина первого столбца "#0"
                max_col_width = []  # ширина основных столбцов

                if self.headers:
                    self.sqlResult["columns"] = list(self.headers)
                    max_col_width = [(len(str(w)) * 10) + 10 for w in self.headers]
                    for head in self.headers:
                        self.sqlResult.heading(head, text=head)
                    self.footer["text"] = "Данных по запросу нет (time = " + delta_time + ")"
                if self.dataset:
                    for i, line in enumerate(self.dataset):
                        self.sqlResult.insert("", "end", text=i+1, values=[str(l) for l in line])
                        index = (len(str(i + 1)) * 10) + 40
                        if index > max_first_width: max_first_width = index
                        for j, cell in enumerate(line):
                            word = (len(str(cell)) * 10) + 10
                            if word > max_col_width[j]: max_col_width[j] = word
                    self.footer["text"] = "Успешно (time = " + delta_time + ")"

                # устанавливаем ширину столбцов
                if self.headers:
                    self.sqlResult.column("#0", width=max_first_width, anchor="w")
                    for h, header in enumerate(self.headers):
                        self.sqlResult.column(header, width=max_col_width[h], anchor="w")

        self.csvButton["state"] = "normal" if self.dataset else "disabled"
        self.root.config(cursor="")

    def beginCSV(self):
        self.footer["text"] = "Ожидание ..."  # self.footer.update()
        self.root.config(cursor="wait")  # self.root.update()

        self.root.after(150, self.execCSV)  # обновляется GUI до запуска следующей задачи

    def execCSV(self):
        try:  # Запись CSV-файла
            with open(filename, 'w', encoding=text_codec) as f:
                w = csv.writer(f, delimiter=separator, lineterminator=end_line)
                if self.headers:
                    w.writerows([self.headers])  # для правильного отображения заголовков в одну строку CSV файла
                if self.dataset:
                    w.writerows(self.dataset)
                self.footer["text"] = "Успешно (" + filename + ")"
        except Exception as err:
            self.footer["text"] = str(err)

        self.root.config(cursor="")


if __name__ == '__main__':
    app = Tk()
    gui = SQLToolsGUI(app)
    app.mainloop()
