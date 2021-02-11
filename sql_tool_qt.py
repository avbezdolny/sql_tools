#!python3
# -*- coding: utf-8 -*-

import sys, os, csv, time

# Returns path containing content - either locally or in pyinstaller tmp file
def resourcePath():
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS)
    return os.path.abspath(os.path.dirname(__file__))

# Строка соединения с БД
database_URI = 'DRIVER={SQL Server};SERVER=tcp:IP,port;database=db_name;UID=user;PWD=password'  # MS SQL Server
# 'user/password@IP:port/db_name'  # Oracle
# 'filename.sqlite'  # SQLite

# Переменные окружения (при необходимости)
#os.environ["NLS_LANG"] = "RUSSIAN_CIS.CL8MSWIN1251"

# Путь для сохранения CSV
dirname = os.path.expanduser("~\Desktop")  # ссылка на рабочий стол

# Имя файла CSV
filename = os.path.join(dirname, "export.csv")

# Разделитель строк в файле CSV
end_line = '\n'

# Разделитель полей в файле CSV
separator = ';'

# Кодировка текста
text_codec = 'cp1251'

# Функция прерывания программы в случае критической ошибки
def exitError(err):
    print('ERROR:', str(err))
    return sys.exit(0)

# Импорт модуля GUI
try:
    from PySide6 import QtCore, QtWidgets, QtGui
except Exception as e:
    exitError(e)

# для тестирования GUI без модуля драйвера базы данных
try:
    import pyodbc as DB  # cx_Oracle, sqlite3
    INFO_TEXT = "Строка статуса"
except Exception as e:
    INFO_TEXT = str(e)
    DB = None


class databaseError(Exception):
    '''Пользовательский класс исключения для базы данных!'''
    pass


class Usedatabase:
    '''Класс диспетчера контекста для соединения с базой данных!'''

    def __init__(self, config: str) -> None:
        self.configuration = config

    def __enter__(self) -> 'cursor':
        try:
            self.conn = DB.connect(self.configuration)  # соединение с базой данных
            self.cursor1 = self.conn.cursor()
            self.cursor2 = self.conn.cursor()
            return self.cursor1, self.cursor2  # возвращаем два курсора (для ситуации когда курсор исп-ся в самом запросе)
        except Exception as err:
            raise databaseError(err)

    def __exit__(self, exc_type, exc_value, exc_trace) -> None:
        self.conn.commit()
        self.cursor1.close()
        self.cursor2.close()
        self.conn.close()
        if exc_type:
            raise databaseError(exc_value)  # если ошибка в SQL-запросе


# GUI
class SQLWidget(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        
        self.setWindowIcon(QtGui.QIcon(os.path.join(resourcePath(), 'pyinstaller.ico')))
        
        self.trans = QtCore.QTranslator(self)
        self.trans.load('qt_' + QtCore.QLocale.system().name(), QtCore.QLibraryInfo.location(QtCore.QLibraryInfo.TranslationsPath))
        QtWidgets.QApplication.instance().installTranslator(self.trans)
        
        self.data = None  # Результат SQL-запроса
        self.headers = None  # Заголовки столбцов
        
        self.gboxSQL = QtWidgets.QGroupBox("SQL-запрос | :cr для курсора")
        self.gboxSQL.setStyleSheet('QGroupBox {color: "#757575"; font-family: sans-serif; font-size: 12px;}')
        
        self.textSQL = QtWidgets.QTextEdit("select count(*) from sys.dm_exec_connections")
        self.textSQL.setStyleSheet('QTextEdit {color: "#1565c0"; font-family: "Consolas", "Courier New", monospace; font-size: 16px;}')
        
        self.buttonSQL = QtWidgets.QPushButton('Выполнить запрос')
        self.buttonSQL.setStyleSheet('QPushButton {color: "#333333"; font-family: sans-serif; font-size: 14px; height: 25px;}')
        
        self.vboxSQL = QtWidgets.QVBoxLayout()
        self.vboxSQL.addWidget(self.textSQL)
        self.vboxSQL.addWidget(self.buttonSQL)
        self.gboxSQL.setLayout(self.vboxSQL)
        
        self.gboxCSV = QtWidgets.QGroupBox("Данные результата запроса")
        self.gboxCSV.setStyleSheet('QGroupBox {color: "#757575"; font-family: sans-serif; font-size: 12px;}')
        
        self.tableCSV = QtWidgets.QTableWidget()
        self.tableCSV.setStyleSheet('QTableWidget {color: "#333333"; font-family: "Consolas", "Courier New", monospace; font-size: 16px;}')
        
        self.buttonCSV = QtWidgets.QPushButton('Экспортировать данные')
        self.buttonCSV.setEnabled(False)
        self.buttonCSV.setStyleSheet('QPushButton {color: "#333333"; font-family: sans-serif; font-size: 14px; height: 25px;}')
        
        self.vboxCSV = QtWidgets.QVBoxLayout()
        self.vboxCSV.addWidget(self.tableCSV)
        self.vboxCSV.addWidget(self.buttonCSV)
        self.gboxCSV.setLayout(self.vboxCSV)
        
        self.statusLabel = QtWidgets.QLabel(INFO_TEXT)
        self.statusLabel.setWordWrap(True)
        self.statusLabel.setTextInteractionFlags(QtCore.Qt.TextSelectableByMouse)
        self.statusLabel.setStyleSheet('QLabel {color: "#757575"; font-family: sans-serif; font-size: 12px;}')
        self.statusLabel.setAlignment(QtCore.Qt.AlignRight)
        
        self.layout = QtWidgets.QVBoxLayout()
        self.layout.addWidget(self.gboxSQL)
        self.layout.addWidget(self.gboxCSV)
        self.layout.addWidget(self.statusLabel)
        self.layout.insertSpacing(1, 25)
        self.layout.insertSpacing(3, 5)
        self.layout.setStretchFactor(self.gboxSQL, 1)
        self.layout.setStretchFactor(self.gboxCSV, 1)
        self.setLayout(self.layout)
        
        self.buttonSQL.clicked.connect(self.execSQL)
        self.buttonCSV.clicked.connect(self.exportCSV)
    
    
    def execSQL(self):
        self.setCursor(QtCore.Qt.WaitCursor)
        self.tableCSV.clear()
        
        _SQL = self.textSQL.toPlainText()
        
        self.data = None  # Результат SQL-запроса
        self.headers = None  # Заголовки столбцов
        self.buttonCSV.setEnabled(False)
        
        is_cursor = True if ':cr' in _SQL else False  # Признак курсора в SQL-запросе
        is_query = True  # Признак выборки
        
        time1 = time.time()  # время начала запроса
        
        try:  # Запрос к БД
            with Usedatabase(database_URI) as cursor:
                if is_cursor:  # SQL-запрос с курсором
                    cursor[0].execute(_SQL, cr=cursor[1])
                    try:
                        self.data = cursor[1].fetchall()
                        self.headers = [ desc[0].upper() for desc in cursor[1].description ]
                    except:
                        is_query = False
                else:  # Чистый SQL-запрос
                    cursor[0].execute(_SQL)
                    try:
                        self.data = cursor[0].fetchall()
                        self.headers = [ desc[0].upper() for desc in cursor[0].description ]
                    except:
                        is_query = False
        except databaseError as err:
            self.statusLabel.setText(str(err))
        else:
            time2 = time.time()  # время окончания запроса
            delta_time = str(round(time2 - time1, 3))  # время запроса (округление до трех знаков после запятой)
            self.statusLabel.setText('Успешно  ( time = ' + delta_time + ' )')  # для SQL-запроса не на выборку (вставка, изменение, удаление)
            
            # Вывод результата SQL-запроса
            if is_query:
                if self.headers:
                    self.tableCSV.setColumnCount(len(self.headers))
                    self.tableCSV.setHorizontalHeaderLabels(self.headers)
                    self.statusLabel.setText('Данных по запросу нет  ( time = ' + delta_time + ' )')
                if self.data:
                    self.tableCSV.setRowCount(len(self.data))
                    for index_row, row in enumerate(self.data):
                        for index_field, field in enumerate(row):
                            self.tableCSV.setItem(index_row, index_field, QtWidgets.QTableWidgetItem(str(field)))
                    self.statusLabel.setText('Успешно  ( time = ' + delta_time + ' )')
                    self.buttonCSV.setEnabled(True)
                
                self.tableCSV.resizeColumnsToContents()
        
        self.setCursor(QtCore.Qt.ArrowCursor)
    
    
    def exportCSV(self):
        self.setCursor(QtCore.Qt.WaitCursor)
        
        # Экспорт в CSV-файл
        if self.data or self.headers:
            try:  # Запись CSV-файла
                with open(filename, 'w', encoding=text_codec) as f:
                    w = csv.writer(f, delimiter=separator, lineterminator=end_line)
                    if self.headers:
                        w.writerows([self.headers])  # для правильного отображения заголовков в одну строку CSV файла
                    if self.data:
                        w.writerows(self.data)
                    self.statusLabel.setText("Успешно ( " + filename + " )")
            except Exception as err:
                self.statusLabel.setText(str(err))
        elif not self.data:
            self.statusLabel.setText('Нет данных для экспорта')
        
        self.setCursor(QtCore.Qt.ArrowCursor)


# Выполнение программы
if __name__ == '__main__':
    app = QtWidgets.QApplication([])
    widget = SQLWidget()
    widget.setWindowTitle('SQL tools')
    widget.resize(900, 600)
    widget.show()
    sys.exit(app.exec_())
