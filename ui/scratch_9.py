import sys
import os
from PyQt5.QtWidgets import QApplication, QPushButton, QMainWindow, QFileDialog
from PyQt5.QtGui import QIcon
from ui.ui_untitled import Ui_MainWindow


class AppWindow(QMainWindow, Ui_MainWindow):
    def __init__(self):
        super().__init__()
        self.setupUi(self)
        self.setWindowTitle('Icon')
        self.setWindowIcon(QIcon('C:\\Users\\adixon\\Desktop\\Projects\\INS Database\\ins-database\\ui\\web.png'))
        self.btn1.clicked.connect(self.browse_folder)

    def browse_folder(self):
        self.listWidget.clear()
        directory = QFileDialog.getExistingDirectory(self, "Pick a folder")

        if directory:
            for file_name in os.listdir(directory):
                self.listWidget.addItem(file_name)


def main():
    app = QApplication(sys.argv)
    form = AppWindow()
    form.show()
    app.exec_()


if __name__ == '__main__':
    main()