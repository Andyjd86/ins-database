# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'untitled.ui'
#
# Created by: PyQt5 UI code generator 5.10.1
#
# WARNING! All changes made in this file will be lost!

from PyQt5 import QtCore, QtGui, QtWidgets

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(510, 600)
        self.cWidget = QtWidgets.QWidget(MainWindow)
        self.cWidget.setObjectName("cWidget")
        self.verticalLayout = QtWidgets.QVBoxLayout(self.cWidget)
        self.verticalLayout.setObjectName("verticalLayout")
        self.listWidget = QtWidgets.QListWidget(self.cWidget)
        self.listWidget.setObjectName("listWidget")
        self.verticalLayout.addWidget(self.listWidget)
        self.hlayout = QtWidgets.QHBoxLayout()
        self.hlayout.setObjectName("hlayout")
        self.btn1 = QtWidgets.QPushButton(self.cWidget)
        self.btn1.setObjectName("btn1")
        self.hlayout.addWidget(self.btn1)
        self.btn2 = QtWidgets.QPushButton(self.cWidget)
        self.btn2.setObjectName("btn2")
        self.hlayout.addWidget(self.btn2)
        self.verticalLayout.addLayout(self.hlayout)
        MainWindow.setCentralWidget(self.cWidget)

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow"))
        self.btn1.setText(_translate("MainWindow", "PushButton"))
        self.btn2.setText(_translate("MainWindow", "PushButton"))

