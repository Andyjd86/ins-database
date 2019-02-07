# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'ui_imagedialog.ui'
#
# Created by: PyQt5 UI code generator 5.10.1
#
# WARNING! All changes made in this file will be lost!

from PyQt5 import QtCore, QtGui, QtWidgets

class Ui_ImageDialog(object):
    def setupUi(self, ImageDialog):
        ImageDialog.setObjectName("ImageDialog")
        ImageDialog.resize(400, 300)
        self.buttonBox = QtWidgets.QDialogButtonBox(ImageDialog)
        self.buttonBox.setGeometry(QtCore.QRect(30, 240, 341, 32))
        self.buttonBox.setOrientation(QtCore.Qt.Horizontal)
        self.buttonBox.setStandardButtons(QtWidgets.QDialogButtonBox.Cancel|QtWidgets.QDialogButtonBox.Ok)
        self.buttonBox.setObjectName("buttonBox")
        self.label = QtWidgets.QLabel(ImageDialog)
        self.label.setGeometry(QtCore.QRect(50, 20, 211, 51))
        self.label.setObjectName("label")
        self.verticalLayoutWidget = QtWidgets.QWidget(ImageDialog)
        self.verticalLayoutWidget.setGeometry(QtCore.QRect(9, 9, 381, 141))
        self.verticalLayoutWidget.setObjectName("verticalLayoutWidget")
        self.v1 = QtWidgets.QVBoxLayout(self.verticalLayoutWidget)
        self.v1.setContentsMargins(0, 0, 0, 0)
        self.v1.setObjectName("v1")

        self.retranslateUi(ImageDialog)
        self.buttonBox.accepted.connect(ImageDialog.accept)
        self.buttonBox.rejected.connect(ImageDialog.reject)
        QtCore.QMetaObject.connectSlotsByName(ImageDialog)

    def retranslateUi(self, ImageDialog):
        _translate = QtCore.QCoreApplication.translate
        ImageDialog.setWindowTitle(_translate("ImageDialog", "Dialog"))
        self.label.setText(_translate("ImageDialog", "TextLabel"))

