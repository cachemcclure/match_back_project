from PyQt6 import QtCore, QtWidgets
from PyQt6.QtWidgets import QApplication, QMainWindow, QFileDialog
import polars as pl
import sys
from pickle import load as pload


creds = {
    "redshift_username": "reservebar-master",
    "redshift_password": "0$sd^e2ivN!9xP!MO4Mr",
    "redshift_host": "reservebar-master.cc6flg5f8ipy.us-east-1.redshift.amazonaws.com",
    "redshift_port": "5439",
    "redshift_database": "reservebar-master",
}


class MainWindow(QMainWindow):
    def __init__(self):
        super(MainWindow, self).__init__()
        self.creds = pload(open("creds.pkl", "rb"))
        self.init_ui()

    def init_ui(self):
        central_widget = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(self)
        central_widget.setLayout(layout)
        self.setCentralWidget(central_widget)

        # Exit Button
        self.exit_b = QtWidgets.QPushButton(self)
        self.exit_b.setText("Exit")
        self.exit_b.clicked.connect(self.close)

        # Retrieve Data Button
        self.ret_b = QtWidgets.QPushButton(self)
        self.ret_b.setText("Retrieve Data")
        self.ret_b.clicked.connect(self.ret_data)
        self.ret_label = QtWidgets.QLabel(self)
        self.ret_label.setText("No Data")
        self.ret_label.setFixedHeight(30)
        self.ret_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

        # Export to CSV Button
        self.export_b = QtWidgets.QPushButton(self)
        self.export_b.setText("Export Data to CSV")
        self.export_b.clicked.connect(self.export_csv)

        # Format Window
        layout.addWidget(self.ret_label)
        layout.addWidget(self.ret_b)
        layout.addWidget(self.export_b)
        layout.addWidget(self.exit_b)
        self.setGeometry(100, 100, 900, 600)

    def ret_data(self):
        options = QFileDialog.Option.DontUseNativeDialog
        self.filename, _ = QFileDialog.getOpenFileName(
            self, "Choose data file", "", "All Files (*)", options=options
        )
        if self.filename:
            self.read_data(self.filename)
            self.read_sql()
            self.join_data()
            self.ret_label.setText(f"Data loaded from {self.filename}")
        return

    def read_data(self, filename):
        self.rawdf = pl.read_csv(source=filename, separator=",")
        return

    def read_sql(self):
        with open("query.sql", "r") as f:
            sql = f.read()
        uri = (
            f'redshift://{self.creds["redshift_username"]}:{self.creds["redshift_password"]}@'
            + f'{self.creds["redshift_host"]}:{self.creds["redshift_port"]}/{self.creds["redshift_database"]}'
        )
        self.querydf = pl.read_database(
            query=sql, connection_uri=uri, engine="connectorx"
        )
        return

    def join_data(self):
        self.exportdf = self.querydf.join(self.rawdf, on="email", how="inner")
        return

    def export_csv(self):
        try:
            self.exportdf.write_csv("exported_data.csv")
            self.ret_label.setText("Data Exported Successfully!")
        except Exception as err:
            print(str(err)[:250])
            self.ret_label.setText(
                "Data not exported. Make sure a plot is generated before attempting to export!"
            )


def window():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.setWindowTitle("Interactive Dashboard Builder")
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    window()
