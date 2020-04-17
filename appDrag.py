#builtin
import os
# 3rd party
from PyQt5 import QtCore, QtGui, QtWidgets
# local
_dir = os.path.dirname(os.path.abspath(__file__))
os.sys.path.append(os.path.join(_dir, 'from_pupil'))
from from_pupil.player_methods import is_pupil_rec_dir
from from_pupil.extract_diameter import process_recording 
from from_pupil.extract_diameter import process_recording_annotations

class dragApp(QtWidgets.QWidget):

    def __init__(self):
        if QtWidgets.QApplication.instance() is None:
            self._app = QtWidgets.QApplication([])
        else: 
            self._app = QtWidgets.QApplication.instance()
        super().__init__()
        pix = QtGui.QPixmap(os.path.join(_dir, 'icon.png'))
        self.icon = QtGui.QIcon(pix)
        self.setWindowIcon(self.icon)
        self.setWindowTitle('Batch exporter')

        self.target_path = ''     
        self.recordings = None
        self.levels = None

        self.setAcceptDrops(True)
        self.setEnabled(True)
        self.main_layout = QtWidgets.QHBoxLayout()
        self.setLayout(self.main_layout)

        self.setup_main_panel()
        self.setup_side_panel()
           
        self.show()
        self.resize(600, 480)

    def setup_main_panel(self):
        self.stack = QtWidgets.QStackedWidget()
        self.stack_wait = QtWidgets.QLabel('Drop folder here!')
        self.stack_wait.setAlignment(
            QtCore.Qt.AlignCenter | QtCore.Qt.AlignVCenter
            )
        font = self.stack_wait.font()
        font.setPixelSize(24)
        font.setBold(True)
        self.stack_wait.setFont(font)

        self.layout().addWidget(
            self.stack, 
            stretch = 5
            )
        self.stack.addWidget(self.stack_wait)
        self.stack.setCurrentWidget(self.stack_wait)

    def setup_side_panel(self):
        # Create layout and widget
        self.side_panel = QtWidgets.QWidget()
        self.side_layout = QtWidgets.QVBoxLayout()
        self.layout().addWidget(self.side_panel)
        self.side_panel.setLayout(self.side_layout)

        # Create controls
        self.chkbox_group = QtWidgets.QCheckBox('Group exports')
        self.chkbox_annotation = QtWidgets.QCheckBox('Export annotations')
        self.chkbox_omit = QtWidgets.QCheckBox('Omit first level')
        self.target = QtWidgets.QLineEdit(self.target_path)
        self.export = QtWidgets.QPushButton('Export')

        self.chkbox_group.clicked.connect(self.clicked_checkbox)
        self.target.textChanged.connect(self.changed_text)
        self.export.clicked.connect(self.clicked_export)

        # Add controls
        self.side_layout.addWidget(self.chkbox_group)
        self.side_layout.addWidget(self.chkbox_annotation)
        self.side_layout.addWidget(self.chkbox_omit)
        self.side_layout.addWidget(self.target)
        self.target.setVisible(False)
        self.side_layout.addStretch(stretch = 5)
        self.side_layout.addWidget(self.export)

    # The drag and drop section
    def dropEvent(self, e):
        self.tree = QtWidgets.QTreeWidget()
        self.tree.setHeaderLabel('Recordings architecture')
        self.stack.addWidget(self.tree)
        self.target_path = ''  

        e.acceptProposedAction()
        data =  e.mimeData()

        recordings = None
        levels = []
        if data.hasUrls():
            for url in data.urls():
                # Uniform path
                path = url.toLocalFile()
                path = os.path.abspath(path)

                (url_tree_item, url_recordings, url_levels) = getTree(path)
                if url_tree_item is None:
                    continue
                
                if recordings is None:
                    recordings = url_recordings
                    levels = url_levels
                else:
                    recordings.extend(url_recordings)
                    levels.extend(url_levels)
                
                # Choose a location for grouped exports
                if len(self.target_path) < 1:
                    self.target.setText(os.path.join(path, 'exports'))

                self.tree.addTopLevelItem(url_tree_item)
                self.stack.setCurrentWidget(self.tree)
                
        if recordings is not None:
            self.recordings = recordings
            self.levels = levels

    # Override events
    def dragEnterEvent(self, e):
        e.accept()

    def dragMoveEvent(self, e):
        e.accept()

    def changed_text(self, s):
        self.target_path = self.target.text()

    def clicked_checkbox(self, s):
        self.target.setVisible(not self.target.isVisible())

    def clicked_export(self, s):
        recordings = self.recordings
        if recordings is None:
            return

        group = self.chkbox_group.isChecked()
        omit = self.chkbox_omit.isChecked()
        target = self.target_path
        annotation = self.chkbox_annotation.isChecked()
        level = self.levels

        for (rec, lvl) in zip(recordings, level):
            name = 'pupil_positions'
            if omit:
                lvl = lvl[lvl.index('_')+1:]

            if not group:
                target = os.path.join(rec, 'exports')
            else:
                name += '_' + lvl

            if not os.path.exists(target):
                os.mkdir(target)

            process_recording(rec, name + '.csv', target, overwrite = True)
            if annotation:
                name = 'annotations'
                if group:
                    name += '_' + lvl
                process_recording_annotations(
                    rec, name + '.csv', target, overwrite = True)

        self.stack.setCurrentWidget(self.stack_wait)
        self.stack_wait.setText('Done! \n Drop another folder here')
        self.target_path = ''     
        self.recordings = None
        self.levels = None

def getTree(path, tree = None):

    # Init
    recordings = []
    level = []
    if tree is None:
        tree = QtWidgets.QTreeWidgetItem()

    # Only directories can be recordings
    if not os.path.isdir(path):
        return tree, recordings, level
        
    cur_di = path.split(os.sep)[-1]

    # Is the directory a recording
    if is_pupil_rec_dir(path):
        recordings.append(path) 
        tree.setText(0, cur_di) 
        level.append(cur_di)

    else: 
        # Look into the directory
        dirs = os.listdir(path)
        for di in dirs:
            # Remove the hidden files
            if di.startswith('.'):
                continue
            
            # Create a new tree for each directory
            di_path = os.path.join(path, di)
            di_tree = QtWidgets.QTreeWidgetItem()

            # Recursion
            (di_tree, rec, lvl) = getTree(di_path, tree = di_tree)
            # If there was a recording we need to have the different 
            # levels
            if len(rec) > 0:
                di_tree.setText(0, di)
                tree.setText(0, cur_di)
                tree.addChild(di_tree)
                recordings.extend(rec)
                new_level = [cur_di + '_' + lev for lev in lvl]  
                level.extend(new_level)
            
    return tree, recordings, level    

if __name__ == '__main__':
    wid = dragApp()
    wid._app.exec()