import wx
import os
import sys
import traceback
import pymssql
import logging
import subprocess
from datetime import datetime, timedelta
from wx.lib import sized_controls
from wx.lib.mixins.listctrl import CheckListCtrlMixin, ListCtrlAutoWidthMixin
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
#from myPanels import MyPanels

# setting to create log file
logging.basicConfig(filename='panelControls.log', format='%(asctime)s %(message)s', level=logging.DEBUG)

def show_error():
    message = ''.join(traceback.format_exception(*sys.exc_info()))
    dialog = wx.MessageDialog(None, message, 'Error!', wx.OK|wx.ICON_ERROR)
    dialog.ShowModal()


class MyPanels(wx.Panel):

    def __init__(self, parent, id):
        wx.Panel.__init__(self, parent, style=wx.RAISED_BORDER)
        #wx.SIMPLE_BORDER wx.RAISED_BORDER wx.SUNKEN_BORDER wx.NO_BORDER
        self.parent = parent


class CheckListCtrl(wx.ListCtrl, CheckListCtrlMixin, ListCtrlAutoWidthMixin):

    def __init__(self, parent):
        wx.ListCtrl.__init__(self, parent, wx.ID_ANY, style=wx.LC_REPORT |
                wx.SUNKEN_BORDER)
        CheckListCtrlMixin.__init__(self)
        ListCtrlAutoWidthMixin.__init__(self)


class UploadTool(sized_controls.SizedFrame):

    def __init__(self, *args, **kwargs):
        super(UploadTool, self).__init__(*args, **kwargs)
        self.SetTitle('Upload Tool')
        
        self.SetInitialSize((800, 400))
        #self.parent = args[0]

        self.panel = self.GetContentsPane()
        self.panel.SetBackgroundColour("grey")

        self.leftpanel = MyPanels(self.panel, -1)
        self.rightpanel = MyPanels(self.panel, -1)

        self.basicsizer = wx.BoxSizer(wx.HORIZONTAL)
        self.vsizer = wx.BoxSizer(wx.VERTICAL)

        self.basicsizer.Add(self.leftpanel, proportion=1, flag=wx.EXPAND)
        self.basicsizer.Add(self.rightpanel, proportion=4, flag=wx.EXPAND)
        self.vsizer.Add(self.basicsizer, 1, wx.EXPAND)
        self.panel.SetSizer(self.vsizer)
        self.panel.Fit()

        self.prodBtn =  wx.Button(self.leftpanel, -1, 'Data Prep', (30, 30))
        self.devBtn = wx.Button(self.leftpanel, -1, 'Insert Data', (30, 70))

        self.prodBtn.Bind(wx.EVT_BUTTON, self.onClickPrepData)
        self.devBtn.Bind(wx.EVT_BUTTON, self.onClickInsertData)


    def onClickPrepData(self, event):
        dlg = wx.MessageDialog(None, "We are preparing prod data. Do you want to get PROD data?", 'RUN Dialog', wx.YES_NO | wx.ICON_QUESTION)
        result = dlg.ShowModal()
        if result == wx.ID_YES:
            try:
                print ("Downloading data ......")
                self.runPythonDataAnalysis()
            except Exception as e:
                print ("runPythonDataAnalysis(), e: {}".format(e))
            else:
                ## read excel file
                #today = datetime.date.today()
                today = datetime.now()
                fstDateOfMonth = today.replace(day=1)
                lastDate = fstDateOfMonth - timedelta(days=1)
                firstDate = lastDate.replace(day=1)

                compfile = 'compare_file_'+ firstDate.strftime("%b%Y") +'_scen_gen.xlsx'
                compExcelFile = os.path.join(os.getcwd(), compfile)
                if os.path.exists(compExcelFile):
                    compExcelDF = pd.read_excel(compExcelFile, sheetname = "Sheet1")
                    print compExcelDF
                    self.list = CheckListCtrl(self.rightpanel)
                    self.list.InsertColumn(0, "key date", wx.LIST_FORMAT_CENTER)
                    self.list.InsertColumn(1, "prod date", wx.LIST_FORMAT_CENTER)
                    self.list.InsertColumn(2, "current date", wx.LIST_FORMAT_CENTER)
                    self.list.InsertColumn(3, "date status", wx.LIST_FORMAT_CENTER)

                    idx = 0 
                    for index, row in compExcelDF.iterrows():
                        status = ''
                        prod_date = row['dateinprod']
                        curr_date = row['scendatesgenera']
                        if (row['datestatus'] != 'NaN'):
                            status = row['datestatus']
                        if (row['newdates'] != 'NaN'):
                            status = row['newdates']

                        # print "MAXINT: ", sys.maxint
                        # self.list.append(('04-01-2018', str(prod_date), str(status), str(curr_date)))
                        indx = self.list.InsertStringItem(idx, '04-01-2018')
                        self.list.SetStringItem(indx, 1, str(prod_date))
                        self.list.SetStringItem(indx, 2, str(status))
                        self.list.SetStringItem(indx, 3, str(curr_date))
                        idx += 1

                else:
                    print ("onClickPrepData(): compare file '{}' not exists ...".format(compExcelFile))
        else:
            print ('Sorry!!!!!This is a warning')
            return

        self.selectAllBtn = wx.Button(self.leftpanel, -1, 'Select All', (30, 110))
        self.deselectAllBtn = wx.Button(self.leftpanel, -1, 'Deselect All', (30, 150))
        self.deleteBtn = wx.Button(self.leftpanel, -1, 'Delete', (30, 190))

        self.selectAllBtn.Bind(wx.EVT_BUTTON, self.OnSelectAll)
        self.deselectAllBtn.Bind(wx.EVT_BUTTON, self.OnDeselectAll)
        self.deleteBtn.Bind(wx.EVT_BUTTON, self.OnDelete)
        #self.Bind(wx.EVT_BUTTON, self.OnApply, id=appBtn.GetId())


    def OnSelectAll(self, event):

        num = self.list.GetItemCount()
        for i in range(num):
            self.list.CheckItem(i)


    def OnDeselectAll(self, event):

        num = self.list.GetItemCount()
        for i in range(num):
            self.list.CheckItem(i, False)


    def OnDelete(self, event):

        num = self.list.GetItemCount()

        for i in range(num):

            if self.list.IsChecked(i):
                print "ID: ", i
                self.list.DeleteItem(i)
                print ("DELETED......")


    def onClickInsertData(self, event):
        pass


    def onClickDevRun(self, event):
        pass


    def runPythonDataAnalysis(self):
        subprocess.call(['python', 'automateDataAnalysis.py'], shell = False)


class LoginFrame(sized_controls.SizedDialog):

    def __init__(self, *args, **kwargs):
        super(LoginFrame, self).__init__(*args, **kwargs)
        self.parent = args[0]
        self.logged_in = False

        pane = self.GetContentsPane()

        pane_form = sized_controls.SizedPanel(pane)
        pane_form.SetSizerType('form')

        label = wx.StaticText(pane_form, label='User Name')
        label.SetSizerProps(halign='right', valign='center')

        self.user_name_ctrl = wx.TextCtrl(pane_form, size=((200, -1)))

        label = wx.StaticText(pane_form, label='Password')
        label.SetSizerProps(halign='right', valign='center')

        self.password_ctrl = wx.TextCtrl(
            pane_form, size=((200, -1)), style=wx.TE_PASSWORD)

        pane_btns = sized_controls.SizedPanel(pane)
        pane_btns.SetSizerType('horizontal')
        pane_btns.SetSizerProps(halign='right')

        login_btn = wx.Button(pane_btns, label='Login')
        login_btn.SetDefault()
        cancel_btn = wx.Button(pane_btns, label='Cancel')
        self.Fit()
        self.SetTitle('Login')
        self.CenterOnParent()
        self.parent.Disable()

        login_btn.Bind(wx.EVT_BUTTON, self.on_btn_login)
        cancel_btn.Bind(wx.EVT_BUTTON, self.on_btn_cancel)
        self.Bind(wx.EVT_CLOSE, self.on_close)


    def on_btn_login(self, event):
        user_name = self.user_name_ctrl.GetValue()
        password = self.password_ctrl.GetValue()
        
        if (user_name == 'admin' and password == 'password'):
            print 'logged in as {} with password {}'.format(user_name, password)
            self.logged_in = True
            self.Close()


    def on_btn_cancel(self, event):
        self.Close()


    def on_close(self, event):
        if not self.logged_in:
            self.parent.Close()
        self.parent.Enable()
        event.Skip()


def getDataFromTable(startDate, endDate):
    ## instance a python db connection object (same as python-mysql drivers)
    conn = pymssql.connect(
                server   = cfg.mssql['host'],
                database = cfg.mssql['db'], 
                user     = cfg.mssql['user'],
                password = cfg.mssql['passwd'], 
                port     = cfg.mssql['port']
                      )   

    # sql query
    #stmt = "SELECT * FROM yld_crv_hist_v WHERE dt > '" + startDate + "' and dt < '" + endDate + "'"
    
    stmt = "EXEC s_SGD_CPA_raw_data_R @bus_dt = '{0}', @liq_period = {1}, @look_back_period = {2}".format('1990-01-10', 10, 5)
    #stmt = "EXEC s_SGD_CPA_raw_data_R @bus_dt = '1990-01-10', @liq_period = 10, @look_back_period = 5".format(my_proc)
    #print stmt
    # Excute Query here
    df = pd.read_sql_query(stmt, conn)
    if df is not None:
        #print df.head(5)
        return df


def callRScript():
    subprocess.check_call(['Rscript', 'myrscript.R'], shell = False)


def main():
    # Set up all the stuff for a wxPython application
    app = wx.App()
    try:
        # Add a top level frame
        parentFrm = UploadTool(None)
        # Display the frame and panel within it
        parentFrm.Show()
        loginFrm = LoginFrame(parentFrm)
        loginFrm.Show()
        # Wait for events and process each one you receive
        app.MainLoop()
    except:
        show_error()


if __name__ == '__main__':
    main()
