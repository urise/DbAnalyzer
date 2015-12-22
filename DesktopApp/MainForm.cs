using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Windows.Forms;
using BusinessLogic.DatabaseStructure;

namespace DesktopApp
{
    public partial class MainForm : DesktopApp.BaseForm
    {
        private const string ConnectionString =
            @"data source=U\SQLSERVER;initial catalog=ALPHA_FRONTIER_QA;User Id=ifs;PWD=frontier;persist security info=True;App=EntityFramework";

        public MainForm()
        {
            InitializeComponent();
        }

        private void MainForm_Load(object sender, EventArgs e)
        {
            InitDatabase();
        }

        private void InitDatabase()
        {
            var db = new DbInfo(ConnectionString);
            var dt = db.GetTableInfos();
            dataGridView1.DataSource = dt;
            dataGridView1.Refresh();
        }
    }
}
