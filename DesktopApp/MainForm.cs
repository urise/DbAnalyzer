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
            "";

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
            var reader = db.GetTables();
            var dt = new DataTable();
            dt.Load(reader);
            dataGridView1.DataSource = dt;
            dataGridView1.Refresh();


        }
    }
}
