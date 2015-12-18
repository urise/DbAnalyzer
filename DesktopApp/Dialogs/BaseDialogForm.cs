using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;

namespace DesktopApp.Dialogs
{
    public partial class BaseDialogForm : DesktopApp.BaseForm
    {
        public BaseDialogForm()
        {
            InitializeComponent();
        }

        private void btnCancel_Click(object sender, EventArgs e)
        {
            Close();
            DialogResult = DialogResult.Cancel;
        }

        protected void btnOk_Click(object sender, EventArgs e)
        {

        }
    }
}
