using System;
using System.Data;
using AccosoftUtilities;
using AccosoftLogWriter;
using AccosoftNineteenCDL;
using System.Configuration;
using System.Linq;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.HtmlControls;
using System.Web.UI.WebControls;
using System.Web.UI.WebControls.WebParts;
using System.Xml.Linq;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Collections;
//using AccosoftRML.Entity_Classes.RawMaterial;
using System.Collections.Generic;
using System.Globalization;

/// <summary>
/// Created By      :   Jins Mathew
/// Created On      :   15-Dec-2012
/// modified by     :   Jomy P chacko 
/// </summary>

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class PurchaseEntryLogicRM
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public string Branch { get; set; }
        public string DateorTime { get; set; }


        #region Fill Combo

        public DataTable FillBranch ( string UserName )
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();



                SQL = "  SELECT AD_BR_CODE CODE,AD_BR_NAME NAME FROM AD_BRANCH_MASTER ";
                SQL = SQL + " WHERE  AD_BR_ACTIVESTATUS_YN ='Y'";

                if (UserName != "ADMIN")
                { // JOMY ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 06 aug 2020
                    SQL = SQL + "   and     AD_BR_CODE  IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                }

                SQL = SQL + " ORDER BY AD_BR_CODE ASC ";

                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dtReturn;
        }


        #endregion


        #region "Fil view
        public DataTable FillView ( string TypeID, string SuppCode, string sFromdate, string sTodate, string Branch )
        {
            DataTable dtType = new DataTable();
            try
            //-- this look i up is using various places os take care // Jomy & jins  11 / jan 23013
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");


                if (TypeID == "SUPPLLIERAPPRVL")
                {

                    SQL = "select distinct Code,Name from ( ";
                    SQL = SQL + " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE and RM_MRD_APPROVED ='N' ";

                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }

                    // SQL = SQL  + "    AND  RM_MRM_RECEIPT_DATE BETWEEN  "
                    //    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                    if (DateorTime == "TIME")
                    {
                        SQL = SQL + "    AND   RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(sFromdate + " 07:00:00 AM", culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";
                        SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(sTodate + " 06:59:59 AM", culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";
                    }
                    else
                    {
                        SQL = SQL + "         AND RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    }


                    SQL = SQL + "         union all ";
                    SQL = SQL + "  SELECT    ";
                    SQL = SQL + "         RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS,RM_RECEPIT_TOLL ";
                    SQL = SQL + "   WHERE     RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = ";
                    SQL = SQL + "             RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE ";
                    SQL = SQL + "             and  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO=RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO ";
                    SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'N' ";
                    SQL = SQL + "         AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ('" + Branch + "')";


                    if (DateorTime == "TIME")
                    {
                        SQL = SQL + "    AND   RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(sFromdate + " 07:00:00 AM", culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";
                        SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(sTodate + " 06:59:59 AM", culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";
                    }
                    else
                    {
                        SQL = SQL + "         AND RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    }
                    SQL = SQL + "         ) ";

                    SQL = SQL + "    ORDER BY  NAME ASC  ";

                }
                else if (TypeID == "SUPPLLIERREVISION")
                {


                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE and RM_MRD_APPROVED ='N' ";
                    // SQL = SQL  + "    AND  RM_MRM_RECEIPT_DATE BETWEEN  "
                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }

                    SQL = SQL + "    ORDER BY RM_VM_VENDOR_NAME ASC  ";

                }
                else if (TypeID == "TRANSPORTER_REVISION")
                {


                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE and RM_MRD_APPROVED ='N' ";
                    // SQL = SQL  + "    AND  RM_MRM_RECEIPT_DATE BETWEEN  "
                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }
                    SQL = SQL + "    ORDER BY RM_VM_VENDOR_NAME ASC  ";

                }

                else if (TypeID == "SUPPLLIER")
                {
                    SQL = " SELECT ";
                    SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name ";
                    SQL = SQL + " FROM RM_VENDOR_MASTER";
                    //SQL = SQL + " WHERE RM_PO_MASTER.RM_PO_ENTRY_TYPE = 'PURCHASE'";
                    SQL = SQL + " ORDER BY RM_VM_VENDOR_NAME ASC ";
                }
                else if (TypeID == "RECEIPTVENDOR")
                {
                    // THIS IS FOR VENDOR PURCHASE ENTRY NOT YET DONE 


                    SQL = " select distinct Code,Name from ( select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  ";
                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }
                    SQL = SQL + "   and RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='Y' AND RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY ='N'";

                    // Enabled Date Filtering as per the request from Mr. Jojimon on 10-Dec-2017.

                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                    SQL = SQL + "         union all ";
                    SQL = SQL + "  SELECT  DISTINCT  ";
                    SQL = SQL + "         RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS,RM_RECEPIT_TOLL,RM_RAWMATERIAL_MASTER ";
                    SQL = SQL + "   WHERE     RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = ";
                    SQL = SQL + "             RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE ";
                    SQL = SQL + "             and  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO=RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO ";

                    SQL = SQL + "             and  RM_RECEPIT_TOLL.RM_RMM_RM_CODE =   RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                    SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y' AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY ='N' ";
                    SQL = SQL + "         and RM_RMM_RM_TYPE<>'TOLL'  ";
                    SQL = SQL + "         AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ('" + Branch + "')";
                    SQL = SQL + "         AND RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + "         ) ";

                    SQL = SQL + "    ORDER BY NAME  ASC  ";

                }

                else if (TypeID == "RECEIPTTOLLVENDOR")
                {
                    // THIS IS FOR VENDOR PURCHASE ENTRY NOT YET DONE 


                    SQL = " select distinct Code,Name from ( select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  ";
                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }
                    SQL = SQL + "   and RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='Y' AND RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY ='N'";

                    // Enabled Date Filtering as per the request from Mr. Jojimon on 10-Dec-2017.

                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                    SQL = SQL + "         union all ";
                    SQL = SQL + "  SELECT    ";
                    SQL = SQL + "         RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS,RM_RECEPIT_TOLL ,RM_RAWMATERIAL_MASTER ";
                    SQL = SQL + "   WHERE     RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = ";
                    SQL = SQL + "             RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE ";
                    SQL = SQL + "             and  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO=RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO ";

                    SQL = SQL + "             and  RM_RECEPIT_TOLL.RM_RMM_RM_CODE =   RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                    SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y' AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY ='N' ";
                    //    SQL = SQL + "         and RM_RMM_RM_TYPE<>'TOLL'  ";
                    SQL = SQL + "         AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ('" + Branch + "')";
                    SQL = SQL + "         AND RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + "         ) ";

                    SQL = SQL + "    ORDER BY NAME  ASC  ";

                }
                else if (TypeID == "RECEIPTTRANSPORTER")
                {
                    // THIS IS FOR  trsporter  PURCHASE ENTRY NOT YET DONE 

                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE  ";
                    SQL = SQL + "   and RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='Y' AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY ='N'";
                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }
                    // Enabled Date Filtering as per the request from Mr. Jojimon on 10-Dec-2017.

                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                    SQL = SQL + "    ORDER BY RM_VM_VENDOR_NAME ASC  ";

                }

                else if (TypeID == "RECEIPTTOLLTRANSPORTER")
                {
                    // THIS IS FOR  trsporter  PURCHASE ENTRY NOT YET DONE 


                    SQL = " select distinct Code,Name from ( select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE  ";
                    SQL = SQL + "   and RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='Y' AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY ='N'";
                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }
                    // Enabled Date Filtering as per the request from Mr. Jojimon on 10-Dec-2017.

                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                    SQL = SQL + "         union all ";
                    SQL = SQL + "  SELECT    ";
                    SQL = SQL + "         RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS,RM_RECEPIT_TOLL ,RM_RAWMATERIAL_MASTER ";
                    SQL = SQL + "   WHERE     RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = ";
                    SQL = SQL + "             RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE ";
                    SQL = SQL + "             and  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO=RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO ";

                    SQL = SQL + "             and  RM_RECEPIT_TOLL.RM_RMM_RM_CODE =   RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                    SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y' AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY ='N' ";
                    //    SQL = SQL + "         and RM_RMM_RM_TYPE<>'TOLL'  ";
                    SQL = SQL + "         AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ('" + Branch + "')";
                    SQL = SQL + "         AND RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + "         ) ";

                    SQL = SQL + "    ORDER BY NAME  ASC  ";
                }



                else if (TypeID == "TOLLTRANSPORTER")
                {
                    // THIS IS FOR  trsporter  PURCHASE ENTRY NOT YET DONE 

                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE  ";
                    SQL = SQL + "   and RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='Y' AND RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_YN ='N'";
                    // Enabled Date Filtering as per the request from Mr. Jojimon on 10-Dec-2017.
                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }
                    SQL = SQL + "   AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                    SQL = SQL + "    ORDER BY RM_VM_VENDOR_NAME ASC  ";

                }
                else if (TypeID == "TOLLVENDOR")
                {
                    // THIS IS FOR  vendor toll   PURCHASE ENTRY NOT YET DONE 

                    //SQL = " select  ";
                    //SQL = SQL + "    DISTINCT  ";
                    //SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    //SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    //SQL = SQL + "    WHERE   ";
                    //SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  ";
                    //SQL = SQL + "    and RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='Y' AND RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_YN ='N'";
                    //// Enabled Date Filtering as per the request from Mr. Jojimon on 10-Dec-2017.
                    //if (!string.IsNullOrEmpty(Branch))
                    //{
                    //    SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    //}

                    //SQL = SQL + "    AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                    //SQL = SQL + "    ORDER BY RM_VM_VENDOR_NAME ASC  ";


                    SQL = SQL + "  SELECT    ";
                    SQL = SQL + "     distinct    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS,RM_RECEPIT_TOLL,RM_RAWMATERIAL_MASTER ";
                    SQL = SQL + "   WHERE     RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = ";
                    SQL = SQL + "             RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE ";
                    SQL = SQL + "             and  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO=RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO ";
                    SQL = SQL + "             and  RM_RECEPIT_TOLL.RM_RMM_RM_CODE =   RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                    SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y'  AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY ='N' ";
                    SQL = SQL + "         and RM_RMM_RM_TYPE='TOLL'  ";
                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + "         AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ('" + Branch + "')";
                    }
                    SQL = SQL + "         AND RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + "    ORDER BY RM_VM_VENDOR_NAME ASC  ";

                }
                else if (TypeID == "TOLLTRANSPORTER")
                {
                    // THIS IS FOR  vendor toll   PURCHASE ENTRY NOT YET DONE 

                    //SQL = " select  ";
                    //SQL = SQL + "    DISTINCT  ";
                    //SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    //SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    //SQL = SQL + "    WHERE   ";
                    //SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  ";
                    //SQL = SQL + "    and RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='Y' AND RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_YN ='N'";
                    //// Enabled Date Filtering as per the request from Mr. Jojimon on 10-Dec-2017.
                    //if (!string.IsNullOrEmpty(Branch))
                    //{
                    //    SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    //}

                    //SQL = SQL + "    AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                    //SQL = SQL + "    ORDER BY RM_VM_VENDOR_NAME ASC  ";


                    SQL = SQL + "  SELECT    ";
                    SQL = SQL + "     distinct    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS,RM_RECEPIT_TOLL,RM_RAWMATERIAL_MASTER ";
                    SQL = SQL + "   WHERE     RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = ";
                    SQL = SQL + "             RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE ";
                    SQL = SQL + "             and  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO=RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO ";
                    SQL = SQL + "             and  RM_RECEPIT_TOLL.RM_RMM_RM_CODE =   RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                    SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y'  AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY ='Y' ";
                    SQL = SQL + "         and RM_RMM_PRODUCT_TYPE IN ('OTHER', 'TOLL')   ";
                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + "         AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ('" + Branch + "')";
                    }
                    SQL = SQL + "         AND RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + "    ORDER BY RM_VM_VENDOR_NAME ASC  ";

                }

                else if (TypeID == "TRANSFERSTRANSPORTER")
                {
                    // THIS IS FOR  stock transfer trsporter  PURCHASE ENTRY NOT YET DONE 
                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_MAT_STK_TRANSFER_DETL,RM_MAT_STK_TRANSFER_MASTER  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANSPORTER_CODE  ";
                    SQL = SQL + "    AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO=RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO ";
                    SQL = SQL + "    AND RM_MSTD_PE_DONE ='N'";
                    SQL = SQL + "    AND RM_MSTM_TRANSFER_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                    //and RMRM_MSTD_REC_FLG ='Y'
                    SQL = SQL + "    ORDER BY RM_VM_VENDOR_NAME ASC  ";


                }
                else if (TypeID == "FPSALL")
                {
                    SQL = " SELECT ";
                    SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name ";
                    SQL = SQL + " FROM RM_VENDOR_MASTER";
                    //SQL = SQL + " WHERE RM_PO_MASTER.RM_PO_ENTRY_TYPE = 'PURCHASE'";
                    SQL = SQL + " ORDER BY RM_VM_VENDOR_NAME ASC ";
                }

                else if (TypeID == "ACCCODE" || TypeID == "ADJACCODE" || TypeID == "ADJGRIDACCODE")
                {
                    SQL = " SELECT GL_COAM_ACCOUNT_CODE Code, GL_COAM_ACCOUNT_NAME Name";
                    SQL = SQL + " FROM GL_COA_MASTER WHERE   ";
                    SQL = SQL + " GL_COAM_LEDGER_TYPE = 'SUB' AND GL_COAM_ACCOUNT_STATUS = 'ACTIVE'";
                    SQL = SQL + " ORDER BY GL_COAM_ACCOUNT_CODE ";
                }
                else if (TypeID == "FPSSTATION")
                {
                    SQL = " SELECT ";
                    SQL = SQL + " SALES_STN_STATION_CODE Code, SALES_STN_STATION_NAME Name ";
                    SQL = SQL + "  from SL_STATION_MASTER WHERE  SALES_RAW_MATERIAL_STATION ='Y' ";
                    SQL = SQL + "  ORDER BY SALES_STN_STATION_CODE ";

                }
                else if (TypeID == "FPSRM")
                {

                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "     RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code, RM_RMM_RM_DESCRIPTION Name  ";
                    SQL = SQL + "    FROM RM_RAWMATERIAL_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE =     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE ";
                    SQL = SQL + "   and RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='N'";
                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }

                    if (SuppCode != null)
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = '" + SuppCode + "'";

                    }
                    SQL = SQL + "    ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC  ";

                }
                else if (TypeID == "TRANSPORTER_RM_REVISION")
                {

                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "     RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code, RM_RMM_RM_DESCRIPTION Name  ";
                    SQL = SQL + "    FROM RM_RAWMATERIAL_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE =     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE ";
                    SQL = SQL + "   and RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='N'";
                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }
                    if (SuppCode != null)
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE = '" + SuppCode + "'";

                    }
                    SQL = SQL + "    ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC  ";

                }
                else if (TypeID == "RECEIPTVENDOR_PURCHASEENTRY")
                {

                    // SLECT TEH PENDING RM DETAILS AGAINST THE SUPPLIER 
                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "     RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code, RM_RMM_RM_DESCRIPTION Name  ";
                    SQL = SQL + "    FROM RM_RAWMATERIAL_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE =     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE   ";
                    SQL = SQL + "   and RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='Y' AND RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY ='N'";
                    //  SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }

                    if (SuppCode != null)
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = '" + SuppCode + "'";

                    }
                    SQL = SQL + "    ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC  ";

                }
                else if (TypeID == "RECEIPTTRANSPORTER_PURCHASEENTRY")
                {

                    // SLECT TEH PENDING RM DETAILS AGAINST THE SUPPLIER 
                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "     RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code, RM_RMM_RM_DESCRIPTION Name  ";
                    SQL = SQL + "    FROM RM_RAWMATERIAL_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE =     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE   ";
                    SQL = SQL + "   and RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='Y' AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY ='N'";
                    //  SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }
                    if (SuppCode != null)
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE = '" + SuppCode + "'";

                    }
                    SQL = SQL + "    ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC  ";

                }

                else if (TypeID == "TRANSFERSTRANSPORTER_PURCHASEENTRY")
                {

                    // SLECT TEH   RM DETAILS AGAINST THE SUPPLIER  

                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "     RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code, RM_RMM_RM_DESCRIPTION Name  ";
                    SQL = SQL + "    FROM RM_RAWMATERIAL_MASTER, RM_MAT_STK_TRANSFER_DETL  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE =     RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE   ";
                    SQL = SQL + "  AND RM_MSTD_PE_DONE ='N'";

                    // and RMRM_MSTD_REC_FLG ='Y' 

                    if (SuppCode != null)
                    {
                        SQL = SQL + " AND   RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANSPORTER_CODE  = '" + SuppCode + "'";

                    }
                    SQL = SQL + "    ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC  ";

                }

                else if (TypeID == "FPSTRANS")
                {

                    // SELECT TEH TRANSPORTER AGAINST THE VENDOR ' RECEIPT APPROVAL  // JOMY
                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE and RM_MRD_APPROVED ='N'";
                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }
                    if (SuppCode != null)
                    {// ie against the vendor select the transproter details between the period // jomy 08 0 6 2013 
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = '" + SuppCode + "'";

                    }
                    SQL = SQL + "    ORDER BY RM_VENDOR_MASTER.RM_VM_VENDOR_NAME ASC  ";

                }
                else if (TypeID == "FPSRMSOURCEAPPOVAL")
                {

                    // SELECT TEH TRANSPORTER AGAINST THE VENDOR ' RECEIPT APPROVAL  // JOMY
                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "   RM_SOURCE_MASTER. RM_SM_SOURCE_CODE   Code,  RM_SOURCE_MASTER. RM_SM_SOURCE_DESC Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS ,  RM_SOURCE_MASTER ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE and RM_MRD_APPROVED ='N'";
                    SQL = SQL + "   AND  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE  =  RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }
                    if (SuppCode != null)
                    {// ie against the vendor select the transproter details between the period // jomy 08 0 6 2013 
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = '" + SuppCode + "'";

                    }
                    SQL = SQL + "    ORDER BY RM_SOURCE_MASTER.  RM_SM_SOURCE_DESC   ASC  ";
                }

                else if (TypeID == "FPS_PURCHASE_ENTRY_SOURCE")
                {

                    // SELECT TEH TRANSPORTER AGAINST THE VENDOR ' RECEIPT APPROVAL  // JOMY
                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "   RM_SOURCE_MASTER. RM_SM_SOURCE_CODE   Code,  RM_SOURCE_MASTER. RM_SM_SOURCE_DESC Name  ";
                    SQL = SQL + "    FROM    RM_SOURCE_MASTER ";
                    SQL = SQL + "    ORDER BY RM_SOURCE_MASTER.  RM_SM_SOURCE_DESC   ASC  ";


                }


                #region "Debit Note Update Purchase entry RM"

                else if (TypeID == "DNOTEACC" || TypeID == "DNOTERMACC")
                {
                    SQL = " SELECT GL_COAM_ACCOUNT_CODE Code, GL_COAM_ACCOUNT_NAME Name,GL_COAM_COSTING_YN COSTING_YN ";
                    SQL = SQL + " FROM GL_COA_MASTER WHERE   ";
                    SQL = SQL + " GL_COAM_LEDGER_TYPE = 'SUB' AND GL_COAM_ACCOUNT_STATUS = 'ACTIVE'";
                    SQL = SQL + " ORDER BY GL_COAM_ACCOUNT_CODE ";
                }

                else if (TypeID == "DNOTERM")
                {

                    SQL = " SELECT DISTINCT RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE CODE, ";
                    SQL = SQL + " RM_RMM_RM_DESCRIPTION NAME, RM_IP_PARAMETER_VALUE ACCOUNT_CODE,GL_COAM_ACCOUNT_NAME  ACCOUNT_NAME ";
                    SQL = SQL + " FROM RM_RAWMATERIAL_MASTER, RM_RECEPIT_DETAILS, ";
                    SQL = SQL + " RM_DEFUALTS_GL_PARAMETERS,GL_COA_MASTER ";
                    SQL = SQL + " WHERE RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE = RM_RECEPIT_DETAILS.RM_RMM_RM_CODE ";
                    SQL = SQL + " and RM_DEFUALTS_GL_PARAMETERS.RM_IP_PARAMETER_VALUE = gl_coa_master.GL_COAM_ACCOUNT_CODE ";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_APPROVED = 'Y' ";
                    SQL = SQL + " AND RM_DEFUALTS_GL_PARAMETERS.RM_IP_PARAMETER_DESC = 'IRGRACCOUNT' ";
                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.AD_BR_CODE in ('" + Branch + "')";
                    }
                    if (SuppCode != null)
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = '" + SuppCode + "'";

                    }
                    SQL = SQL + "    ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC  ";

                }

                #endregion


                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dtType;
        }


        #endregion

        #region "FectchReceiptApprovalDetails -- INSERTION MODE"
        public DataSet FectchReceiptApprovalDetails ( string txtSupplierCode, string sSlectedStation, string sSelectedRM, string sSelecteSource,
            string dtpFromDate, string dtpToDate, string cmbTransType, string rdoVendorTransporter, string sExFacoryOrOnsite )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                //  SQL = " select * from ( SELECT ";
                SQL = "SELECT RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_UM_UOM_CODE  UOM_CODE,  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_FIN_FINYRID,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID,";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY, ";

                if (rdoVendorTransporter == "0")
                {

                    //if (cmbTransType == "RECEIPT")
                    //{// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE VENDOR_CODE, ";
                    //SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO_NEW ORDER_NO, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_VENDOR_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW AMOUNT_NEW ,";
                    //}
                    //else if (cmbTransType == "TOLL")
                    //{// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    //    SQL = SQL + " RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE VENDOR_CODE , ";
                    //    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO , RM_MPOM_FIN_FINYRID PO_FIN_FINYRID , ";
                    //    SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRD_VENDOR_DOC_NO  DOC_NO , ";
                    //    SQL = SQL + " RM_RECEPIT_DETAILS.RM_POD_TOLL_RATE_NEW    PRICE_NEW ,";
                    //    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOLL_AMOUNT_NEW  AMOUNT_NEW ,";
                    //}

                }
                else
                {
                    //if (cmbTransType == "RECEIPT")
                    //{// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt

                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE VENDOR_CODE , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO_NEW ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    //SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW AMOUNT_NEW ,";

                    //}
                    //else if (cmbTransType == "TOLL")
                    //{// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    //    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE VENDOR_CODE , ";
                    //    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    //    SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO  DOC_NO , ";
                    //    SQL = SQL + " RM_RECEPIT_DETAILS.RM_POD_TOLL_RATE_NEW   PRICE_NEW ,";
                    //    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOLL_AMOUNT_NEW AMOUNT_NEW ,";
                    //}
                }

                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_UNIQE_NO,RM_RECEIPT_APPL_MASTER.AD_BR_CODE,'RM' ReceiptType,  ";
                SQL = SQL + " RM_MRD_SUPP_VAT_TYPE_CODE  SUPP_VAT_TYPE ,  RM_MRM_SUPP_VAT_PERCENTAGE SUPP_VAT_PERC,";
                SQL = SQL + " RM_MRD_TRANS_VAT_TYPE_CODE TRANS_VAT_TYPE ,  RM_MRM_TRANS_VAT_PERCENTAGE TRANS_VAT_PERC ";
                SQL = SQL + " FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER,";
                SQL = SQL + " RM_VENDOR_MASTER,";
                SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER, RM_RECEIPT_APPL_MASTER ";

                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS. RM_MRD_APPROVED = 'Y'";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO = RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID = RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER. RM_MRMA_APPROVED_STATUS ='Y'";
                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE ";
                SQL = SQL + " BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                if (rdoVendorTransporter == "0")
                {
                    // VENDOR 
                    //if (cmbTransType == "RECEIPT")
                    //{// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE= '" + txtSupplierCode + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY ='N'";
                    //}
                    //else if (cmbTransType == "TOLL")// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    //{// toll purchae entry for supploer // jomy new wenhancments 10 mar 2016
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE= '" + txtSupplierCode + "'";
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_YN='N'";
                    //}


                }
                else if (rdoVendorTransporter == "1")
                { // TRANSSPORTER  <<< TWO TYPES // 1. receipt 2.  toll
                  //if (cmbTransType == "RECEIPT")
                  //{// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE= '" + txtSupplierCode + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY ='N'";
                    //}// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    //else if (cmbTransType == "TOLL")
                    //{
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE= '" + txtSupplierCode + "'";
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_YN='N'";
                    //}
                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE in('" + sSlectedStation + "')";
                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ('" + sSelectedRM + "')";
                }

                if (!string.IsNullOrEmpty(sSelecteSource))
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE IN ('" + sSelecteSource + "')";
                }

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + Branch + "'";
                }


                if (sExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + sExFacoryOrOnsite + "'";
                }


                //if (rdoVendorTransporter == "0")
                //{
                //    SQL = SQL + " order by RM_RECEPIT_DETAILS.RM_MRD_VENDOR_DOC_NO ";
                //}
                //else
                //{
                //    SQL = SQL + " order by RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO ";
                //}

                #region "Other Items"

                SQL = SQL + " union all SELECT RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                SQL = SQL + "         SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_UOM_UOM_CODE UOM_CODE,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE  SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.AD_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_SUPPLY_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE              VENDOR_CODE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO_NEW               ORDER_NO, ";
                // SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO               ORDER_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID            PO_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_NO      DOC_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE       PRICE_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT           AMOUNT_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.AD_BR_CODE ,RM_RMM_PRODUCT_TYPE ReceiptType,";
                SQL = SQL + "          ''  SUPP_VAT_TYPE ,  0  SUPP_VAT_PERC, ";
                SQL = SQL + "         RM_MRM_VAT_TYPE_CODE TRANS_VAT_TYPE ,RM_MRM_VAT_PERCENTAGE TRANS_VAT_PERC";



                SQL = SQL + "    FROM RM_RECEPIT_TOLL, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "         RM_UOM_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         RM_SOURCE_MASTER, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS ";



                SQL = SQL + "   WHERE     RM_RECEPIT_TOLL.RM_RMM_RM_CODE = ";
                SQL = SQL + "             RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE = ";
                SQL = SQL + "             RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = ";
                SQL = SQL + "             SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + "         AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVED_STATUS = 'Y' ";
                SQL = SQL + "         and RM_RMM_PRODUCT_TYPE <>'TOLL' ";
                // SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                // SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                SQL = SQL + "             RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "     AND RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO =  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ";

                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE ";
                SQL = SQL + " BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = '" + txtSupplierCode + "'";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY = 'N' ";



                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE in('" + sSlectedStation + "')";
                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_RMM_RM_CODE IN ('" + sSelectedRM + "')";
                }

                if (!string.IsNullOrEmpty(sSelecteSource))
                {
                    SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE IN ('" + sSelecteSource + "')";
                }

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + Branch + "'";
                }


                if (sExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + sExFacoryOrOnsite + "'";
                }

                SQL = SQL + "  order by ReceiptType DESC";
                //if (rdoVendorTransporter == "0")
                //{
                //    //SQL = SQL + ") order by RM_RECEPIT_DETAILS.RM_MRD_VENDOR_DOC_NO ";
                //    SQL = SQL + ")   ";
                //    SQL = SQL + ") order by DOC_NO ";
                //}
                //else
                //{
                //  //  SQL = SQL + ") order by RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO ";
                //    SQL = SQL + ") order by DOC_NO ";
                //}


                #endregion






                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        #endregion

        #region "FectchReceipt Toll ApprovalDetails -- INSERTION MODE"
        public DataSet FectchReceiptTollApprovalDetails ( string txtSupplierCode, string sSlectedStation, string sSelectedRM, string sSelecteSource,
            string dtpFromDate, string dtpToDate, string cmbTransType, string rdoVendorTransporter, string sExFacoryOrOnsite )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                #region "Toll Items"

                SQL = "  SELECT RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                SQL = SQL + "         SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_UOM_UOM_CODE UOM_CODE,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE  SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.AD_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_SUPPLY_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE              VENDOR_CODE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO_NEW               ORDER_NO, ";
                // SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO               ORDER_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID            PO_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_NO      DOC_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE       PRICE_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT           AMOUNT_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.AD_BR_CODE ,RM_RMM_PRODUCT_TYPE ReceiptType,";
                SQL = SQL + "         RM_MRM_VAT_TYPE_CODE SUPP_VAT_TYPE , RM_MRM_VAT_PERCENTAGE SUPP_VAT_PERC ";
                SQL = SQL + "    FROM RM_RECEPIT_TOLL, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "         RM_UOM_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         RM_SOURCE_MASTER, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS ";



                SQL = SQL + "   WHERE     RM_RECEPIT_TOLL.RM_RMM_RM_CODE = ";
                SQL = SQL + "             RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE = ";
                SQL = SQL + "             RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = ";
                SQL = SQL + "             SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + "         AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVED_STATUS = 'Y' ";
                SQL = SQL + "         and RM_RMM_PRODUCT_TYPE IN ( 'TOLL')  ";//'OTHER', coming Under Receipt Itself // Abin on 03 Now 2022
                                                                              // SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                                                                              //SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                SQL = SQL + "             RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "     AND RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO =  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ";
                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE ";
                SQL = SQL + " BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = '" + txtSupplierCode + "'";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY = 'N' ";



                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE in('" + sSlectedStation + "')";
                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_RMM_RM_CODE IN ('" + sSelectedRM + "')";
                }

                if (!string.IsNullOrEmpty(sSelecteSource))
                {
                    SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE IN ('" + sSelecteSource + "')";
                }

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + Branch + "'";
                }


                if (sExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + sExFacoryOrOnsite + "'";
                }

                SQL = SQL + "  order by ReceiptType DESC";



                #endregion






                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        #endregion

        #region "FectchReceiptApprovalDetails -- INSERTION MODE"
        public DataSet FectchReceiptApprovalDetailswithReceiptandToll ( string txtSupplierCode, string sSlectedStation, string sSelectedRM, string sSelecteSource,
            string dtpFromDate, string dtpToDate, string cmbTransType, string rdoVendorTransporter, string sExFacoryOrOnsite )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                //  SQL = " select * from ( SELECT ";
                SQL = "select  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_UM_UOM_CODE  UOM_CODE,  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_FIN_FINYRID,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID,";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY, ";

                if (rdoVendorTransporter == "0")
                {

                    //if (cmbTransType == "RECEIPTTOLL")
                    //{// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE VENDOR_CODE, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO_NEW ORDER_NO, ";
                    //   SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_VENDOR_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW AMOUNT_NEW ,";
                    //}
                    //else if (cmbTransType == "TOLL")// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    //{
                    //    SQL = SQL + " RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE VENDOR_CODE , ";
                    //    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO , RM_MPOM_FIN_FINYRID PO_FIN_FINYRID , ";
                    //    SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRD_VENDOR_DOC_NO  DOC_NO , ";
                    //    SQL = SQL + " RM_RECEPIT_DETAILS.RM_POD_TOLL_RATE_NEW    PRICE_NEW ,";
                    //    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOLL_AMOUNT_NEW  AMOUNT_NEW ,";
                    //}

                }
                else
                {
                    //if (cmbTransType == "RECEIPTTOLL")
                    //{// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt

                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE VENDOR_CODE , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO_NEW ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    //SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW AMOUNT_NEW ,";
                    //}
                    //else if (cmbTransType == "TOLL")
                    //{// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    //SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE VENDOR_CODE , ";
                    //SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    //SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO  DOC_NO , ";
                    //SQL = SQL + " RM_RECEPIT_DETAILS.RM_POD_TOLL_RATE_NEW   PRICE_NEW ,";
                    //SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOLL_AMOUNT_NEW AMOUNT_NEW ,";
                    // }
                }

                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_UNIQE_NO,RM_RECEIPT_APPL_MASTER.AD_BR_CODE,'RM' ReceiptType,1 SortID , ";
                SQL = SQL + " RM_MRD_SUPP_VAT_TYPE_CODE  SUPP_VAT_TYPE ,  RM_MRM_SUPP_VAT_PERCENTAGE SUPP_VAT_PERC,";
                SQL = SQL + " RM_MRD_TRANS_VAT_TYPE_CODE TRANS_VAT_TYPE ,  RM_MRM_TRANS_VAT_PERCENTAGE TRANS_VAT_PERC ";

                SQL = SQL + " FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER,";
                SQL = SQL + " RM_VENDOR_MASTER,";
                SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER, RM_RECEIPT_APPL_MASTER ";

                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS. RM_MRD_APPROVED = 'Y'";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO = RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID = RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER. RM_MRMA_APPROVED_STATUS ='Y'";
                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE ";
                SQL = SQL + " BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                if (rdoVendorTransporter == "0")
                {
                    // VENDOR 
                    //if (cmbTransType == "RECEIPTTOLL")
                    //{// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE= '" + txtSupplierCode + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY ='N'";
                    //}
                    //else if (cmbTransType == "TOLL")// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    //{// toll purchae entry for supploer // jomy new wenhancments 10 mar 2016
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE= '" + txtSupplierCode + "'";
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_YN='N'";
                    //}


                }
                else if (rdoVendorTransporter == "1")
                { // TRANSSPORTER  <<< TWO TYPES // 1. receipt 2.  toll
                  //if (cmbTransType == "RECEIPTTOLL")
                  //{// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE= '" + txtSupplierCode + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY ='N'";
                    //}
                    //else if (cmbTransType == "TOLL")
                    //{// Commenetd on 05 Nov 2022// Abin Toll Controlling Same Like Receipt
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE= '" + txtSupplierCode + "'";
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_YN='N'";
                    //}
                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE in('" + sSlectedStation + "')";
                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ('" + sSelectedRM + "')";
                }

                if (!string.IsNullOrEmpty(sSelecteSource))
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE IN ('" + sSelecteSource + "')";
                }

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + Branch + "'";
                }


                if (sExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + sExFacoryOrOnsite + "'";
                }


                //if (rdoVendorTransporter == "0")
                //{
                //    SQL = SQL + " order by RM_RECEPIT_DETAILS.RM_MRD_VENDOR_DOC_NO ";
                //}
                //else
                //{
                //    SQL = SQL + " order by RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO ";
                //}

                #region "Other Items"

                SQL = SQL + " union all SELECT RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                SQL = SQL + "         SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "        RM_RECEPIT_TOLL.RM_UOM_UOM_CODE UOM_CODE,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE  SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC,";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.AD_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_SUPPLY_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE              VENDOR_CODE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO_NEW               ORDER_NO, ";
                //SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO               ORDER_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID            PO_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_NO      DOC_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE       PRICE_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT           AMOUNT_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.AD_BR_CODE ,  RM_RMM_PRODUCT_TYPE  ReceiptType ,2 SortID,";
                SQL = SQL + "         '' SUPP_VAT_TYPE ,  0 SUPP_VAT_PERC, ";
                SQL = SQL + "         RM_MRM_VAT_TYPE_CODE  TRANS_VAT_TYPE ,  RM_MRM_VAT_PERCENTAGE TRANS_VAT_PERC";


                SQL = SQL + "    FROM RM_RECEPIT_TOLL, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "         RM_UOM_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         RM_SOURCE_MASTER, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS ";



                SQL = SQL + "   WHERE     RM_RECEPIT_TOLL.RM_RMM_RM_CODE = ";
                SQL = SQL + "             RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE = ";
                SQL = SQL + "             RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = ";
                SQL = SQL + "             SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + "         AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVED_STATUS = 'Y' ";
                //SQL = SQL + "         and RM_RMM_RM_TYPE<>'TOLL' " ;
                //     SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                // SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                SQL = SQL + "             RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "     AND RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO =  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ";
                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE ";
                SQL = SQL + " BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = '" + txtSupplierCode + "'";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY = 'N' ";



                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE in('" + sSlectedStation + "')";
                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_RMM_RM_CODE IN ('" + sSelectedRM + "')";
                }

                if (!string.IsNullOrEmpty(sSelecteSource))
                {
                    SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE IN ('" + sSelecteSource + "')";
                }

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + Branch + "'";
                }


                if (sExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + sExFacoryOrOnsite + "'";
                }

                SQL = SQL + "  order by SortID,ReceiptType,DOC_NO Asc";
                //if (rdoVendorTransporter == "0")
                //{
                //    //SQL = SQL + ") order by RM_RECEPIT_DETAILS.RM_MRD_VENDOR_DOC_NO ";
                //    SQL = SQL + ")   ";
                //    SQL = SQL + ") order by DOC_NO ";
                //}
                //else
                //{
                //  //  SQL = SQL + ") order by RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO ";
                //    SQL = SQL + ") order by DOC_NO ";
                //}


                #endregion






                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        #endregion


        #region "FectchStock Transfer Details INSERTION MODE"
        // JOMY FOR FETCHING THE STOCK TRANSFER DETAILS WHERE NOT YET DONE THE PURCHSE ENTRY 29 AUG 2013
        public DataSet FectchStockTransferDetails ( string txtSupplierCode, string sSlectedStation, string sSelectedRM, string dtpFromDate, string dtpToDate, string cmbTransType, string rdoVendorTransporter )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                cmbTransType = "TRANSFERS";


                SQL = " ";
                SQL = "    SELECT ";
                SQL = SQL + "            SA_STN_STATION_CODE_FROM SALES_STN_STATION_CODE , ";
                SQL = SQL + "            SALES_STN_STATION_NAME,  ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_UM_UOM_CODE UOM_CODE,  RM_MAT_STK_TRANSFER_DETL.RM_SM_SOURCE_CODE SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO RM_MRM_RECEIPT_NO , ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL + "            RM_MSTM_TRANSFER_DATE RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO RM_MRM_APPROVED_NO, "; // since there is not apprval same document number isusing 
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID RM_MRD_APPROVED_FINYRID, "; // since there is not apprval same document number isusing 
                SQL = SQL + "            RM_MSTM_TRANSFER_DATE   RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "            RM_MSTD_QUANTITY  RM_MRD_SUPPLY_QTY ,  ";
                SQL = SQL + "            RM_MSTD_RCV_QTY  RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "            RM_MSTD_TRANSPORTER_CODE VENDOR_CODE,  ";
                SQL = SQL + "            RM_MTRANSPO_ORDER_NO ORDER_NO, ";
                SQL = SQL + "            RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID, ";
                SQL = SQL + "            RM_MSTD_TRNSPORTER_DOC_NO DOC_NO , ";
                SQL = SQL + "            RM_MSTD_TRANS_CHARGES PRICE_NEW, ";
                SQL = SQL + "            RM_MSTD_TRANS_AMOUNT  AMOUNT_NEW, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE,   ";
                SQL = SQL + "            RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_MSTD_SL_NO   RM_MRM_RECEIPT_UNIQE_NO , 'TRANS' RECEIPTTYPE,";
                SQL = SQL + "            RM_MSTD_TRANS_VAT_TYPE_CODE TRANS_VAT_TYPE ,RM_MSTD_TRANS_VAT_PERCENTAGE TRANS_VAT_PERC ";

                SQL = SQL + "    FROM ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_MASTER, ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_DETL, ";
                SQL = SQL + "        RM_RAWMATERIAL_MASTER , ";
                SQL = SQL + "        SL_STATION_MASTER, ";
                SQL = SQL + "        RM_SOURCE_MASTER, ";
                SQL = SQL + "        RM_VENDOR_MASTER ";
                SQL = SQL + "    WHERE  ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO = RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO  ";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID = RM_MAT_STK_TRANSFER_DETL.AD_FIN_FINYRID  ";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + "        AND RM_MAT_STK_TRANSFER_DETL.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE  ";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM = SL_STATION_MASTER.SALES_STN_STATION_CODE  ";
                SQL = SQL + "        AND RM_MSTD_TRANSPORTER_CODE = RM_VM_VENDOR_CODE  ";
                SQL = SQL + "        AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_APPRV_STATUS ='Y'";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_DETL.RM_MSTD_PE_DONE ='N'";
                SQL = SQL + "   AND  RM_MSTM_TRANSFER_DATE";
                SQL = SQL + " BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "      AND RM_MSTD_TRANSPORTER_CODE= '" + txtSupplierCode + "'";

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM in('" + sSlectedStation + "')";
                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE IN ('" + sSelectedRM + "')";
                }

                SQL = SQL + " order by RM_MAT_STK_TRANSFER_DETL. RM_MSTD_TRNSPORTER_DOC_NO ";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        #endregion


        #region "checking Dupplicate invoice number "

        public string GetDuplicatedInoiceNumber ( object mngrclassobj, string sVendorInovice, string sVendorCode )
        {
            DataSet dsReturn = new DataSet("INVOICE_COUNT");
            string sReturn = string.Empty;
            try
            {


                OracleHelper oTrns = new OracleHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                OracleCommand ocCommand = new OracleCommand("PK_VENDOR_INVOICE_DUP_CHECKING.GetVendorInvoiceCount");
                ocCommand.Connection = ocConn;
                ocCommand.CommandType = CommandType.StoredProcedure;


                //ocCommand.Parameters.Add("finYer", OracleType.Number).Direction = ParameterDirection.Input;
                //ocCommand.Parameters.Add("finYer", OracleType.Number).Value = mngrclass.FinYearID;

                ////ocCommand.Parameters.Add("sVendorInovice", OracleType.Number).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("sVendorInovice", OracleType.VarChar).Value = sVendorInovice;

                ////ocCommand.Parameters.Add("sVendorCode", OracleType.Number).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("sVendorCode", OracleType.VarChar).Value = sVendorCode;

                ///==========================
                ///


                ocCommand.Parameters.Add("finYer", OracleDbType.Decimal, mngrclass.FinYearID, ParameterDirection.Input);
                ocCommand.Parameters.Add("sVendorInovice", OracleDbType.Varchar2, sVendorInovice, ParameterDirection.Input);
                ocCommand.Parameters.Add("sVendorCode", OracleDbType.Varchar2, sVendorCode, ParameterDirection.Input);
                ocCommand.Parameters.Add("cur_return", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                OracleDataAdapter odAdpt = new OracleDataAdapter(ocCommand);

                odAdpt.Fill(dsReturn);

                if (int.Parse(dsReturn.Tables[0].Rows[0]["CNT"].ToString()) > 0)
                {
                    sReturn = "Vendor Invoice Already booked, Kindly try to  book New invoice .Unable to save";
                }
                else
                {
                    sReturn = "CONTINUE";
                }
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                sReturn = "Error Occrured, Check Log File";
                return null;

            }

            return sReturn;

        }

        #endregion


        #region "DML"

        public string GetRMPEDocNumber ( string sPreFix )
        {
            DataTable dtCode = new DataTable();
            try
            {


                String strName = string.Empty;
                strName = sPreFix.Trim();


                OracleCommand ocCommand = new OracleCommand("PK_CODE_CREATION.getrmpedocnumber");


                ocCommand.Connection = ocConn;

                ocCommand.CommandType = CommandType.StoredProcedure;

                ////ocCommand.Parameters.Add("sPreFix", OracleType.VarChar, 10).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("sPreFix", OracleType.VarChar).Value = sPreFix;


                ////ocCommand.Parameters.Add("cur_return", OracleType.Cursor).Direction = ParameterDirection.Output;


                ocCommand.Parameters.Add("sPreFix", OracleDbType.Varchar2, sPreFix, ParameterDirection.Input);
                ocCommand.Parameters.Add("cur_return", OracleDbType.RefCursor).Direction = ParameterDirection.Output;


                OracleDataAdapter odAdpt = new OracleDataAdapter(ocCommand);
                odAdpt.Fill(dtCode);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return System.Convert.ToString(dtCode.Rows[0].ItemArray[0]);
        }


        public string InsertMasterSql ( PurchaseEntryEntity oEntity, List<PurchaseEntryReceiptsDet> EntityPEReceipt,
            List<PEDebitAccounts> EntityPeDebit, List<PEDebitNoteGrid> EntityPeDebitNote, bool Autogen, object mngrclassobj,
             bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber, List<PurchaseRMMatchDetailsEntity> objMatchEntity )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                //  sTransType  " V " for Vendor "R" for ReceiptAppDate " T for transfer  ( using one funtino for assign the value ) 
                //  if (rdoVendorTransporter.SelectedValue == "0")  EntryType = "V"; // vendor     EntryType = "T"; // transporter 


                sSQLArray.Clear();

                SQL = " INSERT INTO RM_PE_MASTER (";
                SQL = SQL + " RM_MPEM_ENTRY_NO, AD_FIN_FINYRID, RM_MPEM_ENTRY_DATE, ";
                SQL = SQL + " RM_MPEM_ENTRY_TYPE, RM_MPEM_TRANS_TYPE, RM_VM_VENDOR_CODE, ";
                SQL = SQL + " RM_MPEM_VENDOR_INV_NO, RM_MPEM_VENDOR_INV_DATE, RM_MPEM_GRAND_TOTAL, ";
                SQL = SQL + " RM_MPEM_DISC_AMT, RM_MPEM_STK_ADJ_AMT, RM_MPEM_INVOICE_TOTAL, ";
                SQL = SQL + " RM_MPEM_NET_TOTAL, RM_MPEM_BALANCE_TOTAL, RM_MPEM_DISC_ACC_CODE, ";
                SQL = SQL + " RM_MPEM_STKADJ_ACCCODE, RM_MPEM_REMARKS, AD_CM_COMPANY_CODE, ";
                SQL = SQL + " RM_MPEM_CREATEDBY, RM_MPEM_CREATEDDATE, RM_MPEM_UPDATEDBY, ";
                SQL = SQL + " RM_MPEM_UPDATEDDATE, RM_MPEM_DELETESTATUS, RM_MPEM_VERIFIED, ";
                SQL = SQL + " RM_MPEM_VERIFIED_BY, RM_MPEM_APPROVED, RM_MPEM_APPROVED_BY, ";
                SQL = SQL + " RM_MPEM_ALLOW_EDIT , ";
                SQL = SQL + " RM_MPEM_app_from_DATE,RM_MPEM_app_to_DATE ,";
                SQL = SQL + " RM_MPEM_VAT_PERCENTAGE,RM_MPEM_VAT_AMOUNT,RM_MPEM_VAT_ACC_CODE,RM_MPEM_VAT_TYPE_CODE ,";
                SQL = SQL + " RM_MPEM_VAT_OUTPUT_ACC_CODE,RM_MPEM_TAX_APPLICABLE_AMOUNT,RM_MPEM_NON_TAX_APPL_AMOUNT,";
                SQL = SQL + " RM_MPEM_VAT_RC_AMOUNT, AD_BR_CODE,RM_MPEM_INV_RECIVED_DATE,";
                SQL = SQL + "   RM_PE_VAT_AMT_ADV_MATCHED,RM_PE_ADVANCE_MATCHED_AMT,RM_MPEM_PE_PO_NO ";
                SQL = SQL + "  ) ";
                SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "' , " + mngrclass.FinYearID + ", '" + Convert.ToDateTime(oEntity.dtpTransactionDate).ToString("dd-MMM-yyyy") + "',";
                SQL = SQL + "'" + oEntity.EntryType + "','" + oEntity.sTransType + "' , '" + oEntity.txtSupplierCode + "' ,";
                SQL = SQL + "'" + oEntity.txtInvNo + "' ,'" + Convert.ToDateTime(oEntity.dtpInvDate).ToString("dd-MMM-yyyy") + "' , " + Convert.ToDouble(oEntity.TxtGrndTotal) + ",";
                SQL = SQL + "" + Convert.ToDouble(oEntity.txtDiscAmt) + "," + Convert.ToDouble(oEntity.TxtStockAdjAmount) + " ," + Convert.ToDouble(oEntity.txtInvTotal) + " ,";
                SQL = SQL + "" + Convert.ToDouble(oEntity.TxtNetTotal) + " ," + Convert.ToDouble(oEntity.TxtNetTotal) + " ,'" + oEntity.txtDiscAcc + "' ,";
                SQL = SQL + "'" + oEntity.txtStkAdjAcCode + "' , '" + oEntity.txtRemarks + "', '" + mngrclass.CompanyCode + "',";
                SQL = SQL + "'" + mngrclass.UserName + "' , TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),'' ,";
                SQL = SQL + "'', 0,'" + oEntity.varify + "' ,";
                SQL = SQL + "'" + oEntity.txtVerifiedBy + "' , '" + oEntity.approve + "','" + oEntity.txtApprovedBy + "' ,'N',";
                SQL = SQL + "'" + Convert.ToDateTime(oEntity.dtpReceiptAppFromDate).ToString("dd-MMM-yyyy") + "' ,'" + Convert.ToDateTime(oEntity.dtpReceiptAppToDate).ToString("dd-MMM-yyyy") + "',";
                SQL = SQL + " " + oEntity.TaxPerc + " , " + oEntity.TaxAmount + ", '" + oEntity.TaxAccountCode + "', '" + oEntity.TaxVatType + "' ,";
                SQL = SQL + "   '" + oEntity.TaxAccountCodeOutput + "', '" + oEntity.txtTaxableAmount + "' ,";
                SQL = SQL + " " + oEntity.txtNonTaxableAmount + "," + oEntity.TaxRcAmount + " ,'" + oEntity.Branch + "',";
                SQL = SQL + " '" + Convert.ToDateTime(oEntity.ReceivedDate).ToString("dd-MMM-yyyy") + "', ";

                SQL = SQL + "  " + oEntity.AdvanceMatchedVATAmount + " ," + oEntity.AdvanceMatchedAmount + ",'" + oEntity.txtPoRefNo+ "'  ";
                SQL = SQL + " )";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertPEDetails(oEntity, EntityPEReceipt, EntityPeDebit, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                sRetun = InsertPEDebitNoteDetails(oEntity.txtEntryNo, mngrclass.FinYearID, EntityPeDebitNote, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }
                sRetun = InsertUpdateAdvanceMatchingDtlQrs(oEntity.txtEntryNo, mngrclass.FinYearID, objMatchEntity, "N", mngrclass);

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }



                sRetun = string.Empty;
                if (oEntity.approve == "Y")
                {
                    sRetun = InsertApprovalQrs(oEntity, mngrclassobj);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }



                sRetun = string.Empty;
                // SINCE WE ARE USING THE INSERTION  DOCUMENT TYPE IS GL SHOUBLE BE PASSED THE GL TAG -- JOMY 15 - JAN -2015  DICUSSED WITH JOJIMON // 
                // again changed jomy based on praveen GLMISCPUR

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMPE", oEntity.txtEntryNo, Autogen, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);

                return sRetun;
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return sRetun;
        }

        private string InsertApprovalQrs ( PurchaseEntryEntity oEntity, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " INSERT INTO RM_PE_MASTER_TRIGGER (";
                SQL = SQL + " RM_MPEM_ENTRY_NO, AD_FIN_FINYRID, RM_MPEM_ENTRY_DATE, ";
                SQL = SQL + " RM_MPEM_ENTRY_TYPE, RM_MPEM_TRANS_TYPE, RM_MPEM_APPROVED_BY) ";
                SQL = SQL + " VALUES ( '" + oEntity.txtEntryNo + "', " + mngrclass.FinYearID + ",'" + Convert.ToDateTime(oEntity.dtpTransactionDate).ToString("dd-MMM-yyyy") + "' ,";
                SQL = SQL + "'" + oEntity.EntryType + "','" + oEntity.sTransType + "' ,'" + oEntity.txtApprovedBy + "' )";

                sSQLArray.Add(SQL);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);

                return sRetun;
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return "CONTINUE";
        }

        private string InsertPEDetails ( PurchaseEntryEntity oEntity, List<PurchaseEntryReceiptsDet> EntityPEReceipt, List<PEDebitAccounts> etyPeDebit, object mngrclassobj )
        {
            string sRetun = string.Empty;

            int i;
            //object oChk = null;
            try
            {

                SessionManager mngrclass = (SessionManager)mngrclassobj;
                //<< inserting recipt details

                //  sTransType  " V " for Vendor  //
                //  "R" for TRASPORTERE Receipt  " //
                //  "TOLL" for TRASPORTERE  TOLL  " //
                //   T for transfer  ( using one funtino for assign the value ) 


                {
                    int k = 1;
                    foreach (var Data in EntityPEReceipt)
                    {
                        SQL = " INSERT INTO RM_PE_DETAILS (";
                        SQL = SQL + " RM_MPEM_ENTRY_NO, AD_FIN_FINYRID, RM_MPEM_ENTRY_TYPE,RM_MPEM_ENTRY_SLNO, ";
                        SQL = SQL + " RM_MPEM_TRANS_TYPE, RM_MRM_RECEIPT_UNIQE_NO, RM_MRM_RECEIPT_NO, ";
                        SQL = SQL + " AD_FIN_FINYRID_RECEIPT, RM_MRMA_APPROVAL_NO, AD_FIN_FINYRID_APPROVAL, ";
                        SQL = SQL + " RM_MPEM_APPROVED_STATUS, RM_MPEM_ALLOW_EDIT,RM_MPED_RECEPT_TRANS_TYPE,RM_MPED_VENDOR_DOC_NO, ";

                        SQL = SQL + " RM_MRM_RECEIPT_DATE ,RM_MRMA_APPROVALTRANS_DATE  ,RM_PO_PONO ,   ";
                        SQL = SQL + " RM_PO_PONO_FINYRID,SALES_STN_STATION_CODE , RM_UM_UOM_CODE   , ";
                        SQL = SQL + " RM_SM_SOURCE_CODE,RM_RMM_RM_CODE  , RM_MRD_SUPPLY_QTY, ";
                        SQL = SQL + " RM_MPED_QTY_APPROVED ,RM_MPED_RATE  ,RM_MPED_AMOUNT  , ";
                        SQL = SQL + " RM_MPED_STK_ADJ_AMT  ,RM_MPED_DISC_AMT,RM_MPED_VAT_TYPE_CODE, ";
                        SQL = SQL + " RM_MPED_VAT_PERCENTAGE ,RM_MPED_VAT_AMOUNT ,RM_MPED_VAT_RC_AMOUNT , ";
                        SQL = SQL + " RM_MPED_TOTAL_AMOUNT) ";

                        SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "' ," + mngrclass.FinYearID + ",'" + oEntity.EntryType + "' ,'" + k + "',";
                        SQL = SQL + "'" + oEntity.sTransType + "' ," + Data.dRctUniquenoPEReceiptsDet + " , '" + Data.oReceiptNoPEReceiptsDet + "' ,";
                        SQL = SQL + "" + Data.iReceiptFinYrPEReceiptsDet + " ,'" + Data.oRctAppNoPEReceiptsDet + "' ," + Data.iRctAppFinYrPEReceiptsDet + " ,";
                        SQL = SQL + "'" + oEntity.approve + "', 'N'  ,'" + Data.sReceiptType + "','" + Data.sSuppDocNo + "', ";

                        SQL = SQL + "'" + Convert.ToDateTime(Data.ReceiptAppDate).ToString("dd-MMM-yyyy") + "','" + Convert.ToDateTime(Data.ReceiptApptTransDate).ToString("dd-MMM-yyyy") + "','" + Data.sPONo + "' ,";
                        SQL = SQL + "'" + Data.sPONoFinYR + " ','" + Data.sStationCode + "' ,'" + Data.sUOMCode + "' ,";
                        SQL = SQL + "'" + Data.sSourceCode + "' ,'" + Data.sRMCode + "' ," + Data.sSupplyQTY + " ,";
                        SQL = SQL + "" + Data.sPEQtyApprved + " ," + Data.sPERate + " ," + Data.sPEAmount + " ,";
                        SQL = SQL + "" + Data.sPEStkAdjAmount + " ," + Data.sPEDiscAmount + " ,'" + Data.sVatType + "' ,";
                        SQL = SQL + "'" + Data.sVatRate + "' ," + Data.sPEVatAmount + " ," + Data.sPERCVatAmount + " ,";
                        SQL = SQL + "" + Data.sPETotalAmount + "";

                        SQL = SQL + " )";

                        sSQLArray.Add(SQL);

                        k++;

                        if (Data.sReceiptType == "RM")
                        {
                            //-------------- RECEIIPT  UPDATION BLOCK 
                            if (oEntity.EntryType == "V")
                            {// VENDOR 

                                if (oEntity.sTransType == "V")
                                {
                                    SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                                    SQL = SQL + " RM_MRD_SUPP_ENTRY ='Y', RM_MRD_SUPP_PENO='" + oEntity.txtEntryNo + "' , RM_MPED_VFINYR_ID =" + mngrclass.FinYearID + "";

                                    SQL = SQL + " WHERE ";
                                    SQL = SQL + " RM_MRM_RECEIPT_UNIQE_NO = " + Data.dRctUniquenoPEReceiptsDet + " AND RM_MRM_RECEIPT_NO = '" + Data.oReceiptNoPEReceiptsDet + "' AND AD_FIN_FINYRID= " + Data.iReceiptFinYrPEReceiptsDet + "";

                                    sSQLArray.Add(SQL);
                                }
                                //else if (oEntity.sTransType == "TOLLVENDOR")
                                //{
                                //    SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                                //    SQL = SQL + "  RM_MRD_TOLL_PE_ENTRY_YN  ='Y',   RM_MRD_TOLL_PE_ENTRY_NO = '" + oEntity.txtEntryNo + "' , RM_MRD_TOLL_PE_ENTRY_FINYR_ID =" + mngrclass.FinYearID + "";

                                //    SQL = SQL + " WHERE ";
                                //    SQL = SQL + " RM_MRM_RECEIPT_UNIQE_NO = " + Data.dRctUniquenoPEReceiptsDet + "";
                                //    SQL = SQL + " AND RM_MRM_RECEIPT_NO = '" + Data.oReceiptNoPEReceiptsDet + "'";
                                //    SQL = SQL + " AND AD_FIN_FINYRID= " + Data.iReceiptFinYrPEReceiptsDet + "";

                                //    sSQLArray.Add(SQL);
                                //}// old Cocept // Commented By Abin if It is toll not considering As Data.sReceiptType == "RM"
                                else if (oEntity.sTransType == "RECEIPTTOLLVEND")
                                {
                                    SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                                    SQL = SQL + " RM_MRD_SUPP_ENTRY ='Y', RM_MRD_SUPP_PENO='" + oEntity.txtEntryNo + "' , RM_MPED_VFINYR_ID =" + mngrclass.FinYearID + "";

                                    SQL = SQL + " WHERE ";
                                    SQL = SQL + " RM_MRM_RECEIPT_UNIQE_NO = " + Data.dRctUniquenoPEReceiptsDet + " AND RM_MRM_RECEIPT_NO = '" + Data.oReceiptNoPEReceiptsDet + "' AND AD_FIN_FINYRID= " + Data.iReceiptFinYrPEReceiptsDet + "";

                                    sSQLArray.Add(SQL);
                                }

                            }
                            else if (oEntity.EntryType == "T")
                            {

                                if (oEntity.sTransType == "R")
                                { //< tRANSPORTER  RECEIPT 
                                    SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                                    SQL = SQL + " RM_MRD_TRANS_ENTRY ='Y', RM_MRD_TRANS_PENO='" + oEntity.txtEntryNo + "' , RM_MPED_TFINYR_ID =" + mngrclass.FinYearID + "";

                                    SQL = SQL + " WHERE ";
                                    SQL = SQL + " RM_MRM_RECEIPT_UNIQE_NO = " + Data.dRctUniquenoPEReceiptsDet + " AND RM_MRM_RECEIPT_NO = '" + Data.oReceiptNoPEReceiptsDet + "' AND AD_FIN_FINYRID= " + Data.iReceiptFinYrPEReceiptsDet + "";

                                    sSQLArray.Add(SQL);
                                }
                                else if (oEntity.sTransType == "RECEIPTTOLLTRANSPORTER")
                                { //< tRANSPORTER  RECEIPT 
                                    SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                                    SQL = SQL + " RM_MRD_TRANS_ENTRY ='Y', RM_MRD_TRANS_PENO='" + oEntity.txtEntryNo + "' , RM_MPED_TFINYR_ID =" + mngrclass.FinYearID + "";

                                    SQL = SQL + " WHERE ";
                                    SQL = SQL + " RM_MRM_RECEIPT_UNIQE_NO = " + Data.dRctUniquenoPEReceiptsDet + " AND RM_MRM_RECEIPT_NO = '" + Data.oReceiptNoPEReceiptsDet + "' AND AD_FIN_FINYRID= " + Data.iReceiptFinYrPEReceiptsDet + "";

                                    sSQLArray.Add(SQL);
                                }
                                ////else if (oEntity.sTransType == "TOLL")
                                ////{ //<  TOLL  transporter 
                                ////    SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                                ////    SQL = SQL + "  RM_MRD_TOLL_PE_ENTRY_YN  ='Y',   RM_MRD_TOLL_PE_ENTRY_NO = '" + oEntity.txtEntryNo + "' , RM_MRD_TOLL_PE_ENTRY_FINYR_ID =" + mngrclass.FinYearID + "";

                                ////    SQL = SQL + " WHERE ";
                                ////    SQL = SQL + " RM_MRM_RECEIPT_UNIQE_NO = " + Data.dRctUniquenoPEReceiptsDet + "";
                                ////    SQL = SQL + " AND RM_MRM_RECEIPT_NO = '" + Data.oReceiptNoPEReceiptsDet + "'";
                                ////    SQL = SQL + " AND AD_FIN_FINYRID= " + Data.iReceiptFinYrPEReceiptsDet + "";

                                ////    sSQLArray.Add(SQL);
                                ////}// old Cocept // Commented By Abin if It is toll not considering As Data.sReceiptType == "RM"
                                else if (oEntity.sTransType == "T")
                                {
                                    //< sTOCK TRANSFER
                                    SQL = " UPDATE RM_MAT_STK_TRANSFER_DETL SET ";
                                    SQL = SQL + " RM_MSTD_PE_DONE ='Y', RM_MSTD_PE_NO='" + oEntity.txtEntryNo + "' , RM_MSTD_PE_FINYR =" + mngrclass.FinYearID + "";

                                    SQL = SQL + " WHERE ";
                                    SQL = SQL + " RM_MSTD_SL_NO = " + Data.dRctUniquenoPEReceiptsDet + " AND RM_MSTM_TRANSFER_NO = '" + Data.oReceiptNoPEReceiptsDet + "' AND AD_FIN_FINYRID= " + Data.iReceiptFinYrPEReceiptsDet + "";

                                    sSQLArray.Add(SQL);

                                }
                            }
                            //-----------------------------------
                        }
                        else
                        {
                            SQL = " UPDATE RM_RECEPIT_TOLL SET ";
                            SQL = SQL + " RM_MRD_SUPP_ENTRY ='Y', RM_MRD_SUPP_PENO='" + oEntity.txtEntryNo + "' , RM_MPED_VFINYR_ID =" + mngrclass.FinYearID + "";

                            SQL = SQL + " WHERE ";
                            SQL = SQL + " RM_MRM_TOLL_SLNO = " + Data.dRctUniquenoPEReceiptsDet + " AND RM_MRM_RECEIPT_NO = '" + Data.oReceiptNoPEReceiptsDet + "' AND AD_FIN_FINYRID= " + Data.iReceiptFinYrPEReceiptsDet + "";

                            sSQLArray.Add(SQL);
                        }

                    }
                }

                foreach (var Data in etyPeDebit)
                {
                    SQL = " INSERT INTO RM_PE_MASTER_DEBIT_ACCOUNTS ( ";
                    SQL = SQL + "   RM_MPEM_ENTRY_NO, AD_FIN_FINYRID, RM_MPEM_SL_NO,  ";
                    SQL = SQL + "   RM_MPEM_DEBIT_REMARKS, RM_MPEM_DEBIT_ACCCODE,";

                    SQL = SQL + "  RM_MPEM_DEBIT_AMOUNT,RM_MPED_VAT_TYPE_CODE , RM_MPED_VAT_PERCENTAGE,";
                    SQL = SQL + " RM_MPED_VAT_AMOUNT  ,RM_MPED_VAT_RC_AMOUNT,";
                    SQL = SQL + "  RM_MPED_APP_TOTAL_AMOUNT ";
                    SQL = SQL + "  )  ";

                    SQL = SQL + "VALUES (  '" + oEntity.txtEntryNo + "' , " + mngrclass.FinYearID + " , " + Data.dSlNo + " , ";
                    SQL = SQL + "'" + Data.sAccountRemarks + "'   , '" + Data.sAccountCode + "' ,";

                    SQL = SQL + "  " + Data.dDebitAmounts + " ,'" + Data.dDebitAccVatType + "' ,'" + Data.dDebitAccVatRate + "' , ";
                    SQL = SQL + "  " + Data.dDebitAccVatAmount + " ," + Data.dDebitAccRCVatAmount + "  , ";
                    SQL = SQL + "  " + Data.dTotalDebitAmount + " ) ";

                    sSQLArray.Add(SQL);

                }
            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
                return sRetun;
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return "CONTINUE";
        }


        #region Accrual Matching SQL

        public string InsertUpdateAdvanceMatchingDtlQrs ( string sDocNo, int EntryFinYearId, List<PurchaseRMMatchDetailsEntity> objMatchEntity, string sAPPROVAL, object mngrclassobj )
        {
            try
            {
                int SlNo = 0;
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                foreach (var Data in objMatchEntity)
                {
                    ++SlNo;
                    SQL = "  INSERT INTO PR_MATCH_PAY_ADVNCE_INVOICES  ";
                    SQL = SQL + " (  ";
                    SQL = SQL + "  AD_CM_COMPANY_CODE, AD_FIN_FINYRID, GL_MPI_DOC_NO,  ";
                    SQL = SQL + "   GL_MPI_DOC_TYPE, GL_MPI_SL_NO, GL_MPI_ENTRY_NO,  ";
                    SQL = SQL + "   GL_MPI_VOUCHER_FINYRID, GL_MPI_ENTRY_TYPE,  ";
                    SQL = SQL + "   GL_MPI_MATCH_AMOUNT, GL_MPI_VAT_AMOUNT, GL_MPI_VAT_PERCENTAGE,  ";
                    SQL = SQL + "     AD_BR_CODE ,GL_MPI_BAL_UPDATED   ";
                    SQL = SQL + " ) VALUES  ";

                    SQL = SQL + "       ('" + mngrclass.CompanyCode + "'," + EntryFinYearId + ",'" + sDocNo + "',";
                    SQL = SQL + "              'RTMPE'," + SlNo + ",'" + Data.oVoucherNo + "',";
                    SQL = SQL + "  " + Data.oVoucherFinYr + ",'" + Data.oVoucherType + "',";
                    SQL = SQL + "" + Data.dMatchAmt + "," + Data.dMatchedVATAmount + ", " + Data.VATpercentage + ",";
                    SQL = SQL + "'" + Data.VoucheBranchCode + "',";
                    SQL = SQL + " '" + sAPPROVAL + "'";

                    SQL = SQL + ")";

                    sSQLArray.Add(SQL);

                }


            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return "ERROR WHILE CREATING THE MATCHING QUERY' CHECK LOG FILE";
            }
            finally
            {

                //cCollection = null;
                //objBase = null;
            }

            return "CONTINUE";
        }


        #endregion

        private string InsertPEDebitNoteDetails ( string sPEno, int iPEFinYrID, List<PEDebitNoteGrid> oGridEntity, object mngrclassobj )
        {
            string sRetun = string.Empty;

            int i;
            //object oChk = null;
            try
            {

                SessionManager mngrclass = (SessionManager)mngrclassobj;
                //<< inserting recipt details
                {



                    foreach (var Data in oGridEntity)
                    {
                        SQL = " INSERT INTO RM_PE_MASTER_DEBITNOTE ( ";
                        SQL = SQL + " RM_MPEM_ENTRY_NO, AD_FIN_FINYRID, RM_MPEM_SL_NO,  ";
                        SQL = SQL + " RM_MPEM_UPDATESTOCK_YN, RM_MPEM_DEBITNOTE_NO, RM_MPEM_REF_CODE,  ";
                        SQL = SQL + " RM_RMM_RM_CODE, RM_MPEM_RM_ACCCODE, RM_MPEM_DEBIT_ACCCODE,  ";
                        SQL = SQL + " RM_MPEM_NARRATION, RM_MPEM_GRAND_TOTAL_AMOUNT, RM_MPEM_VAT_TYPE,  ";
                        SQL = SQL + " RM_MPEM_VAT_RATE, RM_MPEM_VAT_AMOUNT, RM_MPEM_NET_TOTAL_AMOUNT)  ";
                        SQL = SQL + " VALUES ('" + sPEno + "'," + iPEFinYrID + "," + Data.dSlNo + ", ";
                        SQL = SQL + " '" + Data.sUpdateStockYN + "','" + Data.sSuppDebitnoteNo + "','" + Data.sRefNo + "', ";
                        SQL = SQL + " '" + Data.sRMCode + "','" + Data.sRMAccCode + "','" + Data.sAccCode + "', ";
                        SQL = SQL + " '" + Data.sNarration + "'," + Data.dNetAmount + ",'" + Data.sVatType + "', ";
                        SQL = SQL + " " + Data.dVatRate + "," + Data.dVatAmount + "," + Data.dTotalAmount + ") ";

                        sSQLArray.Add(SQL);

                    }
                }
            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
                return sRetun;
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return "CONTINUE";
        }

        public string ModifySql ( PurchaseEntryEntity oEntity, List<PurchaseEntryReceiptsDet> EntityPEReceipt,
            List<PEDebitAccounts> EntityPeDebit, List<PEDebitNoteGrid> EntityPeDebitNote, object mngrclassobj,
            List<PurchaseRMMatchDetailsEntity> objMatchEntity, int hdEntryFinYearId )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                //  sTransType  " V " for Vendor "R" for ReceiptAppDate " T for transfer  ( using one funtino for assign the value ) 
                //  if (rdoVendorTransporter.SelectedValue == "0")  EntryType = "V"; // vendor     EntryType = "T"; // transporter 


                sSQLArray.Clear();

                SQL = " UPDATE RM_PE_MASTER SET ";
                SQL = SQL + " RM_MPEM_ENTRY_DATE= '" + Convert.ToDateTime(oEntity.dtpTransactionDate).ToString("dd-MMM-yyyy") + "', ";

                SQL = SQL + " RM_MPEM_VENDOR_INV_NO = '" + oEntity.txtInvNo + "', RM_MPEM_VENDOR_INV_DATE ='" + Convert.ToDateTime(oEntity.dtpInvDate).ToString("dd-MMM-yyyy") + "', RM_MPEM_GRAND_TOTAL =" + Convert.ToDouble(oEntity.TxtGrndTotal) + ", ";
                SQL = SQL + "  RM_MPEM_INV_RECIVED_DATE ='" + Convert.ToDateTime(oEntity.ReceivedDate).ToString("dd-MMM-yyyy") + "'  ,  ";
                SQL = SQL + " RM_MPEM_DISC_AMT =" + Convert.ToDouble(oEntity.txtDiscAmt) + ", RM_MPEM_STK_ADJ_AMT =" + Convert.ToDouble(oEntity.TxtStockAdjAmount) + ", RM_MPEM_INVOICE_TOTAL =" + Convert.ToDouble(oEntity.txtInvTotal) + " , ";
                SQL = SQL + " RM_MPEM_NET_TOTAL=" + Convert.ToDouble(oEntity.TxtNetTotal) + " , RM_MPEM_BALANCE_TOTAL =" + Convert.ToDouble(oEntity.TxtNetTotal) + " , RM_MPEM_DISC_ACC_CODE ='" + oEntity.txtDiscAcc + "', ";
                SQL = SQL + " RM_MPEM_STKADJ_ACCCODE ='" + oEntity.txtStkAdjAcCode + "' , RM_MPEM_REMARKS ='" + oEntity.txtRemarks + "', ";
                SQL = SQL + " RM_MPEM_UPDATEDBY ='" + mngrclass.UserName + "', ";
                SQL = SQL + " RM_MPEM_UPDATEDDATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM') , RM_MPEM_VERIFIED ='" + oEntity.varify + "', ";
                SQL = SQL + " RM_MPEM_VERIFIED_BY ='" + oEntity.txtVerifiedBy + "', RM_MPEM_APPROVED ='" + oEntity.approve + "', RM_MPEM_APPROVED_BY ='" + oEntity.txtApprovedBy + "', ";
                SQL = SQL + " RM_MPEM_ALLOW_EDIT ='N' ,";

                SQL = SQL + "  RM_PE_VAT_AMT_ADV_MATCHED= " + oEntity.AdvanceMatchedVATAmount + ",RM_PE_ADVANCE_MATCHED_AMT = " + oEntity.AdvanceMatchedAmount + " , ";

                SQL = SQL + " RM_MPEM_VAT_PERCENTAGE= " + oEntity.TaxPerc + " ,RM_MPEM_VAT_AMOUNT=" + oEntity.TaxAmount + ",RM_MPEM_VAT_ACC_CODE='" + oEntity.TaxAccountCode + "',RM_MPEM_VAT_TYPE_CODE= '" + oEntity.TaxVatType + "', ";
                SQL = SQL + " RM_MPEM_VAT_OUTPUT_ACC_CODE= '" + oEntity.TaxAccountCodeOutput + "' ,RM_MPEM_TAX_APPLICABLE_AMOUNT=" + oEntity.txtTaxableAmount + " , RM_MPEM_NON_TAX_APPL_AMOUNT=" + oEntity.txtNonTaxableAmount + " , RM_MPEM_VAT_RC_AMOUNT=" + oEntity.TaxRcAmount + " ,";
                SQL = SQL + " RM_MPEM_PE_PO_NO='" + oEntity.txtPoRefNo + "' ";
                SQL = SQL + " WHERE RM_MPEM_ENTRY_NO = '" + oEntity.txtEntryNo + "' AND AD_FIN_FINYRID = " + hdEntryFinYearId + "";
                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_PE_DETAILS ";
                SQL = SQL + " WHERE RM_MPEM_ENTRY_NO = '" + oEntity.txtEntryNo + "' AND AD_FIN_FINYRID = " + hdEntryFinYearId + "";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_PE_MASTER_DEBITNOTE ";
                SQL = SQL + " WHERE RM_MPEM_ENTRY_NO = '" + oEntity.txtEntryNo + "' AND AD_FIN_FINYRID = " + hdEntryFinYearId + "";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_PE_MASTER_DEBIT_ACCOUNTS   ";
                SQL = SQL + " WHERE RM_MPEM_ENTRY_NO = '" + oEntity.txtEntryNo + "' AND AD_FIN_FINYRID = " + hdEntryFinYearId + "";

                sSQLArray.Add(SQL);


                SQL = " DELETE FROM PR_MATCH_PAY_ADVNCE_INVOICES";
                SQL = SQL + "  WHERE gl_mpi_doc_no = '" + oEntity.txtEntryNo + "'";
                SQL = SQL + "   AND gl_mpi_doc_type = 'RTMPE'";
                SQL = SQL + "   AND ad_fin_finyrid = " + hdEntryFinYearId + "";
                sSQLArray.Add(SQL);



                sRetun = string.Empty;

                if (oEntity.EntryType == "V")
                {
                    //if (oEntity.sTransType == "V")
                    //{
                    SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                    SQL = SQL + " RM_MRD_SUPP_ENTRY ='N', RM_MRD_SUPP_PENO= NULL, RM_MPED_VFINYR_ID = 0 ";

                    SQL = SQL + " WHERE ";
                    SQL = SQL + "  RM_MRD_SUPP_PENO='" + oEntity.txtEntryNo + "'";
                    SQL = SQL + " AND RM_MPED_VFINYR_ID = " + hdEntryFinYearId + "";

                    sSQLArray.Add(SQL);
                    //}
                    //else if (oEntity.sTransType == "TOLLVENDOR")
                    //{
                    //    SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                    //    SQL = SQL + " RM_MRD_TOLL_PE_ENTRY_YN  ='N', RM_MRD_TOLL_PE_ENTRY_NO= NULL , RM_MRD_TOLL_PE_ENTRY_FINYR_ID =0 ";

                    //    SQL = SQL + " WHERE ";
                    //    SQL = SQL + "  RM_MRD_TOLL_PE_ENTRY_NO='" + oEntity.txtEntryNo + "'";
                    //    SQL = SQL + " AND RM_MRD_TOLL_PE_ENTRY_FINYR_ID	 =" + mngrclass.FinYearID + "";

                    //    sSQLArray.Add(SQL);// old Cocept // Commented By Abin if It is toll not considering As Data.sReceiptType == "RM"
                    //   }

                }
                else if (oEntity.EntryType == "T")
                {
                    //< tRANSPORTER
                    if (oEntity.sTransType == "R")
                    {
                        SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                        SQL = SQL + " RM_MRD_TRANS_ENTRY ='N', RM_MRD_TRANS_PENO= NULL , RM_MPED_TFINYR_ID =0 ";

                        SQL = SQL + " WHERE ";
                        SQL = SQL + "  RM_MRD_TRANS_PENO='" + oEntity.txtEntryNo + "'";
                        SQL = SQL + " AND RM_MPED_TFINYR_ID =" + hdEntryFinYearId + "";

                        sSQLArray.Add(SQL);
                    }

                    if (oEntity.sTransType == "RECEIPTTOLLTRANSPORTER")
                    {
                        SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                        SQL = SQL + " RM_MRD_TRANS_ENTRY ='N', RM_MRD_TRANS_PENO= NULL , RM_MPED_TFINYR_ID =0 ";

                        SQL = SQL + " WHERE ";
                        SQL = SQL + "  RM_MRD_TRANS_PENO='" + oEntity.txtEntryNo + "'";
                        SQL = SQL + " AND RM_MPED_TFINYR_ID =" + hdEntryFinYearId + "";

                        sSQLArray.Add(SQL);
                    }
                    //else if (oEntity.sTransType == "TOLL")
                    //{
                    //    SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                    //    SQL = SQL + " RM_MRD_TOLL_PE_ENTRY_YN  ='N', RM_MRD_TOLL_PE_ENTRY_NO= NULL , RM_MRD_TOLL_PE_ENTRY_FINYR_ID =0 ";

                    //    SQL = SQL + " WHERE ";
                    //    SQL = SQL + "  RM_MRD_TOLL_PE_ENTRY_NO='" + oEntity.txtEntryNo + "'";
                    //    SQL = SQL + " AND RM_MRD_TOLL_PE_ENTRY_FINYR_ID	 =" + mngrclass.FinYearID + "";

                    //    sSQLArray.Add(SQL);
                    //}// old Cocept // Commented By Abin if It is toll not considering As Data.sReceiptType == "RM"
                    else if (oEntity.sTransType == "T")
                    {
                        //< sTOCK TRANSFER

                        SQL = " UPDATE RM_MAT_STK_TRANSFER_DETL SET ";
                        SQL = SQL + " RM_MSTD_PE_DONE ='N', RM_MSTD_PE_NO='' , RM_MSTD_PE_FINYR =0";

                        SQL = SQL + " WHERE ";

                        SQL = SQL + "  RM_MSTD_PE_NO='" + oEntity.txtEntryNo + "'";
                        SQL = SQL + " AND RM_MSTD_PE_FINYR =" + hdEntryFinYearId + "";

                        sSQLArray.Add(SQL);

                    }
                }

                SQL = " UPDATE RM_RECEPIT_TOLL SET ";
                SQL = SQL + " RM_MRD_SUPP_ENTRY ='N', RM_MRD_SUPP_PENO= NULL, RM_MPED_VFINYR_ID = 0 ";

                SQL = SQL + " WHERE ";
                SQL = SQL + "  RM_MRD_SUPP_PENO='" + oEntity.txtEntryNo + "'";
                SQL = SQL + " AND RM_MPED_VFINYR_ID = " + hdEntryFinYearId + "";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertPEDetails(oEntity, EntityPEReceipt, EntityPeDebit, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                sRetun = InsertPEDebitNoteDetails(oEntity.txtEntryNo, hdEntryFinYearId, EntityPeDebitNote, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }
                sRetun = string.Empty;

                sRetun = InsertUpdateAdvanceMatchingDtlQrs(oEntity.txtEntryNo, hdEntryFinYearId, objMatchEntity, "N", mngrclass);

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }
                sRetun = string.Empty;

                if (oEntity.approve == "Y")
                {
                    sRetun = InsertApprovalQrs(oEntity, mngrclassobj);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMPE", oEntity.txtEntryNo, false, Environment.MachineName, "U", sSQLArray);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);

                return sRetun;
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return sRetun;
        }


        public string DeleteSql ( string txtEntryNo, string sTransType, string EntryType, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                //  sTransType  " V " for Vendor "R" for ReceiptAppDate " T for transfer  ( using one funtino for assign the value ) 
                //  if (rdoVendorTransporter.SelectedValue == "0")  EntryType = "V"; // vendor     EntryType = "T"; // transporter 

                SQL = " DELETE FROM   RM_PE_MASTER_DEBIT_ACCOUNTS ";
                SQL = SQL + " WHERE RM_MPEM_ENTRY_NO='" + txtEntryNo + "'";
                SQL = SQL + "  AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";
                sSQLArray.Add(SQL);


                SQL = " DELETE FROM RM_PE_ATTACHMENT_DETAILS";
                SQL = SQL + " WHERE RM_MPEM_ENTRY_NO='" + txtEntryNo + "'";
                SQL = SQL + "  AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";
                sSQLArray.Add(SQL);



                SQL = " DELETE from RM_PE_MASTER ";
                SQL = SQL + " WHERE RM_MPEM_ENTRY_NO = '" + txtEntryNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_PE_DETAILS ";
                SQL = SQL + " WHERE RM_MPEM_ENTRY_NO = '" + txtEntryNo + "' AND AD_FIN_FINYRID =" + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;
                //  sTransType  " V " for Vendor  //
                //  "R" for TRASPORTERE Receipt  " //
                //  "TOLL" for TRASPORTERE  TOLL  " //
                //   T for transfer  ( using one funtino for assign the value ) 

                ////if (rdoVendorTransporter.SelectedValue == "0")
                ////{
                ////    EntryType = "V"; // vendor 
                ////}
                ////else
                ////{
                ////    EntryType = "T"; // transporter 
                ////}

                if (EntryType == "V")
                {


                    //if (sTransType == "V")
                    //{

                    SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                    SQL = SQL + " RM_MRD_SUPP_ENTRY ='N', RM_MRD_SUPP_PENO= NULL, RM_MPED_VFINYR_ID = 0 ";

                    SQL = SQL + " WHERE ";
                    SQL = SQL + "  RM_MRD_SUPP_PENO='" + txtEntryNo + "' AND RM_MPED_VFINYR_ID = " + mngrclass.FinYearID + "";

                    sSQLArray.Add(SQL);
                    //}
                    //else if (sTransType == "TOLLVENDOR")
                    //{
                    //    SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                    //    SQL = SQL + "  RM_MRD_TOLL_PE_ENTRY_YN ='N', RM_MRD_TOLL_PE_ENTRY_NO = NULL , RM_MRD_TOLL_PE_ENTRY_FINYR_ID  =0 ";
                    //    SQL = SQL + " WHERE ";
                    //    SQL = SQL + "   RM_MRD_TOLL_PE_ENTRY_NO ='" + txtEntryNo + "' AND RM_MRD_TOLL_PE_ENTRY_FINYR_ID =" + mngrclass.FinYearID + "";

                    //    sSQLArray.Add(SQL);
                    //}

                }
                else if (EntryType == "T")
                {
                    //< tRANSPORTER
                    if (sTransType == "R")
                    {
                        SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                        SQL = SQL + " RM_MRD_TRANS_ENTRY ='N', RM_MRD_TRANS_PENO= NULL , RM_MPED_TFINYR_ID =0 ";

                        SQL = SQL + " WHERE ";
                        SQL = SQL + "   RM_MRD_TRANS_PENO='" + txtEntryNo + "' AND RM_MPED_TFINYR_ID =" + mngrclass.FinYearID + "";

                        sSQLArray.Add(SQL);

                    }
                    //else if (sTransType == "TOLL")
                    //{
                    //    SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                    //    SQL = SQL + "  RM_MRD_TOLL_PE_ENTRY_YN ='N', RM_MRD_TOLL_PE_ENTRY_NO = NULL , RM_MRD_TOLL_PE_ENTRY_FINYR_ID  =0 ";

                    //    SQL = SQL + " WHERE ";
                    //    SQL = SQL + "   RM_MRD_TOLL_PE_ENTRY_NO ='" + txtEntryNo + "' AND RM_MRD_TOLL_PE_ENTRY_FINYR_ID =" + mngrclass.FinYearID + "";

                    //    sSQLArray.Add(SQL);

                    //}// Abin commented on 05 nov 2022 // Toll updating to RM_RECEPIT_TOLL // as per new concept

                    else if (sTransType == "T")
                    {
                        //< sTOCK TRANSFER


                        SQL = " UPDATE RM_MAT_STK_TRANSFER_DETL SET ";
                        SQL = SQL + " RM_MSTD_PE_DONE ='N', RM_MSTD_PE_NO='' , RM_MSTD_PE_FINYR =0";

                        SQL = SQL + " WHERE ";
                        SQL = SQL + "   RM_MSTD_PE_NO='" + txtEntryNo + "'";
                        SQL = SQL + " AND RM_MSTD_PE_FINYR =" + mngrclass.FinYearID + "";

                        sSQLArray.Add(SQL);
                    }
                }


                SQL = " UPDATE RM_RECEPIT_TOLL SET ";
                SQL = SQL + " RM_MRD_SUPP_ENTRY ='N', RM_MRD_SUPP_PENO= NULL, RM_MPED_VFINYR_ID = 0 ";

                SQL = SQL + " WHERE ";
                SQL = SQL + "  RM_MRD_SUPP_PENO='" + txtEntryNo + "'";
                SQL = SQL + " AND RM_MPED_VFINYR_ID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);


                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMPE", txtEntryNo, false, Environment.MachineName, "D", sSQLArray);
            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);

                return sRetun;
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return sRetun;
        }



        #endregion

        #region "Fetch  pe data details  view mode "


        public DataTable FillViewPurchaseEntry ( string fromdate, string todate, string EntryType, object mngrclassobj )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT RM_PE_MASTER.RM_MPEM_ENTRY_NO ENTRYNO, RM_PE_MASTER.AD_FIN_FINYRID FINYR, to_char(RM_PE_MASTER.RM_MPEM_ENTRY_DATE,'DD-MON-YYYY') ENTRYDATE,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_CODE VENDORCODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME VENDORNAME,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_VENDOR_INV_NO VENODR_INV_NO,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_VENDOR_INV_DATE VENDOR_INV_DATE,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_GRAND_TOTAL GRAND_TOTAL,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_INVOICE_TOTAL INVOICE_TOTAL,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_NET_TOTAL NET_TOTAL,RM_MPEM_VERIFIED VERIFIED,RM_MPEM_APPROVED APPROVED ";
                SQL = SQL + " FROM RM_PE_MASTER, RM_VENDOR_MASTER";
                SQL = SQL + " WHERE RM_PE_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " and RM_PE_MASTER.AD_FIN_FINYRID =" + mngrclass.FinYearID + "";
                SQL = SQL + " AND RM_PE_MASTER.RM_MPEM_ENTRY_DATE BETWEEN '" + Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";

                if (EntryType == "A")
                {
                    //<
                }
                else if (EntryType == "Y")
                {
                    SQL = SQL + " AND RM_PE_MASTER.RM_MPEM_APPROVED ='Y'";
                }
                else if (EntryType == "N")
                {
                    SQL = SQL + " AND RM_PE_MASTER.RM_MPEM_APPROVED ='N'";
                }



                if (mngrclass.UserName != "ADMIN")
                { // JOMY ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 06 aug 2020
                    SQL = SQL + "   and   RM_PE_MASTER.AD_BR_CODE  IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                }


                SQL = SQL + " order by RM_PE_MASTER.RM_MPEM_ENTRY_NO desc";

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dtType;
        }

        // RECEIPT VIEW
        public DataTable FillViewReceiptEntry(string fromdate, string todate, string EntryType, object mngrclassobj)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT RM_PE_MASTER.RM_MPEM_ENTRY_NO ENTRYNO, RM_PE_MASTER.AD_FIN_FINYRID FINYR, TO_CHAR(RM_PE_MASTER.RM_MPEM_ENTRY_DATE,'DD-MON-YYYY') ENTRYDATE, ";
                SQL = SQL + " RM_MRM_RECEIPT_NO RECEIPTNO,TO_CHAR(RM_PE_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY')RECEIPTDATE, ";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_CODE VENDORCODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME VENDORNAME, ";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_VENDOR_INV_NO VENODR_INV_NO, ";
                SQL = SQL + " TO_CHAR(RM_PE_MASTER.RM_MPEM_VENDOR_INV_DATE,'DD-MON-YYYY')VENDOR_INV_DATE,RM_PE_DETAILS.RM_RMM_RM_CODE RMCODE,RM_RMM_RM_DESCRIPTION RMNAME,RM_MPED_QTY_APPROVED APPROVEDQTY, ";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_GRAND_TOTAL GRAND_TOTAL, ";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_INVOICE_TOTAL INVOICE_TOTAL, ";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_NET_TOTAL NET_TOTAL,RM_MPEM_VERIFIED VERIFIED,RM_MPEM_APPROVED APPROVED  ";
                SQL = SQL + " FROM RM_PE_MASTER, RM_VENDOR_MASTER,RM_PE_DETAILS,RM_RAWMATERIAL_MASTER ";
                SQL = SQL + " WHERE RM_PE_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                SQL = SQL + " AND RM_PE_MASTER.RM_MPEM_ENTRY_NO=RM_PE_DETAILS.RM_MPEM_ENTRY_NO ";
                SQL = SQL + " AND RM_PE_MASTER.AD_FIN_FINYRID=RM_PE_DETAILS.AD_FIN_FINYRID ";
                SQL = SQL + " AND RM_PE_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                SQL = SQL + " AND RM_PE_MASTER.AD_FIN_FINYRID =" + mngrclass.FinYearID + "";
                SQL = SQL + " AND RM_PE_MASTER.RM_MPEM_ENTRY_DATE BETWEEN '" + Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";

                

                if (mngrclass.UserName != "ADMIN")
                { // JOMY ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 06 aug 2020
                    SQL = SQL + "   AND   RM_PE_MASTER.AD_BR_CODE  IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                }


                SQL = SQL + " order by RM_PE_MASTER.RM_MPEM_ENTRY_NO desc";

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dtType;
        } 

        #endregion

        #region "purchase entry Details - VIEW MODE  "

        public DataSet PurchaseEntryReceiptDetails ( string EntryNo, string FinYear, string sType, string rdoVendorTransporter )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT ";
                SQL = SQL + "        RM_PE_DETAILS.SALES_STN_STATION_CODE,       SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "        RM_PE_DETAILS.RM_MRM_RECEIPT_NO,       RM_PE_DETAILS.AD_FIN_FINYRID_RECEIPT, ";
                SQL = SQL + "        RM_PE_DETAILS.RM_MRM_RECEIPT_DATE,       RM_PE_DETAILS.RM_MRMA_APPROVAL_NO, ";
                SQL = SQL + "        RM_PE_DETAILS.AD_FIN_FINYRID_APPROVAL,       RM_PE_DETAILS.RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "        RM_PE_DETAILS.RM_MRD_SUPPLY_QTY,  RM_PE_DETAILS.RM_MPED_QTY_APPROVED, ";
                SQL = SQL + "        RM_PE_DETAILS.RM_PO_PONO   ORDER_NO, ";
                SQL = SQL + "        RM_PE_DETAILS.RM_PO_PONO_FINYRID  PO_FIN_FINYRID,RM_PE_DETAILS.RM_MPED_VENDOR_DOC_NO  DOC_NO, ";
                SQL = SQL + "        RM_PE_DETAILS.RM_MPED_RATE  PRICE_NEW, RM_PE_DETAILS.RM_MPED_AMOUNT  AMOUNT_NEW, ";
                SQL = SQL + "        RM_PE_DETAILS.RM_RMM_RM_CODE,       RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,RM_MPEM_ENTRY_SLNO, ";
                SQL = SQL + "        RM_PE_DETAILS.RM_MRM_RECEIPT_UNIQE_NO, RM_MPED_RECEPT_TRANS_TYPE  RECEIPTTYPE ,RM_MPEM_ENTRY_TYPE, ";
                SQL = SQL + "        RM_MPED_STK_ADJ_AMT,  RM_MPED_DISC_AMT,  RM_MPED_VAT_TYPE_CODE, ";
                SQL = SQL + "        RM_MPED_VAT_PERCENTAGE,  RM_MPED_VAT_AMOUNT,  RM_MPED_VAT_RC_AMOUNT, ";
                SQL = SQL + "        RM_MPED_TOTAL_AMOUNT, RM_PE_DETAILS.RM_UM_UOM_CODE,RM_PE_DETAILS.RM_SM_SOURCE_CODE,RM_SM_SOURCE_DESC ";
                SQL = SQL + "  FROM ";
                SQL = SQL + "        RM_PE_DETAILS,RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "        RM_UOM_MASTER,SL_STATION_MASTER, ";
                SQL = SQL + "        RM_SOURCE_MASTER ";
                SQL = SQL + "  WHERE  ";
                SQL = SQL + "        RM_PE_DETAILS.RM_RMM_RM_CODE =RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "        AND RM_PE_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + "        AND RM_PE_DETAILS.RM_SM_SOURCE_CODE =RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "        AND RM_PE_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "        AND RM_PE_DETAILS.RM_MPEM_ENTRY_NO = '" + EntryNo + "' ";
                SQL = SQL + "        AND RM_PE_DETAILS.AD_FIN_FINYRID = '" + FinYear + "'";
                SQL = SQL + "        ORDER BY RM_MPEM_ENTRY_SLNO ASC";



                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet PurchaseEntryStationDetails ( string EntryNo, string FinYear, string sType, string rdoVendorTransporter )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT DISTINCT  ";
                SQL = SQL + "    RM_PE_DETAILS.SALES_STN_STATION_CODE STATIONCODE, ";
                SQL = SQL + "    SL_STATION_MASTER.SALES_STN_STATION_NAME STATIONNAME ";
                SQL = SQL + " FROM  ";
                SQL = SQL + "   RM_PE_DETAILS, SL_STATION_MASTER ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + " RM_PE_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE  ";
                SQL = SQL + " AND RM_PE_DETAILS.RM_MPEM_ENTRY_NO='" + EntryNo + "' AND RM_PE_DETAILS.AD_FIN_FINYRID = " + FinYear + "";
                SQL = SQL + " ORDER BY RM_PE_DETAILS.SALES_STN_STATION_CODE asc";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet PurchaseEntryRMDetails ( string EntryNo, string FinYear, string sType, string rdoVendorTransporter )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT DISTINCT  ";
                SQL = SQL + "    RM_PE_DETAILS.RM_RMM_RM_CODE CODE , ";
                SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION NAME ";
                SQL = SQL + " FROM  ";
                SQL = SQL + "    RM_PE_DETAILS, RM_RAWMATERIAL_MASTER ";
                SQL = SQL + " WHERE ";
                SQL = SQL + "    RM_PE_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_PE_DETAILS.RM_MPEM_ENTRY_NO='" + EntryNo + "' AND RM_PE_DETAILS.AD_FIN_FINYRID = " + FinYear + "";
                SQL = SQL + " ORDER BY RM_PE_DETAILS.RM_RMM_RM_CODE asc";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FectchPurchaseEntryStockTransferDetails ( string EntryNo, string FinYear, string sType, string rdoVendorTransporter )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " ";
                SQL = "    SELECT ";
                SQL = SQL + "            SA_STN_STATION_CODE_FROM SALES_STN_STATION_CODE , ";
                SQL = SQL + "            SALES_STN_STATION_NAME,  ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO RM_MRM_RECEIPT_NO , ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL + "            RM_MSTM_TRANSFER_DATE RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO  RM_MRM_APPROVED_NO, "; // RM_MSTD_RCV_DOC_NO  // using the same document number sin no approval task 
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID RM_MRD_APPROVED_FINYRID, ";
                SQL = SQL + "            RM_MSTM_TRANSFER_DATE   RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "            RM_MSTD_QUANTITY  RM_MRD_SUPPLY_QTY ,  ";
                SQL = SQL + "            RM_MSTD_RCV_QTY  RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "            RM_MSTD_TRANSPORTER_CODE VENDOR_CODE,  ";
                SQL = SQL + "            RM_MTRANSPO_ORDER_NO ORDER_NO, ";
                SQL = SQL + "            RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID, ";
                SQL = SQL + "            RM_MSTD_TRNSPORTER_DOC_NO DOC_NO , ";
                SQL = SQL + "            RM_MSTD_TRANS_CHARGES PRICE_NEW, ";
                SQL = SQL + "            RM_MSTD_TRANS_AMOUNT  AMOUNT_NEW, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE,   ";
                SQL = SQL + "            RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_MSTD_SL_NO   RM_MRM_RECEIPT_UNIQE_NO ";
                SQL = SQL + "    FROM ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_MASTER, ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_DETL, ";
                SQL = SQL + "        RM_RAWMATERIAL_MASTER , ";
                SQL = SQL + "        SL_STATION_MASTER, ";
                SQL = SQL + "        RM_SOURCE_MASTER, ";
                SQL = SQL + "        RM_VENDOR_MASTER ";
                SQL = SQL + "    WHERE  ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO = RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO  ";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID = RM_MAT_STK_TRANSFER_DETL.AD_FIN_FINYRID  ";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + "        AND RM_MAT_STK_TRANSFER_DETL.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE  ";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM = sl_station_master.SALES_STN_STATION_CODE  ";
                SQL = SQL + "        AND RM_MSTD_TRANSPORTER_CODE = RM_VM_VENDOR_CODE  ";
                SQL = SQL + "     AND RM_MSTD_PE_NO='" + EntryNo + "' AND RM_MSTD_PE_FINYR = " + FinYear + "";

                sReturn = clsSQLHelper.GetDataset(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }
        #endregion


        #region "purchase entry Details - VIEW MODE_OLD  "

        public DataSet PurchaseEntryReceiptDetails_old ( string EntryNo, string FinYear, string sType, string rdoVendorTransporter )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + " RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_FIN_FINYRID,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID,";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY, ";

                if (sType == "V")
                {
                    // VENODER 
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE VENDOR_CODE, ";
                    // SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO_NEW ORDER_NO, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_VENDOR_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW AMOUNT_NEW ,";
                }
                else if (sType == "RECEIPTTOLLVEND")
                {
                    // VENODER 
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE VENDOR_CODE, ";
                    // SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO_NEW ORDER_NO, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_VENDOR_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW AMOUNT_NEW ,";
                }
                else if (sType == "TOLLVENDOR")
                {
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE VENDOR_CODE , ";
                    //SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO , RM_MPOM_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO_NEW ORDER_NO , RM_MPOM_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS. RM_MRD_VENDOR_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_POD_TOLL_RATE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOLL_AMOUNT_NEW AMOUNT_NEW ,";
                }
                else if (sType == "R")
                {
                    // TRASPORTER RECEIPT 
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE VENDOR_CODE , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO_NEW ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    // SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW AMOUNT_NEW ,";
                }
                else if (sType == "TOLL")
                {
                    // TOLL 

                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE VENDOR_CODE , ";
                    // SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO_NEW ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_POD_TOLL_RATE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOLL_AMOUNT_NEW AMOUNT_NEW ,";

                }
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_UNIQE_NO ,'RM' ReceiptType, 1 SortID";

                SQL = SQL + " FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER,";
                SQL = SQL + " RM_VENDOR_MASTER,";
                SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER, RM_RECEIPT_APPL_MASTER ";

                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS. RM_MRD_APPROVED = 'Y'";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO = RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID = RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER. RM_MRMA_APPROVED_STATUS ='Y'";

                if (sType == "V")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO='" + EntryNo + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MPED_VFINYR_ID = " + FinYear + "";
                }

                if (sType == "RECEIPTTOLLVEND")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO='" + EntryNo + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MPED_VFINYR_ID = " + FinYear + "";
                }
                else if (sType == "TOLLVENDOR")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS. RM_MRD_TOLL_PE_ENTRY_NO='" + EntryNo + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_FINYR_ID =" + FinYear + "";

                }
                else if (sType == "R")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO='" + EntryNo + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MPED_TFINYR_ID =" + FinYear + "";
                }
                else if (sType == "TOLL")
                {
                    // AGAINST TRANSPORTER 
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS. RM_MRD_TOLL_PE_ENTRY_NO='" + EntryNo + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_FINYR_ID =" + FinYear + "";
                }

                //if (rdoVendorTransporter == "0")
                //{
                //    SQL = SQL + " order by RM_RECEPIT_DETAILS.RM_MRD_VENDOR_DOC_NO ";
                //}
                //else
                //{
                //    SQL = SQL + " order by RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO ";
                //}




                SQL = SQL + " union all  SELECT RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                SQL = SQL + "         SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.AD_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_SUPPLY_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE              VENDOR_CODE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO_NEW               ORDER_NO, ";
                //SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO               ORDER_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID            PO_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_NO      DOC_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE       PRICE_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT           AMOUNT_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO, ";
                SQL = SQL + "            RM_RMM_PRODUCT_TYPE   ReceiptType, 2 SortID";
                SQL = SQL + "    FROM RM_RECEPIT_TOLL, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "         RM_UOM_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         RM_SOURCE_MASTER, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS ";
                SQL = SQL + "   WHERE     RM_RECEPIT_TOLL.RM_RMM_RM_CODE = ";
                SQL = SQL + "             RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "                and RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO =RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE = ";
                SQL = SQL + "             RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = ";
                SQL = SQL + "             SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + "         AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVED_STATUS = 'Y' ";
                //     SQL = SQL + "         and RM_RMM_RM_TYPE='TOLL' ";
                // SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                //  SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                SQL = SQL + "             RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";

                SQL = SQL + " AND RM_RECEPIT_TOLL.RM_MRD_SUPP_PENO='" + EntryNo + "'";
                SQL = SQL + " AND RM_RECEPIT_TOLL.RM_MPED_VFINYR_ID = " + FinYear + "";


                SQL = SQL + "  order by SortID,ReceiptType DESC";



                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet PurchaseEntryStationDetails_old ( string EntryNo, string FinYear, string sType, string rdoVendorTransporter )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT DISTINCT RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE STATIONCODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME STATIONNAME";

                SQL = SQL + " FROM RM_RECEPIT_DETAILS, SL_STATION_MASTER";

                SQL = SQL + " WHERE RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";

                if (sType == "V")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO='" + EntryNo + "' AND RM_RECEPIT_DETAILS.RM_MPED_VFINYR_ID = " + FinYear + "";
                }
                else if (sType == "R")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO='" + EntryNo + "' AND RM_RECEPIT_DETAILS.RM_MPED_TFINYR_ID =" + FinYear + "";
                }
                else if (sType == "TOLL")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS. RM_MRD_TOLL_PE_ENTRY_NO='" + EntryNo + "' AND RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_FINYR_ID =" + FinYear + "";
                }
                else if (sType == "TOLLVENDOR")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS. RM_MRD_TOLL_PE_ENTRY_NO='" + EntryNo + "' AND RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_FINYR_ID =" + FinYear + "";
                }
                SQL = SQL + " ORDER BY RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE asc";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet PurchaseEntryRMDetails_old ( string EntryNo, string FinYear, string sType, string rdoVendorTransporter )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT DISTINCT RM_RECEPIT_DETAILS.RM_RMM_RM_CODE CODE ,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION NAME";

                SQL = SQL + " FROM RM_RECEPIT_DETAILS, RM_RAWMATERIAL_MASTER";

                SQL = SQL + " WHERE RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";

                if (sType == "V")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO='" + EntryNo + "' AND RM_RECEPIT_DETAILS.RM_MPED_VFINYR_ID = " + FinYear + "";
                }
                else if (sType == "R")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO='" + EntryNo + "' AND RM_RECEPIT_DETAILS.RM_MPED_TFINYR_ID =" + FinYear + "";
                }
                else if (sType == "TOLL")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS. RM_MRD_TOLL_PE_ENTRY_NO='" + EntryNo + "' AND RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_FINYR_ID =" + FinYear + "";
                }
                else if (sType == "TOLLVENDOR")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS. RM_MRD_TOLL_PE_ENTRY_NO='" + EntryNo + "' AND RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_FINYR_ID =" + FinYear + "";
                }
                SQL = SQL + " ORDER BY RM_RECEPIT_DETAILS.RM_RMM_RM_CODE asc";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FectchPurchaseEntryStockTransferDetails_old ( string EntryNo, string FinYear, string sType, string rdoVendorTransporter )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " ";
                SQL = "    SELECT ";
                SQL = SQL + "            SA_STN_STATION_CODE_FROM SALES_STN_STATION_CODE , ";
                SQL = SQL + "            SALES_STN_STATION_NAME,  ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO RM_MRM_RECEIPT_NO , ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL + "            RM_MSTM_TRANSFER_DATE RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO  RM_MRM_APPROVED_NO, "; // RM_MSTD_RCV_DOC_NO  // using the same document number sin no approval task 
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID RM_MRD_APPROVED_FINYRID, ";
                SQL = SQL + "            RM_MSTM_TRANSFER_DATE   RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "            RM_MSTD_QUANTITY  RM_MRD_SUPPLY_QTY ,  ";
                SQL = SQL + "            RM_MSTD_RCV_QTY  RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "            RM_MSTD_TRANSPORTER_CODE VENDOR_CODE,  ";
                SQL = SQL + "            RM_MTRANSPO_ORDER_NO ORDER_NO, ";
                SQL = SQL + "            RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID, ";
                SQL = SQL + "            RM_MSTD_TRNSPORTER_DOC_NO DOC_NO , ";
                SQL = SQL + "            RM_MSTD_TRANS_CHARGES PRICE_NEW, ";
                SQL = SQL + "            RM_MSTD_TRANS_AMOUNT  AMOUNT_NEW, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE,   ";
                SQL = SQL + "            RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_MSTD_SL_NO   RM_MRM_RECEIPT_UNIQE_NO ";
                SQL = SQL + "    FROM ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_MASTER, ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_DETL, ";
                SQL = SQL + "        RM_RAWMATERIAL_MASTER , ";
                SQL = SQL + "        SL_STATION_MASTER, ";
                SQL = SQL + "        RM_SOURCE_MASTER, ";
                SQL = SQL + "        RM_VENDOR_MASTER ";
                SQL = SQL + "    WHERE  ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO = RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO  ";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID = RM_MAT_STK_TRANSFER_DETL.AD_FIN_FINYRID  ";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + "        AND RM_MAT_STK_TRANSFER_DETL.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE  ";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM = sl_station_master.SALES_STN_STATION_CODE  ";
                SQL = SQL + "        AND RM_MSTD_TRANSPORTER_CODE = RM_VM_VENDOR_CODE  ";
                SQL = SQL + "     AND RM_MSTD_PE_NO='" + EntryNo + "' AND RM_MSTD_PE_FINYR = " + FinYear + "";

                sReturn = clsSQLHelper.GetDataset(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }
        #endregion



        #region "Fetch accounts deatils "

        public bool FnChkAlreadyApporved ( string[,] psTabs )
        {
            int i;
            DataSet sReturn = default(DataSet);

            try
            {
                SQL = string.Empty;
                OracleHelper oTrns = new OracleHelper();
                SqlHelper clsSQLHelper = new SqlHelper();


                for (i = 0; i <= psTabs.GetUpperBound(0); i++)
                {
                    SQL = "Select Count(" + psTabs[i, 2] + ") FROM ";
                    SQL = SQL + psTabs[i, 1] + " ";

                    SQL = SQL + psTabs[i, 3];

                    sReturn = clsSQLHelper.GetDataset(SQL);

                    if (Convert.ToInt16(sReturn.Tables[0].Rows[0][0]) > 0)
                    {
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return false;
        }

        public string GetAccountName ( string ItemCode )
        {
            string sRet = string.Empty;

            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  GL_COAM_ACCOUNT_NAME ACCOUNT_NAME";
                SQL = SQL + "  FROM GL_COA_MASTER WHERE GL_COAM_ACCOUNT_CODE  ='" + ItemCode + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);

                if (sReturn.Tables[0].Rows.Count > 0)
                {
                    sRet = sReturn.Tables[0].Rows[0][0].ToString();
                }
                else
                {
                    sRet = "";
                }
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return sRet;
        }



        public string fnGetIRGRAccount ( )
        {
            DataSet dsIRGR = new DataSet();
            string sReturn = string.Empty;

            try
            {
                SQL = string.Empty;
                OracleHelper oTrns = new OracleHelper();
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_ip_parameter_value irgr_acc";
                SQL = SQL + " FROM RM_DEFUALTS_GL_PARAMETERS";
                SQL = SQL + " WHERE RM_ip_parameter_desc = 'IRGRACCOUNT'";

                dsIRGR = clsSQLHelper.GetDataset(SQL);

                if (dsIRGR == null)
                {
                    sReturn = "";
                }

                if (dsIRGR.Tables[0].Rows.Count > 0)
                {
                    sReturn = dsIRGR.Tables[0].Rows[0][0] + "";
                }
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return sReturn;
        }

        #endregion

        #region "Master fech"
        public DataSet EntryMasterFetch ( string EntryNo, string FinYear )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_PE_MASTER.RM_MPEM_ENTRY_NO, RM_PE_MASTER.AD_FIN_FINYRID,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_ENTRY_DATE,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_ENTRY_TYPE,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_TRANS_TYPE, RM_PE_MASTER.RM_VM_VENDOR_CODE,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_VENDOR_INV_NO,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_VENDOR_INV_DATE,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_GRAND_TOTAL, RM_PE_MASTER.RM_MPEM_DISC_AMT,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_STK_ADJ_AMT,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_INVOICE_TOTAL,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_NET_TOTAL,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_BALANCE_TOTAL,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_DISC_ACC_CODE,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_STKADJ_ACCCODE,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_REMARKS, RM_PE_MASTER.RM_MPEM_CREATEDBY,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_VERIFIED, RM_PE_MASTER.RM_MPEM_VERIFIED_BY,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_APPROVED, RM_PE_MASTER.RM_MPEM_APPROVED_BY,";
                SQL = SQL + " RM_PE_MASTER.RM_MPEM_ALLOW_EDIT , RM_MPEM_app_from_DATE,RM_MPEM_app_to_DATE,";
                SQL = SQL + " RM_MPEM_VAT_PERCENTAGE,RM_MPEM_VAT_AMOUNT,RM_MPEM_VAT_ACC_CODE,RM_MPEM_VAT_TYPE_CODE ,";
                SQL = SQL + " RM_MPEM_VAT_OUTPUT_ACC_CODE,RM_MPEM_TAX_APPLICABLE_AMOUNT,RM_MPEM_NON_TAX_APPL_AMOUNT,";
                SQL = SQL + "  RM_PE_VAT_AMT_ADV_MATCHED , RM_PE_ADVANCE_MATCHED_AMT  ,  ";
                SQL = SQL + " ( RM_MPEM_NET_TOTAL -  RM_PE_ADVANCE_MATCHED_AMT ) ADVANCE_MATCH_BALANCE_AMT , ";

                SQL = SQL + " RM_MPEM_VAT_RC_AMOUNT ,RM_PE_MASTER.AD_BR_CODE,AD_BR_NAME,RM_MPEM_INV_RECIVED_DATE,RM_MPEM_PE_PO_NO  ";
                SQL = SQL + " FROM  RM_PE_MASTER, RM_VENDOR_MASTER,AD_BRANCH_MASTER";

                SQL = SQL + " WHERE RM_PE_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " AND RM_PE_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE";
                SQL = SQL + " AND     RM_PE_MASTER.AD_FIN_FINYRID =" + FinYear + "";
                SQL = SQL + " AND RM_PE_MASTER.RM_MPEM_ENTRY_NO='" + EntryNo + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        #endregion

        #region "print"

        public DataSet FetchVourcherPrintData ( double iFromInvoiceNumber, double iToInvoiceNumber, string sType, object mngrclassobj )
        {
            DataSet dsPoStatus = new DataSet("RMAPPROVALPRINT");
            DataTable dtDetailsData = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                if (sType == "VENDOR")
                {

                    SQL = " SELECT RM_MRM_APPROVED_NO, RM_MRD_APPROVED_FINYRID, RM_MRM_RECEIPT_NO, RECEIPTFINYR, RM_MRM_RECEIPT_DATE,  ";
                    SQL = SQL + " PEVENDORCODE, SUPPNAME, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, RM_MPOM_PRICE_TYPE,  ";
                    SQL = SQL + " RM_MRD_SL_NO, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION,  ";
                    SQL = SQL + " RM_UM_UOM_CODE, RM_UM_UOM_DESC, RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC, RM_MRM_TRANSPORTER_CODE,  ";
                    SQL = SQL + " TRNSPORTERNAME, RM_MTRANSPO_ORDER_NO, RM_MTRANSPO_FIN_FINYRID, RM_MRM_TRNSPORTER_DOC_NO, RM_MRD_ORDER_QTY,  ";
                    SQL = SQL + " RM_MRD_VENDOR_DOC_NO, FA_FAM_ASSET_CODE, RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME,  ";
                    SQL = SQL + " RM_MRD_SUPPLY_QTY, RM_MRD_WEIGH_QTY, RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY, RM_MRD_SUPP_UNIT_PRICE,  ";
                    SQL = SQL + " RM_MRD_SUPP_AMOUNT, RM_MRD_TRANS_RATE, RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT, RM_MRD_SUPP_UNIT_PRICE_NEW,  ";
                    SQL = SQL + " RM_MRD_SUPP_AMOUNT_NEW, RM_MRD_TRANS_RATE_NEW, RM_MRD_TRANS_AMOUNT_NEW, RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_SUPP_PENO,  ";
                    SQL = SQL + " RM_MRD_TRANS_PENO, RM_MRM_RECEIPT_UNIQE_NO, RM_MPEM_ENTRY_NO, PEFINYR, RM_MPEM_ENTRY_DATE,  ";
                    SQL = SQL + " RM_MPEM_ENTRY_TYPE, RM_MPEM_TRANS_TYPE, RM_MPEM_VENDOR_INV_NO, RM_MPEM_VENDOR_INV_DATE, RM_MPEM_GRAND_TOTAL,  ";
                    SQL = SQL + " RM_MPEM_DISC_AMT, RM_MPEM_STK_ADJ_AMT, RM_MPEM_INVOICE_TOTAL, RM_MPEM_NET_TOTAL, RM_MPEM_BALANCE_TOTAL,  ";
                    SQL = SQL + " RM_MPEM_DISC_ACC_CODE,DISCACCNAME, RM_MPEM_STKADJ_ACCCODE,STKADJACCNAME, RM_MPEM_REMARKS, RM_MPEM_CREATEDBY, RM_MPEM_VERIFIED,  ";
                    SQL = SQL + " RM_MPEM_VERIFIED_BY, RM_MPEM_APPROVED, RM_MPEM_APPROVED_BY, ";
                    SQL = SQL + " RM_MPEM_VAT_PERCENTAGE,RM_MPEM_VAT_AMOUNT,RM_MPEM_VAT_ACC_CODE, VATACCNAME,RM_MPEM_VAT_TYPE_CODE, ";
                    SQL = SQL + " RM_MPEM_VAT_OUTPUT_ACC_CODE, vatoutputaccname, ";
                    SQL = SQL + " RM_MPEM_TAX_APPLICABLE_AMOUNT, ";
                    SQL = SQL + " RM_MPEM_NON_TAX_APPL_AMOUNT,RM_MPEM_VAT_RC_AMOUNT , ";
                    SQL = SQL + "       MASTER_BRANCH_CODE ,   MASTER_BRANCH_NAME , ";
                    SQL = SQL + "       MASTER_BRANCHDOC_PREFIX,  MASTER_BRANCH_POBOX,  MASTER_BRANCH_ADDRESS, ";
                    SQL = SQL + "       MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE,  MASTER_BRANCH_FAX, ";
                    SQL = SQL + "       MASTER_BRANCH_SPONSER_NAME,  MASTER_BRANCH_TRADE_LICENSE_NO,  MASTER_BRANCH_EMAIL_ID, ";
                    SQL = SQL + "       MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER,RM_MPEM_INV_RECIVED_DATE ";
                    SQL = SQL + " FROM  ";
                    SQL = SQL + " RM_PURCHASE_ENTRY_PRINT_VW  ";
                    SQL = SQL + " WHERE  ";
                    //SQL = SQL + "  RM_MPEM_ENTRY_NO ='" + sEntryNo + "' " ";
                    SQL = SQL + " PEFINYR=" + mngrclass.FinYearID + "";

                    if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and  RM_MPEM_ENTRY_NO >=" + iFromInvoiceNumber + "";
                    }

                    if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and RM_MPEM_ENTRY_NO  <=" + iToInvoiceNumber + "";
                    }

                    dsPoStatus = clsSQLHelper.GetDataset(SQL);
                    dsPoStatus.Tables[0].TableName = "RM_PURCHASE_ENTRY_PRINT_VW";
                }
                else if (sType == "TOLLVENDOR")
                {
                    SQL = " SELECT  ";
                    SQL = SQL + " RM_MRM_APPROVED_NO, RM_MRD_APPROVED_FINYRID, RM_MRM_RECEIPT_NO, RECEIPTFINYR, RM_MRM_RECEIPT_DATE,  ";
                    SQL = SQL + " PEVENDORCODE, SUPPNAME, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, RM_MPOM_PRICE_TYPE,  ";
                    SQL = SQL + " RM_MRD_SL_NO, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION,  ";
                    SQL = SQL + " RM_UM_UOM_CODE, RM_UM_UOM_DESC, RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC, RM_MTRANSPO_ORDER_NO,  ";
                    SQL = SQL + " RM_MTRANSPO_FIN_FINYRID, RM_MRM_TRNSPORTER_DOC_NO, RM_MRD_ORDER_QTY, RM_MRD_VENDOR_DOC_NO, FA_FAM_ASSET_CODE,  ";
                    SQL = SQL + " RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME, RM_MRD_SUPPLY_QTY, RM_MRD_WEIGH_QTY,  ";
                    SQL = SQL + " RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY, RM_MRD_SUPP_UNIT_PRICE, RM_MRD_SUPP_AMOUNT, RM_MRD_TRANS_RATE,  ";
                    SQL = SQL + " RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT, RM_MRD_SUPP_UNIT_PRICE_NEW, RM_MRD_SUPP_AMOUNT_NEW, RM_MRD_TRANS_RATE_NEW,  ";
                    SQL = SQL + " RM_MRD_TRANS_AMOUNT_NEW, RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_SUPP_PENO, RM_MRD_TRANS_PENO, RM_MRM_RECEIPT_UNIQE_NO,  ";
                    SQL = SQL + " RM_MPEM_ENTRY_NO, PEFINYR, RM_MPEM_ENTRY_DATE, RM_MPEM_ENTRY_TYPE, RM_MPEM_TRANS_TYPE,  ";
                    SQL = SQL + " RM_MPEM_VENDOR_INV_NO, RM_MPEM_VENDOR_INV_DATE, RM_MPEM_GRAND_TOTAL, RM_MPEM_DISC_AMT, RM_MPEM_STK_ADJ_AMT,  ";
                    SQL = SQL + " RM_MPEM_INVOICE_TOTAL, RM_MPEM_NET_TOTAL, RM_MPEM_BALANCE_TOTAL, RM_MPEM_DISC_ACC_CODE,DISCACCNAME, RM_MPEM_STKADJ_ACCCODE, STKADJACCNAME, ";
                    SQL = SQL + " RM_MPEM_REMARKS, RM_MPEM_CREATEDBY, RM_MPEM_VERIFIED, RM_MPEM_VERIFIED_BY, RM_MPEM_APPROVED,  ";
                    SQL = SQL + " RM_MPEM_APPROVED_BY , ";
                    SQL = SQL + " RM_MPEM_VAT_PERCENTAGE,RM_MPEM_VAT_AMOUNT,";
                    SQL = SQL + " RM_MPEM_VAT_ACC_CODE, VATACCNAME,RM_MPEM_VAT_TYPE_CODE,";
                    SQL = SQL + " RM_MPEM_VAT_OUTPUT_ACC_CODE,vatoutputaccname,RM_MPEM_TAX_APPLICABLE_AMOUNT,RM_MPEM_NON_TAX_APPL_AMOUNT,RM_MPEM_VAT_RC_AMOUNT,";
                    SQL = SQL + " RM_UOM_TOLL_CODE  , RM_POD_TOLL_RATE  , RM_MRD_TOLL_AMOUNT	 , ";
                    SQL = SQL + " RM_POD_TOLL_RATE_NEW  , RM_MRD_TOLL_AMOUNT_NEW	 , ";
                    SQL = SQL + " RM_MRD_TOLL_PE_ENTRY_YN	,   RM_MRD_TOLL_PE_ENTRY_NO,  RM_MRD_TOLL_PE_ENTRY_FINYR_ID	 ,  ";
                    SQL = SQL + "       MASTER_BRANCH_CODE ,   MASTER_BRANCH_NAME , ";
                    SQL = SQL + "       MASTER_BRANCHDOC_PREFIX,  MASTER_BRANCH_POBOX,  MASTER_BRANCH_ADDRESS, ";
                    SQL = SQL + "       MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE,  MASTER_BRANCH_FAX, ";
                    SQL = SQL + "       MASTER_BRANCH_SPONSER_NAME,  MASTER_BRANCH_TRADE_LICENSE_NO,  MASTER_BRANCH_EMAIL_ID, ";
                    SQL = SQL + "       MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER,RM_MPEM_INV_RECIVED_DATE  ";

                    SQL = SQL + " FROM RM_PURCHASE_VEND_RCPT_TOLL_VW ";
                    SQL = SQL + " WHERE  ";
                    //SQL = SQL + " RM_MPEM_ENTRY_NO ='" + sEntryNo + "' ";
                    SQL = SQL + " PEFINYR=" + mngrclass.FinYearID + "";

                    if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and  RM_MPEM_ENTRY_NO >=" + iFromInvoiceNumber + "";
                    }

                    if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and RM_MPEM_ENTRY_NO  <=" + iToInvoiceNumber + "";
                    }


                    dsPoStatus = clsSQLHelper.GetDataset(SQL);
                    dsPoStatus.Tables[0].TableName = "RM_PURCHASE_TRNS_RCPT_PRINT_VW";


                }
                else if (sType == "TRANPORTER_OR_RECEIPT")
                {
                    ///   
                    /// 

                    SQL = " SELECT  ";
                    SQL = SQL + " RM_MRM_APPROVED_NO, RM_MRD_APPROVED_FINYRID, RM_MRM_RECEIPT_NO, RECEIPTFINYR, RM_MRM_RECEIPT_DATE,  ";
                    SQL = SQL + " PEVENDORCODE, SUPPNAME, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, RM_MPOM_PRICE_TYPE,  ";
                    SQL = SQL + " RM_MRD_SL_NO, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION,  ";
                    SQL = SQL + " RM_UM_UOM_CODE, RM_UM_UOM_DESC, RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC, RM_MTRANSPO_ORDER_NO,  ";
                    SQL = SQL + " RM_MTRANSPO_FIN_FINYRID, RM_MRM_TRNSPORTER_DOC_NO, RM_MRD_ORDER_QTY, RM_MRD_VENDOR_DOC_NO, FA_FAM_ASSET_CODE,  ";
                    SQL = SQL + " RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME, RM_MRD_SUPPLY_QTY, RM_MRD_WEIGH_QTY,  ";
                    SQL = SQL + " RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY, RM_MRD_SUPP_UNIT_PRICE, RM_MRD_SUPP_AMOUNT, RM_MRD_TRANS_RATE,  ";
                    SQL = SQL + " RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT, RM_MRD_SUPP_UNIT_PRICE_NEW, RM_MRD_SUPP_AMOUNT_NEW, RM_MRD_TRANS_RATE_NEW,  ";
                    SQL = SQL + " RM_MRD_TRANS_AMOUNT_NEW, RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_SUPP_PENO, RM_MRD_TRANS_PENO, RM_MRM_RECEIPT_UNIQE_NO,  ";
                    SQL = SQL + " RM_MPEM_ENTRY_NO, PEFINYR, RM_MPEM_ENTRY_DATE, RM_MPEM_ENTRY_TYPE, RM_MPEM_TRANS_TYPE,  ";
                    SQL = SQL + " RM_MPEM_VENDOR_INV_NO, RM_MPEM_VENDOR_INV_DATE, RM_MPEM_GRAND_TOTAL, RM_MPEM_DISC_AMT, RM_MPEM_STK_ADJ_AMT,  ";
                    SQL = SQL + " RM_MPEM_INVOICE_TOTAL, RM_MPEM_NET_TOTAL, RM_MPEM_BALANCE_TOTAL, RM_MPEM_DISC_ACC_CODE,discaccname, RM_MPEM_STKADJ_ACCCODE,stkadjaccname,  ";
                    SQL = SQL + " RM_MPEM_REMARKS, RM_MPEM_CREATEDBY, RM_MPEM_VERIFIED, RM_MPEM_VERIFIED_BY, RM_MPEM_APPROVED,  ";
                    SQL = SQL + " RM_MPEM_APPROVED_BY , ";
                    SQL = SQL + " RM_MPEM_VAT_PERCENTAGE, RM_MPEM_VAT_AMOUNT, RM_MPEM_VAT_ACC_CODE,";
                    SQL = SQL + " VATACCNAME, RM_MPEM_VAT_TYPE_CODE,";
                    SQL = SQL + " RM_MPEM_VAT_OUTPUT_ACC_CODE, vatoutputaccname, ";
                    SQL = SQL + " RM_MPEM_TAX_APPLICABLE_AMOUNT, ";
                    SQL = SQL + " RM_MPEM_NON_TAX_APPL_AMOUNT,RM_MPEM_VAT_RC_AMOUNT , ";
                    SQL = SQL + " RM_UOM_TOLL_CODE  , RM_POD_TOLL_RATE  , RM_MRD_TOLL_AMOUNT	 , ";
                    SQL = SQL + " RM_POD_TOLL_RATE_NEW  , RM_MRD_TOLL_AMOUNT_NEW	 , ";
                    SQL = SQL + " RM_MRD_TOLL_PE_ENTRY_YN	,   RM_MRD_TOLL_PE_ENTRY_NO,  RM_MRD_TOLL_PE_ENTRY_FINYR_ID,	   ";
                    SQL = SQL + "       MASTER_BRANCH_CODE ,   MASTER_BRANCH_NAME , ";
                    SQL = SQL + "       MASTER_BRANCHDOC_PREFIX,  MASTER_BRANCH_POBOX,  MASTER_BRANCH_ADDRESS, ";
                    SQL = SQL + "       MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE,  MASTER_BRANCH_FAX, ";
                    SQL = SQL + "       MASTER_BRANCH_SPONSER_NAME,  MASTER_BRANCH_TRADE_LICENSE_NO,  MASTER_BRANCH_EMAIL_ID, ";
                    SQL = SQL + "       MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER,RM_MPEM_INV_RECIVED_DATE ";

                    SQL = SQL + " FROM RM_PURCHASE_TRNS_RCPT_PRINT_VW ";
                    SQL = SQL + " WHERE  ";
                    // SQL = SQL + "  RM_MPEM_ENTRY_NO ='" + sEntryNo + "'  ";
                    SQL = SQL + "PEFINYR=" + mngrclass.FinYearID + "";

                    if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and  RM_MPEM_ENTRY_NO >=" + iFromInvoiceNumber + "";
                    }

                    if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and RM_MPEM_ENTRY_NO  <=" + iToInvoiceNumber + "";
                    }


                    dsPoStatus = clsSQLHelper.GetDataset(SQL);
                    dsPoStatus.Tables[0].TableName = "RM_PURCHASE_TRNS_RCPT_PRINT_VW";

                }
                else if (sType == "TOLL")
                {

                    SQL = " SELECT  ";
                    SQL = SQL + " RM_MRM_APPROVED_NO, RM_MRD_APPROVED_FINYRID, RM_MRM_RECEIPT_NO, RECEIPTFINYR, RM_MRM_RECEIPT_DATE,  ";
                    SQL = SQL + " PEVENDORCODE, SUPPNAME, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, RM_MPOM_PRICE_TYPE,  ";
                    SQL = SQL + " RM_MRD_SL_NO, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION,  ";
                    SQL = SQL + " RM_UM_UOM_CODE, RM_UM_UOM_DESC, RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC, RM_MTRANSPO_ORDER_NO,  ";
                    SQL = SQL + " RM_MTRANSPO_FIN_FINYRID, RM_MRM_TRNSPORTER_DOC_NO, RM_MRD_ORDER_QTY, RM_MRD_VENDOR_DOC_NO, FA_FAM_ASSET_CODE,  ";
                    SQL = SQL + " RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME, RM_MRD_SUPPLY_QTY, RM_MRD_WEIGH_QTY,  ";
                    SQL = SQL + " RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY, RM_MRD_SUPP_UNIT_PRICE, RM_MRD_SUPP_AMOUNT, RM_MRD_TRANS_RATE,  ";
                    SQL = SQL + " RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT, RM_MRD_SUPP_UNIT_PRICE_NEW, RM_MRD_SUPP_AMOUNT_NEW, RM_MRD_TRANS_RATE_NEW,  ";
                    SQL = SQL + " RM_MRD_TRANS_AMOUNT_NEW, RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_SUPP_PENO, RM_MRD_TRANS_PENO, RM_MRM_RECEIPT_UNIQE_NO,  ";
                    SQL = SQL + " RM_MPEM_ENTRY_NO, PEFINYR, RM_MPEM_ENTRY_DATE, RM_MPEM_ENTRY_TYPE, RM_MPEM_TRANS_TYPE,  ";
                    SQL = SQL + " RM_MPEM_VENDOR_INV_NO, RM_MPEM_VENDOR_INV_DATE, RM_MPEM_GRAND_TOTAL, RM_MPEM_DISC_AMT, RM_MPEM_STK_ADJ_AMT,  ";
                    SQL = SQL + " RM_MPEM_INVOICE_TOTAL, RM_MPEM_NET_TOTAL, RM_MPEM_BALANCE_TOTAL, RM_MPEM_DISC_ACC_CODE,discaccname, RM_MPEM_STKADJ_ACCCODE, stkadjaccname, ";
                    SQL = SQL + " RM_MPEM_REMARKS, RM_MPEM_CREATEDBY, RM_MPEM_VERIFIED, RM_MPEM_VERIFIED_BY, RM_MPEM_APPROVED,  ";
                    SQL = SQL + " RM_MPEM_APPROVED_BY , ";
                    SQL = SQL + " RM_MPEM_VAT_PERCENTAGE, RM_MPEM_VAT_AMOUNT, RM_MPEM_VAT_ACC_CODE,";
                    SQL = SQL + " VATACCNAME, RM_MPEM_VAT_TYPE_CODE,";
                    SQL = SQL + " RM_MPEM_VAT_OUTPUT_ACC_CODE,vatoutputaccname,RM_MPEM_TAX_APPLICABLE_AMOUNT,RM_MPEM_NON_TAX_APPL_AMOUNT,RM_MPEM_VAT_RC_AMOUNT,";
                    SQL = SQL + " RM_UOM_TOLL_CODE  , RM_POD_TOLL_RATE  , RM_MRD_TOLL_AMOUNT	 , ";
                    SQL = SQL + " RM_POD_TOLL_RATE_NEW  , RM_MRD_TOLL_AMOUNT_NEW	 , ";
                    SQL = SQL + " RM_MRD_TOLL_PE_ENTRY_YN	,   RM_MRD_TOLL_PE_ENTRY_NO,  RM_MRD_TOLL_PE_ENTRY_FINYR_ID	,   ";
                    SQL = SQL + "       MASTER_BRANCH_CODE ,   MASTER_BRANCH_NAME , ";
                    SQL = SQL + "       MASTER_BRANCHDOC_PREFIX,  MASTER_BRANCH_POBOX,  MASTER_BRANCH_ADDRESS, ";
                    SQL = SQL + "       MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE,  MASTER_BRANCH_FAX, ";
                    SQL = SQL + "       MASTER_BRANCH_SPONSER_NAME,  MASTER_BRANCH_TRADE_LICENSE_NO,  MASTER_BRANCH_EMAIL_ID, ";
                    SQL = SQL + "       MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER,RM_MPEM_INV_RECIVED_DATE ";

                    SQL = SQL + " FROM RM_PURCHASE_TRNS_RCPT_TOLL_VW ";
                    SQL = SQL + " WHERE  ";
                    //SQL = SQL + "  RM_MPEM_ENTRY_NO ='" + sEntryNo + "'  ";
                    SQL = SQL + " PEFINYR =" + mngrclass.FinYearID + "";

                    if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and  RM_MPEM_ENTRY_NO >=" + iFromInvoiceNumber + "";
                    }

                    if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and RM_MPEM_ENTRY_NO  <=" + iToInvoiceNumber + "";
                    }


                    dsPoStatus = clsSQLHelper.GetDataset(SQL);
                    dsPoStatus.Tables[0].TableName = "RM_PURCHASE_TRNS_RCPT_PRINT_VW";

                }
                else if (sType == "TRANPORTER_STOCK_TRANSFER")
                {

                    SQL = "     SELECT  ";
                    SQL = SQL + "   RM_MRM_APPROVED_NO, RM_MRD_APPROVED_FINYRID, RM_MRM_RECEIPT_NO, RECEIPTFINYR, RM_MRM_RECEIPT_DATE,  ";
                    SQL = SQL + "   PEVENDORCODE, SUPPNAME, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, RM_MPOM_PRICE_TYPE,  ";
                    SQL = SQL + "   RM_MRD_SL_NO, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION,  ";
                    SQL = SQL + "   RM_UM_UOM_CODE, RM_UM_UOM_DESC, RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC, RM_MTRANSPO_ORDER_NO,  ";
                    SQL = SQL + "   RM_MTRANSPO_FIN_FINYRID,RM_MSTD_TRNSPORTER_DOC_NO RM_MSTD_TRNSPORTER_DOC_NO, RM_MRD_ORDER_QTY, RM_MRD_VENDOR_DOC_NO, FA_FAM_ASSET_CODE,  ";
                    SQL = SQL + "   RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME, RM_MRD_SUPPLY_QTY, RM_MRD_WEIGH_QTY,  ";
                    SQL = SQL + "   RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY, RM_MRD_SUPP_UNIT_PRICE, RM_MRD_SUPP_AMOUNT, RM_MRD_TRANS_RATE,  ";
                    SQL = SQL + "   RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT, RM_MRD_SUPP_UNIT_PRICE_NEW, RM_MRD_SUPP_AMOUNT_NEW, RM_MRD_TRANS_RATE_NEW,  ";
                    SQL = SQL + "   RM_MRD_TRANS_AMOUNT_NEW, RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_SUPP_PENO, RM_MRD_TRANS_PENO, RM_MRM_RECEIPT_UNIQE_NO,  ";
                    SQL = SQL + "   RM_MPEM_ENTRY_NO, PEFINYR, RM_MPEM_ENTRY_DATE, RM_MPEM_ENTRY_TYPE, RM_MPEM_TRANS_TYPE,  ";
                    SQL = SQL + "   RM_MPEM_VENDOR_INV_NO, RM_MPEM_VENDOR_INV_DATE, RM_MPEM_GRAND_TOTAL, RM_MPEM_DISC_AMT, RM_MPEM_STK_ADJ_AMT,  ";
                    SQL = SQL + "   RM_MPEM_INVOICE_TOTAL, RM_MPEM_NET_TOTAL, RM_MPEM_BALANCE_TOTAL, RM_MPEM_DISC_ACC_CODE,DISCACCNAME, RM_MPEM_STKADJ_ACCCODE,STKADJACCNAME,  ";
                    SQL = SQL + "   RM_MPEM_REMARKS, RM_MPEM_CREATEDBY, RM_MPEM_VERIFIED, RM_MPEM_VERIFIED_BY, RM_MPEM_APPROVED,  ";
                    SQL = SQL + "   RM_MPEM_APPROVED_BY, ";
                    SQL = SQL + "   RM_MPEM_VAT_PERCENTAGE,RM_MPEM_VAT_AMOUNT,";
                    SQL = SQL + "   RM_MPEM_VAT_ACC_CODE, VATACCNAME,RM_MPEM_VAT_TYPE_CODE ,";
                    SQL = SQL + "   RM_MPEM_VAT_OUTPUT_ACC_CODE, vatoutputaccname, ";
                    SQL = SQL + "   RM_MPEM_TAX_APPLICABLE_AMOUNT, ";
                    SQL = SQL + "   RM_MPEM_NON_TAX_APPL_AMOUNT,RM_MPEM_VAT_RC_AMOUNT ,";
                    SQL = SQL + "       MASTER_BRANCH_CODE ,   MASTER_BRANCH_NAME , ";
                    SQL = SQL + "       MASTER_BRANCHDOC_PREFIX,  MASTER_BRANCH_POBOX,  MASTER_BRANCH_ADDRESS, ";
                    SQL = SQL + "       MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE,  MASTER_BRANCH_FAX, ";
                    SQL = SQL + "       MASTER_BRANCH_SPONSER_NAME,  MASTER_BRANCH_TRADE_LICENSE_NO,  MASTER_BRANCH_EMAIL_ID, ";
                    SQL = SQL + "       MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER,RM_MPEM_INV_RECIVED_DATE ";

                    SQL = SQL + "   FROM  ";
                    SQL = SQL + "   RM_PURCHASE_STOCK_TRN_PRINT_VW ";
                    SQL = SQL + " WHERE  ";
                    // SQL = SQL + "  RM_MPEM_ENTRY_NO ='" + sEntryNo + "'  ";
                    SQL = SQL + "PEFINYR=" + mngrclass.FinYearID + "";

                    if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and  RM_MPEM_ENTRY_NO >=" + iFromInvoiceNumber + "";
                    }

                    if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and RM_MPEM_ENTRY_NO  <=" + iToInvoiceNumber + "";
                    }


                    dsPoStatus = clsSQLHelper.GetDataset(SQL);
                    dsPoStatus.Tables[0].TableName = "RM_PURCHASE_TRNS_RCPT_PRINT_VW";
                }


                SQL = " SELECT  ";
                SQL = SQL + "    RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_ENTRY_NO,   RM_PE_MASTER_DEBIT_ACCOUNTS.AD_FIN_FINYRID,   RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_SL_NO,  ";
                SQL = SQL + "    RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_DEBIT_ACCCODE,   GL_COA_MASTER.GL_COAM_ACCOUNT_NAME , RM_MPEM_DEBIT_REMARKS,";
                SQL = SQL + " RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_DEBIT_AMOUNT,RM_PE_MASTER_DEBIT_ACCOUNTS.GL_COSM_ACCOUNT_CODE,GL_COSM_ACCOUNT_NAME ";
                SQL = SQL + "    FROM RM_PE_MASTER_DEBIT_ACCOUNTS , GL_COA_MASTER,GL_COSTING_MASTER ";
                SQL = SQL + "    WHERE  ";
                SQL = SQL + "    RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_DEBIT_ACCCODE =  GL_COA_MASTER.GL_COAM_ACCOUNT_CODE ";
                SQL = SQL + "    AND RM_PE_MASTER_DEBIT_ACCOUNTS.GL_COSM_ACCOUNT_CODE = GL_COSTING_MASTER.GL_COSM_ACCOUNT_CODE (+)";
                //SQL = SQL + "    and RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_ENTRY_NO='" + sEntryNo + "' ";
                SQL = SQL + " AND  RM_PE_MASTER_DEBIT_ACCOUNTS.AD_FIN_FINYRID=" + mngrclass.FinYearID + "";


                if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                {
                    SQL = SQL + "  and  RM_MPEM_ENTRY_NO >=" + iFromInvoiceNumber + "";
                }

                if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                {
                    SQL = SQL + "  and RM_MPEM_ENTRY_NO  <=" + iToInvoiceNumber + "";
                }



                SQL = SQL + "    order by  RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_SL_NO asc ";



                dtDetailsData = clsSQLHelper.GetDataTableByCommand(SQL);
                dtDetailsData.TableName = "RM_PURCHASE_DETAILS_DATA";
                dsPoStatus.Tables.Add(dtDetailsData);

                SQL = " SELECT    ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_ENTRY_NO, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.AD_FIN_FINYRID, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_UPDATESTOCK_YN, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_DEBITNOTE_NO, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_REF_CODE, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_RMM_RM_CODE, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_RM_ACCCODE, ";
                SQL = SQL + " RM_ACCOUNT.GL_COAM_ACCOUNT_NAME RM_ACCOUNT_NAME, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_DEBIT_ACCCODE, ";
                SQL = SQL + " DEBIT_ACCOUNT.GL_COAM_ACCOUNT_NAME DEBIT_ACCOUNT_NAME, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_NARRATION, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_GRAND_TOTAL_AMOUNT, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_VAT_TYPE, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_VAT_RATE, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_VAT_AMOUNT, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_NET_TOTAL_AMOUNT ";
                SQL = SQL + " FROM RM_PE_MASTER_DEBITNOTE,RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + " GL_COA_MASTER RM_ACCOUNT,GL_COA_MASTER DEBIT_ACCOUNT ";
                SQL = SQL + " WHERE RM_PE_MASTER_DEBITNOTE.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_PE_MASTER_DEBITNOTE.RM_MPEM_RM_ACCCODE = RM_ACCOUNT.GL_COAM_ACCOUNT_CODE ";
                SQL = SQL + " AND RM_PE_MASTER_DEBITNOTE.RM_MPEM_DEBIT_ACCCODE = DEBIT_ACCOUNT.GL_COAM_ACCOUNT_CODE(+) ";
                //SQL = SQL + " AND RM_PE_MASTER_DEBITNOTE.RM_MPEM_ENTRY_NO = '" + sEntryNo + "' ";
                SQL = SQL + " AND RM_PE_MASTER_DEBITNOTE.AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";

                if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                {
                    SQL = SQL + "  and  RM_MPEM_ENTRY_NO >=" + iFromInvoiceNumber + "";
                }

                if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                {
                    SQL = SQL + "  and RM_MPEM_ENTRY_NO  <=" + iToInvoiceNumber + "";
                }


                SQL = SQL + " ORDER BY RM_PE_MASTER_DEBITNOTE.RM_MPEM_SL_NO ";


                dtDetailsData = clsSQLHelper.GetDataTableByCommand(SQL);
                dtDetailsData.TableName = "RM_PURCHASE_DEBITNOTE_DATA";
                dsPoStatus.Tables.Add(dtDetailsData);

                
                // JOMY ADDED BUT NOT INCLUDED IN CPRINT 31 AUG 2022
                SQL = "  SELECT   ";
                SQL = SQL + "    GLPE_WSPE_RMPE_AD_BR_CODE,VOUCHERNO,AD_FIN_FINYRID, ";
                SQL = SQL + "    VOUCHERDATE,ENTRY_TYPE,RM_VM_VENDOR_CODE, ";
                SQL = SQL + "    RM_VM_VENDOR_NAME,VENDOR_CODE,PAID_TO,NARRATION,CREDITACCOUNT, ";
                SQL = SQL + "    ACCOUNT_NAME,GRAND_TOTAL,NON_ALLOCATION_AMOUNT,ENTRYNO, ";
                SQL = SQL + "    ENTRYFINYEARID,GL_MPI_DOC_TYPE,BOOKINGTYPE,BOOKINGNO, ";
                SQL = SQL + "    BOOKINGDATE,BOOKINGFINYEARID,VENDOR_INV_NO, ";
                SQL = SQL + "    VENDOR_INV_DATE,INVOCIEAMOUNT,UNMATCHED,GL_MPI_MATCH_AMOUNT,PENDING, ";
                SQL = SQL + "    ACTUAL_BALANCE_TOMATCH,GL_MPI_VAT_AMOUNT,GL_MPI_VAT_PERCENTAGE, ";
                SQL = SQL + "    GL_MPI_SL_NO,PURENTRY_BRANCH_CODE ";
                SQL = SQL + "FROM   ";
                SQL = SQL + "    GL_MATCH_PAYMENT_ADVNCE_DTLS_VW   "; 

                SQL = SQL + " where GL_MPI_DOC_TYPE    ='RTMPE'";
                SQL = SQL + " AND ENTRYFINYEARID      =" + mngrclass.FinYearID + "";
                if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                {
                    SQL = SQL + "  and  ENTRYNO  >=" + iFromInvoiceNumber + "";
                }

                if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                {
                    SQL = SQL + "  and ENTRYNO    <=" + iToInvoiceNumber + "";
                }


                DataTable dtAdvanceMatchDetails = new DataTable();
                dtAdvanceMatchDetails = clsSQLHelper.GetDataTableByCommand(SQL);
                dtAdvanceMatchDetails.TableName = "GL_MATCH_PAYMENT_ADVANCE_DTLS_VW";
                dsPoStatus.Tables.Add(dtAdvanceMatchDetails);





            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dsPoStatus;

        }


        public DataSet FetchVourcherPrintDataSummary ( double iFromInvoiceNumber, double iToInvoiceNumber, string sType, object mngrclassobj )
        {
            DataSet dsPoStatus = new DataSet("RMAPPROVALPRINT");
            DataTable dtDetailsData = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                if (sType == "VENDOR")
                {

                    //SQL = " SELECT RM_MRM_APPROVED_NO, RM_MRD_APPROVED_FINYRID, RM_MRM_RECEIPT_NO, RECEIPTFINYR, RM_MRM_RECEIPT_DATE,  ";
                    //SQL = SQL + " PEVENDORCODE, SUPPNAME, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, RM_MPOM_PRICE_TYPE,  ";
                    //SQL = SQL + " RM_MRD_SL_NO, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION,  ";
                    //SQL = SQL + " RM_UM_UOM_CODE, RM_UM_UOM_DESC, RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC, RM_MRM_TRANSPORTER_CODE,  ";
                    //SQL = SQL + " TRNSPORTERNAME, RM_MTRANSPO_ORDER_NO, RM_MTRANSPO_FIN_FINYRID, RM_MRM_TRNSPORTER_DOC_NO, RM_MRD_ORDER_QTY,  ";
                    //SQL = SQL + " RM_MRD_VENDOR_DOC_NO, FA_FAM_ASSET_CODE, RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME,  ";
                    //SQL = SQL + " RM_MRD_SUPPLY_QTY, RM_MRD_WEIGH_QTY, RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY, RM_MRD_SUPP_UNIT_PRICE,  ";
                    //SQL = SQL + " RM_MRD_SUPP_AMOUNT, RM_MRD_TRANS_RATE, RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT, RM_MRD_SUPP_UNIT_PRICE_NEW,  ";
                    //SQL = SQL + " RM_MRD_SUPP_AMOUNT_NEW, RM_MRD_TRANS_RATE_NEW, RM_MRD_TRANS_AMOUNT_NEW, RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_SUPP_PENO,  ";
                    //SQL = SQL + " RM_MRD_TRANS_PENO, RM_MRM_RECEIPT_UNIQE_NO, RM_MPEM_ENTRY_NO, PEFINYR, RM_MPEM_ENTRY_DATE,  ";
                    //SQL = SQL + " RM_MPEM_ENTRY_TYPE, RM_MPEM_TRANS_TYPE, RM_MPEM_VENDOR_INV_NO, RM_MPEM_VENDOR_INV_DATE, RM_MPEM_GRAND_TOTAL,  ";
                    //SQL = SQL + " RM_MPEM_DISC_AMT, RM_MPEM_STK_ADJ_AMT, RM_MPEM_INVOICE_TOTAL, RM_MPEM_NET_TOTAL, RM_MPEM_BALANCE_TOTAL,  ";
                    //SQL = SQL + " RM_MPEM_DISC_ACC_CODE, RM_MPEM_STKADJ_ACCCODE, RM_MPEM_REMARKS, RM_MPEM_CREATEDBY, RM_MPEM_VERIFIED,  ";
                    //SQL = SQL + " RM_MPEM_VERIFIED_BY, RM_MPEM_APPROVED, RM_MPEM_APPROVED_BY ";
                    //SQL = SQL + " FROM  ";
                    //SQL = SQL + " RM_PURCHASE_ENTRY_PRINT_VW  ";
                    //SQL = SQL + " WHERE  ";
                    //SQL = SQL + "  RM_MPEM_ENTRY_NO ='" + sEntryNo + "'  and  PEFINYR=" + mngrclass.FinYearID + "";
                    // in report suppressing its take long time to get the  corrct values group by query 

                    SQL = "   select     ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_ENTRY_NO,  RM_PURCHASE_ENTRY_PRINT_VW.AD_FIN_FINYRID PEFINYR,   ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_ENTRY_DATE,  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_ENTRY_TYPE,   ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_TRANS_TYPE,    RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_VENDOR_INV_NO,   ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_VENDOR_INV_DATE, RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_INV_RECIVED_DATE ,  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_GRAND_TOTAL,  ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_DISC_AMT,  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_STK_ADJ_AMT,  ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_INVOICE_TOTAL,  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_NET_TOTAL,  ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_BALANCE_TOTAL,   ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_DISC_ACC_CODE, coadisc.gl_coam_account_name discaccname, ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_STKADJ_ACCCODE,coaadj.gl_coam_account_name stkadjaccname,  ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_REMARKS,  ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW. RM_MPEM_CREATEDBY,    RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_VERIFIED,  ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW. RM_MPEM_VERIFIED_BY,  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_APPROVED,   ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW. RM_MPEM_APPROVED_BY ,  ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.rm_mpem_vat_percentage, ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.rm_mpem_vat_amount, ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.rm_mpem_vat_acc_code, ";
                    SQL = SQL + "  vatacc.gl_coam_account_name vataccname,   ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.rm_mpem_vat_type_code , ";
                    SQL = SQL + "  RM_MPEM_VAT_OUTPUT_ACC_CODE,vatrcacc.gl_coam_account_name  vatoutputaccname, ";
                    SQL = SQL + "  RM_MPEM_TAX_APPLICABLE_AMOUNT, ";
                    SQL = SQL + "  RM_MPEM_NON_TAX_APPL_AMOUNT,RM_MPEM_VAT_RC_AMOUNT , ";
                    SQL = SQL + "  RECEPIT_DETAILS.PEVENDORCODE     ,   ";
                    SQL = SQL + "  RECEPIT_DETAILS. SUPPNAME,   ";
                    SQL = SQL + "  RECEPIT_DETAILS.SALES_STN_STATION_CODE,   ";
                    SQL = SQL + "  RECEPIT_DETAILS.SALES_STN_STATION_NAME,   ";
                    SQL = SQL + "          MASTER_BRANCH_CODE, ";
                    SQL = SQL + "          MASTER_BRANCH_NAME, ";
                    SQL = SQL + "          MASTER_BRANCHDOC_PREFIX, ";
                    SQL = SQL + "          MASTER_BRANCH_POBOX, ";
                    SQL = SQL + "          MASTER_BRANCH_ADDRESS, ";
                    SQL = SQL + "          MASTER_BRANCH_CITY, ";
                    SQL = SQL + "          MASTER_BRANCH_PHONE, ";
                    SQL = SQL + "          MASTER_BRANCH_FAX, ";
                    SQL = SQL + "          MASTER_BRANCH_SPONSER_NAME, ";
                    SQL = SQL + "          MASTER_BRANCH_TRADE_LICENSE_NO, ";
                    SQL = SQL + "          MASTER_BRANCH_EMAIL_ID, ";
                    SQL = SQL + "          MASTER_BRANCH_WEB_SITE, ";
                    SQL = SQL + "          MASTER_BRANCH_VAT_REG_NUMBER, ";
                    SQL = SQL + "  RECEPIT_DETAILS.RM_RMM_RM_CODE,   ";
                    SQL = SQL + "  RECEPIT_DETAILS.RM_RMM_RM_DESCRIPTION,   ";
                    SQL = SQL + "  RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY,  ";
                    SQL = SQL + "  RECEPIT_DETAILS.RM_MRD_WEIGH_QTY,  ";
                    SQL = SQL + "  RECEPIT_DETAILS.RM_MRD_APPROVE_QTY ,  ";
                    SQL = SQL + "  RECEPIT_DETAILS.RM_MRD_REJECTED_QTY ,    ";
                    SQL = SQL + "  RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT,   ";
                    SQL = SQL + "  RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT,   ";
                    SQL = SQL + "  RECEPIT_DETAILS. RM_MRD_TOTAL_AMOUNT,   ";
                    SQL = SQL + "  RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW,   ";
                    SQL = SQL + "  RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW,  ";
                    SQL = SQL + "  RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW ,RM_MRD_SUPP_UNIT_PRICE_NEW,  ";
                    SQL = SQL + "  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_PE_PO_NO  ";
                    SQL = SQL + " from  RM_pe_master  RM_PURCHASE_ENTRY_PRINT_VW , ";
                    SQL = SQL + "  (select   ";
                    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO ,  RM_RECEPIT_DETAILS.RM_MPED_VFINYR_ID,  ";
                    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE PEVENDORCODE     ,   ";
                    SQL = SQL + "  RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPNAME,   ";
                    SQL = SQL + "  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,   ";
                    SQL = SQL + "  SL_STATION_MASTER.SALES_STN_STATION_NAME,   ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_CODE MASTER_BRANCH_CODE, ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_NAME MASTER_BRANCH_NAME, ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_DOC_PREFIX MASTER_BRANCHDOC_PREFIX, ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_POBOX MASTER_BRANCH_POBOX, ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_ADDRESS MASTER_BRANCH_ADDRESS, ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_CITY MASTER_BRANCH_CITY, ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_PHONE MASTER_BRANCH_PHONE, ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_FAX MASTER_BRANCH_FAX, ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_SPONSER_NAME MASTER_BRANCH_SPONSER_NAME, ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_TRADE_LICENSE_NO MASTER_BRANCH_TRADE_LICENSE_NO, ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_EMAIL_ID MASTER_BRANCH_EMAIL_ID, ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_WEB_SITE MASTER_BRANCH_WEB_SITE, ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_TAX_VAT_REG_NUMBER MASTER_BRANCH_VAT_REG_NUMBER, ";

                    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,   ";
                    SQL = SQL + "  RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,   RM_MRD_SUPP_UNIT_PRICE_NEW,  ";
                    SQL = SQL + "  sum( RM_MRD_SUPPLY_QTY)RM_MRD_SUPPLY_QTY,  ";
                    SQL = SQL + "  sum(RM_MRD_WEIGH_QTY)RM_MRD_WEIGH_QTY,  ";
                    SQL = SQL + "  sum( RM_MRD_APPROVE_QTY) RM_MRD_APPROVE_QTY,  ";
                    SQL = SQL + "  sum( RM_MRD_REJECTED_QTY)RM_MRD_REJECTED_QTY,    ";
                    SQL = SQL + "  sum(  RM_MRD_SUPP_AMOUNT)RM_MRD_SUPP_AMOUNT,   ";
                    SQL = SQL + "  sum( RM_MRD_TRANS_AMOUNT)RM_MRD_TRANS_AMOUNT,   ";
                    SQL = SQL + "  sum( RM_MRD_TOTAL_AMOUNT) RM_MRD_TOTAL_AMOUNT,   ";
                    SQL = SQL + "  sum(RM_MRD_SUPP_AMOUNT_NEW)RM_MRD_SUPP_AMOUNT_NEW,   ";
                    SQL = SQL + "  sum(RM_MRD_TRANS_AMOUNT_NEW)RM_MRD_TRANS_AMOUNT_NEW,  ";
                    SQL = SQL + "  sum(RM_MRD_TOTAL_AMOUNT_NEW)RM_MRD_TOTAL_AMOUNT_NEW  ";
                    SQL = SQL + "  FROM RM_RECEPIT_DETAILS,  RM_VENDOR_MASTER,  ";
                    SQL = SQL + "  RM_RAWMATERIAL_MASTER, SL_STATION_MASTER,ad_branch_master MASTER_BRANCH   ";
                    SQL = SQL + "  where   ";
                    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE     ";
                    SQL = SQL + "  AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE   ";
                    SQL = SQL + "  AND RM_RECEPIT_DETAILS.AD_BR_CODE =  MASTER_BRANCH.AD_BR_CODE(+)   ";
                    SQL = SQL + "  AND  RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE   ";
                    SQL = SQL + "  group by    ";
                    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO ,  RM_RECEPIT_DETAILS.RM_MPED_VFINYR_ID,  ";
                    SQL = SQL + "   RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,   ";
                    SQL = SQL + "  SL_STATION_MASTER.SALES_STN_STATION_NAME,   ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_CODE , ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_NAME , ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_DOC_PREFIX , ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_POBOX , ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_ADDRESS , ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_CITY , ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_PHONE , ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_FAX , ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_SPONSER_NAME , ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_TRADE_LICENSE_NO , ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_EMAIL_ID , ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_WEB_SITE , ";
                    SQL = SQL + "          MASTER_BRANCH.AD_BR_TAX_VAT_REG_NUMBER , ";

                    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,   ";
                    SQL = SQL + "  RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,    RM_MRD_SUPP_UNIT_PRICE_NEW, ";
                    SQL = SQL + "   RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE       ,   ";
                    SQL = SQL + "  RM_VENDOR_MASTER.RM_VM_VENDOR_NAME ";

                    SQL = SQL + "                 union all ";
                    SQL = SQL + "                 SELECT RM_RECEPIT_TOLL.RM_MRD_SUPP_PENO, ";
                    SQL = SQL + "                 RM_RECEPIT_TOLL.RM_MPED_VFINYR_ID, ";
                    SQL = SQL + "                 RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE ";
                    SQL = SQL + "                     PEVENDORCODE, ";
                    SQL = SQL + "                 RM_VENDOR_MASTER.RM_VM_VENDOR_NAME ";
                    SQL = SQL + "                     SUPPNAME, ";
                    SQL = SQL + "                 RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                    SQL = SQL + "                 SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_CODE ";
                    SQL = SQL + "                     MASTER_BRANCH_CODE, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_NAME ";
                    SQL = SQL + "                     MASTER_BRANCH_NAME, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_DOC_PREFIX ";
                    SQL = SQL + "                     MASTER_BRANCHDOC_PREFIX, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_POBOX ";
                    SQL = SQL + "                     MASTER_BRANCH_POBOX, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_ADDRESS ";
                    SQL = SQL + "                     MASTER_BRANCH_ADDRESS, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_CITY ";
                    SQL = SQL + "                     MASTER_BRANCH_CITY, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_PHONE ";
                    SQL = SQL + "                     MASTER_BRANCH_PHONE, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_FAX ";
                    SQL = SQL + "                     MASTER_BRANCH_FAX, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_SPONSER_NAME ";
                    SQL = SQL + "                     MASTER_BRANCH_SPONSER_NAME, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_TRADE_LICENSE_NO ";
                    SQL = SQL + "                     MASTER_BRANCH_TRADE_LICENSE_NO, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_EMAIL_ID ";
                    SQL = SQL + "                     MASTER_BRANCH_EMAIL_ID, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_WEB_SITE ";
                    SQL = SQL + "                     MASTER_BRANCH_WEB_SITE, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_TAX_VAT_REG_NUMBER ";
                    SQL = SQL + "                     MASTER_BRANCH_VAT_REG_NUMBER, ";
                    SQL = SQL + "                 RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                    SQL = SQL + "                 RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,  RM_MRM_APRPOVED_RATE RM_MRD_SUPP_UNIT_PRICE_NEW ,";
                    SQL = SQL + "                 SUM (RM_MRM_APPROVED_QTY) ";
                    SQL = SQL + "                     RM_MRD_SUPPLY_QTY, ";
                    SQL = SQL + "                 SUM (RM_MRM_APPROVED_QTY) ";
                    SQL = SQL + "                     RM_MRD_WEIGH_QTY, ";
                    SQL = SQL + "                 SUM (RM_MRM_APPROVED_QTY) ";
                    SQL = SQL + "                     RM_MRD_APPROVE_QTY, ";
                    SQL = SQL + "              0 ";
                    SQL = SQL + "                     RM_MRD_REJECTED_QTY, ";
                    SQL = SQL + "                 SUM (RM_MRM_APPROVED_AMOUNT) ";
                    SQL = SQL + "                     RM_MRD_SUPP_AMOUNT, ";
                    SQL = SQL + "                0 ";
                    SQL = SQL + "                     RM_MRD_TRANS_AMOUNT, ";
                    SQL = SQL + "                 SUM (RM_MRM_APPROVED_AMOUNT) ";
                    SQL = SQL + "                     RM_MRD_TOTAL_AMOUNT, ";
                    SQL = SQL + "                 SUM (RM_MRM_APPROVED_AMOUNT) ";
                    SQL = SQL + "                     RM_MRD_SUPP_AMOUNT_NEW, ";
                    SQL = SQL + "                 0 ";
                    SQL = SQL + "                     RM_MRD_TRANS_AMOUNT_NEW, ";
                    SQL = SQL + "                 SUM (RM_MRM_APPROVED_AMOUNT) ";
                    SQL = SQL + "                     RM_MRD_TOTAL_AMOUNT_NEW  ";
                    SQL = SQL + "            FROM RM_RECEPIT_TOLL, ";
                    SQL = SQL + "                 RM_VENDOR_MASTER, ";
                    SQL = SQL + "                 RM_RAWMATERIAL_MASTER, ";
                    SQL = SQL + "                 SL_STATION_MASTER, ";
                    SQL = SQL + "                 ad_branch_master MASTER_BRANCH,RM_RECEPIT_DETAILS ";
                    SQL = SQL + "           WHERE     RM_RECEPIT_TOLL.RM_RMM_RM_CODE = ";
                    SQL = SQL + "                     RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                    SQL = SQL + "                 AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = ";
                    SQL = SQL + "                     SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                    SQL = SQL + "             AND RM_RECEPIT_DETAILS.AD_BR_CODE = ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_CODE(+) ";
                    SQL = SQL + "                  AND RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO = ";
                    SQL = SQL + "                 RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO(+) ";
                    SQL = SQL + "                 AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                    SQL = SQL + "                     RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                    SQL = SQL + "        GROUP BY RM_RECEPIT_TOLL.RM_MRD_SUPP_PENO, ";
                    SQL = SQL + "                 RM_RECEPIT_TOLL.RM_MPED_VFINYR_ID, ";
                    SQL = SQL + "                 RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                    SQL = SQL + "                 SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_CODE, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_NAME, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_DOC_PREFIX, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_POBOX, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_ADDRESS, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_CITY, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_PHONE, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_FAX, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_SPONSER_NAME, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_TRADE_LICENSE_NO, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_EMAIL_ID, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_WEB_SITE, ";
                    SQL = SQL + "                 MASTER_BRANCH.AD_BR_TAX_VAT_REG_NUMBER, ";
                    SQL = SQL + "                 RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                    SQL = SQL + "                 RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                    SQL = SQL + "                 RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE, ";
                    SQL = SQL + "                 RM_VENDOR_MASTER.RM_VM_VENDOR_NAME  ,RM_MRM_APRPOVED_RATE ";

                    SQL = SQL + " )  RECEPIT_DETAILS , ";
                    SQL = SQL + "  gl_coa_master coadisc, ";
                    SQL = SQL + "  gl_coa_master coaadj, ";
                    SQL = SQL + "  gl_coa_master vatacc ,gl_coa_master vatrcacc ";
                    SQL = SQL + "  where   ";
                    SQL = SQL + "  RECEPIT_DETAILS.RM_MRD_SUPP_PENO =  RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_ENTRY_NO   ";
                    SQL = SQL + "  AND  RECEPIT_DETAILS.RM_MPED_VFINYR_ID =RM_PURCHASE_ENTRY_PRINT_VW.AD_FIN_FINYRID   ";
                    SQL = SQL + "  AND RM_PURCHASE_ENTRY_PRINT_VW.rm_mpem_disc_acc_code = coadisc.gl_coam_account_code(+) ";
                    SQL = SQL + "  AND RM_PURCHASE_ENTRY_PRINT_VW.rm_mpem_stkadj_acccode = coaadj.gl_coam_account_code(+) ";
                    SQL = SQL + "  AND RM_PURCHASE_ENTRY_PRINT_VW.rm_mpem_vat_acc_code = vatacc.gl_coam_account_code(+) ";
                    SQL = SQL + "  AND RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_VAT_OUTPUT_ACC_CODE =  vatrcacc.gl_coam_account_code(+)";

                    // SQL = SQL + "  and RECEPIT_DETAILS.RM_MRD_SUPP_PENO ='" + sEntryNo + "' ";
                    SQL = SQL + " AND RECEPIT_DETAILS.RM_MPED_VFINYR_ID=" + mngrclass.FinYearID + "";


                    //SQL = SQL + "  and     RM_PURCHASE_ENTRY_PRINT_VW.RM_MPEM_ENTRY_NO ='" + sEntryNo + "'";
                    //SQL = SQL + "  and  RM_PURCHASE_ENTRY_PRINT_VW.AD_FIN_FINYRID  =" + mngrclass.FinYearID + "";

                    if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and  RM_MPEM_ENTRY_NO >=" + iFromInvoiceNumber + "";
                    }

                    if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and RM_MPEM_ENTRY_NO  <=" + iToInvoiceNumber + "";

                        dsPoStatus = clsSQLHelper.GetDataset(SQL);
                        dsPoStatus.Tables[0].TableName = "RM_PURCHASE_ENTRY_PRINT_VW";
                    }
                    else if (sType == "TRANPORTER_OR_RECEIPT")
                    {
                        ///   
                        /// 

                        SQL = " SELECT  ";
                        SQL = SQL + " RM_MRM_APPROVED_NO, RM_MRD_APPROVED_FINYRID, RM_MRM_RECEIPT_NO, RECEIPTFINYR, RM_MRM_RECEIPT_DATE,  ";
                        SQL = SQL + " PEVENDORCODE, SUPPNAME, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, RM_MPOM_PRICE_TYPE,  ";
                        SQL = SQL + " RM_MRD_SL_NO, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION,  ";
                        SQL = SQL + " RM_UM_UOM_CODE, RM_UM_UOM_DESC, RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC, RM_MTRANSPO_ORDER_NO,  ";
                        SQL = SQL + " RM_MTRANSPO_FIN_FINYRID, RM_MRM_TRNSPORTER_DOC_NO, RM_MRD_ORDER_QTY, RM_MRD_VENDOR_DOC_NO, FA_FAM_ASSET_CODE,  ";
                        SQL = SQL + " RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME, RM_MRD_SUPPLY_QTY, RM_MRD_WEIGH_QTY,  ";
                        SQL = SQL + " RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY, RM_MRD_SUPP_UNIT_PRICE, RM_MRD_SUPP_AMOUNT, RM_MRD_TRANS_RATE,  ";
                        SQL = SQL + " RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT, RM_MRD_SUPP_UNIT_PRICE_NEW, RM_MRD_SUPP_AMOUNT_NEW, RM_MRD_TRANS_RATE_NEW,  ";
                        SQL = SQL + " RM_MRD_TRANS_AMOUNT_NEW, RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_SUPP_PENO, RM_MRD_TRANS_PENO, RM_MRM_RECEIPT_UNIQE_NO,  ";
                        SQL = SQL + " RM_MPEM_ENTRY_NO, PEFINYR, RM_MPEM_ENTRY_DATE, RM_MPEM_ENTRY_TYPE, RM_MPEM_TRANS_TYPE,  ";
                        SQL = SQL + " RM_MPEM_VENDOR_INV_NO, RM_MPEM_VENDOR_INV_DATE, RM_MPEM_GRAND_TOTAL, RM_MPEM_DISC_AMT, RM_MPEM_STK_ADJ_AMT,  ";
                        SQL = SQL + " RM_MPEM_INVOICE_TOTAL, RM_MPEM_NET_TOTAL, RM_MPEM_BALANCE_TOTAL, RM_MPEM_DISC_ACC_CODE, RM_MPEM_STKADJ_ACCCODE,  ";
                        SQL = SQL + " RM_MPEM_REMARKS, RM_MPEM_CREATEDBY, RM_MPEM_VERIFIED, RM_MPEM_VERIFIED_BY, RM_MPEM_APPROVED,  ";
                        SQL = SQL + " RM_MPEM_APPROVED_BY ,";
                        SQL = SQL + "       MASTER_BRANCH_CODE ,   MASTER_BRANCH_NAME , ";
                        SQL = SQL + "       MASTER_BRANCHDOC_PREFIX,  MASTER_BRANCH_POBOX,  MASTER_BRANCH_ADDRESS, ";
                        SQL = SQL + "       MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE,  MASTER_BRANCH_FAX, ";
                        SQL = SQL + "       MASTER_BRANCH_SPONSER_NAME,  MASTER_BRANCH_TRADE_LICENSE_NO,  MASTER_BRANCH_EMAIL_ID, ";
                        SQL = SQL + "       MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER ";
                        SQL = SQL + " FROM RM_PURCHASE_TRNS_RCPT_PRINT_VW ";
                        SQL = SQL + " WHERE  ";
                        // SQL = SQL + "  RM_MPEM_ENTRY_NO ='" + sEntryNo + "' ";
                        SQL = SQL + " PEFINYR=" + mngrclass.FinYearID + "";


                        if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                        {
                            SQL = SQL + "  and  RM_MPEM_ENTRY_NO >=" + iFromInvoiceNumber + "";
                        }

                        if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                        {
                            SQL = SQL + "  and RM_MPEM_ENTRY_NO  <=" + iToInvoiceNumber + "";
                        }

                        dsPoStatus = clsSQLHelper.GetDataset(SQL);
                        dsPoStatus = clsSQLHelper.GetDataset(SQL);
                        dsPoStatus.Tables[0].TableName = "RM_PURCHASE_TRNS_RCPT_PRINT_VW";

                    }

                    else if (sType == "TRANPORTER_STOCK_TRANSFER")
                    {

                        SQL = "     SELECT  ";
                        SQL = SQL + "   RM_MRM_APPROVED_NO, RM_MRD_APPROVED_FINYRID, RM_MRM_RECEIPT_NO, RECEIPTFINYR, RM_MRM_RECEIPT_DATE,  ";
                        SQL = SQL + "   PEVENDORCODE, SUPPNAME, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, RM_MPOM_PRICE_TYPE,  ";
                        SQL = SQL + "   RM_MRD_SL_NO, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION,  ";
                        SQL = SQL + "   RM_UM_UOM_CODE, RM_UM_UOM_DESC, RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC, RM_MTRANSPO_ORDER_NO,  ";
                        SQL = SQL + "   RM_MTRANSPO_FIN_FINYRID, RM_MRM_TRNSPORTER_DOC_NO, RM_MRD_ORDER_QTY, RM_MRD_VENDOR_DOC_NO, FA_FAM_ASSET_CODE,  ";
                        SQL = SQL + "   RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME, RM_MRD_SUPPLY_QTY, RM_MRD_WEIGH_QTY,  ";
                        SQL = SQL + "   RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY, RM_MRD_SUPP_UNIT_PRICE, RM_MRD_SUPP_AMOUNT, RM_MRD_TRANS_RATE,  ";
                        SQL = SQL + "   RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT, RM_MRD_SUPP_UNIT_PRICE_NEW, RM_MRD_SUPP_AMOUNT_NEW, RM_MRD_TRANS_RATE_NEW,  ";
                        SQL = SQL + "   RM_MRD_TRANS_AMOUNT_NEW, RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_SUPP_PENO, RM_MRD_TRANS_PENO, RM_MRM_RECEIPT_UNIQE_NO,  ";
                        SQL = SQL + "   RM_MPEM_ENTRY_NO, PEFINYR, RM_MPEM_ENTRY_DATE, RM_MPEM_ENTRY_TYPE, RM_MPEM_TRANS_TYPE,  ";
                        SQL = SQL + "   RM_MPEM_VENDOR_INV_NO, RM_MPEM_VENDOR_INV_DATE, RM_MPEM_GRAND_TOTAL, RM_MPEM_DISC_AMT, RM_MPEM_STK_ADJ_AMT,  ";
                        SQL = SQL + "   RM_MPEM_INVOICE_TOTAL, RM_MPEM_NET_TOTAL, RM_MPEM_BALANCE_TOTAL, RM_MPEM_DISC_ACC_CODE, RM_MPEM_STKADJ_ACCCODE,  ";
                        SQL = SQL + "   RM_MPEM_REMARKS, RM_MPEM_CREATEDBY, RM_MPEM_VERIFIED, RM_MPEM_VERIFIED_BY, RM_MPEM_APPROVED,  ";
                        SQL = SQL + "   RM_MPEM_APPROVED_BY ,";
                        SQL = SQL + "       MASTER_BRANCH_CODE ,   MASTER_BRANCH_NAME , ";
                        SQL = SQL + "       MASTER_BRANCHDOC_PREFIX,  MASTER_BRANCH_POBOX,  MASTER_BRANCH_ADDRESS, ";
                        SQL = SQL + "       MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE,  MASTER_BRANCH_FAX, ";
                        SQL = SQL + "       MASTER_BRANCH_SPONSER_NAME,  MASTER_BRANCH_TRADE_LICENSE_NO,  MASTER_BRANCH_EMAIL_ID, ";
                        SQL = SQL + "       MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER ";
                        SQL = SQL + "    FROM  ";
                        SQL = SQL + "    RM_PURCHASE_STOCK_TRN_PRINT_VW ";
                        SQL = SQL + " WHERE  ";
                        // SQL = SQL + "  RM_MPEM_ENTRY_NO ='" + sEntryNo + "'";
                        SQL = SQL + "  PEFINYR=" + mngrclass.FinYearID + "";




                        if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                        {
                            SQL = SQL + "  and  RM_MPEM_ENTRY_NO >=" + iFromInvoiceNumber + "";
                        }

                        if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                        {
                            SQL = SQL + "  and RM_MPEM_ENTRY_NO  <=" + iToInvoiceNumber + "";
                        }


                        dsPoStatus = clsSQLHelper.GetDataset(SQL);
                        dsPoStatus.Tables[0].TableName = "RM_PURCHASE_TRNS_RCPT_PRINT_VW";
                    }


                    SQL = " SELECT  ";
                    SQL = SQL + "    RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_ENTRY_NO,   RM_PE_MASTER_DEBIT_ACCOUNTS.AD_FIN_FINYRID,   RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_SL_NO,  ";
                    SQL = SQL + "    RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_DEBIT_ACCCODE,   GL_COA_MASTER.GL_COAM_ACCOUNT_NAME , RM_MPEM_DEBIT_REMARKS,";
                    SQL = SQL + " RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_DEBIT_AMOUNT,RM_PE_MASTER_DEBIT_ACCOUNTS.GL_COSM_ACCOUNT_CODE,GL_COSM_ACCOUNT_NAME ";
                    SQL = SQL + "    FROM RM_PE_MASTER_DEBIT_ACCOUNTS , GL_COA_MASTER,GL_COSTING_MASTER ";
                    SQL = SQL + "    WHERE  ";
                    SQL = SQL + "    RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_DEBIT_ACCCODE =  GL_COA_MASTER.GL_COAM_ACCOUNT_CODE ";
                    SQL = SQL + "    AND RM_PE_MASTER_DEBIT_ACCOUNTS.GL_COSM_ACCOUNT_CODE = GL_COSTING_MASTER.GL_COSM_ACCOUNT_CODE (+)";
                    //SQL = SQL + "    and RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_ENTRY_NO='" + sEntryNo + "' ";
                    SQL = SQL + " AND RM_PE_MASTER_DEBIT_ACCOUNTS.AD_FIN_FINYRID=" + mngrclass.FinYearID + "";


                    if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and  RM_MPEM_ENTRY_NO >=" + iFromInvoiceNumber + "";
                    }

                    if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and RM_MPEM_ENTRY_NO  <=" + iToInvoiceNumber + "";
                    }

                    SQL = SQL + "    order by  RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_SL_NO asc ";

                    dtDetailsData = clsSQLHelper.GetDataTableByCommand(SQL);
                    dtDetailsData.TableName = "RM_PURCHASE_DETAILS_DATA";
                    dsPoStatus.Tables.Add(dtDetailsData);

                    SQL = " SELECT    ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_ENTRY_NO, ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.AD_FIN_FINYRID, ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_UPDATESTOCK_YN, ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_DEBITNOTE_NO, ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_REF_CODE, ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_RMM_RM_CODE, ";
                    SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_RM_ACCCODE, ";
                    SQL = SQL + " RM_ACCOUNT.GL_COAM_ACCOUNT_NAME RM_ACCOUNT_NAME, ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_DEBIT_ACCCODE, ";
                    SQL = SQL + " DEBIT_ACCOUNT.GL_COAM_ACCOUNT_NAME DEBIT_ACCOUNT_NAME, ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_NARRATION, ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_GRAND_TOTAL_AMOUNT, ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_VAT_TYPE, ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_VAT_RATE, ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_VAT_AMOUNT, ";
                    SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_NET_TOTAL_AMOUNT ";
                    SQL = SQL + " FROM RM_PE_MASTER_DEBITNOTE,RM_RAWMATERIAL_MASTER, ";
                    SQL = SQL + " GL_COA_MASTER RM_ACCOUNT,GL_COA_MASTER DEBIT_ACCOUNT ";
                    SQL = SQL + " WHERE RM_PE_MASTER_DEBITNOTE.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                    SQL = SQL + " AND RM_PE_MASTER_DEBITNOTE.RM_MPEM_RM_ACCCODE = RM_ACCOUNT.GL_COAM_ACCOUNT_CODE ";
                    SQL = SQL + " AND RM_PE_MASTER_DEBITNOTE.RM_MPEM_DEBIT_ACCCODE = DEBIT_ACCOUNT.GL_COAM_ACCOUNT_CODE(+) ";
                    // SQL = SQL + " AND RM_PE_MASTER_DEBITNOTE.RM_MPEM_ENTRY_NO = '" + sEntryNo + "' ";
                    SQL = SQL + " AND RM_PE_MASTER_DEBITNOTE.AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";



                    if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and  RM_MPEM_ENTRY_NO >=" + iFromInvoiceNumber + "";
                    }

                    if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                    {
                        SQL = SQL + "  and RM_MPEM_ENTRY_NO  <=" + iToInvoiceNumber + "";
                    }

                    SQL = SQL + " ORDER BY RM_PE_MASTER_DEBITNOTE.RM_MPEM_SL_NO ";

                    dtDetailsData = clsSQLHelper.GetDataTableByCommand(SQL);
                    dtDetailsData.TableName = "RM_PURCHASE_DEBITNOTE_DATA";
                    dsPoStatus.Tables.Add(dtDetailsData);


                    
                // JOMY ADDED BUT NOT INCLUDED IN CPRINT 31 AUG 2022
                SQL = "  SELECT   ";
                SQL = SQL + "    GLPE_WSPE_RMPE_AD_BR_CODE,VOUCHERNO,AD_FIN_FINYRID, ";
                SQL = SQL + "    VOUCHERDATE,ENTRY_TYPE,RM_VM_VENDOR_CODE, ";
                SQL = SQL + "    RM_VM_VENDOR_NAME,VENDOR_CODE,PAID_TO,NARRATION,CREDITACCOUNT, ";
                SQL = SQL + "    ACCOUNT_NAME,GRAND_TOTAL,NON_ALLOCATION_AMOUNT,ENTRYNO, ";
                SQL = SQL + "    ENTRYFINYEARID,GL_MPI_DOC_TYPE,BOOKINGTYPE,BOOKINGNO, ";
                SQL = SQL + "    BOOKINGDATE,BOOKINGFINYEARID,VENDOR_INV_NO, ";
                SQL = SQL + "    VENDOR_INV_DATE,INVOCIEAMOUNT,UNMATCHED,GL_MPI_MATCH_AMOUNT,PENDING, ";
                SQL = SQL + "    ACTUAL_BALANCE_TOMATCH,GL_MPI_VAT_AMOUNT,GL_MPI_VAT_PERCENTAGE, ";
                SQL = SQL + "    GL_MPI_SL_NO,PURENTRY_BRANCH_CODE ";
                SQL = SQL + "FROM   ";
                SQL = SQL + "    GL_MATCH_PAYMENT_ADVNCE_DTLS_VW   "; 

                SQL = SQL + " where GL_MPI_DOC_TYPE    ='RTMPE'";
                SQL = SQL + " AND ENTRYFINYEARID      =" + mngrclass.FinYearID + "";
                if (!string.IsNullOrEmpty(Convert.ToString(iFromInvoiceNumber)) && iFromInvoiceNumber != 0)
                {
                    SQL = SQL + "  and  ENTRYNO  >=" + iFromInvoiceNumber + "";
                }

                if (!string.IsNullOrEmpty(Convert.ToString(iToInvoiceNumber)) && iToInvoiceNumber != 0)
                {
                    SQL = SQL + "  and ENTRYNO    <=" + iToInvoiceNumber + "";
                }


                DataTable dtAdvanceMatchDetails = new DataTable();
                dtAdvanceMatchDetails = clsSQLHelper.GetDataTableByCommand(SQL);
                dtAdvanceMatchDetails.TableName = "GL_MATCH_PAYMENT_ADVANCE_DTLS_VW";
                dsPoStatus.Tables.Add(dtAdvanceMatchDetails);

                }

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dsPoStatus;

        }

        #endregion

        #region "Attach DML"

        public void InsertAttachFile ( string lblCVPath, string AttachRemarks, string txtEntryCode, object mngrclassobj )
        {
            DataSet sReturn = new DataSet();
            Int32 SLNO = 0;

            try
            {
                SQL = string.Empty;
                OracleHelper oTrns = new OracleHelper();
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = "SELECT MAX(RM_MPEM_SLNO) SLNO FROM RM_PE_ATTACHMENT_DETAILS WHERE RM_MPEM_ENTRY_NO ='" + txtEntryCode + "'";
                SQL = SQL + " AND AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";

                sReturn = clsSQLHelper.GetDataset(SQL);

                if (sReturn.Tables[0].Rows[0]["slno"].ToString() == null || sReturn.Tables[0].Rows[0]["slno"].ToString() == string.Empty)
                {
                    SLNO = 1;
                }
                else
                {
                    SLNO = Convert.ToInt32(sReturn.Tables[0].Rows[0]["slno"].ToString()) + 1;
                }

                sReturn = null;
                SQL = string.Empty;

                SQL = " INSERT INTO RM_PE_ATTACHMENT_DETAILS";

                SQL = SQL + " (RM_MPEM_ENTRY_NO, AD_FIN_FINYRID, RM_MPEM_SLNO, RM_MPEM_REMARKS, RM_MPEM_FILE_PATH)";

                SQL = SQL + " Values('" + txtEntryCode + "'," + mngrclass.FinYearID + "," + SLNO + ",'" + AttachRemarks + "',";
                SQL = SQL + "'" + lblCVPath + "')";

                oTrns.GetExecuteNonQueryBySQL(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return;
        }

        public DataSet fillAttachGrid ( string txtEntryCode, object mngrclassobj )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT RM_MPEM_ENTRY_NO, RM_MPEM_REMARKS, RM_MPEM_FILE_PATH";

                SQL = SQL + " FROM RM_PE_ATTACHMENT_DETAILS";

                SQL = SQL + " WHERE RM_MPEM_ENTRY_NO='" + txtEntryCode + "' AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        public DataSet FillDebitAccountsDetails ( string txtEntryCode, object mngrclassobj )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = " SELECT  ";
                SQL = SQL + "     RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_ENTRY_NO,   RM_PE_MASTER_DEBIT_ACCOUNTS.AD_FIN_FINYRID,   RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_SL_NO,  ";
                SQL = SQL + "     RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_DEBIT_ACCCODE,   GL_COA_MASTER.GL_COAM_ACCOUNT_NAME , RM_MPEM_DEBIT_REMARKS,   RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_DEBIT_AMOUNT ,";
                SQL = SQL + "     RM_MPED_VAT_TYPE_CODE , RM_MPED_VAT_PERCENTAGE, ";
                SQL = SQL + "     RM_MPED_VAT_AMOUNT  ,RM_MPED_VAT_RC_AMOUNT,  ";
                SQL = SQL + "     RM_MPED_APP_TOTAL_AMOUNT ";

                SQL = SQL + "    FROM RM_PE_MASTER_DEBIT_ACCOUNTS  , GL_COA_MASTER ";
                SQL = SQL + "    WHERE  ";

                SQL = SQL + " RM_PE_MASTER_DEBIT_ACCOUNTS.  RM_MPEM_DEBIT_ACCCODE =  GL_COA_MASTER.GL_COAM_ACCOUNT_CODE ";
                SQL = SQL + "  and RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_ENTRY_NO='" + txtEntryCode + "' AND RM_PE_MASTER_DEBIT_ACCOUNTS.AD_FIN_FINYRID=" + mngrclass.FinYearID + "";


                SQL = SQL + "    order by  RM_PE_MASTER_DEBIT_ACCOUNTS.RM_MPEM_SL_NO asc ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        public DataSet FillDebitNoteDetails ( string txtEntryCode, object mngrclassobj )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT    ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_ENTRY_NO, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.AD_FIN_FINYRID, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_UPDATESTOCK_YN, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_DEBITNOTE_NO, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_REF_CODE, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_RMM_RM_CODE, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_RM_ACCCODE, ";
                SQL = SQL + " RM_ACCOUNT.GL_COAM_ACCOUNT_NAME RM_ACCOUNT_NAME, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_DEBIT_ACCCODE, ";
                SQL = SQL + " DEBIT_ACCOUNT.GL_COAM_ACCOUNT_NAME DEBIT_ACCOUNT_NAME, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_NARRATION, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_GRAND_TOTAL_AMOUNT, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_VAT_TYPE, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_VAT_RATE, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_VAT_AMOUNT, ";
                SQL = SQL + " RM_PE_MASTER_DEBITNOTE.RM_MPEM_NET_TOTAL_AMOUNT ";
                SQL = SQL + " FROM RM_PE_MASTER_DEBITNOTE,RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + " GL_COA_MASTER RM_ACCOUNT,GL_COA_MASTER DEBIT_ACCOUNT ";
                SQL = SQL + " WHERE RM_PE_MASTER_DEBITNOTE.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_PE_MASTER_DEBITNOTE.RM_MPEM_RM_ACCCODE = RM_ACCOUNT.GL_COAM_ACCOUNT_CODE ";
                SQL = SQL + " AND RM_PE_MASTER_DEBITNOTE.RM_MPEM_DEBIT_ACCCODE = DEBIT_ACCOUNT.GL_COAM_ACCOUNT_CODE(+)";
                SQL = SQL + " AND RM_PE_MASTER_DEBITNOTE.RM_MPEM_ENTRY_NO = '" + txtEntryCode + "' ";
                SQL = SQL + " AND RM_PE_MASTER_DEBITNOTE.AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";
                SQL = SQL + " ORDER BY RM_PE_MASTER_DEBITNOTE.RM_MPEM_SL_NO ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public void DeletePath ( string txtEntryCode, string Path, object mngrclassobj )
        {
            try
            {
                SQL = string.Empty;
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " DELETE FROM RM_PE_ATTACHMENT_DETAILS";
                SQL = SQL + " WHERE RM_MPEM_ENTRY_NO='" + txtEntryCode + "' AND RM_MPEM_FILE_PATH='" + Path + "'";
                SQL = SQL + "  AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";

                oTrns.GetExecuteNonQueryBySQL(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return;
            }
            return;
        }

        #endregion

        #region"FetchData"

        public DataTable FetchSupplierData ( string SCode, string DivCode )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                //SQL = " SELECT  RM_VM_VENDOR_CODE SUPPLIER_CODE,";
                //SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIER_NAME,SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_DESC, GL_TAX_TYPE_CODE ,  RM_VM_TAX_VAT_PERCENTAGE  ,   RM_VM_TAX_VAT_REG_NUMBER ";
                //SQL = SQL + " FROM   RM_VENDOR_MASTER , SL_PAY_TYPE_MASTER ";
                //SQL = SQL + " WHERE  RM_VENDOR_MASTER.RM_VM_VENDOR_CODE='" + SCode + "' ";
                //SQL = SQL + "  And RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS  = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE (+) ";
                //SQL = SQL + "   order by RM_VM_VENDOR_NAME asc  ";

                SQL = " SELECT   ";
                SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE SUPPLIER_CODE, ";
                SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIER_NAME,SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_DESC, ";
                SQL = SQL + "    TAX_TYPE.RM_VM_LABLE_VALUE GL_TAX_TYPE_CODE , GL_TAX_TYPE_PER  RM_VM_TAX_VAT_PERCENTAGE  ,   ";
                SQL = SQL + "    RM_VM_TAX_VAT_REG_NUMBER  ";
                SQL = SQL + " FROM   ";
                SQL = SQL + "    RM_VENDOR_MASTER , SL_PAY_TYPE_MASTER, ";
                SQL = SQL + "    RM_VENDOR_MASTER_BRANCH_DATA CREDIT_TERMS, ";
                SQL = SQL + "    RM_VENDOR_MASTER_BRANCH_DATA TAX_TYPE,GL_DEFAULTS_TAX_TYPE_MASTER ";
                SQL = SQL + " WHERE   ";
                SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE='" + SCode + "'  ";

                SQL = SQL + "    AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE=CREDIT_TERMS.RM_VM_VENDOR_CODE ";
                SQL = SQL + "    AND CREDIT_TERMS.RM_VM_LABLE_ID='PAYMENT_TERMS' ";

                SQL = SQL + "    AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE=TAX_TYPE.RM_VM_VENDOR_CODE ";
                SQL = SQL + "    AND TAX_TYPE.RM_VM_LABLE_ID='TAX_TYPE' ";

                SQL = SQL + "    AND TAX_TYPE.AD_BR_CODE=CREDIT_TERMS.AD_BR_CODE   ";
                SQL = SQL + "    AND TAX_TYPE.AD_BR_CODE='" + DivCode + "' ";
                SQL = SQL + "    And CREDIT_TERMS.RM_VM_LABLE_VALUE  = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE (+)  ";
                SQL = SQL + "    AND TAX_TYPE.RM_VM_LABLE_VALUE  = GL_DEFAULTS_TAX_TYPE_MASTER.GL_TAX_TYPE_CODE (+)  ";
                SQL = SQL + "    order by RM_VM_VENDOR_NAME asc   ";

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dtType;

        }

        #endregion;

        #region LookUp
        public DataSet FectchReceiptApprovalDetailsForGrid ( string Supplier,string BranchCode, string sStation, string sRm, string sRMResource,
                                                             string SupplierType, string ExFacoryOrOnsite, string sSelected )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

               
                SQL = "SELECT RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_UM_UOM_CODE  UOM_CODE,  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_FIN_FINYRID,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID,";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY, ";

                if (SupplierType == "0")
                {
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE VENDOR_CODE, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO_NEW ORDER_NO, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_VENDOR_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW AMOUNT_NEW ,";
                    
                }
                else
                {
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE VENDOR_CODE , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO_NEW ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW AMOUNT_NEW ,";

                }

                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_UNIQE_NO,RM_RECEIPT_APPL_MASTER.AD_BR_CODE,'RM' ReceiptType,  ";
                SQL = SQL + " RM_MRD_SUPP_VAT_TYPE_CODE  SUPP_VAT_TYPE ,  RM_MRM_SUPP_VAT_PERCENTAGE SUPP_VAT_PERC,";
                SQL = SQL + " RM_MRD_TRANS_VAT_TYPE_CODE TRANS_VAT_TYPE ,  RM_MRM_TRANS_VAT_PERCENTAGE TRANS_VAT_PERC  ";
                SQL = SQL + " FROM  RM_RECEPIT_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER,";
                SQL = SQL + " RM_VENDOR_MASTER,";
                SQL = SQL + " SL_STATION_MASTER,RM_RECEIPT_APPL_MASTER, ";
                SQL = SQL + " RM_SOURCE_MASTER  ";

                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS. RM_MRD_APPROVED = 'Y'";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO = RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID = RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER. RM_MRMA_APPROVED_STATUS ='Y'";
                
                if (SupplierType == "0")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE= '" + Supplier + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY ='N'";
                    
                }
                else if (SupplierType == "1")
                { 
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE= '" + Supplier + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY ='N'";
                    
                }

                if (!string.IsNullOrEmpty(sStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE in('" + sStation + "')";
                }

                if (!string.IsNullOrEmpty(sRm))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ('" + sRm + "')";
                }

                if (!string.IsNullOrEmpty(sRMResource))
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE IN ('" + sRMResource + "')";
                }

                if (!string.IsNullOrEmpty(BranchCode))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + BranchCode + "'";
                }


                if (ExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + ExFacoryOrOnsite + "'";
                }

                if (!string.IsNullOrEmpty(sSelected))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO NOT IN ('" + sSelected + "')";
                }

                SQL = SQL + " union all SELECT RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                SQL = SQL + "         SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_UOM_UOM_CODE UOM_CODE,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE  SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.AD_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_SUPPLY_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE              VENDOR_CODE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO_NEW               ORDER_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID            PO_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_NO      DOC_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE       PRICE_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT           AMOUNT_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.AD_BR_CODE ,RM_RMM_PRODUCT_TYPE ReceiptType,";
                SQL = SQL + "          ''  SUPP_VAT_TYPE ,  0  SUPP_VAT_PERC, ";
                SQL = SQL + "         RM_MRM_VAT_TYPE_CODE TRANS_VAT_TYPE ,RM_MRM_VAT_PERCENTAGE TRANS_VAT_PERC";



                SQL = SQL + "    FROM RM_RECEPIT_TOLL, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "         RM_UOM_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         RM_SOURCE_MASTER, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS ";



                SQL = SQL + "   WHERE     RM_RECEPIT_TOLL.RM_RMM_RM_CODE = ";
                SQL = SQL + "             RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE = ";
                SQL = SQL + "             RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = ";
                SQL = SQL + "             SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + "         AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVED_STATUS = 'Y' ";
                SQL = SQL + "         and RM_RMM_PRODUCT_TYPE <>'TOLL' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                SQL = SQL + "             RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "     AND RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO =  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = '" + Supplier + "'";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY = 'N' ";



                if (!string.IsNullOrEmpty(sStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE in('" + sStation + "')";
                }

                if (!string.IsNullOrEmpty(sRm))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_RMM_RM_CODE IN ('" + sRm + "')";
                }

                if (!string.IsNullOrEmpty(sRMResource))
                {
                    SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE IN ('" + sRMResource + "')";
                }

                if (!string.IsNullOrEmpty(BranchCode))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + BranchCode + "'";
                }

                if (ExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + ExFacoryOrOnsite + "'";
                }

                if (!string.IsNullOrEmpty(sSelected))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO NOT IN ('" + sSelected + "')";
                }

                SQL = SQL + "  order by ReceiptType DESC";
                
                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        public DataSet FectchReceiptTollApprovalDetailsForGrid ( string Supplier,string BranchCode, string sStation, string sRm, string sRMResource,
                                                                  string ExFacoryOrOnsite, string sSelected)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  SELECT RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                SQL = SQL + "         SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_UOM_UOM_CODE UOM_CODE,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE  SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.AD_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_SUPPLY_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE              VENDOR_CODE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO_NEW               ORDER_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID            PO_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_NO      DOC_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE       PRICE_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT           AMOUNT_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.AD_BR_CODE ,RM_RMM_PRODUCT_TYPE ReceiptType,";
                SQL = SQL + "         RM_MRM_VAT_TYPE_CODE SUPP_VAT_TYPE , RM_MRM_VAT_PERCENTAGE SUPP_VAT_PERC ";
                SQL = SQL + "    FROM RM_RECEPIT_TOLL, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "         RM_UOM_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         RM_SOURCE_MASTER, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS ";



                SQL = SQL + "   WHERE     RM_RECEPIT_TOLL.RM_RMM_RM_CODE = ";
                SQL = SQL + "             RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE = ";
                SQL = SQL + "             RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = ";
                SQL = SQL + "             SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + "         AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVED_STATUS = 'Y' ";
                SQL = SQL + "         and RM_RMM_PRODUCT_TYPE IN ( 'TOLL')  ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                SQL = SQL + "             RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "     AND RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO =  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = '" + Supplier + "'";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY = 'N' ";



                if (!string.IsNullOrEmpty(sStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE in('" + sStation + "')";
                }

                if (!string.IsNullOrEmpty(sRm))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_RMM_RM_CODE IN ('" + sRm + "')";
                }

                if (!string.IsNullOrEmpty(sRMResource))
                {
                    SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE IN ('" + sRMResource + "')";
                }

                if (!string.IsNullOrEmpty(BranchCode))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + BranchCode + "'";
                }


                if (ExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + ExFacoryOrOnsite + "'";
                }

                if (!string.IsNullOrEmpty(sSelected))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO NOT IN ('" + sSelected + "')";
                }

                SQL = SQL + "  order by ReceiptType DESC";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        public DataSet FectchReceiptApprovalDetailswithReceiptandTollForGrid ( string Supplier,string BranchCode, string sStation, string sRm, string sRMResource,
                                                                                string SupplierType, string ExFacoryOrOnsite, string sSelected )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "select  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_UM_UOM_CODE  UOM_CODE,  ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,";
                SQL = SQL + " RM_SM_SOURCE_DESC SOURCE_DESC,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_FIN_FINYRID,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID,";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY, ";

                if (SupplierType == "0")
                {

                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE VENDOR_CODE, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO_NEW ORDER_NO, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_VENDOR_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW AMOUNT_NEW ,";
                   
                }
                else
                {
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE VENDOR_CODE , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO_NEW ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW AMOUNT_NEW ,";
                  
                }

                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_UNIQE_NO,RM_RECEIPT_APPL_MASTER.AD_BR_CODE,'RM' ReceiptType,1 SortID , ";
                SQL = SQL + " RM_MRD_SUPP_VAT_TYPE_CODE  SUPP_VAT_TYPE ,  RM_MRM_SUPP_VAT_PERCENTAGE SUPP_VAT_PERC,";
                SQL = SQL + " RM_MRD_TRANS_VAT_TYPE_CODE TRANS_VAT_TYPE ,  RM_MRM_TRANS_VAT_PERCENTAGE TRANS_VAT_PERC ";

                SQL = SQL + " FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER,";
                SQL = SQL + " RM_VENDOR_MASTER,";
                SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER, RM_RECEIPT_APPL_MASTER ";

                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS. RM_MRD_APPROVED = 'Y'";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO = RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID = RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER. RM_MRMA_APPROVED_STATUS ='Y'";
               
                if (SupplierType == "0")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE= '" + Supplier + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY ='N'";
                    
                }
                else if (SupplierType == "1")
                { 
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE= '" + Supplier + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY ='N'";
                    
                }

                if (!string.IsNullOrEmpty(sStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE in('" + sStation + "')";
                }

                if (!string.IsNullOrEmpty(sRm))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ('" + sRm + "')";
                }

                if (!string.IsNullOrEmpty(sRMResource))
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE IN ('" + sRMResource + "')";
                }

                if (!string.IsNullOrEmpty(BranchCode))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + BranchCode + "'";
                }


                if (ExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + ExFacoryOrOnsite + "'";
                }
                if (!string.IsNullOrEmpty(sSelected))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO NOT IN ('" + sSelected + "')";
                }

                SQL = SQL + " union all SELECT RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                SQL = SQL + "         SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "        RM_RECEPIT_TOLL.RM_UOM_UOM_CODE UOM_CODE,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE  SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC,";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.AD_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_SUPPLY_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE              VENDOR_CODE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO_NEW               ORDER_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID            PO_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_NO      DOC_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE       PRICE_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT           AMOUNT_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.AD_BR_CODE ,  RM_RMM_PRODUCT_TYPE  ReceiptType ,2 SortID,";
                SQL = SQL + "         '' SUPP_VAT_TYPE ,  0 SUPP_VAT_PERC, ";
                SQL = SQL + "         RM_MRM_VAT_TYPE_CODE  TRANS_VAT_TYPE ,  RM_MRM_VAT_PERCENTAGE TRANS_VAT_PERC";


                SQL = SQL + "    FROM RM_RECEPIT_TOLL, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "         RM_UOM_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         RM_SOURCE_MASTER, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS ";



                SQL = SQL + "   WHERE     RM_RECEPIT_TOLL.RM_RMM_RM_CODE = ";
                SQL = SQL + "             RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE = ";
                SQL = SQL + "             RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = ";
                SQL = SQL + "             SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + "         AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVED_STATUS = 'Y' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                SQL = SQL + "             RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "     AND RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO =  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = '" + Supplier + "'";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY = 'N' ";



                if (!string.IsNullOrEmpty(sStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE in('" + sStation + "')";
                }

                if (!string.IsNullOrEmpty(sRm))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_RMM_RM_CODE IN ('" + sRm + "')";
                }

                if (!string.IsNullOrEmpty(sRMResource))
                {
                    SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE IN ('" + sRMResource + "')";
                }

                if (!string.IsNullOrEmpty(BranchCode))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + BranchCode + "'";
                }


                if (ExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + ExFacoryOrOnsite + "'";
                }

                if (!string.IsNullOrEmpty(sSelected))
                {
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO NOT IN ('" + sSelected + "')";
                }

                SQL = SQL + "  order by SortID,ReceiptType,DOC_NO Asc";
                
                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FectchStockTransferDetailsForGrid ( string Supplier, string sStation, string sRm,string sSelected )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " ";
                SQL = "    SELECT ";
                SQL = SQL + "            SA_STN_STATION_CODE_FROM SALES_STN_STATION_CODE , ";
                SQL = SQL + "            SALES_STN_STATION_NAME,  ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_UM_UOM_CODE UOM_CODE,  ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_SM_SOURCE_CODE SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO RM_MRM_RECEIPT_NO , ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL + "            RM_MSTM_TRANSFER_DATE RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO RM_MRM_APPROVED_NO, "; // since there is not apprval same document number isusing 
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID RM_MRD_APPROVED_FINYRID, "; // since there is not apprval same document number isusing 
                SQL = SQL + "            RM_MSTM_TRANSFER_DATE   RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "            RM_MSTD_QUANTITY  RM_MRD_SUPPLY_QTY ,  ";
                SQL = SQL + "            RM_MSTD_RCV_QTY  RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "            RM_MSTD_TRANSPORTER_CODE VENDOR_CODE,  ";
                SQL = SQL + "            RM_MTRANSPO_ORDER_NO ORDER_NO, ";
                SQL = SQL + "            RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID, ";
                SQL = SQL + "            RM_MSTD_TRNSPORTER_DOC_NO DOC_NO , ";
                SQL = SQL + "            RM_MSTD_TRANS_CHARGES PRICE_NEW, ";
                SQL = SQL + "            RM_MSTD_TRANS_AMOUNT  AMOUNT_NEW, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE,   ";
                SQL = SQL + "            RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_MSTD_SL_NO   RM_MRM_RECEIPT_UNIQE_NO , 'TRANS' RECEIPTTYPE,";
                SQL = SQL + "            RM_MSTD_TRANS_VAT_TYPE_CODE TRANS_VAT_TYPE ,RM_MSTD_TRANS_VAT_PERCENTAGE TRANS_VAT_PERC ";

                SQL = SQL + "    FROM ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_MASTER, ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_DETL, ";
                SQL = SQL + "        RM_RAWMATERIAL_MASTER , ";
                SQL = SQL + "        SL_STATION_MASTER, ";
                SQL = SQL + "        RM_SOURCE_MASTER, ";
                SQL = SQL + "        RM_VENDOR_MASTER ";
                SQL = SQL + "    WHERE  ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO = RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO  ";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID = RM_MAT_STK_TRANSFER_DETL.AD_FIN_FINYRID  ";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + "        AND RM_MAT_STK_TRANSFER_DETL.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE  ";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM = SL_STATION_MASTER.SALES_STN_STATION_CODE  ";
                SQL = SQL + "        AND RM_MSTD_TRANSPORTER_CODE = RM_VM_VENDOR_CODE  ";
                SQL = SQL + "        AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_APPRV_STATUS ='Y'";
                SQL = SQL + "        AND  RM_MAT_STK_TRANSFER_DETL.RM_MSTD_PE_DONE ='N'";
                SQL = SQL + "      AND RM_MSTD_TRANSPORTER_CODE= '" + Supplier + "'";

                if (!string.IsNullOrEmpty(sStation))
                {
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM in('" + sStation + "')";
                }

                if (!string.IsNullOrEmpty(sRm))
                {
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE IN ('" + sRm + "')";
                }

                if (!string.IsNullOrEmpty(sSelected))
                {
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO NOT IN ('" + sSelected + "')";
                }

                SQL = SQL + " order by RM_MAT_STK_TRANSFER_DETL. RM_MSTD_TRNSPORTER_DOC_NO ";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        #endregion

        #region FetchDetailsBy Receipt


        public DataSet FectchReceiptByReceiptNo ( string Supplier,  string rdoVendorTransporter, string sExFacoryOrOnsite,string ReceiptNum)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_UM_UOM_CODE  UOM_CODE,  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_FIN_FINYRID,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID,";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY, ";

                if (rdoVendorTransporter == "0")
                {
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE VENDOR_CODE, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO_NEW ORDER_NO, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_VENDOR_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW AMOUNT_NEW ,";
                   
                }
                else
                {
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE VENDOR_CODE , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO_NEW ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW AMOUNT_NEW ,";

                }

                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_UNIQE_NO,RM_RECEIPT_APPL_MASTER.AD_BR_CODE,'RM' ReceiptType,  ";
                SQL = SQL + " RM_MRD_SUPP_VAT_TYPE_CODE  SUPP_VAT_TYPE ,  RM_MRM_SUPP_VAT_PERCENTAGE SUPP_VAT_PERC,";
                SQL = SQL + " RM_MRD_TRANS_VAT_TYPE_CODE TRANS_VAT_TYPE ,  RM_MRM_TRANS_VAT_PERCENTAGE TRANS_VAT_PERC ";
                SQL = SQL + " FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER,";
                SQL = SQL + " RM_VENDOR_MASTER,";
                SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER, RM_RECEIPT_APPL_MASTER ";

                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS. RM_MRD_APPROVED = 'Y'";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO = RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID = RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER. RM_MRMA_APPROVED_STATUS ='Y'";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ='"+ReceiptNum+"' ";

                if (rdoVendorTransporter == "0")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE= '" + Supplier + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY ='N'";
                    
                }
                else if (rdoVendorTransporter == "1")
                { 
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE= '" + Supplier + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY ='N'";
                   
                }

                    //SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE ='" + Station + "' ";
                    //SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE ='" + RmCode + "' ";
                    //SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE ='" + SourceCode + "' ";// Toll also will come for receipt
                
                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + Branch + "'";
                }


                if (sExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + sExFacoryOrOnsite + "'";
                }


                SQL = SQL + " union all SELECT RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                SQL = SQL + "         SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_UOM_UOM_CODE UOM_CODE,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE  SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.AD_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_SUPPLY_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE              VENDOR_CODE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO_NEW               ORDER_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID            PO_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_NO      DOC_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE       PRICE_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT           AMOUNT_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.AD_BR_CODE ,RM_RMM_PRODUCT_TYPE ReceiptType,";
                SQL = SQL + "          ''  SUPP_VAT_TYPE ,  0  SUPP_VAT_PERC, ";
                SQL = SQL + "         RM_MRM_VAT_TYPE_CODE TRANS_VAT_TYPE ,RM_MRM_VAT_PERCENTAGE TRANS_VAT_PERC";



                SQL = SQL + "    FROM RM_RECEPIT_TOLL, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "         RM_UOM_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         RM_SOURCE_MASTER, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS ";



                SQL = SQL + "   WHERE     RM_RECEPIT_TOLL.RM_RMM_RM_CODE = ";
                SQL = SQL + "             RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE = ";
                SQL = SQL + "             RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = ";
                SQL = SQL + "             SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + "         AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVED_STATUS = 'Y' ";
                SQL = SQL + "         and RM_RMM_PRODUCT_TYPE <>'TOLL' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                SQL = SQL + "             RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "     AND RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO =  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = '" + Supplier + "'";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY = 'N' ";
                //SQL = SQL + " AND  RM_RECEPIT_TOLL.SALES_STN_STATION_CODE ='" + Station + "'";
                //SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_RMM_RM_CODE ='" + RmCode + "' ";
               // SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE ='" + SourceCode + "' ";
                SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO ='"+ReceiptNum+"' ";
               
                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + Branch + "'";
                }


                if (sExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + sExFacoryOrOnsite + "'";
                }

                SQL = SQL + "  order by ReceiptType DESC";
                
                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        public DataSet FectchReceiptTollApprovalByReceiptNo ( string Supplier,string ExFacoryOrOnsite, string ReceiptNum)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  SELECT RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                SQL = SQL + "         SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_UOM_UOM_CODE UOM_CODE,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE  SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.AD_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_SUPPLY_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE              VENDOR_CODE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO_NEW               ORDER_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID            PO_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_NO      DOC_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE       PRICE_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT           AMOUNT_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.AD_BR_CODE ,RM_RMM_PRODUCT_TYPE ReceiptType,";
                SQL = SQL + "         RM_MRM_VAT_TYPE_CODE SUPP_VAT_TYPE , RM_MRM_VAT_PERCENTAGE SUPP_VAT_PERC ";
                SQL = SQL + "    FROM RM_RECEPIT_TOLL, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "         RM_UOM_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         RM_SOURCE_MASTER, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS ";



                SQL = SQL + "   WHERE     RM_RECEPIT_TOLL.RM_RMM_RM_CODE = ";
                SQL = SQL + "             RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE = ";
                SQL = SQL + "             RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = ";
                SQL = SQL + "             SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + "         AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVED_STATUS = 'Y' ";
                SQL = SQL + "         and RM_RMM_PRODUCT_TYPE IN ( 'TOLL')  ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                SQL = SQL + "             RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "     AND RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO =  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = '" + Supplier + "'";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY = 'N' ";

                //SQL = SQL + " AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = '" + Station + "' ";
                //SQL = SQL + " AND RM_RECEPIT_TOLL.RM_RMM_RM_CODE = '" + RmCode + "' ";
                //SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE = '" + SourceCode + "' ";

                SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO = '" + ReceiptNum + "' ";

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + Branch + "'";
                }


                if (ExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + ExFacoryOrOnsite + "'";
                }

                SQL = SQL + "  order by ReceiptType DESC";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        public DataSet FectchReceiptandTollByReceiptNo ( string Supplier,    string SupplierType, string ExFacoryOrOnsite,  string ReceiptNum )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "select  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_UM_UOM_CODE  UOM_CODE,  ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,";
                SQL = SQL + " RM_SM_SOURCE_DESC SOURCE_DESC,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_FIN_FINYRID,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID,";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY, ";

                if (SupplierType == "0")
                {

                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE VENDOR_CODE, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO_NEW ORDER_NO, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_VENDOR_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW AMOUNT_NEW ,";
                   
                }
                else
                {
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE VENDOR_CODE , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO_NEW ORDER_NO , RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS. RM_MRM_TRNSPORTER_DOC_NO DOC_NO , ";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW PRICE_NEW ,";
                    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW AMOUNT_NEW ,";
                  
                }

                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_UNIQE_NO,RM_RECEIPT_APPL_MASTER.AD_BR_CODE,'RM' ReceiptType,1 SortID , ";
                SQL = SQL + " RM_MRD_SUPP_VAT_TYPE_CODE  SUPP_VAT_TYPE ,  RM_MRM_SUPP_VAT_PERCENTAGE SUPP_VAT_PERC,";
                SQL = SQL + " RM_MRD_TRANS_VAT_TYPE_CODE TRANS_VAT_TYPE ,  RM_MRM_TRANS_VAT_PERCENTAGE TRANS_VAT_PERC ";

                SQL = SQL + " FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER,";
                SQL = SQL + " RM_VENDOR_MASTER,";
                SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER, RM_RECEIPT_APPL_MASTER ";

                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS. RM_MRD_APPROVED = 'Y'";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO = RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID = RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER. RM_MRMA_APPROVED_STATUS ='Y'";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ='"+ReceiptNum+"'";
               
                if (SupplierType == "0")
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE ";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE= '" + Supplier + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY ='N'";
                    
                }
                else if (SupplierType == "1")
                { 
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE= '" + Supplier + "'";
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY ='N'";
                    
                }

                    //SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = '" + Station + "' ";
                
                    //SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = '" + RmCode + "' ";
                
                    //SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE = '" + SourceCode + "' ";
                
                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + Branch + "'";
                }


                if (ExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + ExFacoryOrOnsite + "'";
                }
                
                SQL = SQL + " union all SELECT RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                SQL = SQL + "         SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_UOM_UOM_CODE UOM_CODE,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE  SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC,";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.AD_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_SUPPLY_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_QTY RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE              VENDOR_CODE, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO_NEW               ORDER_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID            PO_FIN_FINYRID, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_NO      DOC_NO, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE       PRICE_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT           AMOUNT_NEW, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "         RM_RECEPIT_TOLL.RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER.AD_BR_CODE ,  RM_RMM_PRODUCT_TYPE  ReceiptType ,2 SortID,";
                SQL = SQL + "         '' SUPP_VAT_TYPE ,  0 SUPP_VAT_PERC, ";
                SQL = SQL + "         RM_MRM_VAT_TYPE_CODE  TRANS_VAT_TYPE ,  RM_MRM_VAT_PERCENTAGE TRANS_VAT_PERC";


                SQL = SQL + "    FROM RM_RECEPIT_TOLL, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "         RM_UOM_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         RM_SOURCE_MASTER, ";
                SQL = SQL + "         RM_RECEIPT_APPL_MASTER, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS ";



                SQL = SQL + "   WHERE     RM_RECEPIT_TOLL.RM_RMM_RM_CODE = ";
                SQL = SQL + "             RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE = ";
                SQL = SQL + "             RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = ";
                SQL = SQL + "             SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'Y' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID = ";
                SQL = SQL + "             RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + "         AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVED_STATUS = 'Y' ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = ";
                SQL = SQL + "             RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "     AND RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO =  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = '" + Supplier + "'";
                SQL = SQL + "         AND RM_RECEPIT_TOLL.RM_MRD_SUPP_ENTRY = 'N' ";
                //SQL = SQL + " AND  RM_RECEPIT_TOLL.SALES_STN_STATION_CODE  = '" + Station + "' ";
                //SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_RMM_RM_CODE  = '" + RmCode + "' ";
               // SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE = '" + SourceCode + "' ";
                SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO ='"+ReceiptNum+"'";
                

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND  RM_RECEIPT_APPL_MASTER.AD_BR_CODE  = '" + Branch + "'";
                }


                if (ExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + ExFacoryOrOnsite + "'";
                }

                SQL = SQL + "  order by SortID,ReceiptType,DOC_NO Asc";
                
                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        public DataSet FectchStockTransferDetailsByReceiptNo ( string Supplier, string Station, string RmCode,string ReceiptNum )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " ";
                SQL = "    SELECT ";
                SQL = SQL + "            SA_STN_STATION_CODE_FROM SALES_STN_STATION_CODE , ";
                SQL = SQL + "            SALES_STN_STATION_NAME,  ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_UM_UOM_CODE UOM_CODE,  ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_SM_SOURCE_CODE SOURCE_CODE,RM_SM_SOURCE_DESC SOURCE_DESC, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO RM_MRM_RECEIPT_NO , ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL + "            RM_MSTM_TRANSFER_DATE RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO RM_MRM_APPROVED_NO, "; // since there is not apprval same document number isusing 
                SQL = SQL + "            RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID RM_MRD_APPROVED_FINYRID, "; // since there is not apprval same document number isusing 
                SQL = SQL + "            RM_MSTM_TRANSFER_DATE   RM_MRMA_APPROVALTRANS_DATE, ";
                SQL = SQL + "            RM_MSTD_QUANTITY  RM_MRD_SUPPLY_QTY ,  ";
                SQL = SQL + "            RM_MSTD_RCV_QTY  RM_MRD_APPROVE_QTY, ";
                SQL = SQL + "            RM_MSTD_TRANSPORTER_CODE VENDOR_CODE,  ";
                SQL = SQL + "            RM_MTRANSPO_ORDER_NO ORDER_NO, ";
                SQL = SQL + "            RM_MTRANSPO_FIN_FINYRID PO_FIN_FINYRID, ";
                SQL = SQL + "            RM_MSTD_TRNSPORTER_DOC_NO DOC_NO , ";
                SQL = SQL + "            RM_MSTD_TRANS_CHARGES PRICE_NEW, ";
                SQL = SQL + "            RM_MSTD_TRANS_AMOUNT  AMOUNT_NEW, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE,   ";
                SQL = SQL + "            RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "            RM_MAT_STK_TRANSFER_DETL.RM_MSTD_SL_NO   RM_MRM_RECEIPT_UNIQE_NO , 'TRANS' RECEIPTTYPE,";
                SQL = SQL + "            RM_MSTD_TRANS_VAT_TYPE_CODE TRANS_VAT_TYPE ,RM_MSTD_TRANS_VAT_PERCENTAGE TRANS_VAT_PERC ";

                SQL = SQL + "    FROM ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_MASTER, ";
                SQL = SQL + "        RM_MAT_STK_TRANSFER_DETL, ";
                SQL = SQL + "        RM_RAWMATERIAL_MASTER , ";
                SQL = SQL + "        SL_STATION_MASTER, ";
                SQL = SQL + "        RM_SOURCE_MASTER, ";
                SQL = SQL + "        RM_VENDOR_MASTER ";
                SQL = SQL + "    WHERE  ";
                SQL = SQL + "       RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO = RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO  ";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID = RM_MAT_STK_TRANSFER_DETL.AD_FIN_FINYRID  ";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_DETL.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE  ";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM = SL_STATION_MASTER.SALES_STN_STATION_CODE  ";
                SQL = SQL + " AND   RM_MSTD_TRANSPORTER_CODE = RM_VM_VENDOR_CODE  ";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_APPRV_STATUS ='Y'";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_DETL.RM_MSTD_PE_DONE ='N'";
                SQL = SQL + " AND   RM_MSTD_TRANSPORTER_CODE= '" + Supplier + "'";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM = '" + Station + "' ";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE = '" + RmCode + "' ";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO = '" + ReceiptNum + "' ";
                

                SQL = SQL + " order by RM_MAT_STK_TRANSFER_DETL. RM_MSTD_TRNSPORTER_DOC_NO ";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }



        #endregion
        
    }

    //============================================= ENITY ========================= DO NOT WRIE THE CODE BELOW ===================================== JOMY 

    #region " enitity region "

    public class PurchaseEntryEntity
    {
        public string txtEntryNo
        {
            get;
            set;
        }

        public string sTransType
        {
            get;
            set;
        }

        public string txtSupplierCode
        {
            get;
            set;
        }
        
        public string txtPoRefNo
        {
            get;
            set;
        }

        public string txtInvNo
        {
            get;
            set;
        }

        public string TxtGrndTotal
        {
            get;
            set;
        }

        public string txtDiscAmt
        {
            get;
            set;
        }

        public string TxtStockAdjAmount
        {
            get;
            set;
        }

        public string txtInvTotal
        {
            get;
            set;
        }

        public string TxtNetTotal
        {
            get;
            set;
        }

        public string txtDiscAcc
        {
            get;
            set;
        }

        public string txtStkAdjAcCode
        {
            get;
            set;
        }

        public string txtRemarks
        {
            get;
            set;
        }

        public string txtVerifiedBy
        {
            get;
            set;
        }

        public string txtApprovedBy
        {
            get;
            set;
        }

        public string EntryType
        {
            get;
            set;
        }

        public string approve
        {
            get;
            set;
        }

        public string varify
        {
            get;
            set;
        }

        public DateTime dtpTransactionDate
        {
            get;
            set;
        }

        public DateTime dtpInvDate
        {
            get;
            set;
        }
        public DateTime ReceivedDate
        {
            get;
            set;
        }

        public DateTime dtpReceiptAppFromDate
        {
            get;
            set;
        }
        public DateTime dtpReceiptAppToDate
        {
            get;
            set;
        }

        public double TaxPerc
        {
            get;
            set;
        }
        public double TaxAmount
        {
            get;
            set;
        }
        public double TaxRcAmount
        {
            get;
            set;
        }
        public string TaxAccountCode
        {
            get;
            set;
        }
        public string TaxAccountCodeOutput
        {
            get;
            set;
        }
        public string TaxVatType
        {
            get;
            set;
        }

        public double txtTaxableAmount
        {
            get;
            set;
        }
        public double txtNonTaxableAmount
        {
            get;
            set;
        }
        public string Branch { get; set; }
        public double AdvanceMatchedVATAmount { get; set; }
        public double AdvanceMatchedAmount { get; set; }
    }

    public class PurchaseEntryReceiptsDet
    {
        public object oReceiptNoPEReceiptsDet
        {
            get;
            set;
        }

        public int iReceiptFinYrPEReceiptsDet
        {
            get;
            set;
        }

        public double dRctUniquenoPEReceiptsDet
        {
            get;
            set;
        }

        public object oRctAppNoPEReceiptsDet
        {
            get;
            set;
        }

        public int iRctAppFinYrPEReceiptsDet
        {
            get;
            set;
        }


        public string sReceiptType
        {
            get;
            set;
        }

        public DateTime ReceiptAppDate
        {
            get;
            set;
        }

        public DateTime ReceiptApptTransDate
        {
            get;
            set;
        }

        public string sPONo
        {
            get;
            set;
        }

        public string sPONoFinYR
        {
            get;
            set;
        }
        public string sStationCode
        {
            get;
            set;
        }
        public string sUOMCode
        {
            get;
            set;
        }
        public string sSourceCode
        {
            get;
            set;
        }
        public string sRMCode
        {
            get;
            set;
        }

        public string sRMName
        {
            get;
            set;
        }

        public string sSuppDocNo
        {
            get;
            set;
        }


        public string sSupplyQTY
        {
            get;
            set;
        }

        public string sPEQtyApprved
        {
            get;
            set;
        }

        public double sPERate
        {
            get;
            set;
        }
        public double sPEAmount
        {
            get;
            set;
        }
        public double sPEStkAdjAmount
        {
            get;
            set;
        }
        public double sPEDiscAmount
        {
            get;
            set;
        }

        public string sVatType
        {
            get;
            set;
        }
        public string sVatRate
        {
            get;
            set;
        }
        public double sPEVatAmount
        {
            get;
            set;
        }
        public double sPERCVatAmount
        {
            get;
            set;
        }
        public double sPETotalAmount
        {
            get;
            set;
        }

    }


    public class PEDebitAccounts
    { // this is for debit accounts for sister conern account for special requiest from joji and praveen
        public string sAccountCode { get; set; }
        public string sAccountRemarks { get; set; }
        public double dTotalDebitAmount { get; set; }
        public string dDebitAccVatType { get; set; }
        public string dDebitAccVatRate { get; set; }
        public double dDebitAccVatAmount { get; set; }
        public double dDebitAccRCVatAmount { get; set; }
        public double dDebitAmounts { get; set; }
        public double dSlNo { get; set; }

    }

    public class PEDebitNoteGrid
    {
        public int dSlNo { get; set; }
        public string sUpdateStockYN { get; set; }
        public string sSuppDebitnoteNo { get; set; }
        public string sRefNo { get; set; }
        public string sRMCode { get; set; }
        public string sRMAccCode { get; set; }
        public string sAccCode { get; set; }
        public string sNarration { get; set; }
        public double dNetAmount { get; set; }
        public string sVatType { get; set; }
        public double dVatRate { get; set; }
        public double dVatAmount { get; set; }
        public double dTotalAmount { get; set; }

    }



    public class PurchaseRMMatchDetailsEntity
    {
        public string oVoucherNo { get; set; }

        public string oVoucherType { get; set; }

        public string oVoucherFinYr { get; set; }

        public double dMatchAmt { get; set; }
        public string VoucheBranchCode { get; set; }
        public double VATpercentage { get; set; }
        public double dMatchedVATAmount { get; set; }



    }

    #endregion
    ////================================= END OF ENTITY 

}