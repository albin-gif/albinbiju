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
/// Created On      :   19-Dec-2012
/// Updated by      :   Alex Antony
/// updated on      :   20-Jun-2024
/// </summary>

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class ReceiptApprovalLogic
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public string POReceiptType
        {
            get;
            set;
        }
        public string Branch { get; set; }

        public DateTime ReceptFromDateTime
        {
            get;
            set;
        }
        public DateTime ReceptToDateTime
        {
            get;
            set;
        }

        public string DateTimeFilterType
        {
            get;
            set;
        }


        #region Fill Combo

        public DataTable FillBranch(string UserName)
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
                    SQL = SQL + "   and   AD_BR_CODE  IN  ";
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



        #region "Fetch Data" 

        public string UnApprovedReceiptCheck(string SupplierCode)
        {
            DataTable dtReturn = new DataTable();
            string sReturn = string.Empty;
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT COUNT(RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO) CNT ";

                SQL = SQL + " FROM RM_RECEIPT_MASTER";

                SQL = SQL + " WHERE RM_MRM_RECEIPT_APPROVED = 'N'";

                SQL = SQL + " AND RM_RECEIPT_MASTER.RM_VM_VENDOR_CODE= '" + SupplierCode + "'";

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND RM_RECEIPT_MASTER.AD_BR_CODE ='" + Branch + "'";
                }


                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);

                if (int.Parse(dtReturn.Rows[0]["CNT"].ToString()) > 0)
                {
                    sReturn = int.Parse(dtReturn.Rows[0]["CNT"].ToString()) + " Unapproved Receipt Exist";
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
            return sReturn;
        }

        public DataSet FectchReceiptDetails(string sExFacoryOrOnsite, string txtSupplierCode, string sSlectedStation, string sSelectedTrans, string sSelectedRM, string sSelectedSoruce, string dtpFromDate, string dtpToDate)
        {
            DataSet sReturn = new DataSet();
            CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME  TRANSPORTER_NAME,  ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_FIN_FINYRID,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,";

                SQL = SQL + "case when   ";
                SQL = SQL + "TO_CHAR(RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE_TIME,'HH24:MI:SS') < '07:00:00 AM'   ";
                SQL = SQL + "then  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE -1 ";
                SQL = SQL + "else  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE   ";
                SQL = SQL + "END RM_MRM_RECEIPT_DATE_FOR_RATE,  ";


                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID, ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SL_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_UM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO, RM_MTRANSPO_FIN_FINYRID , RM_MRM_TRNSPORTER_DOC_NO ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_ORDER_QTY, RM_MRD_VENDOR_DOC_NO ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY, RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY  ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW,";

                SQL = SQL + " RM_RECEPIT_DETAILS. RM_UOM_TOLL_CODE, RM_POD_TOLL_RATE_NEW  ,RM_MRD_TOLL_AMOUNT_NEW,";


                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO, RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO , ";
                SQL = SQL + " RM_MRD_TOLL_PE_ENTRY_NO , RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_UNIQE_NO, ";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_BR_CODE ,RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO_NEW, ";
                //TONY ADDED ON 06-Feb-2024 FOR OVERSEAS TRIP AMOUNT 
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_PO_UOM_TYPE,RM_RECEIPT_MASTER.RM_PO_RATE_REVISION_APP_YN     ";

                SQL = SQL + " FROM RM_RECEIPT_MASTER, RM_RECEPIT_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER,";
                SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER ,  RM_VENDOR_MASTER ";
                SQL = SQL + "  where  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO=RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ";
                SQL = SQL + "    and  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE    ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ( +) ";


                SQL = SQL + " AND RM_MRD_APPROVED = 'N' and RM_RECEIPT_MASTER.RM_MRM_RECEIPT_APPROVED ='Y' ";


                if (DateTimeFilterType == "Date")
                {
                    SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))";
                    SQL = SQL + " BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                }
                else if (DateTimeFilterType == "Time")
                {
                    SQL = SQL + "    AND   RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";
                    SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                }

                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE= '" + txtSupplierCode + "'";

                if (sExFacoryOrOnsite != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + sExFacoryOrOnsite + "'";
                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE in('" + sSlectedStation + "')";
                }

                if (!string.IsNullOrEmpty(sSelectedTrans))
                {
                    SQL = SQL + "   AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE  in( '" + sSelectedTrans + "')";
                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ('" + sSelectedRM + "')";
                }
                if (!string.IsNullOrEmpty(sSelectedSoruce))
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE   IN ('" + sSelectedSoruce + "')";
                }

                if (!string.IsNullOrEmpty(POReceiptType))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_PO_RM_TYPE ='" + POReceiptType + "'";
                }

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE ='" + Branch + "'";
                }



                SQL = SQL + " ORDER BY  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO asc";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return sReturn;
        }


        public DataSet FectchReceiptDetailsOtherItems(string sExFacoryOrOnsite, string txtSupplierCode, string sSlectedStation, string sSelectedTrans, string sSelectedRM, string sSelectedSoruce, string dtpFromDate, string dtpToDate)
        {
            DataSet sReturn = new DataSet();
            CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                if (POReceiptType == "RM")
                {

                    SQL = " SELECT  RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE RM_MRM_TRANSPORTER_CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME  TRANSPORTER_NAME,   ";
                    SQL = SQL + " RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO, ";
                    SQL = SQL + " RM_RECEIPT_MASTER.AD_FIN_FINYRID, ";
                    SQL = SQL + " RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE, ";
                    SQL = SQL + "case when    ";
                    SQL = SQL + "TO_CHAR(RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE_TIME,'HH24:MI:SS') < '07:00:00 AM'    ";
                    SQL = SQL + "then  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE -1  ";
                    SQL = SQL + "else  ";
                    SQL = SQL + " RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE    ";
                    SQL = SQL + "END ";
                    SQL = SQL + " RM_MRM_RECEIPT_DATE_FOR_RATE,   ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO, ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID,  ";
                    SQL = SQL + " RM_RECEIPT_MASTER.RM_MPOM_PRICE_TYPE, ";
                    SQL = SQL + " 1 RM_MRD_SL_NO, ";
                    SQL = SQL + " RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                    SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                    SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,RM_RMM_PRODUCT_TYPE, ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_UOM_UOM_CODE RM_UM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC, ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC , ";
                    SQL = SQL + "RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO RM_MTRANSPO_ORDER_NO,0 RM_MTRANSPO_FIN_FINYRID ,0 RM_MRM_TRNSPORTER_DOC_NO , ";
                    SQL = SQL + " 0 RM_MRD_ORDER_QTY,RM_MRM_TOLL_NO RM_MRD_VENDOR_DOC_NO , ";
                    SQL = SQL + "    CASE   WHEN RM_UOM_CATEGORY_MASTER.INVOICE_CALC_CODE = 'TR'  ";
                    SQL = SQL + "    THEN  1  ELSE   RM_MRD_SUPPLY_QTY  END  ";
                    SQL = SQL + "    AS  RM_MRD_SUPPLY_QTY, ";
                    SQL = SQL + "    CASE   WHEN RM_UOM_CATEGORY_MASTER.INVOICE_CALC_CODE = 'TR'  ";
                    SQL = SQL + "    THEN  1  ELSE   RM_MRD_WEIGH_QTY  END  ";
                    SQL = SQL + "    AS  RM_MRD_WEIGH_QTY, ";
                    SQL = SQL + "    CASE   WHEN RM_UOM_CATEGORY_MASTER.INVOICE_CALC_CODE = 'TR'  ";
                    SQL = SQL + "    THEN  1  ELSE   RM_MRD_APPROVE_QTY  END  ";
                    SQL = SQL + "    AS RM_MRD_APPROVE_QTY,   RM_MRD_REJECTED_QTY  , ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_MRM_TOLL_RATE RM_MRD_SUPP_UNIT_PRICE, ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_MRM_TOLL_AMOUNT RM_MRD_SUPP_AMOUNT, ";
                    SQL = SQL + " 0 RM_MRD_TRANS_RATE, ";
                    SQL = SQL + "0 RM_MRD_TRANS_AMOUNT, ";
                    SQL = SQL + " 0 RM_MRD_TOTAL_AMOUNT, ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE RM_MRD_SUPP_UNIT_PRICE_NEW, ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT RM_MRD_SUPP_AMOUNT_NEW, ";
                    SQL = SQL + "0 RM_MRD_TRANS_RATE_NEW, ";
                    SQL = SQL + " 0 RM_MRD_TRANS_AMOUNT_NEW, ";
                    SQL = SQL + " '' RM_UOM_TOLL_CODE,0 RM_POD_TOLL_RATE_NEW  ,0 RM_MRD_TOLL_AMOUNT_NEW, ";
                    SQL = SQL + " RM_MRM_APPROVED_AMOUNT RM_MRD_TOTAL_AMOUNT_NEW, ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_MRD_SUPP_PENO RM_MRD_SUPP_PENO, RM_RECEPIT_TOLL.RM_MRD_TRANS_PENO RM_MRD_TRANS_PENO ,  ";
                    SQL = SQL + " RM_MRM_TOLL_PE_NO RM_MRD_TOLL_PE_ENTRY_NO ,   ";
                    SQL = SQL + " RM_RECEIPT_MASTER.AD_BR_CODE,RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO,RM_MRM_TOLL_ENTRYTYPE   ";
                    SQL = SQL + " FROM RM_RECEIPT_MASTER, RM_RECEPIT_TOLL, ";
                    SQL = SQL + " RM_RAWMATERIAL_MASTER, ";
                    SQL = SQL + " RM_UOM_MASTER, ";
                    SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER ,  RM_VENDOR_MASTER ,RM_UOM_CATEGORY_MASTER, ";
                    SQL = SQL + " RM_RECEPIT_DETAILS ";
                    SQL = SQL + "  where  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO=RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO  ";
                    SQL = SQL + "  and RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO=RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO  ";
                    SQL = SQL + "  and RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO =RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO  ";
                    SQL = SQL + "    and  RM_RECEPIT_TOLL.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE    ";
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                    SQL = SQL + " AND RM_UOM_MASTER.RM_UOM_CAT_CODE = RM_UOM_CATEGORY_MASTER.RM_UOM_CAT_CODE    ";
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE     ";
                    SQL = SQL + " AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ( +)  ";
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'N' and RM_RECEIPT_MASTER.RM_MRM_RECEIPT_APPROVED ='Y'  ";

                    if (DateTimeFilterType == "Date")
                    {
                        SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))";
                        SQL = SQL + " BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                    }
                    else if (DateTimeFilterType == "Time")
                    {
                        SQL = SQL + "    AND   RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";
                        SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    }

                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE= '" + txtSupplierCode + "'";

                    if (sExFacoryOrOnsite != "ALL")
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='" + sExFacoryOrOnsite + "'";
                    }

                    if (!string.IsNullOrEmpty(sSlectedStation))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE in('" + sSlectedStation + "')";
                    }

                    if (!string.IsNullOrEmpty(sSelectedTrans))
                    {
                        SQL = SQL + "   AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE  in( '" + sSelectedTrans + "')";
                    }

                    if (!string.IsNullOrEmpty(sSelectedRM))
                    {
                        SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ('" + sSelectedRM + "')";
                    }
                    if (!string.IsNullOrEmpty(sSelectedSoruce))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE   IN ('" + sSelectedSoruce + "')";
                    }

                    if (!string.IsNullOrEmpty(POReceiptType))
                    {
                        SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_PO_RM_TYPE ='" + POReceiptType + "'";
                    }

                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE ='" + Branch + "'";
                    }



                    SQL = SQL + " ORDER BY  RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO asc";


                }
                else
                {




                    SQL = "SELECT RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE RM_MRM_TRANSPORTER_CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME TRANSPORTER_NAME, ";
                    SQL = SQL + "       RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO, RM_RECEIPT_MASTER.AD_FIN_FINYRID, RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE, ";
                    SQL = SQL + "       CASE  WHEN TO_CHAR (RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE_TIME, 'HH24:MI:SS') < '07:00:00 AM' ";
                    SQL = SQL + "       THEN  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE - 1 ELSE RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE END RM_MRM_RECEIPT_DATE_FOR_RATE, ";
                    SQL = SQL + "       RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO, RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID, RM_RECEIPT_MASTER.RM_MPOM_PRICE_TYPE, 1 RM_MRD_SL_NO, ";
                    SQL = SQL + "       RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                    SQL = SQL + "       RM_RECEPIT_TOLL.RM_RMM_RM_CODE, RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, RM_RMM_PRODUCT_TYPE,  ";
                    SQL = SQL + "       RM_RECEPIT_TOLL.RM_UOM_UOM_CODE RM_UM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC, RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE, ";
                    SQL = SQL + "       RM_SOURCE_MASTER.RM_SM_SOURCE_DESC, RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO RM_MTRANSPO_ORDER_NO, ";
                    SQL = SQL + "       0 RM_MTRANSPO_FIN_FINYRID, 0 RM_MRM_TRNSPORTER_DOC_NO, 0 RM_MRD_ORDER_QTY, RM_MRM_TOLL_NO RM_MRD_VENDOR_DOC_NO, ";
                    SQL = SQL + "       CASE WHEN RM_UOM_CATEGORY_MASTER.INVOICE_CALC_CODE = 'TR' THEN 1 ELSE RM_MRM_TOLL_QTY END AS RM_MRD_SUPPLY_QTY, ";
                    SQL = SQL + "       CASE WHEN RM_UOM_CATEGORY_MASTER.INVOICE_CALC_CODE = 'TR' THEN 1 ELSE RM_MRM_TOLL_QTY END AS RM_MRD_WEIGH_QTY, ";
                    SQL = SQL + "       CASE WHEN RM_UOM_CATEGORY_MASTER.INVOICE_CALC_CODE = 'TR' THEN 1 ELSE RM_MRM_TOLL_QTY END AS RM_MRD_APPROVE_QTY, ";
                    SQL = SQL + "       0 RM_MRD_REJECTED_QTY, RM_RECEPIT_TOLL.RM_MRM_TOLL_RATE RM_MRD_SUPP_UNIT_PRICE, ";
                    SQL = SQL + "       RM_RECEPIT_TOLL.RM_MRM_TOLL_AMOUNT RM_MRD_SUPP_AMOUNT, 0 RM_MRD_TRANS_RATE, 0 RM_MRD_TRANS_AMOUNT, 0 RM_MRD_TOTAL_AMOUNT,  ";
                    SQL = SQL + "       RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE RM_MRD_SUPP_UNIT_PRICE_NEW,  RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT RM_MRD_SUPP_AMOUNT_NEW,  ";
                    SQL = SQL + "       0 RM_MRD_TRANS_RATE_NEW, 0 RM_MRD_TRANS_AMOUNT_NEW, '' RM_UOM_TOLL_CODE, 0 RM_POD_TOLL_RATE_NEW, 0 RM_MRD_TOLL_AMOUNT_NEW, ";
                    SQL = SQL + "       RM_MRM_APPROVED_AMOUNT RM_MRD_TOTAL_AMOUNT_NEW, RM_RECEPIT_TOLL.RM_MRD_SUPP_PENO RM_MRD_SUPP_PENO,  ";
                    SQL = SQL + "       RM_RECEPIT_TOLL.RM_MRD_TRANS_PENO RM_MRD_TRANS_PENO, RM_MRM_TOLL_PE_NO RM_MRD_TOLL_PE_ENTRY_NO,RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO,  ";
                    SQL = SQL + "       RM_MRM_TOLL_ENTRYTYPE ,RM_RECEIPT_MASTER.AD_BR_CODE ";
                    SQL = SQL + "  FROM RM_RECEIPT_MASTER, RM_RECEPIT_TOLL, RM_RAWMATERIAL_MASTER, ";
                    SQL = SQL + "       RM_UOM_MASTER, SL_STATION_MASTER, RM_SOURCE_MASTER, ";
                    SQL = SQL + "       RM_VENDOR_MASTER, RM_UOM_CATEGORY_MASTER ";
                    SQL = SQL + "  WHERE RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO =RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO ";
                    SQL = SQL + "       AND RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO = RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO ";
                    SQL = SQL + "       AND RM_RECEPIT_TOLL.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                    SQL = SQL + "       AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                    SQL = SQL + "       AND RM_UOM_MASTER.RM_UOM_CAT_CODE = RM_UOM_CATEGORY_MASTER.RM_UOM_CAT_CODE ";
                    SQL = SQL + "       AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                    SQL = SQL + "       AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                    SQL = SQL + "       AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+) ";
                    SQL = SQL + "       AND RM_RECEPIT_TOLL.RM_MRD_APPROVED = 'N' ";
                    SQL = SQL + "       AND RM_RECEIPT_MASTER.RM_MRM_RECEIPT_APPROVED = 'Y' ";

                    if (DateTimeFilterType == "Date")
                    {
                        SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))";
                        SQL = SQL + " BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                    }
                    else if (DateTimeFilterType == "Time")
                    {
                        SQL = SQL + "    AND   RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";
                        SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    }

                    SQL = SQL + " AND RM_RECEIPT_MASTER.RM_VM_VENDOR_CODE= '" + txtSupplierCode + "'";

                    if (sExFacoryOrOnsite != "ALL")
                    {
                        SQL = SQL + " AND  RM_RECEIPT_MASTER.RM_MPOM_PRICE_TYPE ='" + sExFacoryOrOnsite + "'";
                    }

                    if (!string.IsNullOrEmpty(sSlectedStation))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_TOLL.SALES_STN_STATION_CODE in('" + sSlectedStation + "')";
                    }


                    //if (!string.IsNullOrEmpty(sSelectedTrans))
                    //{
                    //    SQL = SQL + "   AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE  in( '" + sSelectedTrans + "')"; //Commented by Alex in 20-06-2024
                    //}// Transporter Fileter no need for Steel Receipt //



                    //if (!string.IsNullOrEmpty(sSelectedRM))
                    //{
                    //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ('" + sSelectedRM + "')";//Commented by Alex in 20-06-2024
                    //}// Raw Material Filter No needed



                    //if (!string.IsNullOrEmpty(sSelectedSoruce))
                    //{
                    //    SQL = SQL + " AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE   IN ('" + sSelectedSoruce + "')";
                    //}// Source Filter also no need



                    if (!string.IsNullOrEmpty(POReceiptType))
                    {
                        SQL = SQL + " AND RM_RECEIPT_MASTER.RM_PO_RM_TYPE ='" + POReceiptType + "'";
                    }

                    if (!string.IsNullOrEmpty(Branch))
                    {
                        SQL = SQL + " AND RM_RECEIPT_MASTER.AD_BR_CODE ='" + Branch + "'";
                    }

                    SQL = SQL + " ORDER BY  RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO asc";
                }



                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return sReturn;
        }


        public DataSet Fetch_OnSite_Supplier_Price(string StCode, string SuppCode, string RmCode, string SourceCode, string UomCode, DateTime dtpToDate)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "     SELECT    ";
                SQL = SQL + "        nvl(RM_RATE_SHEET.RM_RS_OUTSIDE,0) RM_RS_OUTSIDE  , RM_UM_UOM_CODE, ";
                SQL = SQL + "        MAX(RM_RS_EFFDATE)  RM_RS_EFFDATE ";
                SQL = SQL + "    FROM   ";
                SQL = SQL + "     RM_RATE_SHEET  ";
                SQL = SQL + "    WHERE  ";
                SQL = SQL + "           RM_RATE_SHEET.RM_RS_OUTSIDE>0 ";
                SQL = SQL + "        AND  RM_RATE_SHEET.SALES_STN_STATION_CODE= '" + StCode + "'";
                SQL = SQL + "        AND  RM_RATE_SHEET.RM_VM_VENDOR_CODE= '" + SuppCode + "'";
                SQL = SQL + "        AND RM_RATE_SHEET.RM_RMM_RM_CODE= '" + RmCode + "'";
                SQL = SQL + "        AND RM_RATE_SHEET.RM_SM_SOURCE_CODE='" + SourceCode + "'";
                SQL = SQL + "        AND  RM_RATE_SHEET.RM_RS_EFFDATE =   ";
                SQL = SQL + "        ( ";
                SQL = SQL + "         SELECT    MAX(RM_RS_EFFDATE) ";
                SQL = SQL + "         FROM  ";
                SQL = SQL + "             RM_RATE_SHEET  ";
                SQL = SQL + "        where ";
                SQL = SQL + "           RM_RATE_SHEET.SALES_STN_STATION_CODE= '" + StCode + "'";
                SQL = SQL + "        AND  RM_RATE_SHEET.RM_VM_VENDOR_CODE= '" + SuppCode + "'";
                SQL = SQL + "        AND RM_RATE_SHEET.RM_RMM_RM_CODE= '" + RmCode + "'";
                SQL = SQL + "        AND RM_RATE_SHEET.RM_SM_SOURCE_CODE='" + SourceCode + "'";
                SQL = SQL + "        and  TO_DATE(TO_CHAR( RM_RATE_SHEET.RM_RS_EFFDATE,'DD-MON-YYYY')) <=  '" + dtpToDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "        )   ";
                SQL = SQL + "    GROUP BY RM_RS_OUTSIDE, RM_UM_UOM_CODE ";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return sReturn;

        }


        public DataSet Fetch_ExFactory_Supplier_Price(string StCode, string SuppCode, string RmCode, string SourceCode, string UomCode, DateTime dtpToDate)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "     SELECT    ";
                SQL = SQL + "        nvl(RM_RATE_SHEET.RM_RS_EXFACT,0) RM_RS_EXFACT  , RM_UM_UOM_CODE, ";
                SQL = SQL + "        MAX(RM_RS_EFFDATE)  RM_RS_EFFDATE ";
                SQL = SQL + "    FROM   ";
                SQL = SQL + "     RM_RATE_SHEET  ";
                SQL = SQL + "    WHERE  ";
                SQL = SQL + "           RM_RATE_SHEET.RM_RS_EXFACT >0 ";
                SQL = SQL + "        AND  RM_RATE_SHEET.SALES_STN_STATION_CODE= '" + StCode + "'";
                SQL = SQL + "        AND  RM_RATE_SHEET.RM_VM_VENDOR_CODE= '" + SuppCode + "'";
                SQL = SQL + "        AND RM_RATE_SHEET.RM_RMM_RM_CODE= '" + RmCode + "'";
                SQL = SQL + "        AND RM_RATE_SHEET.RM_SM_SOURCE_CODE='" + SourceCode + "'";
                SQL = SQL + "        AND  RM_RATE_SHEET.RM_RS_EFFDATE =   ";
                SQL = SQL + "        ( ";
                SQL = SQL + "         SELECT    MAX(RM_RS_EFFDATE) ";
                SQL = SQL + "         FROM  ";
                SQL = SQL + "             RM_RATE_SHEET  ";
                SQL = SQL + "        where ";
                SQL = SQL + "           RM_RATE_SHEET.SALES_STN_STATION_CODE= '" + StCode + "'";
                SQL = SQL + "        AND  RM_RATE_SHEET.RM_VM_VENDOR_CODE= '" + SuppCode + "'";
                SQL = SQL + "        AND RM_RATE_SHEET.RM_RMM_RM_CODE= '" + RmCode + "'";
                SQL = SQL + "        AND RM_RATE_SHEET.RM_SM_SOURCE_CODE='" + SourceCode + "'";
                SQL = SQL + "        and  TO_DATE(TO_CHAR( RM_RATE_SHEET.RM_RS_EFFDATE,'DD-MON-YYYY')) <=  '" + dtpToDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "        )   ";
                SQL = SQL + "    GROUP BY RM_RS_EXFACT, RM_UM_UOM_CODE ";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return sReturn;

        }

        public DataSet Fetch_ExFactory_Transporter_Price(string StCode, string TransPorterCode, string RmCode, string SourceCode, string UomCode, DateTime dtpToDate)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  SELECT  NVL(RM_TRS_PRICE,0) RM_TRS_PRICE , RM_UM_UOM_CODE,MAX(RM_TRS_EFFDATE) RM_TRS_EFFDATE ";
                SQL = SQL + " FROM  RM_TRANSRATE_SHEET ";
                SQL = SQL + " WHERE    ";
                SQL = SQL + "    SALES_STN_STATION_CODE='" + StCode + "'";
                SQL = SQL + "    AND  RM_VM_VENDOR_CODE='" + TransPorterCode + "'";
                SQL = SQL + "    AND  RM_RMM_RM_CODE='" + RmCode + "'";
                SQL = SQL + "    AND  RM_SM_SOURCE_CODE='" + SourceCode + "'";
                SQL = SQL + "    AND  RM_TRS_EFFDATE= ";
                SQL = SQL + "    ( ";
                SQL = SQL + "     SELECT MAX(RM_TRS_EFFDATE)  ";
                SQL = SQL + "     FROM RM_TRANSRATE_SHEET  ";
                SQL = SQL + "     WHERE  ";
                SQL = SQL + "    SALES_STN_STATION_CODE='" + StCode + "'";
                SQL = SQL + "    AND  RM_VM_VENDOR_CODE='" + TransPorterCode + "'";
                SQL = SQL + "    AND  RM_RMM_RM_CODE='" + RmCode + "'";
                SQL = SQL + "    AND  RM_SM_SOURCE_CODE='" + SourceCode + "'";

                SQL = SQL + "       AND   TO_DATE(TO_CHAR(RM_TRS_EFFDATE,'DD-MON-YYYY'))   <='" + dtpToDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "      ) ";
                SQL = SQL + " GROUP BY  RM_TRS_PRICE, RM_UM_UOM_CODE ";


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

        #region "new concept for price RateCard"


        //--- again concept is got changed for eb , now it shold be from the  purchase order  // jomy 01 aug 2021 


        public DataSet Fetch_OnSite_Supplier_RateCard(string SuppCode, DateTime dtpToDate)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                //OLD QRY MODIFIED BY JOMY  01 AUG 2021 
                ////SQL = "     SELECT    ";
                ////SQL = SQL + "    nvl(RM_RATE_SHEET.RM_RS_OUTSIDE,0) RM_RS_OUTSIDE  ,  ";
                ////SQL = SQL + "    nvl(  RM_RS_QTY_TOLL_RATE ,0) RM_RS_QTY_TOLL_RATE ,  ";
                ////SQL = SQL + "    nvl(RM_RS_TRIP_TOLL_RATE,0) RM_RS_TRIP_TOLL_RATE    , ";
                ////SQL = SQL + "    RM_RS_EFFDATE , RM_UM_UOM_CODE,  ";
                ////SQL = SQL + "           RM_RATE_SHEET.SALES_STN_STATION_CODE ,  RM_RATE_SHEET.RM_VM_VENDOR_CODE , RM_RATE_SHEET.RM_RMM_RM_CODE ,";
                ////SQL = SQL + "   RM_RATE_SHEET.RM_SM_SOURCE_CODE ";

                ////SQL = SQL + "    FROM   ";
                ////SQL = SQL + "     RM_RATE_SHEET  ";
                ////SQL = SQL + "    WHERE  ";
                ////SQL = SQL + "           RM_RATE_SHEET.RM_RS_OUTSIDE>0 ";
                ////SQL = SQL + "        AND  RM_RATE_SHEET.RM_VM_VENDOR_CODE= '" + SuppCode + "'";
                ////SQL = SQL + "        and  TO_DATE(TO_CHAR( RM_RATE_SHEET.RM_RS_EFFDATE,'DD-MON-YYYY')) <=  '" + dtpToDate.ToString("dd-MMM-yyyy") + "'";


                SQL = "SELECT  RM_PO_MASTER.RM_PO_PONO," +
                "    RM_VM_VENDOR_CODE, RM_PO_APPROVED, RM_PO_PRICETYPE , RM_PO_ENTRY_TYPE , " +
                "     RM_PO_AGG_START_DATE RM_RS_EFFDATE ,SALES_STN_STATION_CODE, " +
                "    RM_SM_SOURCE_CODE,RM_RMM_RM_CODE, RM_PO_DETAILS.RM_UOM_UOM_CODE RM_UM_UOM_CODE ,   " +
                "    RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE, " +
                "    NVL(   RM_POD_NEWPRICE ,0) RM_RS_OUTSIDE ,     " +
                "    DECODE ( RM_UOM_TOLL_CODE ,'TRIP', RM_POD_TOLL_RATE ,0)  RM_RS_TRIP_TOLL_RATE , " +
                "    DECODE ( RM_UOM_TOLL_CODE ,'QTY', RM_POD_TOLL_RATE ,0)  RM_RS_QTY_TOLL_RATE   " +
                "  FROM RM_PO_MASTER ,RM_PO_DETAILS " +
                "  WHERE  RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID  " +
                "    AND   RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO  " +
                "    AND  NVL(   RM_POD_NEWPRICE ,0) >0  AND RM_PO_MASTER.RM_PO_APPROVED ='Y' AND RM_PO_PRICETYPE='ONSITE' and RM_PO_ENTRY_TYPE ='PURCHASE'  ";

                SQL = SQL + "        AND  RM_PO_MASTER.RM_VM_VENDOR_CODE= '" + SuppCode + "'  and RM_PO_RATE_REVISION_APP_YN='Y'  ";
                SQL = SQL + "        and  TO_DATE(TO_CHAR( RM_PO_AGG_START_DATE,'DD-MON-YYYY')) <=  '" + dtpToDate.ToString("dd-MMM-yyyy") + "'";




                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return sReturn;

        }



        public DataSet Fetch_ExFactory_Supplier_RateCard(string SuppCode, DateTime dtpToDate)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                //OLD QRY JOMY COMMETNED 01 AUG 2021 
                ////SQL = "     SELECT    ";
                ////SQL = SQL + "        nvl(RM_RATE_SHEET.RM_RS_EXFACT,0) RM_RS_EXFACT  , RM_UM_UOM_CODE,  RM_RS_EFFDATE ,  ";
                ////SQL = SQL + "           RM_RATE_SHEET.SALES_STN_STATION_CODE ,  RM_RATE_SHEET.RM_VM_VENDOR_CODE , RM_RATE_SHEET.RM_RMM_RM_CODE , RM_RATE_SHEET.RM_SM_SOURCE_CODE ";

                ////SQL = SQL + "    FROM       RM_RATE_SHEET  ";
                ////SQL = SQL + "    WHERE  ";
                ////SQL = SQL + "        RM_RATE_SHEET.RM_RS_EXFACT >0 ";
                ////SQL = SQL + "    AND  RM_RATE_SHEET.RM_VM_VENDOR_CODE= '" + SuppCode + "'";
                ////SQL = SQL + "        and  TO_DATE(TO_CHAR( RM_RATE_SHEET.RM_RS_EFFDATE,'DD-MON-YYYY')) <=  '" + dtpToDate.ToString("dd-MMM-yyyy") + "'";



                SQL = "SELECT  RM_PO_MASTER.RM_PO_PONO," +// RM_PO_MASTER.RM_PO_PONO added by abin on 15 Aug 2023
               "    RM_VM_VENDOR_CODE, RM_PO_APPROVED, RM_PO_PRICETYPE ,  RM_PO_ENTRY_TYPE , " +
               "     RM_PO_AGG_START_DATE RM_RS_EFFDATE ,SALES_STN_STATION_CODE, " +
               "    RM_SM_SOURCE_CODE,RM_RMM_RM_CODE,  RM_PO_DETAILS.RM_UOM_UOM_CODE RM_UM_UOM_CODE ,  " +
               "    RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE, " +
               "    NVL(   RM_POD_NEWPRICE ,0) RM_RS_EXFACT ,     " +
               "    DECODE ( RM_UOM_TOLL_CODE ,'TRIP', RM_POD_TOLL_RATE ,0)  RM_RS_TRIP_TOLL_RATE , " +
               "    DECODE ( RM_UOM_TOLL_CODE ,'QTY', RM_POD_TOLL_RATE ,0)  RM_RS_QTY_TOLL_RATE   " +
               "  FROM RM_PO_MASTER ,RM_PO_DETAILS " +
               "  WHERE  RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID  " +
               "    AND   RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO  " +
               "    AND  NVL(   RM_POD_NEWPRICE ,0)>0 AND RM_PO_MASTER.RM_PO_APPROVED ='Y' AND RM_PO_PRICETYPE='EX-FACTORY'  and RM_PO_ENTRY_TYPE ='PURCHASE' ";

                SQL = SQL + "        AND  RM_PO_MASTER.RM_VM_VENDOR_CODE= '" + SuppCode + "'  and RM_PO_RATE_REVISION_APP_YN='Y' ";
                SQL = SQL + "        and  TO_DATE(TO_CHAR( RM_PO_AGG_START_DATE,'DD-MON-YYYY')) <=  '" + dtpToDate.ToString("dd-MMM-yyyy") + "'";



                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return sReturn;

        }

        public DataSet Fetch_ExFactory_Transporter_RateCard(DateTime dtpToDate)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                //SQL = "  SELECT  NVL(RM_TRS_PRICE,0) RM_TRS_PRICE ,   ";
                //SQL = SQL + "     NVL(RM_TRS_QTY_TOLL_RATE,0) RM_TRS_QTY_TOLL_RATE  ,  ";
                //SQL = SQL + "    NVL(RM_TRS_TRIP_TOLL_RATE,0) RM_TRS_TRIP_TOLL_RATE , ";
                //SQL = SQL + "    RM_VM_VENDOR_CODE ,  RM_UM_UOM_CODE, RM_TRS_EFFDATE   , ";
                //SQL = SQL + "    SALES_STN_STATION_CODE ,  RM_VM_VENDOR_CODE , RM_RMM_RM_CODE , RM_SM_SOURCE_CODE ";

                //SQL = SQL + " FROM  RM_TRANSRATE_SHEET ";
                //SQL = SQL + " WHERE    ";
                ////SQL = SQL + "    AND  RM_VM_VENDOR_CODE='" + TransPorterCode + "'"  
                //SQL = SQL + "           TO_DATE(TO_CHAR(RM_TRS_EFFDATE,'DD-MON-YYYY'))   <='" + dtpToDate.ToString("dd-MMM-yyyy") + "'";



                SQL = "SELECT RM_PO_MASTER.RM_PO_PONO," +
             "    RM_VM_VENDOR_CODE,  RM_PO_APPROVED, RM_PO_PRICETYPE , RM_PO_ENTRY_TYPE ,  " +
             "     RM_PO_AGG_START_DATE RM_TRS_EFFDATE ,SALES_STN_STATION_CODE, " +
             "    RM_SM_SOURCE_CODE,RM_RMM_RM_CODE, RM_PO_DETAILS.RM_UOM_UOM_CODE RM_UM_UOM_CODE, " +
             "    RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE, " +
             "    NVL(   RM_POD_NEWPRICE ,0) RM_TRS_PRICE ,     " +
             "    DECODE ( RM_UOM_TOLL_CODE ,'TRIP', RM_POD_TOLL_RATE ,0)  RM_TRS_TRIP_TOLL_RATE , " +
             "    DECODE ( RM_UOM_TOLL_CODE ,'QTY', RM_POD_TOLL_RATE ,0)  RM_TRS_QTY_TOLL_RATE   " +
             "  FROM RM_PO_MASTER ,RM_PO_DETAILS " +
             "  WHERE  RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID  " +
             "    AND   RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO  " +
             "    AND  RM_PO_MASTER.RM_PO_APPROVED ='Y' AND  RM_PO_ENTRY_TYPE ='TRANSPORTER'  ";

                SQL = SQL + " and  TO_DATE(TO_CHAR( RM_PO_AGG_START_DATE,'DD-MON-YYYY')) <=  '" + dtpToDate.ToString("dd-MMM-yyyy") + "'";




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

        #region "DML"

        public string InsertMasterSql(ReceiptApprovalEntity oEntity, List<ReceiptApprovalReceiptDet> EntityReceiptAppr, bool Autogen, object mngrclassobj,
            bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber, List<ReceiptApprovalOtherItemsReceiptDet> EntityReceiptOtherItemsAppr)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();


                SQL = " INSERT INTO RM_RECEIPT_APPL_MASTER (";
                SQL = SQL + " RM_MRMA_APPROVAL_NO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                SQL = SQL + " RM_MRMA_APPROVALTRANS_DATE, RM_MRMA_VENDOR_CODE, RM_MRMA_RECEIPTFROM_DATE, ";
                SQL = SQL + " RM_MRMA_RECEIPTTO_DATE, ";

                SQL = SQL + " RM_MRMA_VENDOR_RV_TOTAL, RM_MRMA_TRNS_RV_TOTAL, RM_MRMA_VENDOR_RV_NEW_TOTAL, ";
                SQL = SQL + " RM_MRMA_TRNS_RV_NEW_TOTAL, RM_MRMA_APP_NET_TOTAL , ";

                SQL = SQL + " RM_MRMA_CREATEDBY, RM_MRMA_CREATEDDATE, ";
                SQL = SQL + " RM_MRMA_UPDATEDBY, RM_MRMA_UPDATEDDATE, RM_MRMA_DELETESTATUS, ";
                SQL = SQL + " RM_MRMA_APPROVED_STATUS, RM_MRMA_APPROVED_BY , ";
                SQL = SQL + " RM_MRMA_VERIFIED_STATUS , RM_MRMA_VERIFIED_BY ,RM_MRMA_REMARKS , ";

                SQL = SQL + "  RM_MRMA_TOLL_RV_TOTAL ,  RM_MRMA_TOLL_RV_NEW_TOTAL, AD_BR_CODE ,RM_MRMA_APP_Date_Type,RM_PO_RM_TYPE  ";

                SQL = SQL + " ) ";
                SQL = SQL + " VALUES ('" + oEntity.txtRecptAppNo + "' ,'" + mngrclass.CompanyCode + "' ," + mngrclass.FinYearID + " ,";
                SQL = SQL + "'" + Convert.ToDateTime(oEntity.dtpTransactionDate).ToString("dd-MMM-yyyy") + "','" + oEntity.txtSupplierCode + "' , '" + Convert.ToDateTime(oEntity.dtpFromDate).ToString("dd-MMM-yyyy") + "' ,";
                SQL = SQL + "'" + Convert.ToDateTime(oEntity.dtpToDate).ToString("dd-MMM-yyyy") + "' , ";
                SQL = SQL + " " + Convert.ToDouble(oEntity.txtRVAmount) + ", " + Convert.ToDouble(oEntity.TxtTransAmount) + " , " + Convert.ToDouble(oEntity.txtRVAmountNew) + " ,";
                SQL = SQL + "" + Convert.ToDouble(oEntity.TxtTransAmountNew) + " , " + Convert.ToDouble(oEntity.txtGrandTotalNew) + " , ";
                SQL = SQL + " '" + mngrclass.UserName + "', TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " '','' ,0 ,";
                SQL = SQL + "'" + oEntity.approve + "' ,'" + oEntity.txtApprovedBy + "' , ";
                SQL = SQL + "'" + oEntity.varify + "' ,'" + oEntity.txtVerifiedBy + "',";
                SQL = SQL + " '" + oEntity.txtRemarks + "',";
                SQL = SQL + " " + Convert.ToDouble(oEntity.TxtTollRVAmount) + ", ";
                SQL = SQL + " " + Convert.ToDouble(oEntity.TxtTollRvAmountNew) + ",'" + oEntity.Branch + "','" + oEntity.DateType + "','" + oEntity.ReceiptType + "'";
                SQL = SQL + ")";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertRADetails(oEntity, EntityReceiptAppr, EntityReceiptOtherItemsAppr, mngrclassobj);
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

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMAPP", oEntity.txtRecptAppNo, Autogen, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);

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

        private string InsertRADetails(ReceiptApprovalEntity oEntity, List<ReceiptApprovalReceiptDet> EntityReceiptAppr,
            List<ReceiptApprovalOtherItemsReceiptDet> EntityReceiptOtherItemsAppr, object mngrclassobj)
        {
            string sRetun = string.Empty;

            int i = 0;
            //object oChk = null;


            try
            {

                SessionManager mngrclass = (SessionManager)mngrclassobj;
                //<< inserting recipt details
                {
                    i = 0;
                    foreach (var Data in EntityReceiptAppr)
                    {
                        ++i;
                        SQL = " INSERT INTO RM_RECEIPT_APPL_DETAILS (";
                        SQL = SQL + " RM_MRMA_APPROVAL_NO, AD_FIN_FINYRID, RM_MRM_RECEIPT_UNIQE_NO, ";
                        SQL = SQL + " RM_MRM_RECEIPT_NO, AD_FIN_FINYRID_RECEIPT , ";
                        SQL = SQL + " RM_MRMA_APPROVED_STATUS , RM_MRMA_ALLOW_EDIT ,RM_MRMA_APPROVAL_SLNO ,RM_MRM_RECEIPT_DATE_FOR_RATE) ";
                        SQL = SQL + " VALUES ('" + oEntity.txtRecptAppNo + "' ," + mngrclass.FinYearID + " ," + Data.dRctUniquenoReceiptDet + " ,";
                        SQL = SQL + "'" + Data.oReceiptNoReceiptDet + "'," + Data.iReceiptFinYrReceiptDet + ",";
                        SQL = SQL + "'" + oEntity.approve + "', 'N', " + i + ",'" + Convert.ToDateTime(Data.dRctReceiptRateDate).ToString("dd-MMM-yyyy") + "' ";
                        SQL = SQL + " )";

                        sSQLArray.Add(SQL);
                    }

                    {
                        foreach (var Data in EntityReceiptAppr)
                        {
                            SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                            SQL = SQL + " RM_MRD_APPROVE_QTY = " + Convert.ToDouble(Data.dApprovedQtyReceiptDet) + " , RM_MRD_REJECTED_QTY = " + Convert.ToDouble(Data.dRejectedQtyReceiptDet) + ", ";

                            SQL = SQL + " RM_MRD_SUPP_UNIT_PRICE_NEW =" + Convert.ToDouble(Data.dNewSuppUnitPriceReceiptDet) + ", RM_MRD_SUPP_AMOUNT_NEW = " + Convert.ToDouble(Data.dNewSuppAmountReceiptDet) + ", ";
                            SQL = SQL + " RM_MRD_TRANS_RATE_NEW= " + Convert.ToDouble(Data.dNewTransUnitPriceReceiptDet) + ",";
                            SQL = SQL + " RM_MRD_TRANS_AMOUNT_NEW= " + Convert.ToDouble(Data.dNewTransAmountReceiptDet) + " , RM_MRD_TOTAL_AMOUNT_NEW =" + Convert.ToDouble(Data.dNewTotalAmountReceiptDet) + ",";
                            SQL = SQL + "  RM_UOM_TOLL_CODE ='" + Data.sRctTollUOMCode + "' , RM_POD_TOLL_RATE_NEW = " + Convert.ToDouble(Data.dRctTollRateNew) + "  ,RM_MRD_TOLL_AMOUNT_NEW=" + Convert.ToDouble(Data.dRctTollAmountNew) + " ,";


                            SQL = SQL + " RM_MRD_APPROVED='Y',";
                            SQL = SQL + " RM_MRM_APPROVED_NO='" + oEntity.txtRecptAppNo + "' , RM_MRD_APPROVED_FINYRID=" + mngrclass.FinYearID + ",RM_MPOM_ORDER_NO_NEW='" + Data.doReceipt_NewPo_No + "',RM_MTRANSPO_ORDER_NO_NEW='" + Data.doReceipt_Transp_NewPo_No + "' ";
                            SQL = SQL + " WHERE ";
                            SQL = SQL + " RM_MRM_RECEIPT_UNIQE_NO = " + Data.dRctUniquenoReceiptDet + " AND RM_MRM_RECEIPT_NO = '" + Data.oReceiptNoReceiptDet + "' AND AD_FIN_FINYRID= " + Data.iReceiptFinYrReceiptDet + "";

                            sSQLArray.Add(SQL);
                        }


                        foreach (var Data in EntityReceiptOtherItemsAppr)
                        {
                            SQL = " UPDATE RM_RECEPIT_TOLL SET ";
                            SQL = SQL + " RM_MRM_APPROVED_QTY = " + Convert.ToDouble(Data.dOtherItemsApprovedQtyReceiptDet) + " , ";

                            SQL = SQL + " RM_MRM_APRPOVED_RATE =" + Convert.ToDouble(Data.dOtherItemsNewSuppUnitPriceReceiptDet) + ", ";
                            SQL = SQL + "  RM_MRM_APPROVED_AMOUNT = " + Convert.ToDouble(Data.dOtherItemsNewSuppAmountReceiptDet) + ", ";
                            SQL = SQL + " RM_MRD_APPROVED='Y',";
                            SQL = SQL + " RM_MRM_APPROVED_NO='" + oEntity.txtRecptAppNo + "' , RM_MRD_APPROVED_FINYRID=" + mngrclass.FinYearID + " ,RM_MPOM_ORDER_NO_NEW='" + Data.doReceipt_NewPo_No + "'";
                            SQL = SQL + " WHERE ";
                            SQL = SQL + " RM_MRM_TOLL_SLNO = " + Data.dOtherItemsRctUniquenoReceiptDet + " AND RM_MRM_RECEIPT_NO = '" + Data.oOtherItemsReceiptNoReceiptDet + "'";
                            SQL = SQL + "  AND AD_FIN_FINYRID= " + Data.iOtherItemsReceiptFinYrReceiptDet + " and RM_MRM_TOLL_ENTRYTYPE= '" + Data.Item_ReceiptType + "'";

                            sSQLArray.Add(SQL);
                        }
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

        private string InsertApprovalQrs(ReceiptApprovalEntity oEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = " INSERT INTO RM_RECEIPT_APPL_TRIGGER (";
                SQL = SQL + " RM_MRMA_APPROVAL_NO, RM_MRMA_APPROVALTRANS_DATE, AD_FIN_FINYRID, RM_MRMA_APPROVED_BY) ";
                SQL = SQL + " VALUES ( '" + oEntity.txtRecptAppNo + "', '" + Convert.ToDateTime(oEntity.dtpTransactionDate).ToString("dd-MMM-yyyy") + "' ," + mngrclass.FinYearID + ",'" + oEntity.txtApprovedBy + "' )";


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

        public string ModifySql(ReceiptApprovalEntity oEntity, List<ReceiptApprovalReceiptDet> EntityReceiptAppr,
            List<ReceiptApprovalOtherItemsReceiptDet> EntityReceiptOtherItemsAppr, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " UPDATE RM_RECEIPT_APPL_MASTER SET ";

                SQL = SQL + " RM_MRMA_APPROVALTRANS_DATE = '" + Convert.ToDateTime(oEntity.dtpTransactionDate).ToString("dd-MMM-yyyy") + "', RM_MRMA_REMARKS ='" + oEntity.txtRemarks + "',";

                SQL = SQL + " RM_MRMA_VENDOR_RV_TOTAL = " + Convert.ToDouble(oEntity.txtRVAmount) + " , RM_MRMA_TRNS_RV_TOTAL = " + Convert.ToDouble(oEntity.TxtTransAmount) + ", RM_MRMA_VENDOR_RV_NEW_TOTAL = " + Convert.ToDouble(oEntity.TxtTransAmountNew) + " , ";
                SQL = SQL + " RM_MRMA_TRNS_RV_NEW_TOTAL = " + Convert.ToDouble(oEntity.TxtTransAmountNew) + " , RM_MRMA_APP_NET_TOTAL = " + Convert.ToDouble(oEntity.txtGrandTotalNew) + " , ";

                SQL = SQL + " RM_MRMA_RECEIPTFROM_DATE = '" + Convert.ToDateTime(oEntity.dtpFromDate).ToString("dd-MMM-yyyy") + "' , RM_MRMA_RECEIPTTO_DATE = '" + Convert.ToDateTime(oEntity.dtpToDate).ToString("dd-MMM-yyyy") + "' , ";
                SQL = SQL + " RM_MRMA_UPDATEDBY = '" + mngrclass.UserName + "' , RM_MRMA_UPDATEDDATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM') ,";
                SQL = SQL + " RM_MRMA_APPROVED_STATUS = '" + oEntity.approve + "' , RM_MRMA_APPROVED_BY = '" + oEntity.txtApprovedBy + "' , ";
                SQL = SQL + " RM_MRMA_VERIFIED_STATUS ='" + oEntity.varify + "', RM_MRMA_VERIFIED_BY = '" + oEntity.txtVerifiedBy + "',";
                SQL = SQL + " RM_MRMA_TOLL_RV_TOTAL= " + Convert.ToDouble(oEntity.TxtTollRVAmount) + "  ,  RM_MRMA_TOLL_RV_NEW_TOTAL =" + Convert.ToDouble(oEntity.TxtTollRvAmountNew) + "";


                SQL = SQL + "WHERE RM_MRMA_APPROVAL_NO = '" + oEntity.txtRecptAppNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_RECEIPT_APPL_DETAILS ";
                SQL = SQL + " WHERE RM_MRMA_APPROVAL_NO = '" + oEntity.txtRecptAppNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                SQL = SQL + " RM_MRD_APPROVED='N',";
                SQL = SQL + " RM_MRM_APPROVED_NO=NULL , RM_MRD_APPROVED_FINYRID=0";
                SQL = SQL + " WHERE ";
                SQL = SQL + "   RM_MRM_APPROVED_NO = '" + oEntity.txtRecptAppNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                SQL = " UPDATE RM_RECEPIT_TOLL SET ";
                SQL = SQL + " RM_MRD_APPROVED='N',";
                SQL = SQL + " RM_MRM_APPROVED_NO=NULL , RM_MRD_APPROVED_FINYRID=0";
                //RM_MRM_APPROVED_QTY=0,RM_MRM_APRPOVED_RATE=0,RM_MRM_APPROVED_AMOUNT=0  
                SQL = SQL + " WHERE ";
                SQL = SQL + "   RM_MRM_APPROVED_NO = '" + oEntity.txtRecptAppNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "  ";

                sSQLArray.Add(SQL);

                sRetun = InsertRADetails(oEntity, EntityReceiptAppr, EntityReceiptOtherItemsAppr, mngrclassobj);
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

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMAPP", oEntity.txtRecptAppNo, false, Environment.MachineName, "U", sSQLArray);

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


        public string DeleteSql(string txtRecptAppNo, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " DELETE FROM RM_RECEIPT_APPL_MASTER ";
                SQL = SQL + "WHERE RM_MRMA_APPROVAL_NO = '" + txtRecptAppNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_RECEIPT_APPL_DETAILS ";
                SQL = SQL + " WHERE RM_MRMA_APPROVAL_NO = '" + txtRecptAppNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                SQL = " UPDATE RM_RECEPIT_DETAILS SET ";
                SQL = SQL + " RM_MRD_APPROVED='N',";
                SQL = SQL + " RM_MRM_APPROVED_NO=NULL , RM_MRD_APPROVED_FINYRID=0";
                SQL = SQL + " WHERE ";
                SQL = SQL + "  RM_MRM_APPROVED_NO = '" + txtRecptAppNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);


                SQL = " UPDATE RM_RECEPIT_TOLL SET ";
                SQL = SQL + " RM_MRD_APPROVED='N',";
                SQL = SQL + " RM_MRM_APPROVED_NO=NULL , RM_MRD_APPROVED_FINYRID=0  ";
                SQL = SQL + " WHERE ";
                SQL = SQL + "   RM_MRM_APPROVED_NO = '" + txtRecptAppNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMAPP", txtRecptAppNo, false, Environment.MachineName, "D", sSQLArray);
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

        #region "Fetchss"


        public object FillViewReceiptApproval(string fromdate, string todate, string EntryType, object mngrclassobj)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT RM_RECEIPT_APPL_MASTER.rm_mrma_approval_no APPROVALNO,";

                SQL = SQL + " RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID FinId,";
                SQL = SQL + " to_char(RM_RECEIPT_APPL_MASTER.rm_mrma_approvaltrans_date,'DD-MON-YYYY') TRANSDATE,";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.rm_mrma_vendor_code SUPPCODE,";
                SQL = SQL + " rm_vendor_master.rm_vm_vendor_name SUPPNAME,";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.rm_mrma_receiptfrom_date RCTFORMDATE,";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.rm_mrma_receiptto_date RCTTODATE,";
                SQL = SQL + " RM_MRMA_VENDOR_RV_TOTAL RV_TOTAL, RM_MRMA_TRNS_RV_TOTAL TRANS_RV_TOTAL,";
                SQL = SQL + " RM_MRMA_VENDOR_RV_NEW_TOTAL RN_NEW_TOTAL , RM_MRMA_TRNS_RV_NEW_TOTAL TRANS_NEW_TOTAL,";
                SQL = SQL + " RM_MRMA_APP_NET_TOTAL NET_TOTAL,RM_MRMA_VERIFIED_STATUS VERIFIED,";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.rm_mrma_approved_status APPSTATUS,";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.rm_mrma_approved_by APPBY ";

                SQL = SQL + " FROM RM_RECEIPT_APPL_MASTER, rm_vendor_master";

                SQL = SQL + " WHERE RM_RECEIPT_APPL_MASTER.rm_mrma_vendor_code = rm_vendor_master.rm_vm_vendor_code";
                SQL = SQL + " and RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID =" + mngrclass.FinYearID;

                if (EntryType == "A")
                {
                    //<
                }
                else if (EntryType == "Y")
                {
                    SQL = SQL + " AND RM_MRMA_APPROVED_STATUS ='Y'";
                }
                else if (EntryType == "N")
                {
                    SQL = SQL + " AND RM_MRMA_APPROVED_STATUS ='N'";
                }

                SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";

                if (mngrclass.UserName != "ADMIN")
                { // JOMY ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 06 aug 2020
                    SQL = SQL + "   and   RM_RECEIPT_APPL_MASTER.AD_BR_CODE  IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                }


                SQL = SQL + " order by RM_RECEIPT_APPL_MASTER.rm_mrma_approval_no DESC";

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

        public DataSet ReceiptApprovalReceiptDetails(string ApprovalNo, string FinYear)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO,  RM_VENDOR_MASTER.RM_VM_VENDOR_NAME  TRANSPORTER_NAME, ";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_FIN_FINYRID,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID, ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SL_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_UM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE, ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO, RM_MTRANSPO_FIN_FINYRID , RM_MRM_TRNSPORTER_DOC_NO ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_ORDER_QTY, RM_MRD_VENDOR_DOC_NO ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY, RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO, RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO , ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_UNIQE_NO , RM_RECEIPT_APPL_DETAILS.RM_MRMA_APPROVAL_SLNO, ";

                SQL = SQL + " RM_RECEPIT_DETAILS.RM_UOM_TOLL_CODE, RM_POD_TOLL_RATE_NEW  ,RM_MRD_TOLL_AMOUNT_NEW ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOLL_PE_ENTRY_NO ,RM_MRM_RECEIPT_DATE_FOR_RATE ,RM_MPOM_ORDER_NO_NEW,RM_MTRANSPO_ORDER_NO_NEW  ";


                SQL = SQL + " FROM RM_RECEPIT_DETAILS , ";
                SQL = SQL + " RM_RECEIPT_APPL_DETAILS, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER,";
                SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER  ,RM_VENDOR_MASTER ";

                SQL = SQL + " WHERE RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.rm_mrm_receipt_uniqe_no = RM_RECEIPT_APPL_DETAILS.rm_mrm_receipt_uniqe_no ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.rm_mrm_receipt_no = RM_RECEIPT_APPL_DETAILS.rm_mrm_receipt_no ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.ad_fin_finyrid = RM_RECEIPT_APPL_DETAILS.ad_fin_finyrid_receipt ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.rm_mrm_approved_no = RM_RECEIPT_APPL_DETAILS.rm_mrma_approval_no ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.rm_mrd_approved_finyrid = RM_RECEIPT_APPL_DETAILS.ad_fin_finyrid";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ( +) ";

                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO='" + ApprovalNo + "'";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID= " + FinYear + "";

                SQL = SQL + " ORDER BY  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO asc";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }
        public DataSet ReceiptApprovalOtherItemsReceiptDetails(string ApprovalNo, string FinYear)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                if(POReceiptType=="RM") { 
                SQL = " SELECT  RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE RM_MRM_TRANSPORTER_CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME  TRANSPORTER_NAME,   ";
                SQL = SQL + " RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO, ";
                SQL = SQL + " RM_RECEIPT_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL + " RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "case when    ";
                SQL = SQL + "TO_CHAR(RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE_TIME,'HH24:MI:SS') < '07:00:00 AM'    ";
                SQL = SQL + "then  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE -1  ";
                SQL = SQL + "else  ";
                SQL = SQL + " RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE    ";
                SQL = SQL + "END ";
                SQL = SQL + " RM_MRM_RECEIPT_DATE_FOR_RATE,   ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO, ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID,  ";
                SQL = SQL + " RM_RECEIPT_MASTER.RM_MPOM_PRICE_TYPE, ";
                SQL = SQL + " 1 RM_MRD_SL_NO, ";
                SQL = SQL + " RM_RECEPIT_TOLL.SALES_STN_STATION_CODE, ";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_RMM_RM_CODE, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,RM_RMM_PRODUCT_TYPE, ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_UOM_UOM_CODE RM_UM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC, ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC , ";
                SQL = SQL + "RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO RM_MTRANSPO_ORDER_NO,0 RM_MTRANSPO_FIN_FINYRID ,0 RM_MRM_TRNSPORTER_DOC_NO , ";
                SQL = SQL + " 0 RM_MRD_ORDER_QTY,RM_MRM_TOLL_NO RM_MRD_VENDOR_DOC_NO , ";
                SQL = SQL + "    CASE   WHEN RM_UOM_CATEGORY_MASTER.INVOICE_CALC_CODE = 'TR'  ";
                SQL = SQL + "    THEN  1  ELSE   RM_MRD_SUPPLY_QTY  END  ";
                SQL = SQL + "    AS  RM_MRD_SUPPLY_QTY, ";
                SQL = SQL + "    CASE   WHEN RM_UOM_CATEGORY_MASTER.INVOICE_CALC_CODE = 'TR'  ";
                SQL = SQL + "    THEN  1  ELSE   RM_MRD_WEIGH_QTY  END  ";
                SQL = SQL + "    AS  RM_MRD_WEIGH_QTY, ";
                SQL = SQL + "    CASE   WHEN RM_UOM_CATEGORY_MASTER.INVOICE_CALC_CODE = 'TR'  ";
                SQL = SQL + "    THEN  1  ELSE   RM_MRD_APPROVE_QTY  END  ";
                SQL = SQL + "    AS RM_MRD_APPROVE_QTY,0   RM_MRD_REJECTED_QTY  , ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_MRM_TOLL_RATE RM_MRD_SUPP_UNIT_PRICE, ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_MRM_TOLL_AMOUNT RM_MRD_SUPP_AMOUNT, ";
                SQL = SQL + " 0 RM_MRD_TRANS_RATE, ";
                SQL = SQL + "0 RM_MRD_TRANS_AMOUNT, ";
                SQL = SQL + " 0 RM_MRD_TOTAL_AMOUNT, ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE RM_MRD_SUPP_UNIT_PRICE_NEW, ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT RM_MRD_SUPP_AMOUNT_NEW, ";
                SQL = SQL + "0 RM_MRD_TRANS_RATE_NEW, ";
                SQL = SQL + " 0 RM_MRD_TRANS_AMOUNT_NEW, ";
                SQL = SQL + " '' RM_UOM_TOLL_CODE,0 RM_POD_TOLL_RATE_NEW  ,0 RM_MRD_TOLL_AMOUNT_NEW, ";
                SQL = SQL + " RM_MRM_APPROVED_AMOUNT RM_MRD_TOTAL_AMOUNT_NEW, ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_MRD_SUPP_PENO RM_MRD_SUPP_PENO, RM_RECEPIT_TOLL.RM_MRD_TRANS_PENO RM_MRD_TRANS_PENO ,  ";
                SQL = SQL + " RM_MRM_TOLL_PE_NO RM_MRD_TOLL_PE_ENTRY_NO ,   ";
                SQL = SQL + " RM_RECEIPT_MASTER.AD_BR_CODE,RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO,RM_MRM_TOLL_ENTRYTYPE ,RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO_NEW  ";
                SQL = SQL + " FROM RM_RECEIPT_MASTER, RM_RECEPIT_TOLL, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + " RM_UOM_MASTER, ";
                SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER ,  RM_VENDOR_MASTER ,RM_UOM_CATEGORY_MASTER, ";
                SQL = SQL + " RM_RECEPIT_DETAILS ";
                SQL = SQL + "  where  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO=RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO  ";
                SQL = SQL + "  and RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO=RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO  ";
                SQL = SQL + "  and RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO =RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO  ";
                SQL = SQL + "    and  RM_RECEPIT_TOLL.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE    ";
                SQL = SQL + " AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + " AND RM_UOM_MASTER.RM_UOM_CAT_CODE = RM_UOM_CATEGORY_MASTER.RM_UOM_CAT_CODE    ";
                SQL = SQL + " AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE     ";
                SQL = SQL + " AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + " AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ( +)  ";
                SQL = SQL + " AND RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO='" + ApprovalNo + "'";
                SQL = SQL + " AND RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID= " + FinYear + "";
                SQL = SQL + " ORDER BY  RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO asc";

                }
                else
                {
                    SQL = " SELECT  RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE RM_MRM_TRANSPORTER_CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME  TRANSPORTER_NAME,    ";
                    SQL = SQL + " RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO, RM_RECEIPT_MASTER.AD_FIN_FINYRID,  ";
                    SQL = SQL + " RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE,  case when  TO_CHAR(RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE_TIME,'HH24:MI:SS') < '07:00:00 AM'     ";
                    SQL = SQL + "then  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE -1   else   RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE END  ";
                    SQL = SQL + " RM_MRM_RECEIPT_DATE_FOR_RATE, RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO, RM_RECEPIT_TOLL.RM_MPOM_FIN_FINYRID,   ";
                    SQL = SQL + " RM_RECEIPT_MASTER.RM_MPOM_PRICE_TYPE, 1 RM_MRD_SL_NO, RM_RECEPIT_TOLL.SALES_STN_STATION_CODE,  ";
                    SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME, RM_RECEPIT_TOLL.RM_RMM_RM_CODE,  ";
                    SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,RM_RMM_PRODUCT_TYPE,  ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_UOM_UOM_CODE RM_UM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC,  ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC ,  ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO RM_MTRANSPO_ORDER_NO,0 RM_MTRANSPO_FIN_FINYRID ,0 RM_MRM_TRNSPORTER_DOC_NO ,  ";
                    SQL = SQL + " 0 RM_MRD_ORDER_QTY,RM_MRM_TOLL_NO RM_MRD_VENDOR_DOC_NO ,  ";
                    SQL = SQL + "  CASE   WHEN RM_UOM_CATEGORY_MASTER.INVOICE_CALC_CODE = 'TR' THEN  1  ELSE   RM_MRM_TOLL_QTY  END AS  RM_MRD_SUPPLY_QTY,   ";
                    SQL = SQL + "  CASE   WHEN RM_UOM_CATEGORY_MASTER.INVOICE_CALC_CODE = 'TR' THEN  1  ELSE   RM_MRM_TOLL_QTY  END AS  RM_MRD_WEIGH_QTY,   ";
                    SQL = SQL + "  CASE   WHEN RM_UOM_CATEGORY_MASTER.INVOICE_CALC_CODE = 'TR' THEN  1  ELSE   RM_MRM_TOLL_QTY  END AS  RM_MRD_APPROVE_QTY,   ";
                    SQL = SQL + " 0   RM_MRD_REJECTED_QTY  ,  ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_MRM_TOLL_RATE RM_MRD_SUPP_UNIT_PRICE, RM_RECEPIT_TOLL.RM_MRM_TOLL_AMOUNT RM_MRD_SUPP_AMOUNT, 0 RM_MRD_TRANS_RATE,  ";
                    SQL = SQL + " 0 RM_MRD_TRANS_AMOUNT, 0 RM_MRD_TOTAL_AMOUNT, RM_RECEPIT_TOLL.RM_MRM_APRPOVED_RATE RM_MRD_SUPP_UNIT_PRICE_NEW,  ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_MRM_APPROVED_AMOUNT RM_MRD_SUPP_AMOUNT_NEW, 0 RM_MRD_TRANS_RATE_NEW, 0 RM_MRD_TRANS_AMOUNT_NEW,  ";
                    SQL = SQL + " '' RM_UOM_TOLL_CODE,0 RM_POD_TOLL_RATE_NEW  ,0 RM_MRD_TOLL_AMOUNT_NEW, RM_MRM_APPROVED_AMOUNT RM_MRD_TOTAL_AMOUNT_NEW,  ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_MRD_SUPP_PENO RM_MRD_SUPP_PENO, RM_RECEPIT_TOLL.RM_MRD_TRANS_PENO RM_MRD_TRANS_PENO ,   ";
                    SQL = SQL + " RM_MRM_TOLL_PE_NO RM_MRD_TOLL_PE_ENTRY_NO , RM_RECEIPT_MASTER.AD_BR_CODE,RM_MRM_TOLL_SLNO RM_MRM_RECEIPT_UNIQE_NO,RM_MRM_TOLL_ENTRYTYPE , ";
                    SQL = SQL + " RM_RECEPIT_TOLL.RM_MPOM_ORDER_NO_NEW   ";
                    SQL = SQL + " FROM RM_RECEIPT_MASTER, RM_RECEPIT_TOLL, RM_RAWMATERIAL_MASTER,  ";
                    SQL = SQL + " RM_UOM_MASTER, SL_STATION_MASTER , RM_SOURCE_MASTER ,  RM_VENDOR_MASTER ,RM_UOM_CATEGORY_MASTER  ";
                    SQL = SQL + "  where  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO=RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO   ";
                    SQL = SQL + "  and RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO =RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO   ";
                    SQL = SQL + "    and  RM_RECEPIT_TOLL.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE     ";
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE   ";
                    SQL = SQL + " AND RM_UOM_MASTER.RM_UOM_CAT_CODE = RM_UOM_CATEGORY_MASTER.RM_UOM_CAT_CODE     ";
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE      ";
                    SQL = SQL + " AND RM_RECEPIT_TOLL.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE  ";
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE (+)   ";
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_MRM_APPROVED_NO='" + ApprovalNo + "'";
                    SQL = SQL + " AND RM_RECEPIT_TOLL.RM_MRD_APPROVED_FINYRID= " + FinYear + "";
                    SQL = SQL + " ORDER BY  RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO asc";
                }

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet ReceiptApprovalStationDetails(string ApprovalNo, string FinYear)
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
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO='" + ApprovalNo + "'";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID= " + FinYear + "";

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

        public DataSet ReceiptApprovalTransDetails(string ApprovalNo, string FinYear)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT DISTINCT RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE CODE ,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME NAME ";

                SQL = SQL + " FROM RM_RECEPIT_DETAILS, RM_VENDOR_MASTER";

                SQL = SQL + " WHERE RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO='" + ApprovalNo + "'";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID= " + FinYear + "";

                SQL = SQL + " ORDER BY RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE asc";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet ReceiptApprovalRMDetails(string ApprovalNo, string FinYear)
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
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO='" + ApprovalNo + "'";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_APPROVED_FINYRID= " + FinYear + "";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet ApprovalMasterFetch(string ApprovalNo, string FinYear)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO, RM_RECEIPT_APPL_MASTER.AD_CM_COMPANY_CODE, RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE, RM_RECEIPT_APPL_MASTER. RM_MRMA_RECEIPTFROM_DATE, ";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.rm_mrma_vendor_code SUPPCODE,";
                SQL = SQL + " rm_vendor_master.rm_vm_vendor_name SUPPNAME,";
                SQL = SQL + " RM_MRMA_RECEIPTTO_DATE, ";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_RV_TOTAL, RM_RECEIPT_APPL_MASTER.RM_MRMA_TRNS_RV_TOTAL,RM_RECEIPT_APPL_MASTER. RM_MRMA_VENDOR_RV_NEW_TOTAL, ";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.RM_MRMA_TRNS_RV_NEW_TOTAL, ";
                SQL = SQL + "  RM_MRMA_TOLL_RV_TOTAL ,  RM_MRMA_TOLL_RV_NEW_TOTAL ,  ";
                SQL = SQL + "  RM_RECEIPT_APPL_MASTER.RM_MRMA_APP_NET_TOTAL , ";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.RM_MRMA_CREATEDBY, ";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.RM_MRMA_CREATEDDATE, RM_RECEIPT_APPL_MASTER.RM_MRMA_UPDATEDBY, RM_RECEIPT_APPL_MASTER.RM_MRMA_UPDATEDDATE, ";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER.RM_MRMA_DELETESTATUS, RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVED_BY, RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVED_STATUS, ";
                SQL = SQL + " RM_RECEIPT_APPL_MASTER. RM_MRMA_VERIFIED_BY,RM_RECEIPT_APPL_MASTER. RM_MRMA_VERIFIED_STATUS,";
                SQL = SQL + " RM_MRMA_REMARKS ,RM_RECEIPT_APPL_MASTER.AD_BR_CODE,AD_BR_NAME,RM_MRMA_APP_Date_Type,RM_PO_RM_TYPE     ";

                SQL = SQL + " FROM RM_RECEIPT_APPL_MASTER, rm_vendor_master,AD_BRANCH_MASTER";

                SQL = SQL + " WHERE RM_RECEIPT_APPL_MASTER.rm_mrma_vendor_code = rm_vendor_master.rm_vm_vendor_code";
                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE";
                SQL = SQL + " and RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID =" + FinYear + "";
                SQL = SQL + " AND RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO='" + ApprovalNo + "'";

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

        public DataSet FetchApprovalPrintData(string sEntryNo, object mngrclassobj)
        {
            DataSet dsPoStatus = new DataSet("RMAPPROVALPRINT");

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = SQL + " SELECT   ";
                SQL = SQL + " RM_MRM_APPROVED_NO, RM_MRD_APPROVED_FINYRID, RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_MRM_RECEIPT_DATE,  ";
                SQL = SQL + "  RM_VM_VENDOR_CODE, SUPPNAME, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, RM_MPOM_PRICE_TYPE,  ";
                SQL = SQL + "  RM_MRD_SL_NO, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION,  ";
                SQL = SQL + "  RM_UM_UOM_CODE, RM_UM_UOM_DESC, RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC, RM_MRM_TRANSPORTER_CODE,  ";
                SQL = SQL + "  TRNSPORTERNAME, RM_MTRANSPO_ORDER_NO, RM_MTRANSPO_FIN_FINYRID, RM_MRM_TRNSPORTER_DOC_NO, RM_MRD_ORDER_QTY,  ";
                SQL = SQL + "  RM_MRD_VENDOR_DOC_NO, FA_FAM_ASSET_CODE, RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME,  ";
                SQL = SQL + "  RM_MRD_SUPPLY_QTY, RM_MRD_WEIGH_QTY, RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY, RM_MRD_SUPP_UNIT_PRICE,  ";
                SQL = SQL + "  RM_MRD_SUPP_AMOUNT, RM_MRD_TRANS_RATE, RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT, RM_MRD_SUPP_UNIT_PRICE_NEW,  ";
                SQL = SQL + "  RM_MRD_SUPP_AMOUNT_NEW, RM_MRD_TRANS_RATE_NEW, RM_MRD_TRANS_AMOUNT_NEW, RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_SUPP_PENO,  ";
                SQL = SQL + "  RM_MRD_TRANS_PENO, RM_MRM_RECEIPT_UNIQE_NO, RM_MRMA_APPROVALTRANS_DATE, RM_MRMA_RECEIPTFROM_DATE, RM_MRMA_RECEIPTTO_DATE,  ";
                SQL = SQL + "  RM_MRMA_VENDOR_RV_TOTAL, RM_MRMA_TRNS_RV_TOTAL, RM_MRMA_VENDOR_RV_NEW_TOTAL, RM_MRMA_TRNS_RV_NEW_TOTAL, RM_MRMA_APP_NET_TOTAL,  ";
                SQL = SQL + "  RM_MRMA_CREATEDBY, RM_MRMA_UPDATEDBY, RM_MRMA_UPDATEDDATE, RM_MRMA_APPROVED_BY, RM_MRMA_APPROVED_STATUS,  ";
                SQL = SQL + "  RM_MRMA_VERIFIED_BY ,RM_MRMA_REMARKS ,  ";
                SQL = SQL + " RM_UOM_TOLL_CODE  ,  RM_UOM_TOLL_NAME , RM_POD_TOLL_RATE  , RM_MRD_TOLL_AMOUNT    ,";
                SQL = SQL + "RM_POD_TOLL_RATE_NEW  , RM_MRD_TOLL_AMOUNT_NEW     ,";
                SQL = SQL + " RM_MRD_TOLL_PE_ENTRY_YN    ,   RM_MRD_TOLL_PE_ENTRY_NO,  RM_MRD_TOLL_PE_ENTRY_FINYR_ID ,";
                SQL = SQL + "   MASTER_BRANCH_CODE, ";
                SQL = SQL + "   MASTER_BRANCH_NAME, ";
                SQL = SQL + "   MASTER_BRANCHDOC_PREFIX, ";
                SQL = SQL + "   MASTER_BRANCH_POBOX, ";
                SQL = SQL + "   MASTER_BRANCH_ADDRESS, ";
                SQL = SQL + "   MASTER_BRANCH_CITY, ";
                SQL = SQL + "   MASTER_BRANCH_PHONE, ";
                SQL = SQL + "   MASTER_BRANCH_FAX, ";
                SQL = SQL + "   MASTER_BRANCH_SPONSER_NAME, ";
                SQL = SQL + "   MASTER_BRANCH_TRADE_LICENSE_NO, ";
                SQL = SQL + "   MASTER_BRANCH_EMAIL_ID, ";
                SQL = SQL + "   MASTER_BRANCH_WEB_SITE, ";
                SQL = SQL + "   MASTER_BRANCH_VAT_REG_NUMBER ";
                SQL = SQL + "  FROM  ";
                SQL = SQL + "  RM_RECEIPT_APP_PRINT_VW  ";
                SQL = SQL + " where  RM_MRM_APPROVED_NO='" + sEntryNo + "'AND RM_MRD_APPROVED_FINYRID=" + mngrclass.FinYearID + "";

                dsPoStatus = clsSQLHelper.GetDataset(SQL);
                dsPoStatus.Tables[0].TableName = "RM_RECEIPT_APP_PRINT_VW";
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
    }


    //=========================================== do nor right t code belve jomy 

    #region "entity"

    #region "Master entity"
    public class ReceiptApprovalEntity
    {
        public string txtRecptAppNo
        {
            get;
            set;
        }
        public string DateType
        {
            get;
            set;
        }

        public string txtSupplierCode
        {
            get;
            set;
        }

        public string txtRVAmount
        {
            get;
            set;
        }

        public string ReceiptType
        {
            get;
            set;
        }
        
        public string TxtTransAmount
        {
            get;
            set;
        }

        public string txtRVAmountNew
        {
            get;
            set;
        }

        public string TxtTransAmountNew
        {
            get;
            set;
        }

        public string TxtTollRVAmount
        {
            get;
            set;
        }

        public string TxtTollRvAmountNew
        {
            get;
            set;
        }

        public string txtGrandTotalNew
        {
            get;
            set;
        }

        public string txtApprovedBy
        {
            get;
            set;
        }

        public string txtVerifiedBy
        {
            get;
            set;
        }

        public string txtRemarks
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

        public DateTime dtpFromDate
        {
            get;
            set;
        }

        public DateTime dtpToDate
        {
            get;
            set;
        }

        public string Branch { get; set; }
    }

    #endregion

    #region "Grid  entity"

    public class ReceiptApprovalReceiptDet
    {
        public object oReceiptNoReceiptDet
        {
            get;
            set;
        }

        public int iReceiptFinYrReceiptDet
        {
            get;
            set;
        }

        public double dApprovedQtyReceiptDet
        {
            get;
            set;
        }

        public double dSupplyQtyReceiptDet
        {
            get;
            set;
        }

        public double dRejectedQtyReceiptDet
        {
            get;
            set;
        }

        public double dNewSuppUnitPriceReceiptDet
        {
            get;
            set;
        }

        public double dNewSuppAmountReceiptDet
        {
            get;
            set;
        }

        public double dNewTransUnitPriceReceiptDet
        {
            get;
            set;
        }

        public double dNewTransAmountReceiptDet
        {
            get;
            set;
        }

        public string sRctTollUOMCode
        {
            get;
            set;
        }
        public double dRctTollRateNew
        {
            get;
            set;
        }
        public double dRctTollAmountNew
        {
            get;
            set;
        }

        public string doReceipt_NewPo_No
        {
            get;
            set;
        }
        public string doReceipt_Transp_NewPo_No
        {
            get;
            set;
        }
        public double dNewTotalAmountReceiptDet
        {
            get;
            set;
        }

        public double dRctUniquenoReceiptDet
        {
            get;
            set;
        }
        public DateTime dRctReceiptRateDate
        {
            get;
            set;
        }

        public object oRctAppNoReceiptDet
        {
            get;
            set;
        }

        public int iRctAppFinYrReceiptDet
        {
            get;
            set;
        }

    }

    public class ReceiptApprovalOtherItemsReceiptDet
    {
        public object oOtherItemsReceiptNoReceiptDet
        {
            get;
            set;
        }

        public int iOtherItemsReceiptFinYrReceiptDet
        {
            get;
            set;
        }

        public double dOtherItemsApprovedQtyReceiptDet
        {
            get;
            set;
        }

        public double dOtherItemsSupplyQtyReceiptDet
        {
            get;
            set;
        }

        public double dOtherItemsRejectedQtyReceiptDet
        {
            get;
            set;
        }

        public double dOtherItemsNewSuppUnitPriceReceiptDet
        {
            get;
            set;
        }

        public double dOtherItemsNewSuppAmountReceiptDet
        {
            get;
            set;
        }

        public double dOtherItemsNewTransUnitPriceReceiptDet
        {
            get;
            set;
        }

        public double dOtherItemsNewTransAmountReceiptDet
        {
            get;
            set;
        }

        public string sOtherItemsRctTollUOMCode
        {
            get;
            set;
        }
        public double dOtherItemsRctTollRateNew
        {
            get;
            set;
        }
        public double dOtherItemsRctTollAmountNew
        {
            get;
            set;
        }
        public double dOtherItemsNewTotalAmountReceiptDet
        {
            get;
            set;
        }

        public string doReceipt_NewPo_No
        {
            get;
            set;
        }

        public double dOtherItemsRctUniquenoReceiptDet
        {
            get;
            set;
        }
        public string Item_ReceiptType
        {
            get;
            set;
        }
        public DateTime dOtherItemsRctReceiptRateDate
        {
            get;
            set;
        }

        public object oOtherItemsRctAppNoReceiptDet
        {
            get;
            set;
        }

        public int iOtherItemsRctAppFinYrReceiptDet
        {
            get;
            set;
        }

    }


    #endregion
    #endregion
}
