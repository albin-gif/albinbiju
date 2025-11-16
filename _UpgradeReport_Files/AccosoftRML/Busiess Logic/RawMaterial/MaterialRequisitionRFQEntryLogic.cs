using System;
using System.Data;
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
//using FarPoint.Web.Spread;
using AccosoftUtilities;
using AccosoftLogWriter;
using AccosoftNineteenCDL;
using AccosoftLogWriter;
using System.Collections.Generic;
using System.Globalization;

/// <summary>
/// CreatedBy   : Jerin Johnson
/// CreatedDate : 30/06/2021 05:51 PM
/// </summary>

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class MaterialRequisitionRFQEntryLogic
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;


        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public MaterialRequisitionRFQEntryLogic()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        #region "Fill"

        public DataTable FillRFQPendingView(string fromdate, string todate, string sFilterType, object mngrclassobj)
        {
            DataTable dt = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SQL = " SELECT DISTINCT RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_NO DOC_NO,TO_CHAR(RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_DATE,'DD-MON-YYYY') DOC_DATE,    ";
                SQL = SQL + "           RM_PR_PRNO,RM_PUR_RFQ_MASTER.AD_FIN_FINYRID, RM_PRFQ_REMARKS, RM_PRFQ_VERIFIEDBY,RM_PRFQ_APPROVEDBY    ";
                SQL = SQL + " FROM RM_PUR_RFQ_MASTER, RM_PUR_RFQ_DETAILS   ";
                SQL = SQL + "   WHERE RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_NO = RM_PUR_RFQ_DETAILS.RM_PRFQ_DOC_NO  ";
                SQL = SQL + "   AND  RM_PUR_RFQ_MASTER.AD_FIN_FINYRID = RM_PUR_RFQ_DETAILS.AD_FIN_FINYRID  ";

                SQL = SQL + "   AND to_date(to_char(RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";
                if (mngrclass.UserName != "ADMIN")
                {
                    SQL = SQL + " and RM_PUR_RFQ_MASTER.AD_BR_CODE in (SELECT AD_BR_CODE FROM AD_USER_GRANTED_BRANCH WHERE  AD_UM_USERID = '" + mngrclass.UserName + "') ";
                }


                if (sFilterType == "NOT VERIFIED")
                {
                    SQL = SQL + " AND RM_PUR_RFQ_MASTER.RM_PRFQ_APPROVED ='N'";
                    SQL = SQL + " AND RM_PUR_RFQ_MASTER.RM_PRFQ_VERIFIED ='N'";
                }
                else if (sFilterType == "VERIFIED")
                {
                    SQL = SQL + " AND RM_PUR_RFQ_MASTER.RM_PRFQ_APPROVED ='N'";
                    SQL = SQL + " AND RM_PUR_RFQ_MASTER.RM_PRFQ_VERIFIED ='Y'";
                }
                else if (sFilterType == "NOT APPROVED")
                {
                    SQL = SQL + " AND RM_PUR_RFQ_MASTER.RM_PRFQ_APPROVED ='N'";
                    SQL = SQL + " AND RM_PUR_RFQ_MASTER.RM_PRFQ_VERIFIED ='Y'";
                }
                else if (sFilterType == "APPROVED")
                {
                    SQL = SQL + " AND RM_PUR_RFQ_MASTER.RM_PRFQ_APPROVED ='Y'";
                    SQL = SQL + " AND RM_PUR_RFQ_MASTER.RM_PRFQ_VERIFIED ='Y'";
                }
                else
                {
                    //all option 
                }
                SQL = SQL + "   ORDER BY RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_NO DESC ";

                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dt;

        }

        public DataTable FillSupplier(string SuppCode, string Division)
        {
            DataTable dtData = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_VENDOR_MASTER.RM_VM_VENDOR_CODE CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME NAME,  ";
                SQL = SQL + "   '' PartNo  ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC,  ";
                SQL = SQL + " PAY_TERMS_ID  RM_VM_CREDIT_TERMS, SALES_PAY_TYPE_DESC ";
                SQL = SQL + " FROM RM_VENDOR_MASTER, ";// SL_PAY_TYPE_MASTER,  ";
                SQL = SQL + "(  select ";
                SQL = SQL + "    RM_VM_VENDOR_CODE ,  AD_BR_CODE , ";
                SQL = SQL + "    SALES_PAY_TYPE_CODE ,  SALES_PAY_TYPE_DESC , ";
                SQL = SQL + "    RM_VM_LABLE_VALUE PAY_TERMS_ID , ";
                SQL = SQL + "    SALES_PAY_NO_DAYS   rm_vm_credit_period ,RM_VM_LABLE_ID ";
                SQL = SQL + "    from rm_vendor_master_BRANCH_DATA   ,  ";
                SQL = SQL + "    SL_PAY_TYPE_MASTER  ";
                SQL = SQL + "    where RM_VM_LABLE_ID ='PAYMENT_TERMS'  ";
                SQL = SQL + "    and AD_BR_CODE='" + Division + "' ";
                SQL = SQL + "    AND  rm_vendor_master_BRANCH_DATA.RM_VM_LABLE_VALUE ";
                SQL = SQL + "   = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE(+) )RMVENDMSTBRANCHPAYTERMSDATA, ";

                SQL = SQL + "(  select ";
                SQL = SQL + "    RM_VM_VENDOR_CODE ,  AD_BR_CODE , ";
                SQL = SQL + "    RM_VM_LABLE_VALUE VENDOR_STATUS   ,RM_VM_LABLE_ID ";
                SQL = SQL + "    from rm_vendor_master_BRANCH_DATA     ";
                SQL = SQL + "    where RM_VM_LABLE_ID ='VENDORBR_STAUS_AI'  ";
                SQL = SQL + "    and AD_BR_CODE='" + Division + "'   )RMVENDMSTBRANCHACTIVEDATA ";


                SQL = SQL + " WHERE ";// RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE (+) ";
                //  SQL = SQL + " AND RM_VM_VENDOR_STATUS='A'   ";
                SQL = SQL + "   RM_VENDOR_MASTER.RM_VM_VENDOR_CODE =";
                SQL = SQL + " RMVENDMSTBRANCHPAYTERMSDATA.RM_VM_VENDOR_CODE";
                SQL = SQL + " AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE =";
                SQL = SQL + " RMVENDMSTBRANCHACTIVEDATA.RM_VM_VENDOR_CODE";
                SQL = SQL + " AND RMVENDMSTBRANCHACTIVEDATA.VENDOR_STATUS='A' ";

                if (!string.IsNullOrEmpty(SuppCode))
                {
                    SQL = SQL + " AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE NOT IN ('" + SuppCode.Replace(",", "','") + "') ";
                }
                SQL = SQL + " ORDER BY RM_VENDOR_MASTER.RM_VM_VENDOR_NAME ";


                dtData = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dtData;
        }

        public DataTable FillRFQLPOView(string RFQNo, int EntryFinYearID, string UserName)
        {
            DataTable dt = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "    SELECT RM_PO_MASTER.RM_PO_PONO,  ";
                SQL = SQL + "         RM_PO_MASTER.RM_VM_VENDOR_CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME  ";
                SQL = SQL + "    FROM RM_PO_MASTER, RM_VENDOR_MASTER  ";
                SQL = SQL + "   WHERE     RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                SQL = SQL + "         AND RM_PO_MASTER.RM_PO_APPROVED = 'N'  ";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + " and RM_PO_MASTER.AD_BR_CODE in (SELECT AD_BR_CODE FROM AD_USER_GRANTED_BRANCH WHERE  AD_UM_USERID = '" + UserName + "') ";
                }

                SQL = SQL + "         AND RM_PO_MASTER.RM_VM_VENDOR_CODE IN  ";
                SQL = SQL + "                 (SELECT RM_VM_VENDOR_CODE  ";
                SQL = SQL + "                    FROM RM_PUR_RFQ_DETAILS_SUPPLIER  ";
                SQL = SQL + "                   WHERE RM_PRFQ_DOC_NO = '" + RFQNo + "' AND AD_FIN_FINYRID = " + EntryFinYearID + " )  ";
                SQL = SQL + " ORDER BY RM_PO_MASTER.RM_PO_PONO DESC  ";



                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dt;

        }

        #endregion

        #region "Featch"

        public DataSet FetchRFQPendingView(string RFQNo, int EntryFinYearID)
        {
            DataTable dtMaster = new DataTable();
            DataTable dtDetail = new DataTable();
            DataTable dtSuppDetail = new DataTable();
            DataTable dtPODT = new DataTable();
            DataSet dsReturn = new DataSet();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_PRFQ_DOC_NO, RM_PRFQ_DOC_DATE, AD_FIN_FINYRID,    ";
                SQL = SQL + "   AD_BR_CODE, AD_CM_COMPANY_CODE, RM_PRFQ_REMARKS,    ";
                SQL = SQL + "   RM_PRFQ_CREATEDBY, RM_PRFQ_CREATEDDATE_TIME, RM_PRFQ_VERIFIED,    ";
                SQL = SQL + "   RM_PRFQ_VERIFIEDBY, RM_PRFQ_VERIFIEDDATE_TIME, RM_PRFQ_APPROVED,    ";
                SQL = SQL + "   RM_PRFQ_APPROVEDBY, RM_PRFQ_APPROVEDDATE_TIME   ";
                SQL = SQL + " FROM RM_PUR_RFQ_MASTER   ";
                SQL = SQL + " WHERE RM_PRFQ_DOC_NO = '" + RFQNo + "' ";
                SQL = SQL + " AND AD_FIN_FINYRID = " + EntryFinYearID + "  ";
                SQL = SQL + " ORDER BY RM_PRFQ_DOC_NO ASC  ";

                dtMaster = clsSQLHelper.GetDataTableByCommand(SQL);
                dsReturn.Tables.Add(dtMaster);

                SQL = "SELECT RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_NO, RM_PUR_RFQ_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.RM_PR_PRNO, RM_PUR_RFQ_DETAILS.AD_PR_FIN_FINYRID, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.RM_PRD_SLNO, RM_PUR_RFQ_DETAILS.RM_PRFQD_SLNO, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.RM_RMM_RM_CODE, RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.RM_PRFQ_QTY, ";
                SQL = SQL + "         RM_PR_DETAILS.RM_PRD_DIR_USE_QTY, ";
                SQL = SQL + "         RM_PR_DETAILS.RM_PRD_QTY, ";
                SQL = SQL + "           NVL (RM_PUR_RFQ_DETAILS.RM_PRFQ_QTY, 0) ";
                SQL = SQL + "         -   NVL (RM_PR_DETAILS.RM_PRD_POD_QTY, 0)   REQ_QTY, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.RM_PO_REQUEST_TYPE_CODE, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.RM_PR_STOCK_TYPE_CODE, RM_PO_STOCK_TYPE_NAME, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.GL_COSM_ACCOUNT_CODE, GL_COSTING_MASTER_VW.NAME  COSTINGNAME, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.AD_BR_CODE, AD_BRANCH_MASTER.AD_BR_NAME, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.SALES_STN_STATION_CODE, SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.HR_DEPT_DEPT_CODE, HR_DEPT_MASTER.HR_DEPT_DEPT_DESC, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.RM_UM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.PC_BUD_BUDGET_ITEM_CODE,RM_PUR_RFQ_DETAILS.RM_POD_ITEM_STOCK_TYPE, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.RM_SM_SOURCE_CODE,RM_SM_SOURCE_DESC ,";
                SQL = SQL + "         RM_POD_UNIT_PRICE_LAST_PO,RM_POD_PO_DATE_LAST_PO,RM_POD_VENDOR_NAME_LAST_PO    ";
                SQL = SQL + "    FROM RM_PUR_RFQ_MASTER, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS, ";
                SQL = SQL + "         RM_PR_DETAILS, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         HR_DEPT_MASTER, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "         AD_BRANCH_MASTER, ";
                SQL = SQL + "         GL_COSTING_MASTER_VW, ";
                SQL = SQL + "         RM_STOCK_TYPE_VW, ";
                SQL = SQL + "         RM_UOM_MASTER, ";
                SQL = SQL + "         RM_SOURCE_MASTER ";
                SQL = SQL + "   WHERE     RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_NO = RM_PUR_RFQ_DETAILS.RM_PRFQ_DOC_NO ";
                SQL = SQL + "         AND RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_NO = RM_PR_DETAILS.RM_PUR_RFQ_NO ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS.RM_PR_PRNO = RM_PR_DETAILS.RM_PR_PRNO ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS.RM_PRD_SLNO = RM_PR_DETAILS.RM_PRD_SL_NO ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE(+) ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE(+) ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE(+) ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE(+) ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS.GL_COSM_ACCOUNT_CODE =  GL_COSTING_MASTER_VW.CODE(+) ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS.RM_PR_STOCK_TYPE_CODE = RM_STOCK_TYPE_VW.RM_PO_STOCK_TYPE_CODE(+) ";
                SQL = SQL + "         AND RM_PR_DETAILS.RM_SM_SOURCE_CODE =RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE(+) ";
                SQL = SQL + "         AND RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_NO = '" + RFQNo + "' ";
                SQL = SQL + "         AND RM_PUR_RFQ_MASTER.AD_FIN_FINYRID =" + EntryFinYearID + "  ";
                SQL = SQL + "ORDER BY RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_NO, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS.RM_PRFQD_SLNO ASC ";

                dtDetail = clsSQLHelper.GetDataTableByCommand(SQL);
                dsReturn.Tables.Add(dtDetail);

                SQL = " SELECT RM_PRFQ_DOC_NO, AD_FIN_FINYRID,   ";
                SQL = SQL + "   RM_PUR_RFQ_DETAILS_SUPPLIER.RM_VM_VENDOR_CODE, RM_VM_VENDOR_NAME,  ";
                SQL = SQL + "   RM_PRQ_SUPP_SLNO, RM_PRQ_SUPP_QUOTE_NO, RM_PRQ_SUPP_QUOTE_REF_NO ,   ";
                SQL = SQL + "   TO_CHAR (RM_PRQ_SUPP_QUOTE_DATE, 'DD-MON-YYYY') QUOTE_DATE ,   ";
                SQL = SQL + "   RM_PUR_RFQ_DETAILS_SUPPLIER.SALES_PAY_TYPE_CODE,SALES_PAY_TYPE_DESC,  ";
                SQL = SQL + "   RM_PUR_RFQ_DETAILS_SUPPLIER.RM_PRQ_SUPP_REMARKS  ";
                SQL = SQL + " FROM RM_PUR_RFQ_DETAILS_SUPPLIER,RM_VENDOR_MASTER,  ";
                SQL = SQL + "      SL_PAY_TYPE_MASTER  ";
                SQL = SQL + "   WHERE RM_PUR_RFQ_DETAILS_SUPPLIER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                SQL = SQL + "   AND RM_PUR_RFQ_DETAILS_SUPPLIER.SALES_PAY_TYPE_CODE = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE     ";
                SQL = SQL + "   AND RM_PRFQ_DOC_NO =  '" + RFQNo + "'  ";
                SQL = SQL + "   AND AD_FIN_FINYRID =  " + EntryFinYearID + "   ";
                SQL = SQL + "   ORDER BY RM_PRFQ_DOC_NO,RM_PRQ_SUPP_SLNO ASC  ";

                dtSuppDetail = clsSQLHelper.GetDataTableByCommand(SQL);
                dsReturn.Tables.Add(dtSuppDetail);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dsReturn;

        }

        public DataTable FeatchDetails(string RFQNo, int EntryFinYearID)
        {
            DataTable dtDetail = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = " SELECT RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_NO, RM_PUR_RFQ_MASTER.AD_FIN_FINYRID,    ";
                SQL = SQL + "   RM_PUR_RFQ_DETAILS_SUPPLIER.RM_VM_VENDOR_CODE, RM_VM_VENDOR_NAME  ";
                SQL = SQL + "   FROM RM_PUR_RFQ_MASTER,RM_PUR_RFQ_DETAILS_SUPPLIER,RM_VENDOR_MASTER   ";
                SQL = SQL + " WHERE  RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_NO = RM_PUR_RFQ_DETAILS_SUPPLIER.RM_PRFQ_DOC_NO  ";
                SQL = SQL + "   AND RM_PUR_RFQ_DETAILS_SUPPLIER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE (+)  ";
                SQL = SQL + "   AND RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_NO = '" + RFQNo + "' ";
                SQL = SQL + "   AND RM_PUR_RFQ_MASTER.AD_FIN_FINYRID =  " + EntryFinYearID + "  ";
                SQL = SQL + " ORDER BY RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_NO,RM_PRQ_SUPP_SLNO ASC  ";

                dtDetail = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dtDetail;
        }

        public DataTable FetchSuppQuoteDetails(string RFQNo, int EntryFinYearID)
        {
            DataTable dtDetailQuote = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = "  SELECT RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_PRFQ_DOC_NO,RM_PUR_RFQ_DETAILS_SUPP_QUOTE.AD_FIN_FINYRID,  ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_PR_PRNO,RM_PUR_RFQ_DETAILS_SUPP_QUOTE.AD_PR_FIN_FINYRID,  ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_PRD_SLNO,RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_IM_ITEM_CODE,  ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_PRFQD_SLNO,RM_PRQ_SUPP_SLNO,  ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_VM_VENDOR_CODE,  ";
                SQL = SQL + "         RM_PRQ_SUPP_QUOTE_ORIG_RATE,RM_PRQ_SUPP_QUOTE_DISC_RATE,  ";
                SQL = SQL + "         RM_PRQ_SUPP_QUOTE_UNIT_RATE,nvl(SUPP_ORDER.RM_POD_QTY,0) RM_POD_QTY , ";
                SQL = SQL + "         RM_PRQ_SUPP_QUOTE_APP_QTY APP_QTY,RM_PRQ_SUPP_QUOTE_APP_YN QUOTE_APP_YN ,NVL(PO_GEN_YN,'N')  PO_GEN_YN  ";
                SQL = SQL + "    FROM RM_PUR_RFQ_DETAILS_SUPP_QUOTE,RM_PUR_RFQ_DETAILS_SUPPLIER,  ";
                 
                SQL = SQL + "         (  SELECT RM_PRFQ_DOC_NO,AD_FIN_FINYRID, ";
                SQL = SQL + "                   RM_PR_PRNO,RM_PRD_SLNO,  ";
                SQL = SQL + "                  RM_PRFQD_SLNO,RM_VM_VENDOR_CODE,  ";
                SQL = SQL + "                  SUM (RM_POD_QTY) RM_POD_QTY, CASE WHEN RM_PO_PONO IS NOT NULL THEN 'Y' ELSE 'N' END PO_GEN_YN  ";
                SQL = SQL + "              FROM RM_PUR_RFQ_DETAILS_SUPP_ORDER ";
                SQL = SQL + "             WHERE RM_PRFQ_DOC_NO = '" + RFQNo + "' AND AD_FIN_FINYRID = " + EntryFinYearID + " AND RM_POD_QTY >0  ";
                SQL = SQL + "          GROUP BY RM_PRFQ_DOC_NO,AD_FIN_FINYRID,RM_PR_PRNO, ";
                SQL = SQL + "                  RM_PRD_SLNO,RM_PRFQD_SLNO,RM_VM_VENDOR_CODE,CASE WHEN RM_PO_PONO IS NOT NULL THEN 'Y' ELSE 'N' END  ";
                SQL = SQL + "          ORDER BY RM_PRFQ_DOC_NO, RM_PRFQD_SLNO, RM_PRD_SLNO) SUPP_ORDER ";
                 

                SQL = SQL + "   WHERE     RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_PRFQ_DOC_NO = RM_PUR_RFQ_DETAILS_SUPPLIER.RM_PRFQ_DOC_NO  ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS_SUPP_QUOTE.AD_FIN_FINYRID = RM_PUR_RFQ_DETAILS_SUPPLIER.AD_FIN_FINYRID  ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_VM_VENDOR_CODE = RM_PUR_RFQ_DETAILS_SUPPLIER.RM_VM_VENDOR_CODE  ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_PRFQ_DOC_NO = SUPP_ORDER.RM_PRFQ_DOC_NO (+)  ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS_SUPP_QUOTE.AD_FIN_FINYRID = SUPP_ORDER.AD_FIN_FINYRID (+)  ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_PR_PRNO = SUPP_ORDER.RM_PR_PRNO (+)  ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_PRFQD_SLNO = SUPP_ORDER.RM_PRFQD_SLNO (+)  ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_PRD_SLNO = SUPP_ORDER.RM_PRD_SLNO (+)  ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_VM_VENDOR_CODE = SUPP_ORDER.RM_VM_VENDOR_CODE (+)  ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_PRFQ_DOC_NO = '" + RFQNo + "' ";
                SQL = SQL + "         AND RM_PUR_RFQ_DETAILS_SUPP_QUOTE.AD_FIN_FINYRID = " + EntryFinYearID + " ";
                SQL = SQL + " ORDER BY RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_PRFQ_DOC_NO,RM_PUR_RFQ_DETAILS_SUPP_QUOTE.RM_PRFQD_SLNO, ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS_SUPPLIER.RM_PRQ_SUPP_SLNO ASC ";

                dtDetailQuote = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dtDetailQuote;
        }

        public double GetNextSeqNo()
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_RFQ_DETAILS_PO_POST_SEQ.NEXTVAL next_seq_no  FROM DUAL";

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
            return double.Parse(dtType.Rows[0]["next_seq_no"].ToString());

        }
        //----------------- item history rate look up //---------

        public DataTable RMItemPOHistoryRateList(string sRMCode)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();



                SQL = " SELECT  ";
                SQL = SQL + " PONO,    PODATE,    VENDORCODE,    VENDORNAME,    RMCODE,    RMDES,    PO_QTY, ";
                SQL = SQL + " UNITPRICE,    DISCPERCENTAGE,    DISCAMOUNT,    NEWPRICE,    TOTALAMOUNT,   ";
                SQL = SQL + " CREATEDBY,    VERIFIEDBY,    APPROVEDBY ";
                SQL = SQL + " FROM RM_PO_ITEM_LIST_VW where RMCODE ='" + sRMCode + "'";
                SQL = SQL + " order by  PODATE desc ";


                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }


        //------------------------ MASTER DATA LIST LOGIC ------------------------------------
        public DataTable GetAlertData(string RFQNo, int EntryFinYearId, double SeqNoGenerated)
        {
            DataTable dsData = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT DISTINCT RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_PO_PONO,  ";
                SQL = SQL + "              RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_VM_VENDOR_CODE,  ";
                SQL = SQL + "              RM_VM_VENDOR_NAME,  ";
                SQL = SQL + "              RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_PR_STOCK_TYPE_CODE  ";
                SQL = SQL + " FROM RM_PUR_RFQ_DETAILS_PO_POST,  ";
                SQL = SQL + "     RM_PUR_RFQ_DETAILS_SUPP_ORDER,  ";
                SQL = SQL + "     RM_VENDOR_MASTER  ";
                SQL = SQL + " WHERE     RM_PUR_RFQ_DETAILS_PO_POST.RM_PRFQ_COMBINED_DOC_NO = RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_PRFQ_COMBINED_DOC_NO  ";
                SQL = SQL + "     AND RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_VM_VENDOR_CODE =  RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                SQL = SQL + "     AND RM_PUR_RFQ_DETAILS_PO_POST.RM_PRFQ_DOC_NO = '" + RFQNo + "' ";
                SQL = SQL + "     AND RM_PUR_RFQ_DETAILS_PO_POST.AD_FIN_FINYRID = " + EntryFinYearId + "";
                SQL = SQL + "     AND RM_PUR_RFQ_DETAILS_PO_POST.RM_PRFQ_COMBINED_DOC_NO = " + SeqNoGenerated + " ";
                SQL = SQL + " ORDER BY RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_PO_PONO ASC ";


                dsData = clsSQLHelper.GetDataTableByCommand(SQL);

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
            return dsData;

        }

        public DataSet FeatchPOHistory(string RFQNo, int EntryFinYearID)
        {
            DataSet dsData = new DataSet();
            DataTable dtSuppConut = new DataTable();
            DataTable dtSuppData = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_VM_VENDOR_CODE,RM_VM_VENDOR_NAME, MAX(CNT) SUPPCOLCNT  ";
                SQL = SQL + "  FROM( SELECT  RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_VM_VENDOR_CODE,RM_VM_VENDOR_NAME,   ";
                SQL = SQL + "        RM_PRFQD_SLNO,COUNT (RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_VM_VENDOR_CODE) CNT  ";
                SQL = SQL + "      FROM RM_PUR_RFQ_DETAILS_SUPP_ORDER,RM_VENDOR_MASTER  ";
                SQL = SQL + "      WHERE RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE        ";
                SQL = SQL + "      AND RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_PRFQ_DOC_NO = '" + RFQNo + "'  ";
                SQL = SQL + "      AND RM_PUR_RFQ_DETAILS_SUPP_ORDER.AD_FIN_FINYRID = " + EntryFinYearID + "  ";
                SQL = SQL + "      GROUP BY RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_VM_VENDOR_CODE,RM_VM_VENDOR_NAME, RM_PRFQD_SLNO)  ";
                SQL = SQL + "  GROUP BY RM_VM_VENDOR_CODE,RM_VM_VENDOR_NAME  ";

                dtSuppConut = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtSuppConut);
                SQL = " SELECT RM_PR_PRNO,AD_PR_FIN_FINYRID,  ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_RMM_RM_CODE,RM_RMM_RM_DESCRIPTION,  ";
                SQL = SQL + "         RM_PO_PONO,RM_PRFQ_COMBINED_DOC_NO,RM_PRD_SLNO,RM_PRFQD_SLNO,  ";
                SQL = SQL + "         RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_VM_VENDOR_CODE,RM_VM_VENDOR_NAME  ";
                SQL = SQL + "  FROM RM_PUR_RFQ_DETAILS_SUPP_ORDER,RM_VENDOR_MASTER,RM_RAWMATERIAL_MASTER  ";
                SQL = SQL + "  WHERE RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                SQL = SQL + "  AND RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE(+)  ";
                SQL = SQL + "    AND RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_PRFQ_DOC_NO = '" + RFQNo + "'  ";
                SQL = SQL + "  AND RM_PUR_RFQ_DETAILS_SUPP_ORDER.AD_FIN_FINYRID = " + EntryFinYearID + "  ";
                SQL = SQL + "  ORDER BY RM_PRFQD_SLNO ASC  ";


                dtSuppData = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtSuppData);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dsData;
        }

        #endregion

        #region "Check validation"

        public String CheckPO(string LPO)
        {
            string sRetLPO = "";

            DataTable dtCheckPO = new DataTable();

            try
            {

                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_PO_PONO FROM RM_PO_MASTER WHERE RM_PO_PONO = '" + LPO + "' ";

                dtCheckPO = clsSQLHelper.GetDataTableByCommand(SQL);

                if (dtCheckPO.Rows.Count > 0)
                {
                    sRetLPO = dtCheckPO.Rows[0]["RM_PO_PONO"].ToString();
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

            return sRetLPO;
        }

        #endregion

        #region "DML"

        public string UpdateSQL(MaterialRequisitionRFQEntryWSEntity oEntity, List<MaterialRequisitionRFQEntryWSGridEntity> objWSGridEntity,
            List<MaterialRequisitionRFQEntryWSGridSuppEntity> objWSGridSuppEntity, List<MaterialRequisitionRFQEntryWSGridSuppRateEntity> objWSGridSuppRateEntity, object mngrclassobj)
        {

            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " UPDATE RM_PUR_RFQ_MASTER SET RM_PRFQ_REMARKS  = '" + oEntity.Remarks + "', ";
                SQL = SQL + "       RM_PRFQ_VERIFIED          = '" + oEntity.Verified + "', ";
                SQL = SQL + "       RM_PRFQ_VERIFIEDBY        = '" + oEntity.VerifiedBy + "', ";
                SQL = SQL + "       RM_PRFQ_VERIFIEDDATE_TIME = TO_DATE('" + oEntity.VerifiedDateTime + "', 'DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + "       RM_PRFQ_APPROVED          = '" + oEntity.Approved + "', ";
                SQL = SQL + "       RM_PRFQ_APPROVEDBY        = '" + oEntity.ApprovedBy + "', ";
                SQL = SQL + "       RM_PRFQ_APPROVEDDATE_TIME = TO_DATE('" + oEntity.ApprovedDateTime + "', 'DD-MM-YYYY HH:MI:SS AM') ";
                SQL = SQL + " WHERE  RM_PRFQ_DOC_NO            = '" + oEntity.RFQNo + "' ";
                SQL = SQL + " AND    AD_FIN_FINYRID            = '" + oEntity.RFQFinYrId + "' ";

                sSQLArray.Add(SQL);


                SQL = " DELETE FROM RM_PUR_RFQ_DETAILS_SUPPLIER    ";
                SQL = SQL + " WHERE RM_PRFQ_DOC_NO =  '" + oEntity.RFQNo + "' AND AD_FIN_FINYRID = " + oEntity.RFQFinYrId + "";

                sSQLArray.Add(SQL);



                sRetun = string.Empty;
                sRetun = InsertSupplierDetailsSQL(oEntity.RFQNo, oEntity.RFQFinYrId, objWSGridSuppEntity, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                sRetun = UpdateRateSQL(oEntity.RFQNo, oEntity.RFQFinYrId, objWSGridSuppRateEntity, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, oEntity.RFQFinYrId, mngrclass.CompanyCode, DateTime.Now, "RMPMRQRFQ", oEntity.RFQNo, false, Environment.MachineName, "U", sSQLArray);

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

        private string InsertDetailsSQL(string RFQNo, int EntryFinYearId, List<MaterialRequisitionRFQEntryWSGridEntity> objWSGridEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                int iRow = 0;

                foreach (var Data in objWSGridEntity)
                {
                    ++iRow;


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

        private string InsertSupplierDetailsSQL(string RFQNo, int EntryFinYearId, List<MaterialRequisitionRFQEntryWSGridSuppEntity> objWSGridSuppEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                int iRow = 0;

                foreach (var Data in objWSGridSuppEntity)
                {
                    ++iRow;

                    SQL = " INSERT INTO RM_PUR_RFQ_DETAILS_SUPPLIER ( ";
                    SQL = SQL + "   RM_PRFQ_DOC_NO, AD_FIN_FINYRID, RM_VM_VENDOR_CODE,RM_PRQ_SUPP_SLNO, RM_PRQ_SUPP_QUOTE_NO,  ";
                    SQL = SQL + "   RM_PRQ_SUPP_QUOTE_REF_NO, RM_PRQ_SUPP_QUOTE_DATE, RM_PRQ_SUPP_REMARKS,SALES_PAY_TYPE_CODE )  ";
                    SQL = SQL + " VALUES ('" + RFQNo + "', " + EntryFinYearId + ",'" + Data.SuppCode + "'," + iRow + ",  '" + Data.QuotationNo + "', ";
                    SQL = SQL + " '" + Data.QuotationReffNo + "', '" + Data.QuotationDate + "' , '" + Data.Remarks + "', ";
                    SQL = SQL + " '" + Data.Payterms + "' ) ";

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

        public string UpdateRateSQL(string RFQNo, int EntryFinYearId, List<MaterialRequisitionRFQEntryWSGridSuppRateEntity> objWSGridSuppRateEntity, object mngrclassobj)
        {

            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                DataTable dtData = new DataTable();

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                int iRow = 0;
                // jomyadded thsi statmetn since its duplicaed, jerring missed or i d ont know this is correct or not , test cases missed  , unfortinuatly 
                //this case is occuring duing the repated upate . 

                SQL = " DELETE FROM RM_PUR_RFQ_DETAILS_SUPP_QUOTE    ";
                SQL = SQL + " WHERE RM_PRFQ_DOC_NO =  '" + RFQNo + "' AND AD_FIN_FINYRID = " + EntryFinYearId + "";
                sSQLArray.Add(SQL);

                foreach (var Data in objWSGridSuppRateEntity)
                {
                    ++iRow;

                    SQL = " INSERT INTO RM_PUR_RFQ_DETAILS_SUPP_QUOTE ( ";
                    SQL = SQL + "   RM_PRFQ_DOC_NO, AD_FIN_FINYRID, RM_PR_PRNO,  ";
                    SQL = SQL + "   AD_PR_FIN_FINYRID, RM_PRD_SLNO, RM_IM_ITEM_CODE,  ";
                    SQL = SQL + "   RM_PRFQD_SLNO, RM_VM_VENDOR_CODE, RM_PRQ_SUPP_QUOTE_ORIG_RATE,  ";
                    SQL = SQL + "   RM_PRQ_SUPP_QUOTE_DISC_RATE, RM_PRQ_SUPP_QUOTE_UNIT_RATE, ";
                    SQL = SQL + "   RM_PRQ_SUPP_QUOTE_APP_QTY, RM_PRQ_SUPP_QUOTE_APP_YN )  ";
                    SQL = SQL + " VALUES ('" + RFQNo + "'," + EntryFinYearId + ",'" + Data.PRNO + "', ";
                    SQL = SQL + "   '" + Data.PRFinYr + "'," + Data.PRSLNo + ",'" + Data.ItemCode + "', ";
                    SQL = SQL + "   '" + Data.SLNo + "','" + Data.SuppCode + "','" + Data.OrgRate + "', ";
                    SQL = SQL + "   '" + Data.DisRate + "'," + Data.UnitRate + " , ";
                    SQL = SQL + "   " + Data.POQty + " ,'" + Data.Approved + "' ) ";
                    //}

                    sSQLArray.Add(SQL);
                }

                //sRetun = string.Empty;

                //sRetun = oTrns.SetTrans(mngrclass.UserName, EntryFinYearId, mngrclass.CompanyCode, DateTime.Now, "RMPMRQRFQ", RFQNo, false, Environment.MachineName, "U", sSQLArray);

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

        public string GeneratePORateUpdatedCheckingSQL(string RFQNo, int EntryFinYearId, List<MaterialRequisitionRFQEntryWSGridSuppRateQtyEntity> objWSGridSuppRateQtyEntity, string LPONumber)
        {

            string sRetun = string.Empty;
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                DataTable dtData = new DataTable();
                int POSlNo = 0;

                sSQLArray.Clear();


                SQL = string.Empty;



                foreach (var Data in objWSGridSuppRateQtyEntity)
                {

                    SQL = "SELECT  count( RM_VM_VENDOR_CODE ) COUNT_RATE  FROM RM_PUR_RFQ_DETAILS_SUPP_QUOTE   " +
                    "WHERE  RM_PRFQ_DOC_NO  = '" + RFQNo + "'  AND AD_FIN_FINYRID =   " + EntryFinYearId + "" +
                    "     AND  RM_PR_PRNO = '" + Data.PRNO + "' AND AD_PR_FIN_FINYRID = " + Data.PRFinYr + "  AND RM_PRD_SLNO = '" + Data.PRSLNo + "'" +
                    "     AND RM_PRFQD_SLNO = '" + Data.SLNo + "'  AND  RM_VM_VENDOR_CODE  = '" + Data.SuppCode + "'";

                    dtType = clsSQLHelper.GetDataTableByCommand(SQL);
                    if (double.Parse(dtType.Rows[0]["COUNT_RATE"].ToString()) <= 0)
                    {
                        sRetun = "Rate defined however not yet used update rate option for not yet updated against this items: " + Data.ItemDesc + ",Vendor :" + Data.SuppCode + "";
                        return sRetun;
                    }
                    else
                    {
                        sRetun = "CONTINUE";
                    }

                    if (!string.IsNullOrEmpty(LPONumber))
                    {
                        SQL = "select RM_PO_STOCK_TYPE_CODE,AD_BR_CODE from RM_PO_MASTER where RM_PO_PONO='" + LPONumber + "' ";

                        dtType = clsSQLHelper.GetDataTableByCommand(SQL);
                        if (dtType.Rows[0]["RM_PO_STOCK_TYPE_CODE"].ToString() != Data.TypeCode)
                        {
                            sRetun = "PO Type is Not Matching for PO and PR,PRNO-" + Data.PRNO + " items: " + Data.ItemDesc + ",Po Type :" + dtType.Rows[0]["RM_PO_STOCK_TYPE_CODE"].ToString() + " and Pr type: " + Data.TypeCode + " . ";
                            return sRetun;
                        }
                        else
                        {
                            sRetun = "CONTINUE";
                        }

                        if (dtType.Rows[0]["AD_BR_CODE"].ToString() != Data.DivCode)
                        {
                            sRetun = "Po Division Not Matching. ";
                            return sRetun;
                        }
                        else
                        {
                            sRetun = "CONTINUE";
                        }

                    }


                    dtType = clsSQLHelper.GetDataTableByCommand(SQL);

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
            return sRetun;
        }



        public string GeneratePOSQL(double SeqNo, string RFQNo, int EntryFinYearId, string BranchCode, string sType, string sPrevPONO, string sManualPONO,
            List<MaterialRequisitionRFQEntryWSGridSuppRateQtyEntity> objWSGridSuppRateQtyEntity, List<MaterialRequisitionRFQEntryWSGridSuppRateEntity> objWSGridSuppRateEntity, object mngrclassobj, string dtpPODate)
        {

            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                DataTable dtData = new DataTable();
                int POSlNo = 0;

                sSQLArray.Clear();

                foreach (var Data in objWSGridSuppRateQtyEntity)
                {
                    POSlNo = POSlNo + 1;

                    SQL = " INSERT INTO RM_PUR_RFQ_DETAILS_SUPP_ORDER ( ";
                    SQL = SQL + "   RM_PRFQ_DOC_NO, AD_FIN_FINYRID, RM_PR_PRNO,  ";
                    SQL = SQL + "   AD_PR_FIN_FINYRID, RM_PRD_SLNO, RM_PRFQD_SLNO,  ";
                    SQL = SQL + "   AD_BR_CODE, SALES_STN_STATION_CODE, HR_DEPT_DEPT_CODE,  ";
                    SQL = SQL + "   GL_COSM_ACCOUNT_CODE, PC_BUD_BUDGET_ITEM_CODE, RM_PO_REQUEST_TYPE_CODE,  ";
                    SQL = SQL + "   RM_PR_STOCK_TYPE_CODE, FA_FAM_ASSET_CODE, RM_RMM_RM_CODE,  ";
                    SQL = SQL + "   RM_VM_VENDOR_CODE, RM_POD_ITEM_STOCK_TYPE,  ";
                    SQL = SQL + "   RM_POD_QTY, RM_PRQ_SUPP_QUOTE_ORIG_RATE, RM_PRQ_SUPP_QUOTE_DISC_RATE, RM_PRQ_SUPP_QUOTE_UNIT_RATE, ";
                    SQL = SQL + "   AD_CM_COMPANY_CODE, RM_PRFQ_COMBINED_DOC_NO,RM_UM_UOM_CODE, RM_PO_PONO,RM_SM_SOURCE_CODE)  ";
                    SQL = SQL + " VALUES ('" + RFQNo + "', " + EntryFinYearId + ", ";
                    SQL = SQL + "   '" + Data.PRNO + "'," + Data.PRFinYr + ",'" + Data.PRSLNo + "','" + Data.SLNo + "', ";
                    SQL = SQL + "   '" + Data.DivCode + "','" + Data.StnCode + "','" + Data.DeptCode + "','" + Data.CostCentreCode + "',  ";
                    SQL = SQL + "   '" + Data.BudgItemCode + "','" + Data.PriorityCode + "','" + Data.TypeCode + "','" + Data.AssetCode + "',  ";
                    SQL = SQL + "   '" + Data.ItemCode + "','" + Data.SuppCode + "','" + Data.StockItem + "',  ";
                    SQL = SQL + "   " + Data.AdditionalPOQty + ", " + Data.OrgRate + ",'" + Data.DisRate + "',  ";
                    SQL = SQL + "   " + Data.UnitRate + ",'" + mngrclass.CompanyCode + "',  ";
                    SQL = SQL + "   " + SeqNo + ", '" + Data.UOMCode + "', '" + sPrevPONO + "' , '" + Data.sSourceCode + "' ) ";

                    sSQLArray.Add(SQL);
                }

                SQL = " INSERT INTO RM_PUR_RFQ_DETAILS_PO_POST ( ";
                SQL = SQL + "   RM_PRFQ_DOC_NO, AD_FIN_FINYRID, RM_PRFQ_COMBINED_DOC_NO, RM_PRFQ_POST_DOC_DATE,AD_BR_CODE,  ";
                SQL = SQL + "   RM_PRFQ_COMBINED_POST_BY, RM_PRFQ_COMBINED_POST_BY_DT, RM_PO_GEN_TYPE,RM_PO_PONO_PREVIOUS_PO, RM_PO_PONO_MANUAL)  ";
                SQL = SQL + "  VALUES ('" + RFQNo + "', " + EntryFinYearId + ", ";
                SQL = SQL + "   '" + SeqNo + "',TO_DATE('" + Convert.ToDateTime(dtpPODate).ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + "   '" + BranchCode + "', '" + mngrclass.UserName + "',TO_DATE('" + Convert.ToDateTime(dtpPODate).ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),  ";
                SQL = SQL + "   '" + sType + "', '" + sPrevPONO + "', '" + sManualPONO + "' ) ";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = UpdateRateSQL(RFQNo, EntryFinYearId, objWSGridSuppRateEntity, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }
                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, EntryFinYearId, mngrclass.CompanyCode, DateTime.Now, "RMPMRQRFQ", RFQNo, false, Environment.MachineName, "U", sSQLArray);

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

        #region "Print"

        public DataSet FetchRFQPrintQuotationData(string RFQNo, int EntryFinYearID, string sSelectedItem)
        {
            DataSet dsData = new DataSet();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  ";
                SQL = SQL + " SELECT rm_PRFQ_DOC_NO WS_PRFQ_DOC_NO, AD_FIN_FINYRID,rm_PRFQ_DOC_DATE WS_PRFQ_DOC_DATE,   ";
                SQL = SQL + "   GL_COSM_ACCOUNT_CODE, COSTINGNAME,RM_RMM_RM_CODE WS_IM_ITEM_CODE,RM_RMM_RM_DESCRIPTION WS_IM_ITEM_DESC,  ";
                SQL = SQL + "  '' WS_IM_ITEM_DTL_DESCRIPTION  ,   ";
                SQL = SQL + "   SUM(rm_PRFQ_QTY) WS_PRFQ_QTY,rm_PRFQ_REMARKS WS_PRFQ_REMARKS,RM_PRFQ_CREATEDBY WS_PRFQ_CREATEDBY,  ";
                SQL = SQL + "   MASTER_BRANCH_CODE, MASTER_BRANCH_NAME,    ";
                SQL = SQL + "   MASTER_BRANCHDOC_PREFIX, MASTER_BRANCH_POBOX, MASTER_BRANCH_ADDRESS,    ";
                SQL = SQL + "   MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE, MASTER_BRANCH_FAX,    ";
                SQL = SQL + "   MASTER_BRANCH_SPONSER_NAME, MASTER_BRANCH_TRADE_LICENSE_NO, MASTER_BRANCH_EMAIL_ID,    ";
                SQL = SQL + "   MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER   ,RM_SM_SOURCE_CODE,RM_SM_SOURCE_DESC ";
                SQL = SQL + " FROM rm_PUR_RFQ_REQUEST_VW   ";
                SQL = SQL + "   WHERE rm_PRFQ_DOC_NO = '" + RFQNo + "'  ";
                SQL = SQL + "  AND AD_FIN_FINYRID = " + EntryFinYearID + "   ";

                if (!string.IsNullOrEmpty(sSelectedItem))
                {
                    SQL = SQL + "  AND RM_RMM_RM_CODE  IN (" + sSelectedItem + ")";
                }

                SQL = SQL + "  GROUP BY rm_PRFQ_DOC_NO, AD_FIN_FINYRID, RM_PRFQ_DOC_DATE,   ";
                SQL = SQL + "   GL_COSM_ACCOUNT_CODE, COSTINGNAME, RM_RMM_RM_CODE ,RM_RMM_RM_DESCRIPTION ,  ";
                SQL = SQL + "   RM_PRFQ_REMARKS,RM_PRFQ_CREATEDBY,  ";
                SQL = SQL + "   rm_PRFQ_CREATEDBY,MASTER_BRANCH_CODE, MASTER_BRANCH_NAME,    ";
                SQL = SQL + "   MASTER_BRANCHDOC_PREFIX, MASTER_BRANCH_POBOX, MASTER_BRANCH_ADDRESS,    ";
                SQL = SQL + "   MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE, MASTER_BRANCH_FAX,    ";
                SQL = SQL + "   MASTER_BRANCH_SPONSER_NAME, MASTER_BRANCH_TRADE_LICENSE_NO, MASTER_BRANCH_EMAIL_ID,    ";
                SQL = SQL + "   MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER   ,RM_SM_SOURCE_CODE,RM_SM_SOURCE_DESC  ";

                dsData = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dsData;
        }

        public DataSet FetchRFQPrintRequestData(string RFQNo, int EntryFinYearID, string sSelectedItem)
        {
            // DataSet dsData = new DataSet();
            DataSet dsPoStatus = new DataSet("WSPRPRINT");

            DataTable dtPOStatus = new DataTable();
            DataTable dtItemQty = new DataTable();
            DataTable dtLastPur = new DataTable();
            DataTable dtAvgConsumption = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = " SELECT RM_PRFQ_DOC_NO WS_PRFQ_DOC_NO, AD_FIN_FINYRID,RM_PRFQ_DOC_DATE WS_PRFQ_DOC_DATE,   ";
                SQL = SQL + "   AD_BR_CODE, AD_BR_NAME,RM_PR_PRNO WS_PR_PRNO,   ";
                SQL = SQL + "   AD_PR_FIN_FINYRID,RM_PO_REQUEST_TYPE_CODE WS_PO_REQUEST_TYPE_CODE,'' WS_PO_REQUEST_TYPE_NAME,   ";
                SQL = SQL + " RM_PR_STOCK_TYPE_CODE  WS_PR_STOCK_TYPE_CODE,'' WS_PO_STOCK_TYPE_NAME, FA_FAM_ASSET_CODE,   ";
                SQL = SQL + "   FA_FAM_ASSET_DESCRIPTION, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME,   ";
                SQL = SQL + "   HR_DEPT_DEPT_CODE, HR_DEPT_DEPT_DESC, RM_UM_UOM_CODE,   ";
                SQL = SQL + "   RM_UM_UOM_DESC, GL_COSM_ACCOUNT_CODE, COSTINGNAME,   ";
                SQL = SQL + "   PC_BUD_BUDGET_ITEM_CODE,RM_PRD_SLNO WS_PRD_SLNO,RM_RMM_RM_CODE WS_IM_ITEM_CODE,   ";
                SQL = SQL + "  RM_RMM_RM_DESCRIPTION WS_IM_ITEM_DESC,  '' WS_IM_ITEM_DTL_DESCRIPTION,RM_PRFQD_SLNO WS_PRFQD_SLNO,   ";
                SQL = SQL + " RM_PRFQ_QTY  WS_PRFQ_QTY, MASTER_BRANCH_CODE, MASTER_BRANCH_NAME,   ";
                SQL = SQL + "   MASTER_BRANCHDOC_PREFIX, MASTER_BRANCH_POBOX, MASTER_BRANCH_ADDRESS,   ";
                SQL = SQL + "   MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE, MASTER_BRANCH_FAX,   ";
                SQL = SQL + "   MASTER_BRANCH_SPONSER_NAME, MASTER_BRANCH_TRADE_LICENSE_NO, MASTER_BRANCH_EMAIL_ID,   ";
                SQL = SQL + "   MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER,  ";
                SQL = SQL + " RM_PR_PR_DATE WS_PR_REQN_DATE,  ";
                SQL = SQL + "RM_PR_DELIVERY_PLACE_RMKS WS_PR_DELIVERY_PLACE_RMKS,  ";
                SQL = SQL + "RM_PRD_APP_REMARKS  WS_PRD_APP_REMARKS ,RM_PR_CREATEDBY WS_PR_CREATEDBY, ";
                SQL = SQL + "RM_PR_VERIFIEDBY WS_PR_VERIFIED_BY, ";
                SQL = SQL + "RM_PR_APPROVEDBY WS_PR_APPROVED_BY,RM_PR_REMARKS WS_PR_REMARKS  ";
                SQL = SQL + " FROM RM_PUR_RFQ_REQUEST_VW  ";
                SQL = SQL + "  WHERE RM_PRFQ_DOC_NO = '" + RFQNo + "'  ";
                SQL = SQL + "  AND AD_FIN_FINYRID = " + EntryFinYearID + "  ";
                SQL = SQL + "  ORDER BY RM_PRFQ_DOC_NO ,RM_PRFQD_SLNO ASC  ";


                //dsData = clsSQLHelper.GetDataset(SQL);


                dtPOStatus = clsSQLHelper.GetDataTableByCommand(SQL);
                dsPoStatus.Tables.Add(dtPOStatus);
                dsPoStatus.Tables[0].TableName = "WS_PUR_REQ_PRINT_VW";
                SQL = " SELECT      ";
                SQL = SQL + "       RM_PUR_RFQ_MASTER.Rm_PRFQ_DOC_NO WS_PRFQ_DOC_NO, RM_PUR_RFQ_DETAILS.RM_RMM_RM_CODE   WS_IM_ITEM_CODE, 0 WS_IPD_QTY_ON_HAND,  ";
                SQL = SQL + "       RM_PUR_RFQ_DETAILS.SALES_STN_STATION_CODE   ";
                SQL = SQL + "  FROM   ";
                SQL = SQL + "       RM_PUR_RFQ_MASTER,RM_PUR_RFQ_DETAILS,RM_RAWMATERIAL_MASTER  ";
                SQL = SQL + "  WHERE   ";
                SQL = SQL + "       RM_PUR_RFQ_MASTER.rm_PRFQ_DOC_NO =RM_PUR_RFQ_DETAILS.rm_PRFQ_DOC_NO   ";
                SQL = SQL + "       AND RM_PUR_RFQ_MASTER.AD_FIN_FINYRID=RM_PUR_RFQ_DETAILS.AD_FIN_FINYRID   ";
                SQL = SQL + "       AND RM_PUR_RFQ_MASTER.WS_PRFQ_DOC_NO ='" + RFQNo + "'  ";
                SQL = SQL + "       AND RM_PUR_RFQ_MASTER.AD_FIN_FINYRID =" + EntryFinYearID + "  ";


                dtItemQty = clsSQLHelper.GetDataTableByCommand(SQL);
                dsPoStatus.Tables.Add(dtItemQty);
                dsPoStatus.Tables[1].TableName = "WS_PUR_REQ_PRINT_QTY";

                SQL = "  SELECT  ";
                SQL = SQL + " A.MAXGRVDATE ,B.WS_IM_ITEM_CODE ,B.WS_IM_ITEM_DESC ,sum(B.WS_GRVD_QTY)WS_GRVD_QTY  ";
                SQL = SQL + " FROM  ";
                SQL = SQL + " (  ";
                SQL = SQL + " SELECT MAX(GRVDATE) MAXGRVDATE, WS_IM_ITEM_CODE MAXWS_IM_ITEM_CODE  ";
                SQL = SQL + " FROM (  ";
                SQL = SQL + " SELECT  ";
                SQL = SQL + " MAX(WS_GRV_MASTER.WS_GRV_GRVDATE) GRVDATE,  ";
                SQL = SQL + "  WS_GRV_DETAILS.WS_IM_ITEM_CODE , WS_ITEM_MASTER.WS_IM_ITEM_DESC ,  WS_GRV_DETAILS.WS_GRVD_QTY ";
                SQL = SQL + " FROM  ";
                SQL = SQL + " WS_GRV_MASTER, WS_GRV_DETAILS, WS_ITEM_MASTER  ";
                SQL = SQL + " WHERE WS_GRV_MASTER.WS_GRV_GRVNO= WS_GRV_DETAILS.WS_GRV_GRVNO ";
                SQL = SQL + " AND WS_GRV_DETAILS.AD_FIN_FINYRID = WS_GRV_DETAILS.AD_FIN_FINYRID  ";
                SQL = SQL + " AND WS_GRV_DETAILS.WS_IM_ITEM_CODE = WS_ITEM_MASTER.WS_IM_ITEM_CODE  ";
                SQL = SQL + " AND WS_GRV_DETAILS.WS_IM_ITEM_CODE IN (  ";
                SQL = SQL + " SELECT WS_IM_ITEM_CODE  ";
                SQL = SQL + " FROM  ";
                SQL = SQL + " WS_PUR_RFQ_DETAILS  ";
                SQL = SQL + " WHERE WS_PRFQ_DOC_NO ='" + RFQNo + "')  ";
                SQL = SQL + " GROUP BY  WS_GRV_DETAILS.WS_IM_ITEM_CODE ,WS_ITEM_MASTER.WS_IM_ITEM_DESC , WS_GRV_DETAILS.WS_GRVD_QTY ";
                SQL = SQL + " )  ";
                SQL = SQL + " GROUP BY WS_IM_ITEM_CODE ) A ,  ";
                SQL = SQL + " (SELECT  ";
                SQL = SQL + " MAX(WS_GRV_MASTER.WS_GRV_GRVDATE) GRVDATE, ";
                SQL = SQL + " WS_GRV_DETAILS.WS_IM_ITEM_CODE ,WS_ITEM_MASTER.WS_IM_ITEM_DESC ,WS_GRV_DETAILS.WS_GRVD_QTY ";
                SQL = SQL + " FROM  ";
                SQL = SQL + " WS_GRV_MASTER, WS_GRV_DETAILS, WS_ITEM_MASTER  ";
                SQL = SQL + " WHERE WS_GRV_MASTER.WS_GRV_GRVNO =  WS_GRV_DETAILS.WS_GRV_GRVNO ";
                SQL = SQL + " AND WS_GRV_MASTER.AD_FIN_FINYRID =  WS_GRV_DETAILS.AD_FIN_FINYRID  ";
                SQL = SQL + " AND WS_GRV_DETAILS.WS_IM_ITEM_CODE = WS_ITEM_MASTER.WS_IM_ITEM_CODE  ";
                SQL = SQL + " AND WS_GRV_DETAILS.WS_IM_ITEM_CODE IN (  ";
                SQL = SQL + " SELECT WS_IM_ITEM_CODE  ";
                SQL = SQL + " FROM  ";
                SQL = SQL + " WS_PUR_RFQ_DETAILS  ";
                SQL = SQL + " WHERE WS_PRFQ_DOC_NO ='" + RFQNo + "' )  ";
                SQL = SQL + " GROUP BY WS_GRV_DETAILS.WS_IM_ITEM_CODE ,WS_ITEM_MASTER.WS_IM_ITEM_DESC , WS_GRV_DETAILS.WS_GRVD_QTY ";
                SQL = SQL + " ) B  ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + " B.GRVDATE = A.MAXGRVDATE ";
                SQL = SQL + " AND B.WS_IM_ITEM_CODE = A.MAXWS_IM_ITEM_CODE  ";
                SQL = SQL + " group by B.WS_IM_ITEM_CODE ,B.WS_IM_ITEM_DESC, A.MAXGRVDATE ";

                dtLastPur = clsSQLHelper.GetDataTableByCommand(SQL);
                dsPoStatus.Tables.Add(dtLastPur);
                dsPoStatus.Tables[2].TableName = "WS_PUR_REQ_PRINT_LAST_PUR";


                SQL = " select   ";
                SQL = SQL + "    WS_PRFQ_DOC_DATE ,  ADD_MONTHS(WS_PRFQ_DOC_DATE ,-12) CONSUMPTIO_STARTDATE, manqry.WS_IM_ITEM_CODE,  manqry.AD_BR_CODE,  ";
                SQL = SQL + "    WS_IPD_QTY_ON_HAND ,ISSUE_QTY  ";
                SQL = SQL + "       FROM   ";
                SQL = SQL + "         (   ";
                SQL = SQL + "    SELECT            ";
                SQL = SQL + "       WS_PRFQ_DOC_DATE ,  WS_PUR_RFQ_DETAILS.WS_IM_ITEM_CODE WS_IM_ITEM_CODE, WS_PUR_RFQ_MASTER.AD_BR_CODE,  ";
                SQL = SQL + "        SUM(WS_ITEM_PRICE_DETAILS.WS_IPD_QTY_ON_HAND)   WS_IPD_QTY_ON_HAND  ";
                SQL = SQL + "  FROM   ";
                SQL = SQL + "       WS_PUR_RFQ_MASTER,WS_PUR_RFQ_DETAILS,WS_ITEM_MASTER,WS_ITEM_PRICE_DETAILS   ";
                SQL = SQL + "  WHERE   ";
                SQL = SQL + "    WS_PUR_RFQ_MASTER.WS_PRFQ_DOC_NO =WS_PUR_RFQ_DETAILS.WS_PRFQ_DOC_NO   ";
                SQL = SQL + "    AND WS_PUR_RFQ_MASTER.AD_FIN_FINYRID=WS_PUR_RFQ_DETAILS.AD_FIN_FINYRID   ";
                SQL = SQL + "    AND WS_PUR_RFQ_DETAILS.WS_IM_ITEM_CODE=WS_ITEM_MASTER.WS_IM_ITEM_CODE      ";
                SQL = SQL + "    AND WS_PUR_RFQ_DETAILS.WS_IM_ITEM_CODE=WS_ITEM_PRICE_DETAILS.WS_IM_ITEM_CODE    ";
                SQL = SQL + "    AND WS_PUR_RFQ_DETAILS.SALES_STN_STATION_CODE=  WS_ITEM_PRICE_DETAILS.SALES_STN_STATION_CODE    ";
                SQL = SQL + "    AND WS_PUR_RFQ_MASTER.AD_BR_CODE=  WS_ITEM_PRICE_DETAILS.AD_BR_CODE    ";
                SQL = SQL + "    AND WS_PUR_RFQ_MASTER.WS_PRFQ_DOC_NO = '" + RFQNo + "'  ";
                SQL = SQL + "    group by   WS_PRFQ_DOC_DATE,    WS_PUR_RFQ_DETAILS.WS_IM_ITEM_CODE  ,   WS_PUR_RFQ_MASTER.AD_BR_CODE   ";
                SQL = SQL + "    ) manqry,  ";
                SQL = SQL + "  (  SELECT   WS_IM_ITEM_CODE ,AD_BR_CODE , sum(   ISSUE_QTY ) ISSUE_QTY  FROM   ";
                SQL = SQL + "      (  ";
                SQL = SQL + "            (   select   WS_IM_ITEM_CODE ,AD_BR_CODE , sum( WS_STKL_ISSUE_QTY - WS_STKL_RECD_QTY ) ISSUE_QTY   ";
                SQL = SQL + "              from WS_STOCK_LEDGER  where WS_STKL_TRANS_TYPE in( 'STORE_RETURN','FUEL_CONS','STORE_ISSUE')   ";
                SQL = SQL + "              AND WS_STKL_TRANS_DATE   ";
                SQL = SQL + "               BETWEEN  ( SELECT ADD_MONTHS(WS_PRFQ_DOC_DATE ,-12) FROM WS_PUR_RFQ_MASTER WHERE WS_PRFQ_DOC_NO =  '" + RFQNo + "' )   ";
                SQL = SQL + "                and   ( SELECT  WS_PRFQ_DOC_DATE   FROM WS_PUR_RFQ_MASTER WHERE WS_PRFQ_DOC_NO = '" + RFQNo + "' )   ";
                SQL = SQL + "              GROUP BY WS_IM_ITEM_CODE ,AD_BR_CODE   ";
                SQL = SQL + "              UNION ALL     ";
                SQL = SQL + "              SELECT  WS_IM_ITEM_CODE ,AD_BR_CODE , sum( WS_STKL_ISSUE_QTY - WS_STKL_NON_STOCK_RETURN_QTY ) ISSUE_QTY   ";
                SQL = SQL + "               FROM WS_STOCK_LEDGER WHERE ( WS_STKL_NON_STOCK_RECD_QTY >0  OR WS_STKL_NON_STOCK_RETURN_QTY >0 )   ";
                SQL = SQL + "               AND        WS_STKL_TRANS_TYPE in( 'GRV','GRTN' )   ";
                SQL = SQL + "                AND WS_STKL_TRANS_DATE   ";
                SQL = SQL + "               BETWEEN  ( SELECT ADD_MONTHS(WS_PRFQ_DOC_DATE ,-12) FROM WS_PUR_RFQ_MASTER WHERE WS_PRFQ_DOC_NO =  '" + RFQNo + "')   ";
                SQL = SQL + "                and   ( SELECT  WS_PRFQ_DOC_DATE   FROM WS_PUR_RFQ_MASTER WHERE WS_PRFQ_DOC_NO =  '" + RFQNo + "' )   ";
                SQL = SQL + "              GROUP BY WS_IM_ITEM_CODE ,AD_BR_CODE  ";
                SQL = SQL + "            )    ";
                SQL = SQL + "      )   GROUP BY WS_IM_ITEM_CODE ,AD_BR_CODE   ";
                SQL = SQL + "      ) CONSUMPTIONQRY  ";
                SQL = SQL + "      WHERE  manqry.WS_IM_ITEM_CODE  =  CONSUMPTIONQRY.WS_IM_ITEM_CODE   ";
                SQL = SQL + "      AND  manqry.AD_BR_CODE  =  CONSUMPTIONQRY.AD_BR_CODE  ";


                dtAvgConsumption = clsSQLHelper.GetDataTableByCommand(SQL);
                dsPoStatus.Tables.Add(dtAvgConsumption);
                dsPoStatus.Tables[3].TableName = "WS_STOCK_LEDGER";


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dsPoStatus;
        }

        public DataSet FetchRFQPrintComparisonData(string RFQNo, int EntryFinYearID, string sSelectedItem)
        {
            DataSet dsData = new DataSet();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  RM_PRFQ_DOC_NO WS_PRFQ_DOC_NO,AD_FIN_FINYRID,RM_PRFQ_DOC_DATE WS_PRFQ_DOC_DATE, AD_BR_CODE,   ";
                SQL = SQL + "        AD_BR_NAME,RM_PR_PRNO WS_PR_PRNO, AD_PR_FIN_FINYRID,   ";
                SQL = SQL + "       RM_PO_REQUEST_TYPE_CODE WS_PO_REQUEST_TYPE_CODE,'' WS_PO_REQUEST_TYPE_NAME, ";
                SQL = SQL + "       RM_PR_STOCK_TYPE_CODE WS_PR_STOCK_TYPE_CODE,   ";
                SQL = SQL + "     ''   WS_PO_STOCK_TYPE_NAME, FA_FAM_ASSET_CODE, FA_FAM_ASSET_DESCRIPTION,    ";
                SQL = SQL + "        SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, HR_DEPT_DEPT_CODE,    ";
                SQL = SQL + "        HR_DEPT_DEPT_DESC, RM_UM_UOM_CODE, RM_UM_UOM_DESC,    ";
                SQL = SQL + "        GL_COSM_ACCOUNT_CODE, COSTINGNAME, PC_BUD_BUDGET_ITEM_CODE,    ";
                SQL = SQL + "      rm_PRD_SLNO  WS_PRD_SLNO,RM_RMM_RM_CODE WS_IM_ITEM_CODE,RM_RMM_RM_DESCRIPTION WS_IM_ITEM_DESC,  ";
                SQL = SQL + "      ''  WS_IM_ITEM_DTL_DESCRIPTION,rm_PRFQD_SLNO WS_PRFQD_SLNO,RM_PRFQ_QTY WS_PRFQ_QTY,    ";
                SQL = SQL + "        RM_VM_VENDOR_CODE, RM_VM_VENDOR_NAME,rm_PRQ_SUPP_QUOTE_ORIG_RATE WS_PRQ_SUPP_QUOTE_ORIG_RATE,    ";
                SQL = SQL + "      rm_PRQ_SUPP_QUOTE_DISC_RATE WS_PRQ_SUPP_QUOTE_DISC_RATE,rm_PRQ_SUPP_QUOTE_UNIT_RATE WS_PRQ_SUPP_QUOTE_UNIT_RATE,  ";
                SQL = SQL + "        MASTER_BRANCH_CODE,MASTER_BRANCH_NAME,MASTER_BRANCHDOC_PREFIX,  ";
                SQL = SQL + "        MASTER_BRANCH_POBOX,MASTER_BRANCH_ADDRESS,MASTER_BRANCH_CITY,  ";
                SQL = SQL + "        MASTER_BRANCH_PHONE,MASTER_BRANCH_FAX,MASTER_BRANCH_SPONSER_NAME,  ";
                SQL = SQL + "        MASTER_BRANCH_TRADE_LICENSE_NO,MASTER_BRANCH_EMAIL_ID,MASTER_BRANCH_WEB_SITE,  ";
                SQL = SQL + "        MASTER_BRANCH_VAT_REG_NUMBER ,RM_SM_SOURCE_CODE,RM_SM_SOURCE_DESC  ";
                SQL = SQL + "  FROM rm_PUR_RFQ_RATE_COMPARISON_VW    ";
                SQL = SQL + "  WHERE rm_PRFQ_DOC_NO = '" + RFQNo + "'  ";
                SQL = SQL + "  AND AD_FIN_FINYRID = " + EntryFinYearID + "  ";
                if (!string.IsNullOrEmpty(sSelectedItem))
                {
                    SQL = SQL + "  AND RM_RMM_RM_CODE  IN (" + sSelectedItem + ")";
                }


                dsData = clsSQLHelper.GetDataset(SQL);
                /// dsData.Tables[0].TableName = "WS_PUR_RFQ_RATE_COMPARISON_VW";

                SQL = " SELECT   ";
                SQL = SQL + " A.MAXPODATE ,B.RM_VM_VENDOR_CODE ,B.RM_VM_VENDOR_NAME ,B.WS_IM_ITEM_CODE ,B.WS_IM_ITEM_DESC , ";
                SQL = SQL + " B.RM_SM_SOURCE_CODE, B.RM_SM_SOURCE_DESC,B.WS_POD_UNIT_PRICE   ";
                SQL = SQL + " FROM   ";
                SQL = SQL + " (    ";
                SQL = SQL + " SELECT MAX(PODATE) MAXPODATE, RM_RMM_RM_CODE MAXWS_IM_ITEM_CODE  ,RM_SM_SOURCE_CODE ";
                SQL = SQL + " FROM (   ";
                SQL = SQL + " SELECT   ";
                SQL = SQL + " MAX(rm_PO_MASTER.rm_PO_PO_DATE) PODATE, rm_PO_MASTER.RM_VM_VENDOR_CODE , RM_VENDOR_MASTER.RM_VM_VENDOR_NAME ,   ";
                SQL = SQL + " rm_PO_DETAILS.RM_RMM_RM_CODE   , RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION WS_IM_ITEM_DESC ,  ";
                SQL = SQL + "  rm_PO_DETAILS.RM_POD_UNIT_PRICE WS_POD_UNIT_PRICE  ,rm_PO_DETAILS.RM_SM_SOURCE_CODE ";
                SQL = SQL + " FROM   ";
                SQL = SQL + " rm_PO_MASTER, rm_PO_DETAILS, RM_VENDOR_MASTER, RM_RAWMATERIAL_MASTER  ,RM_SOURCE_MASTER ";
                SQL = SQL + " WHERE rm_PO_MASTER.RM_PO_PONO = rm_PO_DETAILS.RM_PO_PONO   ";
                SQL = SQL + " AND rm_PO_DETAILS.RM_SM_SOURCE_CODE  =RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + " AND rm_PO_MASTER.AD_FIN_FINYRID = rm_PO_DETAILS.AD_FIN_FINYRID   ";
                SQL = SQL + " AND rm_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE   ";
                SQL = SQL + " AND rm_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + " AND rm_PO_DETAILS.RM_RMM_RM_CODE||rm_PO_DETAILS.RM_SM_SOURCE_CODE IN (   ";
                SQL = SQL + " SELECT DISTINCT  RM_RMM_RM_CODE||RM_SM_SOURCE_CODE ";
                SQL = SQL + " FROM   ";
                SQL = SQL + " rm_pr_DETAILS   ";
                SQL = SQL + " WHERE rm_PR_PRNO  IN(      ";
                SQL = SQL + " SELECT DISTINCT  rm_PR_PRNO  FROM  rm_PUR_RFQ_RATE_COMPARISON_VW WHERE    ";
                SQL = SQL + "  rm_PRFQ_DOC_NO = '" + RFQNo + "' ";
                SQL = SQL + " AND AD_FIN_FINYRID = " + EntryFinYearID + " ";
                SQL = SQL + " )   ";
                SQL = SQL + " )   ";
                SQL = SQL + " GROUP BY rm_PO_MASTER.RM_VM_VENDOR_CODE ,   ";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME ,  rm_PO_DETAILS.RM_RMM_RM_CODE ,RM_RMM_RM_DESCRIPTION , rm_PO_DETAILS.RM_POD_UNIT_PRICE , ";
                SQL = SQL + " rm_PO_DETAILS.RM_SM_SOURCE_CODE  ";
                SQL = SQL + " )   ";
                SQL = SQL + " GROUP BY WS_IM_ITEM_DESC ,RM_RMM_RM_CODE  ,RM_SM_SOURCE_CODE ";
                SQL = SQL + " )    ";
                SQL = SQL + " A ,   ";
                SQL = SQL + " ( ";
                SQL = SQL + " SELECT   ";
                SQL = SQL + " MAX(rm_PO_MASTER.rm_PO_PO_DATE) PODATE,rm_PO_MASTER.RM_VM_VENDOR_CODE ,RM_VENDOR_MASTER.RM_VM_VENDOR_NAME ,   ";
                SQL = SQL + "rm_PO_DETAILS.RM_RMM_RM_CODE WS_IM_ITEM_CODE ,  ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION WS_IM_ITEM_DESC ,rm_PO_DETAILS.RM_POD_UNIT_PRICE WS_POD_UNIT_PRICE , ";
                SQL = SQL + "  rm_PO_DETAILS.RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC ";
                SQL = SQL + " FROM   ";
                SQL = SQL + " rm_PO_MASTER, rm_PO_DETAILS, RM_VENDOR_MASTER, RM_RAWMATERIAL_MASTER  ,RM_SOURCE_MASTER ";
                SQL = SQL + " WHERE rm_PO_MASTER.rm_PO_PONO = rm_PO_DETAILS.rm_PO_PONO   ";
                SQL = SQL + "  AND rm_PO_DETAILS.RM_SM_SOURCE_CODE  =RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + " AND rm_PO_MASTER.AD_FIN_FINYRID = rm_PO_DETAILS.AD_FIN_FINYRID   ";
                SQL = SQL + " AND rm_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE   ";
                SQL = SQL + " AND rm_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + " AND rm_PO_DETAILS.RM_RMM_RM_CODE||rm_PO_DETAILS.RM_SM_SOURCE_CODE  IN (   ";
                SQL = SQL + " SELECT DISTINCT  RM_RMM_RM_CODE||RM_SM_SOURCE_CODE   ";
                SQL = SQL + " FROM   ";
                SQL = SQL + " rm_Pr_DETAILS   ";
                SQL = SQL + " WHERE rm_PR_PRNO|| RM_SM_SOURCE_CODE  IN(      ";
                SQL = SQL + " SELECT DISTINCT  rm_PR_PRNO||RM_SM_SOURCE_CODE  FROM  rm_PUR_RFQ_RATE_COMPARISON_VW  WHERE   ";
                SQL = SQL + "  rm_PRFQ_DOC_NO = '" + RFQNo + "' ";
                SQL = SQL + " AND AD_FIN_FINYRID = " + EntryFinYearID + " ";
                SQL = SQL + " )   ";
                SQL = SQL + " )   ";
                SQL = SQL + " GROUP BY rm_PO_MASTER.RM_VM_VENDOR_CODE ,   ";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME , rm_PO_DETAILS.RM_RMM_RM_CODE ,RM_RMM_RM_DESCRIPTION, rm_PO_DETAILS.RM_POD_UNIT_PRICE     ";
                SQL = SQL + "   ,rm_PO_DETAILS.RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC ";
                SQL = SQL + " ) B   ";
                SQL = SQL + " WHERE   ";
                SQL = SQL + " B.PODATE = A.MAXPODATE   ";
                SQL = SQL + " AND B.WS_IM_ITEM_CODE = A.MAXWS_IM_ITEM_CODE   ";

                if (!string.IsNullOrEmpty(sSelectedItem))
                {
                    SQL = SQL + "  AND B.WS_IM_ITEM_CODE  IN (" + sSelectedItem + ")";
                }

                DataTable dtPRStatus = new DataTable();
                dtPRStatus = clsSQLHelper.GetDataTableByCommand(SQL);
                dtPRStatus.TableName = "WS_PUR_REQ_PREV_DATA_PRINT_VW";
                dsData.Tables.Add(dtPRStatus);


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dsData;
        }

        #endregion

        #region  "Doc Attach funtions - Insert - Fill - delete Path "

        public void InsertAttachFile(string lblCVPath, string AttachRemarks, string PurChaseRqeuestNo)
        {
            DataSet sReturn = new DataSet();
            Int32 SLNO = 0;

            try
            {
                SQL = string.Empty;
                OracleHelper oTrns = new OracleHelper();
                SqlHelper clsSQLHelper = new SqlHelper();
                sSQLArray.Clear();


                SQL = "SELECT MAX(RM_PRFQA_SLNO) slno FROM RM_PUR_RFQ_ATTACH_DTLS WHERE RM_PRFQ_DOC_NO  ='" + PurChaseRqeuestNo + "'";

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

                SQL = " INSERT INTO RM_PUR_RFQ_ATTACH_DTLS";

                SQL = SQL + " (RM_PRFQ_DOC_NO  , RM_PRFQA_SLNO  , RM_PRFQA_REMARKS , RM_PRFQA_FILE_PATH  )";

                SQL = SQL + " Values('" + PurChaseRqeuestNo + "'," + SLNO + ",'" + AttachRemarks + "','" + lblCVPath + "')";

                oTrns.GetExecuteNonQueryBySQL(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return;
        }

        public DataSet fillAttachGrid(string PurChaseRqeuestNo)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_PRFQ_DOC_NO  , RM_PRFQA_REMARKS    , RM_PRFQA_FILE_PATH  ";

                SQL = SQL + " FROM RM_PUR_RFQ_ATTACH_DTLS ";

                SQL = SQL + " WHERE RM_PRFQ_DOC_NO  ='" + PurChaseRqeuestNo + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public void DeletePath(string PurChaseRqeuestNo, string Path)
        {
            try
            {
                SQL = string.Empty;

                OracleHelper oTrns = new OracleHelper();
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " DELETE FROM RM_PUR_REQ_ATTACH_DTLS ";
                SQL = SQL + " WHERE RM_PR_PRNO  ='" + PurChaseRqeuestNo + "' AND RM_PR_FILE_PATH  ='" + Path + "'";

                oTrns.GetExecuteNonQueryBySQL(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return;
        }

        #endregion



        //Nayana Created on  01/Apr/2025 for EBRM
        #region "RowDeletion Function"


        public string DeleteRowSQL(string RFQNo, int RFQFinYR, string PRNo, int PRFinYR, int PRSlNo, object mngrclassobj)
        {

            ////////////////////////// grid index end 

            string sRetun = string.Empty;


            try
            {
                SQL = string.Empty;
                OracleHelper oTrns = new OracleHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sRetun = DeleteRowCheck(PRNo, PRFinYR, PRSlNo);

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sSQLArray.Clear();


                SQL = "    UPDATE   ";
                SQL = SQL + "     RM_PR_DETAILS  ";
                SQL = SQL + "    SET   ";
                SQL = SQL + "      RM_PUR_RFQ_NO='', RM_PUR_RFQ_DONE='N'  ";
                SQL = SQL + "    WHERE  ";
                SQL = SQL + "      RM_PR_PRNO='" + PRNo + "' ";
                SQL = SQL + "      AND RM_PRD_SL_NO  ='" + PRSlNo + "' ";
                SQL = SQL + "      AND  AD_FIN_FINYRID ='" + PRFinYR + "' ";

                sSQLArray.Add(SQL);


                SQL = "    UPDATE   ";
                SQL = SQL + "     RM_PR_MASTER  ";
                SQL = SQL + "    SET   ";
                SQL = SQL + "      RM_PR_PR_STATUS='O' ,  ";
                SQL = SQL + "      RM_PR_UPDATEDBY= '" + mngrclass.UserName + "' ,   ";
                SQL = SQL + "      RM_PR_UPDATEDDATE= SYSDATE   ";
                SQL = SQL + "    WHERE  ";
                SQL = SQL + "      RM_PR_PRNO='" + PRNo + "' ";
                SQL = SQL + "      AND AD_FIN_FINYRID ='" + PRFinYR + "' ";

                sSQLArray.Add(SQL);



                SQL = "    DELETE FROM    ";
                SQL = SQL + "     RM_PUR_RFQ_DETAILS  ";
                SQL = SQL + "    WHERE  ";
                SQL = SQL + "      RM_PRFQ_DOC_NO='" + RFQNo + "' ";
                SQL = SQL + "      AND AD_FIN_FINYRID='" + RFQFinYR + "' ";
                SQL = SQL + "      AND RM_PR_PRNO='" + PRNo + "' ";
                SQL = SQL + "      AND AD_PR_FIN_FINYRID ='" + PRFinYR + "' ";
                SQL = SQL + "      AND RM_PRD_SLNO ='" + PRSlNo + "' ";

                sSQLArray.Add(SQL);



                SQL = "    DELETE FROM    ";
                SQL = SQL + "     RM_PUR_RFQ_DETAILS_SUPP_QUOTE  ";
                SQL = SQL + "    WHERE  ";
                SQL = SQL + "      RM_PRFQ_DOC_NO='" + RFQNo + "' ";
                SQL = SQL + "     AND  AD_FIN_FINYRID='" + RFQFinYR + "' ";
                SQL = SQL + "     AND  RM_PR_PRNO='" + PRNo + "' ";
                SQL = SQL + "     AND AD_PR_FIN_FINYRID ='" + PRFinYR + "' ";
                SQL = SQL + "     AND  RM_PRD_SLNO ='" + PRSlNo + "' ";

                sSQLArray.Add(SQL);


                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMPMRQRFQ", RFQNo, false, Environment.MachineName, "U", sSQLArray);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return sRetun;


        }



        public string DeleteRowCheck(string PRNo, int PRFinYR, int PRSlNo)
        {

            DataSet dsCnt = new DataSet();


            string sRetun = string.Empty;
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();
                sSQLArray.Clear();

                SQL = "    SELECT  ";
                SQL = SQL + "      NVL(RM_PRD_POD_QTY,0) PO_QTY  ";
                SQL = SQL + "    FROM  ";
                SQL = SQL + "      RM_PR_DETAILS ";
                SQL = SQL + "    WHERE  ";
                SQL = SQL + "      RM_PR_PRNO='" + PRNo + "' ";
                SQL = SQL + "      AND  RM_PRD_SL_NO  ='" + PRSlNo + "' ";
                SQL = SQL + "      AND AD_FIN_FINYRID ='" + PRFinYR + "' ";

                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "PO Generated For this Item...";
                }


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return System.Convert.ToString(ex.Message);
            }
            return "CONTINUE";

        }



        #endregion 
    }

    #region"Entity"

    public class MaterialRequisitionRFQEntryWSEntity
    {
        public MaterialRequisitionRFQEntryWSEntity()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public string RFQNo
        {
            get; set;
        }
        public int RFQFinYrId
        {
            get;
            set;
        }
        public DateTime RFQDate
        {
            get;
            set;
        }
        public string Branch
        {
            get;
            set;
        }
        public string Remarks
        {
            get;
            set;
        }
        public string Verified
        {
            get;
            set;
        }
        public string VerifiedBy
        {
            get;
            set;
        }
        public string VerifiedDateTime
        {
            get;
            set;
        }
        public string Approved
        {
            get;
            set;
        }
        public string ApprovedBy
        {
            get;
            set;
        }
        public string ApprovedDateTime
        {
            get;
            set;
        }
    }

    public class MaterialRequisitionRFQEntryWSGridEntity
    {
        public MaterialRequisitionRFQEntryWSGridEntity()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public string PRNO
        {
            get;
            set;
        }

        public int PRFinYr
        {
            get;
            set;
        }

        public int PRSLNo
        {
            get;
            set;
        }

        public int SLNo
        {
            get;
            set;
        }

        public string RMCode
        {
            get;
            set;
        }

        public string ItemDesc
        {
            get;
            set;
        }

        public double Qty
        {
            get;
            set;
        }

        public string AccCode
        {
            get;
            set;
        }

        public string DivCode
        {
            get;
            set;
        }

        public string SourceCode
        {
            get;
            set;
        }

        public string StnCode
        {
            get;
            set;
        }

        public string DeptCode
        {
            get;
            set;
        }

        public string UOMCode
        {
            get;
            set;
        }

        public string BudgItemCode
        {
            get;
            set;
        }

        public string StockItem
        {
            get;
            set;
        }

        public string Createdby
        {
            get;
            set;
        }

        public string Approvedby
        {
            get;
            set;
        }
        public string PrintYN
        {
            get;
            set;
        }

    }

    public class MaterialRequisitionRFQEntryWSGridSuppEntity
    {
        public MaterialRequisitionRFQEntryWSGridSuppEntity()
        {

        }

        public string SuppCode
        {
            get;
            set;
        }
        public string QuotationNo
        {
            get;
            set;
        }
        public string QuotationDate
        {
            get;
            set;
        }
        public string QuotationReffNo
        {
            get;
            set;
        }
        public string Payterms
        {
            get;
            set;
        }
        public string Remarks
        {
            get;
            set;
        }
    }

    public class MaterialRequisitionRFQEntryWSGridSuppRateEntity
    {
        public MaterialRequisitionRFQEntryWSGridSuppRateEntity()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public string PRNO
        {
            get;
            set;
        }

        public int PRFinYr
        {
            get;
            set;
        }

        public int PRSLNo
        {
            get;
            set;
        }

        public int SLNo
        {
            get;
            set;
        }

        public string ItemCode
        {
            get;
            set;
        }

        public string ItemDesc
        {
            get;
            set;
        }

        public double Qty
        {
            get;
            set;
        }

        public string PriorityCode
        {
            get;
            set;
        }

        public string TypeCode
        {
            get;
            set;
        }

        public string CostCentreCode
        {
            get;
            set;
        }

        public string DivCode
        {
            get;
            set;
        }

        public string AssetCode
        {
            get;
            set;
        }

        public string StnCode
        {
            get;
            set;
        }

        public string DeptCode
        {
            get;
            set;
        }

        public string UOMCode
        {
            get;
            set;
        }

        public string BudgItemCode
        {
            get;
            set;
        }

        public string Createdby
        {
            get;
            set;
        }

        public string Approvedby
        {
            get;
            set;
        }

        public string SuppCode
        {
            get;
            set;
        }

        public double OrgRate
        {
            get;
            set;
        }

        public double DisRate
        {
            get;
            set;
        }

        public double UnitRate
        {
            get;
            set;
        }

        public string Approved
        {
            get;
            set;
        }

        public double POQty
        {
            get;
            set;
        }

        public double PrevQty
        {
            get;
            set;
        }
    }

    public class MaterialRequisitionRFQEntryWSGridSuppRateQtyEntity
    {
        public MaterialRequisitionRFQEntryWSGridSuppRateQtyEntity()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public string PRNO
        {
            get;
            set;
        }

        public int PRFinYr
        {
            get;
            set;
        }

        public int PRSLNo
        {
            get;
            set;
        }

        public int SLNo
        {
            get;
            set;
        }

        public string ItemCode
        {
            get;
            set;
        }

        public string ItemDesc
        {
            get;
            set;
        }

        public double Qty
        {
            get;
            set;
        }

        public string PriorityCode
        {
            get;
            set;
        }

        public string TypeCode
        {
            get;
            set;
        }

        public string CostCentreCode
        {
            get;
            set;
        }

        public string DivCode
        {
            get;
            set;
        }

        public string AssetCode
        {
            get;
            set;
        }

        public string StnCode
        {
            get;
            set;
        }

        public string DeptCode
        {
            get;
            set;
        }

        public string UOMCode
        {
            get;
            set;
        }

        public string StockItem
        {
            get;
            set;
        }

        public string BudgItemCode
        {
            get;
            set;
        }

        public string Createdby
        {
            get;
            set;
        }

        public string Approvedby
        {
            get;
            set;
        }

        public string SuppCode
        {
            get;
            set;
        }

        public double OrgRate
        {
            get;
            set;
        }

        public double DisRate
        {
            get;
            set;
        }

        public double UnitRate
        {
            get;
            set;
        }

        public string Approved
        {
            get;
            set;
        }

        public double POQty
        {
            get;
            set;
        }
        public double AdditionalPOQty
        {
            get;
            set;
        }

        public double PrevQty
        {
            get;
            set;
        }
        public string sSourceCode
        {
            get;
            set;
        }
    }

    #endregion
}