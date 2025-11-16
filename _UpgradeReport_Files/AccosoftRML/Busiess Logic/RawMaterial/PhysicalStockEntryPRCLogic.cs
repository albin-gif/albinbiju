using System;
using System.Data; using AccosoftUtilities;   using AccosoftLogWriter;  using AccosoftNineteenCDL;   
using System.Configuration;
using System.Linq;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.HtmlControls;
using System.Web.UI.WebControls;
using System.Web.UI.WebControls.WebParts;
using System.Xml.Linq;
using Oracle.ManagedDataAccess.Client; using Oracle.ManagedDataAccess.Types;
using System.Collections; 
using System.Collections.Generic;

/// <summary>
/// Created By      :   Jins Mathew
/// Created On      :   22-Dec-2012
/// </summary>

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class PhysicalStockEntryPRCLogic
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;
        public DateTime BatchFromDate
        {
            get;
            set;
        }
        public DateTime BatchToDate
        {
            get;
            set;
        }

        public string FilterType
        {
            get;
            set;
        }
        string fsInvDesc = null;
        double lobjFactor = 0;

        System.DateTime dtTrDate = default(System.DateTime);

        //static DataSet dsComposistion=new DataSet();


        #region "look up " 

        public DataTable FillSearchView(string sFilterType, object mngrclassobj)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT ";
                SQL = SQL + " RM_PSEM_ENTRY_NO ENTRYNO, AD_FIN_FINYRID FINID,  to_char(RM_PSEM_ENTRY_DATE,'DD-MON-YYYY') ENTRYDATE, ";
                SQL = SQL + " RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE STCODE,SALES_STN_STATION_NAME STNAME,  to_char(RM_PSEM_FROM_DATE,'DD-MON-YYYY') FROMDATE, ";
                SQL = SQL + "    to_char(RM_PSEM_TO_DATE,'DD-MON-YYYY') TODATE,   ";
                SQL = SQL + "    RM_PSEM_REMARKS REMARKS  ";

                SQL = SQL + " FROM  RM_PHYSICAL_STOCK_MASTER,SL_STATION_MASTER  ";

                SQL = SQL + "    WHERE  ";
                SQL = SQL + "    RM_PHYSICAL_STOCK_MASTER.AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";
                SQL = SQL + "    AND SL_STATION_MASTER.SALES_STN_STATION_CODE=RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE ";

                if (sFilterType == "APPROVED")
                {
                    SQL = SQL + "  And RM_PSEM_APPROVED ='Y'";

                }
                else if (sFilterType == "NOT APPROVED")
                {
                    SQL = SQL + "  And RM_PSEM_APPROVED ='N'";
                }

                SQL = SQL + "  order by RM_PSEM_ENTRY_NO   desc";


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

     
        #endregion +
        #region "Average  Price "
        public DataSet FetchAvgPriceRMDtls(string lsStnCode, string dtpToDate, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT mas.rm_rmm_rm_code RMCODE,";
                SQL = SQL + " SUM ( NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0) - NVL (issued.iss_qty, 0) ) open_qty,";
                SQL = SQL + " trunc  ( ";
                SQL = SQL + " CASE when sUM ( NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0))- SUM(NVL (issued.iss_qty, 0)) > 0 THEN ";

                SQL = SQL + "  SUM ( NVL (opening.ob_val, 0) + NVL (recd.rec_value, 0) - NVL (issued.iss_value, 0))/ SUM (NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0)-NVL (issued.iss_qty, 0) )";
                SQL = SQL + "  when sUM ( NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0)) > 0 THEN ";
                SQL = SQL + "  SUM ( NVL (opening.ob_val, 0) + NVL (recd.rec_value, 0))/ SUM (NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0) )";
                SQL = SQL + "  when sUM ( NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0))- SUM(NVL (issued.iss_qty, 0)) = 0 then 0 ";
                SQL = SQL + "  ELSE 0 END ,5 )";
                SQL = SQL + "  AVGPRICE ,";

                SQL = SQL + " SUM (";
                SQL = SQL + " NVL (opening.ob_val, 0)";
                SQL = SQL + " + NVL (recd.rec_value, 0)";
                SQL = SQL + " - NVL (issued.iss_value, 0)";
                SQL = SQL + " ) open_value";
                SQL = SQL + " FROM rm_rawmaterial_master mas,";
                SQL = SQL + " rm_rawmaterial_details det,";
                SQL = SQL + " ( ";
                SQL = SQL + "  SELECT rm_rmm_rm_code rm_code, sales_stn_station_code stn_code,";
                SQL = SQL + " SUM (rm_ob_qty)";
                SQL = SQL + " ob_qty, SUM ( RM_OB_AMOUNT ) ob_val";
                SQL = SQL + " FROM RM_OPEN_BALANCES";
                SQL = SQL + " WHERE ad_fin_finyrid = " + mngrclass.FinYearID + " ";
                SQL = SQL + " AND sales_stn_station_code = '" + lsStnCode + "'";
                SQL = SQL + " GROUP BY rm_rmm_rm_code, sales_stn_station_code ) opening,";
                SQL = SQL + " ( ";
                SQL = SQL + "  SELECT RM_IM_ITEM_CODE rm_code, sales_stn_station_code stn_code,";
                SQL = SQL + " SUM (NVL (RM_STKL_RECD_QTY, 0))";
                SQL = SQL + " rec_qty,";
                SQL = SQL + " SUM (NVL (RM_STKL_RECD_AMT, 0) ) rec_value";
                SQL = SQL + " FROM RM_STOCK_LEDGER";
                SQL = SQL + " WHERE TO_DATE (TO_CHAR (rm_stkl_trans_date, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND ad_fin_finyrid =" + mngrclass.FinYearID + "";
                SQL = SQL + " AND sales_stn_station_code = '" + lsStnCode + "'";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE , sales_stn_station_code ";
                SQL = SQL + "  ) recd, ";
                SQL = SQL + " ( ";
                SQL = SQL + "  SELECT RM_IM_ITEM_CODE rm_code, sales_stn_station_code stn_code,";
                SQL = SQL + " SUM (NVL (RM_STKL_ISSUE_QTY, 0))";
                SQL = SQL + " iss_qty,";
                SQL = SQL + " SUM ( NVL (RM_STKL_ISSUE_AMOUNT, 0) ) iss_value";
                SQL = SQL + " FROM RM_STOCK_LEDGER";
                SQL = SQL + " WHERE TO_DATE (TO_CHAR (rm_stkl_trans_date, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND ad_fin_finyrid = " + mngrclass.FinYearID + "";
                SQL = SQL + " AND sales_stn_station_code = '" + lsStnCode + "'";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE , sales_stn_station_code ";
                SQL = SQL + "  ) issued";
                SQL = SQL + " WHERE ";
                SQL = SQL + " ";
                SQL = SQL + " mas.rm_rmm_rm_code = det.rm_rmm_rm_code";
                SQL = SQL + " AND det.rm_rmm_rm_code = opening.rm_code(+)";
                SQL = SQL + " AND det.sales_stn_station_code = opening.stn_code(+)";
                SQL = SQL + " AND det.rm_rmm_rm_code = recd.rm_code(+)";
                SQL = SQL + " AND det.sales_stn_station_code = recd.stn_code(+)";
                SQL = SQL + " AND det.rm_rmm_rm_code = issued.rm_code(+)";
                SQL = SQL + " AND det.sales_stn_station_code = issued.stn_code(+)";
                SQL = SQL + " AND det.sales_stn_station_code = '" + lsStnCode + "'";
                SQL = SQL + " GROUP BY mas.rm_rmm_rm_code,";
                SQL = SQL + " rm_rmm_rm_description,";
                SQL = SQL + " det.sales_stn_station_code";


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

        #region "  Fetch Opening  balances / RVD qty / stk in / stk out / sales / consumption
        public DataSet FetchOpeningBALANCES(string lsStnCode, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_RMM_RM_CODE RMCODE ,RM_OB_QTY QTY,";
                SQL = SQL + " RM_OB_AMOUNT OPENVALUE";
                SQL = SQL + " FROM RM_OPEN_BALANCES";


                SQL = SQL + " WHERE AD_FIN_FINYRID=" + mngrclass.FinYearID + "";
                SQL = SQL + " AND RM_OB_DELETESTATUS=0";
                SQL = SQL + " AND SALES_STN_STATION_CODE='" + lsStnCode + "' ORDER BY RM_RMM_RM_CODE";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchPrevReceivedQty(string lsStnCode, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "";
                SQL = " SELECT RM_IM_ITEM_CODE RMCODE, SALES_STN_STATION_CODE STN_CODE, ";
                SQL = SQL + " NVL(SUM(RM_STKL_RECD_QTY),0)- NVL(SUM(RM_STKL_ISSUE_QTY),0) QTY , ";
                //< SQL = SQL & " NVL(SUM(RM_STKL_RECD_QTY*RM_STKL_COST_PRICE),0)-NVL(SUM(RM_STKL_ISSUE_QTY*RM_STKL_ISSUE_PRICE),0) OPENSTKAMT"

                SQL = SQL + " NVL(SUM(RM_STKL_RECD_AMT),0)-NVL(SUM(RM_STKL_ISSUE_AMOUNT),0) OPENSTKAMT";

                SQL = SQL + " FROM RM_STOCK_LEDGER ";

                SQL = SQL + " WHERE";
                SQL = SQL + " TO_DATE(TO_CHAR(RM_STKL_TRANS_DATE,'DD-MON-YYYY')) < '" + ldFromDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND SALES_STN_STATION_CODE='" + lsStnCode + "'";
                SQL = SQL + " and AD_FIN_FINYRID=" + mngrclass.FinYearID + "";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE, SALES_STN_STATION_CODE ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchCurrentTOTALReceivedQty(string lsStnCode, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_IM_ITEM_CODE RMCODE,SALES_STN_STATION_CODE STN_CODE, ";
                SQL = SQL + " NVL(SUM(RM_STKL_RECD_QTY),0)- NVL(SUM(RM_STKL_ISSUE_QTY),0) QTY , ";
                SQL = SQL + " NVL(SUM(RM_STKL_RECD_AMT),0)-NVL(SUM(RM_STKL_ISSUE_AMOUNT),0) PURCHASEAMT";

                SQL = SQL + " FROM RM_STOCK_LEDGER ";

                SQL = SQL + " WHERE";
                SQL = SQL + " TO_DATE(TO_CHAR(RM_STKL_TRANS_DATE,'DD-MON-YYYY')) BETWEEN '" + ldFromDate.ToString("dd-MMM-yyyy") + "' AND '" + ldToDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND SALES_STN_STATION_CODE='" + lsStnCode + "'";
                SQL = SQL + " AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE,SALES_STN_STATION_CODE ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchCurrentMonthReceivedQty(string lsStnCode, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_IM_ITEM_CODE RMCODE,SALES_STN_STATION_CODE STN_CODE, ";
                SQL = SQL + " NVL(SUM(RM_STKL_RECD_QTY),0) RVDQTY , ";
                SQL = SQL + " NVL(SUM(RM_STKL_RECD_AMT),0) RVDAMT";

                SQL = SQL + " FROM RM_STOCK_LEDGER ";

                SQL = SQL + " WHERE";
                SQL = SQL + " TO_DATE(TO_CHAR(RM_STKL_TRANS_DATE,'DD-MON-YYYY')) BETWEEN '" + ldFromDate.ToString("dd-MMM-yyyy") + "' AND '" + ldToDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND SALES_STN_STATION_CODE='" + lsStnCode + "'";
                SQL = SQL + " AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";
                SQL = SQL + " AND RM_STKL_TRANS_TYPE ='RECEIPT'";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE,SALES_STN_STATION_CODE ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchCurrentMonthStocktransferInQty(string lsStnCode, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_IM_ITEM_CODE RMCODE,SALES_STN_STATION_CODE STN_CODE, ";
                SQL = SQL + " NVL(SUM(RM_STKL_RECD_QTY),0) STKTRNS_INQTY , ";
                SQL = SQL + " NVL(SUM(RM_STKL_RECD_AMT),0) STKTRNS_INAMT";

                SQL = SQL + " FROM RM_STOCK_LEDGER ";

                SQL = SQL + " WHERE";
                SQL = SQL + " TO_DATE(TO_CHAR(RM_STKL_TRANS_DATE,'DD-MON-YYYY')) BETWEEN '" + ldFromDate.ToString("dd-MMM-yyyy") + "' AND '" + ldToDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND SALES_STN_STATION_CODE='" + lsStnCode + "'";
                SQL = SQL + " AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";
                SQL = SQL + " AND RM_STKL_TRANS_TYPE ='STK_TRANS'";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE,SALES_STN_STATION_CODE ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchCurrentMonthStocktransferOUTQty(string lsStnCode, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_IM_ITEM_CODE RMCODE,SALES_STN_STATION_CODE STN_CODE, ";
                SQL = SQL + " NVL(SUM(RM_STKL_ISSUE_QTY),0) STKTRNS_OUTQTY , ";
                SQL = SQL + " NVL(SUM(RM_STKL_ISSUE_AMOUNT),0) STKTRNS_OUTAMT";

                SQL = SQL + " FROM RM_STOCK_LEDGER ";

                SQL = SQL + " WHERE";
                SQL = SQL + " TO_DATE(TO_CHAR(RM_STKL_TRANS_DATE,'DD-MON-YYYY')) BETWEEN '" + ldFromDate.ToString("dd-MMM-yyyy") + "' AND '" + ldToDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND SALES_STN_STATION_CODE='" + lsStnCode + "'";
                SQL = SQL + " AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";
                SQL = SQL + " AND RM_STKL_TRANS_TYPE ='STK_TRANS'";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE,SALES_STN_STATION_CODE ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchCurrentMonthSalesQty(string lsStnCode, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_IM_ITEM_CODE RMCODE,SALES_STN_STATION_CODE STN_CODE, ";
                SQL = SQL + " NVL(SUM(RM_STKL_ISSUE_QTY),0) SALES_OUTQTY , ";
                SQL = SQL + " NVL(SUM(RM_STKL_ISSUE_AMOUNT),0)SALES_AMOUNT";

                SQL = SQL + " FROM RM_STOCK_LEDGER ";

                SQL = SQL + " WHERE";
                SQL = SQL + " TO_DATE(TO_CHAR(RM_STKL_TRANS_DATE,'DD-MON-YYYY')) BETWEEN '" + ldFromDate.ToString("dd-MMM-yyyy") + "' AND '" + ldToDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND SALES_STN_STATION_CODE='" + lsStnCode + "'";
                SQL = SQL + " AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";
                SQL = SQL + " AND RM_STKL_TRANS_TYPE ='RM_SALE'";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE,SALES_STN_STATION_CODE ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchComposition(string lsStnCode, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT pr_dn_mix_details.tech_pm_product_code, pr_dn_mix_details.rm_rmm_rm_code,";
                SQL = SQL + " rm_rmm_rm_description, pr_dn_mix_details.rm_um_uom_code basic_uom, tech_pcd_uom_code composition_uom,";
                SQL = SQL + "  uom.rm_um_uom_desc basic_uom_desc, uomd.rm_um_uom_desc comp_uom_desc,";
                SQL = SQL + " SUM (tech_pcd_weight * (pr_dlyn_dn_qty - PR_DLYN_RETURN_IN_TRUCK_QTY)) cons_qty, rm_rmm_inv_acc_code, ";
                SQL = SQL + "  rm_rmd_price cons_price,rm_rmm_cos_acc_code, sales_stn_station_code";

                SQL = SQL + " FROM pr_dn_mix_details, PR_DELIVERY_NOTE, rm_rawmaterial_master, rm_uom_master uom,";
                SQL = SQL + "  rm_uom_master uomd";

                SQL = SQL + " WHERE pr_dn_mix_details.prod_dlyn_dn_no = PR_DELIVERY_NOTE.pr_dlyn_dn_no";
                SQL = SQL + " AND pr_dn_mix_details.ad_fin_finyrid = PR_DELIVERY_NOTE.ad_fin_finyrid";
                SQL = SQL + " AND pr_dn_mix_details.rm_rmm_rm_code = rm_rawmaterial_master.rm_rmm_rm_code";
                SQL = SQL + "  AND pr_dn_mix_details.rm_um_uom_code = uom.rm_um_uom_code";
                SQL = SQL + "  AND pr_dn_mix_details.tech_pcd_uom_code = uomd.rm_um_uom_code";
                SQL = SQL + " AND PR_DELIVERY_NOTE.pr_dlyn_dn_date BETWEEN ";
                SQL = SQL + " '" + ldFromDate.ToString("dd-MMM-yyyy") + "' AND '" + ldToDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND sales_stn_station_code = '" + lsStnCode + "'";
                SQL = SQL + " AND PR_DELIVERY_NOTE.PR_CONRTN_DNCANCEL='N'";
                SQL = SQL + "     AND pr_delivery_note.PR_DLYN_CONC_MIXED = 'Y' ";
                SQL = SQL + " GROUP BY pr_dn_mix_details.tech_pm_product_code, pr_dn_mix_details.rm_rmm_rm_code,";
                SQL = SQL + " rm_rmm_rm_description, tech_pcd_uom_code, pr_dn_mix_details.rm_um_uom_code,";
                SQL = SQL + " uom.rm_um_uom_desc, uomd.rm_um_uom_desc, rm_rmm_inv_acc_code, rm_rmm_cos_acc_code,";
                SQL = SQL + "  sales_stn_station_code, rm_rmd_price";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchPlantComposition(string lsStnCode, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "  SELECT RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPRM_ENTRY_NO, " ;
                SQL = SQL  +  "    RM_PHYSICAL_STOCK_PROJ_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL  + "    RM_PHYSICAL_STOCK_PROJ_MASTER.SALES_STN_STATION_CODE, ";
                SQL = SQL  + "    RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE,    ";
                SQL = SQL  + "    SUM( RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_CONSUM_THEORITICAL) THEO_QTY , ";
                SQL = SQL  + "    SUM( RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_CONSUMPTION)CONS_QTY ";
                SQL = SQL  + "FROM  ";
                SQL = SQL  + "    RM_PHYSICAL_STOCK_PROJ_MASTER, RM_PHYSICAL_STOCK_PROJ_DETAILS ";
                SQL = SQL  + "WHERE   ";
                SQL = SQL  + "    RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPRM_ENTRY_NO =RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPRM_ENTRY_NO ";
                SQL = SQL  + " AND  RM_PHYSICAL_STOCK_PROJ_MASTER.AD_FIN_FINYRID =RM_PHYSICAL_STOCK_PROJ_DETAILS.AD_FIN_FINYRID ";

                SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPR_ENTRY_DATE BETWEEN  ";
                SQL = SQL + " '" + ldFromDate.ToString("dd-MMM-yyyy") + "' AND '" + ldToDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_MASTER.sales_stn_station_code = '" + lsStnCode + "' ";
                SQL = SQL + " AND  RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPR_APPROVED ='Y' ";

                SQL = SQL  + " GROUP BY RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPRM_ENTRY_NO, ";
                SQL = SQL  + "RM_PHYSICAL_STOCK_PROJ_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL  + "RM_PHYSICAL_STOCK_PROJ_MASTER.SALES_STN_STATION_CODE, ";
                SQL = SQL + "RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE ";


                  

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

        #region " otherfuncion "
        public DataSet FetchStation(string stcode)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT SALES_STN_STATION_CODE CODE,SALES_STN_STATION_NAME NAME ,GL_COSM_ACCOUNT_CODE COSMACCODE";
                SQL = SQL + " FROM SL_STATION_MASTER WHERE SALES_STN_DELETESTATUS=0 ";
                SQL = SQL + " AND SALES_STN_STATION_STATUS='A' ";
                

                if (!string.IsNullOrEmpty(stcode))
                {
                    SQL = SQL + " AND SALES_STN_STATION_CODE='" +stcode+ "' ";
                }

                SQL = SQL + " ORDER BY SALES_STN_STATION_NAME ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public string FnValidate(string dtpFromDate, string dtpToDate)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT COUNT(*)";
                SQL = SQL + " FROM RM_SALES_MASTER WHERE RM_MSM_APPROVED ='N'";
                SQL = SQL + " AND TO_DATE(TO_CHAR(RM_MSM_ENTRY_DATE,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);

                if (Convert.ToInt32(sReturn.Tables[0].Rows[0][0]) > 0)
                {
                    return "Unapproved raw material sales entry exists !! Unable to to continue..";
                }

                sReturn = null;

                SQL = " SELECT COUNT(*)";
                SQL = SQL + " FROM RM_RECEIPT_APPL_MASTER WHERE RM_MRMA_APPROVED_STATUS ='N'";
                SQL = SQL + " AND TO_DATE(TO_CHAR( RM_MRMA_APPROVALTRANS_DATE,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);

                if (Convert.ToInt32(sReturn.Tables[0].Rows[0][0]) > 0)
                {
                    return "Unapproved raw material approval entry exists !! Unable to to continue..";
                }

                sReturn = null;

             //   SQL = " SELECT count(*) FROM rm_mat_stk_transfer_master m, rm_mat_stk_transfer_detl d ";
             //   SQL = SQL + "WHERE m.rm_mstm_transfer_no = d.rm_mstm_transfer_no AND m.ad_fin_finyrid = d.ad_fin_finyrid and ";
             ////   SQL = SQL + "d.RM_MSTD_REC_FLG ='N' ";
             //   SQL = SQL + "AND TO_DATE(TO_CHAR( m.RM_MSTM_TRANSFER_DATE ,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";


                SQL = " SELECT count(*) FROM rm_mat_stk_transfer_master m  ";
                SQL = SQL + "WHERE  RM_MSTM_APPRV_STATUS <>'Y' ";
                 
                SQL = SQL + "AND TO_DATE(TO_CHAR( m.RM_MSTM_TRANSFER_DATE ,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";


                sReturn = clsSQLHelper.GetDataset(SQL);

                if (Convert.ToInt32(sReturn.Tables[0].Rows[0][0]) > 0)
                {
                    return "Unapproved raw material approval entry exists !! Unable to to continue..";
                }


                SQL = " SELECT COUNT(*)";
                SQL = SQL + "    from RM_PHYSICAL_STOCK_MASTER   ";
                SQL = SQL + "  where   RM_PSEM_APPROVED ='N' AND TO_DATE(TO_CHAR(RM_PSEM_FROM_DATE,'DD-MON-YYYY'))  < '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "'";


                sReturn = clsSQLHelper.GetDataset(SQL);

                if (Convert.ToInt32(sReturn.Tables[0].Rows[0][0]) > 0)
                {
                    return "Unapproved raw material  physical stock entry  entry exists  ( less then entry from date )   !! Unable to to continue..";
                }


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return "CONTINUE";
        }

        public DataSet fnConversion()
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                //SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT rm_um_uom_code_from, rm_um_uom_code_to, rm_ucm_factor, rm_rmm_rm_code";
                SQL = SQL + " FROM rm_uom_conversion";
                SQL = SQL + " WHERE rm_ucm_conversion_id = (SELECT MAX (rm_ucm_conversion_id)";
                SQL = SQL + " FROM rm_uom_conversion";
                SQL = SQL + " WHERE rm_ucm_deletestatus = 0)";
                SQL = SQL + " AND rm_ucm_deletestatus = 0";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }



        public DataSet FetchEntryDateDetails(string sStationCode, object mngrclassobj)
        {
            DataSet dsReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = "   select RM_PSEM_FROM_DATE,RM_PSEM_TO_DATE, SALES_STN_STATION_CODE from RM_PHYSICAL_STOCK_MASTER where  SALES_STN_STATION_CODE =  '" + sStationCode + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + "";


                dsReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dsReturn;
        }


        #endregion

        #region "DML"

        public string InsertMasterSql(PhysicalStockEntryPRCEntity oEntity, List<PhysicalStockEntryPRCDetails> EntityDetails, bool Autogen, object mngrclassobj, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " INSERT INTO RM_PHYSICAL_STOCK_MASTER (";
                SQL = SQL + " RM_PSEM_ENTRY_NO, RM_PSEM_ENTRY_DATE, RM_PSEM_FROM_DATE, ";
                SQL = SQL + " RM_PSEM_TO_DATE, RM_PSEM_INTERVAL_DAYS, RM_PSEM_REMARKS, ";
                SQL = SQL + " RM_PSEM_DOC_STATUS, AD_CM_COMPANY_CODE, ";
                SQL = SQL + " AD_FIN_FINYRID, RM_PSEM_CREATEDBY, RM_PSEM_CREATEDDATE, ";
                SQL = SQL + " RM_PSEM_DELETESTATUS, ";
                SQL = SQL + " SALES_STN_STATION_CODE, HR_DEPT_DEPT_CODE, RM_PSEM_APPROVED, ";
                SQL = SQL + " RM_PSEM_APPROVEDBY ,RM_PSEM_VERIFIED, RM_PSEM_VERIFIEDBY, ";
                SQL = SQL + " GL_COSM_ACCOUNT_CODE, ";
                SQL = SQL + " RM_PSPRM_ENTRY_NO,RM_PSPRM_AD_FINYRID  ,    RM_PSEPM_ENTRY_NO ,  RM_PSPEM_AD_FINYRID  , AD_BR_CODE   ";
                SQL = SQL + " ) ";

                SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "','" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "' ,'" + Convert.ToDateTime(oEntity.dtpFromDate).ToString("dd-MMM-yyyy") + "' ,";
                SQL = SQL + " '" + Convert.ToDateTime(oEntity.dtpToDate).ToString("dd-MMM-yyyy") + "' , " + Convert.ToInt32(oEntity.txtNoOfDays) + ",'" + oEntity.txtRemarks + "' ,";
                SQL = SQL + " '' , '" + mngrclass.CompanyCode + "' ,";
                SQL = SQL + " " + mngrclass.FinYearID + " ,'" + mngrclass.UserName + "' , TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " 0,'" + oEntity.ddlStation + "', ' ' ,'" + oEntity.approve + "' ,";
                SQL = SQL + "'" + oEntity.txtApprovedBy + "',";
                SQL = SQL + "'" + oEntity.varify + "' ,'" + oEntity.txtVerifiedBy + "',";
                SQL = SQL + "'" + oEntity.CostCenter + "',";
                SQL = SQL + "'" + oEntity.ActualPltEntryNo + "'," + oEntity.ActualPltEntryFinid + ",'" + oEntity.ActualPltEntryNo + "'," + oEntity.ActualPltEntryFinid + ",'" + mngrclass.BranchCode  + "'";

                SQL = SQL + ")";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertDetails(oEntity, EntityDetails, mngrclassobj);
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

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTPSE", oEntity.txtEntryNo, Autogen, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, mngrclass.BranchCode, sAtuoGenBranchDocNumber);





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

        private string InsertApprovalQrs(PhysicalStockEntryPRCEntity oEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " INSERT INTO rM_PHYSICAL_STOCK_TRIGGER (";
                SQL = SQL + " RM_PSEM_ENTRY_NO, RM_PSEM_ENTRY_DATE, AD_FIN_FINYRID, ";
                SQL = SQL + " RM_PSEM_APPROVEDBY) ";
                SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "' ,'" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "' ," + mngrclass.FinYearID + " ,";
                SQL = SQL + " '" + oEntity.txtApprovedBy + "' ";
                SQL = SQL + " )";

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

        private string InsertDetails(PhysicalStockEntryPRCEntity oEntity, List<PhysicalStockEntryPRCDetails> EntityDetails, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                int liSlNo = 0;
                {
                    foreach (var Data in EntityDetails)
                    {
                        liSlNo = liSlNo + 1;
                        lobjFactor = 1;

                        // fsStkAdjDesc = "Against Inventory A/c Of RM :" & CStr(lobjRM_Code) & ""
                        fsInvDesc = "Against Consumption A/c Of RM :" + Convert.ToString(Data.lobjRM_CodeDetails) + "";

                        SQL = "";
                        SQL = " INSERT INTO RM_PHYSICAL_STOCK_DETAILS (";
                        SQL = SQL + " RM_PSEM_ENTRY_NO, RM_PSED_SL_NO, RM_RMM_RM_CODE, ";
                        SQL = SQL + " RM_UM_UOM_CODE_MEASURED, RM_PSED_FACTOR, RM_UM_UOM_CODE_BASIC, ";
                        SQL = SQL + " AD_CM_COMPANY_CODE, AD_FIN_FINYRID, RM_PSED_DELETESTATUS, ";
                        SQL = SQL + " RM_PSED_STOCK_ON_HAND, RM_PSED_OPEN_RATE, RM_PSED_OPEN_AMOUNT, ";
                        SQL = SQL + " RM_PSED_RVD_QTY,RM_PSED_RVD_RATE, RM_PSED_RVD_AMOUNT, ";
                        SQL = SQL + " RM_PSED_STKTRN_IN_QTY, RM_PSED_STKTRN_IN_RATE, RM_PSED_STKTRN_IN_AMOUNT, ";
                        SQL = SQL + " RM_PSED_STKTRN_OUT_QTY, RM_PSED_STKTRN_OUT_RATE, RM_PSED_STKTRN_OUT_AMOUNT,";
                        SQL = SQL + " RM_PSED_SALES_QTY, RM_PSED_SALES_RATE, RM_PSED_SALES_AMOUNT, ";
                        SQL = SQL + " RM_PSED_RECEIVED, RM_PSED_RECEIVED_AMOUNT, RM_PSED_AVG_PRICE, RM_PSED_AVG_PRICE_NEW, ";
                        SQL = SQL + " RM_PSED_CONSUM_THEORITICAL, RM_PSED_ACTUALQTY_FROM_PLANT, RM_PSED_CONSUMPTION, ";
                        SQL = SQL + " RM_PSED_CONSUMPTION_AMOUNT, RM_PSED_CLOSING, RM_PSED_CLOSING_AMOUNT, ";
                        SQL = SQL + " RM_PSED_VARIANCE, RM_PSED_VARIANCEAMOUNT) ";

                        SQL = SQL + " VALUES ( '" + oEntity.txtEntryNo + "', " + liSlNo + ",'" + Data.lobjRM_CodeDetails + "' ,";
                        SQL = SQL + " '" + Data.lobjNewUnitDetails + "' , " + lobjFactor + " ,'" + Data.lobjBasicUomDetails + "',";
                        SQL = SQL + " '" + mngrclass.CompanyCode + "'," + mngrclass.FinYearID + ",0,";
                        SQL = SQL + " " + Data.lobjRMOpeningDetails + " ," + Data.lobjRMOpenRateDetails + "," + Data.dOpenAmountDetails + ",";
                        SQL = SQL + " " + Data.dRVDQtyDetails + " ," + Data.dRVDRateDetails + "," + Data.dRVDamountDetails + ",";
                        SQL = SQL + " " + Data.dStkINQtyDetails + " ," + Data.dStkINRateDetails + "," + Data.dStkINamountDetails + ",";
                        SQL = SQL + " " + Data.dStkOUTQtyDetails + " ," + Data.dStkOUTRateDetails + "," + Data.dStkOUTamountDetails + ",";
                        SQL = SQL + " " + Data.dSalesQtyDetails + " ," + Data.dSalesRateDetails + "," + Data.dSalesamountDetails + ",";
                        SQL = SQL + " " + Data.lobjRMReceivedDetails + "," + Data.lobjRMReceived_AMOUNTDetails + ", " + Data.lobjRMAVGRateDetails + ", " + Data.lobjRMRate_NewDetails + ",";
                        SQL = SQL + " " + Data.lobjRMCons_TheoriticalDetails + "," + Data.lobjActualQtyFrmPlantDetails + ", " + Data.lobjRMConsumptionDetails + ",";
                        SQL = SQL + " " + Data.lobjRMConsumption_AmountDetails + "," + Data.lobjRMClosingDetails + "," + Data.lobjRMClosing_AmountDetails + ",";
                        SQL = SQL + " " + Data.lobjRMVarianceQtyDetails + "," + Data.lobjRMVarianceAmtDetails + "";
                        SQL = SQL + " ) ";

                        sSQLArray.Add(SQL);
                    }
                }
            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
                return sRetun;
            }
            return "CONTINUE";
        }

        public string ModifySql(PhysicalStockEntryPRCEntity oEntity, List<PhysicalStockEntryPRCDetails> EntityDetails, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " update RM_PHYSICAL_STOCK_MASTER set ";
                SQL = SQL + " RM_PSEM_ENTRY_DATE ='" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "' , RM_PSEM_FROM_DATE ='" + Convert.ToDateTime(oEntity.dtpFromDate).ToString("dd-MMM-yyyy") + "', ";
                SQL = SQL + " RM_PSEM_TO_DATE ='" + Convert.ToDateTime(oEntity.dtpToDate).ToString("dd-MMM-yyyy") + "' , RM_PSEM_INTERVAL_DAYs =" + Convert.ToInt32(oEntity.txtNoOfDays) + ", RM_PSEM_REMARKS ='" + oEntity.txtRemarks + "' , ";
                SQL = SQL + " RM_PSEM_UPDATEDBY = '" + mngrclass.UserName + "' , RM_PSEM_UPDATEDDATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " RM_PSEM_APPROVED = '" + oEntity.approve + "' , ";
                SQL = SQL + " RM_PSEM_APPROVEDBY ='" + oEntity.txtApprovedBy + "',";
                SQL = SQL + " RM_PSEM_VERIFIED= '" + oEntity.varify + "',RM_PSEM_VERIFIEDBY ='" + oEntity.txtVerifiedBy + "',GL_COSM_ACCOUNT_CODE='" + oEntity.CostCenter + "', ";
                SQL = SQL + " RM_PSPRM_ENTRY_NO='" + oEntity.ActualPltEntryNo + "',RM_PSPRM_AD_FINYRID=" + oEntity.ActualPltEntryFinid + " ";
                SQL = SQL + " where RM_PSEM_ENTRY_NO = '" + oEntity.txtEntryNo + "' and AD_FIN_FINYRID =" + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);

                SQL = "Delete From RM_PHYSICAL_STOCK_DETAILS WHERE RM_PSEM_ENTRY_NO = '" + oEntity.txtEntryNo + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);



                sRetun = string.Empty;

                sRetun = InsertDetails(oEntity, EntityDetails, mngrclassobj);

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

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTPSE", oEntity.txtEntryNo, false, Environment.MachineName, "U", sSQLArray);

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

        public string DeleteSql(string txtEntryNo, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " DELETE FROM RM_PHYSICAL_STOCK_MASTER WHERE RM_PSEM_ENTRY_NO = '" + txtEntryNo + "' and AD_FIN_FINYRID =" + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                SQL = "Delete From RM_PHYSICAL_STOCK_DETAILS WHERE RM_PSEM_ENTRY_NO = '" + txtEntryNo + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTPSE", txtEntryNo, false, Environment.MachineName, "D", sSQLArray);
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

        public string DeleteRowSql(string sEntryNo, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();



                sRetun = string.Empty;

                SQL = "Delete From RM_PHYSICAL_STOCK_DETAILS WHERE RM_PSEM_ENTRY_NO = '" + sEntryNo + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";
                SQL = SQL + "    AND  RM_PSED_STOCK_ON_HAND =0 AND RM_PSED_OPEN_AMOUNT =0  AND  ";
                SQL = SQL + "     RM_PSED_RVD_QTY =0 AND   RM_PSED_RVD_AMOUNT= 0 AND  ";
                SQL = SQL + "    RM_PSED_STKTRN_IN_QTY =0  AND   RM_PSED_STKTRN_IN_AMOUNT= 0 AND   ";
                SQL = SQL + "    RM_PSED_STKTRN_OUT_QTY =0 AND    RM_PSED_STKTRN_OUT_AMOUNT= 0 AND   ";
                SQL = SQL + "    RM_PSED_SALES_QTY =0 AND   RM_PSED_SALES_AMOUNT =0  AND   ";
                SQL = SQL + "    RM_PSED_RECEIVED =0 AND  RM_PSED_RECEIVED_AMOUNT =0 AND   ";
                SQL = SQL + "    RM_PSED_CONSUM_THEORITICAL =0 AND RM_PSED_ACTUALQTY_FROM_PLANT= 0 AND   ";
                SQL = SQL + "    RM_PSED_CONSUMPTION =0 AND  RM_PSED_CONSUMPTION_AMOUNT =0 AND  RM_PSED_CLOSING =0 AND   ";
                SQL = SQL + "     RM_PSED_CLOSING_AMOUNT = 0 AND  RM_PSED_VARIANCE = 0 AND  RM_PSED_VARIANCEAMOUNT =0  ";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTPSE", sEntryNo, false, Environment.MachineName, "U", sSQLArray);





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

        #region "Fetch Data"
        public DataSet FnFillGrid()
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + " RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RMM_RM_DESCRIPTION,RM_RMM_RM_TYPE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC,";
                SQL = SQL + " RM_RMM_INV_ACC_CODE , RM_RMM_CONS_ACC_CODE ";
                SQL = SQL + " FROM ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,RM_UOM_MASTER";
                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        public DataSet fnCountStkNo(string dtpToDate, string ddlstation)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " select count(*) StkNo from RM_PHYSICAL_STOCK_MASTER where RM_PSEM_TO_DATE > " + "'" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "";
                SQL = SQL + "' and SALES_STN_STATION_CODE ='" + ddlstation + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

    

        public DataSet FetchPhysicalStockEntryDetails(string EntryNo, string FinId)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + " RM_PSEM_ENTRY_NO, RM_PSEM_ENTRY_DATE, RM_PSEM_FROM_DATE, ";
                SQL = SQL + " RM_PSEM_TO_DATE, RM_PSEM_INTERVAL_DAYS, SALES_STN_STATION_CODE, ";
                SQL = SQL + " HR_DEPT_DEPT_CODE, RM_PSEM_REMARKS, RM_PSEM_DOC_STATUS, ";
                SQL = SQL + " AD_CM_COMPANY_CODE, AD_FIN_FINYRID, RM_PSEM_CREATEDBY, ";
                SQL = SQL + " RM_PSEM_CREATEDDATE, RM_PSEM_UPDATEDBY, RM_PSEM_UPDATEDDATE, ";
                SQL = SQL + " RM_PSEM_DELETESTATUS, RM_PSEM_APPROVED, RM_PSEM_APPROVEDBY ,";
                SQL = SQL + " RM_PSEM_VERIFIED, RM_PSEM_VERIFIEDBY,GL_COSM_ACCOUNT_CODE,RM_PSPRM_ENTRY_NO ,RM_PSPRM_AD_FINYRID ";
                SQL = SQL + " FROM RM_PHYSICAL_STOCK_MASTER ";

                SQL = SQL + " WHERE RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO='" + EntryNo + "'";
                SQL = SQL + " And RM_PHYSICAL_STOCK_MASTER.AD_FIN_FINYRID=" + FinId + " ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchDetails(string EntryNo, string FinId)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " select ";
                SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS.RM_PSED_SL_NO Sl_No,";
                SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE RM_Code,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_Description,RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE,";
                SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS.RM_UM_UOM_CODE_MEASURED,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC,";
                SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS.RM_PSED_FACTOR,";
                SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS.RM_UM_UOM_CODE_BASIC,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_INV_ACC_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_CONS_ACC_CODE RM_RMM_COS_ACC_CODE,";
                SQL = SQL + " RM_PSED_STOCK_ON_HAND, RM_PSED_OPEN_RATE,RM_PSED_OPEN_AMOUNT, RM_PSED_RVD_QTY, ";
                SQL = SQL + " RM_PSED_RVD_RATE, RM_PSED_RVD_AMOUNT, RM_PSED_STKTRN_IN_QTY, ";
                SQL = SQL + " RM_PSED_STKTRN_IN_RATE, RM_PSED_STKTRN_IN_AMOUNT, RM_PSED_STKTRN_OUT_QTY, ";
                SQL = SQL + " RM_PSED_STKTRN_OUT_RATE, RM_PSED_STKTRN_OUT_AMOUNT, ";
                SQL = SQL + " RM_PSED_SALES_QTY,RM_PSED_SALES_RATE, RM_PSED_SALES_AMOUNT, ";
                SQL = SQL + " RM_PSED_RECEIVED, RM_PSED_AVG_PRICE, RM_PSED_RECEIVED_AMOUNT, RM_PSED_AVG_PRICE_NEW, ";
                SQL = SQL + " RM_PSED_CONSUM_THEORITICAL, RM_PSED_ACTUALQTY_FROM_PLANT, RM_PSED_CONSUMPTION, ";
                SQL = SQL + " RM_PSED_CONSUMPTION_AMOUNT, RM_PSED_CLOSING, RM_PSED_CLOSING_AMOUNT, ";
                SQL = SQL + " RM_PSED_VARIANCE, RM_PSED_VARIANCEAMOUNT ";

                SQL = SQL + " from ";
                SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS, RM_UOM_MASTER, RM_RAWMATERIAL_MASTER";

                SQL = SQL + " where ";
                SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS.RM_UM_UOM_CODE_MEASURED = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " And RM_PHYSICAL_STOCK_DETAILS.RM_PSEM_ENTRY_NO='" + EntryNo + "'";
                SQL = SQL + " And RM_PHYSICAL_STOCK_DETAILS.AD_FIN_FINYRID=" + FinId + " ";
                SQL = SQL + " ORDER BY RM_PHYSICAL_STOCK_DETAILS.RM_PSED_SL_NO";


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

        public DataSet FetchPrintData(string txtEntryNo, string iDocFinYear)
        {
            DataSet dsData = new DataSet("RMPHYSTOCKPRINT");

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "  ";

                SQL = " SELECT  ";
                SQL = SQL + "   RM_PSEM_ENTRY_NO, AD_FIN_FINYRID, RM_PSEM_ENTRY_DATE, RM_PSEM_FROM_DATE, RM_PSEM_TO_DATE,  ";
                SQL = SQL + "   RM_PSEM_INTERVAL_DAYS, RM_PSEM_CREATEDBY, RM_PSEM_VERIFIEDBY, RM_PSEM_APPROVED, RM_PSEM_APPROVEDBY,  ";
                SQL = SQL + "   RM_PSEM_REMARKS, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, RM_PSED_SL_NO, RM_RMM_RM_CODE,  ";
                SQL = SQL + "   RM_RMM_RM_DESCRIPTION,RM_RMM_RM_TYPE , RM_UM_UOM_CODE_MEASURED, RM_PSED_STOCK_ON_HAND, RM_PSED_OPEN_RATE, RM_PSED_OPEN_AMOUNT,  ";
                SQL = SQL + "   RM_PSED_RVD_QTY, RM_PSED_RVD_RATE, RM_PSED_RVD_AMOUNT, RM_PSED_STKTRN_IN_QTY, RM_PSED_STKTRN_IN_RATE,  ";
                SQL = SQL + "   RM_PSED_STKTRN_IN_AMOUNT, RM_PSED_STKTRN_OUT_QTY, RM_PSED_STKTRN_OUT_RATE, RM_PSED_STKTRN_OUT_AMOUNT, RM_PSED_SALES_QTY,  ";
                SQL = SQL + "   RM_PSED_SALES_RATE, RM_PSED_SALES_AMOUNT, RM_PSED_RECEIVED, RM_PSED_RECEIVED_AMOUNT, RM_PSED_AVG_PRICE,  ";
                SQL = SQL + "   RM_PSED_AVG_PRICE_NEW, RM_PSED_CONSUM_THEORITICAL, RM_PSED_ACTUALQTY_FROM_PLANT, RM_PSED_CONSUMPTION, RM_PSED_CONSUMPTION_AMOUNT,  ";
                SQL = SQL + "   RM_PSED_CLOSING, RM_PSED_CLOSING_AMOUNT, RM_PSED_VARIANCE, RM_PSED_VARIANCEAMOUNT  ";

                SQL = SQL + "  FROM  ";
                SQL = SQL + "     RM_PHYSTK_ENTRY_PRC_PRINT_VW  ";
                SQL = SQL + "  WHERE  ";
                SQL = SQL + "     RM_PSEM_ENTRY_NO ='" + txtEntryNo + "'AND AD_FIN_FINYRID=" + iDocFinYear + "";

                dsData = clsSQLHelper.GetDataset(SQL);
                dsData.Tables[0].TableName = "RM_PHYSTK_ENTRY_PRINT_VW";

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

        #endregion

    }



    ////=====================================ENITTY ================================================= DO NOT WRITE TEH CODE BELOW STATEMENT ============================ JOMY 

    #region "enity"

    public class PhysicalStockEntryPRCEntity
    {
        public string txtEntryNo
        {
            get;
            set;
        }

        public string txtNoOfDays
        {
            get;
            set;
        }

        public string txtRemarks
        {
            get;
            set;
        }

        public string ddlStation
        {
            get;
            set;
        }

        public string CostCenter
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

        public DateTime dtpEntryDate
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
        public string ActualPltEntryNo
        {
            get;
            set;
        }
        public string ActualPltEntryFinid
        {
            get;
            set;
        }
  
    }

    public class PhysicalStockEntryPRCDetails
    {
        public object lobjRM_CodeDetails { get; set; }
        public object lobjNewUnitDetails { get; set; }
        public object lobjRMInvAccountDetails { get; set; }
        public object lobjRMConsAccountDetails { get; set; }
        public object lobjBasicUomDetails { get; set; }
        public double lobjRMOpeningDetails { get; set; }
        public double lobjRMOpenRateDetails { get; set; }
        public double dOpenAmountDetails { get; set; }
        public double dRVDQtyDetails { get; set; }
        public double dRVDRateDetails { get; set; }
        public double dRVDamountDetails { get; set; }
        public double dStkINQtyDetails { get; set; }
        public double dStkINRateDetails { get; set; }
        public double dStkINamountDetails { get; set; }
        public double dStkOUTQtyDetails { get; set; }
        public double dStkOUTRateDetails { get; set; }
        public double dStkOUTamountDetails { get; set; }
        public double dSalesQtyDetails { get; set; }
        public double dSalesRateDetails { get; set; }
        public double dSalesamountDetails { get; set; }
        public double lobjRMReceivedDetails { get; set; }
        public double lobjRMAVGRateDetails { get; set; }
        public double lobjRMReceived_AMOUNTDetails { get; set; }
        public double lobjRMRate_NewDetails { get; set; }
        public double lobjRMCons_TheoriticalDetails { get; set; }
        public double lobjRMConsumptionDetails { get; set; }
        public double lobjRMConsumption_AmountDetails { get; set; }
        public double lobjActualQtyFrmPlantDetails { get; set; }
        public double lobjRMClosingDetails { get; set; }
        public double lobjRMClosing_AmountDetails { get; set; }
        public double lobjRMVarianceQtyDetails { get; set; }
        public double lobjRMVarianceAmtDetails { get; set; }
    }

#endregion 
    //===========================================END OF ENITY
}
