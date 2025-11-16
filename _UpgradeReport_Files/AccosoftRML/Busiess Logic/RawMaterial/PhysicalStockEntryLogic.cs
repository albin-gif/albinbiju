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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Drawing;

/// <summary>
/// Created By      :   Jins Mathew
/// Created On      :   22-Dec-2012
/// </summary>

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class PhysicalStockEntryLogic
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        string fsInvDesc = null;
        double lobjFactor = 0;

        System.DateTime dtTrDate = default(System.DateTime);

        //static DataSet dsComposistion=new DataSet();


        #region "look up " 

        public DataTable FillSearchView ( string sFilterType, object mngrclassobj )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT ";
                SQL = SQL + " RM_PSEM_ENTRY_NO ENTRYNO, AD_FIN_FINYRID FINID,  to_char(RM_PSEM_ENTRY_DATE,'DD-MON-YYYY') ENTRYDATE, ";
                SQL = SQL + " RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE STCODE,SALES_STN_STATION_NAME STNAME,";
                SQL = SQL + " to_char(RM_PSEM_FROM_DATE,'DD-MON-YYYY') FROMDATE, ";
                SQL = SQL + "    to_char(RM_PSEM_TO_DATE,'DD-MON-YYYY') TODATE,   ";
                SQL = SQL + "    RM_PSEM_REMARKS REMARKS ,RM_PSEM_APPROVED APPROVED,RM_PSEM_VERIFIED VERIFIED ,";
                SQL = SQL + " RM_PHYSICAL_STOCK_MASTER.AD_BR_CODE,AD_BR_NAME_ALIAS AD_BR_NAME  ";

                SQL = SQL + " FROM  RM_PHYSICAL_STOCK_MASTER,SL_STATION_BRANCH_MAPP_DTLS_VW  ";

                SQL = SQL + "    WHERE  ";
                SQL = SQL + "    RM_PHYSICAL_STOCK_MASTER.AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";
                SQL = SQL + "  AND RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE=SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE (+)";
                SQL = SQL + "  AND RM_PHYSICAL_STOCK_MASTER.AD_BR_CODE=SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE (+)";

                if (sFilterType == "APPROVED")
                {
                    SQL = SQL + "  And RM_PSEM_APPROVED ='Y'";

                }
                else if (sFilterType == "NOT APPROVED")
                {
                    SQL = SQL + "  And RM_PSEM_APPROVED ='N'";
                }

                if (mngrclass.UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                    SQL = SQL + "  and     RM_PHYSICAL_STOCK_MASTER.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
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




        public DataTable FillSearchRefEntryNo ( DateTime FromDate, DateTime ToDate, object mngrclassobj )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "   SELECT  ";
                SQL = SQL + " RM_PSEM_ENTRY_NO ENTRYNO,    to_char(RM_PSEM_ENTRY_DATE,'DD-MON-YYYY') ENTRYDATE, ";
                SQL = SQL + " RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE STCODE,SALES_STN_STATION_NAME STNAME,  to_char(RM_PSEM_FROM_DATE,'DD-MON-YYYY') FROMDATE,  ";
                SQL = SQL + "    to_char(RM_PSEM_TO_DATE,'DD-MON-YYYY') TODATE,    ";
                SQL = SQL + "    RM_PSEM_APPROVED APPROVED  ,RM_PHYSICAL_STOCK_MASTER.AD_BR_CODE ,AD_BR_NAME";

                SQL = SQL + " FROM  RM_PHYSICAL_STOCK_MASTER,SL_STATION_MASTER ,AD_BRANCH_MASTER ";

                SQL = SQL + "    WHERE   ";
                SQL = SQL + "    RM_PHYSICAL_STOCK_MASTER.AD_FIN_FINYRID=" + mngrclass.FinYearID + "  ";
                SQL = SQL + "    AND SL_STATION_MASTER.SALES_STN_STATION_CODE=RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE  ";
                SQL = SQL + "    AND RM_PHYSICAL_STOCK_MASTER.AD_BR_CODE=AD_BRANCH_MASTER.AD_BR_CODE(+)  ";
                SQL = SQL + "    AND RM_PSEM_ENTRY_DATE BETWEEN '" + FromDate.ToString("dd-MMM-yyyy") + "' AND '" + ToDate.ToString("dd-MMM-yyyy") + "' ";
                SQL = SQL + "    and RM_PSEM_APPROVED ='Y' ";

                if (mngrclass.UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                    SQL = SQL + "  and     RM_PHYSICAL_STOCK_MASTER.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
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
        public DataSet FetchAvgPriceRMDtls ( string lsStnCode, string IsBranch, string dtpToDate, object mngrclassobj )
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
                SQL = SQL + "  SELECT rm_rmm_rm_code rm_code, sales_stn_station_code stn_code,  AD_BR_CODE , ";
                SQL = SQL + " SUM (rm_ob_qty)";
                SQL = SQL + " ob_qty, SUM ( RM_OB_AMOUNT ) ob_val";
                SQL = SQL + " FROM RM_OPEN_BALANCES";
                SQL = SQL + " WHERE ad_fin_finyrid = " + mngrclass.FinYearID + " ";
                SQL = SQL + " AND sales_stn_station_code = '" + lsStnCode + "'";
                SQL = SQL + " AND AD_BR_CODE = '" + IsBranch + "'";
                SQL = SQL + " GROUP BY rm_rmm_rm_code, sales_stn_station_code ,AD_BR_CODE  ) opening,";
                SQL = SQL + " ( ";
                SQL = SQL + "  SELECT RM_IM_ITEM_CODE rm_code, sales_stn_station_code stn_code,AD_BR_CODE,";
                SQL = SQL + " SUM (NVL (RM_STKL_RECD_QTY, 0))";
                SQL = SQL + " rec_qty,";
                SQL = SQL + " SUM (NVL (RM_STKL_RECD_AMT, 0) ) rec_value";
                SQL = SQL + " FROM RM_STOCK_LEDGER";
                SQL = SQL + " WHERE TO_DATE (TO_CHAR (rm_stkl_trans_date, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND ad_fin_finyrid =" + mngrclass.FinYearID + "";
                SQL = SQL + " AND sales_stn_station_code = '" + lsStnCode + "'";
                SQL = SQL + " AND AD_BR_CODE = '" + IsBranch + "'";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE , sales_stn_station_code ,AD_BR_CODE ";
                SQL = SQL + "  ) recd, ";
                SQL = SQL + " ( ";
                SQL = SQL + "  SELECT RM_IM_ITEM_CODE rm_code, sales_stn_station_code stn_code,AD_BR_CODE ,";
                SQL = SQL + " SUM (NVL (RM_STKL_ISSUE_QTY, 0))";
                SQL = SQL + " iss_qty,";
                SQL = SQL + " SUM ( NVL (RM_STKL_ISSUE_AMOUNT, 0) ) iss_value";
                SQL = SQL + " FROM RM_STOCK_LEDGER";
                SQL = SQL + " WHERE TO_DATE (TO_CHAR (rm_stkl_trans_date, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND ad_fin_finyrid = " + mngrclass.FinYearID + "";
                SQL = SQL + " AND sales_stn_station_code  = '" + lsStnCode + "'";
                SQL = SQL + " AND AD_BR_CODE = '" + IsBranch + "'";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE , sales_stn_station_code ,AD_BR_CODE ";
                SQL = SQL + "  ) issued";
                SQL = SQL + " WHERE ";
                SQL = SQL + " ";
                SQL = SQL + " mas.rm_rmm_rm_code = det.rm_rmm_rm_code";
                SQL = SQL + " AND det.rm_rmm_rm_code = opening.rm_code(+)";
                SQL = SQL + " AND det.sales_stn_station_code = opening.stn_code(+)";
                SQL = SQL + "  AND det.AD_BR_CODE  = opening.AD_BR_CODE(+) ";

                SQL = SQL + " AND det.rm_rmm_rm_code = recd.rm_code(+)";
                SQL = SQL + " AND det.sales_stn_station_code = recd.stn_code(+)";
                SQL = SQL + "  AND det.AD_BR_CODE  = recd.AD_BR_CODE(+) ";

                SQL = SQL + " AND det.rm_rmm_rm_code = issued.rm_code(+)";
                SQL = SQL + " AND det.sales_stn_station_code = issued.stn_code(+)";
                SQL = SQL + "  AND det.AD_BR_CODE  = recd.AD_BR_CODE(+) ";

                SQL = SQL + " AND det.sales_stn_station_code = '" + lsStnCode + "'";
                SQL = SQL + " AND det.AD_BR_CODE = '" + IsBranch + "'";
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


        #region " Validate missing  uom jomy 08 feb 2023#

        public string FnValidateMissingMixdetails ( string dtpFromDate, string dtpToDate, string sStationCode )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                double dDnCount = 0;
                double dMixDnCount = 0;


                SQL = "     SELECT   nvl(  COUNT(PR_DLYN_DN_NO ),0)  DNCOUNT FROM    ";
                SQL = SQL + "    PR_DELIVERY_NOTE WHERE  PR_DLYN_DN_DATE   ";
                SQL = SQL + "     BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + "     AND SALES_STN_STATION_CODE   =   '" + sStationCode + "'";
                SQL = SQL + "    AND PR_DELIVERY_NOTE.PR_CONRTN_DNCANCEL='N' ";
                SQL = SQL + "     AND PR_DELIVERY_NOTE.PR_CONRTN_DNCANCEL='N' ";
                SQL = SQL + "    AND PR_DELIVERY_NOTE.PROD_DLYN_PRODUCT_TRADE_YN = 'Y'  ";



                sReturn = clsSQLHelper.GetDataset(SQL);

                dDnCount = double.Parse(sReturn.Tables[0].Rows[0]["DNCOUNT"].ToString());


                sReturn = null;

                SQL = "       ";
                SQL = "SELECT   NVL( COUNT( DISTINCT  PR_DN_MIX_DETAILS .PROD_DLYN_DN_NO    ),0)  DNMIXCOUNT   FROM   PR_DELIVERY_NOTE, PR_DN_MIX_DETAILS   ";
                SQL = SQL + "   WHERE PR_DN_MIX_DETAILS.PROD_DLYN_DN_NO = PR_DELIVERY_NOTE.PR_DLYN_DN_NO ";
                SQL = SQL + "   AND PR_DN_MIX_DETAILS.AD_FIN_FINYRID = PR_DELIVERY_NOTE.AD_FIN_FINYRID  ";
                SQL = SQL + "  AND  PR_DELIVERY_NOTE.PR_DLYN_DN_DATE  ";
                SQL = SQL + "     BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + "  AND SALES_STN_STATION_CODE   =   '" + sStationCode + "'";
                SQL = SQL + "  AND PR_DELIVERY_NOTE.PR_CONRTN_DNCANCEL='N' ";
                SQL = SQL + "  AND PR_DELIVERY_NOTE.PR_DLYN_CONC_MIXED = 'Y'  ";
                SQL = SQL + "    AND PR_DELIVERY_NOTE.PROD_DLYN_PRODUCT_TRADE_YN = 'Y'  ";

                sReturn = clsSQLHelper.GetDataset(SQL);

                dMixDnCount = double.Parse(sReturn.Tables[0].Rows[0]["DNMIXCOUNT"].ToString());
                sReturn = null;

                if (dMixDnCount != dDnCount)
                {
                    return "Total Delivery Note Count  : " + dDnCount + " ,  Total Mix Details count  : " + dMixDnCount + " is not matching.Kindly contact ERP Support.";
                }


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return "eerrrer in query";
            }
            return "CONTINUE";
        }
        public int fnConversionFactopryMissingReadymix ( )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                //SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " ";
                SQL = "          select count(*) cnt   from (  ";
                SQL = SQL + "            select     rm_rmm_rm_code   from   ";
                SQL = SQL + "             (   ";
                SQL = SQL + "          select   distinct rm_rmm_rm_code  from TECH_PRODUCT_COMP_DETAILS ";
                SQL = SQL + "          union all   ";
                SQL = SQL + "          select   distinct rm_rmm_rm_code  from bl_PRODUCT_COMP_DETAILS  ";
                SQL = SQL + "          union all  ";
                SQL = SQL + "           select distinct  RMCODE      from   RM_STORE_ISSUE_VOUCHER_PRINT   ";
                SQL = SQL + "           union all  ";
                SQL = SQL + "           select  distinct RM_RMM_RM_CODE   from PC_ELEMENT_RIFCOMP_DETAILS ";
                SQL = SQL + "           )  ";
                SQL = SQL + "         minus   ";
                SQL = SQL + "          SELECT   rm_rmm_rm_code    FROM rm_uom_conversion   )  ";
                SQL = SQL + "            ";



                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return 1;
            }

            return (int.Parse(sReturn.Tables[0].Rows[0]["CNT"].ToString()));
        }


        public int fnConversionFactopryMissing ( )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                //SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                //SQL = " select count(*) cnt   from ( ";
                //SQL = SQL + " select   rm_rmm_rm_code  from RM_RAWMATERIAL_MASTER where RM_RMM_PHYSICAL_STOCK_YN ='Y' ";
                //SQL = SQL + " minus  ";
                //SQL = SQL + "  SELECT   rm_rmm_rm_code    FROM rm_uom_conversion   ) ";

                SQL = " select count(*) cnt   from ( ";
                SQL = SQL + " select   rm_rmm_rm_code  from  RM_UOM_CONVERSION_RFSH_VW ";
                SQL = SQL + "     ) ";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return 1;
            }

            return (int.Parse(sReturn.Tables[0].Rows[0]["CNT"].ToString()));
        }


        public int fnConversionFactopryMissingBLOCK ( )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                //SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = " select count(*) cnt   from ( ";
                SQL = SQL + " select   rm_rmm_rm_code  from BL_PRODUCT_COMP_DETAILS ";
                SQL = SQL + " minus  ";
                SQL = SQL + "  SELECT   rm_rmm_rm_code    FROM rm_uom_conversion   ) ";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return 1;
            }

            return (int.Parse(sReturn.Tables[0].Rows[0]["CNT"].ToString()));
        }


        #endregion 

        public DataTable FillBranch ( )
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  SELECT AD_BR_CODE CODE,AD_BR_NAME NAME FROM AD_BRANCH_MASTER ";
                SQL = SQL + " WHERE  AD_BR_ACTIVESTATUS_YN ='Y'";
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

        #region "  Fetch Opening  balances / RVD qty / stk in / stk out / sales / consumption
        public DataSet FetchOpeningBALANCES ( string lsStnCode, string IsBranch, object mngrclassobj )
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
                SQL = SQL + " AND SALES_STN_STATION_CODE='" + lsStnCode + "' ";
                SQL = SQL + " AND AD_BR_CODE='" + IsBranch + "' ";
                SQL = SQL + " ORDER BY RM_RMM_RM_CODE";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchPrevReceivedQty ( string lsStnCode, string IsBranch, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj )
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
                SQL = SQL + " AND AD_BR_CODE='" + IsBranch + "'";
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

        public DataSet FetchCurrentTOTALReceivedQty ( string lsStnCode, string IsBranch, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj )
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
                SQL = SQL + " AND AD_BR_CODE='" + IsBranch + "'";
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

        public DataSet FetchCurrentMonthReceivedQty ( string lsStnCode, string IsBranch, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj )
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
                SQL = SQL + " AND AD_BR_CODE='" + IsBranch + "'";
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

        public DataSet FetchCurrentMonthStocktransferInQty ( string lsStnCode, string IsBranch, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj )
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
                SQL = SQL + " AND AD_BR_CODE='" + IsBranch + "'";
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

        public DataSet FetchCurrentMonthStocktransferOUTQty ( string lsStnCode, string IsBranch, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj )
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
                SQL = SQL + " AND AD_BR_CODE='" + IsBranch + "'";
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

        public DataSet FetchCurrentMonthSalesQty ( string lsStnCode, string IsBranch, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj )
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
                SQL = SQL + " AND AD_BR_CODE='" + IsBranch + "'";
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

        public DataSet FetchComposition ( string lsStnCode, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj )
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



        //public DataSet FetchCompositionBlock(string lsStnCode, DateTime ldFromDate, DateTime ldToDate)
        //{
        //    DataSet sReturn = new DataSet();
        //    CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

        //    try
        //    {
        //        SQL = string.Empty;
        //        // SessionManager mngrclass = (SessionManager)mngrclassobj;
        //        SqlHelper clsSQLHelper = new SqlHelper();


        //        //-------------------SMAPLE QUERY 


        //        SQL = "  SELECT  ";
        //        SQL = SQL + "       ''   RM_PSPRM_ENTRY_NO,0   AD_FIN_FINYRID,   SALES_STN_STATION_CODE,   RM_RMM_RM_CODE, ";
        //        SQL = SQL + "         RM_RMM_RM_DESCRIPTION,      BASIC_UOM,     BASIC_UOM_DESC,  ";
        //        SQL = SQL + "         SUM (FG_QTY_PRODUCED) FG_QTY_PRODUCED, ";
        //        SQL = SQL + "         SUM (THEO_QTY)  THEO_QTY, ";
        //        SQL = SQL + "         SUM (CONS_QTY)  CONS_QTY, ";
        //        SQL = SQL + "         SUM (THEO_CONVERTED_QTY)     THEO_CONVERTED_QTY, ";
        //        SQL = SQL + "        SUM ( CONS_CONVERTED_QTY ) CONS_CONVERTED_QTY ";
        //        SQL = SQL + "    FROM  ";
        //        SQL = SQL + "    (  ";
        //        SQL = SQL + "     SELECT  ";
        //        SQL = SQL + "         FG_FINISHED_GOODS_MASTER.SALES_STN_STATION_CODE, ";
        //        SQL = SQL + "         FG_FINISHED_GOODS_CONS_DETAILS.RM_RMM_RM_CODE, ";
        //        SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
        //        SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE               BASIC_UOM, ";
        //        SQL = SQL + "         UOM_BASIC.RM_UM_UOM_DESC                           BASIC_UOM_DESC, ";
        //        SQL = SQL + "         FG_FINISHED_GOODS_CONS_DETAILS.FG_PCD_UOM_CODE     COMPOSITION_UOM, ";
        //        SQL = SQL + "         COMPOSITION_UOM_MSTR.RM_UM_UOM_DESC                COMP_UOM_DESC, ";
        //        SQL = SQL + "         SUM (FG_QTY_PRODUCED)                              FG_QTY_PRODUCED, ";
        //        SQL = SQL + "         SUM (FG_FNG_WEIGHT_FROM)                           THEO_QTY, ";
        //        SQL = SQL + "         SUM (FG_FNG_WEIGHT_ACTUAL)                         CONS_QTY, ";
        //        SQL = SQL + "         SUM (FG_FNG_WEIGHT_FROM*FG_FNG_CONVERSION_FACT)     THEO_CONVERTED_QTY, ";
        //        SQL = SQL + "        SUM ( FG_FNG_CONVERSION_FACT * FG_FNG_WEIGHT_ACTUAL ) CONS_CONVERTED_QTY ";
        //        SQL = SQL + "    FROM FG_FINISHED_GOODS_MASTER, ";
        //        SQL = SQL + "         FG_FINISHED_GOODS_CONS_DETAILS, ";
        //        SQL = SQL + "         BL_PRODUCT_MASTER, ";
        //        SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
        //        SQL = SQL + "         RM_UOM_MASTER UOM_BASIC, ";
        //        SQL = SQL + "         RM_UOM_MASTER COMPOSITION_UOM_MSTR ";
        //        SQL = SQL + "   WHERE     FG_FINISHED_GOODS_MASTER.FG_FINISHED_GOODS_DOC_NO = ";
        //        SQL = SQL + "             FG_FINISHED_GOODS_CONS_DETAILS.FG_FINISHED_GOODS_DOC_NO ";
        //        SQL = SQL + "         AND FG_FINISHED_GOODS_MASTER.AD_FIN_FINYRID = ";
        //        SQL = SQL + "             FG_FINISHED_GOODS_CONS_DETAILS.AD_FIN_FINYRID ";
        //        SQL = SQL + "         AND FG_FINISHED_GOODS_MASTER.TECH_PM_PRODUCT_CODE = ";
        //        SQL = SQL + "             BL_PRODUCT_MASTER.TECH_PM_PRODUCT_CODE ";
        //        SQL = SQL + "         AND FG_FINISHED_GOODS_CONS_DETAILS.RM_RMM_RM_CODE = ";
        //        SQL = SQL + "             RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
        //        SQL = SQL + "         AND FG_FINISHED_GOODS_CONS_DETAILS.FG_PCD_UOM_CODE = ";
        //        SQL = SQL + "             COMPOSITION_UOM_MSTR.RM_UM_UOM_CODE ";
        //        SQL = SQL + "         AND RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE = UOM_BASIC.RM_UM_UOM_CODE ";
        //        SQL = SQL + "         AND FG_FINISHED_GOODS_MASTER.FG_CONC_MIXED = 'Y' ";

        //        SQL = SQL + "  AND FG_FINISHED_GOODS_MASTER.sales_stn_station_code = '" + lsStnCode + "'";
        //        SQL = SQL + "   AND FG_FINISHED_GOODS_MASTER.FG_PRODUCTION_DATE  BETWEEN '" + System.Convert.ToDateTime(ldFromDate, culture).ToString("dd-MMM-yyyy") + "'";
        //        SQL = SQL + "   AND  '" + System.Convert.ToDateTime(ldToDate, culture).ToString("dd-MMM-yyyy") + "'";

        //        SQL = SQL + "GROUP BY FG_FINISHED_GOODS_CONS_DETAILS.RM_RMM_RM_CODE, ";
        //        SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
        //        SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE, ";
        //        SQL = SQL + "         UOM_BASIC.RM_UM_UOM_DESC, ";
        //        SQL = SQL + "         FG_FINISHED_GOODS_CONS_DETAILS.FG_PCD_UOM_CODE, ";
        //        SQL = SQL + "         COMPOSITION_UOM_MSTR.RM_UM_UOM_DESC, ";
        //        SQL = SQL + "         FG_FINISHED_GOODS_MASTER.SALES_STN_STATION_CODE ";
        //        SQL = SQL + " UNION ALL ";
        //        SQL = SQL + "  SELECT  BL_DELIVERY_NOTE.SALES_STN_STATION_CODE , ";
        //        SQL = SQL + "                   BL_PRODUCT_COMP_DETAILS.RM_RMM_RM_CODE, ";
        //        SQL = SQL + "                   RM_RMM_RM_DESCRIPTION, ";
        //        SQL = SQL + "                   RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE     BASIC_UOM, ";
        //        SQL = SQL + "                   UOM_BASIC.RM_UM_UOM_DESC                 BASIC_UOM_DESC, ";
        //        SQL = SQL + "                    RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE     COMPOSITION_UOM, ";
        //        SQL = SQL + "                   UOM_BASIC.RM_UM_UOM_DESC                 COMP_UOM_DESC, ";
        //        SQL = SQL + "                   SUM (PROD_AC_HOURS_QTY)                  FG_QTY_PRODUCED, ";
        //        SQL = SQL + "                   SUM (PROD_AC_HOURS_QTY)                  THEO_QTY, ";
        //        SQL = SQL + "                   SUM (PROD_AC_HOURS_QTY)                  CONS_QTY, ";
        //        SQL = SQL + "                   SUM (PROD_AC_HOURS_QTY)                  THEO_CONVERTED_QTY, ";
        //        SQL = SQL + "                   SUM (PROD_AC_HOURS_QTY)                  CONS_CONVERTED_QTY  ";
        //        SQL = SQL + " FROM  ";
        //        SQL = SQL + "        BL_DELIVERY_NOTE , BL_DLYN_ADDCHAGRES, ";
        //        SQL = SQL + "        BL_PRODUCT_MASTER ,BL_PRODUCT_COMP_DETAILS , ";
        //        SQL = SQL + "        RM_RAWMATERIAL_MASTER, ";
        //        SQL = SQL + "        RM_UOM_MASTER UOM_BASIC ";
        //        SQL = SQL + " where  ";
        //        SQL = SQL + "    BL_DELIVERY_NOTE.PR_DLYN_DN_NO = PROD_DLYN_DN_NO  ";
        //        SQL = SQL + "    and  BL_DELIVERY_NOTE.AD_FIN_FINYRID =   BL_DLYN_ADDCHAGRES.AD_FIN_FINYRID  ";
        //        SQL = SQL + "    and BL_DLYN_ADDCHAGRES.TECH_PM_PRODUCT_CODE = BL_PRODUCT_MASTER.TECH_PM_PRODUCT_CODE  ";
        //        SQL = SQL + "    and BL_PRODUCT_MASTER.TECH_PM_PRODUCT_CODE =   BL_PRODUCT_COMP_DETAILS.TECH_PM_PRODUCT_CODE  ";
        //        SQL = SQL + "    and BL_PRODUCT_COMP_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
        //        SQL = SQL + "    and RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE = UOM_BASIC.RM_UM_UOM_CODE  ";
        //        SQL = SQL + "    and PROD_AC_HOURS_QTY > 0   and  PR_DLYN_APPROVED ='Y' ";
        //        SQL = SQL + "    and  PR_CONRTN_DNCANCEL ='N'  and PR_DLYN_CONC_MIXED ='Y'";

        //        SQL = SQL + "  AND BL_DELIVERY_NOTE.sales_stn_station_code = '" + lsStnCode + "'";
        //        SQL = SQL + "   AND BL_DELIVERY_NOTE.PR_DLYN_DN_DATE  BETWEEN '" + System.Convert.ToDateTime(ldFromDate, culture).ToString("dd-MMM-yyyy") + "'";
        //        SQL = SQL + "   AND  '" + System.Convert.ToDateTime(ldToDate, culture).ToString("dd-MMM-yyyy") + "'"; 
        //        SQL = SQL + "    GROUP BY  ";
        //        SQL = SQL + "       BL_DELIVERY_NOTE.SALES_STN_STATION_CODE  , BL_PRODUCT_COMP_DETAILS.RM_RMM_RM_CODE  ,RM_RMM_RM_DESCRIPTION , ";
        //        SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE   ,   UOM_BASIC.RM_UM_UOM_DESC           "; 
        //        SQL = SQL + "         )  ";
        //        SQL = SQL + "        GROUP BY  ";
        //        SQL = SQL + "         SALES_STN_STATION_CODE, ";
        //        SQL = SQL + "         RM_RMM_RM_CODE, ";
        //        SQL = SQL + "         RM_RMM_RM_DESCRIPTION,     BASIC_UOM,  BASIC_UOM_DESC   "; 

        //        sReturn = clsSQLHelper.GetDataset(SQL);
        //    }
        //    catch (Exception ex)
        //    {
        //        ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
        //        objLogWriter.WriteLog(ex);
        //    }

        //    return sReturn;
        //}


        public DataSet FetchCompositionBlockProductWise ( string lsStnCode, DateTime ldFromDate, DateTime ldToDate )
        {
            DataSet sReturn = new DataSet();
            CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

            try
            {
                SQL = string.Empty;
                // SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "select  ";
                SQL = SQL + "    MSTQRY.SALES_STN_STATION_CODE   ,MSTQRY.TECH_PM_PRODUCT_CODE,     ";
                SQL = SQL + "    COINSQRY.TECH_PM_PRODUCT_DESCRIPTION, ";
                SQL = SQL + "    COINSQRY.RM_RMM_RM_CODE, COINSQRY.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "    COINSQRY.RM_RMM_RM_TYPE, COINSQRY. BASIC_UOM, ";
                SQL = SQL + "    COINSQRY.BASIC_UOM_DESC , ";
                SQL = SQL + "    COINSQRY.TECH_PM_COSTING_OPTION_NOS_MSQ,  ";
                SQL = SQL + "    MSTQRY.FG_QTY_PRODUCED , MSTQRY.TECH_PM_DMG_QTY ,  ";
                SQL = SQL + "    NVL(COINSQRY.THEO_QTY,0) THEO_QTY, ";
                SQL = SQL + "    NVL(COINSQRY. CONS_QTY,0)CONS_QTY , ";
                SQL = SQL + "    NVL(COINSQRY. THEO_CONVERTED_QTY,0) THEO_CONVERTED_QTY, ";
                SQL = SQL + "    NVL(COINSQRY. CONS_CONVERTED_QTY,0)  CONS_CONVERTED_QTY   ,";
                SQL = SQL + "  1  TECH_PLNT_NO_OF_PCS_PER_MIX,    1  TECH_PLNT_MSQ_METER_PER_MIX, 1 TECH_PLNT_LINER_METR_PER_MIX,  0 RM_PSED_AVG_PRICE_NEW ,0  FG_FNG_COST_OF_PRODUCTION_RATE ";
                SQL = SQL + " from  ";
                SQL = SQL + " ( ";
                SQL = SQL + "    select     SALES_STN_STATION_CODE ,   TECH_PM_PRODUCT_CODE,   sum( FG_QTY_PRODUCED ) FG_QTY_PRODUCED ,   sum(   TECH_PM_DMG_QTY   )   TECH_PM_DMG_QTY ";
                SQL = SQL + "   from    ";
                SQL = SQL + "   (  ";
                SQL = SQL + "    select     SALES_STN_STATION_CODE ,   TECH_PM_PRODUCT_CODE,    FG_QTY_PRODUCED  FG_QTY_PRODUCED , 0  TECH_PM_DMG_QTY  ";
                SQL = SQL + "    from FG_FINISHED_GOODS_MASTER  ";
                SQL = SQL + "    where  FG_FINISHED_GOODS_MASTER.FG_CONC_MIXED = 'Y' ";
                SQL = SQL + "    AND FG_FINISHED_GOODS_MASTER.sales_stn_station_code = '" + lsStnCode + "' AND FG_QTY_PRODUCED >0 ";
                SQL = SQL + "    AND FG_FINISHED_GOODS_MASTER.FG_PRODUCTION_DATE ";
                SQL = SQL + "        BETWEEN '" + System.Convert.ToDateTime(ldFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "      AND  '" + System.Convert.ToDateTime(ldToDate, culture).ToString("dd-MMM-yyyy") + "'";
                /// no need this moved to trigger ///SQL = SQL + " UNION ALL  ";
                //////SQL = SQL + "     select  SALES_STN_STATION_CODE ,   TECH_PM_PRODUCT_CODE,   0  FG_QTY_PRODUCED ,    TECH_PM_DMG_QTY  TECH_PM_DMG_QTY ";
                //////SQL = SQL + "      from BL_PRODUCT_STOCK_LEDGER  where  ";
                //////SQL = SQL + "      BL_PRODUCT_STOCK_LEDGER.sales_stn_station_code = '" + lsStnCode + "' AND   TECH_PM_DMG_QTY >0  ";
                //////SQL = SQL + "      AND BL_PRODUCT_STOCK_LEDGER.TECH_PM_TRANS_DATE ";
                //////SQL = SQL + "        BETWEEN '" + System.Convert.ToDateTime(ldFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                //////SQL = SQL + "   AND  '" + System.Convert.ToDateTime(ldToDate, culture).ToString("dd-MMM-yyyy") + "'";    
                SQL = SQL + "   ) ";
                SQL = SQL + "      GROUP BY SALES_STN_STATION_CODE ,   TECH_PM_PRODUCT_CODE ";


                SQL = SQL + "   )mstqry  ,     ";
                SQL = SQL + " (   ";
                SQL = SQL + "    SELECT ";
                SQL = SQL + "           FG_FINISHED_GOODS_MASTER.SALES_STN_STATION_CODE,  ";
                SQL = SQL + "           FG_FINISHED_GOODS_MASTER.TECH_PM_PRODUCT_CODE,  TECH_PM_PRODUCT_DESCRIPTION, ";
                SQL = SQL + "           FG_FINISHED_GOODS_CONS_DETAILS.RM_RMM_RM_CODE, ";
                SQL = SQL + "           RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "           RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE, ";
                SQL = SQL + "           RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE  BASIC_UOM, ";
                SQL = SQL + "           UOM_BASIC.RM_UM_UOM_DESC BASIC_UOM_DESC , ";
                SQL = SQL + "           BL_PRODUCT_MASTER.TECH_PM_COSTING_OPTION_NOS_MSQ,   ";
                SQL = SQL + "           SUM (FG_FNG_WEIGHT_FROM)    THEO_QTY, ";
                SQL = SQL + "           SUM (FG_FNG_WEIGHT_ACTUAL)  CONS_QTY, ";
                SQL = SQL + "           SUM (FG_FNG_WEIGHT_FROM * FG_FNG_CONVERSION_FACT)   THEO_CONVERTED_QTY, ";
                SQL = SQL + "           SUM (FG_FNG_CONVERSION_FACT * FG_FNG_WEIGHT_ACTUAL)  CONS_CONVERTED_QTY ";
                SQL = SQL + "      FROM FG_FINISHED_GOODS_MASTER, ";
                SQL = SQL + "           FG_FINISHED_GOODS_CONS_DETAILS,  ";
                SQL = SQL + "           BL_PRODUCT_MASTER, ";
                SQL = SQL + "           RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "           RM_UOM_MASTER UOM_BASIC, ";
                SQL = SQL + "           RM_UOM_MASTER COMPOSITION_UOM_MSTR  ";
                SQL = SQL + "     WHERE     FG_FINISHED_GOODS_MASTER.FG_FINISHED_GOODS_DOC_NO =    FG_FINISHED_GOODS_CONS_DETAILS.FG_FINISHED_GOODS_DOC_NO ";
                SQL = SQL + "           AND FG_FINISHED_GOODS_MASTER.AD_FIN_FINYRID =    FG_FINISHED_GOODS_CONS_DETAILS.AD_FIN_FINYRID ";
                SQL = SQL + "           AND FG_FINISHED_GOODS_MASTER.TECH_PM_PRODUCT_CODE = BL_PRODUCT_MASTER.TECH_PM_PRODUCT_CODE ";
                SQL = SQL + "           AND FG_FINISHED_GOODS_CONS_DETAILS.RM_RMM_RM_CODE =    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                SQL = SQL + "           AND FG_FINISHED_GOODS_CONS_DETAILS.FG_PCD_UOM_CODE =  COMPOSITION_UOM_MSTR.RM_UM_UOM_CODE ";
                SQL = SQL + "           AND RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE =  UOM_BASIC.RM_UM_UOM_CODE ";
                SQL = SQL + "           AND FG_FINISHED_GOODS_MASTER.FG_CONC_MIXED = 'Y' ";
                SQL = SQL + "           AND FG_FINISHED_GOODS_MASTER.sales_stn_station_code = '" + lsStnCode + "'";
                SQL = SQL + "           AND FG_FINISHED_GOODS_MASTER.FG_PRODUCTION_DATE ";
                SQL = SQL + "                    BETWEEN '" + System.Convert.ToDateTime(ldFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "   AND  '" + System.Convert.ToDateTime(ldToDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "  GROUP BY  ";
                SQL = SQL + "            FG_FINISHED_GOODS_MASTER.SALES_STN_STATION_CODE,  ";
                SQL = SQL + "           FG_FINISHED_GOODS_MASTER.TECH_PM_PRODUCT_CODE,  TECH_PM_PRODUCT_DESCRIPTION, ";
                SQL = SQL + "           FG_FINISHED_GOODS_CONS_DETAILS.RM_RMM_RM_CODE, ";
                SQL = SQL + "           RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "           RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE, ";
                SQL = SQL + "           RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE   , ";
                SQL = SQL + "           UOM_BASIC.RM_UM_UOM_DESC   , ";
                SQL = SQL + "           BL_PRODUCT_MASTER.TECH_PM_COSTING_OPTION_NOS_MSQ  ";
                SQL = SQL + "         ) coinsqry ";
                SQL = SQL + "  where ";
                SQL = SQL + "     mstqry.SALES_STN_STATION_CODE = coinsqry.SALES_STN_STATION_CODE  (+)  ";
                SQL = SQL + "    and  mstqry.TECH_PM_PRODUCT_CODE = coinsqry.TECH_PM_PRODUCT_CODE   (+)   ";
                SQL = SQL + " UNION ALL  ";
                SQL = SQL + "    SELECT   BL_DELIVERY_NOTE.SALES_STN_STATION_CODE   ,BL_DLYN_ADDCHAGRES.TECH_PM_PRODUCT_CODE,     ";
                SQL = SQL + "    TECH_PM_PRODUCT_DESCRIPTION, ";
                SQL = SQL + "   RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE, RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE, ";
                SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE     BASIC_UOM,     UOM_BASIC.RM_UM_UOM_DESC   BASIC_UOM_DESC, ";
                SQL = SQL + "    BL_PRODUCT_MASTER.TECH_PM_COSTING_OPTION_NOS_MSQ,   ";
                SQL = SQL + "    SUM (PROD_AC_HOURS_QTY)  FG_QTY_PRODUCED,  0 TECH_PM_DMG_QTY  , ";
                SQL = SQL + "    SUM (PROD_AC_HOURS_QTY)   THEO_QTY, ";
                SQL = SQL + "    SUM (PROD_AC_HOURS_QTY)   CONS_QTY, ";
                SQL = SQL + "    SUM (PROD_AC_HOURS_QTY)   THEO_CONVERTED_QTY, ";
                SQL = SQL + "    SUM (PROD_AC_HOURS_QTY)   CONS_CONVERTED_QTY,  ";
                SQL = SQL + "  1  TECH_PLNT_NO_OF_PCS_PER_MIX,    1  TECH_PLNT_MSQ_METER_PER_MIX, 1 TECH_PLNT_LINER_METR_PER_MIX,  0 RM_PSED_AVG_PRICE_NEW ,0  FG_FNG_COST_OF_PRODUCTION_RATE ";
                SQL = SQL + "          FROM BL_DELIVERY_NOTE, ";
                SQL = SQL + "               BL_DLYN_ADDCHAGRES, ";
                SQL = SQL + "               BL_PRODUCT_MASTER, ";
                SQL = SQL + "               BL_PRODUCT_COMP_DETAILS, ";
                SQL = SQL + "               RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "               RM_UOM_MASTER UOM_BASIC ";
                SQL = SQL + "         WHERE    ";
                SQL = SQL + "                    BL_DELIVERY_NOTE.PR_DLYN_DN_NO = BL_DLYN_ADDCHAGRES.PROD_DLYN_DN_NO ";
                SQL = SQL + "                    AND BL_DELIVERY_NOTE.AD_FIN_FINYRID =  BL_DLYN_ADDCHAGRES.AD_FIN_FINYRID ";
                SQL = SQL + "                    AND BL_DLYN_ADDCHAGRES.TECH_PM_PRODUCT_CODE = BL_PRODUCT_MASTER.TECH_PM_PRODUCT_CODE ";
                SQL = SQL + "                    AND BL_PRODUCT_MASTER.TECH_PM_PRODUCT_CODE = BL_PRODUCT_COMP_DETAILS.TECH_PM_PRODUCT_CODE ";
                SQL = SQL + "                    AND BL_PRODUCT_COMP_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                SQL = SQL + "                    AND RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE =  UOM_BASIC.RM_UM_UOM_CODE  ";
                SQL = SQL + "                    AND PROD_AC_HOURS_QTY > 0   AND PR_DLYN_APPROVED = 'Y' ";
                SQL = SQL + "                    AND PR_CONRTN_DNCANCEL = 'N' AND PR_DLYN_CONC_MIXED = 'Y' ";
                SQL = SQL + "                    AND BL_DELIVERY_NOTE.sales_stn_station_code =  '" + lsStnCode + "'";
                SQL = SQL + "                    AND PR_DLYN_DN_DATE  BETWEEN '" + System.Convert.ToDateTime(ldFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "     AND  '" + System.Convert.ToDateTime(ldToDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "      GROUP BY BL_DELIVERY_NOTE.SALES_STN_STATION_CODE, ";
                SQL = SQL + "               BL_DLYN_ADDCHAGRES.TECH_PM_PRODUCT_CODE, ";
                SQL = SQL + "               TECH_PM_PRODUCT_DESCRIPTION, ";
                SQL = SQL + "               BL_PRODUCT_MASTER.TECH_PM_COSTING_OPTION_NOS_MSQ, ";
                SQL = SQL + "               RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE, ";
                SQL = SQL + "               RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "               RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE, ";
                SQL = SQL + "               RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE, ";
                SQL = SQL + "               UOM_BASIC.RM_UM_UOM_DESC    ";
                SQL = SQL + "  ORDER BY   TECH_PM_PRODUCT_CODE,  RM_RMM_RM_CODE  ";

                sReturn = clsSQLHelper.GetDataset(SQL);


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }





        public DataSet FetchReadymixOrBlockPlantComposition ( string lsStnCode, string IsBranch, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();



                SQL = "  SELECT  ";
                SQL = SQL + "     RM_PHYSICAL_STOCK_PLANT_MASTER.RM_PSEPM_ENTRY_NO RM_PSPRM_ENTRY_NO ,RM_PHYSICAL_STOCK_PLANT_MASTER.AD_FIN_FINYRID,";
                SQL = SQL + " RM_PHYSICAL_STOCK_PLT_DETAILS.rm_rmm_rm_code, ";
                SQL = SQL + " rm_rmm_rm_description, RM_PHYSICAL_STOCK_PLT_DETAILS.RM_UM_UOM_CODE_MEASURED  basic_uom,";
                SQL = SQL + " RM_UM_UOM_CODE_BASIC composition_uom, ";
                SQL = SQL + " uom.rm_um_uom_desc basic_uom_desc, uomd.rm_um_uom_desc comp_uom_desc, ";
                SQL = SQL + " SUM (RM_PSED_CONSUM_THEORITICAL)Theo_qty,SUM (RM_PSED_CONSUMPTION) cons_qty, ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PLANT_MASTER.sales_stn_station_code ";
                SQL = SQL + " FROM RM_PHYSICAL_STOCK_PLANT_MASTER,RM_PHYSICAL_STOCK_PLT_DETAILS, rm_rawmaterial_master, rm_uom_master uom, ";
                SQL = SQL + " rm_uom_master uomd,TECH_PLANT_MASTER ";
                SQL = SQL + " WHERE RM_PHYSICAL_STOCK_PLANT_MASTER.RM_PSEPM_ENTRY_NO = RM_PHYSICAL_STOCK_PLT_DETAILS.RM_PSEPM_ENTRY_NO ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PLANT_MASTER.AD_FIN_FINYRID= RM_PHYSICAL_STOCK_PLT_DETAILS.AD_FIN_FINYRID ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PLT_DETAILS.rm_rmm_rm_code = rm_rawmaterial_master.rm_rmm_rm_code ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PLT_DETAILS.RM_UM_UOM_CODE_MEASURED = uom.rm_um_uom_code ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PLT_DETAILS.RM_UM_UOM_CODE_BASIC = uomd.rm_um_uom_code ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PLT_DETAILS.TECH_PLM_PLANT_CODE=TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PLANT_MASTER.sales_stn_station_code=TECH_PLANT_MASTER.sales_stn_station_code ";
                SQL = SQL + " AND RM_PSEM_ENTRY_DATE BETWEEN  ";
                SQL = SQL + " '" + ldFromDate.ToString("dd-MMM-yyyy") + "' AND '" + ldToDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PLANT_MASTER.sales_stn_station_code = '" + lsStnCode + "'";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PLANT_MASTER.AD_BR_CODE = '" + IsBranch + "'";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PLANT_MASTER.RM_PSEM_APPROVED='Y' ";
                SQL = SQL + " GROUP BY  RM_PHYSICAL_STOCK_PLANT_MASTER.RM_PSEPM_ENTRY_NO,RM_PHYSICAL_STOCK_PLANT_MASTER.AD_FIN_FINYRID,";
                SQL = SQL + " RM_PHYSICAL_STOCK_PLT_DETAILS.rm_rmm_rm_code, ";
                SQL = SQL + " rm_rmm_rm_description, RM_PHYSICAL_STOCK_PLT_DETAILS.RM_UM_UOM_CODE_MEASURED, ";
                SQL = SQL + " RM_UM_UOM_CODE_BASIC,uom.rm_um_uom_desc , uomd.rm_um_uom_desc , ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PLANT_MASTER.sales_stn_station_code ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchPrecastProjectComposition ( string lsStnCode, string IsBranch, DateTime ldFromDate, DateTime ldToDate, object mngrclassobj )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "  SELECT  ";
                SQL = SQL + "  RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPRM_ENTRY_NO, ";
                SQL = SQL + "    RM_PHYSICAL_STOCK_PROJ_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL + "    RM_PHYSICAL_STOCK_PROJ_MASTER.SALES_STN_STATION_CODE, ";
                SQL = SQL + "    RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE,    ";
                SQL = SQL + "    SUM( RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_CONSUM_THEORITICAL) THEO_QTY , ";
                SQL = SQL + "    SUM( RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_CONSUMPTION)CONS_QTY ";
                SQL = SQL + "FROM  ";
                SQL = SQL + "    RM_PHYSICAL_STOCK_PROJ_MASTER, RM_PHYSICAL_STOCK_PROJ_DETAILS ";
                SQL = SQL + "WHERE   ";
                SQL = SQL + "    RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPRM_ENTRY_NO =RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPRM_ENTRY_NO ";
                SQL = SQL + " AND  RM_PHYSICAL_STOCK_PROJ_MASTER.AD_FIN_FINYRID =RM_PHYSICAL_STOCK_PROJ_DETAILS.AD_FIN_FINYRID ";

                SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPR_ENTRY_DATE BETWEEN  ";
                SQL = SQL + " '" + ldFromDate.ToString("dd-MMM-yyyy") + "' AND '" + ldToDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_MASTER.sales_stn_station_code = '" + lsStnCode + "' ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_MASTER.AD_BR_CODE = '" + IsBranch + "'";
                SQL = SQL + " AND  RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPR_APPROVED ='Y' ";

                SQL = SQL + " GROUP BY RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPRM_ENTRY_NO, ";
                SQL = SQL + "RM_PHYSICAL_STOCK_PROJ_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL + "RM_PHYSICAL_STOCK_PROJ_MASTER.SALES_STN_STATION_CODE, ";
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


        public DataSet FetchProjectWisePrecastConsumption ( DateTime dDnFromDate, DateTime dToDate, string ddlStation, string txtBranchCode )
        {
            DataSet dsReturn = new DataSet("CONSUMPTION");
            DataTable dtDnData = new DataTable();
            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                // and  STATION_CODE  ='" + ddlStation  + "'";  REMOVED STATAIO CODE SICE ONLY ONE STATION WE NEED TO PASS THE OCNSUMPTIN, OTHER STATION NO RECIEPT 
                // AND MATERISAL ISSUE SITE NTO KEEPING ANY STOCK LWEDGER TRANSAXCATINOS JOMY 28 02 2025  / /LONG DICUSSIN WITH RADED 


                SQL = "   select ACCOUNT_CODE SALES_PM_PROJECT_CODE , ACCOUNT_NAME PROJNAME ,  ";
                SQL = SQL + "          STATION_CODE STN_CODE , STATION_NAME STN_NAME ,   ";
                SQL = SQL + "          RMCODE RM_RMM_RM_CODE , RMDESC RM_NAME ,RM_TYPE,  ";
                SQL = SQL + "          UOM_CODE TECH_PCD_UOM_CODE , UOM_DESC FROM_UOM_NAME , ";
                SQL = SQL + "          TO_BASIC_UOM RM_UM_UOM_CODE, TO_UOM_DESC,  ";
                SQL = SQL + "          PRODUCT_TYPE_CODE , PRODUCT_TYPE_DESC , ";
                SQL = SQL + "          ONSITE_OR_FACTORY,PC_BUD_RESOURCE_CODE,CONS_ACC_CODE,  ";
                SQL = SQL + "     SUM (QUANTITY_CONVRTD) QUANTITY_CONVRTD,  ";
                SQL = SQL + "     SUM (QUANTITY) TECT_CONTMP_THR_CON_QTY              ";
                SQL = SQL + "        from  ";
                SQL = SQL + "        RM_STORE_ISSUE_VOUCHER_PRINT  WHERE  APPROVED_YN ='Y'";

                SQL = SQL + " AND ENTRY_DATE BETWEEN '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + "         GROUP BY  ";
                SQL = SQL + "         ACCOUNT_CODE   , ";
                SQL = SQL + "        ACCOUNT_NAME   , ";
                SQL = SQL + "        STATION_CODE, ";
                SQL = SQL + "        STATION_NAME,  ";
                SQL = SQL + "        RMCODE, ";
                SQL = SQL + "        RMDESC, RM_TYPE,";
                SQL = SQL + "        UOM_CODE, ";
                SQL = SQL + "        UOM_DESC,TO_BASIC_UOM,TO_UOM_DESC,PRODUCT_TYPE_CODE,PRODUCT_TYPE_DESC,  ";
                SQL = SQL + "        ONSITE_OR_FACTORY,PC_BUD_RESOURCE_CODE,CONS_ACC_CODE  ";

                dtDnData = clsSQLHelper.GetDataTableByCommand(SQL);

                dtDnData.TableName = "CONSUMPTION";
                dsReturn.Tables.Add(dtDnData);

            }
            catch (Exception ex)
            {

                return null;
            }

            return dsReturn;
        }



        #endregion

        #region " otherfuncion "
        public DataSet FetchStation ( string stcode )
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
                    SQL = SQL + " AND SALES_STN_STATION_CODE='" + stcode + "' ";
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
        public string FnValidateWIPBlockFactory ( string dtpFromDate, string dtpToDate )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "   select sum ( QTY)  QTY ";
                SQL = SQL + "    FROM  ";
                SQL = SQL + "    ( ";
                SQL = SQL + "     SELECT    ";
                SQL = SQL + "            SUM    (   TECH_PM_WIP_IN_QTY ) QTY     ";
                SQL = SQL + "            FROM BL_PRODUCT_STOCK_LEDGER ";
                SQL = SQL + "            WHERE ";
                SQL = SQL + "             TECH_PM_TRANS_DATE   between ";
                SQL = SQL + "'" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + "            AND TECH_PM_TRANS_TYPE ='WIP'  ";
                SQL = SQL + "       UNION ALL   ";
                SQL = SQL + "        select    SUM(  FG_QTY_PRODUCED -FG_QTY_WET_DAMAGE)   *-1 ";
                SQL = SQL + "        from FG_FINISHED_GOODS_MASTER   ";
                SQL = SQL + "        where  FG_FINISHED_GOODS_MASTER.FG_CONC_MIXED = 'Y'  ";
                SQL = SQL + "         AND FG_QTY_PRODUCED >0  ";
                SQL = SQL + "        AND FG_FINISHED_GOODS_MASTER.FG_PRODUCTION_DATE    between ";
                SQL = SQL + "'" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "        ) ";

                sReturn = clsSQLHelper.GetDataset(SQL);

                if (double.Parse(sReturn.Tables[0].Rows[0]["QTY"].ToString()) > 0 || double.Parse(sReturn.Tables[0].Rows[0]["QTY"].ToString()) < 0)
                {
                    return "WIP Qty is not matching with movement qty check with admin, unable to continue  ";
                }


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return "CONTINUE";
        }

        public string FnValidateUnapprove ( string dtpFromDate, string dtpToDate, string Station )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "select Count(*) CNT From RM_PHYSICAL_STOCK_PROJ_MASTER ";
                SQL = SQL + " where RM_PSPR_ENTRY_DATE between   '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " and RM_PSPR_APPROVED='N' ";
                SQL = SQL + " and SALES_STN_STATION_CODE='" + Station + "' ";

                sReturn = clsSQLHelper.GetDataset(SQL);

                if (double.Parse(sReturn.Tables[0].Rows[0]["CNT"].ToString()) > 0)
                {
                    return "Unapprove Entry, unable to continue  ";
                }


                //SQL = "select Count(*) CNT From RM_PHYSICAL_STOCK_PROJ_MASTER ";
                //SQL = SQL + " where RM_PSPR_ENTRY_DATE between   '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                // SQL = SQL + " and SALES_STN_STATION_CODE='" + Station + "'   ";

                //sReturn = clsSQLHelper.GetDataset(SQL);

                //if (double.Parse(sReturn.Tables[0].Rows[0]["CNT"].ToString()) == 0)
                //{
                //    return "Project Consumption Entry not exists  , unable to continue  ";
                //}



            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return "CONTINUE";
        }

        public string FnValidate ( string dtpFromDate, string dtpToDate, string BranchCode, string Stationcode )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT COUNT(*)";
                SQL = SQL + " FROM RM_SALES_MASTER WHERE RM_MSM_APPROVED ='N'  and AD_BR_CODE='" + BranchCode + "' and SALES_STN_STATION_CODE='" + Stationcode + "'  ";
                SQL = SQL + " AND TO_DATE(TO_CHAR(RM_MSM_ENTRY_DATE,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);

                if (Convert.ToInt32(sReturn.Tables[0].Rows[0][0]) > 0)
                {
                    return "Unapproved raw material sales entry exists !! Unable to to continue..";
                }

                sReturn = null;

                SQL = " SELECT COUNT(*)";
                SQL = SQL + " FROM RM_RECEIPT_APPL_MASTER WHERE RM_MRMA_APPROVED_STATUS ='N' and AD_BR_CODE='" + BranchCode + "' ";
                SQL = SQL + " AND TO_DATE(TO_CHAR( RM_MRMA_APPROVALTRANS_DATE,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";


                //SQL = " SELECT COUNT(*)";
                //SQL = SQL + " FROM RM_RECEIPT_APPL_MASTER WHERE RM_MRMA_APPROVED_STATUS ='N' and AD_BR_CODE='" + BranchCode + "' ";
                //SQL = SQL + " AND TO_DATE(TO_CHAR( RM_MRMA_APPROVALTRANS_DATE,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                SQL = "  ";
                SQL = SQL + " SELECT COUNT(DISTINCT RM_MRM_APPROVED_NO) FROM RM_RECEPIT_DETAILS,RM_RECEIPT_APPL_MASTER ";
                SQL = SQL + " WHERE RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO=RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO  ";
                SQL = SQL + " AND RM_MRMA_APPROVED_STATUS ='N' ";
                SQL = SQL + " and RM_RECEIPT_APPL_MASTER.AD_BR_CODE = '" + BranchCode + "' ";
                SQL = SQL + "and RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE='" + Stationcode + "' ";
                SQL = SQL + " AND TO_DATE(TO_CHAR( RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE,'DD-MON-YYYY'))";
                SQL = SQL + "  BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";


                sReturn = clsSQLHelper.GetDataset(SQL);

                if (Convert.ToInt32(sReturn.Tables[0].Rows[0][0]) > 0)
                {
                    return "Unapproved raw material approval entry exists !! Unable to to continue..";
                }

                sReturn = null;
                SQL = "select  COUNT(*)  from RM_RECEPIT_DETAILS  ";
                SQL = SQL + "where RM_MRD_APPROVED='N' and AD_BR_CODE ='" + BranchCode + "' and SALES_STN_STATION_CODE ='" + Stationcode + "'   ";
                SQL = SQL + " AND TO_DATE(TO_CHAR( RM_MRM_RECEIPT_DATE,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);

                if (Convert.ToInt32(sReturn.Tables[0].Rows[0][0]) > 0)
                {
                    return "Raw material Receipt approval entry not created !! Unable to to continue..";
                }

                sReturn = null;

                //   SQL = " SELECT count(*) FROM rm_mat_stk_transfer_master m, rm_mat_stk_transfer_detl d ";
                //   SQL = SQL + "WHERE m.rm_mstm_transfer_no = d.rm_mstm_transfer_no AND m.ad_fin_finyrid = d.ad_fin_finyrid and ";
                ////   SQL = SQL + "d.RM_MSTD_REC_FLG ='N' ";
                //   SQL = SQL + "AND TO_DATE(TO_CHAR( m.RM_MSTM_TRANSFER_DATE ,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";


                SQL = " SELECT count(*) FROM rm_mat_stk_transfer_master m  ";
                SQL = SQL + "WHERE  RM_MSTM_APPRV_STATUS <>'Y' ";
                SQL = SQL + " and ( AD_BR_CODE_FROM  ='" + BranchCode + "' or   AD_BR_CODE_TO='" + BranchCode + "') ";
                SQL = SQL + " and ( SA_STN_STATION_CODE_FROM  ='" + Stationcode + "' or   SA_STN_STATION_CODE_TO='" + Stationcode + "') ";
                SQL = SQL + "AND TO_DATE(TO_CHAR( m.RM_MSTM_TRANSFER_DATE ,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";


                sReturn = clsSQLHelper.GetDataset(SQL);

                if (Convert.ToInt32(sReturn.Tables[0].Rows[0][0]) > 0)
                {
                    return "Unapproved raw material stock Transfer entry for Approval exists !! Unable to to continue..";
                }


                SQL = " SELECT COUNT(*)";
                SQL = SQL + "    from RM_PHYSICAL_STOCK_MASTER   ";
                SQL = SQL + "  where   RM_PSEM_APPROVED ='N' AND TO_DATE(TO_CHAR(RM_PSEM_FROM_DATE,'DD-MON-YYYY'))  < '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "  and  AD_BR_CODE='" + BranchCode + "' and SALES_STN_STATION_CODE  ='" + Stationcode + "' ";


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


        public string FnValidatePlantConsumption ( string dtpFromDate, string dtpToDate, string BranchCode, string Station )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();



                SQL = " SELECT COUNT(*)";
                SQL = SQL + "    from RM_PHYSICAL_STOCK_PLANT_MASTER   ";
                SQL = SQL + "  where   RM_PSEM_APPROVED ='Y' ";
                SQL = SQL + "  AND TO_DATE(TO_CHAR(RM_PSEM_ENTRY_DATE,'DD-MON-YYYY'))";
                SQL = SQL + "   BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND";
                SQL = SQL + "  '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "  and  AD_BR_CODE='" + BranchCode + "' and SALES_STN_STATION_CODE='" + Station + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);

                if (Convert.ToInt32(sReturn.Tables[0].Rows[0][0]) == 0)
                {
                    return "Create Raw material  Plant consumption entry !! Unable to to continue..";
                }

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return "CONTINUE";
        }

        public DataSet fnConversion ( )
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

        public string VaildateCheckProductType ( )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "SELECT Count(RM_RAWMATERIAL_MASTER.rm_rmm_rm_code) CNT  ";
                SQL = SQL + "        FROM RM_RAWMATERIAL_MASTER ,RM_RAWMATERIAL_CONS_ACC_DTL ,GL_PRODUCT_TYPE_MASTER ";
                SQL = SQL + "       WHERE RM_RAWMATERIAL_MASTER.rm_rmm_rm_code = RM_RAWMATERIAL_CONS_ACC_DTL.rm_rmm_rm_code  (+)     ";
                SQL = SQL + "         and GL_PRODUCT_TYPE_MASTER.GL_PRODUCT_TYPE_CODE = RM_RAWMATERIAL_CONS_ACC_DTL.GL_PRODUCT_TYPE_CODE  (+)  ";
                SQL = SQL + "         and RM_RAWMATERIAL_CONS_ACC_DTL.RM_RMM_CONS_ACC_CODE is null ";

                sReturn = clsSQLHelper.GetDataset(SQL);

                if (double.Parse(sReturn.Tables[0].Rows[0]["CNT"].ToString()) > 0)
                {
                    return "Product Type Account Code is not Connected ";
                }

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return "CONTINUE";
        }


        public string CheckEntryExists ( string sStationCode, string sBranchCode, string sEntryDate )
        {
            // JOmy P chacko 2 / jan 2013 checking the Physical stock entry against the selected period
            DataSet dsCnt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the  opening 


                SQL = " select  count(*) StkNo  ";
                SQL = SQL + " from RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + " where  SALES_STN_STATION_CODE =  '" + sStationCode + "' and AD_BR_CODE  ='" + sBranchCode + "' ";
                SQL = SQL + "  and      '" + System.Convert.ToDateTime(sEntryDate).ToString("dd-MMM-yyyy") + "' between RM_PSEM_FROM_DATE   and   RM_PSEM_TO_DATE";


                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "Unable to create the entry since physcial stock entry exists  against the selected period  kindly change the date and try ";
                }




            }
            catch (Exception ex)
            {
                return System.Convert.ToString(ex.Message);
            }
            return "CONTINUE";
        }

        public DataSet FetchEntryDateDetails ( string sStationCode, string sBranchCode, object mngrclassobj )
        {
            DataSet dsReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = " select RM_PSEM_FROM_DATE,RM_PSEM_TO_DATE, SALES_STN_STATION_CODE ";
                SQL = SQL + " from RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + " where  SALES_STN_STATION_CODE =  '" + sStationCode + "' ";
                SQL = SQL + " and  AD_BR_CODE =  '" + sBranchCode + "' ";
                SQL = SQL + " and AD_FIN_FINYRID=" + mngrclass.FinYearID + "";


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

        public string InsertMasterSql ( PhysicalStockEntryEntity oEntity,
            List<PhysicalStockEntryDetails> EntityDetails, List<PhysicalStockConsuProductDtls> EntityConsumDetails, List<PhysicalStockConsuPrecastDtls> EntityConsPrecastDtls,
            bool Autogen, object mngrclassobj, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber )
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
                SQL = SQL + " RM_PSEM_VERIFIED_ACC_YN ,RM_PSEM_VERIFIED_ACC_BY, RM_PSEM_VERIFIED_ACC_DATE, ";
                SQL = SQL + " GL_COSM_ACCOUNT_CODE, ";
                SQL = SQL + " RM_PSPRM_ENTRY_NO,RM_PSPRM_AD_FINYRID  ,  RM_PSEPM_ENTRY_NO   ,  RM_PSPEM_AD_FINYRID  , ";
                SQL = SQL + " AD_BR_CODE ,RM_PSEPM_CONSUMEDQTYTYPE    ";

                SQL = SQL + " ) ";

                SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "','" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "' ,'" + Convert.ToDateTime(oEntity.dtpFromDate).ToString("dd-MMM-yyyy") + "' ,";
                SQL = SQL + " '" + Convert.ToDateTime(oEntity.dtpToDate).ToString("dd-MMM-yyyy") + "' , " + Convert.ToInt32(oEntity.txtNoOfDays) + ",'" + oEntity.txtRemarks + "' ,";
                SQL = SQL + " '' , '" + mngrclass.CompanyCode + "' ,";
                SQL = SQL + " " + mngrclass.FinYearID + " ,'" + mngrclass.UserName + "' , TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " 0,'" + oEntity.ddlStation + "', ' ' ,'" + oEntity.approve + "' ,";
                SQL = SQL + "'" + oEntity.txtApprovedBy + "',";
                SQL = SQL + "'" + oEntity.varify + "' ,'" + oEntity.txtVerifiedBy + "',";
                SQL = SQL + "'" + oEntity.verifyAcc + "' ,'" + oEntity.txtVerifiedByAcc + "',TO_DATE('" + oEntity.VerifyAccDatetime + "','DD-MM-YYYY HH:MI:SS AM') ,";
                SQL = SQL + "'" + oEntity.CostCenter + "',";
                SQL = SQL + "'" + oEntity.ActualPltEntryNo + "'," + oEntity.ActualPltEntryFinid + ",'" + oEntity.ActualPltEntryNo + "'," + oEntity.ActualPltEntryFinid + ",";
                SQL = SQL + " '" + oEntity.ddlBranch + "','" + oEntity.ConsumedQtyEntryCallModule + "'";


                SQL = SQL + ")";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertDetails(oEntity, EntityDetails, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                if (EntityConsumDetails.Count > 0)
                {
                    sRetun = InsertProductConsumDetails(oEntity, EntityConsumDetails, mngrclassobj);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }


                if (EntityConsPrecastDtls.Count > 0)
                {
                    SQL = " INSERT INTO RM_PHYSICAL_STOCK_PROJ_MASTER (";
                    SQL = SQL + " RM_PSPRM_ENTRY_NO, RM_PSPR_ENTRY_DATE, RM_PSPR_FROM_DATE, ";
                    SQL = SQL + " RM_PSPR_TO_DATE, RM_PSPR_INTERVAL_DAYS, RM_PSPR_REMARKS, ";
                    SQL = SQL + " RM_PSPR_DOC_STATUS, AD_CM_COMPANY_CODE, ";
                    SQL = SQL + " AD_FIN_FINYRID, RM_PSPR_CREATEDBY, RM_PSPR_CREATEDDATE, ";
                    SQL = SQL + " RM_PSPR_DELETESTATUS, ";
                    SQL = SQL + " SALES_STN_STATION_CODE, HR_DEPT_DEPT_CODE, RM_PSPR_APPROVED, ";
                    SQL = SQL + " RM_PSPR_APPROVEDBY ,RM_PSPR_VERIFIED, RM_PSPR_VERIFIEDBY,GL_COSM_ACCOUNT_CODE , AD_BR_CODE   ";
                    SQL = SQL + ") ";

                    SQL = SQL + " VALUES ('" + oEntity.ActualPltEntryNo + "','" + oEntity.dtpEntryDate.ToString("dd-MMM-yyyy") + "','" + oEntity.dtpFromDate.ToString("dd-MMM-yyyy") + "', ";
                    SQL = SQL + " '" + oEntity.dtpToDate.ToString("dd-MMM-yyyy") + "','" + oEntity.txtNoOfDays + "','" + oEntity.txtRemarks + "', ";
                    SQL = SQL + " '' , '" + mngrclass.CompanyCode + "' ,";
                    SQL = SQL + " " + mngrclass.FinYearID + " ,'" + mngrclass.UserName + "' , TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                    SQL = SQL + " 0,'" + oEntity.ddlStation + "', '' ,'Y' ,";
                    SQL = SQL + "'" + mngrclass.UserName + "',";
                    SQL = SQL + "'Y' ,'" + mngrclass.UserName + "','" + oEntity.CostCenter + "','" + oEntity.ddlBranch + "'";

                    SQL = SQL + ")";

                    sSQLArray.Add(SQL);

                    sRetun = string.Empty;

                    sRetun = InsertPrecastConsumDetails(oEntity, EntityConsPrecastDtls, mngrclassobj);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }

                    SQL = "Insert into RM_PHYSICAL_STOCK_PROJ_RATE_TR ";
                    SQL = SQL + "   (RM_PSEM_ENTRY_NO, AD_FIN_FINYRID, RM_PSEM_APPROVED_YN";
                    SQL = SQL + "  ) ";
                    SQL = SQL + " VALUES ('" + oEntity.ActualPltEntryNo + "' , " + mngrclass.FinYearID + ",'Y'";
                    SQL = SQL + " )";

                    sSQLArray.Add(SQL);
                }
                sRetun = string.Empty;


                if (EntityConsumDetails.Count > 0)
                {

                    SQL = " INSERT INTO RM_PHYSICAL_STOCK_PROD_RATE_TR (";
                    SQL = SQL + " RM_PSEM_ENTRY_NO,  AD_FIN_FINYRID , RM_PSEM_APPROVED_YN   ";
                    SQL = SQL + "  ) ";
                    // SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "' , " + mngrclass.FinYearID + ",'" + oEntity.approve + "'";
                    SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "' , " + mngrclass.FinYearID + ",'N'";
                    SQL = SQL + " )";

                    sSQLArray.Add(SQL);

                }


                if (oEntity.approve == "Y")
                {
                    sRetun = InsertApprovalQrs(oEntity, mngrclassobj);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTPSE", oEntity.txtEntryNo, Autogen, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.ddlBranch, sAtuoGenBranchDocNumber);


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

        private string InsertApprovalQrs ( PhysicalStockEntryEntity oEntity, object mngrclassobj )
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

        private string InsertDetails ( PhysicalStockEntryEntity oEntity, List<PhysicalStockEntryDetails> EntityDetails, object mngrclassobj )
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



        private string InsertProductConsumDetails ( PhysicalStockEntryEntity oEntity, List<PhysicalStockConsuProductDtls> EntityProductconsumDetails, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                int liSlNo = 0;
                {
                    foreach (var Data in EntityProductconsumDetails)
                    {
                        liSlNo = liSlNo + 1;
                        lobjFactor = 1;

                        SQL = "";
                        SQL = " insert into  ";
                        SQL = SQL + " RM_PHYSICAL_STOCK_PROD_CONS_DTLS  ";
                        SQL = SQL + " (   ";
                        SQL = SQL + "    RM_PSEM_ENTRY_NO, AD_FIN_FINYRID, RM_PSEM_ENTRY_DATE,  ";
                        SQL = SQL + "   SALES_STN_STATION_CODE, TECH_PM_PRODUCT_CODE, RM_RMM_RM_CODE,  ";
                        ////SQL = SQL + "   TECH_PM_COSTING_OPTION_NOS_MSQ, ";
                        ////SQL = SQL + "    TECH_PLNT_NO_OF_PCS_PER_MIX, TECH_PLNT_MSQ_METER_PER_MIX,  ";
                        ////SQL = SQL + "   TECH_PLNT_LINER_METR_PER_MIX, ";
                        SQL = SQL + "    FG_QTY_PRODUCED, ";
                        //SQL = SQL + "   TECH_PM_DMG_QTY , ";
                        SQL = SQL + "   FG_FNG_WEIGHT_FROM,  ";
                        SQL = SQL + "   FG_FNG_WEIGHT_ACTUAL, FG_FNG_WEIGHT_FROM_CONVERTED, FG_FNG_WEIGHT_ACTUAL_CONVERTED,  ";
                        SQL = SQL + "   RM_PSED_AVG_PRICE_NEW, FG_FNG_COST_OF_PRODUCTION_RATE ";
                        SQL = SQL + "  ) ";


                        SQL = SQL + " VALUES ( '" + oEntity.txtEntryNo + "', " + mngrclass.FinYearID + ",'" + oEntity.dtpEntryDate.ToString("dd-MMM-yyyy") + "' ,";
                        SQL = SQL + " '" + Data.StationCode + "' , '" + Data.ProductCode + "','" + Data.RmCode + "',";

                        ///     SQL = SQL + " '" + Data.CostingOption   + "' ," + Data.dPicesPerMix  + "," + Data.dMeterSquarePerMix  + ",";
                        //SQL = SQL + " " + Data.dPLinerMeterMix + " ,";
                        SQL = SQL + " " + Data.dProducedQty + ",";
                        //    SQL = SQL + " " + Data.dDamagedQty + ",";
                        SQL = SQL + " " + Data.dTheoriticalQty + ",";
                        SQL = SQL + " " + Data.dConsumedQty + " ," + Data.dTheoriticalConvertedQty + "," + Data.dConsumedConvertedQty + ",";
                        SQL = SQL + " " + Data.dAverageRate + " ," + Data.dCostProductionRate + "  ";
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



        private string InsertPrecastConsumDetails ( PhysicalStockEntryEntity oEntity, List<PhysicalStockConsuPrecastDtls> EntityConsPrecastDtls, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                int liSlNo = 0;
                {
                    foreach (var Data in EntityConsPrecastDtls)
                    {
                        liSlNo = liSlNo + 1;
                        lobjFactor = 1;

                        SQL = "";
                        SQL = " INSERT INTO RM_PHYSICAL_STOCK_PROJ_DETAILS (";
                        SQL = SQL + " RM_PSPRM_ENTRY_NO, RM_PSPD_SL_NO,SALES_PM_PROJECT_CODE, RM_RMM_RM_CODE, ";
                        SQL = SQL + " RM_UM_UOM_CODE_MEASURED, RM_PSPD_FACTOR, RM_UM_UOM_CODE_BASIC, ";
                        SQL = SQL + " AD_CM_COMPANY_CODE, AD_FIN_FINYRID, RM_PSPD_DELETESTATUS, ";
                        SQL = SQL + " RM_PSPD_CONSUM_THEORITICAL, RM_PSPD_CONSUMPTION,GL_PRODUCT_TYPE_CODE,RM_STISU_ONSITE_OR_FACTORY,";
                        SQL = SQL + " PC_BUD_RESOURCE_CODE,RM_RMM_CONS_ACC_CODE";
                        SQL = SQL + "  ) ";
                        SQL = SQL + " VALUES ( '" + oEntity.ActualPltEntryNo + "', " + liSlNo + ",'" + Data.ProjectCode + "' ,'" + Data.RmCode + "' ,";
                        SQL = SQL + " '" + Data.RmUomMeasured + "' , " + lobjFactor + " ,'" + Data.RmUomCodeBasic + "',";
                        SQL = SQL + " '" + mngrclass.CompanyCode + "'," + mngrclass.FinYearID + ",0,";
                        SQL = SQL + " " + Data.RmConsumTheoritical + "," + Data.RmConsumption + ", '" + Data.ProductType + "','" + Data.RMIssueTo + "', ";
                        SQL = SQL + " '" + Data.RMBudgetResource + "','" + Data.RmConsumptionAccCode + "' ";
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


        public string ModifySql ( PhysicalStockEntryEntity oEntity, List<PhysicalStockEntryDetails> EntityDetails,
            List<PhysicalStockConsuPrecastDtls> EntityConsPrecastDtls, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();
                // THIS IS USED FOR THE RATE REFEERSH TRIGGER, .. HOWEFVER FOR THE PROUDCT CONSUMPTINO THERE IS NO CHANGES THEREFOR NO NEED TO DELET // JOMY 
                SQL = "Delete From RM_PHYSICAL_STOCK_PROD_RATE_TR WHERE RM_PSEM_ENTRY_NO = '" + oEntity.txtEntryNo + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);

                SQL = "Delete From RM_PHYSICAL_STOCK_DETAILS WHERE RM_PSEM_ENTRY_NO = '" + oEntity.txtEntryNo + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);


                SQL = " update RM_PHYSICAL_STOCK_MASTER set ";
                SQL = SQL + " RM_PSEM_ENTRY_DATE ='" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "' , RM_PSEM_FROM_DATE ='" + Convert.ToDateTime(oEntity.dtpFromDate).ToString("dd-MMM-yyyy") + "', ";
                SQL = SQL + " RM_PSEM_TO_DATE ='" + Convert.ToDateTime(oEntity.dtpToDate).ToString("dd-MMM-yyyy") + "' , RM_PSEM_INTERVAL_DAYs =" + Convert.ToInt32(oEntity.txtNoOfDays) + ", RM_PSEM_REMARKS ='" + oEntity.txtRemarks + "' , ";
                SQL = SQL + " RM_PSEM_UPDATEDBY = '" + mngrclass.UserName + "' , RM_PSEM_UPDATEDDATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " RM_PSEM_APPROVED = '" + oEntity.approve + "' , ";
                SQL = SQL + " RM_PSEM_APPROVEDBY ='" + oEntity.txtApprovedBy + "',";
                SQL = SQL + " RM_PSEM_VERIFIED= '" + oEntity.varify + "',RM_PSEM_VERIFIEDBY ='" + oEntity.txtVerifiedBy + "',GL_COSM_ACCOUNT_CODE='" + oEntity.CostCenter + "', ";
                SQL = SQL + " RM_PSEM_VERIFIED_ACC_YN ='" + oEntity.verifyAcc + "',RM_PSEM_VERIFIED_ACC_BY='" + oEntity.txtVerifiedByAcc + "', RM_PSEM_VERIFIED_ACC_DATE=TO_DATE('" + oEntity.VerifyAccDatetime + "','DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + " RM_PSPRM_ENTRY_NO='" + oEntity.ActualPltEntryNo + "',RM_PSPRM_AD_FINYRID=" + oEntity.ActualPltEntryFinid + " ";
                SQL = SQL + " where RM_PSEM_ENTRY_NO = '" + oEntity.txtEntryNo + "' and AD_FIN_FINYRID =" + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);




                sRetun = string.Empty;

                sRetun = InsertDetails(oEntity, EntityDetails, mngrclassobj);

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                if (EntityConsPrecastDtls.Count > 0)
                {
                    //SQL = " update RM_PHYSICAL_STOCK_PROJ_MASTER set ";
                    //SQL = SQL + " RM_PSPR_ENTRY_DATE ='" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "' , RM_PSPR_FROM_DATE ='" + Convert.ToDateTime(oEntity.dtpFromDate).ToString("dd-MMM-yyyy") + "', ";
                    //SQL = SQL + " RM_PSPR_TO_DATE ='" + Convert.ToDateTime(oEntity.dtpToDate).ToString("dd-MMM-yyyy") + "' , RM_PSPR_INTERVAL_DAYs =" + Convert.ToInt32(oEntity.txtNoOfDays) + ", RM_PSPR_REMARKS ='" + oEntity.txtRemarks + "' , ";
                    //SQL = SQL + " RM_PSPR_UPDATEDBY = '" + mngrclass.UserName + "' , RM_PSPR_UPDATEDDATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                    //SQL = SQL + " RM_PSPR_APPROVED = '" + oEntity.approve + "' , ";
                    //SQL = SQL + " RM_PSPR_APPROVEDBY ='" + oEntity.txtApprovedBy + "',";
                    //SQL = SQL + "RM_PSPR_VERIFIED= '" + oEntity.varify + "' ,RM_PSPR_VERIFIEDBY ='" + oEntity.txtVerifiedBy + "',GL_COSM_ACCOUNT_CODE='" + oEntity.CostCenter + "' ";

                    //SQL = SQL + " where RM_PSPRM_ENTRY_NO = '" + oEntity.ActualPltEntryNo + "' and AD_FIN_FINYRID ='" + oEntity.ActualPltEntryFinid + "' ";

                    //sSQLArray.Add(SQL);


                    //SQL = "Delete From RM_PHYSICAL_STOCK_PROJ_DETAILS WHERE RM_PSPRM_ENTRY_NO = '" + oEntity.ActualPltEntryNo + "' and AD_FIN_FINYRID='" + oEntity.ActualPltEntryFinid + "' ";

                    //sSQLArray.Add(SQL);

                    //sRetun = string.Empty;



                    // incase if any one changes the rated then we can re psot it 
                    SQL = "Delete From RM_PHYSICAL_STOCK_PROJ_RATE_TR WHERE RM_PSEM_ENTRY_NO = '" + oEntity.ActualPltEntryNo + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";

                    sSQLArray.Add(SQL);


                    SQL = "Insert into RM_PHYSICAL_STOCK_PROJ_RATE_TR ";
                    SQL = SQL + "   (RM_PSEM_ENTRY_NO, AD_FIN_FINYRID, RM_PSEM_APPROVED_YN";
                    SQL = SQL + "  ) ";
                    SQL = SQL + " VALUES ('" + oEntity.ActualPltEntryNo + "' , " + mngrclass.FinYearID + ",'Y'";
                    SQL = SQL + " )";

                    sSQLArray.Add(SQL);

                }
                sRetun = string.Empty;


                SQL = " INSERT INTO RM_PHYSICAL_STOCK_PROD_RATE_TR (";
                SQL = SQL + " RM_PSEM_ENTRY_NO,  AD_FIN_FINYRID , RM_PSEM_APPROVED_YN   ";
                SQL = SQL + "  ) ";
                SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "' , " + mngrclass.FinYearID + ",'N'";
                //  SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "' , " + mngrclass.FinYearID + ",'" + oEntity.approve + "'";
                SQL = SQL + " )";


                sSQLArray.Add(SQL);


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

        public string DeleteSql ( string txtEntryNo, string TxtActConsumption, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();


                SQL = "Delete From RM_PHYSICAL_STOCK_PROD_RATE_TR WHERE RM_PSEM_ENTRY_NO = '" + txtEntryNo + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                SQL = "Delete From RM_PHYSICAL_STOCK_PROJ_RATE_TR WHERE RM_PSEM_ENTRY_NO = '" + TxtActConsumption + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);
                SQL = "Delete From RM_PHYSICAL_STOCK_PROJ_Master WHERE RM_PSPRM_ENTRY_NO = '" + TxtActConsumption + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);


                //--- JOMY ADDED FOR BLOCK  FACTORY 18 MAR 2022 
                SQL = "Delete From RM_PHYSICAL_STOCK_PROD_CONS_DTLS WHERE RM_PSEM_ENTRY_NO = '" + txtEntryNo + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);
                // insdie the trigger its delete BL_COSTOF_PRODUCT_PHYSTK_DTLS   //--- JOMY ADDED FOR BLOCK  FACTORY 18 MAR 2022 



                SQL = "Delete From RM_PHYSICAL_STOCK_DETAILS WHERE RM_PSEM_ENTRY_NO = '" + txtEntryNo + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);


                SQL = "Delete From RM_PHYSICAL_STOCK_PROJ_DETAILS WHERE RM_PSPRM_ENTRY_NO = '" + TxtActConsumption + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_PHYSICAL_STOCK_MASTER WHERE RM_PSEM_ENTRY_NO = '" + txtEntryNo + "' and AD_FIN_FINYRID =" + mngrclass.FinYearID + "";

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

        public string DeleteRowSql ( string sEntryNo, object mngrclassobj )
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
        public DataSet FnFillGrid ( )
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


        public DataSet fnCountStkNo ( string dtpToDate, string ddlstation, string ddlBranch )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " select count(*) StkNo from RM_PHYSICAL_STOCK_MASTER where RM_PSEM_TO_DATE > '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " and SALES_STN_STATION_CODE ='" + ddlstation + "'";
                SQL = SQL + " and AD_BR_CODE ='" + ddlBranch + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }



        public DataSet FetchPhysicalStockEntryDetails ( string EntryNo, string FinId )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                ////SQL = " SELECT ";
                ////SQL = SQL + " RM_PSEM_ENTRY_NO, RM_PSEM_ENTRY_DATE, RM_PSEM_FROM_DATE, ";
                ////SQL = SQL + " RM_PSEM_TO_DATE, RM_PSEM_INTERVAL_DAYS,";
                ////SQL = SQL + "RM_PHYSICAL_STOCK_MASTER. SALES_STN_STATION_CODE,SALES_STN_STATION_NAME,";
                ////SQL = SQL + " RM_PHYSICAL_STOCK_MASTER.AD_BR_CODE,AD_BR_NAME, ";
                ////SQL = SQL + " HR_DEPT_DEPT_CODE, RM_PSEM_REMARKS, RM_PSEM_DOC_STATUS, ";
                ////SQL = SQL + " AD_CM_COMPANY_CODE, AD_FIN_FINYRID, RM_PSEM_CREATEDBY, ";
                ////SQL = SQL + " RM_PSEM_CREATEDDATE, RM_PSEM_UPDATEDBY, RM_PSEM_UPDATEDDATE, ";
                ////SQL = SQL + " RM_PSEM_DELETESTATUS, RM_PSEM_APPROVED, RM_PSEM_APPROVEDBY ,";
                ////SQL = SQL + " RM_PSEM_VERIFIED, RM_PSEM_VERIFIEDBY,RM_PHYSICAL_STOCK_MASTER.GL_COSM_ACCOUNT_CODE,RM_PSPRM_ENTRY_NO ,RM_PSPRM_AD_FINYRID ,";
                ////SQL = SQL + "  AD_BR_CODE_FROM  , BRANCH_FROM.AD_BR_NAME AD_BR_NAME_FROM ,   SALES_STN_STATION_CODE_FROM ,  ST_FROM.SALES_STN_STATION_NAME SALES_STN_STATION_NAME_FROM , RM_PSEM_ENTRY_NO_FROM    ";

                ////SQL = SQL + " FROM RM_PHYSICAL_STOCK_MASTER,SL_STATION_BRANCH_MAPP_DTLS_VW  ,AD_BRANCH_MASTER  BRANCH_FROM  ,SL_STATION_MASTER ST_FROM  ";

                ////SQL = SQL + " WHERE RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO='" + EntryNo + "'";
                ////SQL = SQL + " And RM_PHYSICAL_STOCK_MASTER.AD_FIN_FINYRID=" + FinId + " ";
                ////SQL = SQL + " AND RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE=SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE(+)";
                ////SQL = SQL + " AND RM_PHYSICAL_STOCK_MASTER.AD_BR_CODE=SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE(+)";

                ////SQL = SQL + " AND RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE_FROM=ST_FROM.SALES_STN_STATION_CODE(+)";
                ////SQL = SQL + " AND RM_PHYSICAL_STOCK_MASTER.AD_BR_CODE_FROM=BRANCH_FROM.AD_BR_CODE(+)";


                SQL
                = "    SELECT  " +
                "    RM_PSEM_ENTRY_NO, RM_PSEM_ENTRY_DATE, RM_PSEM_FROM_DATE,  " +
                "    RM_PSEM_TO_DATE, RM_PSEM_INTERVAL_DAYS, " +
                "    RM_PHYSICAL_STOCK_MASTER. SALES_STN_STATION_CODE,SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME, " +
                "    RM_PHYSICAL_STOCK_MASTER.AD_BR_CODE,SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME,  " +
                "    RM_PHYSICAL_STOCK_MASTER.HR_DEPT_DEPT_CODE, RM_PSEM_REMARKS, RM_PSEM_DOC_STATUS,  " +
                "    RM_PHYSICAL_STOCK_MASTER.AD_CM_COMPANY_CODE, AD_FIN_FINYRID, RM_PHYSICAL_STOCK_MASTER.RM_PSEPM_CONSUMEDQTYTYPE ,  RM_PSEM_CREATEDBY,  " +
                "    RM_PSEM_CREATEDDATE, RM_PSEM_UPDATEDBY, RM_PSEM_UPDATEDDATE,  " +
                "    RM_PSEM_DELETESTATUS, RM_PSEM_APPROVED, RM_PSEM_APPROVEDBY , " +
                "    RM_PSEM_VERIFIED, RM_PSEM_VERIFIEDBY,RM_PHYSICAL_STOCK_MASTER.GL_COSM_ACCOUNT_CODE,RM_PSPRM_ENTRY_NO ,RM_PSPRM_AD_FINYRID,   " +
                "     RM_PSEM_VERIFIED_ACC_YN ,RM_PSEM_VERIFIED_ACC_BY, RM_PSEM_VERIFIED_ACC_DATE " +
                "    FROM RM_PHYSICAL_STOCK_MASTER,SL_STATION_BRANCH_MAPP_DTLS_VW  ";
                SQL = SQL + "  WHERE  RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE=SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE(+) ";
                SQL = SQL + "    AND RM_PHYSICAL_STOCK_MASTER.AD_BR_CODE=SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE(+) ";

                SQL = SQL + " and  RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO='" + EntryNo + "'";
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

        public DataSet FetchDetails ( string EntryNo, string FinId )
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




        public DataSet FetchDetailsActualproductconsumtionBlockFactory ( string EntryNo, string FinId )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_PSEM_ENTRY_NO,     AD_FIN_FINYRID,  SALES_STN_STATION_CODE, ";
                SQL = SQL + "       RM_PHYSICAL_STOCK_PROD_CONS_DTLS.TECH_PM_PRODUCT_CODE, TECH_PM_PRODUCT_DESCRIPTION  , ";
                SQL = SQL + "       RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE, ";
                SQL = SQL + "       RM_PHYSICAL_STOCK_PROD_CONS_DTLS.RM_RMM_RM_CODE,  RM_RMM_RM_DESCRIPTION  , ";
                SQL = SQL + "       RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE BASIC_UOM ,   RM_UM_UOM_DESC  BASIC_UOM_DESC, ";
                //SQL = SQL + "       RM_PHYSICAL_STOCK_PROD_CONS_DTLS.TECH_PM_COSTING_OPTION_NOS_MSQ, ";
                //SQL = SQL + "       TECH_PLNT_NO_OF_PCS_PER_MIX, ";
                //SQL = SQL + "       TECH_PLNT_MSQ_METER_PER_MIX, ";
                //SQL = SQL + "       TECH_PLNT_LINER_METR_PER_MIX, ";
                SQL = SQL + "       FG_QTY_PRODUCED FG_QTY_PRODUCED ,  ";
                //SQL = SQL + "     RM_PHYSICAL_STOCK_PROD_CONS_DTLS.TECH_PM_DMG_QTY , ";
                SQL = SQL + "        FG_FNG_WEIGHT_FROM THEO_QTY , FG_FNG_WEIGHT_ACTUAL CONS_QTY , ";
                SQL = SQL + "       FG_FNG_WEIGHT_FROM_CONVERTED THEO_CONVERTED_QTY ,    FG_FNG_WEIGHT_ACTUAL_CONVERTED CONS_CONVERTED_QTY , ";
                SQL = SQL + "       RM_PSED_AVG_PRICE_NEW,  RM_PSED_CONSUMPTION_AMOUNT , ";
                SQL = SQL + "       FG_FNG_COST_OF_PRODUCTION_RATE   ";
                SQL = SQL + "  FROM  ";
                SQL = SQL + "            RM_PHYSICAL_STOCK_PROD_CONS_DTLS , BL_PRODUCT_MASTER ,  ";
                SQL = SQL + "            RM_RAWMATERIAL_MASTER , RM_UOM_MASTER ";
                SQL = SQL + "            where RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE =  RM_UOM_MASTER.RM_UM_UOM_CODE   ";
                SQL = SQL + "            and RM_PHYSICAL_STOCK_PROD_CONS_DTLS.RM_RMM_RM_CODE =  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                SQL = SQL + "            and RM_PHYSICAL_STOCK_PROD_CONS_DTLS.TECH_PM_PRODUCT_CODE =  BL_PRODUCT_MASTER.TECH_PM_PRODUCT_CODE  ";
                SQL = SQL + " And RM_PHYSICAL_STOCK_PROD_CONS_DTLS.RM_PSEM_ENTRY_NO='" + EntryNo + "'";
                SQL = SQL + " And RM_PHYSICAL_STOCK_PROD_CONS_DTLS.AD_FIN_FINYRID=" + FinId + " ";
                SQL = SQL + " ORDER BY RM_PHYSICAL_STOCK_PROD_CONS_DTLS. TECH_PM_PRODUCT_CODE ,RM_PHYSICAL_STOCK_PROD_CONS_DTLS.RM_RMM_RM_CODE ";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }




        public DataSet FetchDetailsActualproductconsumtionPrecastFactory ( string EntryNo, string FinId )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " select  RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPRM_ENTRY_NO ,  RM_PHYSICAL_STOCK_PROJ_DETAILS.AD_FIN_FINYRID ,  ";
                SQL = SQL + "         RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_SL_NO Sl_No,  ";
                SQL = SQL + "         RM_PHYSICAL_STOCK_PROJ_DETAILS.SALES_PM_PROJECT_CODE PROJECT_CODE,  ";
                SQL = SQL + "         PC_ENQUIRY_MASTER.SALES_PM_PROJECT_NAME PROJECT_NAME,  ";
                SQL = SQL + "         RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE RM_Code,  ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_Description,  ";
                SQL = SQL + "         RM_RMM_RM_TYPE RM_TYPE,  ";
                SQL = SQL + "         RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_MEASURED,  ";
                SQL = SQL + "         RM_UOM_MASTER.RM_UM_UOM_DESC,  ";
                SQL = SQL + "         RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_FACTOR,  ";
                SQL = SQL + "         RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_BASIC,  ";
                SQL = SQL + "         RM_PSPD_CONSUM_THEORITICAL, RM_PSPD_CONSUMPTION  ,   RM_PSED_AVG_PRICE_NEW, RM_PSED_CONSUMPTION_AMOUNT,   ";
                SQL = SQL + "         RM_PHYSICAL_STOCK_PROJ_DETAILS.GL_PRODUCT_TYPE_CODE  ,GL_PRODUCT_TYPE_MASTER.GL_PRODUCT_TYPE_DESC, ";
                SQL = SQL + "         RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_STISU_ONSITE_OR_FACTORY, ";
                SQL = SQL + "         RM_PHYSICAL_STOCK_PROJ_DETAILS.PC_BUD_RESOURCE_CODE, ";
                SQL = SQL + "         RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_CONS_ACC_CODE   ";
                SQL = SQL + "    FROM RM_PHYSICAL_STOCK_PROJ_Master,RM_PHYSICAL_STOCK_PROJ_DETAILS,RM_UOM_MASTER, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, PC_ENQUIRY_MASTER, GL_PRODUCT_TYPE_MASTER ";
                SQL = SQL + "   WHERE RM_PHYSICAL_STOCK_PROJ_Master.RM_PSPRM_ENTRY_NO=RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPRM_ENTRY_NO ";
                SQL = SQL + "         AND RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_MEASURED = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_DETAILS.SALES_PM_PROJECT_CODE = PC_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE(+) ";
                SQL = SQL + "AND RM_PHYSICAL_STOCK_PROJ_DETAILS.GL_PRODUCT_TYPE_CODE  =   GL_PRODUCT_TYPE_MASTER.GL_PRODUCT_TYPE_CODE  ";
                SQL = SQL + " AND  ( RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_CONSUM_THEORITICAL +  RM_PHYSICAL_STOCK_PROJ_DETAILS .RM_PSPD_CONSUMPTION  )   > 0     ";
                SQL = SQL + " And RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPRM_ENTRY_NO='" + EntryNo + "'";
                SQL = SQL + " And RM_PHYSICAL_STOCK_PROJ_DETAILS.AD_FIN_FINYRID=" + FinId + " ";
                SQL = SQL + " ORDER BY RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_SL_NO, RM_PHYSICAL_STOCK_PROJ_DETAILS.SALES_PM_PROJECT_CODE  ,   RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE asc   ";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }



        public DataSet FetchEntryDetailsforRate ( string EntryNo )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " select ";
                SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS.RM_PSED_SL_NO Sl_No,";
                SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE  ,";

                SQL = SQL + " RM_PSED_RECEIVED, RM_PSED_AVG_PRICE, RM_PSED_RECEIVED_AMOUNT, RM_PSED_AVG_PRICE_NEW  ";

                SQL = SQL + " from ";
                SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS ";

                SQL = SQL + " where ";
                SQL = SQL + "   RM_PHYSICAL_STOCK_DETAILS.RM_PSEM_ENTRY_NO='" + EntryNo + "'";


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

        public DataSet FetchPrintData ( string txtEntryNo, string iDocFinYear )
        {
            DataSet dsData = new DataSet("RMPHYSTOCKPRINT");

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "  ";

                SQL = " SELECT  ";
                SQL = SQL + "   RM_PSEM_ENTRY_NO, AD_FIN_FINYRID, RM_PSEM_ENTRY_DATE, RM_PSEM_FROM_DATE, RM_PSEM_TO_DATE,  ";
                SQL = SQL + "   RM_PSEM_INTERVAL_DAYS, RM_PSEM_CREATEDBY, RM_PSEM_VERIFIEDBY, RM_PSEM_APPROVED, RM_PSEM_APPROVEDBY,  ";
                SQL = SQL + "   RM_PSEM_VERIFIED_ACC_YN ,RM_PSEM_VERIFIED_ACC_BY, RM_PSEM_VERIFIED_ACC_DATE, ";
                SQL = SQL + "   RM_PSEM_REMARKS, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, RM_PSED_SL_NO, RM_RMM_RM_CODE,  ";
                SQL = SQL + "   RM_RMM_RM_DESCRIPTION,RM_RMM_RM_TYPE , RM_UM_UOM_CODE_MEASURED, RM_PSED_STOCK_ON_HAND, RM_PSED_OPEN_RATE, RM_PSED_OPEN_AMOUNT,  ";
                SQL = SQL + "   RM_PSED_RVD_QTY, RM_PSED_RVD_RATE, RM_PSED_RVD_AMOUNT, RM_PSED_STKTRN_IN_QTY, RM_PSED_STKTRN_IN_RATE,  ";
                SQL = SQL + "   RM_PSED_STKTRN_IN_AMOUNT, RM_PSED_STKTRN_OUT_QTY, RM_PSED_STKTRN_OUT_RATE, RM_PSED_STKTRN_OUT_AMOUNT, RM_PSED_SALES_QTY,  ";
                SQL = SQL + "   RM_PSED_SALES_RATE, RM_PSED_SALES_AMOUNT, RM_PSED_RECEIVED, RM_PSED_RECEIVED_AMOUNT, RM_PSED_AVG_PRICE,  ";
                SQL = SQL + "   RM_PSED_AVG_PRICE_NEW, RM_PSED_CONSUM_THEORITICAL, RM_PSED_ACTUALQTY_FROM_PLANT, RM_PSED_CONSUMPTION, RM_PSED_CONSUMPTION_AMOUNT,  ";
                SQL = SQL + "   RM_PSED_CLOSING, RM_PSED_CLOSING_AMOUNT, RM_PSED_VARIANCE, RM_PSED_VARIANCEAMOUNT , ";
                SQL = SQL + "       MASTER_BRANCH_CODE ,   MASTER_BRANCH_NAME , ";
                SQL = SQL + "       MASTER_BRANCHDOC_PREFIX,  MASTER_BRANCH_POBOX,  MASTER_BRANCH_ADDRESS, ";
                SQL = SQL + "       MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE,  MASTER_BRANCH_FAX, ";
                SQL = SQL + "       MASTER_BRANCH_SPONSER_NAME,  MASTER_BRANCH_TRADE_LICENSE_NO,  MASTER_BRANCH_EMAIL_ID, ";
                SQL = SQL + "       MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER ";

                SQL = SQL + "  FROM  ";
                SQL = SQL + "     RM_PHYSTK_ENTRY_PRINT_VW  ";
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

    public class PhysicalStockEntryEntity
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
        public string ddlBranch
        {
            get; set;
        }
        public string ConsumedQtyEntryCallModule
        {
            get; set;
        }


        public string StationIssueFrom
        {
            get;
            set;
        }
        public string BranchFrom
        {
            get; set;
        }

        public string IssueFromEntryRefNo
        {
            get; set;
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
        public string txtVerifiedByAcc
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

        public string verifyAcc
        {
            get;
            set;
        }

        public string VerifyAccDatetime
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

    public class PhysicalStockEntryDetails
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

    public class PhysicalStockConsuProductDtls
    {

        public string StationCode { get; set; }
        public string ProductCode { get; set; }
        public string RmCode { get; set; }
        public string CostingOption { get; set; }


        public double dPicesPerMix { get; set; }
        public double dMeterSquarePerMix { get; set; }
        public double dPLinerMeterMix { get; set; }

        public double dProducedQty { get; set; }
        public double dDamagedQty { get; set; }

        public double dTheoriticalQty { get; set; }
        public double dConsumedQty { get; set; }
        public double dTheoriticalConvertedQty { get; set; }
        public double dConsumedConvertedQty { get; set; }
        public double dAverageRate { get; set; }
        public double dCostProductionRate { get; set; }


    }

    public class PhysicalStockConsuPrecastDtls
    {

        public string EntryNo { get; set; }
        public string ProjectCode { get; set; }
        public string RmCode { get; set; }
        public string RmUomMeasured { get; set; }
        public string RmUomCodeBasic { get; set; }
        public string ProductType { get; set; }
        public string RMIssueTo { get; set; }
        public string RMBudgetResource { get; set; }
        public string RmConsumptionAccCode { get; set; }


        public DateTime RmEntryDate
        {
            get;
            set;
        }
        public DateTime RmFromDate
        {
            get;
            set;
        }
        public DateTime RmToDate
        {
            get;
            set;
        }

        public double RmFactor { get; set; }
        public double RmConsumTheoritical { get; set; }
        public double RmConsumption { get; set; }
        public double RmAvgPriceNew { get; set; }
        public double RmConsumptionAmt { get; set; }

    }

    #endregion
    //===========================================END OF ENITY
}
