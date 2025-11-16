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

/// <summary>
/// Created By      :   Jins Mathew
/// Created On      :   18-Feb-2013
/// </summary>

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class SalesRMLogic
    {
        #region "Global Declaration"

        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public string Branch
        {
            get;
            set;
        }

        #endregion

        #region "GetDefaults"

        public DataSet GetDefaultDept ( )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_IP_PARAMETER_VALUE FROM RM_DEFUALTS_GL_PARAMETERS ";
                SQL = SQL + "   WHERE RM_IP_PARAMETER_DESC ='DEPARTMENT' ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataTable GetBranch ( object mngrclassobj, string stcode )
        {
            DataTable sReturn = new DataTable();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = "  SELECT AD_BR_CODE Code,AD_BR_NAME Name  FROM  ";
                SQL = SQL + "  AD_BRANCH_MASTER WHERE AD_BR_DELETESTATUS =0 ";

                //if (mngrclass.UserName == "ADMIN")
                //{
                //    SQL = SQL + " AND AD_BR_CODE not in (  SELECT     AD_BR_CODE FROM    WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + mngrclass.UserName + "')";
                //}
                // SQL = SQL + " AND SALES_RAW_MATERIAL_STATION ='Y'";

                if (!string.IsNullOrEmpty(stcode))
                {
                    SQL = SQL + " AND AD_BR_CODE='" + stcode + "' ";
                }

                //SQL = SQL + " AND SALES_STN_STATION_STATUS='A' ";


                SQL = SQL + " ORDER BY AD_BR_NAME ";

                sReturn = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet GetStation ( object mngrclassobj, string stcode, string BranchCode )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = "  SELECT SALES_STN_STATION_CODE Code,SALES_STN_STATION_NAME Name ,GL_COSM_ACCOUNT_CODE COSMACCODE FROM  ";
                SQL = SQL + "  SL_STATION_MASTER WHERE  SALES_STN_DELETESTATUS=0 ";

                if (mngrclass.UserName == "ADMIN")
                {
                    SQL = SQL + " AND SALES_STN_STATION_CODE not in (  SELECT     SALES_STN_STATION_CODE FROM    WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + mngrclass.UserName + "')";
                }
                // SQL = SQL + " AND SALES_RAW_MATERIAL_STATION ='Y'";

                if (!string.IsNullOrEmpty(stcode))
                {
                    SQL = SQL + " AND SALES_STN_STATION_CODE='" + stcode + "' ";
                }
                if (!string.IsNullOrEmpty(BranchCode))
                {
                    SQL = SQL + " AND AD_BR_CODE='" + BranchCode + "' ";
                }

                SQL = SQL + " AND SALES_STN_STATION_STATUS='A' ";


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

        public DataSet GetDestination ( )
        {
            DataSet dsReturn = new DataSet();

            SqlHelper ClsSqlhelper = new SqlHelper();

            try
            {

                SQL = " SELECT TECH_PLM_PLANT_CODE CODE, TECH_PLM_PLANT_NAME NAME ";

                SQL = SQL + " FROM TECH_PLANT_MASTER ";

                SQL = SQL + " WHERE TECH_PLM_WASHING_PLANT ='N' AND  TECH_PLM_RM_RCPT_DESITINATION='Y' AND  TECH_PLM_PRODUTION_PLANT='N' ";

                SQL = SQL + " order by  TECH_PLM_PLANT_NAME asc";

                dsReturn = ClsSqlhelper.GetDataset(SQL);

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
            return dsReturn;
        }

        public DataSet GetDefault ( string Type, string Code )
        {
            DataSet dsReturn = new DataSet();

            SqlHelper ClsSqlhelper = new SqlHelper();

            try
            {
                if (Type == "STATION")
                {
                    SQL = " SELECT SALES_STN_STATION_CODE CODE, SALES_STN_STATION_NAME NAME ";
                    SQL = SQL + " FROM SL_STATION_MASTER ";
                    SQL = SQL + " WHERE SALES_STN_DELETESTATUS=0  and SALES_RAW_MATERIAL_STATION ='Y'  order by SALES_STN_STATION_NAME asc";
                }
                else if (Type == "SOURCE")
                {
                    SQL = " SELECT ";
                    SQL = SQL + "RM_SM_SOURCE_CODE CODE, RM_SM_SOURCE_DESC NAME ";
                    SQL = SQL + "FROM RM_SOURCE_MASTER  order by RM_SM_SOURCE_DESC asc ";
                }


                dsReturn = ClsSqlhelper.GetDataset(SQL);

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
            return dsReturn;
        }

        public DataSet FetchBranch ( string stcode )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT AD_BR_CODE CODE,AD_BR_NAME NAME";
                SQL = SQL + " FROM AD_BRANCH_MASTER WHERE AD_BR_DELETESTATUS=0 ";


                if (!string.IsNullOrEmpty(stcode))
                {
                    SQL = SQL + " AND AD_BR_CODE='" + stcode + "' ";
                }

                SQL = SQL + " ORDER BY AD_BR_NAME ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

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

        #endregion

        #region "Fill View"

        public DataTable FillView ( string Type, string StCode, string Date, string itemCode, object mngrclassobj )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                if (Type == "CUSTCODE")
                {
                    //SQL = " SELECT   SALES_CUS_CUSTOMER_CODE CODE,";
                    //SQL = SQL + "      SALES_CUS_CUSTOMER_NAME NAME,";
                    //SQL = SQL + "      GL_COAM_ACCOUNT_CODE ACCOUNTCODE,";

                    ////****For Filling Only Didnt Use This******
                    //SQL = SQL + "      GL_COAM_ACCOUNT_CODE UOMDesc,";
                    //SQL = SQL + "      GL_COAM_ACCOUNT_CODE RMPrice,";
                    //SQL = SQL + "      GL_COAM_ACCOUNT_CODE RM_RMM_INV_ACC_CODE,";
                    //SQL = SQL + "      GL_COAM_ACCOUNT_CODE RM_RMM_CONS_ACC_CODE,";
                    //SQL = SQL + "      GL_COAM_ACCOUNT_CODE open_qty,";
                    ////**************************************************
                    //SQL = SQL + "  GL_TAX_TYPE_CODE TAXTYPE, SALES_CUS_TAX_VAT_PERCENTAGE TAXPER , SALES_CUS_TAX_VAT_REG_NUMBER TAXREGNO, ";
                    //SQL = SQL + "  SL_CUSTOMER_MASTER.SALES_CUS_CREDIT_TERMS CREDIT_TERMS, SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_DESC  PAY_TYPE_DESC ";

                    //SQL = SQL + " FROM SL_CUSTOMER_MASTER,SL_PAY_TYPE_MASTER";
                    //SQL = SQL + " WHERE ";
                    //SQL = SQL + " SALES_CUS_CUSTOMER_STATUS = 'A'";
                    //SQL = SQL + "  AND GL_COAM_ACCOUNT_CODE IS NOT NULL";
                    //SQL = SQL + "  AND SL_CUSTOMER_MASTER.SALES_CUS_CREDIT_TERMS=SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE(+) ";
                    //SQL = SQL + " ORDER BY SALES_CUS_CUSTOMER_CODE";



                    SQL = "  SELECT   ";
                    SQL = SQL + "        SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_CODE CODE , SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_NAME NAME ,   ";
                    SQL = SQL + "        SL_CUSTOMER_MASTER.GL_COAM_ACCOUNT_CODE ACCOUNTCODE ,   ";
                    SQL = SQL + "        SALES_CUS_TAX_VAT_REG_NUMBER TAXREGNO, GL_TAX_TYPE_PER TAXPER,  ";
                    SQL = SQL + "      SL_CUSTOMER_MASTER.GL_COAM_ACCOUNT_CODE UOMDesc,";
                    SQL = SQL + "      SL_CUSTOMER_MASTER.GL_COAM_ACCOUNT_CODE RMPrice,";
                    SQL = SQL + "      SL_CUSTOMER_MASTER.GL_COAM_ACCOUNT_CODE RM_RMM_INV_ACC_CODE,";
                    SQL = SQL + "      SL_CUSTOMER_MASTER.GL_COAM_ACCOUNT_CODE RM_RMM_CONS_ACC_CODE,";
                    SQL = SQL + "      SL_CUSTOMER_MASTER.GL_COAM_ACCOUNT_CODE open_qty,";
                    SQL = SQL + "        TAX_TYPE.SALES_CUS_LABLE_VALUE TAXTYPE ,CREDIT_TERMS.SALES_CUS_LABLE_VALUE SALES_CUS_CREDIT_TERMS, ";
                    SQL = SQL + "        'CUST' GL_MSEM_ENTRY_PARTY_TYPE   ";
                    SQL = SQL + "   from  ";
                    SQL = SQL + "        gl_coa_master,  SL_CUSTOMER_MASTER ,GL_DEFAULTS_TAX_TYPE_MASTER, ";
                    SQL = SQL + "        SL_CUSTOMER_MASTER_BRANCH_DATA TAX_TYPE,SL_CUSTOMER_MASTER_BRANCH_DATA CREDIT_TERMS  ";
                    SQL = SQL + "   where  ";
                    SQL = SQL + "        gl_coam_account_status = 'ACTIVE'    ";
                    SQL = SQL + "        and   gl_coa_master.gl_coam_account_code  =  SL_CUSTOMER_MASTER.GL_COAM_ACCOUNT_CODE   ";
                    SQL = SQL + "        AND SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_CODE=TAX_TYPE.SALES_CUS_CUSTOMER_CODE ";
                    SQL = SQL + "        AND TAX_TYPE.SALES_CUS_LABLE_ID='TAX_TYPE' ";
                    SQL = SQL + "        AND TAX_TYPE.SALES_CUS_LABLE_VALUE  = GL_DEFAULTS_TAX_TYPE_MASTER.GL_TAX_TYPE_CODE (+)   ";
                    SQL = SQL + "        AND SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_CODE=CREDIT_TERMS.SALES_CUS_CUSTOMER_CODE ";
                    SQL = SQL + "        AND CREDIT_TERMS.SALES_CUS_LABLE_ID='PAYMENT_TERMS' ";
                    SQL = SQL + "        AND CREDIT_TERMS.AD_BR_CODE=TAX_TYPE.AD_BR_CODE ";
                    SQL = SQL + "        AND TAX_TYPE.AD_BR_CODE='" + Branch + "' ";
                    SQL = SQL + "        ORDER BY  SL_CUSTOMER_MASTER.GL_COAM_ACCOUNT_CODE asc   ";




                }
                if (Type == "SUPPCODE")
                {
                    //SQL = " SELECT   RM_VM_VENDOR_CODE CODE,";
                    //SQL = SQL + "      RM_VM_VENDOR_NAME NAME,";
                    //SQL = SQL + "      GL_COAM_ACCOUNT_CODE ACCOUNTCODE,";

                    ////****For Filling Only Didnt Use This******
                    //SQL = SQL + "      GL_COAM_ACCOUNT_CODE UOMDesc,";
                    //SQL = SQL + "      GL_COAM_ACCOUNT_CODE RMPrice,";
                    //SQL = SQL + "      GL_COAM_ACCOUNT_CODE RM_RMM_INV_ACC_CODE,";
                    //SQL = SQL + "      GL_COAM_ACCOUNT_CODE RM_RMM_CONS_ACC_CODE,";
                    //SQL = SQL + "      GL_COAM_ACCOUNT_CODE open_qty,";
                    ////**************************************************
                    //SQL = SQL + "     GL_TAX_TYPE_CODE TAXTYPE,RM_VM_TAX_VAT_PERCENTAGE TAXPER, RM_VM_TAX_VAT_REG_NUMBER TAXREGNO ,";
                    //SQL = SQL + "  RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS CREDIT_TERMS, SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_DESC  PAY_TYPE_DESC ";
                    //SQL = SQL + " FROM RM_VENDOR_MASTER,SL_PAY_TYPE_MASTER ";
                    //SQL = SQL + " WHERE ";
                    //SQL = SQL + " RM_VM_VENDOR_STATUS = 'A'";
                    //SQL = SQL + "  AND GL_COAM_ACCOUNT_CODE IS NOT NULL";
                    //SQL = SQL + "  AND RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS=SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE(+) ";
                    //SQL = SQL + " ORDER BY RM_VM_VENDOR_CODE";

                    SQL = "  SELECT    ";
                    SQL = SQL + "        RM_VENDOR_MASTER.RM_VM_VENDOR_CODE CODE , RM_VENDOR_MASTER.RM_VM_VENDOR_NAME NAME ,    ";
                    SQL = SQL + "        RM_VENDOR_MASTER.GL_COAM_ACCOUNT_CODE ACCOUNTCODE ,    ";
                    SQL = SQL + "        RM_VM_TAX_VAT_REG_NUMBER TAXREGNO, GL_TAX_TYPE_PER TAXPER,   ";
                    SQL = SQL + "      RM_VENDOR_MASTER.GL_COAM_ACCOUNT_CODE UOMDesc, ";
                    SQL = SQL + "      RM_VENDOR_MASTER.GL_COAM_ACCOUNT_CODE RMPrice, ";
                    SQL = SQL + "      RM_VENDOR_MASTER.GL_COAM_ACCOUNT_CODE RM_RMM_INV_ACC_CODE, ";
                    SQL = SQL + "      RM_VENDOR_MASTER.GL_COAM_ACCOUNT_CODE RM_RMM_CONS_ACC_CODE, ";
                    SQL = SQL + "      RM_VENDOR_MASTER.GL_COAM_ACCOUNT_CODE open_qty, ";
                    SQL = SQL + "        TAX_TYPE.RM_VM_LABLE_VALUE TAXTYPE ,CREDIT_TERMS.RM_VM_LABLE_VALUE RM_VM_CREDIT_TERMS,  ";
                    SQL = SQL + "        'CUST' GL_MSEM_ENTRY_PARTY_TYPE    ";
                    SQL = SQL + "   from   ";
                    SQL = SQL + "        gl_coa_master,  RM_VENDOR_MASTER ,GL_DEFAULTS_TAX_TYPE_MASTER,  ";
                    SQL = SQL + "        RM_VENDOR_MASTER_BRANCH_DATA TAX_TYPE,RM_VENDOR_MASTER_BRANCH_DATA CREDIT_TERMS   ";
                    SQL = SQL + "   where   ";
                    SQL = SQL + "        gl_coam_account_status = 'ACTIVE'     ";
                    SQL = SQL + "        and   gl_coa_master.gl_coam_account_code  =  RM_VENDOR_MASTER.GL_COAM_ACCOUNT_CODE    ";
                    SQL = SQL + "        AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE=TAX_TYPE.RM_VM_VENDOR_CODE  ";
                    SQL = SQL + "        AND TAX_TYPE.RM_VM_LABLE_ID='TAX_TYPE'  ";
                    SQL = SQL + "        AND TAX_TYPE.RM_VM_LABLE_VALUE  = GL_DEFAULTS_TAX_TYPE_MASTER.GL_TAX_TYPE_CODE (+)    ";
                    SQL = SQL + "        AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE=CREDIT_TERMS.RM_VM_VENDOR_CODE  ";
                    SQL = SQL + "        AND CREDIT_TERMS.RM_VM_LABLE_ID='PAYMENT_TERMS'  ";
                    SQL = SQL + "        AND CREDIT_TERMS.AD_BR_CODE=TAX_TYPE.AD_BR_CODE  ";
                    SQL = SQL + "        AND TAX_TYPE.AD_BR_CODE='" + Branch + "'  ";
                    SQL = SQL + "        ORDER BY  RM_VENDOR_MASTER.GL_COAM_ACCOUNT_CODE asc    ";

                }

                else if (Type == "ITEMCODE")
                {

                    SQL = "  select * from  ";
                    SQL = SQL + " ( ";

                    SQL = SQL + "  SELECT mas.rm_rmm_rm_code CODE, mas.RM_RMM_RM_DESCRIPTION NAME,";
                    SQL = SQL + " MAS.RM_UM_UOM_CODE ACCOUNTCODE,UOMMAS.RM_UM_UOM_DESC UOMDesc,";
                    SQL = SQL + " RM_RMM_INV_ACC_CODE, RM_RMM_CONS_ACC_CODE,";
                    SQL = SQL + " SUM ( NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0) - NVL (issued.iss_qty, 0) ) open_qty,";
                    SQL = SQL + " trunc  ( ";
                    SQL = SQL + " CASE when sUM ( NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0))- SUM(NVL (issued.iss_qty, 0)) > 0 THEN ";

                    SQL = SQL + "  SUM ( NVL (opening.ob_val, 0) + NVL (recd.rec_value, 0) - NVL (issued.iss_value, 0))/ SUM (NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0)-NVL (issued.iss_qty, 0) )";
                    SQL = SQL + "  when sUM ( NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0)) > 0 THEN ";
                    SQL = SQL + "  SUM ( NVL (opening.ob_val, 0) + NVL (recd.rec_value, 0))/ SUM (NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0) )";
                    SQL = SQL + "  when sUM ( NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0))- SUM(NVL (issued.iss_qty, 0)) = 0 then 0 ";
                    SQL = SQL + "  ELSE 0 END ,5 )";
                    SQL = SQL + "  RMPrice ,";

                    SQL = SQL + " SUM (";
                    SQL = SQL + " NVL (opening.ob_val, 0)";
                    SQL = SQL + " + NVL (recd.rec_value, 0)";
                    SQL = SQL + " - NVL (issued.iss_value, 0)";
                    SQL = SQL + " ) open_value,";

                    SQL = SQL + " '' TAXTYPE,0 TAXPER,'' TAXREGNO,'' CREDIT_TERMS, '' PAY_TYPE_DESC ";

                    SQL = SQL + " FROM rm_rawmaterial_master mas,RM_UOM_MASTER uommas,";
                    SQL = SQL + " rm_rawmaterial_details det,";
                    SQL = SQL + " ( ";
                    SQL = SQL + "  SELECT rm_rmm_rm_code rm_code, sales_stn_station_code stn_code,";
                    SQL = SQL + " SUM (rm_ob_qty)";
                    SQL = SQL + " ob_qty, SUM ( RM_OB_AMOUNT ) ob_val";
                    SQL = SQL + " FROM RM_OPEN_BALANCES";
                    SQL = SQL + " WHERE ad_fin_finyrid = " + mngrclass.FinYearID + " ";
                    SQL = SQL + " AND sales_stn_station_code = '" + StCode + "'";
                    SQL = SQL + " GROUP BY rm_rmm_rm_code, sales_stn_station_code ) opening,";
                    SQL = SQL + " ( ";
                    SQL = SQL + "  SELECT RM_IM_ITEM_CODE rm_code, sales_stn_station_code stn_code,";
                    SQL = SQL + " SUM (NVL (RM_STKL_RECD_QTY, 0))";
                    SQL = SQL + " rec_qty,";
                    SQL = SQL + " SUM (NVL (RM_STKL_RECD_AMT, 0) ) rec_value";
                    SQL = SQL + " FROM RM_STOCK_LEDGER";
                    SQL = SQL + " WHERE TO_DATE (TO_CHAR (rm_stkl_trans_date, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(Date).ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + " AND ad_fin_finyrid =" + mngrclass.FinYearID + "";
                    SQL = SQL + " AND sales_stn_station_code = '" + StCode + "'";
                    SQL = SQL + " GROUP BY RM_IM_ITEM_CODE , sales_stn_station_code ";
                    SQL = SQL + "  ) recd, ";
                    SQL = SQL + " ( ";
                    SQL = SQL + "  SELECT RM_IM_ITEM_CODE rm_code, sales_stn_station_code stn_code,";
                    SQL = SQL + " SUM (NVL (RM_STKL_ISSUE_QTY, 0))";
                    SQL = SQL + " iss_qty,";
                    SQL = SQL + " SUM ( NVL (RM_STKL_ISSUE_AMOUNT, 0) ) iss_value";
                    SQL = SQL + " FROM RM_STOCK_LEDGER";
                    SQL = SQL + " WHERE TO_DATE (TO_CHAR (rm_stkl_trans_date, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(Date).ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + " AND ad_fin_finyrid = " + mngrclass.FinYearID + "";
                    SQL = SQL + " AND sales_stn_station_code = '" + StCode + "'";
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
                    SQL = SQL + " AND det.sales_stn_station_code = '" + StCode + "'";
                    SQL = SQL + " AND mas.RM_UM_UOM_CODE = uommas.RM_UM_UOM_CODE (+)";
                    SQL = SQL + " AND mas.RM_RMM_RM_CODE = det.RM_RMM_RM_CODE (+)";

                    if (!string.IsNullOrEmpty(itemCode.Trim()))
                    {
                        // SQL = SQL + " AND mas.rm_rmm_rm_code not in(" + itemCode + ")";
                        SQL = SQL + " AND mas.rm_rmm_rm_code not IN('" + itemCode.Replace(",", "','") + "')";
                    }

                    SQL = SQL + " GROUP BY mas.rm_rmm_rm_code,";
                    SQL = SQL + " mas.RM_UM_UOM_CODE,";
                    SQL = SQL + " RM_RMM_INV_ACC_CODE,";
                    SQL = SQL + " RM_RMM_CONS_ACC_CODE,";
                    SQL = SQL + " rm_rmm_rm_description,";
                    SQL = SQL + " UOMMAS.RM_UM_UOM_DESC,";
                    SQL = SQL + " det.sales_stn_station_code";

                    SQL = SQL + " )     where  OPEN_QTY > 0";



                    //SQL = " SELECT            ";
                    //SQL = SQL + "       RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE CODE, ";
                    //SQL = SQL + "       RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION NAME, ";
                    //SQL = SQL + "       RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE ACCOUNTCODE, ";
                    //SQL = SQL + "        RM_UOM_MASTER.RM_UM_UOM_DESC UOMDesc, ";
                    //SQL = SQL + "       RM_RMD_PRICE RMPrice, ";
                    //SQL = SQL + "       RM_RMM_INV_ACC_CODE, ";
                    //SQL = SQL + "      RM_RMM_CONS_ACC_CODE   ";
                    //SQL = SQL + "      FROM  ";
                    //SQL = SQL + "      RM_RAWMATERIAL_MASTER ,RM_UOM_MASTER ,RM_RAWMATERIAL_DETAILS";
                    //SQL = SQL + "    where  ";
                    //SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE AND ";
                    //SQL = SQL + "      RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE = RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE  ";

                    //SQL = SQL + " AND RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE = '" + StCode + "'";
                }

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

        public DataTable FillViewSalesEntry ( string fromdate, string todate, string EntryType, object mngrclassobj )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT * from( ";
                SQL = SQL + " SELECT 'RMSALESRADIOBTN' TYPE, RM_MSM_ENTRY_NO ENTRYNO, to_char(RM_MSM_ENTRY_DATE,'DD-MON-YYYY') ENTRY_DATE, AD_FIN_FINYRID, ";
                SQL = SQL + " RM_SALES_MASTER.SALES_CUS_CUSTOMER_CODE CUSTOMER_CODE, SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_NAME CUSTOMER_NAME,";
                SQL = SQL + " RM_MSM_TOTAL_AMOUNT TOTAL_AMOUNT,";
                SQL = SQL + " RM_MSM_APPROVED, RM_MSM_APPROVED_BY APPROVED_BY, RM_MSM_REMARKS REMARKS, DECODE(RM_MSM_ENTRY_TYPE,'SL','Sales','RT','Return') ENTRY_TYPE ";
                SQL = SQL + " FROM RM_SALES_MASTER, SL_CUSTOMER_MASTER";
                SQL = SQL + " WHERE ";
                SQL = SQL + " AD_FIN_FINYRID = " + mngrclass.FinYearID + " AND RM_MSM_ENTRY_TYPE = 'SL'";
                if (EntryType == "Y")
                {
                    SQL = SQL + " AND RM_MSM_APPROVED ='Y'";
                }
                else if (EntryType == "N")
                {
                    SQL = SQL + " AND RM_MSM_APPROVED ='N'";
                }
                SQL = SQL + " AND RM_MSM_ENTRY_DATE BETWEEN '" + Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND RM_SALES_MASTER.SALES_CUS_CUSTOMER_CODE = SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_CODE";

                if (mngrclass.UserName != "ADMIN")
                {
                    SQL = SQL + " AND RM_SALES_MASTER.AD_BR_CODE in ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                }


                SQL = SQL + " UNION ALL SELECT  'RMRETURNRADIOBTN' TYPE, RM_MSM_ENTRY_NO ENTRYNO, to_char(RM_MSM_ENTRY_DATE,'DD-MON-YYYY') ENTRY_DATE, AD_FIN_FINYRID, ";
                SQL = SQL + " RM_SALES_MASTER.RM_VM_VENDOR_CODE CUSTOMER_CODE, RM_VM_VENDOR_NAME CUSTOMER_NAME,";
                SQL = SQL + " RM_MSM_TOTAL_AMOUNT TOTAL_AMOUNT,";
                SQL = SQL + " RM_MSM_APPROVED, RM_MSM_APPROVED_BY APPROVED_BY, RM_MSM_REMARKS REMARKS, DECODE(RM_MSM_ENTRY_TYPE,'SL','Sales','RT','Return') ENTRY_TYPE ";
                SQL = SQL + " FROM RM_SALES_MASTER, RM_VENDOR_MASTER";
                SQL = SQL + " WHERE ";
                SQL = SQL + " AD_FIN_FINYRID = " + mngrclass.FinYearID + " AND RM_MSM_ENTRY_TYPE = 'RT'";
                SQL = SQL + " AND RM_SALES_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                if (EntryType == "Y")
                {
                    SQL = SQL + " AND RM_MSM_APPROVED ='Y'";
                }
                else if (EntryType == "N")
                {
                    SQL = SQL + " AND RM_MSM_APPROVED ='N'";
                }
                SQL = SQL + " AND RM_MSM_ENTRY_DATE BETWEEN '" + Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";

                if (mngrclass.UserName != "ADMIN")
                {
                    SQL = SQL + " AND RM_SALES_MASTER.AD_BR_CODE in ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                }
                SQL = SQL + " )";


                if (mngrclass.UserName != "ADMIN")
                {
                    SQL = SQL + " where TYPE in ( ";
                    SQL = SQL + " SELECT   AD_AF_ID";
                    SQL = SQL + "  FROM ad_mail_approval_users";
                    SQL = SQL + " WHERE UPPER (ad_um_userid) = '" + mngrclass.UserName + "' )";
                }

                SQL = SQL + " order by 1 desc";

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

        public DataTable FillDriverAccountView ( )//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE CAT_CODE, HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_NAME CAT_NAME, ";
                SQL = SQL + " HR_EMPLOYEE_MASTER.HR_EMP_TELNO TEL_NO, HR_EMPLOYEE_MASTER.HR_EMP_MOBILENO MOBILE_NO ";
                SQL = SQL + " FROM HR_EMPLOYEE_MASTER  ";
                SQL = SQL + " WHERE  HR_EMP_STATUS ='A' AND  HR_DM_DESIGNATION_CODE  ";
                SQL = SQL + " IN  (  SELECT RM_IP_PARAMETER_VALUE FROM   RM_DEFUALTS_DRIVER_TRAILER ";
                SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TRAILER_DESIG_CODE' ) ";

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

        public DataTable FillTrailerView ( )//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                //SQL = "  SELECT   FA_FIXED_ASSET_MASTER.FA_FAM_REF_CODE REF_CODE ,  ";
                //SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE code , FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_DESCRIPTION name,  ";
                //SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_PLATENO  phone,RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE drcode,HR_EMP_EMPLOYEE_NAME drname  ";
                //SQL = SQL + " FROM FA_FIXED_ASSET_MASTER , WS_ASSET_CATEGORY_TYPE,RM_DRIVER_MASTER_DETS,HR_EMPLOYEE_MASTER  ";
                //SQL = SQL + " WHERE FA_FIXED_ASSET_MASTER.FA_FAM_VEHICLE_TYPE = WS_ASSET_CATEGORY_TYPE.SL_NO  ";
                //SQL = SQL + " AND FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_STATUS ='A'  ";
                //SQL = SQL + " AND  HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE = RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE(+)";
                //SQL = SQL + " AND  FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE = RM_DRIVER_MASTER_DETS.FA_FAM_ASSET_CODE(+)";
                //SQL = SQL + " AND  FA_FIXED_ASSET_MASTER.FA_FAM_VEHICLE_TYPE   ";
                //SQL = SQL + " IN  (  SELECT RM_IP_PARAMETER_VALUE FROM   RM_DEFUALTS_DRIVER_TRAILER ";
                //SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TRAILER_TYPE_CODE' ) ";


                SQL = "  SELECT   FA_FIXED_ASSET_MASTER.FA_FAM_REF_CODE REF_CODE ,  ";
                SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE code , FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_DESCRIPTION name,  ";
                SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_PLATENO  phone,RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE drcode,HR_EMP_EMPLOYEE_NAME drname  ";
                SQL = SQL + " FROM FA_FIXED_ASSET_MASTER , WS_ASSET_CATEGORY_TYPE,RM_DRIVER_MASTER_DETS,HR_EMPLOYEE_MASTER  ";
                SQL = SQL + " WHERE FA_FIXED_ASSET_MASTER.FA_FAM_VEHICLE_TYPE = WS_ASSET_CATEGORY_TYPE.SL_NO  ";
                SQL = SQL + " AND FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_STATUS ='A'  ";
                SQL = SQL + " AND  RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE = HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE (+)";
                //  SQL = SQL + " AND  RM_DRIVER_MASTER_DETS.FA_FAM_ASSET_CODE = FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE (+) ";
                SQL = SQL + " AND   FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE= RM_DRIVER_MASTER_DETS.FA_FAM_ASSET_CODE (+) ";
                SQL = SQL + " AND  FA_FIXED_ASSET_MASTER.FA_FAM_VEHICLE_TYPE   ";
                SQL = SQL + " IN  (  SELECT RM_IP_PARAMETER_VALUE FROM   RM_DEFUALTS_DRIVER_TRAILER ";
                SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TRAILER_TYPE_CODE' ) ";

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

        public DataTable FillViewContractMaster ( string sCustomerCode )
        {
            DataTable dtEnquiryList = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT SL_ENQUIRY_MASTER.SALES_INQM_ENQUIRY_NO code, SL_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE PROJECTCODE,";
                SQL = SQL + " SL_PROJECT_MASTER.SALES_PM_PROJECT_NAME name,";
                SQL = SQL + " SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_CODE CUSTOMERCODE,";
                SQL = SQL + " SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_NAME FIELD_THREE,";
                SQL = SQL + " ";
                SQL = SQL + " SL_ENQUIRY_MASTER.SALES_INQM_ENQUIRY_STATUS CONTRACTSTATUS";
                SQL = SQL + " FROM SL_CUSTOMER_MASTER, SL_ENQUIRY_MASTER, SL_PROJECT_MASTER";
                SQL = SQL + " WHERE SL_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE =SL_PROJECT_MASTER.SALES_PM_PROJECT_CODE  ";
                SQL = SQL + " AND  SL_ENQUIRY_MASTER.SALES_CUS_CUSTOMER_CODE = SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_CODE (+)";

                SQL = SQL + " AND SL_ENQUIRY_MASTER.SALES_INQM_LAST_REVISED ='Y' ";
                SQL = SQL + " AND SL_ENQUIRY_MASTER.SALES_INQM_ENQUIRY_STATUS in (  'LIVE','COMPLETED')";
                SQL = SQL + " AND SL_ENQUIRY_MASTER.SALES_CUS_CUSTOMER_CODE = '" + sCustomerCode + "'";

                SQL = SQL + " ORDER BY SALES_INQM_ENQUIRY_NO DESC";

                dtEnquiryList = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);

            }
            finally
            {
                // cCollection = null;
                // objBase = null;
            }
            return dtEnquiryList;
        }

        public DataTable FillStationView ( string UserName )
        {
            DataTable dtData = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                ////SQL = " SELECT SALES_STN_STATION_CODE STCODE,SALES_STN_STATION_NAME STNAME ,GL_COSM_ACCOUNT_CODE COSMACCODE, ";
                ////SQL = SQL + " AD_BRANCH_MASTER.AD_BR_CODE BRCODE,AD_BR_NAME BRNAME ";
                ////SQL = SQL + " FROM SL_STATION_MASTER,AD_BRANCH_MASTER ";
                ////SQL = SQL + " WHERE SL_STATION_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE ";
                ////SQL = SQL + " AND SALES_STN_DELETESTATUS = 0 "; 
                ////SQL = SQL + " AND SALES_RAW_MATERIAL_STATION = 'Y' ";
                ////SQL = SQL + " AND SALES_STN_STATION_STATUS='A' ";

                SQL = "SELECT ";
                SQL = SQL + "        SALES_STN_STATION_CODE STCODE,SALES_STN_STATION_NAME STNAME , ";
                SQL = SQL + "        GL_COSM_ACCOUNT_CODE COSMACCODE,  ";
                SQL = SQL + "        AD_BR_CODE BRCODE,AD_BR_NAME BRNAME   ";
                SQL = SQL + "    FROM  ";
                SQL = SQL + "        SL_STATION_BRANCH_MAPP_DTLS_VW  ";
                SQL = SQL + "    where  ";
                SQL = SQL + "     SALES_RAW_MATERIAL_STATION = 'Y'   AND SALES_STN_STATION_STATUS='A'  ";

                if (UserName != "ADMIN")
                {
                    SQL = SQL + "    AND SALES_STN_STATION_CODE IN ( SELECT     SALES_STN_STATION_CODE FROM ";
                    SQL = SQL + "     WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + UserName + "') ";
                }

                if (UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                    SQL = SQL + "   and      AD_BR_CODE  IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                }

                SQL = SQL + "  order by   AD_BR_CODE ,     SALES_STN_STATION_CODE  asc ";




                dtData = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dtData;
        }

        #endregion
        #region"Fill Grid"
        public DataSet fillAttachGrid(string txtEntryCode, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "  SELECT RM_MSM_ENTRY_NO,  RM_MSM_REMARKS , RM_MSM_FILE_PATH";

                SQL = SQL + " FROM rm_sales_ATTACH_DTLS";

                SQL = SQL + " WHERE RM_MSM_ENTRY_NO='" +txtEntryCode+ "' ";

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

        #region "Delete File From Grid"
        public void DeletePath(string txtEntryCode, string Path, object mngrclassobj)
        {
            try
            {
                SQL = string.Empty;
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " DELETE FROM rm_sales_ATTACH_DTLS";
                SQL = SQL + "  WHERE RM_MSM_ENTRY_NO='" + txtEntryCode + "' AND RM_MSM_FILE_PATH='" + Path + "'";

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

        #region"Insert Attach Data "
        public void InsertAttachFile(string lblCVPath, string AttachRemarks, string txtEntryCode, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();
            Int32 SLNO = 0;

            try
            {
                SQL = string.Empty;
                OracleHelper oTrns = new OracleHelper();
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = "SELECT MAX(RM_MSM_SLNO) SLNO FROM rm_sales_ATTACH_DTLS WHERE RM_MSM_ENTRY_NO ='" + txtEntryCode + "'";
               

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

                SQL = "  INSERT INTO rm_sales_ATTACH_DTLS";

                SQL = SQL + "(RM_MSM_ENTRY_NO, RM_MSM_SLNO, RM_MSM_REMARKS, RM_MSM_FILE_PATH)";

                SQL = SQL + " Values('" + txtEntryCode + "'," + SLNO + ",'" + AttachRemarks + "',";
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


        #endregion

        #region "Insert/Update/Delete"
        public string FetchDebitNoteNo ( string sDeliveryNoteRetNo )
        {
            DataTable dtCustCode = new DataTable();
            string sCode = "";

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = "    SELECT  GL_CRDRM_NO    ";
                SQL = SQL + "    FROM  RM_SALES_MASTER ";
                SQL = SQL + "    WHERE  RM_MSM_ENTRY_NO      =    '" + sDeliveryNoteRetNo + "'";


                dtCustCode = clsSQLHelper.GetDataTableByCommand(SQL);
                if (dtCustCode.Rows.Count > 0)
                {
                    sCode = dtCustCode.Rows[0]["GL_CRDRM_NO"].ToString();
                }
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {

                //cCollection = null;
                //objBase = null;
            }
            return sCode;
        }

        public string InsertMasterSql ( SalesRMEntity oEntity, List<SalesRMEntryDetails> EntityDetails, bool Autogen, object mngrclassobj, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = "";


                DateTime dReceiptTime = System.Convert.ToDateTime(oEntity.dtpEntryDate.ToString("dd-MMM-yyyy") + " " + oEntity.EntryTime);
                 

                SQL = " INSERT INTO rm_sales_master";
                SQL = SQL + " (rm_msm_entry_no, rm_msm_entry_date,  RM_MSM_ENTRY_DATE_TIME  , ad_fin_finyrid,";
                SQL = SQL + " sales_stn_station_code, hr_dept_dept_code,TECH_PLM_PLANT_CODE ,";
                SQL = SQL + " sales_cus_customer_code, rm_msm_total_amount,";
                SQL = SQL + " rm_msm_balance_total, rm_msm_remarks, rm_msm_approved,";
                SQL = SQL + " rm_msm_approved_by, rm_msm_createdby, rm_msm_createddate,";
                SQL = SQL + " ad_cm_company_code,gl_coam_account_code,FA_FAM_ASSET_CODE,HR_EMP_EMPLOYEE_CODE  , ";
                SQL = SQL + "  RM_MSM_MATERIAL_TOTAL_AMOUNT ,RM_MSM_TRANS_TOTAL_AMOUNT    ,";
                SQL = SQL + "  RM_SM_SOURCE_CODE,GL_COSM_ACCOUNT_CODE,RM_VM_VENDOR_CODE,RM_MSM_ENTRY_TYPE,  ";
                SQL = SQL + " RM_MSM_RNDOFF_AMOUNT , RM_MSM_RNDOFF_ACCOUNT_CODE ,   AD_BR_CODE , ";
                SQL = SQL + " RM_MSM_VAT_PERCENTAGE,RM_MSM_VAT_AMOUNT,RM_MSM_VAT_ACC_CODE,RM_MSM_VAT_TYPE_CODE,RM_MSM_GRAND_TOTAL, ";
                SQL = SQL + " SALES_INQM_ENQUIRY_NO ,RM_MSM_PAYTERMS,RM_MSM_CUST_LPO_NO";
                SQL = SQL + " )";
                SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "', '" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "', TO_DATE('" + dReceiptTime.ToString("dd-MMM-yyyy hh:mm:ss tt") + "', 'DD-MM-YYYY HH:MI:SS AM')  , " + mngrclass.FinYearID + ",";
                SQL = SQL + " '" + oEntity.cboStation + "', '" + oEntity.txtDept + "', '" + oEntity.cboDestination + "',  ";
                SQL = SQL + " '" + oEntity.txtCustCode + "', " + Convert.ToDouble(oEntity.txtAmount) + ",";
                SQL = SQL + " " + Convert.ToDouble(oEntity.txtAmount) + ", '" + oEntity.txtRemarks + "', '" + oEntity.Apprv + "',";
                SQL = SQL + " '" + oEntity.txtAppdBy + "', '" + mngrclass.UserName + "', TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " '" + mngrclass.CompanyCode + "','" + oEntity.txtAccountCode + "','" + oEntity.txtTrailer + "','" + oEntity.txtDriver + "',";
                SQL = SQL + "" + Convert.ToDouble(oEntity.txtMaterialTotal) + "," + Convert.ToDouble(oEntity.txtTransTotal) + ",'" + oEntity.txtSource + "','" + oEntity.CostCenter + "',";
                SQL = SQL + "'" + oEntity.VendorCode + "','" + oEntity.EntryType + "',";
                SQL = SQL + " " + oEntity.txtRndOffAmt + ",'" + oEntity.txtRndOffAccCode + "' ,'" + oEntity.cboBranch + "',";
                SQL = SQL + " " + Convert.ToDouble(oEntity.TaxPerc) + ", " + Convert.ToDouble(oEntity.TaxAmount) + ",'" + oEntity.TaxAccountCode + "','" + oEntity.TaxVatType + "', " + Convert.ToDouble(oEntity.txtGradndTotalAmount) + ",  ";
                SQL = SQL + " '" + oEntity.EnquiryNo + "','" + oEntity.PaymentTerms + "','" + oEntity.CustLpoNo + "' ";
                SQL = SQL + ")";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertDetails(oEntity, EntityDetails, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                if (oEntity.Apprv == "Y")
                {
                    sRetun = InsertApprovalQrs(oEntity, mngrclassobj);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMSALES", oEntity.txtEntryNo, Autogen, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.cboBranch, sAtuoGenBranchDocNumber);

            }
            catch (Exception ex)
            {

                sRetun = "Error occured while creating  sql statement. Check log files to get details.";


                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return sRetun;
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return sRetun;
        }

        private string InsertDetails ( SalesRMEntity oEntity, List<SalesRMEntryDetails> EntityDetails, object mngrclassobj )
        {
            string sRetun = string.Empty;

            int i = 0;
            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                {
                    i = 0;
                    foreach (var Data in EntityDetails)
                    {
                        if (!string.IsNullOrEmpty(Data.rmCodeDetails.ToString()) & double.Parse(Data.qtyDetails.ToString()) > 0)
                        {
                            ++i;
                            SQL = " INSERT INTO RM_SALES_DETAILS";
                            SQL = SQL + " (rm_msm_entrty_no, ad_fin_finyrid, rm_msd_sl_no, rm_rmm_rm_code,";
                            SQL = SQL + " rm_um_uom_code, rm_msd_avg_cost, rm_msd_sale_rate, rm_msd_qty,ad_cm_company_code,rm_rmm_inv_acc_code,rm_rmm_cons_acc_code,";
                            SQL = SQL + " RM_MSD_AMOUNT , RM_MSD_TRANS_RATE, RM_MSD_TRNS_AMOUNT )";
                            SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "', " + mngrclass.FinYearID + "," + i + ", '" + Convert.ToString(Data.rmCodeDetails) + "',";
                            SQL = SQL + " '" + Convert.ToString(Data.uomCodeDetails) + "'," + Convert.ToDouble(Data.avgCostDetails) + " , " + Convert.ToDouble(Data.saleRateDetails) + ", " + Convert.ToDouble(Data.qtyDetails) + ",'" + mngrclass.CompanyCode + "','" + Convert.ToString(0) + "','" + Convert.ToString(0) + "',";
                            SQL = SQL + " '" + Data.totalDetails + "' , " + Convert.ToDouble(Data.transRate) + "  ," + Convert.ToDouble(Data.transAmount) + "";

                            SQL = SQL + "  )";
                            // End If

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
            return "CONTINUE";
        }

        private string InsertApprovalQrs ( SalesRMEntity oEntity, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " INSERT INTO RM_SALES_TRIGGER ( ";
                SQL = SQL + " RM_MSM_ENTRY_NO, AD_FIN_FINYRID, RM_MSM_ENTRY_DATE, ";
                SQL = SQL + " RM_MSM_APPROVED_BY) ";
                SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "', " + mngrclass.FinYearID + ",";
                SQL = SQL + " '" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "', '" + mngrclass.UserName + "'";
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

        public string UpdateMasterSql ( SalesRMEntity oEntity, List<SalesRMEntryDetails> EntityDetails, bool Autogen, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                DateTime dReceiptTime = System.Convert.ToDateTime(oEntity.dtpEntryDate.ToString("dd-MMM-yyyy") + " " + oEntity.EntryTime);



                SQL = " UPDATE rm_sales_master";

                SQL = SQL + " SET rm_msm_entry_date = '" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "', RM_MSM_ENTRY_DATE_TIME =TO_DATE('" + dReceiptTime.ToString("dd-MMM-yyyy hh:mm:ss tt") + "', 'DD-MM-YYYY HH:MI:SS AM')  ,";
                SQL = SQL + " sales_stn_station_code = '" + oEntity.cboStation + "',";
                SQL = SQL + " AD_BR_CODE = '" + oEntity.cboBranch + "',";
                SQL = SQL + " hr_dept_dept_code = '" + oEntity.txtDept + "',TECH_PLM_PLANT_CODE ='" + oEntity.cboDestination + "',";
                SQL = SQL + " sales_cus_customer_code = '" + oEntity.txtCustCode + "',";
                SQL = SQL + " RM_MSM_CUST_LPO_NO = '" + oEntity.CustLpoNo + "',";
                SQL = SQL + " gl_coam_account_code = '" + oEntity.txtAccountCode + "',";
                SQL = SQL + " rm_msm_total_amount = " + Convert.ToDouble(oEntity.txtAmount) + ",";
                SQL = SQL + " rm_msm_balance_total = " + Convert.ToDouble(oEntity.txtAmount) + ",";
                SQL = SQL + " rm_msm_remarks = '" + oEntity.txtRemarks + "',";
                SQL = SQL + " rm_msm_approved = '" + oEntity.Apprv + "',";
                SQL = SQL + " rm_msm_approved_by = '" + oEntity.txtAppdBy + "',";
                SQL = SQL + " RM_MSM_RNDOFF_AMOUNT = " + oEntity.txtRndOffAmt + ",RM_MSM_RNDOFF_ACCOUNT_CODE = '" + oEntity.txtRndOffAccCode + "',";
                SQL = SQL + " rm_msm_updatedby = '" + mngrclass.UserName + "', GL_COSM_ACCOUNT_CODE='" + oEntity.CostCenter + "',";
                SQL = SQL + " FA_FAM_ASSET_CODE = '" + oEntity.txtTrailer + "',HR_EMP_EMPLOYEE_CODE= '" + oEntity.txtDriver + "',";
                SQL = SQL + " rm_msm_updateddate = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " RM_MSM_MATERIAL_TOTAL_AMOUNT = " + Convert.ToDouble(oEntity.txtMaterialTotal) + ", RM_MSM_TRANS_TOTAL_AMOUNT  = " + Convert.ToDouble(oEntity.txtTransTotal) + ",";
                SQL = SQL + " RM_SM_SOURCE_CODE    ='" + oEntity.txtSource + "',";
                SQL = SQL + " RM_MSM_VAT_PERCENTAGE=" + Convert.ToDouble(oEntity.TaxPerc) + ",RM_MSM_VAT_AMOUNT=" + Convert.ToDouble(oEntity.TaxAmount) + ",";
                SQL = SQL + " RM_MSM_VAT_ACC_CODE='" + oEntity.TaxAccountCode + "',RM_MSM_VAT_TYPE_CODE='" + oEntity.TaxVatType + "',RM_MSM_GRAND_TOTAL=" + Convert.ToDouble(oEntity.txtGradndTotalAmount) + ", ";
                SQL = SQL + " SALES_INQM_ENQUIRY_NO = '" + oEntity.EnquiryNo + "',RM_MSM_PAYTERMS='" + oEntity.PaymentTerms + "' ";
                SQL = SQL + " WHERE rm_msm_entry_no = '" + oEntity.txtEntryNo + "' AND ad_fin_finyrid = " + mngrclass.FinYearID + " AND rm_msm_deletestatus = 0";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_SALES_DETAILS";
                SQL = SQL + " WHERE rm_msm_entrty_no = '" + oEntity.txtEntryNo + "'";
                SQL = SQL + " AND ad_fin_finyrid = " + mngrclass.FinYearID + "";
                SQL = SQL + " AND rm_msd_deletestatus = 0";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertDetails(oEntity, EntityDetails, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                if (oEntity.Apprv == "Y")
                {
                    sRetun = InsertApprovalQrs(oEntity, mngrclassobj);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMSALES", oEntity.txtEntryNo, Autogen, Environment.MachineName, "U", sSQLArray);

            }
            catch (Exception ex)
            {

                sRetun = "Error occured while creating  sql statement. Check log files to get details.";


                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return sRetun;
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return sRetun;
        }

        public string DeleteSql ( string txtEntryNo, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " Delete from rm_sales_master";
                SQL = SQL + " WHERE rm_msm_entry_no = '" + txtEntryNo + "' AND ad_fin_finyrid = " + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);

                SQL = " Delete from RM_SALES_DETAILS";
                SQL = SQL + " WHERE rm_msm_entrty_no ='" + txtEntryNo + "' AND ad_fin_finyrid = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMSALES", txtEntryNo, false, Environment.MachineName, "D", sSQLArray);
            }
            catch (Exception ex)
            {

                sRetun = "Error occured while creating  sql statement. Check log files to get details.";


                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);

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

        #region "Fetch Datas"
        public DataSet FetchMasterData ( string Entry_No, int FinYr, string UserName )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT * from( ";
                SQL = SQL + " SELECT 'RMSALESRADIOBTN' TYPE,   RM_MSM_ENTRY_NO ENTRYNO, RM_MSM_ENTRY_DATE,   RM_MSM_ENTRY_DATE_TIME ,  ";
                SQL = SQL + "         RM_SALES_MASTER.SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_SALES_MASTER.TECH_PLM_PLANT_CODE,  ";
                SQL = SQL + "         RM_SALES_MASTER.HR_DEPT_DEPT_CODE,  ";
                SQL = SQL + "         RM_SALES_MASTER.SALES_CUS_CUSTOMER_CODE CODE,  ";
                SQL = SQL + "         SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_NAME NAME, RM_MSM_ENTRY_TYPE,  ";
                SQL = SQL + "         SL_CUSTOMER_MASTER.GL_COAM_ACCOUNT_CODE, RM_MSM_TOTAL_AMOUNT,  ";
                SQL = SQL + "         RM_MSM_GRAND_TOTAL, RM_MSM_APPROVED, RM_MSM_APPROVED_BY,  ";
                SQL = SQL + "         RM_MSM_REMARKS, RM_MSM_CREATEDBY,  ";
                SQL = SQL + "         RM_SALES_MASTER.GL_COSM_ACCOUNT_CODE,  ";
                SQL = SQL + "         RM_SALES_MASTER.FA_FAM_ASSET_CODE,  ";
                SQL = SQL + "         RM_SALES_MASTER.HR_EMP_EMPLOYEE_CODE,  ";
                SQL = SQL + "         RM_SALES_MASTER.RM_MSM_RNDOFF_AMOUNT,  ";
                SQL = SQL + "         RM_SALES_MASTER.RM_MSM_RNDOFF_ACCOUNT_CODE,  ";
                SQL = SQL + "         GL_COA_MASTER.GL_COAM_ACCOUNT_NAME ROUNOFF_ACC,  ";
                SQL = SQL + "         FA_FAM_ASSET_DESCRIPTION, HR_EMP_EMPLOYEE_NAME,  ";
                SQL = SQL + "         RM_MSM_MATERIAL_TOTAL_AMOUNT, RM_MSM_TRANS_TOTAL_AMOUNT,  ";
                SQL = SQL + "         RM_SM_SOURCE_CODE, RM_MSM_VAT_PERCENTAGE, RM_MSM_VAT_AMOUNT,  ";
                SQL = SQL + "         RM_MSM_VAT_ACC_CODE, RM_MSM_VAT_TYPE_CODE,  ";
                SQL = SQL + "         RM_SALES_MASTER.SALES_INQM_ENQUIRY_NO, RM_MSM_CUST_LPO_NO, ";
                SQL = SQL + "         SL_PROJECT_MASTER.SALES_PM_PROJECT_NAME,RM_MSM_PAYTERMS,  ";
                SQL = SQL + "         RM_SALES_MASTER.AD_BR_CODE ,AD_BR_NAME    ,RM_SALES_MASTER.GL_CRDRM_NO,RM_SALES_MASTER.AD_FIN_FINYRID  ";
                SQL = SQL + "    FROM RM_SALES_MASTER,  ";
                SQL = SQL + "         SL_CUSTOMER_MASTER,  ";
                SQL = SQL + "         FA_FIXED_ASSET_MASTER,  ";
                SQL = SQL + "         HR_EMPLOYEE_MASTER,  ";
                SQL = SQL + "         GL_COA_MASTER,  ";
                SQL = SQL + "         SL_ENQUIRY_MASTER , ";
                SQL = SQL + "         SL_PROJECT_MASTER , ";
                SQL = SQL + "         AD_BRANCH_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER ";
                SQL = SQL + "   WHERE RM_SALES_MASTER.AD_FIN_FINYRID = " + FinYr + "";
                SQL = SQL + "     AND RM_MSM_ENTRY_NO = '" + Entry_No + "'";
                SQL = SQL + "     AND RM_SALES_MASTER.RM_MSM_RNDOFF_ACCOUNT_CODE = GL_COA_MASTER.GL_COAM_ACCOUNT_CODE(+)  ";
                SQL = SQL + "     AND RM_SALES_MASTER.SALES_CUS_CUSTOMER_CODE = SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_CODE  ";
                SQL = SQL + "     AND RM_SALES_MASTER.FA_FAM_ASSET_CODE = FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE(+)  ";
                SQL = SQL + "     AND RM_SALES_MASTER.HR_EMP_EMPLOYEE_CODE = HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE(+)  ";
                SQL = SQL + "     AND RM_SALES_MASTER.SALES_INQM_ENQUIRY_NO = SL_ENQUIRY_MASTER.SALES_INQM_ENQUIRY_NO(+)  ";
                SQL = SQL + "     AND SL_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE = SL_PROJECT_MASTER.SALES_PM_PROJECT_CODE(+)  ";
                SQL = SQL + "     AND RM_SALES_MASTER.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "     AND RM_SALES_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE ";
                SQL = SQL + "     AND RM_MSM_ENTRY_TYPE = 'SL'  ";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + " AND RM_SALES_MASTER.AD_BR_CODE in ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                }

                SQL = SQL + "UNION ALL  ";
                SQL = SQL + "SELECT 'RMRETURNRADIOBTN' TYPE,  RM_MSM_ENTRY_NO ENTRYNO, RM_MSM_ENTRY_DATE, RM_MSM_ENTRY_DATE_TIME ,  ";
                SQL = SQL + "         RM_SALES_MASTER.SALES_STN_STATION_CODE,SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_SALES_MASTER.TECH_PLM_PLANT_CODE,   ";
                SQL = SQL + "         RM_SALES_MASTER.HR_DEPT_DEPT_CODE,  ";
                SQL = SQL + "         RM_SALES_MASTER.RM_VM_VENDOR_CODE CODE,  ";
                SQL = SQL + "         RM_VENDOR_MASTER.RM_VM_VENDOR_NAME NAME, RM_MSM_ENTRY_TYPE,  ";
                SQL = SQL + "         RM_VENDOR_MASTER.GL_COAM_ACCOUNT_CODE, RM_MSM_TOTAL_AMOUNT,  ";
                SQL = SQL + "         RM_MSM_GRAND_TOTAL, RM_MSM_APPROVED, RM_MSM_APPROVED_BY,  ";
                SQL = SQL + "         RM_MSM_REMARKS, RM_MSM_CREATEDBY,  ";
                SQL = SQL + "         RM_SALES_MASTER.GL_COSM_ACCOUNT_CODE,  ";
                SQL = SQL + "         RM_SALES_MASTER.FA_FAM_ASSET_CODE,  ";
                SQL = SQL + "         RM_SALES_MASTER.HR_EMP_EMPLOYEE_CODE,  ";
                SQL = SQL + "         RM_SALES_MASTER.RM_MSM_RNDOFF_AMOUNT,  ";
                SQL = SQL + "         RM_SALES_MASTER.RM_MSM_RNDOFF_ACCOUNT_CODE,  ";
                SQL = SQL + "         GL_COA_MASTER.GL_COAM_ACCOUNT_NAME ROUNOFF_ACC,  ";
                SQL = SQL + "         FA_FAM_ASSET_DESCRIPTION, HR_EMP_EMPLOYEE_NAME,  ";
                SQL = SQL + "         RM_MSM_MATERIAL_TOTAL_AMOUNT, RM_MSM_TRANS_TOTAL_AMOUNT,  ";
                SQL = SQL + "         RM_SM_SOURCE_CODE, RM_MSM_VAT_PERCENTAGE, RM_MSM_VAT_AMOUNT,  ";
                SQL = SQL + "         RM_MSM_VAT_ACC_CODE, RM_MSM_VAT_TYPE_CODE,  ";
                SQL = SQL + "         RM_SALES_MASTER.SALES_INQM_ENQUIRY_NO,RM_MSM_CUST_LPO_NO,  ";
                SQL = SQL + "         SL_PROJECT_MASTER.SALES_PM_PROJECT_NAME ,RM_MSM_PAYTERMS,  ";
                SQL = SQL + "         RM_SALES_MASTER.AD_BR_CODE ,AD_BR_NAME  ,RM_SALES_MASTER.GL_CRDRM_NO ,RM_SALES_MASTER.AD_FIN_FINYRID ";
                SQL = SQL + "    FROM RM_SALES_MASTER,  ";
                SQL = SQL + "         RM_VENDOR_MASTER,  ";
                SQL = SQL + "         FA_FIXED_ASSET_MASTER,  ";
                SQL = SQL + "         HR_EMPLOYEE_MASTER,  ";
                SQL = SQL + "         GL_COA_MASTER,  ";
                SQL = SQL + "         SL_ENQUIRY_MASTER , ";
                SQL = SQL + "         SL_PROJECT_MASTER , ";
                SQL = SQL + "         AD_BRANCH_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER ";
                SQL = SQL + "   WHERE RM_SALES_MASTER.AD_FIN_FINYRID = " + FinYr + "";
                SQL = SQL + "     AND RM_MSM_ENTRY_NO = '" + Entry_No + "'";
                SQL = SQL + "     AND RM_SALES_MASTER.RM_MSM_RNDOFF_ACCOUNT_CODE = GL_COA_MASTER.GL_COAM_ACCOUNT_CODE(+)  ";
                SQL = SQL + "     AND RM_SALES_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                SQL = SQL + "     AND RM_SALES_MASTER.FA_FAM_ASSET_CODE = FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE(+)  ";
                SQL = SQL + "     AND RM_SALES_MASTER.HR_EMP_EMPLOYEE_CODE = HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE(+)  ";
                SQL = SQL + "     AND RM_SALES_MASTER.SALES_INQM_ENQUIRY_NO = SL_ENQUIRY_MASTER.SALES_INQM_ENQUIRY_NO(+)  ";
                SQL = SQL + "     AND SL_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE = SL_PROJECT_MASTER.SALES_PM_PROJECT_CODE(+)  ";
                SQL = SQL + "     AND RM_SALES_MASTER.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "     AND RM_SALES_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE ";
                SQL = SQL + "     AND RM_MSM_ENTRY_TYPE = 'RT'  ";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + " AND RM_SALES_MASTER.AD_BR_CODE in ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                }


                SQL = SQL + ")  ";


                if (UserName != "ADMIN")
                {
                    SQL = SQL + " where TYPE in ( ";
                    SQL = SQL + " SELECT   AD_AF_ID";
                    SQL = SQL + "  FROM ad_mail_approval_users";
                    SQL = SQL + " WHERE UPPER (ad_um_userid) = '" + UserName + "' )";
                }
                SQL = SQL + "ORDER BY 1 DESC  ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchPurchaseDetailData ( string Entry_No, int FinYr )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_SALES_DETAILS.RM_RMM_RM_CODE RMCODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_SALES_DETAILS.RM_UM_UOM_CODE,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC, RM_MSD_AVG_COST, RM_MSD_SALE_RATE,";
                SQL = SQL + " RM_MSD_QTY, RM_MSD_SALE_RATE * RM_MSD_QTY EXT_AMNT, RM_MSD_SL_NO,";
                SQL = SQL + " RM_SALES_DETAILS.RM_RMM_INV_ACC_CODE,";
                SQL = SQL + " RM_SALES_DETAILS.RM_RMM_CONS_ACC_CODE , RM_MSD_TRANS_RATE, RM_MSD_TRNS_AMOUNT ";
                SQL = SQL + " FROM RM_SALES_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER";
                SQL = SQL + " WHERE ";
                SQL = SQL + "  RM_SALES_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " AND RM_SALES_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE";
                SQL = SQL + " AND RM_SALES_DETAILS.AD_FIN_FINYRID = " + FinYr + "";
                SQL = SQL + " AND RM_SALES_DETAILS.RM_MSM_ENTRTY_NO = '" + Entry_No + "'";
                SQL = SQL + " ORDER BY RM_SALES_DETAILS.RM_MSD_SL_NO ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchAttachDetailData(string Entry_No, int FinYr)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                //SQL = " SELECT  RM_MSM_REMARKS , RM_MSM_FILE_PATH ";
                //SQL = SQL + " FROM rm_sales_ATTACH_DTLS ";
                //SQL = SQL + "WHERE RM_MSM_ENTRY_NO='"+ Entry_No + "' ";


                SQL = "SELECT  RM_MSM_REMARKS , RM_MSM_FILE_PATH  ";
                SQL = SQL + " FROM rm_sales_ATTACH_DTLS ";
                SQL = SQL + " WHERE RM_MSM_ENTRY_NO='"+ Entry_No + "' ";


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

        #region Get Current Quantity"

        public DataSet GetCurrentQty ( string ItemCode, string StCode, string EntryDate, object mngrclassobj )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "  SELECT  ";
                SQL = SQL + " SUM ( NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0) - NVL (issued.iss_qty, 0) ) Current_qty ";
                SQL = SQL + " FROM rm_rawmaterial_master mas,RM_UOM_MASTER uommas, ";
                SQL = SQL + " rm_rawmaterial_details det, ";
                SQL = SQL + " (  ";
                SQL = SQL + "  SELECT rm_rmm_rm_code rm_code, sales_stn_station_code stn_code, ";
                SQL = SQL + " SUM (rm_ob_qty) ";
                SQL = SQL + " ob_qty, SUM ( RM_OB_AMOUNT ) ob_val ";
                SQL = SQL + " FROM RM_OPEN_BALANCES ";
                SQL = SQL + " WHERE ad_fin_finyrid = " + mngrclass.FinYearID + " ";
                SQL = SQL + " AND sales_stn_station_code = '" + StCode + "' ";
                SQL = SQL + " GROUP BY rm_rmm_rm_code, sales_stn_station_code ) opening, ";
                SQL = SQL + " (  ";
                SQL = SQL + "  SELECT RM_IM_ITEM_CODE rm_code, sales_stn_station_code stn_code, ";
                SQL = SQL + " SUM (NVL (RM_STKL_RECD_QTY, 0)) ";
                SQL = SQL + " rec_qty, ";
                SQL = SQL + " SUM (NVL (RM_STKL_RECD_AMT, 0) ) rec_value ";
                SQL = SQL + " FROM RM_STOCK_LEDGER ";
                SQL = SQL + " WHERE TO_DATE (TO_CHAR (rm_stkl_trans_date, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(EntryDate).ToString("dd-MMM-yyyy") + "' ";
                SQL = SQL + " AND ad_fin_finyrid =" + mngrclass.FinYearID + " ";
                SQL = SQL + " AND sales_stn_station_code = '" + StCode + "' ";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE , sales_stn_station_code  ";
                SQL = SQL + "  ) recd,  ";
                SQL = SQL + " (  ";
                SQL = SQL + "  SELECT RM_IM_ITEM_CODE rm_code, sales_stn_station_code stn_code, ";
                SQL = SQL + " SUM (NVL (RM_STKL_ISSUE_QTY, 0)) ";
                SQL = SQL + " iss_qty, ";
                SQL = SQL + " SUM ( NVL (RM_STKL_ISSUE_AMOUNT, 0) ) iss_value ";
                SQL = SQL + " FROM RM_STOCK_LEDGER ";
                SQL = SQL + " WHERE TO_DATE (TO_CHAR (rm_stkl_trans_date, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(EntryDate).ToString("dd-MMM-yyyy") + "' ";
                SQL = SQL + " AND ad_fin_finyrid = " + mngrclass.FinYearID + " ";
                SQL = SQL + " AND sales_stn_station_code = '" + StCode + "' ";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE , sales_stn_station_code  ";
                SQL = SQL + "  ) issued ";
                SQL = SQL + " WHERE   ";
                SQL = SQL + " mas.rm_rmm_rm_code = det.rm_rmm_rm_code ";
                SQL = SQL + " AND det.rm_rmm_rm_code = opening.rm_code(+) ";
                SQL = SQL + " AND det.sales_stn_station_code = opening.stn_code(+) ";
                SQL = SQL + " AND det.rm_rmm_rm_code = recd.rm_code(+) ";
                SQL = SQL + " AND det.sales_stn_station_code = recd.stn_code(+) ";
                SQL = SQL + " AND det.rm_rmm_rm_code = issued.rm_code(+) ";
                SQL = SQL + " AND det.sales_stn_station_code = issued.stn_code(+) ";
                SQL = SQL + " AND det.sales_stn_station_code = '" + StCode + "' ";
                SQL = SQL + " AND mas.RM_UM_UOM_CODE = uommas.RM_UM_UOM_CODE (+) ";
                SQL = SQL + " AND mas.RM_RMM_RM_CODE = det.RM_RMM_RM_CODE (+) ";
                SQL = SQL + " AND mas.rm_rmm_rm_code='" + ItemCode + "' ";


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

        #region " Print Voucher and match details "

        public DataSet FetchVoucherPrintData ( string voucherno, object mngrclassobj )
        {
            DataSet dSPrint = new DataSet("VOUCHERPRINT");

            DataTable dtMaster = new DataTable();
            DataTable dtMatchDetails = new DataTable();
            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT   ";
                SQL = SQL + " SLNO, ENTRY_NO, ENTRY_DATE, FINID, ACCOUNT_CODE, CUSTOMER_CODE, CUSTOMER_NAME, ";
                SQL = SQL + "  SALES_CUS_PO_BOX, ";
                SQL = SQL + "   SALES_CUS_ADDRESS, ";
                SQL = SQL + "   SALES_CUS_CITY, ";
                SQL = SQL + "   SALES_CUS_TEL_NO, ";
                SQL = SQL + "   SALES_CUS_FAX_NO, ";
                SQL = SQL + "   SALES_CUS_EMAIL, ";
                SQL = SQL + "   SALES_CUS_PRICE_REMARKS, ";
                SQL = SQL + "   SALES_CUS_SPONSOR_NAME, ";
                SQL = SQL + "   SALES_CUS_REMARKS, ";
                SQL = SQL + "   SALES_CUS_CONT_PERSON1, ";
                SQL = SQL + "   SALES_CUS_CONT_DESIG1, ";
                SQL = SQL + "   SALES_CUS_CONT_PERSON1_MOBNO, ";
                SQL = SQL + "   SALES_CUS_CONT_PERSON2, ";
                SQL = SQL + "   SALES_CUS_CONT_DESIG2, ";
                SQL = SQL + "   SALES_CUS_CONT_PERSON2_MOBNO, ";
                SQL = SQL + "   SALES_CUST_CREDIT_TYPE, ";
                SQL = SQL + "   SALES_CUS_CONT_PERSON1_EMAIL, ";
                SQL = SQL + "   SALES_CUS_CONT_PERSON2_EMAIL, ";
                SQL = SQL + "   SALES_CUS_CONT_PERSON3_EMAIL, ";
                SQL = SQL + "   SALES_CUS_CREDIT_TERMS,SALES_CUS_CREDIT_PERIOD, ";
                SQL = SQL + "   SALES_PAY_TYPE_DESC, ";
                SQL = SQL + " STATION_CODE, STATION_NAME, DEPT_CODE, DEPT_DESC, TECH_PLM_PLANT_CODE, TECH_PLM_PLANT_NAME,  ";
                SQL = SQL + " RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC, FA_FAM_ASSET_CODE, FA_FAM_ASSET_DESCRIPTION, ";
                SQL = SQL + " HR_EMP_EMPLOYEE_CODE, HR_EMP_EMPLOYEE_NAME, REMARKS, MATAMOUNT, TRANSAMOUNT, TOTAL_AMOUNT,grand_total,RM_MSM_RNDOFF_AMOUNT, ";
                SQL = SQL + " RM_MSM_BALANCE_TOTAL, RM_MSM_CREATEDBY, RM_MSM_APPROVED_BY, RMCODE, RMDESC, ";
                SQL = SQL + " UOM_CODE, UOM_DESC, AVG_COST, SALE_RATE, QUANTITY, EXT_AMNT, TRANS_RATE, TRANS_AMNT, ";
                SQL = SQL + " INV_ACC_CODE, CONS_ACC_CODE ,";
                SQL = SQL + " RM_MSM_VAT_PERCENTAGE,RM_MSM_VAT_AMOUNT,";
                SQL = SQL + " RM_MSM_VAT_ACC_CODE,VAT_ACC_NAME,RM_MSM_VAT_TYPE_CODE,VAT_TYPE_NAME,TAX_VAT_REG_NUMBER, ";
                SQL = SQL + " SALES_INQM_ENQUIRY_NO, ";
                SQL = SQL + " RM_MSM_CUST_LPO_NO, ";
                SQL = SQL + " SALES_PM_PROJECT_CODE, ";
                SQL = SQL + " SALES_PM_PROJECT_NAME, ";
                SQL = SQL + " SALES_CREP_COMP_REP_CODE, ";
                SQL = SQL + " SALES_CREP_COMP_REP_NAME,PAYTERMS ,";

                SQL = SQL + " MASTER_BRANCH_CODE,    MASTER_BRANCH_NAME,    MASTER_BRANCHDOC_PREFIX, ";
                SQL = SQL + " MASTER_BRANCH_POBOX,    MASTER_BRANCH_ADDRESS,    MASTER_BRANCH_CITY, ";
                SQL = SQL + "MASTER_BRANCH_PHONE,    MASTER_BRANCH_FAX,    MASTER_BRANCH_SPONSER_NAME, ";
                SQL = SQL + "   MASTER_BRANCH_TRADE_LICENSE_NO,    MASTER_BRANCH_EMAIL_ID,    MASTER_BRANCH_WEB_SITE, ";
                SQL = SQL + "   MASTER_BRANCH_VAT_REG_NUMBER ";

                SQL = SQL + " FROM RM_SALES_VOUCHER_PRINT ";
                SQL = SQL + " WHERE ";
                SQL = SQL + " ENTRY_NO='" + voucherno + "' AND FINID=" + mngrclass.FinYearID + " ";

                dtMaster = clsSQLHelper.GetDataTableByCommand(SQL);
                dtMaster.TableName = "RM_SALES_DETAILS";
                dSPrint.Tables.Add(dtMaster);
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return null;
            }
            finally
            {

                //cCollection = null;
                //objBase = null;
            }
            return dSPrint;
        }
        #endregion


    }
    public class SalesRMEntity
    {
        public string txtEntryNo
        {
            get;
            set;
        }

        public string cboStation
        {
            get;
            set;
        }
        public string cboBranch
        {
            get; set;
        }

        public string CostCenter
        {
            get;
            set;
        }

        public string txtDept
        {
            get;
            set;
        }

        public string cboDestination
        {
            get;
            set;
        }


        public string txtTrailer
        {
            get;
            set;
        }

        public string txtDriver
        {
            get;
            set;
        }

        public string txtCustCode
        {
            get;
            set;
        }

        public string txtAmount
        {
            get;
            set;
        }

        public double txtMaterialTotal
        {
            get;
            set;
        }
        public double txtTransTotal
        {
            get;
            set;
        }

        public string txtRemarks
        {
            get;
            set;
        }


        public string txtSource
        {
            get;
            set;
        }

        public string PaymentTerms
        {
            get; set;
        }

        public string CustLpoNo
        {
            get; set;
        }

        public string txtAccountCode
        {
            get;
            set;
        }

        public string Apprv
        {
            get;
            set;
        }

        public string txtAppdBy
        {
            get;
            set;
        }
        public string txtRndOffAccCode
        {
            get;
            set;
        }
        public double txtRndOffAmt
        {
            get;
            set;
        }
        public string EnquiryNo
        {
            get; set;
        }

        public DateTime dtpEntryDate
        {
            get;
            set;
        }

        public string EntryTime
        {
            get;
            set;
        }
        public string EntryType
        {
            set;
            get;
        }

        public string VendorCode
        {
            set;
            get;
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
        public string TaxAccountCode
        {
            get;
            set;
        }

        public string TaxVatType
        {
            get;
            set;
        }

        public string txtGradndTotalAmount
        {
            get;
            set;
        }

    }

    public class SalesRMEntryDetails
    {
        public object slNoDetails { get; set; }
        public object rmCodeDetails { get; set; }
        public object uomCodeDetails { get; set; }
        public object avgCostDetails { get; set; }
        public object saleRateDetails { get; set; }
        public object qtyDetails { get; set; }
        public object totalDetails { get; set; }
        public object rmInvAccDetails { get; set; }
        public object rmConsAccDetails { get; set; }
        public object transRate { get; set; }

        public object transAmount { get; set; }
    }
}