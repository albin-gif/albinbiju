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
using Newtonsoft.Json;

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class PurchaseOrderPRCLogic
    {

        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public PurchaseOrderPRCLogic()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public string stCodeFilter
        {
            get;
            set;
        }
        public string SelectedBranchCode
        {
            get;
            set;
        }

        public string UserName { get; set; }
        #region "Fill view" 

        public DataTable FillViewSupplierOld()
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                {

                    SQL = " SELECT RM_VM_VENDOR_CODE Code,";
                    SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME Name,  RM_VM_VENDOR_TYPE  FIELDTHREE ,0 AD_FIN_FINYRID , SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_DESC ";
                    SQL = SQL + " FROM RM_VENDOR_MASTER , SL_PAY_TYPE_MASTER  ";
                    SQL = SQL + " where  RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS  = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE (+) ";
                    SQL = SQL + " AND RM_VM_VENDOR_STATUS='A' ";
                    SQL = SQL + " order by RM_VM_VENDOR_NAME asc  ";
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

        public DataTable FillViewSupplier(string prNo, string finyrid, string Division)
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();



                if (!string.IsNullOrEmpty(prNo))
                {
                    SQL = " SELECT DISTINCT RM_PRREQN_QTN.RM_VM_VENDOR_CODE SUPPLIER_CODE,";
                    SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIER_NAME";
                    SQL = SQL + " FROM RM_PRREQN_QTN, RM_VENDOR_MASTER";
                    SQL = SQL + " WHERE   ";
                    SQL = SQL + " RM_PRREQN_QTN.RM_PRQ_APPROVED = 'Y'";
                    SQL = SQL + " AND RM_PRREQN_QTN.RM_VM_VENDOR_CODE =";
                    SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_PO_PONO IS NULL";
                    SQL = SQL + " AND RM_PRREQN_QTN.RM_PR_PRNO='" + prNo + "' AND  AD_FIN_FINYRID   = " + finyrid + "";


                }
                else
                {
                    SQL = " SELECT  RM_VM_VENDOR_CODE SUPPLIER_CODE,";
                    SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIER_NAME";
                    SQL = SQL + " FROM   RM_VENDOR_MASTER";
                    SQL = SQL + " WHERE RM_VM_VENDOR_STATUS='A' ";

                    SQL = " SELECT  RM_VENDOR_MASTER.RM_VM_VENDOR_CODE SUPPLIER_CODE,";
                    SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIER_NAME,RMVENDMSTBRANCHPAYTERMSDATA.PAY_TERMS_ID ";
                    SQL = SQL + " RM_VM_CREDIT_TERMS,SALES_PAY_TYPE_DESC ";
                    SQL = SQL + " FROM   RM_VENDOR_MASTER, ";

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
                    SQL = SQL + "   = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE(+) )RMVENDMSTBRANCHPAYTERMSDATA ,";
                    SQL = SQL + "(  select ";
                    SQL = SQL + "    RM_VM_VENDOR_CODE ,  AD_BR_CODE , ";
                    SQL = SQL + "    RM_VM_LABLE_VALUE VENDOR_STATUS   ,RM_VM_LABLE_ID ";
                    SQL = SQL + "    from rm_vendor_master_BRANCH_DATA     ";
                    SQL = SQL + "    where RM_VM_LABLE_ID ='VENDORBR_STAUS_AI'  ";
                    SQL = SQL + "    and AD_BR_CODE='" + Division + "'   )RMVENDMSTBRANCHACTIVEDATA ";
                    SQL = SQL + " WHERE ";// RM_VM_VENDOR_STATUS='A' ";
                    SQL = SQL + "   RM_VENDOR_MASTER.RM_VM_VENDOR_CODE =";
                    SQL = SQL + " RMVENDMSTBRANCHPAYTERMSDATA.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE =";
                    SQL = SQL + " RMVENDMSTBRANCHACTIVEDATA.RM_VM_VENDOR_CODE";
                    SQL = SQL + " AND RMVENDMSTBRANCHACTIVEDATA.VENDOR_STATUS='A' ";
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

        public double FillPrGridCount(string prNo, string finyrid)
        {

            DataTable dtType = new DataTable();
            double count = 0;
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                if (!string.IsNullOrEmpty(prNo))
                {
                    SQL = " SELECT count(RM_PR_PRNO) Cnt ";
                    SQL = SQL + " FROM RM_PR_DETAILS ";
                    SQL = SQL + " WHERE   ";
                    SQL = SQL + " RM_PR_PRNO='" + prNo + "' AND  AD_FIN_FINYRID   = " + finyrid + "";


                    //SQL = "   select  COUNT (RM_RMM_RM_CODE ) cnt ";
                    //SQL = SQL + "    from RM_PRREQN_QTN ";
                    //SQL = SQL + "    where     ";
                    //SQL = SQL + "    RM_PRQ_APPROVED ='N' ";
                    //SQL = SQL + "     AND    RM_PRREQN_QTN.RM_PR_PRNO='" + prNo + "'";
                    //SQL = SQL + "     AND  RM_PRREQN_QTN.AD_FIN_FINYRID= " + finyrid + " ";
                    //SQL = SQL + "    AND RM_RMM_RM_CODE NOT IN ";
                    //SQL = SQL + "    (  ";
                    //SQL = SQL + "     select     RM_RMM_RM_CODE   from RM_PRREQN_QTN ";
                    //SQL = SQL + "     where     ";
                    //SQL = SQL + "     RM_PRQ_APPROVED ='Y' ";
                    //SQL = SQL + "      AND    RM_PRREQN_QTN.RM_PR_PRNO='" + prNo + "'";
                    //SQL = SQL + "     AND  RM_PRREQN_QTN.AD_FIN_FINYRID=" + finyrid + " ";
                    //SQL = SQL + "     ) ";
                    //SQL = SQL + "    AND GL_COSM_ACCOUNT_CODE NOT IN ";
                    //SQL = SQL + "    (  ";
                    //SQL = SQL + "     select     GL_COSM_ACCOUNT_CODE   from RM_PRREQN_QTN ";
                    //SQL = SQL + "     where     ";
                    //SQL = SQL + "     RM_PRQ_APPROVED ='Y' ";
                    //SQL = SQL + "      AND    RM_PRREQN_QTN.RM_PR_PRNO='" + prNo + "'";
                    //SQL = SQL + "     AND  RM_PRREQN_QTN.AD_FIN_FINYRID=" + finyrid + " ";
                    //SQL = SQL + "     ) ";
                    //SQL = SQL + "    AND SALES_STN_STATION_CODE NOT IN ";
                    //SQL = SQL + "    (  ";
                    //SQL = SQL + "     select     SALES_STN_STATION_CODE   from RM_PRREQN_QTN ";
                    //SQL = SQL + "     where     ";
                    //SQL = SQL + "     RM_PRQ_APPROVED ='Y' ";
                    //SQL = SQL + "      AND    RM_PRREQN_QTN.RM_PR_PRNO='" + prNo + "'";
                    //SQL = SQL + "     AND  RM_PRREQN_QTN.AD_FIN_FINYRID=" + finyrid + " ";
                    //SQL = SQL + "     ) ";
                    //SQL = SQL + "    AND RM_SM_SOURCE_CODE NOT IN ";
                    //SQL = SQL + "    (  ";
                    //SQL = SQL + "     select     RM_SM_SOURCE_CODE   from RM_PRREQN_QTN ";
                    //SQL = SQL + "     where     ";
                    //SQL = SQL + "     RM_PRQ_APPROVED ='Y' ";
                    //SQL = SQL + "      AND    RM_PRREQN_QTN.RM_PR_PRNO='" + prNo + "'";
                    //SQL = SQL + "     AND  RM_PRREQN_QTN.AD_FIN_FINYRID=" + finyrid + " ";
                    //SQL = SQL + "     ) ";

                }

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

                if (dtType.Rows.Count > 0)
                {
                    count = Convert.ToDouble(dtType.Rows[0]["Cnt"].ToString());
                }
                else
                {
                    count = 0;
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
            return count;
        }

        public DataTable FillViewPRNO(string UserName)
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT RM_PR_PRNO PR_NO , to_char(RM_PR_MASTER.RM_PR_PR_DATE,'DD-MON-YYYY') PR_DATE, ";
                SQL = SQL + " to_char(AD_FIN_FINYRID) PRFINYEAR,RM_PR_COST_CENTER,GL_COSTING_MASTER_VW.NAME COSTING_NAME,  ";
                SQL = SQL + " RM_PR_MASTER.AD_BR_CODE ";
                SQL = SQL + " FROM RM_PR_MASTER,GL_COSTING_MASTER_VW";
                SQL = SQL + " WHERE  ";
                SQL = SQL + " RM_PR_MASTER.RM_PR_COST_CENTER=GL_COSTING_MASTER_VW.CODE(+)";
                SQL = SQL + " AND RM_PR_PR_STATUS = 'O'";
                SQL = SQL + " AND  RM_PR_PRNO IN (SELECT DISTINCT RM_PR_PRNO";
                SQL = SQL + " FROM RM_PRREQN_QTN";
                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_PRQ_APPROVED = 'Y'";
                SQL = SQL + " AND RM_PO_PONO IS NULL)";
                SQL = SQL + " AND RM_PR_APPROVED='Y'";

                if (UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                    SQL = SQL + "  and     RM_PR_MASTER.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                }
                SQL = SQL + " order by RM_PR_MASTER.RM_PR_PRNO desc";

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


        public DataSet Fillcombo()
        {
            DataSet dsData = new DataSet();
            DataTable dtpayterms = new DataTable();
            DataTable dtQtyType = new DataTable();
            DataTable dtDept = new DataTable();
            try
            {


                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  ";
                SQL = SQL + " SELECT  ";
                SQL = SQL + " SALES_PAY_TYPE_CODE CODE , SALES_PAY_TYPE_DESC NAME   ";
                SQL = SQL + " FROM SL_PAY_TYPE_MASTER ORDER BY SALES_PAY_TYPE_DESC  ASC  ";

                dtpayterms = clsSQLHelper.GetDataTableByCommand(SQL);

                dsData.Tables.Add(dtpayterms);


                SQL = "  ";
                SQL = SQL + " SELECT  ";
                SQL = SQL + " RM_PO_QTY_RATE_TYPE CODE , RM_PO_QTY_RATE_TYPE_NAME NAME   ";
                SQL = SQL + " FROM RM_PO_QTYORRATE_VW ORDER BY RM_PO_QTY_RATE_TYPE_SORTID  ASC  ";

                dtQtyType = clsSQLHelper.GetDataTableByCommand(SQL);

                dsData.Tables.Add(dtQtyType);



                SQL = "   SELECT  ";
                SQL = SQL + "   HR_DEPT_MASTER.HR_DEPT_DEPT_CODE CODE , HR_DEPT_DEPT_DESC NAME ";
                SQL = SQL + "   FROM    HR_DEPT_MASTER   ";

                SQL = SQL + "   ORDER BY HR_DEPT_DEPT_DESC ASC ";

                dtDept = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtDept);


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dsData;
        }



        public DataSet FETCHNOTES(string BranchCode)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "select RM_PO_DEFAULTS_SL_NO,RM_PO_DEFAULTS_TEXT from RM_DEFAULTS_PO_TEMRS ";
                SQL = SQL + "where RM_PO_DELSTATUS=0 and AD_BR_CODE='" + BranchCode + "' order by  RM_PO_DEFAULTS_SL_NO asc ";
                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }



        public DataTable FillViewEmployee()
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();



                SQL = " SELECT hr_emp_employee_code Code, hr_emp_employee_name Name,";
                SQL = SQL + " hr_employee_master.HR_EMP_MOBILENO FIELDTHREE";
                SQL = SQL + " FROM hr_employee_master, hr_desg_master";
                SQL = SQL + " WHERE ";
                SQL = SQL + " hr_emp_deletestatus = 0";
                SQL = SQL + " AND hr_employee_master.hr_dm_designation_code =";
                SQL = SQL + " hr_desg_master.hr_dm_designation_code";
                SQL = SQL + " AND HR_EMP_STATUS = 'A' ";
                SQL = SQL + " ORDER BY hr_emp_employee_code";


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

        public DataTable FillView(string col, string gtcode, string sBudgetItemCode, string sProjectCode)
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                if (col == "2")
                {
                    //SQL = " SELECT SALES_STN_STATION_CODE Code,SALES_STN_STATION_NAME  Name ,'' Id   ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC,''BUDGET_AMOUNT  FROM ";
                    //SQL = SQL + " SL_STATION_MASTER WHERE SALES_STN_DELETESTATUS=0  and SALES_RAW_MATERIAL_STATION ='Y' ";

                    //if (gtcode != "")
                    //{
                    //    SQL = SQL + " 	  AND  SALES_STN_STATION_CODE ='" + gtcode + "'";
                    //}

                    //SQL = SQL + " ORDER BY SALES_STN_STATION_NAME ";



                    SQL = " SELECT   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE, SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME, ";
                    SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE Code  , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME Name  , ";
                    SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.GL_COSM_ACCOUNT_CODE ,";
                    SQL = SQL + "   '' Id   ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC,''BUDGET_AMOUNT ";
                    SQL = SQL + " FROM ";
                    SQL = SQL + "   SL_STATION_BRANCH_MAPP_DTLS_VW WHERE   ";
                    SQL = SQL + "    SALES_STATION_BRANCH_STATUS_AI ='A' and SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_RAW_MATERIAL_STATION ='Y'    ";
                    SQL = SQL + "  and  (AD_BR_READYMIX_VISIBILE_YN = 'Y' OR AD_BR_BLOCK_VISIBILE_YN = 'Y' OR AD_BR_PRECAST_VISIBILE_YN = 'Y' ) ";

                    if (!string.IsNullOrEmpty(SelectedBranchCode))
                    {
                        SQL = SQL + "   AND SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE ='" + SelectedBranchCode + "'";
                    }

                    if (UserName != "ADMIN")
                    {
                        SQL = SQL + "   AND SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE in (select AD_BR_CODE from  ";
                        SQL = SQL + "  AD_USER_GRANTED_BRANCH where AD_UM_USERID = '" + UserName + "')   ";
                    }
                    if (UserName != "ADMIN")
                    {
                        SQL = SQL + "  AND SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE IN ( SELECT     SALES_STN_STATION_CODE FROM ";
                        SQL = SQL + "     WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + UserName + "') ";
                    }


                    if (gtcode != "")
                    {
                        SQL = SQL + " 	  AND  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE  ='" + gtcode + "'";
                    }


                    SQL = SQL + "  ORDER BY   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE , SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_SORT_ID  , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE   ASC  ";





                }
                //else if (col == "2")
                //{
                //    SQL = "SELECT HR_DEPT_DEPT_CODE Code,HR_DEPT_DEPT_DESC Name , '' Id  ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC ";
                //    SQL = SQL + " FROM HR_DEPT_MASTER WHERE HR_DEPT_DELETESTATUS=0";

                //    SQL = SQL + " and HR_DEPT_DEPT_CODE   in ( SELECT  RM_IP_PARAMETER_VALUE FROM  RM_DEFUALTS_GL_PARAMETERS where RM_IP_PARAMETER_DESC ='DEPARTMENT' ) "; 
                //    SQL = SQL + " ORDER BY HR_DEPT_DEPT_CODE";
                //}
                else if (col == "4")
                {
                    SQL = " SELECT ";
                    SQL = SQL + "RM_SM_SOURCE_CODE Code , RM_SM_SOURCE_DESC Name ,'' Id ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC,''BUDGET_AMOUNT ";
                    SQL = SQL + "FROM RM_SOURCE_MASTER ";

                    if (gtcode != "")
                    {
                        SQL = SQL + " 	  where  RM_SM_SOURCE_CODE ='" + gtcode + "'";
                    }

                    SQL = SQL + " ORDER BY RM_SM_SOURCE_DESC ";
                }
                else if (col == "8")
                {
                    if (sBudgetItemCode != "")
                    {
                        SQL = " SELECT ";

                        SQL = SQL + "   RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code , RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION Name, '' Id,    ";

                        SQL = SQL + "   RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE ,  RM_UOM_MASTER.RM_UM_UOM_DESC,    ";

                        SQL = SQL + "   RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE RESOURCE_CODE,PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_NAME  RESOURCE_NAME,";

                        SQL = SQL + "   PC_BUD_BUDGET_ITEM_CODE,";

                        SQL = SQL + "   PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_AMOUNT BUDGET_AMOUNT  ";

                        SQL = SQL + " FROM ";

                        SQL = SQL + "   RM_RAWMATERIAL_MASTER , RM_UOM_MASTER,  ";
                        SQL = SQL + "   RM_RAWMATERIAL_DETAILS,";
                        SQL = SQL + "   PC_BUD_RESOURCE_MASTER,";
                        SQL = SQL + "   PC_BUD_BUDGET_MASTER,";
                        SQL = SQL + "   PC_BUD_BUDGET_DETAILS  ";

                        SQL = SQL + " WHERE  ";

                        SQL = SQL + "   RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE  =  RM_UOM_MASTER.RM_UM_UOM_CODE (+)";
                        SQL = SQL + "   AND  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE=RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE ";
                        SQL = SQL + "   AND  PC_BUD_BUDGET_MASTER.PC_BUD_BUDGET_CODE = PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_CODE  ";
                        SQL = SQL + "   AND  PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE= PC_BUD_BUDGET_DETAILS.PC_BUD_RESOURCE_CODE ";
                        SQL = SQL + "   AND  RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE = PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE ";
                        SQL = SQL + "   AND  PC_BUD_BUDGET_MASTER.PC_BUD_BUDGET_LAST_YN='Y' ";
                        SQL = SQL + "   AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_STATUS ='A'";

                        if (!string.IsNullOrEmpty(SelectedBranchCode))
                        {
                            SQL = SQL + "   AND RM_RAWMATERIAL_DETAILS.AD_BR_CODE ='" + SelectedBranchCode + "'";
                        }

                        if (!string.IsNullOrEmpty(sBudgetItemCode.ToString().Trim()))
                        {
                            SQL = SQL + "   AND  PC_BUD_BUDGET_ITEM_CODE= '" + sBudgetItemCode + "' ";
                        }

                        if (!string.IsNullOrEmpty(sProjectCode.ToString().Trim()))
                        {
                            SQL = SQL + "   AND  PC_BUD_BUDGET_MASTER.SALES_PM_PROJECT_CODE= '" + sProjectCode + "' ";
                        }

                        if (!string.IsNullOrEmpty(sProjectCode.ToString().Trim()))
                        {
                            SQL = SQL + "   AND  RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE= '" + stCodeFilter + "' ";
                        }
                    }
                    else
                    {
                        SQL = " SELECT RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code , RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION Name, '' Id,    RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE ,  RM_UOM_MASTER.RM_UM_UOM_DESC,    ";

                        SQL = SQL + " '' BUDGET_AMOUNT  ";
                        SQL = SQL + " FROM RM_RAWMATERIAL_MASTER , RM_UOM_MASTER  ";
                        SQL = SQL + "  where RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE  =  RM_UOM_MASTER.RM_UM_UOM_CODE (+)";
                        SQL = SQL + " And RM_RAWMATERIAL_MASTER.RM_RMM_RM_STATUS ='A'";
                    }

                    SQL = SQL + " ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  asc ";
                }
                else if (col == "11")
                {
                    SQL = " SELECT ";
                    SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_CODE Code, ";
                    SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC Name  , '' Id ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC ,''BUDGET_AMOUNT ";
                    SQL = SQL + " FROM ";
                    SQL = SQL + " RM_UOM_MASTER order by RM_UM_UOM_DESC ";
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

        public DataTable FetchSupplierData(string prNo, string finyrid, string srno, string Branch)
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();



                if (!string.IsNullOrEmpty(prNo))
                {
                    SQL = " SELECT DISTINCT RM_PRREQN_QTN.RM_VM_VENDOR_CODE SUPPLIER_CODE,";
                    SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIER_NAME,RMVENDMSTBRANCHPAYTERMSDATA.PAY_TERMS_ID RM_VM_CREDIT_TERMS,";
                    SQL = SQL + "  RMVENDMSTBRANCHPAYTERMSDATA.SALES_PAY_TYPE_DESC,";
                    SQL = SQL + " RMVENDMSTBRANCHTAXTYPEDATA.GL_TAX_TYPE_CODE ,GL_TAX_TYPE_PER  RM_VM_TAX_VAT_PERCENTAGE  ,   RM_VM_TAX_VAT_REG_NUMBER";
                    SQL = SQL + " FROM RM_PRREQN_QTN, RM_VENDOR_MASTER,";// SL_PAY_TYPE_MASTER";

                    SQL = SQL + "(  select ";
                    SQL = SQL + "    RM_VM_VENDOR_CODE ,  AD_BR_CODE , ";
                    SQL = SQL + "    SALES_PAY_TYPE_CODE ,  SALES_PAY_TYPE_DESC , ";
                    SQL = SQL + "    RM_VM_LABLE_VALUE PAY_TERMS_ID , ";
                    SQL = SQL + "    SALES_PAY_NO_DAYS   rm_vm_credit_period ,RM_VM_LABLE_ID ";
                    SQL = SQL + "    from rm_vendor_master_BRANCH_DATA   ,  ";
                    SQL = SQL + "    SL_PAY_TYPE_MASTER  ";
                    SQL = SQL + "    where RM_VM_LABLE_ID ='PAYMENT_TERMS'  ";
                    SQL = SQL + "    and AD_BR_CODE='" + Branch + "' ";
                    SQL = SQL + "    AND  rm_vendor_master_BRANCH_DATA.RM_VM_LABLE_VALUE ";
                    SQL = SQL + "   = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE(+) )RMVENDMSTBRANCHPAYTERMSDATA, ";

                    SQL = SQL + "(select ";
                    SQL = SQL + "RM_VM_VENDOR_CODE ,  AD_BR_CODE , ";
                    SQL = SQL + "GL_TAX_TYPE_CODE ,  GL_TAX_TYPE_NAME , ";
                    SQL = SQL + "RM_VM_LABLE_VALUE   , ";
                    SQL = SQL + "GL_TAX_TYPE_PER,RM_VM_LABLE_ID ";
                    SQL = SQL + "from rm_vendor_master_BRANCH_DATA   ,  ";
                    SQL = SQL + "GL_DEFAULTS_TAX_TYPE_MASTER  ";
                    SQL = SQL + "where RM_VM_LABLE_ID ='TAX_TYPE'  ";
                    SQL = SQL + "    and AD_BR_CODE='" + Branch + "' ";
                    SQL = SQL + "AND  rm_vendor_master_BRANCH_DATA.RM_VM_LABLE_VALUE = GL_DEFAULTS_TAX_TYPE_MASTER.GL_TAX_TYPE_CODE(+)   ";
                    SQL = SQL + ")  RMVENDMSTBRANCHTAXTYPEDATA  ";

                    SQL = SQL + " WHERE   ";
                    SQL = SQL + "   RM_PRREQN_QTN.RM_PRQ_APPROVED = 'Y'";
                    SQL = SQL + " AND RM_PRREQN_QTN.RM_VM_VENDOR_CODE =";
                    SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + "    AND RM_PRREQN_QTN.RM_VM_VENDOR_CODE =  RMVENDMSTBRANCHPAYTERMSDATA.RM_VM_VENDOR_CODE ";
                    SQL = SQL + "    AND RM_PRREQN_QTN.RM_VM_VENDOR_CODE =  RMVENDMSTBRANCHTAXTYPEDATA.RM_VM_VENDOR_CODE ";

                    SQL = SQL + " AND RM_PO_PONO IS NULL";
                    SQL = SQL + " AND RM_PRREQN_QTN.RM_PR_PRNO='" + prNo + "' AND  AD_FIN_FINYRID   = " + finyrid + "";
                    SQL = SQL + " AND RM_PRREQN_QTN.RM_VM_VENDOR_CODE='" + srno + "'";
                    // SQL = SQL + "  And RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS  = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE (+) ";
                    SQL = SQL + "   order by RM_VM_VENDOR_NAME asc  ";
                    //  SL_PAY_TYPE_MASTER  ";
                    //SQL = SQL + "  where  RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS  = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE (+) ";

                }
                else
                {
                    SQL = " SELECT  RM_VENDOR_MASTER.RM_VM_VENDOR_CODE SUPPLIER_CODE,";
                    SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIER_NAME,RMVENDMSTBRANCHPAYTERMSDATA.SALES_PAY_TYPE_DESC,  ";
                    SQL = SQL + "  RMVENDMSTBRANCHTAXTYPEDATA.GL_TAX_TYPE_CODE ,RMVENDMSTBRANCHTAXTYPEDATA.GL_TAX_TYPE_PER RM_VM_TAX_VAT_PERCENTAGE  ,";
                    SQL = SQL + "   RM_VM_TAX_VAT_REG_NUMBER, RMVENDMSTBRANCHPAYTERMSDATA.PAY_TERMS_ID RM_VM_CREDIT_TERMS ";
                    SQL = SQL + " FROM   RM_VENDOR_MASTER , ";// SL_PAY_TYPE_MASTER ";
                    SQL = SQL + "(  select ";
                    SQL = SQL + "    RM_VM_VENDOR_CODE ,  AD_BR_CODE , ";
                    SQL = SQL + "    SALES_PAY_TYPE_CODE ,  SALES_PAY_TYPE_DESC , ";
                    SQL = SQL + "    RM_VM_LABLE_VALUE PAY_TERMS_ID , ";
                    SQL = SQL + "    SALES_PAY_NO_DAYS   rm_vm_credit_period ,RM_VM_LABLE_ID ";
                    SQL = SQL + "    from rm_vendor_master_BRANCH_DATA   ,  ";
                    SQL = SQL + "    SL_PAY_TYPE_MASTER  ";
                    SQL = SQL + "    where RM_VM_LABLE_ID ='PAYMENT_TERMS'  ";
                    SQL = SQL + "    and AD_BR_CODE='" + Branch + "' ";
                    SQL = SQL + "    AND  rm_vendor_master_BRANCH_DATA.RM_VM_LABLE_VALUE ";
                    SQL = SQL + "   = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE(+) )RMVENDMSTBRANCHPAYTERMSDATA, ";

                    SQL = SQL + "(select ";
                    SQL = SQL + "RM_VM_VENDOR_CODE ,  AD_BR_CODE , ";
                    SQL = SQL + "GL_TAX_TYPE_CODE ,  GL_TAX_TYPE_NAME , ";
                    SQL = SQL + "RM_VM_LABLE_VALUE   , ";
                    SQL = SQL + "GL_TAX_TYPE_PER,RM_VM_LABLE_ID ";
                    SQL = SQL + "from rm_vendor_master_BRANCH_DATA   ,  ";
                    SQL = SQL + "GL_DEFAULTS_TAX_TYPE_MASTER  ";
                    SQL = SQL + "where RM_VM_LABLE_ID ='TAX_TYPE'  ";
                    SQL = SQL + "    and AD_BR_CODE='" + Branch + "' ";
                    SQL = SQL + "AND  rm_vendor_master_BRANCH_DATA.RM_VM_LABLE_VALUE = GL_DEFAULTS_TAX_TYPE_MASTER.GL_TAX_TYPE_CODE(+)   ";
                    SQL = SQL + ")  RMVENDMSTBRANCHTAXTYPEDATA  ";
                    SQL = SQL + " WHERE  RM_VENDOR_MASTER.RM_VM_VENDOR_CODE='" + srno + "' ";
                    //  SQL = SQL + "  And RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS  = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE (+) ";
                    SQL = SQL + "    AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE =  RMVENDMSTBRANCHPAYTERMSDATA.RM_VM_VENDOR_CODE ";
                    SQL = SQL + "    AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE =  RMVENDMSTBRANCHTAXTYPEDATA.RM_VM_VENDOR_CODE ";
                    SQL = SQL + "   order by RM_VM_VENDOR_NAME asc  ";

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

        public DataTable DefaultRMStation()
        {
            DataTable dtTable = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "select RM_IP_PARAMETER_VALUE,SALES_STN_STATION_NAME  FROM RM_DEFUALTS_GL_PARAMETERS,SL_STATION_MASTER ";
                SQL = SQL + " WHERE RM_IP_PARAMETER_DESC='STATION' ";
                SQL = SQL + " AND RM_DEFUALTS_GL_PARAMETERS.RM_IP_PARAMETER_VALUE=SL_STATION_MASTER.SALES_STN_STATION_CODE";


                dtTable = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
            }

            return dtTable;
        }

        //public DataSet GetReqDtls ( string prNo, string finyrid, string srno )
        //{

        //    DataSet dtType = new DataSet();
        //    try
        //    {
        //        SQL = string.Empty;
        //        SqlHelper clsSQLHelper = new SqlHelper();

        //        SQL = " SELECT DISTINCT RM_PR_DETAILS.RM_PR_PRNO,";
        //        SQL = SQL + " RM_PR_DETAILS.AD_FIN_FINYRID,";
        //        SQL = SQL + " RM_PR_DETAILS.SALES_STN_STATION_CODE,";
        //        SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
        //        SQL = SQL + " RM_PR_DETAILS.HR_DEPT_DEPT_CODE,";
        //        SQL = SQL + " HR_DEPT_MASTER.HR_DEPT_DEPT_DESC,";
        //        SQL = SQL + " RM_PR_DETAILS.RM_SM_SOURCE_CODE,";
        //        SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC,";
        //        SQL = SQL + " RM_PR_DETAILS.RM_RMM_RM_CODE,";
        //        SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
        //        SQL = SQL + " RM_PR_DETAILS.RM_IM_ITEM_DTL_DESCRIPTION,";
        //        SQL = SQL + " RM_PR_DETAILS.RM_PRD_SL_NO,";
        //        SQL = SQL + " RM_PR_DETAILS.RM_PRD_DIR_USE_QTY+ RM_PR_DETAILS.RM_PRD_INV_QTY TOTALQTY, ";
        //        //SQL = SQL + " RM_PR_DETAILS.RM_PRD_QTY   TOTALQTY, ";
        //        SQL = SQL + " RM_PR_DETAILS.RM_UOM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC ,";
        //        SQL = SQL + " RM_PRREQN_QTN.RM_PRQ_UNIT_PRICE UNIT_PRICE, RM_PRREQN_QTN.RM_PRQ_DISCOUNT, ";
        //        SQL = SQL + " RM_PR_DETAILS.PC_BUD_BUDGET_ITEM_CODE,0 BUDGET_AMOUNT,";
        //        SQL = SQL + " RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE,GL_COSTING_MASTER_VW.NAME COSTNAME,GL_COSM_BUDGET_VAL_YN, ";
        //        SQL = SQL + " RM_PR_MASTER.AD_BR_CODE,AD_BRANCH_MASTER.AD_BR_NAME , RM_PR_STOCK_TYPE_CODE, ";
        //        SQL = SQL + " RM_VENDOR_MASTER.GL_TAX_TYPE_CODE , GL_TAX_TYPE_NAME,  RM_VM_TAX_VAT_PERCENTAGE,RM_RMM_VAT_APPLICABLE_YN   ";
        //        SQL = SQL + " FROM ";
        //        SQL = SQL + " RM_PR_DETAILS,RM_PRREQN_QTN,";
        //        SQL = SQL + " RM_RAWMATERIAL_MASTER,";
        //        SQL = SQL + " SL_STATION_MASTER,";
        //        SQL = SQL + " HR_DEPT_MASTER,";
        //        SQL = SQL + " RM_UOM_MASTER ,RM_SOURCE_MASTER,GL_COSTING_MASTER_VW ,RM_PR_MASTER ,AD_BRANCH_MASTER";//PC_BUD_BUDGET_DETAILS,
        //        SQL = SQL + " WHERE     RM_PR_DETAILS.RM_RMM_RM_CODE =RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
        //        SQL = SQL + " AND RM_PR_MASTER.RM_PR_PRNO =RM_PR_DETAILS.RM_PR_PRNO ";
        //        SQL = SQL + " AND RM_PR_MASTER.AD_BR_CODE =AD_BRANCH_MASTER.AD_BR_CODE ";
        //        SQL = SQL + " AND RM_PR_DETAILS.SALES_STN_STATION_CODE =SL_STATION_MASTER.SALES_STN_STATION_CODE";
        //        SQL = SQL + " AND RM_PR_DETAILS.HR_DEPT_DEPT_CODE =HR_DEPT_MASTER.HR_DEPT_DEPT_CODE(+)";
        //        SQL = SQL + " AND RM_PR_DETAILS.RM_SM_SOURCE_CODE =RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
        //        SQL = SQL + " AND RM_PR_DETAILS.RM_UOM_UOM_CODE =RM_UOM_MASTER.RM_UM_UOM_CODE";
        //        SQL = SQL + " AND RM_PRREQN_QTN.RM_PRQ_APPROVED='Y' ";
        //        SQL = SQL + " AND RM_PRREQN_QTN.RM_PO_PONO IS NULL ";
        //        SQL = SQL + " AND RM_PRREQN_QTN.RM_RMM_RM_CODE=RM_PR_DETAILS.RM_RMM_RM_CODE ";
        //        SQL = SQL + " AND RM_PR_DETAILS.RM_PR_PRNO = RM_PRREQN_QTN.RM_PR_PRNO ";
        //        SQL = SQL + " AND RM_PR_DETAILS.AD_FIN_FINYRID=  RM_PRREQN_QTN.AD_FIN_FINYRID  ";
        //        SQL = SQL + " AND RM_PR_DETAILS.SALES_STN_STATION_CODE = RM_PRREQN_QTN.SALES_STN_STATION_CODE ";
        //        SQL = SQL + " AND RM_PR_DETAILS.RM_SM_SOURCE_CODE=  RM_PRREQN_QTN.RM_SM_SOURCE_CODE ";
        //        // SQL = SQL + " AND RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE=  RM_PRREQN_QTN.GL_COSM_ACCOUNT_CODE ";
        //        // SQL = SQL + " AND RM_PR_DETAILS.PC_BUD_BUDGET_ITEM_CODE=PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_ITEM_CODE(+)";
        //        SQL = SQL + " AND RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE=GL_COSTING_MASTER_VW.CODE(+)";
        //        SQL = SQL + " AND RM_PRREQN_QTN.RM_VM_VENDOR_CODE='" + srno + "'   AND RM_PR_DETAILS.RM_PR_PRNO ='" + prNo + "'";
        //        SQL = SQL + " AND RM_PR_DETAILS.AD_FIN_FINYRID=" + finyrid + "";
        //        SQL = SQL + " ";


        //        dtType = clsSQLHelper.GetDataset(SQL);

        //    }
        //    catch (Exception ex)
        //    {
        //        ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
        //        objLogWriter.WriteLog(ex);
        //    }
        //    finally
        //    {
        //        //ocConn.Close();
        //        //ocConn.Dispose();
        //    }
        //    return dtType;

        //}
        public DataSet GetReqDtls(string prNo, string finyrid, string srno)
        {

            DataSet dtType = new DataSet();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT DISTINCT RM_PR_DETAILS.RM_PR_PRNO,";
                SQL = SQL + " RM_PR_DETAILS.AD_FIN_FINYRID,";
                SQL = SQL + " RM_PR_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + " RM_PR_DETAILS.HR_DEPT_DEPT_CODE,";
                SQL = SQL + " HR_DEPT_MASTER.HR_DEPT_DEPT_DESC,";
                SQL = SQL + " RM_PR_DETAILS.RM_SM_SOURCE_CODE,";
                SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC,";
                SQL = SQL + " RM_PR_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_PR_DETAILS.RM_IM_ITEM_DTL_DESCRIPTION,";
                SQL = SQL + " RM_PR_DETAILS.RM_PRD_SL_NO,";
                SQL = SQL + " RM_PR_DETAILS.RM_PRD_DIR_USE_QTY+ RM_PR_DETAILS.RM_PRD_INV_QTY TOTALQTY, ";
                //SQL = SQL + " RM_PR_DETAILS.RM_PRD_QTY   TOTALQTY, ";
                SQL = SQL + " RM_PR_DETAILS.RM_UOM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC ,";
                SQL = SQL + " RM_PRREQN_QTN.RM_PRQ_UNIT_PRICE UNIT_PRICE, RM_PRREQN_QTN.RM_PRQ_DISCOUNT, ";
                SQL = SQL + " RM_PR_DETAILS.PC_BUD_BUDGET_ITEM_CODE,0 BUDGET_AMOUNT,";
                SQL = SQL + " RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE,GL_COSTING_MASTER_VW.NAME COSTNAME,GL_COSM_BUDGET_VAL_YN, ";
                SQL = SQL + " RM_PR_MASTER.AD_BR_CODE,AD_BRANCH_MASTER.AD_BR_NAME , RM_PR_STOCK_TYPE_CODE, ";
                SQL = SQL + " RM_VENDOR_MASTER.GL_TAX_TYPE_CODE , GL_TAX_TYPE_NAME,  RM_VM_TAX_VAT_PERCENTAGE,RM_RMM_VAT_APPLICABLE_YN   ";
                SQL = SQL + " FROM ";
                SQL = SQL + " RM_PR_DETAILS,RM_PRREQN_QTN,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " SL_STATION_MASTER,";
                SQL = SQL + " HR_DEPT_MASTER,RM_VENDOR_MASTER,GL_DEFAULTS_TAX_TYPE_MASTER,";
                SQL = SQL + " RM_UOM_MASTER ,RM_SOURCE_MASTER,GL_COSTING_MASTER_VW ,RM_PR_MASTER ,AD_BRANCH_MASTER";//PC_BUD_BUDGET_DETAILS,
                SQL = SQL + " WHERE     RM_PR_DETAILS.RM_RMM_RM_CODE =RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " AND RM_PR_MASTER.RM_PR_PRNO =RM_PR_DETAILS.RM_PR_PRNO ";
                SQL = SQL + " AND RM_PR_MASTER.AD_BR_CODE =AD_BRANCH_MASTER.AD_BR_CODE ";
                SQL = SQL + " AND RM_PR_DETAILS.SALES_STN_STATION_CODE =SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_PR_DETAILS.HR_DEPT_DEPT_CODE =HR_DEPT_MASTER.HR_DEPT_DEPT_CODE(+)";
                SQL = SQL + " AND RM_PR_DETAILS.RM_SM_SOURCE_CODE =RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                SQL = SQL + " AND RM_PR_DETAILS.RM_UOM_UOM_CODE =RM_UOM_MASTER.RM_UM_UOM_CODE";
                SQL = SQL + " AND RM_PRREQN_QTN.RM_PRQ_APPROVED='Y' ";
                SQL = SQL + " AND RM_PRREQN_QTN.RM_PO_PONO IS NULL ";
                SQL = SQL + " AND RM_PRREQN_QTN.RM_RMM_RM_CODE=RM_PR_DETAILS.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_PR_DETAILS.RM_PR_PRNO = RM_PRREQN_QTN.RM_PR_PRNO ";
                SQL = SQL + " AND RM_PR_DETAILS.AD_FIN_FINYRID=  RM_PRREQN_QTN.AD_FIN_FINYRID  ";
                SQL = SQL + " AND RM_PR_DETAILS.SALES_STN_STATION_CODE = RM_PRREQN_QTN.SALES_STN_STATION_CODE ";
                SQL = SQL + " AND RM_PR_DETAILS.RM_SM_SOURCE_CODE=  RM_PRREQN_QTN.RM_SM_SOURCE_CODE ";
                SQL = SQL + " AND RM_PRREQN_QTN.RM_VM_VENDOR_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                SQL = SQL + " AND RM_VENDOR_MASTER.GL_TAX_TYPE_CODE  = GL_DEFAULTS_TAX_TYPE_MASTER.GL_TAX_TYPE_CODE (+)  ";

                // SQL = SQL + " AND RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE=  RM_PRREQN_QTN.GL_COSM_ACCOUNT_CODE ";
                // SQL = SQL + " AND RM_PR_DETAILS.PC_BUD_BUDGET_ITEM_CODE=PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_ITEM_CODE(+)";
                SQL = SQL + " AND RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE=GL_COSTING_MASTER_VW.CODE(+)";
                SQL = SQL + " AND RM_PRREQN_QTN.RM_VM_VENDOR_CODE='" + srno + "'   AND RM_PR_DETAILS.RM_PR_PRNO ='" + prNo + "'";
                SQL = SQL + " AND RM_PR_DETAILS.AD_FIN_FINYRID=" + finyrid + "";
                SQL = SQL + " ";


                dtType = clsSQLHelper.GetDataset(SQL);

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
        public DataTable FetchRMUsingKeyIn(string SelectedItem, string sEnteredCode)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                // new Modifiction Consumption UOM



                SQL = " SELECT RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE RMCode , RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION Description, '' Id,    RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE RM_UM_UOM_CODE  ,  RM_UOM_MASTER.RM_UM_UOM_DESC    RM_UM_UOM_DESC  ";
                SQL = SQL + " FROM RM_RAWMATERIAL_MASTER , RM_UOM_MASTER  ";
                SQL = SQL + "  where RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE  =  RM_UOM_MASTER.RM_UM_UOM_CODE (+)";
                SQL = SQL + " And RM_RAWMATERIAL_MASTER.RM_RMM_RM_STATUS ='A'";

                SQL = SQL + " 	  AND  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ='" + sEnteredCode + "'";

                if (!string.IsNullOrEmpty(SelectedItem))
                {
                    SQL = SQL + " 	  AND  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  NOT IN ( '" + SelectedItem + "') ";
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


        public DataTable FillItemWiseViewPurchaseOrderTelrick(string fromdate, string todate, string sFilterType, object mngrclassobj)
        {
            DataTable dtMain = new DataTable("MAIN");
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = " SELECT RM_PO_MASTER.RM_PO_PONO PCODE,RM_PO_MASTER.RM_VM_VENDOR_CODE SCODE,     ";
                SQL = SQL + "        RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SNAME,RM_PO_MASTER.AD_FIN_FINYRID ID,RM_PO_MASTER.AD_BR_CODE,   " ;
                SQL = SQL + "        TO_CHAR(RM_PO_MASTER.RM_PO_PO_DATE,'DD-MON-YYYY') PDATE,RM_PO_MASTER.RM_MPOM_PAYMENT_TERMS,   " ;
                SQL = SQL + "        RM_PO_DETAILS.RM_RMM_RM_CODE RM_CODE,RM_RMM_RM_DESCRIPTION RM_NAME,RM_IM_ITEM_DTL_DESCRIPTION RM_SPECIFICATION,   " ;
                SQL = SQL + "        TO_CHAR(RM_PO_AGG_START_DATE,'DD-MON-YYYY') RM_PO_AGG_START_DATE,   " ;
                SQL = SQL + "        TO_CHAR(RM_PO_AGG_END_DATE,'DD-MON-YYYY') RM_PO_AGG_END_DATE,   " ;
                SQL = SQL + "        RM_POD_UNIT_PRICE,RM_PO_MASTER.RM_PO_NET_AMOUNT,   " ;
                SQL = SQL + "        RM_PO_MASTER. RM_PO_VERIFIED VERIFIED, RM_PO_MASTER.RM_PO_APPROVED RM_PO_APPROVED ,   " ;
                SQL = SQL + "        RM_PO_REMARKS, RM_SM_SOURCE_DESC SOURCE,  " ;
                SQL = SQL + "        DECODE(RM_PO_PO_STATUS,'O','OPEN','C','CLOSED','N','CANCELLED')  STATUS , " ;
                SQL = SQL + "        RM_PO_MASTER.AD_CUR_CURRENCY_CODE,AD_CUR_CURRENCY_NAME  " ;
                SQL = SQL + " FROM RM_VENDOR_MASTER,RM_PO_MASTER,RM_PO_DETAILS ,RM_RAWMATERIAL_MASTER ,RM_SOURCE_MASTER ,AD_CURRENCY_MASTER  " ;
                SQL = SQL + " WHERE RM_PO_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE   " ;
                SQL = SQL + "  AND RM_PO_MASTER.RM_PO_PONO =RM_PO_DETAILS.RM_PO_PONO   " ;
                SQL = SQL + "  AND RM_PO_DETAILS.RM_RMM_RM_CODE =RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE (+)    ";
                SQL = SQL + "  AND RM_PO_DETAILS.RM_SM_SOURCE_CODE =RM_SOURCE_MASTER.RM_SM_SOURCE_CODE  (+)   ";
                SQL = SQL + "  AND RM_PO_MASTER.AD_CUR_CURRENCY_CODE = AD_CURRENCY_MASTER.AD_CUR_CURRENCY_CODE  (+)  " ;


                //SQL = SQL + "  AND RM_PO_MASTER.ad_fin_finyrid = " + mngrclass.FinYearID + "  AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "    AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";


                if (mngrclass.UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                    SQL = SQL + "  and     RM_PO_MASTER.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                }

                if (sFilterType == "NOT APPROVED")
                {
                    SQL = SQL + " AND   RM_PO_MASTER.RM_PO_VERIFIED = 'Y' and  RM_PO_MASTER.RM_PO_VERIFIED_MNGR = 'Y'  AND RM_PO_MASTER.RM_PO_APPROVED ='N'";
                }
                else if (sFilterType == "APPROVED")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_APPROVED ='Y'";
                }
                else if (sFilterType == "CLOSED")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_PO_STATUS ='C'";
                }
                else if (sFilterType == "CANCELLED")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_CANCEL_REMARKS is not null";
                }
                else if (sFilterType == "VERIFIED")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_VERIFIED = 'Y' "; //and WS_PO_MASTER.WS_PO_APPROVED ='N' AND WS_PO_MASTER.WS_PO_PO_STATUS = 'O' JOMY 
                }
                else if (sFilterType == "NOT VERIFIED")
                {
                    SQL = SQL + " AND   RM_PO_MASTER.RM_PO_VERIFIED = 'N' and  RM_PO_MASTER.RM_PO_VERIFIED_MNGR = 'N' AND RM_PO_MASTER.RM_PO_APPROVED ='N' "; // AND WS_PO_MASTER.WS_PO_PO_STATUS = 'O'
                }
                else if (sFilterType == "VERIFIED-MNGR")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_VERIFIED_MNGR = 'Y' ";
                }
                else if (sFilterType == "NOT VERIFIED-MNGR")
                {
                    SQL = SQL + " AND   RM_PO_MASTER.RM_PO_VERIFIED = 'Y' and  RM_PO_MASTER.RM_PO_VERIFIED_MNGR = 'N' AND RM_PO_MASTER.RM_PO_APPROVED ='N' "; // AND WS_PO_MASTER.WS_PO_PO_STATUS = 'O'
                }
                else if (sFilterType == "REJECTED")
                {
                    // SQL = SQL + " AND RM_PO_MASTER.WS_PO_PO_STATUS = 'R' ";
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_PO_STATUS = 'R' ";
                }


                //   <asp:ListItem>NOT APPROVED</asp:ListItem>
                //            <asp:ListItem>APPROVED</asp:ListItem>
                //            <asp:ListItem>ALL</asp:ListItem> 
                //            <asp:ListItem>CLOSED</asp:ListItem> 
                //            <asp:ListItem></asp:ListItem> 

                SQL = SQL + " order by RM_PO_MASTER.RM_PO_PONO desc";

                dtMain = clsSQLHelper.GetDataTableByCommand(SQL);





            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dtMain;
        }



        public List<SearchPoSavedItems> FillItemWiseViewPurchaseOrder(string fromdate, string todate, string sFilterType, object mngrclassobj)
        {
            List<SearchPoSavedItems> PoitemsListRet = new List<SearchPoSavedItems>();
            DataTable dtMain = new DataTable("MAIN");
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = " SELECT RM_PO_MASTER.RM_PO_PONO PCode,RM_PO_MASTER.RM_VM_VENDOR_CODE SCode,   ";
                SQL = SQL + "RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SName,RM_PO_MASTER.AD_FIN_FINYRID Id,RM_PO_MASTER.AD_BR_CODE, ";
                SQL = SQL + " to_char(RM_PO_MASTER.RM_PO_PO_DATE,'DD-MON-YYYY') PDate,RM_PO_MASTER.RM_MPOM_PAYMENT_TERMS, ";
                SQL = SQL + " RM_PO_DETAILS.RM_RMM_RM_CODE RM_CODE,RM_RMM_RM_DESCRIPTION RM_NAME,RM_IM_ITEM_DTL_DESCRIPTION RM_SPECIFICATION, ";
                SQL = SQL + " to_char(RM_PO_AGG_START_DATE,'DD-MON-YYYY') RM_PO_AGG_START_DATE, ";
                SQL = SQL + "  to_char(RM_PO_AGG_END_DATE,'DD-MON-YYYY') RM_PO_AGG_END_DATE, ";
                SQL = SQL + "  RM_POD_UNIT_PRICE,RM_PO_MASTER.RM_PO_NET_AMOUNT, ";
                SQL = SQL + " RM_PO_MASTER. RM_PO_VERIFIED VERIFIED, RM_PO_MASTER.RM_PO_APPROVED RM_PO_APPROVED , ";
                SQL = SQL + " RM_PO_REMARKS, RM_SM_SOURCE_DESC SOURCE,";
                SQL = SQL + " DECODE(RM_PO_PO_STATUS,'O','OPEN','C','CLOSED','N','CANCELLED')  STATUS ";

                SQL = SQL + " FROM RM_VENDOR_MASTER,RM_PO_MASTER,RM_PO_DETAILS ,RM_RAWMATERIAL_MASTER ,RM_SOURCE_MASTER ";

                SQL = SQL + " WHERE RM_PO_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + " and RM_PO_MASTER.RM_PO_PONO =RM_PO_DETAILS.RM_PO_PONO ";
                SQL = SQL + "  and RM_PO_DETAILS.RM_RMM_RM_CODE =RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "  and RM_PO_DETAILS.RM_SM_SOURCE_CODE =RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";

                SQL = SQL + "  AND RM_PO_MASTER.ad_fin_finyrid = " + mngrclass.FinYearID + "  AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";


                if (mngrclass.UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                    SQL = SQL + "  and     RM_PO_MASTER.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                }

                if (sFilterType == "NOT APPROVED")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_APPROVED ='N'";
                }
                else if (sFilterType == "APPROVED")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_APPROVED ='Y'";
                }
                else if (sFilterType == "CLOSED")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_PO_STATUS ='C'";
                }
                else if (sFilterType == "CANCELLED")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_CANCEL_REMARKS is not null";
                }


                //   <asp:ListItem>NOT APPROVED</asp:ListItem>
                //            <asp:ListItem>APPROVED</asp:ListItem>
                //            <asp:ListItem>ALL</asp:ListItem> 
                //            <asp:ListItem>CLOSED</asp:ListItem> 
                //            <asp:ListItem></asp:ListItem> 

                SQL = SQL + " order by RM_PO_MASTER.RM_PO_PONO desc";

                dtMain = clsSQLHelper.GetDataTableByCommand(SQL);



                PoitemsListRet = (from DataRow dr in dtMain.Rows
                                  select new SearchPoSavedItems()
                                  {
                                      PCode = dr["PCode"].ToString(),
                                      ID = Convert.ToInt32(dr["ID"].ToString()),
                                      SCode = dr["SCode"].ToString(),
                                      SName = dr["SName"].ToString(),
                                      AD_BR_CODE = dr["AD_BR_CODE"].ToString(),
                                      RM_MPOM_PAYMENT_TERMS = dr["RM_MPOM_PAYMENT_TERMS"].ToString(),
                                      PDate = dr["PDate"].ToString(),
                                      RM_PO_AGG_START_DATE = dr["RM_PO_AGG_START_DATE"].ToString(),
                                      RM_PO_AGG_END_DATE = dr["RM_PO_AGG_END_DATE"].ToString(),
                                      SOURCE = dr["SOURCE"].ToString(),
                                      RM_CODE = dr["RM_CODE"].ToString(),
                                      RM_NAME = dr["RM_NAME"].ToString(),
                                      RM_PO_APPROVED = dr["RM_PO_APPROVED"].ToString(),
                                      RM_SPECIFICATION = dr["RM_SPECIFICATION"].ToString(),
                                      RM_POD_UNIT_PRICE = Convert.ToDouble(dr["RM_POD_UNIT_PRICE"].ToString()),
                                      RM_PO_NET_AMOUNT = Convert.ToDouble(dr["RM_PO_NET_AMOUNT"].ToString()),
                                      STATUS = dr["STATUS"].ToString(),
                                      RM_PO_REMARKS = dr["RM_PO_REMARKS"].ToString()
                                  }).ToList();


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return PoitemsListRet;
        }
        public DataTable FillViewPurchaseOrder(string fromdate, string todate, string sFilterType, object mngrclassobj)
        {
            DataTable dt = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;



                SQL = " SELECT RM_PO_MASTER.RM_PO_PONO PCode,RM_PO_MASTER.RM_VM_VENDOR_CODE SCode,  ";
                SQL = SQL + "RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SName,AD_FIN_FINYRID Id,RM_PO_MASTER.AD_BR_CODE,";
                SQL = SQL + " to_char(RM_PO_MASTER.RM_PO_PO_DATE,'DD-MON-YYYY') PDate,RM_PO_MASTER.RM_MPOM_PAYMENT_TERMS,";
                SQL = SQL + " RM_PO_MASTER. RM_PO_VERIFIED VERIFIED, RM_PO_MASTER.RM_PO_APPROVED RM_PO_APPROVED ";

                SQL = SQL + " FROM RM_VENDOR_MASTER,RM_PO_MASTER ";

                SQL = SQL + " WHERE RM_PO_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
               // SQL = SQL + "  AND ad_fin_finyrid = " + mngrclass.FinYearID + "  AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "   AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";


                if (mngrclass.UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                    SQL = SQL + "  and     RM_PO_MASTER.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                }

                if (sFilterType == "NOT APPROVED")
                {
                    // SQL = SQL + " AND RM_PO_MASTER.RM_PO_APPROVED ='N'";
                    SQL = SQL + " AND   RM_PO_MASTER.RM_PO_VERIFIED = 'Y' and  RM_PO_MASTER.RM_PO_VERIFIED_MNGR = 'Y' AND RM_PO_MASTER.RM_PO_APPROVED ='N' "; // AND WS_PO_MASTER.WS_PO_PO_STATUS = 'O'

                }
                else if (sFilterType == "APPROVED")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_APPROVED ='Y'";
                }
                else if (sFilterType == "CLOSED")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_PO_STATUS ='C'";
                }
                else if (sFilterType == "CANCELLED")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_CANCEL_REMARKS is not null";
                }


                else if (sFilterType == "VERIFIED")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_VERIFIED = 'Y' "; //and WS_PO_MASTER.WS_PO_APPROVED ='N' AND WS_PO_MASTER.WS_PO_PO_STATUS = 'O' JOMY 
                }
                else if (sFilterType == "NOT VERIFIED")
                {
                    SQL = SQL + " AND   RM_PO_MASTER.RM_PO_VERIFIED = 'N' and  RM_PO_MASTER.RM_PO_VERIFIED_MNGR = 'N' AND RM_PO_MASTER.RM_PO_APPROVED ='N' "; // AND WS_PO_MASTER.WS_PO_PO_STATUS = 'O'
                }
                else if (sFilterType == "VERIFIED-MNGR")
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_VERIFIED_MNGR = 'Y' ";
                }
                else if (sFilterType == "NOT VERIFIED-MNGR")
                {
                    SQL = SQL + " AND   RM_PO_MASTER.RM_PO_VERIFIED = 'Y' and  RM_PO_MASTER.RM_PO_VERIFIED_MNGR = 'N' AND RM_PO_MASTER.RM_PO_APPROVED ='N' "; // AND WS_PO_MASTER.WS_PO_PO_STATUS = 'O'
                }
                else if (sFilterType == "REJECTED")
                {
                    // SQL = SQL + " AND RM_PO_MASTER.WS_PO_PO_STATUS = 'R' ";
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_PO_STATUS = 'R' ";
                }



                //   <asp:ListItem>NOT APPROVED</asp:ListItem>
                //            <asp:ListItem>APPROVED</asp:ListItem>
                //            <asp:ListItem>ALL</asp:ListItem> 
                //            <asp:ListItem>CLOSED</asp:ListItem> 
                //            <asp:ListItem></asp:ListItem> 

                SQL = SQL + " order by RM_PO_MASTER.RM_PO_PONO desc";


                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dt;

        }

        public DataTable FillBranch()
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();



                SQL = "  SELECT AD_BR_CODE CODE,AD_BR_NAME NAME FROM AD_BRANCH_MASTER ";
                SQL = SQL + " WHERE  AD_BR_ACTIVESTATUS_YN ='Y'";
                SQL = SQL + "   ORDER BY AD_BR_SORT_ID ASC  ";

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


        public DataSet FillCurrencyCombo()
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  AD_CUR_CURRENCY_CODE CODE , AD_CUR_CURRENCY_NAME NAME  , AD_CUR_EXCHANGE_RATE  FROM  AD_CURRENCY_MASTER order by AD_CUR_CURRENCY_NAME ";
                dtGrade = clsSQLHelper.GetDataTableByCommand(SQL);
                // GRADE
                dsData.Tables.Add(dtGrade);
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

        public double FetchCurrencyRate(string sCurreencyCode)
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT   AD_CUR_EXCHANGE_RATE  FROM  AD_CURRENCY_MASTER  where AD_CUR_CURRENCY_CODE = '" + sCurreencyCode + "'";
                dtGrade = clsSQLHelper.GetDataTableByCommand(SQL);
                // GRADE
                dsData.Tables.Add(dtGrade);
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
            return double.Parse(dsData.Tables[0].Rows[0]["AD_CUR_EXCHANGE_RATE"].ToString());
        }

        public DataTable fillViewBudgetItemCode(string sSelected)
        {
            DataTable dtTable = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT  ";
                SQL = SQL + "   PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_ITEM_CODE BUDGET_ITEM_CODE , ";
                SQL = SQL + "   PC_BUD_BUDGET_DETAILS.PC_BUD_COST_TYPE_CODE COST_TYPE_CODE,   PC_BUD_COST_TYPE_MASTER.PC_BUD_COST_TYPE_NAME COST_TYPE_NAME,";
                SQL = SQL + "   PC_BUD_BUDGET_DETAILS.PC_BUD_DIVISION_CODE DIV_CODE,    PC_BUD_DIVSION_MASTER.PC_BUD_DIVISION_NAME DIV_NAME,";
                SQL = SQL + "   PC_BUD_BUDGET_DETAILS.PC_BOQG_CODE BOQG_CODE, PC_BOQ_GROUP_MASTER.PC_BOQG_NAME BOQG_NAME,";
                SQL = SQL + "   PC_BUD_BUDGET_DETAILS.PC_BUD_CATEGORY_CODE CAT_CODE,   PC_BUD_CATEGORY_MASTER.PC_BUD_CATEGORY_NAME CAT_NAME,  ";
                SQL = SQL + "   PC_BUD_BUDGET_DETAILS.PC_BUD_RESOURCE_CODE RESC_CODE,  PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_NAME RESC_NAME ";

                SQL = SQL + " FROM    ";
                SQL = SQL + "   PC_BUD_BUDGET_MASTER,PC_BUD_BUDGET_DETAILS, ";
                SQL = SQL + "   PC_BUD_COST_TYPE_MASTER,PC_BUD_DIVSION_MASTER,PC_BOQ_GROUP_MASTER ,PC_BUD_CATEGORY_MASTER, PC_BUD_RESOURCE_MASTER ";
                SQL = SQL + " ";

                SQL = SQL + " WHERE  ";
                SQL = SQL + "   PC_BUD_BUDGET_MASTER.PC_BUD_BUDGET_CODE =  PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_CODE ";
                SQL = SQL + "   AND  PC_BUD_BUDGET_MASTER.SALES_INQM_ENQUIRY_NO = PC_BUD_BUDGET_DETAILS.SALES_INQM_ENQUIRY_NO ";
                SQL = SQL + "   AND  PC_BUD_BUDGET_DETAILS.PC_BUD_COST_TYPE_CODE =   PC_BUD_COST_TYPE_MASTER.PC_BUD_COST_TYPE_CODE";
                SQL = SQL + "   AND  PC_BUD_BUDGET_DETAILS.PC_BUD_DIVISION_CODE =PC_BUD_DIVSION_MASTER.PC_BUD_DIVISION_CODE ";
                SQL = SQL + "   AND  PC_BUD_BUDGET_DETAILS.PC_BOQG_CODE =PC_BOQ_GROUP_MASTER.PC_BOQG_CODE  ";
                SQL = SQL + "   AND  PC_BUD_BUDGET_DETAILS.PC_BUD_CATEGORY_CODE =PC_BUD_CATEGORY_MASTER.PC_BUD_CATEGORY_CODE  ";
                SQL = SQL + "   AND  PC_BUD_BUDGET_DETAILS.PC_BUD_RESOURCE_CODE =PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE  ";
                SQL = SQL + "   AND PC_BUD_BUDGET_LAST_YN = 'Y'";
                SQL = SQL + "   AND PC_BUD_BUDGET_APPROVED='Y' ";
                SQL = SQL + "   AND PC_BUD_BUDGET_STATUS ='ACTIVE' ";

                SQL = SQL + "   and PC_BUD_BUDGET_DETAILS.PC_BUD_RESOURCE_CODE in (select distinct PC_BUD_RESOURCE_CODE from RM_RAWMATERIAL_MASTER)";
                if (!string.IsNullOrEmpty(sSelected.ToString().Trim()))
                {
                    SQL = SQL + " AND PC_BUD_BUDGET_MASTER.SALES_PM_PROJECT_CODE='" + sSelected + "'  ";
                }

                SQL = SQL + " ORDER BY PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_ITEM_CODE ASC";

                dtTable = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
            }

            return dtTable;
        }

        public DataSet FillTollCombo()
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  RM_UOM_TOLL_CODE,RM_UOM_TOLL_NAME ,RM_UOM_TOLL_SORT_ID FROM RM_UOM_TOLL_MASTER order by RM_UOM_TOLL_SORT_ID asc ";

                dtGrade = clsSQLHelper.GetDataTableByCommand(SQL);
                // GRADE
                dsData.Tables.Add(dtGrade);
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

        public string MaterialType(string ItemCode)
        {

            DataTable dtType = new DataTable();
            string RmType = string.Empty;
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT distinct RM_RMM_RM_TYPE ";
                SQL = SQL + " FROM RM_RAWMATERIAL_MASTER   ";
                SQL = SQL + " where RM_RMM_RM_CODE='" + ItemCode + "'";

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

                RmType = dtType.Rows[0]["RM_RMM_RM_TYPE"].ToString();

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
            return RmType;

        }



        public DataTable CostAccountView(string Branch, string UserName)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT CODE,NAME ,GROUPNAME,GL_COSM_BUDGET_VAL_YN ";
                SQL = SQL + " FROM GL_COSTING_MASTER_VW ";
                SQL = SQL + " WHERE GL_COSM_ACCOUNT_STATUS ='ACTIVE'";// Active Project only needs to come 


                SQL = SQL + " AND AD_BR_CODE='" + Branch + "' ";

                if (UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                    SQL = SQL + "  and     AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                }

                // SQL = SQL + " AND GROUPCODE='101' ";// Project Costing Group
                SQL = SQL + " ORDER BY CODE ASC";
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


        #region "Approval Reghts"
        public DataTable fnChkApprovalRigths(string userId, double totalAmnt)
        {

            string SQL = null;
            SqlHelper clsSQLHelper = new SqlHelper();
            DataTable dsAppr = default(DataTable);

            try
            {
                SQL = "";
                SQL = " SELECT AD_APPROVALSLAB_PO.ad_apo_slab slab";
                SQL = SQL + " FROM AD_APPROVALSLAB_PO, ad_mail_approval_users";
                SQL = SQL + " WHERE AD_APPROVALSLAB_PO.ad_um_userid =  ad_mail_approval_users.ad_um_userid";
                SQL = SQL + " AND AD_APPROVALSLAB_PO.ad_af_id = ad_mail_approval_users.ad_af_id";
                SQL = SQL + " AND AD_APPROVALSLAB_PO.ad_um_userid = '" + userId + "'";
                SQL = SQL + " AND AD_APPROVALSLAB_PO.ad_af_id = 'RMOTOA'";

                dsAppr = null;

                dsAppr = clsSQLHelper.GetDataTableByCommand(SQL);


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);

            }
            return dsAppr;

        }
        #endregion 

        #region "DML"

        public string InsertSQL(PurchaseOrderPRCEntity oEntity, List<PurchaseOrderPRCLPO> EntityLPO,
            List<PurchaseOrderPRCTermsAndConditionEntity> objFPSNoteEntity,
            string piNoofSup, object mngrclassobj,
            bool bDocAutogenerated, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " INSERT INTO RM_PO_MASTER (";
                SQL = SQL + " RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                SQL = SQL + " RM_PO_PO_DATE, RM_VM_VENDOR_CODE, RM_PO_PRICETYPE, RM_PO_EXP_DATE, ";

                SQL = SQL + " RM_PO_ENTRY_TYPE , RM_PO_EXP_DATE_TO, ";

                ///FORIEGN CURRENCY CHANGE DONE BY TONY ON 14-NOV-2022
                ///

                SQL = SQL + " RM_PO_GRAND_TOTAL_FC, RM_PO_DISCOUNT_PERC_FC, RM_PO_DISCOUNT_AMOUNT_FC, ";
                SQL = SQL + " RM_PO_NET_AMOUNT_FC,RM_PO_VAT_PERCENTAGE_FC,RM_PO_VAT_AMOUNT_FC,   ";

                ///----------------------------/////////////////////////------
                ///

                SQL = SQL + " RM_PO_GRAND_TOTAL, RM_PO_DISCOUNT_PERC, RM_PO_DISCOUNT_AMOUNT, ";
                SQL = SQL + " RM_PO_NET_AMOUNT,RM_PO_VAT_PERCENTAGE,RM_PO_VAT_AMOUNT,   ";


                SQL = SQL + "RM_PO_PO_STATUS, RM_PO_REMARKS, ";
                SQL = SQL + " AD_CUR_CURRENCY_CODE, RM_PO_EXCHANGE_RATE, RM_PO_APPROVED, ";
                SQL = SQL + " RM_PO_APPROVEDBY, RM_PO_PURENTRY, RM_PO_POTYPE, ";
                SQL = SQL + " RM_PO_CANCEL_REMARKS, RM_PO_CONTAC1_CODE, RM_PO_CONTAC2_CODE, ";

                SQL = SQL + " RM_PR_PRNO, RM_PR_FIN_FINYRID, RM_PO_SUPP_REF, ";
                SQL = SQL + " RM_PO_SUPP_REF_DATE, RM_PO_OUR_REF, RM_PO_CREATEDBY, ";
                SQL = SQL + " RM_PO_CREATEDDATE, RM_PO_UPDATEDBY, RM_PO_UPDATEDDATE, ";
                SQL = SQL + " RM_PO_DELETESTATUS, ";
                SQL = SQL + " RM_PO_VERIFIED, RM_PO_VERIFIEDBY, RM_PO_POSTEDREMARKS, ";

                SQL = SQL + " RM_MPOM_DELIVERY_TIME_RMKS, RM_MPOM_DELIVERY_PLACE_RMKS, ";
                SQL = SQL + " RM_MPOM_VALIDITY, RM_MPOM_PAYMENT_TERMS, RM_MPOM_PAYMENT_MODE,RM_PO_COST_CENTER,";

                SQL = SQL + " RM_PO_UOM_TYPE , AD_BR_CODE,RM_PO_RM_TYPE,RM_PO_CURR_TYPE, ";
                SQL = SQL + " RM_VM_CREDIT_TERMS,RM_PO_QTY_RATE_TYPE,HR_DEPT_DEPT_CODE, ";
                SQL = SQL + " RM_PO_AGG_START_DATE,RM_PO_AGG_END_DATE , RM_PO_AGG_END_DATE_REQUIRED_YN,RM_PO_APPROVEDBY_DATE,";
                SQL = SQL + "  RM_PO_VERIFIEDBY_DATE,RM_PO_VERIFIED_MNGR,RM_PO_VERIFIEDBY_MNGR,RM_PO_VERIFIED_MNGR_DATE,RM_PO_STOCK_TYPE_CODE,RM_PO_RATE_REVISION_APP_YN ) ";
                SQL = SQL + " VALUES ('" + oEntity.RM_PO_PONO + "' ,'" + oEntity.AD_CM_COMPANY_CODE + "' ," + oEntity.AD_FIN_FINYRID + " ,";
                SQL = SQL + "'" + oEntity.RM_PO_PO_DATE.ToString("dd-MMM-yyyy") + "' ,'" + oEntity.RM_VM_VENDOR_CODE + "' ,'" + oEntity.RM_PO_PRICETYPE + "', '" + oEntity.RM_PO_EXP_DATE.ToString("dd-MMM-yyyy") + "' ,";

                SQL = SQL + "'" + oEntity.RM_PO_ENTRY_TYPE + "', '" + oEntity.RM_PO_EXP_DATE_TO.ToString("dd-MMM-yyyy") + "' ,";

                SQL = SQL + " " + Convert.ToDouble(oEntity.RM_PO_GRAND_TOTAL_FC) + ",0,0 ,";
                SQL = SQL + "" + Convert.ToDouble(oEntity.RM_PO_NET_AMOUNT_FC) + ", " + Convert.ToDouble(oEntity.TaxPerc_FC) + "," + Convert.ToDouble(oEntity.TaxAmount_FC) + " , ";


                SQL = SQL + " " + Convert.ToDouble(oEntity.RM_PO_GRAND_TOTAL) + ",0,0 ,";
                SQL = SQL + "" + Convert.ToDouble(oEntity.RM_PO_NET_AMOUNT) + ", " + Convert.ToDouble(oEntity.TaxPerc) + "," + Convert.ToDouble(oEntity.TaxAmount) + " , ";


                SQL = SQL + "'O' ,'" + oEntity.RM_PO_REMARKS + "' ,";
                SQL = SQL + "'" + oEntity.AD_CUR_CURRENCY_CODE + "' ," + oEntity.RM_PO_EXCHANGE_RATE + " ,'" + oEntity.RM_PO_APPROVED + "',";
                SQL = SQL + " '" + oEntity.RM_PO_APPROVEDBY + "','" + oEntity.RM_PO_PURENTRY + "' ,'" + oEntity.RM_PO_POTYPE + "',";
                SQL = SQL + " '', '" + oEntity.RM_PO_CONTAC1_CODE + "' ,'" + oEntity.RM_PO_CONTAC2_CODE + "' ,";
                SQL = SQL + "'" + oEntity.RM_PR_PRNO + "' ,'" + oEntity.RM_PR_FIN_FINYRID + "' ,'" + oEntity.RM_PO_SUPP_REF + "' ,";

                SQL = SQL + "'" + oEntity.RM_PO_SUPP_REF_DATE.ToString("dd-MMM-yyyy") + "' ,'" + oEntity.RM_PO_OUR_REF + "' , '" + mngrclass.UserName + "' ,";
                SQL = SQL + " TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM'), '','' ,";
                SQL = SQL + "0, ";
                SQL = SQL + "'" + oEntity.RM_PO_VERIFIED + "' ,'" + oEntity.RM_PO_VERIFIEDBY + "','" + oEntity.RM_PO_POSTEDREMARKS + "',";

                SQL = SQL + " '" + oEntity.RM_MPOM_DELIVERY_TIME_RMKS + "' , '" + oEntity.RM_MPOM_DELIVERY_PLACE_RMKS + "' , ";
                SQL = SQL + "'" + oEntity.RM_MPOM_VALIDITY + "','" + oEntity.RM_MPOM_PAYMENT_TERMS + "', '" + oEntity.RM_MPOM_PAYMENT_MODE + "', '" + oEntity.sCostCenter + "',";

                SQL = SQL + "'" + oEntity.sPOUOMType + "' ,'" + oEntity.Branch + "','" + oEntity.sLPOReceiptType + "','" + oEntity.SPOCurrType + "',";

                SQL = SQL + " '" + oEntity.PayTerms + "','" + oEntity.QtyType + "','" + oEntity.Dept + "',";
                SQL = SQL + " '" + oEntity.AggStartDate.ToString("dd-MMM-yyyy") + "' ,";
                if (oEntity.AggrementEndDateApplicableYN == "Y")
                {
                    SQL = SQL + "'" + oEntity.AggEndDate.ToString("dd-MMM-yyyy") + "',";
                }
                else
                {
                    SQL = SQL + "'',";
                }
                SQL = SQL + "'" + oEntity.AggrementEndDateApplicableYN + "',TO_DATE( '" + oEntity.ApprovedTime + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + "TO_DATE( '" + oEntity.VerifiedTime + "','DD-MM-YYYY HH:MI:SS AM'),'" + oEntity.ManagerVerified + "',";
                SQL = SQL + "'" + oEntity.ManagerVerifiedBy + "',TO_DATE( '" + oEntity.ManagerVerifiedTime + "','DD-MM-YYYY HH:MI:SS AM') ,'" + oEntity.StockType + "','" + oEntity.RM_PO_RATE_REVISION_APP_YN + "' ";

                SQL = SQL + "  )";

                sSQLArray.Add(SQL);



                //if (!string.IsNullOrEmpty(oEntity.RM_PR_PRNO))
                //{ // as soon as created the Po against system should close the PR

                //    SQL = " UPDATE RM_PR_MASTER";
                //    SQL = SQL + " SET RM_PR_PR_STATUS = 'C'";
                //    SQL = SQL + " WHERE RM_pR_pRno = '" + oEntity.RM_PR_PRNO + "' AND ad_fin_finyrid = " + oEntity.RM_PR_FIN_FINYRID + "";

                //    sSQLArray.Add(SQL);
                //}

                sRetun = string.Empty;
                sRetun = InsertPOProjectDetails(oEntity.RM_PO_PONO, oEntity.AD_FIN_FINYRID, EntityLPO, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;
                sRetun = InsertPODetails(oEntity.RM_PO_PONO, oEntity.AD_FIN_FINYRID, EntityLPO, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }
                sRetun = string.Empty;

                sRetun = InsertTermsAndConditions(oEntity.RM_PO_PONO, objFPSNoteEntity, mngrclassobj, oEntity.AD_FIN_FINYRID);

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;
                if (oEntity.RM_PO_APPROVED == "Y")
                {
                    sRetun = InsertApprovalQrs(oEntity.RM_PO_PONO, oEntity.AD_FIN_FINYRID, oEntity.RM_PO_PO_DATE.ToString("dd-MMM-yyyy"), oEntity.RM_PO_APPROVEDBY);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }
                //sSQLArray.Add(ResSetNextDocNumberStation("RTMO", oEntity.RM_PO_PONO, oEntity.AD_FIN_FINYRID));

                sRetun = string.Empty;
                sRetun = ModifiyPurQuotation("I", oEntity, EntityLPO, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;
                //double PrDetailCount = 0;

                //if (!string.IsNullOrEmpty(oEntity.RM_PR_PRNO))
                //{
                //    PrDetailCount = FillPrGridCount(oEntity.RM_PR_PRNO, oEntity.RM_PR_FIN_FINYRID);

                //    //if (piNoofSup == "1")
                //    if (Convert.ToDouble(piNoofSup.ToString()) == PrDetailCount)
                //    {

                //        sRetun = ModifyPurReqStatus("C", oEntity.RM_PR_PRNO, int.Parse(oEntity.RM_PR_FIN_FINYRID));
                //        if (sRetun != "CONTINUE")
                //        {
                //            return sRetun;
                //        }
                //    }
                //}



                sRetun = string.Empty;
                //WTPO //  RTMO // need to icrease the same sesaril therefor usind this tab number .// jomy 
                sRetun = oTrns.SetTrans(mngrclass.UserName, oEntity.AD_FIN_FINYRID, mngrclass.CompanyCode, DateTime.Now, "WTPO", oEntity.RM_PO_PONO, bDocAutogenerated, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);

                if (sRetun == "Save Successful")
                {
                    //sRetun = string.Empty;
                    double PrDetailCount = 0;

                    if (!string.IsNullOrEmpty(oEntity.RM_PR_PRNO))
                    {
                        PrDetailCount = FillPrGridCount(oEntity.RM_PR_PRNO, oEntity.RM_PR_FIN_FINYRID);

                        //if (piNoofSup == "1")
                        if (PrDetailCount == 0)
                        {
                            sSQLArray.Clear();

                            sRetun = ModifyPurReqStatus("C", oEntity.RM_PR_PRNO, int.Parse(oEntity.RM_PR_FIN_FINYRID));

                            if (sRetun != "CONTINUE")
                            {
                                return sRetun;
                            }
                            //if (sRetun == "CONTINUE")
                            //{
                            //    return "Save Successful";
                            //}
                            else
                            {
                                sRetun = oTrns.SetTrans(mngrclass.UserName, oEntity.AD_FIN_FINYRID, mngrclass.CompanyCode, DateTime.Now, "RTMO", oEntity.RM_PO_PONO, bDocAutogenerated, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);
                            }
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
            return sRetun;
        }

        private string GetRMDefaultDpt()
        {

            string SQL = null;
            SqlHelper clsSQLHelper = new SqlHelper();
            DataTable dsRMDefaltDept = new DataTable();

            try
            {

                SQL = "";
                SQL = " select RM_IP_PARAMETER_VALUE   ";
                SQL = SQL + "    from RM_DEFUALTS_GL_PARAMETERS ";
                SQL = SQL + "    where  RM_IP_PARAMETER_DESC ='DEPARTMENT' ";


                dsRMDefaltDept = null;

                dsRMDefaltDept = clsSQLHelper.GetDataTableByCommand(SQL);


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);

            }
            return dsRMDefaltDept.Rows[0]["RM_IP_PARAMETER_VALUE"].ToString();

        }

        private string InsertPOProjectDetails(string sOrderNo, int iFinYr, List<PurchaseOrderPRCLPO> EntityLPO, object mngrclassobj)
        {
            string sRetun = string.Empty;
            Int16 i = 0;

            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                object oDeptCode = null;

                oDeptCode = GetRMDefaultDpt();

                if (string.IsNullOrWhiteSpace(oDeptCode.ToString()))
                {
                    return "Not Yet defined Rawmaterila Default department";
                }

                i = 0;
                foreach (var Data in EntityLPO)
                {
                    if (Data.oItemCodeLPO != null)
                    {
                        ++i;
                        SQL = " INSERT INTO RM_PO_PROJECT_DETAILS (";
                        SQL = SQL + " RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                        SQL = SQL + " RM_RMM_RM_CODE,RM_IM_ITEM_DTL_DESCRIPTION, RM_POD_SL_NO, SALES_STN_STATION_CODE, ";
                        SQL = SQL + " HR_DEPT_DEPT_CODE, RM_SM_SOURCE_CODE,";// RM_IM_ITEM_DTL_DESCRIPTION, ";
                        SQL = SQL + " RM_POD_QTY, RM_UOM_UOM_CODE, ";

                        ////FOREING CURRENCY CHANGES DONE BY TONY ON  14-NOV-2022 
                        SQL = SQL + "RM_POD_UNIT_PRICE_FC,RM_POD_NEWPRICE_FC, RM_POD_DISCOUNT_PERCENT_FC, RM_POD_DISCOUNT_AMOUNT_FC, RM_POD_TOTALAMT_FC, ";
                        SQL = SQL + "RM_POD_RM_AMOUNT_BFR_VAT_FC, RM_POD_RM_VAT_RATE_FC,RM_POD_RM_VAT_AMOUNT_FC ,";

                        //--------------------///////////////

                        SQL = SQL + "RM_POD_UNIT_PRICE,RM_POD_NEWPRICE, RM_POD_DISCOUNT_PERCENT, RM_POD_DISCOUNT_AMOUNT, RM_POD_TOTALAMT, ";
                        SQL = SQL + "RM_POD_RM_AMOUNT_BFR_VAT, RM_POD_RM_VAT_RATE,RM_POD_RM_VAT_AMOUNT ,";



                        SQL = SQL + " RM_POD_PENDING_QTY,PC_BUD_BUDGET_ITEM_CODE,GL_COSM_ACCOUNT_CODE, ";
                        SQL = SQL + " RM_UOM_TOLL_CODE, RM_POD_TOLL_RATE,";
                        SQL = SQL + " RM_POD_RM_VAT_TYPE,RM_PRFQ_DOC_NO,RM_PRFQ_COMBINED_DOC_NO,RM_PRFQD_SLNO   ) ";
                        SQL = SQL + " VALUES ('" + sOrderNo + "' , '" + mngrclass.CompanyCode + "'  ," + iFinYr + " ,";
                        SQL = SQL + " '" + Data.oItemCodeLPO + "' ,'" + Data.oItemSpecification + "' , " + i + ", '" + Data.oStcodeLPO + "',";
                        SQL = SQL + "'" + oDeptCode + "' ,'" + Data.oSoruceCodeLPO + "' ,";// '" + Data.oDetailDescLPO + "' ,";
                        SQL = SQL + " " + Data.dOrderQtyLPO + " ,  '" + Data.oUOMCodeLPO + "' ,";

                        SQL = SQL + "" + Data.dUnitPirceLPO_FC + " ," + Data.dNewUnitPirceLPO_FC + "," + Data.dDiscntPerLPO_FC + " , " + Data.dDiscAmountLPO_FC + ", " + Data.dTotalAmountLPO_FC + ",";
                        SQL = SQL + "" + Data.AmountBefTax_FC + "," + Data.VatRate_FC + "," + Data.VatAmount_FC + " ,";

                        SQL = SQL + "" + Data.dUnitPirceLPO + " ," + Data.dNewUnitPirceLPO + "," + Data.dDiscntPerLPO + " , " + Data.dDiscAmountLPO + ", " + Data.dTotalAmountLPO + ",";
                        SQL = SQL + "" + Data.AmountBefTax + "," + Data.VatRate + "," + Data.VatAmount + " ,";



                        SQL = SQL + "" + Data.dOrderQtyLPO + " , '" + Data.oBudget_ItemCodeLPO + "','" + Data.oProectCostCodeLPO + "',";
                        SQL = SQL + " '" + Data.oTollUom + "' ," + Data.oTollRate + ",";
                        SQL = SQL + " '" + Data.VatType + "','" + Data.RFQNo + "','" + Data.CombinedDocno + "','" + Data.RFQslno + "')";

                        sSQLArray.Add(SQL);

                        // VERY IMPORTANT ORDER QTY AND PENDING QTY IS KEEPING SAME BCZU THIS VALUE USING IN RM PENDING ORDER REPORT / JOMY 28 JAN 20113 
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


        public string InsertTermsAndConditions(string QTNNo, List<PurchaseOrderPRCTermsAndConditionEntity> oEntity, object mngrclassobj, int EntryFinYearId)
        {
            string sRetun = string.Empty;
            Int32 SLNO = 0;
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                foreach (var Data in oEntity)
                {
                    SLNO++;

                    SQL = " INSERT INTO RM_PO_NOTES ( ";
                    SQL = SQL + "RM_PO_PONO, AD_FIN_FINYRID, RM_PO_SL_NO,  ";
                    SQL = SQL + "   RM_PO_NOTES, RM_PO_PRINT_YN, RM_PO_DELSTATUS ";
                    SQL = SQL + " ) ";
                    SQL = SQL + "VALUES ( '" + QTNNo + "'," + EntryFinYearId + ", " + Data.SlNo + ", ";
                    SQL = SQL + " '" + Data.sText + "', '" + Data.sPrintYN + "' ,0 ) ";

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


        private string InsertPODetails(string sOrderNo, int iFinYr, List<PurchaseOrderPRCLPO> EntityLPO, object mngrclassobj)
        {
            string sRetun = string.Empty;
            Int16 i = 0;

            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                object oDeptCode = null;

                oDeptCode = GetRMDefaultDpt();

                if (string.IsNullOrWhiteSpace(oDeptCode.ToString()))
                {
                    return "Not Yet defined Rawmaterila Default department";
                }

                i = 0;
                //foreach (var Data in EntityLPO)
                //{
                //    if (Data.oItemCodeLPO != null)
                //    {
                //        ++i;
                //        SQL = " INSERT INTO RM_PO_DETAILS (";
                //        SQL = SQL + " RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                //        SQL = SQL + " RM_RMM_RM_CODE, RM_POD_SL_NO, SALES_STN_STATION_CODE, ";
                //        SQL = SQL + " HR_DEPT_DEPT_CODE, RM_SM_SOURCE_CODE, RM_IM_ITEM_DTL_DESCRIPTION, ";
                //        SQL = SQL + " RM_POD_QTY, RM_POD_UNIT_PRICE, RM_UOM_UOM_CODE, ";
                //        SQL = SQL + " RM_POD_DISCOUNT_PERCENT, RM_POD_DISCOUNT_AMOUNT, RM_POD_TOTALAMT, ";
                //        SQL = SQL + " RM_POD_PENDING_QTY, RM_POD_NEWPRICE,PC_BUD_BUDGET_ITEM_CODE) ";
                //        SQL = SQL + " VALUES ('" + sOrderNo + "' , '" + mngrclass.CompanyCode + "'  ," + iFinYr + " ,";
                //        SQL = SQL + " '" + Data.oItemCodeLPO + "' , " + i + ", '" + Data.oStcodeLPO + "',";
                //        SQL = SQL + "'" + oDeptCode + "' ,'" + Data.oSoruceCodeLPO + "' ,'" + Data.oDetailDescLPO + "' ,";
                //        SQL = SQL + " " + Data.dOrderQtyLPO + " , " + Data.dUnitPirceLPO + " , '" + Data.oUOMCodeLPO + "' ,";
                //        SQL = SQL + "" + Data.dDiscntPerLPO + " , " + Data.dDiscAmountLPO + ", " + Data.dTotalAmountLPO + ",";
                //        SQL = SQL + "" + Data.dOrderQtyLPO + " ," + Data.dNewUnitPirceLPO + ", '" + Data.oBudget_ItemCodeLPO + "')";

                //        sSQLArray.Add(SQL);

                //        // VERY IMPORTANT ORDER QTY AND PENDING QTY IS KEEPING SAME BCZU THIS VALUE USING IN RM PENDING ORDER REPORT / JOMY 28 JAN 20113 
                //    }
                //}

                /////JOMY AND NAYANA MODIFIED FOR TRHE VAT CHANGES 13 AUG 2022 ///////////SQL = " INSERT INTO RM_PO_DETAILS (";
                ////////////////SQL = SQL + "  RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                ////////////////SQL = SQL + "  RM_RMM_RM_CODE, RM_POD_SL_NO, SALES_STN_STATION_CODE, ";
                ////////////////SQL = SQL + "  HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE,  RM_IM_ITEM_DTL_DESCRIPTION, ";
                ////////////////SQL = SQL + "  RM_POD_QTY, RM_POD_UNIT_PRICE, RM_UOM_UOM_CODE, ";
                ////////////////SQL = SQL + "  RM_POD_DISCOUNT_PERCENT, RM_POD_DISCOUNT_AMOUNT, RM_POD_TOTALAMT, ";
                ////////////////SQL = SQL + "  RM_POD_PENDING_QTY, RM_POD_NEWPRICE, RM_POD_STATUS,PC_BUD_BUDGET_ITEM_CODE ,";
                ////////////////SQL = SQL + "  RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE,RM_POD_RM_AMOUNT_BFR_VAT,RM_POD_RM_VAT_TYPE,";
                ////////////////SQL = SQL + "  RM_POD_RM_VAT_RATE,RM_POD_RM_VAT_AMOUNT)";
                ////////////////SQL = SQL + "  select ";
                ////////////////SQL = SQL + "  RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                ////////////////SQL = SQL + "  RM_RMM_RM_CODE,  row_number() over (order by null) AS   RM_POD_SL_NO, SALES_STN_STATION_CODE, ";
                ////////////////SQL = SQL + "  HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE,  RM_IM_ITEM_DTL_DESCRIPTION, ";
                ////////////////SQL = SQL + "  sum(RM_POD_QTY) RM_POD_QTY, ";
                ////////////////SQL = SQL + "   CASE SUM (RM_POD_QTY)";
                ////////////////SQL = SQL + "     WHEN 0 ";
                ////////////////SQL = SQL + "       THEN 0";
                ////////////////// SQL = SQL + "    ELSE (SUM (RM_POD_TOTALAMT) / SUM (RM_POD_QTY))";
                ////////////////SQL = SQL + "    ELSE ((SUM (RM_POD_TOTALAMT) / SUM (RM_POD_QTY))-RM_POD_TOLL_RATE)";
                ////////////////SQL = SQL + "   END AS RM_POD_UNIT_PRICE,";
                ////////////////SQL = SQL + "   RM_UOM_UOM_CODE, ";
                ////////////////SQL = SQL + "  0 RM_POD_DISCOUNT_PERCENT, 0 RM_POD_DISCOUNT_AMOUNT, sum(RM_POD_TOTALAMT) RM_POD_TOTALAMT, ";
                ////////////////SQL = SQL + "  sum(RM_POD_PENDING_QTY)RM_POD_PENDING_QTY,";
                ////////////////SQL = SQL + "   CASE SUM (RM_POD_QTY)";
                ////////////////SQL = SQL + "     WHEN 0 ";
                ////////////////SQL = SQL + "      THEN 0"; 
                ////////////////SQL = SQL + "    ELSE ((SUM (RM_POD_TOTALAMT) / SUM (RM_POD_QTY))-RM_POD_TOLL_RATE)";
                ////////////////SQL = SQL + "   END AS RM_POD_NEWPRICE,";
                ////////////////SQL = SQL + "  'O',PC_BUD_BUDGET_ITEM_CODE,RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE ,sum(RM_POD_RM_AMOUNT_BFR_VAT) RM_POD_RM_AMOUNT_BFR_VAT ,";
                ////////////////SQL = SQL + "  RM_POD_RM_VAT_TYPE,sum(RM_POD_RM_VAT_RATE) RM_POD_RM_VAT_RATE,sum(RM_POD_RM_VAT_AMOUNT) RM_POD_RM_VAT_AMOUNT ";
                ////////////////SQL = SQL + "  from RM_PO_PROJECT_DETAILS ";
                ////////////////SQL = SQL + "  WHERE  RM_PO_PONO ='" + sOrderNo + "' ";
                ////////////////SQL = SQL + "  AND   AD_FIN_FINYRID = " + iFinYr + " ";
                ////////////////SQL = SQL + "  group by  RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                ////////////////SQL = SQL + "  RM_RMM_RM_CODE, SALES_STN_STATION_CODE, ";
                ////////////////SQL = SQL + "  HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE, ";
                ////////////////SQL = SQL + "  RM_UOM_UOM_CODE,PC_BUD_BUDGET_ITEM_CODE,RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE ,RM_IM_ITEM_DTL_DESCRIPTION,RM_POD_RM_VAT_TYPE";


                SQL = "    INSERT INTO RM_PO_DETAILS ";
                SQL = SQL + "    ( ";
                SQL = SQL + "        RM_PO_PONO,AD_CM_COMPANY_CODE,AD_FIN_FINYRID,RM_RMM_RM_CODE, ";
                SQL = SQL + "        RM_POD_SL_NO, ";
                SQL = SQL + "        SALES_STN_STATION_CODE,HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE,RM_IM_ITEM_DTL_DESCRIPTION, ";
                SQL = SQL + "        RM_POD_QTY,RM_UOM_UOM_CODE, ";

                SQL = SQL + "        RM_POD_UNIT_PRICE_FC,RM_POD_NEWPRICE_FC, ";
                SQL = SQL + "        RM_POD_RM_AMOUNT_BFR_VAT_FC,  ";
                SQL = SQL + "        RM_POD_DISCOUNT_PERCENT_FC,RM_POD_DISCOUNT_AMOUNT_FC, ";
                SQL = SQL + "        RM_POD_RM_VAT_RATE_FC,RM_POD_RM_VAT_AMOUNT_FC , ";
                SQL = SQL + "        RM_POD_TOTALAMT_FC ,  ";

                SQL = SQL + "        RM_POD_UNIT_PRICE,RM_POD_NEWPRICE, ";
                SQL = SQL + "        RM_POD_RM_AMOUNT_BFR_VAT,  ";
                SQL = SQL + "        RM_POD_DISCOUNT_PERCENT,RM_POD_DISCOUNT_AMOUNT, ";
                SQL = SQL + "        RM_POD_RM_VAT_RATE,RM_POD_RM_VAT_AMOUNT , ";
                SQL = SQL + "        RM_POD_TOTALAMT,   ";


                SQL = SQL + "        RM_POD_PENDING_QTY, ";
                SQL = SQL + "        RM_POD_STATUS,PC_BUD_BUDGET_ITEM_CODE,RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE,  ";
                SQL = SQL + "        RM_POD_RM_VAT_TYPE  ";
                SQL = SQL + "    ) ";
                SQL = SQL + "    SELECT ";
                SQL = SQL + "        RM_PO_PONO,AD_CM_COMPANY_CODE,AD_FIN_FINYRID, RM_RMM_RM_CODE, ";
                SQL = SQL + "        ROW_NUMBER () OVER (ORDER BY NULL)    AS RM_POD_SL_NO, ";
                SQL = SQL + "        SALES_STN_STATION_CODE,HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE, RM_IM_ITEM_DTL_DESCRIPTION, ";
                SQL = SQL + "        SUM (RM_POD_QTY)  RM_POD_QTY, RM_UOM_UOM_CODE,";

                /////FOREIGN CURRENCY CHANGES DONE BY TONY ON 14-NOV-2022

                SQL = SQL + "        CASE SUM (RM_POD_QTY)     WHEN 0 THEN 0  ELSE  (  (SUM (RM_POD_RM_AMOUNT_BFR_VAT_FC) / SUM (RM_POD_QTY)) )  END AS RM_POD_UNIT_PRICE_FC, ";
                SQL = SQL + "        CASE SUM (RM_POD_QTY) WHEN 0 THEN 0 ";
                SQL = SQL + "        ELSE(  (SUM (RM_POD_RM_AMOUNT_BFR_VAT_FC) / SUM (RM_POD_QTY)) ) ";
                SQL = SQL + "        END  AS RM_POD_NEWPRICE_FC, ";

                SQL = SQL + "        SUM (RM_POD_RM_AMOUNT_BFR_VAT_FC)  RM_POD_RM_AMOUNT_BFR_VAT_FC ,      ";
                SQL = SQL + "        0  RM_POD_DISCOUNT_PERCENT_FC,0 RM_POD_DISCOUNT_AMOUNT_FC,  ";

                //SQL = SQL + "        CASE SUM (RM_POD_RM_AMOUNT_BFR_VAT_FC)  WHEN 0 THEN 0  ELSE ((SUM (RM_POD_RM_VAT_AMOUNT_FC) / SUM (RM_POD_RM_AMOUNT_BFR_VAT_FC))*100) END AS RM_POD_RM_VAT_RATE_FC, ";
                SQL = SQL + "        RM_POD_RM_VAT_RATE RM_POD_RM_VAT_RATE_FC, ";

                SQL = SQL + "        SUM (RM_POD_RM_VAT_AMOUNT_FC)            RM_POD_RM_VAT_AMOUNT_FC, ";
                SQL = SQL + "         SUM (RM_POD_TOTALAMT_FC)        RM_POD_TOTALAMT_FC,   ";


                ////---------------------------------//////--------------------

                SQL = SQL + "        CASE SUM (RM_POD_QTY)     WHEN 0 THEN 0  ELSE  (  (SUM (RM_POD_RM_AMOUNT_BFR_VAT) / SUM (RM_POD_QTY)) )  END AS RM_POD_UNIT_PRICE, ";
                SQL = SQL + "        CASE SUM (RM_POD_QTY) WHEN 0 THEN 0 ";
                SQL = SQL + "        ELSE(  (SUM (RM_POD_RM_AMOUNT_BFR_VAT) / SUM (RM_POD_QTY)) ) ";
                SQL = SQL + "        END  AS RM_POD_NEWPRICE, ";

                SQL = SQL + "        SUM (RM_POD_RM_AMOUNT_BFR_VAT)  RM_POD_RM_AMOUNT_BFR_VAT ,      ";
                SQL = SQL + "        0  RM_POD_DISCOUNT_PERCENT,0 RM_POD_DISCOUNT_AMOUNT,  ";

                // SQL = SQL + "        CASE SUM (RM_POD_RM_AMOUNT_BFR_VAT)  WHEN 0 THEN 0  ELSE ((SUM (RM_POD_RM_VAT_AMOUNT) / SUM (RM_POD_RM_AMOUNT_BFR_VAT))*100) END AS RM_POD_RM_VAT_RATE, ";
                SQL = SQL + "         RM_POD_RM_VAT_RATE, ";



                SQL = SQL + "        SUM (RM_POD_RM_VAT_AMOUNT)            RM_POD_RM_VAT_AMOUNT, ";
                SQL = SQL + "         SUM (RM_POD_TOTALAMT)        RM_POD_TOTALAMT ,  ";




                SQL = SQL + "        SUM (RM_POD_PENDING_QTY)              RM_POD_PENDING_QTY, ";
                SQL = SQL + "        'O',PC_BUD_BUDGET_ITEM_CODE,RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE,  ";
                SQL = SQL + "        RM_POD_RM_VAT_TYPE ";
                // SQL = SQL + "        CASE SUM (RM_POD_RM_VAT_RATE)     WHEN 0 THEN 0 ELSE  (  (SUM (RM_POD_RM_VAT_AMOUNT) / SUM (RM_POD_RM_VAT_RATE)) )  END AS RM_POD_RM_VAT_RATE,  ";
                SQL = SQL + "    FROM ";
                SQL = SQL + "        RM_PO_PROJECT_DETAILS ";
                SQL = SQL + "  WHERE  RM_PO_PONO ='" + sOrderNo + "'  AND   AD_FIN_FINYRID = " + iFinYr + "";
                SQL = SQL + "    GROUP BY  ";
                SQL = SQL + "        RM_PO_PONO,AD_CM_COMPANY_CODE,AD_FIN_FINYRID, ";
                SQL = SQL + "        RM_RMM_RM_CODE,SALES_STN_STATION_CODE,HR_DEPT_DEPT_CODE, ";
                SQL = SQL + "        RM_SM_SOURCE_CODE,RM_UOM_UOM_CODE,PC_BUD_BUDGET_ITEM_CODE, ";
                SQL = SQL + "        RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE,RM_IM_ITEM_DTL_DESCRIPTION,RM_POD_RM_VAT_TYPE,RM_POD_RM_VAT_RATE  ";



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

        public string InsertApprovalQrs(string sPono, int ifinYear, string sPoDate, string sAprovedby)
        {
            string sRetun = string.Empty;

            try
            {
                SQL = " INSERT INTO RM_PO_TRIGGER (";
                SQL = SQL + " RM_PO_PONO, AD_FIN_FINYRID, AD_UM_USERID, ";
                SQL = SQL + " RM_PO_PO_DATE, RM_PO_POST_DATE) ";

                SQL = SQL + " VALUES ('" + sPono + "', '" + ifinYear + "', '" + sAprovedby + "', '" + sPoDate + "',  TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM') )";

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

        public string ModifyPurReqStatus(string sStatus, string PRNO, int PurReqFinYear)
        {

            string sRetun = string.Empty;

            try
            {
                ////SessionManager mngrclass = (SessionManager)mngrclassobj;
                OracleHelper oTrns = new OracleHelper();

                //string strSql = null;
                int i = 0;
                if (sStatus == "C")
                {
                    SQL = "UPDATE RM_PR_MASTER SET RM_PR_PR_STATUS = '" + sStatus + "' WHERE ";
                    SQL = SQL + " RM_PR_PRNO = '" + PRNO + "' AND  AD_FIN_FINYRID =" + PurReqFinYear + " AND RM_PR_DELETESTATUS =0";
                }
                else
                {
                    SQL = "UPDATE RM_PR_MASTER SET RM_PR_PR_STATUS = '" + sStatus + "' WHERE ";
                    SQL = SQL + " RM_PR_PRNO = '" + PRNO + "'  AND AD_FIN_FINYRID =" + PurReqFinYear + " AND RM_PR_DELETESTATUS =0";
                }

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

        public string ModifiyPurQuotation(string sFlag, PurchaseOrderPRCEntity oEntity, List<PurchaseOrderPRCLPO> objWSPOGridEntity, object mngrclassobj)
        {

            string sRetun = string.Empty;

            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                OracleHelper oTrns = new OracleHelper();
                //string strSql = null;
                int i = 0;
                object loItem = null;

                foreach (var Data in objWSPOGridEntity)
                {
                    if (!string.IsNullOrEmpty(Data.oItemCodeLPO.ToString()))
                    {

                        if (sFlag == "I")
                        {
                            SQL = "UPDATE RM_PRREQN_QTN SET RM_PO_PONO = '" + oEntity.RM_PO_PONO + "', RM_PO_PO_FINYEAR_ID  = " + oEntity.AD_FIN_FINYRID + " WHERE ";
                            SQL = SQL + " RM_PR_PRNO = '" + oEntity.RM_PR_PRNO + "' AND RM_RMM_RM_CODE = '" + Data.oItemCodeLPO + "' ";
                            SQL = SQL + " AND RM_VM_VENDOR_CODE ='" + oEntity.RM_VM_VENDOR_CODE + "' AND RM_PRQ_APPROVED ='Y' AND AD_FIN_FINYRID =" + oEntity.RM_PR_FIN_FINYRID + "";
                        }
                        else
                        {
                            SQL = "UPDATE RM_PRREQN_QTN SET RM_PO_PONO = Null  , RM_PO_PO_FINYEAR_ID  =0 WHERE ";
                            SQL = SQL + " RM_PR_PRNO = '" + oEntity.RM_PR_PRNO + "' AND RM_RMM_RM_CODE = '" + Data.oItemCodeLPO + "' ";
                            SQL = SQL + " AND RM_VM_VENDOR_CODE ='" + oEntity.RM_VM_VENDOR_CODE + "' AND RM_PRQ_APPROVED ='Y' AND AD_FIN_FINYRID =" + oEntity.RM_PR_FIN_FINYRID + "";
                        }

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

        public string ResSetNextDocNumber(string FormCode, string CurrentNumber, int fiFinYrId)
        {
            int liLength = 0;
            string lsCode = null;
            int liZeros = 0;
            int liNumber = 0;
            int i = 0;

            string lsResult = "";

            lsCode = CurrentNumber;
            liLength = lsCode.Length;
            liNumber = Convert.ToInt16(lsCode) + 1;
            liZeros = liLength - Convert.ToString(liNumber).Length;

            for (i = 1; i <= liZeros; i++)
            {
                lsResult = lsResult + "0";
            }

            lsResult = lsResult + liNumber;

            return "UPDATE AD_DOC_NUMBER SET AD_DN_NEXT_NO='" + lsResult + "' WHERE AD_MI_ITEMID='" + FormCode + "' AND AD_FIN_FINYRID=" + fiFinYrId + "";
        }

        public string ModifySQL(PurchaseOrderPRCEntity oEntity, List<PurchaseOrderPRCLPO> EntityLPO, List<PurchaseOrderPRCTermsAndConditionEntity> objFPSNoteEntity, object mngrclassobj, string EntryFinYearId)
        {
            string sRetun = string.Empty;
            double ProjCount = 0;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " UPDATE RM_PO_MASTER SET ";

                SQL = SQL + " RM_PO_PO_DATE ='" + oEntity.RM_PO_PO_DATE.ToString("dd-MMM-yyyy") + "' ,RM_VM_VENDOR_CODE ='" + oEntity.RM_VM_VENDOR_CODE + "' , RM_PO_PRICETYPE = '" + oEntity.RM_PO_PRICETYPE + "', AD_CUR_CURRENCY_CODE ='" + oEntity.AD_CUR_CURRENCY_CODE + "', RM_PO_EXCHANGE_RATE   =" + oEntity.RM_PO_EXCHANGE_RATE + ",";
                SQL = SQL + " RM_PO_ENTRY_TYPE ='" + oEntity.RM_PO_ENTRY_TYPE + "', RM_PO_EXP_DATE ='" + oEntity.RM_PO_EXP_DATE.ToString("dd-MMM-yyyy") + "' ,RM_PO_EXP_DATE_TO = '" + oEntity.RM_PO_EXP_DATE_TO.ToString("dd-MMM-yyyy") + "' ,";

                ///FORIEGN CURRENCT CHANGE DONE BY TONY ON 14-NOV-2022
                ///

                SQL = SQL + " RM_PO_GRAND_TOTAL_FC = " + Convert.ToDouble(oEntity.RM_PO_GRAND_TOTAL_FC) + "  , RM_PO_DISCOUNT_PERC_FC =0 ,RM_PO_DISCOUNT_AMOUNT_FC =0 ,";
                SQL = SQL + " RM_PO_VAT_PERCENTAGE_FC=" + Convert.ToDouble(oEntity.TaxPerc_FC) + ", RM_PO_VAT_AMOUNT_FC=" + Convert.ToDouble(oEntity.TaxAmount_FC) + " , ";
                SQL = SQL + " RM_PO_NET_AMOUNT_FC =" + Convert.ToDouble(oEntity.RM_PO_NET_AMOUNT_FC) + " ,";

                ///-------------------//////////////-------------//////----------
                ///


                SQL = SQL + " RM_PO_GRAND_TOTAL = " + Convert.ToDouble(oEntity.RM_PO_GRAND_TOTAL) + "  , RM_PO_DISCOUNT_PERC =0 ,RM_PO_DISCOUNT_AMOUNT =0 ,";
                SQL = SQL + " RM_PO_VAT_PERCENTAGE=" + Convert.ToDouble(oEntity.TaxPerc) + ", RM_PO_VAT_AMOUNT=" + Convert.ToDouble(oEntity.TaxAmount) + " , ";
                SQL = SQL + " RM_PO_NET_AMOUNT =" + Convert.ToDouble(oEntity.RM_PO_NET_AMOUNT) + " ,";



                SQL = SQL + " RM_PO_REMARKS ='" + oEntity.RM_PO_REMARKS + "' ,";
                SQL = SQL + " RM_PO_APPROVED ='" + oEntity.RM_PO_APPROVED + "',";
                SQL = SQL + " RM_PO_APPROVEDBY= '" + oEntity.RM_PO_APPROVEDBY + "',RM_PO_RATE_REVISION_APP_YN='" + oEntity.RM_PO_RATE_REVISION_APP_YN + "',   ";
                SQL = SQL + " RM_PO_CONTAC1_CODE= '" + oEntity.RM_PO_CONTAC1_CODE + "' ,RM_PO_CONTAC2_CODE='" + oEntity.RM_PO_CONTAC2_CODE + "' ,";
                SQL = SQL + " RM_PO_SUPP_REF ='" + oEntity.RM_PO_SUPP_REF + "' ,";

                SQL = SQL + " RM_PO_SUPP_REF_DATE='" + oEntity.RM_PO_SUPP_REF_DATE.ToString("dd-MMM-yyyy") + "' ,RM_PO_OUR_REF='" + oEntity.RM_PO_OUR_REF + "' ,RM_PO_UPDATEDBY= '" + mngrclass.UserName + "' ,";
                SQL = SQL + " RM_PO_UPDATEDDATE= TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM'),";

                SQL = SQL + " RM_PO_VERIFIED ='" + oEntity.RM_PO_VERIFIED + "' , RM_PO_VERIFIEDBY ='" + oEntity.RM_PO_VERIFIEDBY + "', RM_PO_POSTEDREMARKS= '" + oEntity.RM_PO_POSTEDREMARKS + "',";

                SQL = SQL + " RM_MPOM_DELIVERY_TIME_RMKS= '" + oEntity.RM_MPOM_DELIVERY_TIME_RMKS + "' ,RM_MPOM_DELIVERY_PLACE_RMKS= '" + oEntity.RM_MPOM_DELIVERY_PLACE_RMKS + "' , ";
                SQL = SQL + " RM_MPOM_VALIDITY= '" + oEntity.RM_MPOM_VALIDITY + "',RM_MPOM_PAYMENT_TERMS='" + oEntity.RM_MPOM_PAYMENT_TERMS + "',RM_MPOM_PAYMENT_MODE= '" + oEntity.RM_MPOM_PAYMENT_MODE + "',";
                SQL = SQL + " RM_PO_COST_CENTER= '" + oEntity.sCostCenter + "',RM_PO_UOM_TYPE='" + oEntity.sPOUOMType + "',RM_PO_RM_TYPE='" + oEntity.sLPOReceiptType + "',RM_PO_CURR_TYPE='" + oEntity.SPOCurrType + "',";
                SQL = SQL + "AD_BR_CODE='" + oEntity.Branch + "' , RM_VM_CREDIT_TERMS ='" + oEntity.PayTerms + "', RM_PO_QTY_RATE_TYPE ='" + oEntity.QtyType + "', ";
                SQL = SQL + " HR_DEPT_DEPT_CODE='" + oEntity.Dept + "', ";
                SQL = SQL + " RM_PO_AGG_START_DATE='" + oEntity.AggStartDate.ToString("dd-MMM-yyyy") + "',";

                if (oEntity.AggrementEndDateApplicableYN == "Y")
                {
                    SQL = SQL + "   RM_PO_AGG_END_DATE = '" + oEntity.AggEndDate.ToString("dd-MMM-yyyy") + "',";
                }
                else
                {
                    SQL = SQL + "   RM_PO_AGG_END_DATE ='',";
                }

                SQL = SQL + " RM_PO_AGG_END_DATE_REQUIRED_YN='" + oEntity.AggrementEndDateApplicableYN + "',";

                SQL = SQL + "  RM_PO_APPROVEDBY_DATE=TO_DATE( '" + oEntity.ApprovedTime + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + "  RM_PO_VERIFIEDBY_DATE=TO_DATE( '" + oEntity.VerifiedTime + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " RM_PO_VERIFIED_MNGR='" + oEntity.ManagerVerified + "',RM_PO_VERIFIEDBY_MNGR='" + oEntity.ManagerVerifiedBy + "',";
                SQL = SQL + " RM_PO_VERIFIED_MNGR_DATE =TO_DATE( '" + oEntity.ManagerVerifiedTime + "','DD-MM-YYYY HH:MI:SS AM') ";

                SQL = SQL + " WHERE RM_PO_PONO = '" + oEntity.RM_PO_PONO + "' AND AD_FIN_FINYRID = " + Convert.ToInt32(EntryFinYearId) + "";

                sSQLArray.Add(SQL);


                SQL = " DELETE FROM RM_PO_PROJECT_DETAILS ";
                SQL = SQL + " WHERE RM_PO_PONO = '" + oEntity.RM_PO_PONO + "' AND AD_FIN_FINYRID = " + Convert.ToInt32(EntryFinYearId) + "";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_PO_NOTES ";
                SQL = SQL + " WHERE RM_PO_PONO = '" + oEntity.RM_PO_PONO + "' AND AD_FIN_FINYRID = " + Convert.ToInt32(EntryFinYearId) + "";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;
                sRetun = InsertPOProjectDetails(oEntity.RM_PO_PONO, Convert.ToInt32(EntryFinYearId), EntityLPO, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }


                ProjCount = PoProjectDetCount(oEntity.RM_PO_PONO, Convert.ToInt32(EntryFinYearId));

                if (ProjCount > 0)// for old data before project costing updation/ else details data will delete during updation
                {
                    SQL = " DELETE FROM RM_PO_DETAILS ";
                    SQL = SQL + " WHERE RM_PO_PONO = '" + oEntity.RM_PO_PONO + "' AND AD_FIN_FINYRID = " + Convert.ToInt32(EntryFinYearId) + "";
                    sSQLArray.Add(SQL);

                    sRetun = string.Empty;

                    sRetun = InsertPODetails(oEntity.RM_PO_PONO, Convert.ToInt32(EntryFinYearId), EntityLPO, mngrclassobj);

                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }




                sRetun = string.Empty;

                sRetun = InsertTermsAndConditions(oEntity.RM_PO_PONO, objFPSNoteEntity, mngrclassobj, Convert.ToInt32(EntryFinYearId));

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                if (oEntity.RM_PO_APPROVED == "Y")
                {
                    sRetun = InsertApprovalQrs(oEntity.RM_PO_PONO, Convert.ToInt32(EntryFinYearId), oEntity.RM_PO_PO_DATE.ToString("dd-MMM-yyyy"), oEntity.RM_PO_APPROVEDBY);

                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }
                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, oEntity.AD_FIN_FINYRID, mngrclass.CompanyCode, DateTime.Now, "RTMO", oEntity.RM_PO_PONO, false, Environment.MachineName, "U", sSQLArray);
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

        public string ModifyApprovedDataSQL(PurchaseOrderPRCEntity oEntity, List<PurchaseOrderPRCLPO> EntityLPO, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                // only change the price 

                sRetun = string.Empty;
                sSQLArray.Clear();
                int i;
                foreach (var Data in EntityLPO)
                {
                    if (Data.oItemCodeLPO != null)
                    {

                        SQL = "  update  RM_PO_DETAILS  set RM_POD_UNIT_PRICE =  " + Data.dUnitPirceLPO + ",  RM_POD_NEWPRICE = " + Data.dUnitPirceLPO + "";
                        SQL = SQL + "  where  RM_PO_PONO = '" + oEntity.RM_PO_PONO + "' and AD_FIN_FINYRID  = " + oEntity.AD_FIN_FINYRID + "";
                        SQL = SQL + " and RM_RMM_RM_CODE ='" + Data.oItemCodeLPO + "'  and  SALES_STN_STATION_CODE =  '" + Data.oStcodeLPO + "'";
                        SQL = SQL + "  and RM_SM_SOURCE_CODE = '" + Data.oSoruceCodeLPO + "'";

                        sSQLArray.Add(SQL);

                        // VERY IMPORTANT ORDER QTY AND PENDING QTY IS KEEPING SAME BCZU THIS VALUE USING IN RM PENDING ORDER REPORT / JOMY 28 JAN 20113 
                    }
                }

                sRetun = oTrns.SetTrans(mngrclass.UserName, oEntity.AD_FIN_FINYRID, mngrclass.CompanyCode, DateTime.Now, "RTMO", oEntity.RM_PO_PONO, false, Environment.MachineName, "U", sSQLArray);
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



        public string DeleteSQL(PurchaseOrderPRCEntity oEntity, List<PurchaseOrderPRCLPO> EntityLPO, object mngrclassobj, string EntryFinYearId)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " DELETE FROM RM_PO_MASTER ";
                SQL = SQL + " WHERE RM_PO_PONO = '" + oEntity.RM_PO_PONO + "' AND AD_FIN_FINYRID = " + Convert.ToInt32(EntryFinYearId) + "";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_PO_PROJECT_DETAILS ";
                SQL = SQL + " WHERE RM_PO_PONO = '" + oEntity.RM_PO_PONO + "' AND AD_FIN_FINYRID = " + Convert.ToInt32(EntryFinYearId) + "";

                sSQLArray.Add(SQL);


                SQL = " DELETE FROM RM_PO_DETAILS ";
                SQL = SQL + " WHERE RM_PO_PONO = '" + oEntity.RM_PO_PONO + "' AND AD_FIN_FINYRID = " + Convert.ToInt32(EntryFinYearId) + "";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_PO_NOTES  ";
                SQL = SQL + " WHERE RM_PO_PONO = '" + oEntity.RM_PO_PONO + "' AND AD_FIN_FINYRID = " + Convert.ToInt32(EntryFinYearId) + "";

                sSQLArray.Add(SQL);

                //if (!string.IsNullOrEmpty(sRM_PR_PRNO))
                //{ // as soon as created the Po against system should close the PR

                //    SQL = " UPDATE RM_PR_MASTER";
                //    SQL = SQL + " SET RM_PR_PR_STATUS = 'O'";
                //    SQL = SQL + " WHERE RM_pR_pRno = '" + sRM_PR_PRNO + "' AND ad_fin_finyrid = " + PrFinYear  + "";
                //    sSQLArray.Add(SQL);
                //}



                sRetun = ModifyPurReqStatus("O", oEntity.RM_PR_PRNO, int.Parse(oEntity.RM_PR_FIN_FINYRID));
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }


                sRetun = string.Empty;
                sRetun = ModifiyPurQuotation("D", oEntity, EntityLPO, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }


                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMO", oEntity.RM_PO_PONO, false, Environment.MachineName, "D", sSQLArray);
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


        public string UnApproveData(string PurChaseOrderNo, int ifinYear, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " DELETE FROM  RM_PO_TRIGGER  ";
                SQL = SQL + " WHERE   RM_PO_PONO = '" + PurChaseOrderNo + "'  AND  AD_FIN_FINYRID = " + ifinYear + "";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, ifinYear, mngrclass.CompanyCode, DateTime.Now, "RTMO", PurChaseOrderNo, false, Environment.MachineName, "U", sSQLArray);

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

        #region " UUpdated remarks and status / OPen  CVlose  " 


        public string ModifyRemarksSQL(PurchaseOrderPRCEntity oEntity, object mngrclassobj)
        {
            string sReturn = string.Empty;
            try
            {
                OracleHelper oTrans = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                sSQLArray.Clear();

                SQL = " UPDATE    RM_PO_MASTER SET ";

                SQL = SQL + " RM_PO_PO_DATE  ='" + oEntity.RM_PO_PO_DATE.ToString("dd-MMM-yyyy") + "' ,RM_VM_VENDOR_CODE ='" + oEntity.RM_VM_VENDOR_CODE + "' , AD_CUR_CURRENCY_CODE ='" + oEntity.AD_CUR_CURRENCY_CODE + "',";

                SQL = SQL + " RM_PO_ENTRY_TYPE ='" + oEntity.RM_PO_ENTRY_TYPE + "',RM_PO_EXP_DATE ='" + oEntity.RM_PO_EXP_DATE.ToString("dd-MMM-yyyy") + "'  ,RM_PO_EXP_DATE_TO = '" + oEntity.RM_PO_EXP_DATE_TO.ToString("dd-MMM-yyyy") + "' ,";
                SQL = SQL + "   RM_PO_REMARKS ='" + oEntity.RM_PO_REMARKS + "' ,";

                SQL = SQL + " RM_PO_CONTAC1_CODE= '" + oEntity.RM_PO_CONTAC1_CODE + "'  ,RM_PO_CONTAC2_CODE='" + oEntity.RM_PO_CONTAC2_CODE + "' ,";
                SQL = SQL + " RM_PO_SUPP_REF ='" + oEntity.RM_PO_SUPP_REF + "'  ,";

                SQL = SQL + "RM_PO_SUPP_REF_DATE='" + oEntity.RM_PO_SUPP_REF_DATE.ToString("dd-MMM-yyyy") + "' ,RM_PO_OUR_REF='" + oEntity.RM_PO_OUR_REF + "',";
                SQL = SQL + "RM_PO_UPDATEDDATE= TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";

                SQL = SQL + "  RM_PO_POSTEDREMARKS= '" + oEntity.RM_PO_POSTEDREMARKS + "',";

                SQL = SQL + " RM_MPOM_DELIVERY_TIME_RMKS= '" + oEntity.RM_MPOM_DELIVERY_TIME_RMKS + "' ,RM_MPOM_DELIVERY_PLACE_RMKS= '" + oEntity.RM_MPOM_DELIVERY_PLACE_RMKS + "' , ";
                SQL = SQL + "RM_MPOM_VALIDITY= '" + oEntity.RM_MPOM_VALIDITY + "',RM_MPOM_PAYMENT_TERMS='" + oEntity.RM_MPOM_PAYMENT_TERMS + "',RM_MPOM_PAYMENT_MODE= '" + oEntity.RM_MPOM_PAYMENT_MODE + "'";


                SQL = SQL + " WHERE   RM_PO_PONO = '" + oEntity.RM_PO_PONO + "'  AND  AD_FIN_FINYRID = " + oEntity.AD_FIN_FINYRID + "";

                sSQLArray.Add(SQL);

                sReturn = oTrans.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMO", oEntity.RM_PO_PONO, false, Environment.MachineName, "U", sSQLArray);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;

        }

        public string ButtonOpen(string txtPONo, string AggrementEndDateApplicableYN, DateTime dtAgreementEndDate, int Finyr, object mngrclassobj, string EntryType)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrans = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                sSQLArray.Clear();

                SQL = " UPDATE RM_PO_MASTER";
                SQL = SQL + " SET RM_PO_PO_STATUS = 'C' , ";
                if (EntryType == "REVISION")
                {
                    SQL = SQL + "  RM_PO_LAST_REVISED_YN = 'N',";
                }
                SQL = SQL + " RM_PO_AGG_END_DATE_REQUIRED_YN ='" + AggrementEndDateApplicableYN + "', RM_PO_AGG_END_DATE ='" + dtAgreementEndDate.ToString("dd-MMM-yyyy") + "',";
                SQL = SQL + " RM_PO_OPENCLOSED_BY = '" + mngrclass.UserName + "' ,RM_PO_OPENCLOSED_DATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy hh:mm:ss tt") + "' ,'DD-MM-YYYY HH:MI:SS AM') ";

                SQL = SQL + " WHERE RM_PO_PONO = '" + txtPONo + "' AND AD_FIN_FINYRID = " + Finyr;

                sSQLArray.Add(SQL);


                SQL = " UPDATE RM_PO_DETAILS";
                SQL = SQL + " SET RM_POD_STATUS = 'C'";
                SQL = SQL + " WHERE RM_PO_PONO = '" + txtPONo + "' AND AD_FIN_FINYRID = " + Finyr;

                sSQLArray.Add(SQL);




                sRetun = oTrans.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMO", txtPONo, false, Environment.MachineName, "U", sSQLArray);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
            }
            finally
            {
            }
            return sRetun;

        }

        public string ButtonClose(string txtPONo, DateTime dtAgreementEndDate, int Finyr, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrans = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                sSQLArray.Clear();

                SQL = " UPDATE RM_PO_MASTER";
                SQL = SQL + " SET RM_PO_PO_STATUS = 'O',RM_PO_AGG_END_DATE ='" + dtAgreementEndDate.ToString("dd-MMM-yyyy") + "',";
                SQL = SQL + " RM_PO_OPENCLOSED_BY = '" + mngrclass.UserName + "' ,RM_PO_OPENCLOSED_DATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy hh:mm:ss tt") + "' ,'DD-MM-YYYY HH:MI:SS AM') ";
                SQL = SQL + " WHERE RM_PO_PONO = '" + txtPONo + "' AND AD_FIN_FINYRID = " + Finyr;

                sSQLArray.Add(SQL);


                SQL = " UPDATE RM_PO_DETAILS";
                SQL = SQL + " SET RM_POD_STATUS = 'O'";
                SQL = SQL + " WHERE RM_PO_PONO = '" + txtPONo + "' AND ad_fin_finyrid = " + Finyr;

                sSQLArray.Add(SQL);



                sRetun = oTrans.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMO", txtPONo, false, Environment.MachineName, "U", sSQLArray);
            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
            }
            finally
            {
            }
            return sRetun;
        }

        public string btnClosePurchaseRequest(string txtPRNo, string txtPRFinYrId, object mngrclassobj)
        {
            string sReturn = string.Empty;

            try
            {
                OracleHelper oTrans = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;



                sSQLArray.Clear();

                SQL = " UPDATE RM_PR_MASTER";
                SQL = SQL + " SET RM_PR_PR_STATUS = 'C'";
                SQL = SQL + " WHERE RM_pR_pRno = '" + txtPRNo + "' AND ad_fin_finyrid = " + txtPRFinYrId + "";
                sSQLArray.Add(SQL);

                sReturn = oTrans.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRQ", txtPRNo, false, Environment.MachineName, "U", sSQLArray);


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return sReturn;
        }

        public string btnCancel(string txtCancelRemarks, string txtPONo, int Finyr, object mngrclassobj,string  EntryType)
        {
            string sReturn = string.Empty;

            try
            {
                OracleHelper oTrans = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " UPDATE RM_PO_MASTER";
                SQL = SQL + " SET RM_po_po_status = 'N', RM_po_cancel_remarks='" + txtCancelRemarks + "',";
                if(EntryType== "REVISION")
                {
                    SQL = SQL + "  RM_PO_LAST_REVISED_YN='Y', ";
                }

                SQL = SQL + " RM_PO_CANCELLED_BY = '" + mngrclass.UserName + "' ,RM_PO_CANCELLED_DATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy hh:mm:ss tt") + "' ,'DD-MM-YYYY HH:MI:SS AM') ";
                SQL = SQL + " WHERE RM_po_pono = '" + txtPONo + "' AND ad_fin_finyrid = " + Finyr;

                sSQLArray.Add(SQL);

                SQL = " UPDATE RM_PO_DETAILS";
                SQL = SQL + " SET RM_POD_STATUS = 'C'";
                SQL = SQL + " WHERE RM_PO_PONO = '" + txtPONo + "' AND ad_fin_finyrid = " + Finyr;

                sSQLArray.Add(SQL);

                sReturn = oTrans.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMO", txtPONo, false, Environment.MachineName, "U", sSQLArray);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return sReturn;

        }
        #endregion

        #region " other functions- validation" 

        public bool fnChkMonthClosing(string dtTransDate, int iTransFinYr, string ModuleId)
        {
            bool functionReturnValue = false;
            try
            {
                string SQL = null;
                SqlHelper clsSQLHelper = new SqlHelper();
                DataTable dsQuery = new DataTable();

                SQL = " SELECT ad_mc_month, ad_mc_createdby, ad_mc_createddate";
                SQL = SQL + " FROM ad_month_closing";
                SQL = SQL + " WHERE AD_MD_MODULEID ='" + ModuleId + "'";
                SQL = SQL + " AND ad_fin_finyrid = " + iTransFinYr;

                dsQuery = clsSQLHelper.GetDataTableByCommand(SQL);

                if ((dsQuery == null) == true)
                {

                    functionReturnValue = false;
                    return functionReturnValue;
                }
                if (dsQuery.Rows.Count > 0)
                {
                    functionReturnValue = true;
                    return functionReturnValue;
                }
                functionReturnValue = true;
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                functionReturnValue = false;
            }
            return functionReturnValue;
            //throw new NotImplementedException();
        }

        public DataTable HideApprovlControl(string userId, string sItemId)
        {

            string SQL = null;

            DataTable dtGrade = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "";
                SQL = " SELECT count(*) cnt";
                SQL = SQL + " FROM ad_mail_approval_users";
                SQL = SQL + " WHERE ";
                SQL = SQL + " ad_mail_approval_users.ad_um_userid = '" + userId + "'";
                SQL = SQL + " AND ad_mail_approval_users.AD_AF_ID = '" + sItemId + "'";



                dtGrade = clsSQLHelper.GetDataTableByCommand(SQL);



            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return null;
            }
            return dtGrade;
        }

        public DataSet fnDeleteValidation(string txtPONo, int Finyr, object mngrclassobj)
        {
            DataSet dsPoStatus = new DataSet();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                sSQLArray.Clear();

                SQL = " SELECT RM_PO_PO_STATUS";
                SQL = SQL + " FROM RM_PO_MASTER";
                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_PO_PONO='" + txtPONo + "'AND AD_FIN_FINYRID=" + Finyr + "";

                dsPoStatus = clsSQLHelper.GetDataset(SQL);
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

        public DataSet ValidateApprovedAmount(string ProjectCode, string BudgetItem_Code, object mngrclassobj, string InsertionType, string ponO)
        {

            DataSet dtType = new DataSet();
            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                if (InsertionType == "I")
                {

                    SQL = " SELECT NVL(SUM(RM_POD_TOTALAMT),0) TOTAL_PO_AMOUNT ";
                    SQL = SQL + " FROM  ";
                    SQL = SQL + "   RM_PO_MASTER,rm_po_project_details";
                    SQL = SQL + " WHERE  ";
                    SQL = SQL + "   RM_PO_MASTER.RM_PO_PONO=rm_po_project_details.RM_PO_PONO ";
                    SQL = SQL + "   AND rm_po_project_details.PC_BUD_BUDGET_ITEM_CODE='" + BudgetItem_Code + "'";
                    SQL = SQL + "   AND rm_po_project_details.GL_COSM_ACCOUNT_CODE='" + ProjectCode + "'";
                    SQL = SQL + "   AND RM_PO_PO_STATUS<>'N' ";
                }

                else if (InsertionType == "U")
                {
                    SQL = " SELECT NVL(SUM(RM_POD_TOTALAMT),0) TOTAL_PO_AMOUNT ";
                    SQL = SQL + " FROM  ";
                    SQL = SQL + "   RM_PO_MASTER,rm_po_project_details";
                    SQL = SQL + " WHERE  ";
                    SQL = SQL + "   RM_PO_MASTER.RM_PO_PONO=rm_po_project_details.RM_PO_PONO ";
                    SQL = SQL + "   AND rm_po_project_details.PC_BUD_BUDGET_ITEM_CODE='" + BudgetItem_Code + "'";
                    SQL = SQL + "   AND rm_po_project_details.GL_COSM_ACCOUNT_CODE='" + ProjectCode + "'";
                    SQL = SQL + "   and RM_PO_PO_STATUS<>'N' ";
                    SQL = SQL + "   and RM_PO_MASTER.RM_PO_PONO <> '" + ponO + "' ";
                }

                dtType = clsSQLHelper.GetDataset(SQL);

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

        public DataSet TotalBudgetAmount(string ProjectCode, string BudgetItem_Code, object mngrclassobj)
        {

            DataSet dtType = new DataSet();
            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT PC_BUD_BUDGET_DETAILS.SALES_PM_PROJECT_CODE,PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_ITEM_CODE,";
                SQL = SQL + " NVL(SUM(PC_BUD_BUDGET_AMOUNT),0) TOTAL_BUD_AMOUNT ";
                SQL = SQL + " FROM  ";
                SQL = SQL + "   PC_BUD_BUDGET_MASTER,PC_BUD_BUDGET_DETAILS ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + "   PC_BUD_BUDGET_MASTER.PC_BUD_BUDGET_CODE=PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_CODE ";
                SQL = SQL + "   AND PC_BUD_BUDGET_MASTER.PC_BUD_BUDGET_LAST_YN='Y' ";
                SQL = SQL + "   AND PC_BUD_BUDGET_MASTER.PC_BUD_BUDGET_APPROVED='Y' ";
                SQL = SQL + "   AND PC_BUD_BUDGET_MASTER.PC_BUD_BUDGET_STATUS ='ACTIVE' ";
                SQL = SQL + "   AND PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_ITEM_CODE='" + BudgetItem_Code + "'";
                SQL = SQL + "   AND PC_BUD_BUDGET_DETAILS.SALES_PM_PROJECT_CODE='" + ProjectCode + "'";

                SQL = SQL + "    group by PC_BUD_BUDGET_DETAILS.SALES_PM_PROJECT_CODE,PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_ITEM_CODE";

                dtType = clsSQLHelper.GetDataset(SQL);

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

        public double PoProjectDetCount(string txtPONo, int finID)
        {
            DataSet dsPoStatus = new DataSet();
            double PoCount = 0;
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                // SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = " select count(RM_PO_PONO) cnt FROM RM_PO_PROJECT_DETAILS ";
                SQL = SQL + " WHERE RM_PO_PONO = '" + txtPONo + "' AND AD_FIN_FINYRID = " + finID + " ";

                dsPoStatus = clsSQLHelper.GetDataset(SQL);

                if (dsPoStatus.Tables[0].Rows.Count > 0)
                {
                    PoCount = Convert.ToDouble(dsPoStatus.Tables[0].Rows[0]["cnt"].ToString());
                }

                else
                {
                    PoCount = 0;
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
            return PoCount;

        }

        #endregion

        #region "FEtch Data - fill form view mode " 

        public DataTable FetchData(string pono, string id, string UserName)
        {
            DataTable dt = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_PO_PONO, AD_FIN_FINYRID, RM_PO_PO_DATE,";
                SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE,RM_PO_LAST_REVISED_YN,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIERNAME, RM_PO_PRICETYPE, RM_PO_EXP_DATE,";
                SQL = SQL + " RM_PO_ENTRY_TYPE,RM_PO_EXP_DATE_TO,";

                SQL = SQL + " RM_PO_GRAND_TOTAL_FC, RM_PO_DISCOUNT_PERC_FC, RM_PO_DISCOUNT_AMOUNT_FC,";
                SQL = SQL + "RM_PO_NET_AMOUNT_FC,RM_PO_VAT_AMOUNT_FC ,RM_PO_VAT_PERCENTAGE_FC,";

                SQL = SQL + " RM_PO_COST_CENTER COST_CENTER_CODE,GL_COSTING_MASTER_VW.NAME COST_CENTER_NAME,";
                SQL = SQL + "  RM_PO_PO_STATUS, RM_PO_REMARKS, RM_PO_MASTER.AD_CUR_CURRENCY_CODE,";
                SQL = SQL + " RM_PO_EXCHANGE_RATE, RM_PO_APPROVED, RM_PO_APPROVEDBY, RM_PO_PURENTRY,";
                SQL = SQL + " RM_PO_POTYPE, RM_PO_CANCEL_REMARKS, RM_PO_CONTAC1_CODE, C1.HR_EMP_EMPLOYEE_NAME C1NAME,";
                SQL = SQL + " RM_PO_CONTAC2_CODE, C2.HR_EMP_EMPLOYEE_NAME C2NAME , ";

                SQL = SQL + " RM_PR_PRNO, RM_PR_FIN_FINYRID, RM_PO_SUPP_REF,";
                SQL = SQL + " RM_PO_SUPP_REF_DATE, RM_PO_OUR_REF , ";

                SQL = SQL + " RM_PO_CREATEDBY, RM_PO_VERIFIED, RM_PO_VERIFIEDBY, RM_PO_POSTEDREMARKS,RM_PO_CREATEDDATE, ";


                SQL = SQL + " RM_MPOM_DELIVERY_TIME_RMKS, RM_MPOM_DELIVERY_PLACE_RMKS,RM_PO_UOM_TYPE, ";
                SQL = SQL + " RM_MPOM_VALIDITY, RM_MPOM_PAYMENT_TERMS, RM_MPOM_PAYMENT_MODE,RM_PO_COST_CENTER,RM_PO_RM_TYPE,RM_PO_CURR_TYPE,";

                SQL = SQL + " RM_PO_MASTER.AD_BR_CODE ,AD_BRANCH_MASTER.AD_BR_NAME,RM_PO_MASTER.RM_VM_CREDIT_TERMS,RM_PO_QTY_RATE_TYPE, ";
                SQL = SQL + "  RM_PO_AGG_START_DATE,RM_PO_AGG_END_DATE,RM_PO_MASTER.HR_DEPT_DEPT_CODE ,  ";

                SQL = SQL + "  RM_PO_AGG_END_DATE_REQUIRED_YN ,  RM_PO_APPROVEDBY_DATE,RM_PO_APPROVED,";
                SQL = SQL + "  RM_PO_VERIFIEDBY_DATE,RM_PO_VERIFIED_MNGR,RM_PO_VERIFIEDBY_MNGR,RM_PO_VERIFIED_MNGR_DATE,RM_PO_STOCK_TYPE_CODE,RM_PR_PRNO_CONCATENATE_NO,RM_PO_RATE_REVISION_APP_YN ";

                SQL = SQL + " FROM RM_PO_MASTER, RM_VENDOR_MASTER, HR_EMPLOYEE_MASTER C1 , ";
                SQL = SQL + " HR_EMPLOYEE_MASTER C2 ,GL_COSTING_MASTER_VW , AD_BRANCH_MASTER ";
                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " AND RM_PO_MASTER.RM_PO_CONTAC1_CODE=C1.HR_EMP_EMPLOYEE_CODE (+)";
                SQL = SQL + " and RM_PO_MASTER.AD_BR_CODE=AD_BRANCH_MASTER.AD_BR_CODE(+) ";
                SQL = SQL + " AND RM_PO_MASTER.RM_PO_CONTAC2_CODE=C2.HR_EMP_EMPLOYEE_CODE (+)";
                SQL = SQL + " AND RM_PO_MASTER.RM_PO_COST_CENTER=GL_COSTING_MASTER_VW.CODE(+)";
                if (UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                    SQL = SQL + "  and     RM_PO_MASTER.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                }
                SQL = SQL + " AND RM_PO_MASTER.RM_PO_PONO = '" + pono + "' AND RM_PO_MASTER.AD_FIN_FINYRID =" + id + "";

                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
            }
            return dt;
        }


        public DataSet fillRFQDetails(string PurChaseOrderNo)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;


                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = " SELECT distinct RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_PRFQ_DOC_NO   ,RM_PRFQ_DOC_DATE,RM_PUR_RFQ_MASTER.AD_FIN_FINYRID  ";
                SQL = SQL + "  FROM RM_PUR_RFQ_DETAILS_SUPP_ORDER ,RM_PUR_RFQ_MASTER  ";
                SQL = SQL + "  where    ";
                SQL = SQL + "   RM_PUR_RFQ_DETAILS_SUPP_ORDER.RM_PRFQ_DOC_NO=RM_PUR_RFQ_MASTER.RM_PRFQ_DOC_NO   ";
                SQL = SQL + " and     ";
                SQL = SQL + "   RM_PO_PONO='" + PurChaseOrderNo + "'   ";



                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        public DataSet FETCHNOTESSaved(string PoNo)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  ";
                SQL = SQL + "RM_PO_PONO, AD_FIN_FINYRID, RM_PO_SL_NO,  ";
                SQL = SQL + "   RM_PO_NOTES, RM_PO_PRINT_YN, RM_PO_DELSTATUS from RM_PO_NOTES ";
                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_PO_PONO ='" + PoNo + "' order by RM_PO_SL_NO";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }



        public DataTable FetchDataFpoSpread(string pono, string id)
        {
            DataTable dt = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_PO_DETAILS. RM_RMM_RM_CODE ITEM_CODE,";
                SQL = SQL + " RM_PO_DETAILS.SALES_STN_STATION_CODE,";

                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + " RM_PO_DETAILS.HR_DEPT_DEPT_CODE,";
                SQL = SQL + " HR_DEPT_MASTER.HR_DEPT_DEPT_DESC,";

                SQL = SQL + " RM_PO_DETAILS.RM_SM_SOURCE_CODE , ";
                SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC , ";

                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION ITEM_DESC , RM_IM_ITEM_DTL_DESCRIPTION DTLDESC,";
                SQL = SQL + " RM_PO_DETAILS.RM_UOM_UOM_CODE UOM_CODE,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC, RM_POD_QTY QTY,";

                SQL = SQL + " RM_POD_UNIT_PRICE_FC UNIT_PRICE,RM_POD_NEWPRICE_FC NEW_PRICE, RM_POD_DISCOUNT_PERCENT_FC DISC_PERC,";
                SQL = SQL + " RM_POD_DISCOUNT_AMOUNT_FC DISC_AMT, RM_POD_TOTALAMT_FC TOT_AMT,";
                SQL = SQL + "RM_POD_RM_AMOUNT_BFR_VAT_FC ,RM_POD_RM_VAT_RATE_FC,RM_POD_RM_VAT_AMOUNT_FC,";


                SQL = SQL + " RM_POD_SL_NO SL_NO, RM_PO_DETAILS.PC_BUD_BUDGET_ITEM_CODE,";
                SQL = SQL + " 0 BUDGET_AMOUNT,";
                SQL = SQL + " RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE ,RM_POD_RM_VAT_TYPE ";

                SQL = SQL + " FROM RM_PO_DETAILS, RM_RAWMATERIAL_MASTER, RM_UOM_MASTER,";
                SQL = SQL + " SL_STATION_MASTER,";
                SQL = SQL + " HR_DEPT_MASTER , RM_SOURCE_MASTER";//,PC_BUD_BUDGET_DETAILS,PC_BUD_BUDGET_MASTER 

                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_PO_PONO='" + pono + "' AND AD_FIN_FINYRID=" + id + "";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE (+)";
                SQL = SQL + " AND RM_PO_DETAILS.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE (+)";
                SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE =SL_STATION_MASTER.SALES_STN_STATION_CODE(+)";
                SQL = SQL + " AND RM_PO_DETAILS.HR_DEPT_DEPT_CODE =HR_DEPT_MASTER.HR_DEPT_DEPT_CODE (+)";
                SQL = SQL + " AND RM_PO_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE (+)";
                //SQL = SQL + " AND RM_PO_DETAILS.PC_BUD_BUDGET_ITEM_CODE=PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_ITEM_CODE(+)";

                SQL = SQL + " ORDER BY RM_POD_SL_NO";

                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
            }
            return dt;
        }
        public DataTable FetchDataProjectFpoSpread(string pono, string id)
        {
            DataTable dt = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_PO_PROJECT_DETAILS. RM_RMM_RM_CODE ITEM_CODE,";
                SQL = SQL + " RM_PO_PROJECT_DETAILS.SALES_STN_STATION_CODE,";

                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + " RM_PO_PROJECT_DETAILS.HR_DEPT_DEPT_CODE,";
                SQL = SQL + " HR_DEPT_MASTER.HR_DEPT_DEPT_DESC,";

                SQL = SQL + " RM_PO_PROJECT_DETAILS.RM_SM_SOURCE_CODE , ";
                SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC , ";

                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION ITEM_DESC , RM_IM_ITEM_DTL_DESCRIPTION DTLDESC,";
                SQL = SQL + " RM_PO_PROJECT_DETAILS.RM_UOM_UOM_CODE UOM_CODE,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC, RM_POD_QTY QTY,";


                SQL = SQL + " RM_POD_UNIT_PRICE_FC UNIT_PRICE, RM_POD_DISCOUNT_PERCENT_FC DISC_PERC,";
                SQL = SQL + " RM_POD_DISCOUNT_AMOUNT_FC DISC_AMT, RM_POD_TOTALAMT_FC TOT_AMT,RM_POD_RM_AMOUNT_BFR_VAT_FC,";
                SQL = SQL + "RM_POD_RM_VAT_RATE_FC,RM_POD_RM_VAT_AMOUNT_FC, ";

                SQL = SQL + " RM_POD_SL_NO SL_NO, RM_POD_NEWPRICE NEW_PRICE,RM_PO_PROJECT_DETAILS.PC_BUD_BUDGET_ITEM_CODE,";
                SQL = SQL + " 0 BUDGET_AMOUNT,";
                SQL = SQL + " RM_PO_PROJECT_DETAILS.GL_COSM_ACCOUNT_CODE,GL_COSTING_MASTER_VW.Name CostName ,GL_COSM_BUDGET_VAL_YN,";
                SQL = SQL + " RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE,RM_POD_RM_VAT_TYPE ,RM_PRFQ_DOC_NO,RM_PRFQ_COMBINED_DOC_NO,RM_PRFQD_SLNO  ";

                SQL = SQL + " FROM RM_PO_PROJECT_DETAILS, RM_RAWMATERIAL_MASTER, RM_UOM_MASTER,";
                SQL = SQL + " SL_STATION_MASTER,";
                SQL = SQL + " HR_DEPT_MASTER , RM_SOURCE_MASTER,GL_COSTING_MASTER_VW";

                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_PO_PONO='" + pono + "' AND AD_FIN_FINYRID=" + id + "";
                SQL = SQL + " AND RM_PO_PROJECT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE (+)";
                SQL = SQL + " AND RM_PO_PROJECT_DETAILS.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE (+)";
                SQL = SQL + " AND RM_PO_PROJECT_DETAILS.SALES_STN_STATION_CODE =SL_STATION_MASTER.SALES_STN_STATION_CODE(+)";
                SQL = SQL + " AND RM_PO_PROJECT_DETAILS.HR_DEPT_DEPT_CODE =HR_DEPT_MASTER.HR_DEPT_DEPT_CODE (+)";
                SQL = SQL + " AND RM_PO_PROJECT_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE (+)";
                //  SQL = SQL + " AND RM_PO_PROJECT_DETAILS.PC_BUD_BUDGET_ITEM_CODE=PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_ITEM_CODE(+)";
                //SQL = SQL + " AND rm_po_project_details.GL_COSM_ACCOUNT_CODE= pc_bud_budget_details.SALES_PM_PROJECT_CODE(+)";
                SQL = SQL + " AND RM_PO_PROJECT_DETAILS.GL_COSM_ACCOUNT_CODE=GL_COSTING_MASTER_VW.CODE(+)";
                // SQL = SQL + " AND  pc_bud_budget_details.PC_BUD_BUDGET_CODE=PC_BUD_BUDGET_MASTER.PC_BUD_BUDGET_CODE ";


                SQL = SQL + " ORDER BY RM_POD_SL_NO";

                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
            }
            return dt;
        }

        public string AlreadyApprovedOrNot(string sPoNo, int iFinYear)
        {
            DataSet dsCnt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the  opening 

                SQL = " select    count(RM_PO_PONO) CNT  from RM_PO_TRIGGER ";
                SQL = SQL + "where  ";
                SQL = SQL + "RM_PO_PONO   ='" + sPoNo + "' And  AD_FIN_FINYRID  = " + iFinYear + "";


                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "Already been changed the approved status. Kindly unapprove or query and check the status.";
                }



            }
            catch (Exception ex)
            {
                return System.Convert.ToString(ex.Message);
            }
            return "CONTINUE";
        }


        #endregion

        #region "Feth purchase Requesition Data " 


        public DataSet FetchPurchaseRequestData(string sPurchaseRequestNo, string sPurchaseRequestFinYearId)
        {
            DataSet dsRetData = new DataSet();
            DataTable dtMaster = new DataTable();
            DataTable dtDetails = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT RM_PR_PRNO PRNO, AD_FIN_FINYRID PRFINYEAR,  ";
                SQL = SQL + "  RM_PR_MASTER.RM_VM_VENDOR_CODE SUPPCODE,";
                SQL = SQL + "  RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPNAME,";
                SQL = SQL + "  RM_PR_MASTER.RM_PR_PR_DATE PRDATE,RM_PR_COST_CENTER,";
                SQL = SQL + "  RM_PR_EXP_DATE EXP_FROM_DATE ,RM_PR_EXP_DATE_TO EXP_TO_DATE,SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_DESC ";

                SQL = SQL + "  FROM RM_VENDOR_MASTER,RM_PR_MASTER,SL_PAY_TYPE_MASTER";
                SQL = SQL + "  WHERE  RM_PR_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE (+)";
                SQL = SQL + "  AND  RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS =SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE (+)";
                SQL = SQL + "  AND RM_PR_MASTER.RM_PR_PR_STATUS='O' ";
                SQL = SQL + "  AND RM_PR_MASTER.RM_PR_APPROVED ='Y' ";
                SQL = SQL + "  AND RM_PR_MASTER.RM_PR_PRNO = '" + sPurchaseRequestNo + "' AND RM_PR_MASTER.AD_FIN_FINYRID =" + sPurchaseRequestFinYearId + "";

                dtMaster = clsSQLHelper.GetDataTableByCommand(SQL);

                dtMaster.TableName = "RM_PR_MASTER";
                dsRetData.Tables.Add(dtMaster);


                SQL = " SELECT   RM_PR_DETAILS.  RM_RMM_RM_CODE ITEM_CODE,";
                SQL = SQL + "  RM_PR_DETAILS.SALES_STN_STATION_CODE,";

                SQL = SQL + "  SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + "  RM_PR_DETAILS.HR_DEPT_DEPT_CODE,";
                SQL = SQL + "  HR_DEPT_MASTER.HR_DEPT_DEPT_DESC,";

                SQL = SQL + "    RM_PR_DETAILS.RM_SM_SOURCE_CODE , ";
                SQL = SQL + "    RM_SOURCE_MASTER.RM_SM_SOURCE_DESC  , ";

                SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION  ITEM_DESC , RM_IM_ITEM_DTL_DESCRIPTION DTLDESC,";
                SQL = SQL + "  RM_PR_DETAILS.RM_UOM_UOM_CODE UOM_CODE,";
                SQL = SQL + "  RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC, RM_PRD_QTY QTY, 0 UNIT_PRICE, 0 TOT_AMT, ";

                SQL = SQL + "  RM_PRD_SL_NO SL_NO ";
                SQL = SQL + "  FROM RM_PR_DETAILS, RM_RAWMATERIAL_MASTER, RM_UOM_MASTER,";
                SQL = SQL + "  SL_STATION_MASTER,";
                SQL = SQL + "  HR_DEPT_MASTER , RM_SOURCE_MASTER ";

                SQL = SQL + "  WHERE   ";
                SQL = SQL + "  RM_PR_DETAILS.RM_PR_PRNO=  '" + sPurchaseRequestNo + "'  AND RM_PR_DETAILS.AD_FIN_FINYRID =" + sPurchaseRequestFinYearId + "";
                SQL = SQL + "  AND RM_PR_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE (+)";
                SQL = SQL + "  AND RM_PR_DETAILS.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  (+)";
                SQL = SQL + "  AND RM_PR_DETAILS.SALES_STN_STATION_CODE =SL_STATION_MASTER.SALES_STN_STATION_CODE(+)";
                SQL = SQL + "  AND RM_PR_DETAILS.HR_DEPT_DEPT_CODE =HR_DEPT_MASTER.HR_DEPT_DEPT_CODE (+)";
                SQL = SQL + "  AND RM_PR_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE (+)";

                SQL = SQL + "  ORDER BY RM_PRD_SL_NO";

                dtDetails = clsSQLHelper.GetDataTableByCommand(SQL);

                dtDetails.TableName = "RM_PR_DETAILS";
                dsRetData.Tables.Add(dtDetails);


            }
            catch (Exception ex)
            {
                return null;
            }
            return dsRetData;
        }




        #endregion

        #region "GET SET document number " 

        public string GetDocumnentNo(string sFromCode, int iFinYearId)
        {
            DataTable dtCode = new DataTable();
            try
            {


                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT AD_DN_NEXT_NO FROM AD_DOC_NUMBER WHERE  AD_MI_ITEMID='" + sFromCode + "' AND AD_FIN_FINYRID=" + iFinYearId + "";

                dtCode = clsSQLHelper.GetDataTableByCommand(SQL);


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return System.Convert.ToString(dtCode.Rows[0].ItemArray[0]);
        }

        public string ResSetNextDocNumberStation(String FormCode, String CurrentNumber, int iFinYearId)
        {

            int liLength = 0;
            String lsCode = string.Empty;
            int liZeros = 0;
            int liNumber = 0;
            int i = 0;

            String lsResult = string.Empty;

            lsCode = CurrentNumber;
            liLength = lsCode.Length;

            liNumber = int.Parse(lsCode) + 1;
            liZeros = liLength - liNumber.ToString().Length;

            for (i = 1; i <= liZeros; i++)
            {
                lsResult = lsResult + "0";
            }

            lsResult = lsResult + liNumber;

            return "UPDATE  AD_DOC_NUMBER SET AD_DN_NEXT_NO='" + lsResult + "' WHERE AD_MI_ITEMID='" + FormCode + "' AND AD_FIN_FINYRID=" + iFinYearId + "";

        }


        #endregion

        #region "print"

        public DataSet FetchPOPrintData(string txtPONo, int Finyr, object mngrclassobj, string ChkRemoveDuplicateItemYesNo)
        {
            DataSet dsPoStatus = new DataSet("RMPOPRINT");
            DataTable dsPoNotes = new DataTable("PONotes");

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = "  SELECT RM_PO_PONO, AD_FIN_FINYRID, RM_PO_PO_DATE, RM_PR_PRNO, RM_PR_FIN_FINYRID,  ";
                SQL = SQL + " GL_COAM_ACCOUNT_CODE, RM_VM_VENDOR_CODE, RM_VM_VENDOR_NAME, RM_VM_VENDOR_TYPE, RM_VM_POBOX,  ";
                SQL = SQL + " RM_VM_ADDRESS, RM_VM_CITY, RM_VM_TEL_NO, RM_VM_FAXNO, RM_VM_EMAIL,  ";
                SQL = SQL + " RM_VM_CREDIT_TERMS, RM_VM_CREDIT_PERIOD, RM_VM_CREDIT_LIMIT, RM_VM_CONT_PERSON1, RM_VM_CONT_DESIG1,  ";
                SQL = SQL + " RM_VM_CONT_PERSON2, RM_VM_CONT_DESIG2, RM_VM_CONT_PERSON1_MOBNO, RM_VM_CONT_PERSON2_MOBNO, RM_PO_PRICETYPE,  ";
                SQL = SQL + " RM_PO_ENTRY_TYPE, RM_PO_POTYPE, RM_PO_SUPP_REF, RM_PO_SUPP_REF_DATE, RM_PO_OUR_REF,RM_PO_COST_CENTER, RM_PO_UOM_TYPE, ";
                SQL = SQL + " RM_PO_EXP_DATE, RM_PO_EXP_DATE_TO,     ";
                SQL = SQL + "  RM_PO_PO_STATUS, RM_PO_REMARKS, RM_PO_VERIFIEDBY, RM_PO_APPROVED,  ";
                SQL = SQL + " RM_PO_APPROVEDBY, RM_PO_CREATEDBY, RM_PO_CONTAC1_CODE,C1NAME,c1MobNo, RM_PO_CONTAC2_CODE,C2NAME,c2MobNo, RM_PO_CREATEDDATE,  ";
                SQL = SQL + " RM_PO_UPDATEDBY, RM_PO_UPDATEDDATE, RM_PO_DELETESTATUS, RM_MPOM_DELIVERY_TIME_RMKS, RM_MPOM_DELIVERY_PLACE_RMKS,  ";
                SQL = SQL + " RM_MPOM_VALIDITY, RM_MPOM_PAYMENT_TERMS, RM_MPOM_PAYMENT_MODE, ";
                SQL = SQL + " AD_CUR_CURRENCY_CODE,  AD_CUR_CURRENCY_NAME ,AD_CUR_ABBREVIATION,AD_CUR_COIN, RM_PO_CURR_TYPE,RM_PO_RM_TYPE,";
                SQL = SQL + " RM_POD_SL_NO, SALES_STN_STATION_CODE,  ";
                SQL = SQL + " SALES_STN_STATION_NAME, HR_DEPT_DEPT_CODE, HR_DEPT_DEPT_DESC, RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC,  ";
                SQL = SQL + " RM_RMM_RM_CODE,";
                SQL = SQL + "       case when RM_IM_ITEM_DTL_DESCRIPTION is null then  ";
                SQL = SQL + "       RM_RMM_RM_DESCRIPTION ";
                SQL = SQL + "       when RM_RMM_RM_DESCRIPTION is null then RM_IM_ITEM_DTL_DESCRIPTION ";
                SQL = SQL + "       else RM_RMM_RM_DESCRIPTION ||'-'||RM_IM_ITEM_DTL_DESCRIPTION end  as RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "  RM_IM_ITEM_DTL_DESCRIPTION, RM_POD_QTY,   ";


                SQL = SQL + "RM_POD_UNIT_PRICE_FC RM_POD_UNIT_PRICE,RM_POD_DISCOUNT_PERCENT_FC RM_POD_DISCOUNT_PERCENT,RM_POD_DISCOUNT_AMOUNT_FC RM_POD_DISCOUNT_AMOUNT,";
                SQL = SQL + "RM_POD_TOTALAMT_FC RM_POD_TOTALAMT ,RM_POD_RM_AMOUNT_BFR_VAT_FC RM_POD_RM_AMOUNT_BFR_VAT ,RM_POD_RM_VAT_RATE_FC RM_POD_RM_VAT_RATE,    RM_POD_RM_VAT_AMOUNT_FC RM_POD_RM_VAT_AMOUNT, ";

                SQL = SQL + "RM_PO_GRAND_TOTAL_FC RM_PO_GRAND_TOTAL,RM_PO_DISCOUNT_PERC_FC RM_PO_DISCOUNT_PERC,RM_PO_DISCOUNT_AMOUNT_FC RM_PO_DISCOUNT_AMOUNT,RM_PO_VAT_PERCENTAGE_FC RM_PO_VAT_PERCENTAGE,";
                SQL = SQL + "RM_PO_VAT_AMOUNT_FC RM_PO_VAT_AMOUNT,RM_PO_NET_AMOUNT_FC RM_PO_NET_AMOUNT,";


                SQL = SQL + " RM_UOM_UOM_CODE, RM_UM_UOM_DESC,   ";
                SQL = SQL + "        RM_POD_RM_VAT_TYPE,    GL_TAX_TYPE_NAME, ";
                SQL = SQL + " RM_POD_TOLL_RATE,rm_uom_toll_code, rm_uom_toll_name,";
                SQL = SQL + "  RM_VM_TAX_VAT_REG_NUMBER VAT_REG_NUMBER ,RM_VM_TRN_DATE, ";
                SQL = SQL + "   MASTER_BRANCH_CODE,     MASTER_BRANCH_NAME,   MASTER_BRANCH_NAME_ALIAS, ";
                SQL = SQL + "   MASTER_BRANCHDOC_PREFIX,     MASTER_BRANCH_POBOX,   MASTER_BRANCH_ADDRESS,  MASTER_BRANCH_CITY,   MASTER_BRANCH_PHONE,    MASTER_BRANCH_FAX,   MASTER_BRANCH_SPONSER_NAME, ";
                SQL = SQL + "   MASTER_BRANCH_TRADE_LICENSE_NO,  MASTER_BRANCH_EMAIL_ID,    MASTER_BRANCH_WEB_SITE,   MASTER_BRANCH_VAT_REG_NUMBER ,";
                SQL = SQL + "  SALES_PAY_TYPE_DESC,";
                SQL = SQL + "  RM_PO_QTY_RATE_TYPE,  RM_PO_VERIFIED_MNGR,RM_PO_VERIFIEDBY_MNGR,RM_PO_VERIFIED_MNGR_DATE,";
                SQL = SQL + "  RM_PO_QTY_RATE_TYPE_NAME, RM_PO_AGG_START_DATE,RM_PO_AGG_END_DATE,Master_DEPT_CODE,Master_DEPT_DESC,RM_PO_AGG_END_DATE_REQUIRED_YN ";
                SQL = SQL + " FROM  RM_PO_PRINT_VENDOR_PRC_VW  ";

                SQL = SQL + " where  RM_PO_PONO='" + txtPONo + "'AND AD_FIN_FINYRID=" + Finyr + "";

                dsPoStatus = clsSQLHelper.GetDataset(SQL);
                dsPoStatus.Tables[0].TableName = "RM_PO_PRINT_VENDOR_PRC_VW";


                SQL = "SELECT  ";
                SQL = SQL + "     RM_PO_PONO, AD_FIN_FINYRID, RM_PO_SL_NO,  ";
                SQL = SQL + "   RM_PO_NOTES, RM_PO_PRINT_YN, RM_PO_DELSTATUS ";
                SQL = SQL + "FROM  RM_PO_NOTES where RM_PO_PONO='" + txtPONo + "' and AD_FIN_FINYRID =" + Finyr + "";
                SQL = SQL + " and RM_PO_PRINT_YN ='Y' order by RM_PO_SL_NO ASC ";
                dsPoNotes = clsSQLHelper.GetDataTableByCommand(SQL);

                dsPoStatus.Tables.Add(dsPoNotes);
                dsPoStatus.Tables[1].TableName = "RM_PO_NOTES";

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

        public void InsertAttachFile(string lblCVPath, string AttachRemarks, string txtPurchaseOrderCode, int Finyr, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();
            Int32 SLNO = 0;

            try
            {
                SQL = string.Empty;
                OracleHelper oTrns = new OracleHelper();
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = "SELECT MAX(RM_PO_SLNO) SLNO FROM RM_PO_ATTACHMENT_DETAILS WHERE RM_PO_PONO ='" + txtPurchaseOrderCode + "'";
                SQL = SQL + " AND AD_FIN_FINYRID=" + Finyr + " ";

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

                SQL = " INSERT INTO RM_PO_ATTACHMENT_DETAILS";

                SQL = SQL + " (RM_PO_PONO, AD_FIN_FINYRID, RM_PO_SLNO, RM_PO_REMARKS, RM_PO_FILE_PATH)";

                SQL = SQL + " Values('" + txtPurchaseOrderCode + "'," + Finyr + "," + SLNO + ",'" + AttachRemarks + "',";
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

        public DataSet fillAttachGrid(string txtPurchaseOrderCode, int Finyr, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT RM_PO_PONO , RM_PO_REMARKS, RM_PO_FILE_PATH";

                SQL = SQL + " FROM RM_PO_ATTACHMENT_DETAILS";

                SQL = SQL + " WHERE RM_PO_PONO='" + txtPurchaseOrderCode + "' AND AD_FIN_FINYRID=" + Finyr + "";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public void DeletePath(string txtPurchaseOrderCode, string Path, int Finyr, object mngrclassobj)
        {
            try
            {
                SQL = string.Empty;
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " DELETE FROM RM_PO_ATTACHMENT_DETAILS";
                SQL = SQL + " WHERE RM_PO_PONO='" + txtPurchaseOrderCode + "' AND RM_PO_FILE_PATH='" + Path + "'";
                SQL = SQL + "  AND AD_FIN_FINYRID=" + Finyr + "";

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

        #region API

        public DataTable FillViewDivision(string username)
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "SELECT AD_BR_CODE CODE , AD_BR_NAME  NAME FROM AD_BRANCH_MASTER ";
                if (username != "ADMIN")
                {
                    SQL = SQL + " where AD_BR_CODE in (SELECT AD_BR_CODE FROM AD_USER_GRANTED_BRANCH WHERE  AD_UM_USERID = '" + username + "') ";


                }
                SQL = SQL + "   order BY AD_BR_SORT_ID asc  ";

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

        public string FillMasterDataAndDetailsList(PurchaseOrderRMPRCSearchModel oModel, string UserName)
        {
            DataSet dsReturn = new DataSet("PO_DATA");
            DataTable dtPOMaster = new DataTable();
            DataTable dtPODetails = new DataTable();

            string JSONString = null;
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT  ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_PONO pcode, to_char(AD_FIN_FINYRID) Id,      ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_PO_DATE PDate,    ";
                SQL = SQL + "    RM_PO_MASTER.RM_VM_VENDOR_CODE scode,  RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SName,    ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_GRAND_TOTAL GRANDTOTAL,    ";
                SQL = SQL + "    to_char(RM_PO_MASTER.RM_PO_NET_AMOUNT ) NETTOTAL,  ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_APPROVED, RM_PO_APPROVEDBY , RM_PO_MASTER.RM_PO_VERIFIED,  RM_PO_VERIFIEDBY ,    ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_VERIFIED_MNGR,RM_PO_MASTER.RM_PO_VERIFIEDBY_MNGR,   ";
                SQL = SQL + "    DECODE(RM_PO_MASTER.RM_PO_PO_STATUS, 'O', 'Open', 'C','Close','Cancel') STATUS,RM_PO_MASTER.RM_PO_CANCEL_REMARKS,    ";
                SQL = SQL + "    RM_PR_PRNO_CONCATENATE_NO  PRNO, RM_PO_SUPP_REF SUPPREF, RM_PO_OUR_REF OURREF,    ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_COST_CENTER COST_CENTER,    ";
                SQL = SQL + "    RM_PO_MASTER.RM_MPOM_PAYMENT_TERMS PayTerms, RM_PO_MASTER.RM_PO_EXP_DATE DDate ,   ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_REMARKS,AD_BRANCH_MASTER.AD_BR_NAME   ";
                SQL = SQL + " FROM   ";
                SQL = SQL + "    RM_PO_MASTER,RM_VENDOR_MASTER,AD_BRANCH_MASTER    ";
                SQL = SQL + " WHERE    ";
                SQL = SQL + "    RM_PO_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE       ";
                SQL = SQL + "    AND RM_PO_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE(+)    ";

                SQL = SQL + " AND RM_PO_MASTER.RM_PO_PO_DATE  BETWEEN '" + System.Convert.ToDateTime(oModel.fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(oModel.todate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE = '" + oModel.DivCode + "' ";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + "  AND  RM_PO_MASTER.AD_BR_CODE  in (SELECT AD_BR_CODE FROM AD_USER_GRANTED_BRANCH WHERE  AD_UM_USERID = '" + UserName + "') ";
                }

                SQL = SQL + " order by  RM_PO_MASTER.RM_PO_PONO   desc";

                dtPOMaster = clsSQLHelper.GetDataTableByCommand(SQL);

                dsReturn.Tables.Add(dtPOMaster);
                dsReturn.Tables[0].TableName = "PO_MASTER";

                SQL = "     SELECT    ";
                SQL = SQL + "        RM_PO_MASTER.RM_PO_PONO  , TO_CHAR(RM_PO_MASTER.AD_FIN_FINYRID) ID,       ";
                SQL = SQL + "        RM_POD_SL_NO   POD_SL_NO  ,   RM_PO_DETAILS.RM_RMM_RM_CODE ITEM_CODE,     ";
                SQL = SQL + "        RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION ||'-'||  RM_IM_ITEM_DTL_DESCRIPTION  DTLDESC,   ";
                SQL = SQL + "        RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC, RM_POD_QTY QTY,     ";
                SQL = SQL + "        RM_POD_UNIT_PRICE  UNIT_PRICE,   RM_POD_TOTALAMT TOT_AMT  ,  RM_POD_PENDING_QTY  NOT_YET_RECIVED_QTY , ";
                SQL = SQL + "        RM_PO_DETAILS.RM_SM_SOURCE_CODE,RM_SM_SOURCE_DESC ";
                SQL = SQL + "     FROM   RM_PO_MASTER,  RM_PO_DETAILS, RM_RAWMATERIAL_MASTER, RM_UOM_MASTER,RM_SOURCE_MASTER ,AD_BRANCH_MASTER       ";
                SQL = SQL + "     WHERE  RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO      ";
                SQL = SQL + "    and   RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE (+)     ";
                SQL = SQL + "    AND RM_PO_DETAILS.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE (+)      ";
                SQL = SQL + "    AND RM_PO_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+)    ";
                SQL = SQL + "    AND RM_PO_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE(+)   ";

                SQL = SQL + "    AND RM_PO_MASTER.RM_PO_PO_DATE  BETWEEN '" + System.Convert.ToDateTime(oModel.fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(oModel.todate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "    AND RM_PO_MASTER.AD_BR_CODE = '" + oModel.DivCode + "' ";

                if (UserName != "ADMIN")
                {
                    SQL = SQL + "  AND  RM_PO_MASTER.AD_BR_CODE  in (SELECT AD_BR_CODE FROM AD_USER_GRANTED_BRANCH WHERE  AD_UM_USERID = '" + UserName + "') ";
                }

                SQL = SQL + " order by  RM_PO_MASTER.RM_PO_PONO  , RM_POD_SL_NO    asc ";

                dtPODetails = clsSQLHelper.GetDataTableByCommand(SQL);
                dtPODetails.TableName = "PO_DETAILS";
                dsReturn.Tables.Add(dtPODetails);
                dsReturn.Tables[1].TableName = "PO_DETAILS";


                JSONString = JsonConvert.SerializeObject(dsReturn, Formatting.None);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return JSONString;

        }

        public string FillDataWithItemDetailsList(PurchaseOrderRMPRCSearchModel oModel, string UserName)
        {
            DataSet dsReturn = new DataSet("PO_DATA");
            DataTable dtPOMaster = new DataTable();
            DataTable dtPODetails = new DataTable();

            string JSONString = null;
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_PONO pcode, to_char(RM_PO_MASTER.AD_FIN_FINYRID) Id, RM_PO_MASTER.RM_PO_PO_DATE PDate,    ";
                SQL = SQL + "    RM_PO_MASTER.RM_VM_VENDOR_CODE scode, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SName,    ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_GRAND_TOTAL GRANDTOTAL,  to_char(RM_PO_MASTER.RM_PO_NET_AMOUNT ) NETTOTAL,  ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_APPROVED, RM_PO_MASTER.RM_PO_PO_STATUS STATUS,RM_PO_MASTER.RM_PO_CANCEL_REMARKS,    ";
                SQL = SQL + "    RM_PR_PRNO_CONCATENATE_NO PRNO, RM_PO_SUPP_REF SUPPREF, RM_PO_OUR_REF OURREF,    ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_COST_CENTER COST_CENTER,    ";
                SQL = SQL + "    RM_PO_MASTER.RM_MPOM_PAYMENT_TERMS  PayTerms, to_char(RM_PO_MASTER.RM_PO_EXP_DATE, 'DD-MON-YYYY') DDate ,   ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_VERIFIED  VERIFIED , RM_PO_MASTER.RM_PO_APPROVED   APPROVED,   ";
                SQL = SQL + "     RM_PO_MASTER.RM_PO_REMARKS ,  ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_PONO  , TO_CHAR(RM_PO_MASTER.AD_FIN_FINYRID) ID,       ";
                SQL = SQL + "    RM_POD_SL_NO   POD_SL_NO  ,   RM_PO_DETAILS.RM_RMM_RM_CODE ITEM_CODE,      ";
                SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION ||'-'||  RM_IM_ITEM_DTL_DESCRIPTION DTLDESC,   ";
                SQL = SQL + "    RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC, RM_POD_QTY QTY,     ";
                SQL = SQL + "    RM_POD_UNIT_PRICE  UNIT_PRICE,   RM_POD_TOTALAMT TOT_AMT  ,  RM_POD_PENDING_QTY NOT_YET_RECIVED_QTY ,  ";
                SQL = SQL + "    RM_PO_DETAILS.RM_SM_SOURCE_CODE,RM_SM_SOURCE_DESC,  ";
                SQL = SQL + "    AD_BRANCH_MASTER.AD_BR_NAME    ";
                SQL = SQL + " FROM  RM_PO_MASTER,RM_PO_DETAILS, RM_VENDOR_MASTER,  ";
                SQL = SQL + "      RM_RAWMATERIAL_MASTER, RM_UOM_MASTER,  RM_SOURCE_MASTER, AD_BRANCH_MASTER    ";
                SQL = SQL + " WHERE  RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO   ";
                SQL = SQL + "    and RM_PO_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE          ";
                SQL = SQL + "    and   RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE (+)     ";
                SQL = SQL + "    AND RM_PO_DETAILS.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE (+)    ";
                SQL = SQL + "    AND RM_PO_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+)        ";
                SQL = SQL + "    AND RM_PO_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE(+)   ";
                SQL = SQL + " AND RM_PO_MASTER.RM_PO_PO_DATE  BETWEEN '" + System.Convert.ToDateTime(oModel.fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(oModel.todate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE = '" + oModel.DivCode + "' ";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + "  AND  RM_PO_MASTER.AD_BR_CODE  in (SELECT AD_BR_CODE FROM AD_USER_GRANTED_BRANCH WHERE  AD_UM_USERID = '" + UserName + "') ";
                }

                SQL = SQL + " order by  RM_PO_MASTER.RM_PO_PONO   desc";

                dtPOMaster = clsSQLHelper.GetDataTableByCommand(SQL);

                dsReturn.Tables.Add(dtPOMaster);
                dsReturn.Tables[0].TableName = "PO_MASTER";

                JSONString = JsonConvert.SerializeObject(dsReturn, Formatting.None);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return JSONString;

        }

        #endregion

        #region "Row Deletion"
        public string DeleteRowSQL(string LPONo, string itemcostCode, string itemSationcode, string itemSourcecode, string itemcode, string itemdescription, string itemRFQNo, int PurChaseOrderEntryFinYearId, object mngrclassobj)
        {

            ////////////////////////// grid index end 

            string sRetun = string.Empty;


            try
            {
                SQL = string.Empty;
                OracleHelper oTrns = new OracleHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                //sRetun = DeleteRowCheck(LPONo, itemcode, itemdescription);

                //if (sRetun != "CONTINUE")
                //{
                //    return sRetun;
                //}

                sSQLArray.Clear();

                SQL = "   Insert into RM_PO_PROJECT_DETAILS_DEL_LOG (  ";
                SQL = SQL + " RM_PO_PONO, AD_FIN_FINYRID, RM_PRFQ_COMBINED_DOC_NO, RM_POD_SL_NO, RM_RMM_RM_CODE, ";
                SQL = SQL + " RM_IM_ITEM_DTL_DESCRIPTION,RM_SM_SOURCE_CODE,   SALES_STN_STATION_CODE ,  HR_DEPT_DEPT_CODE , GL_COSM_ACCOUNT_CODE,  ";
                SQL = SQL + " RM_POD_QTY, RM_POD_REDUCED_QTY, RM_PO_ROW_DEL_BY, RM_PO_ROW_DEL_BY_DT) ";
                SQL = SQL + " SELECT ";
                SQL = SQL + " RM_PO_PONO,AD_FIN_FINYRID, RM_PRFQ_COMBINED_DOC_NO,RM_POD_SL_NO,RM_RMM_RM_CODE, ";
                SQL = SQL + " RM_IM_ITEM_DTL_DESCRIPTION,RM_SM_SOURCE_CODE,  SALES_STN_STATION_CODE ,  HR_DEPT_DEPT_CODE ,  GL_COSM_ACCOUNT_CODE,  ";
                SQL = SQL + "  RM_POD_QTY, RM_POD_QTY , '" + mngrclass.UserName + "', TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM') ";
                SQL = SQL + "  FROM  ";
                SQL = SQL + "   RM_PO_PROJECT_DETAILS ";
                SQL = SQL + "   WHERE RM_PO_PONO = '" + LPONo + "'";

                SQL = SQL + "  and GL_COSM_ACCOUNT_CODE = '" + itemcostCode + "'  and  SALES_STN_STATION_CODE = '" + itemSationcode + "'";
                SQL = SQL + "    and  RM_PRFQ_COMBINED_DOC_NO = '" + itemRFQNo + "'";
                SQL = SQL + "    and  RM_SM_SOURCE_CODE = '" + itemSourcecode + "'";

                //SQL = SQL + " and HR_DEPT_DEPT_CODE = '" + itemDeptcode + "'";


                if (!string.IsNullOrEmpty(itemcode.ToString().Trim()))
                {
                    SQL = SQL + "  AND RM_PO_PROJECT_DETAILS.RM_RMM_RM_CODE ='" + itemcode + "'";
                }
                else if (!string.IsNullOrEmpty(itemdescription.ToString().Trim()))
                {
                    SQL = SQL + "  AND RM_PO_PROJECT_DETAILS.RM_IM_ITEM_DTL_DESCRIPTION ='" + itemdescription + "'";
                }



                sSQLArray.Add(SQL);

                SQL = " DELETE  FROM  RM_PO_PROJECT_DETAILS WHERE RM_PO_PONO ='" + LPONo + "'   ";

                SQL = SQL + "  and GL_COSM_ACCOUNT_CODE = '" + itemcostCode + "'  and  SALES_STN_STATION_CODE = '" + itemSationcode + "'";

                //SQL = SQL + " and HR_DEPT_DEPT_CODE = '" + itemDeptcode + "'";



                if (!string.IsNullOrEmpty(itemcode.ToString().Trim()))
                {
                    SQL = SQL + " AND RM_PO_PROJECT_DETAILS.RM_RMM_RM_CODE ='" + itemcode + "'  ";
                }
                else if (!string.IsNullOrEmpty(itemdescription.ToString().Trim()))
                {
                    SQL = SQL + " AND RM_PO_PROJECT_DETAILS.RM_IM_ITEM_DTL_DESCRIPTION ='" + itemdescription + "'  ";
                }

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM  RM_PO_DETAILS  ";
                SQL = SQL + " WHERE   RM_PO_PONO = '" + LPONo + "' AND AD_FIN_FINYRID =" + PurChaseOrderEntryFinYearId + "";
                sSQLArray.Add(SQL);



                SQL
             = " INSERT INTO RM_PO_DETAILS ( " +
             " RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID,   ";
                SQL = SQL + " RM_RMM_RM_CODE, RM_POD_SL_NO, SALES_STN_STATION_CODE, RM_SM_SOURCE_CODE,  ";
                SQL = SQL + " HR_DEPT_DEPT_CODE, RM_IM_ITEM_DTL_DESCRIPTION,   ";
                SQL = SQL + " RM_POD_QTY, RM_POD_UNIT_PRICE, RM_UOM_UOM_CODE,   ";
                SQL = SQL + " RM_POD_DISCOUNT_PERCENT, RM_POD_DISCOUNT_AMOUNT,  ";
                SQL = SQL + " RM_POD_TOTALAMT,   ";
                SQL = SQL + " RM_POD_PENDING_QTY, RM_POD_NEWPRICE,  ";
                SQL = SQL + "     RM_POD_DISCOUNT_AMOUNT_FC,    ";
                SQL = SQL + " RM_POD_TOTALAMT_FC, PC_BUD_BUDGET_ITEM_CODE )";


                SQL = SQL + "   SELECT  ";
                SQL = SQL + " RM_PO_PROJECT_DETAILS.RM_PO_PONO, RM_PO_PROJECT_DETAILS.AD_CM_COMPANY_CODE, RM_PO_PROJECT_DETAILS.AD_FIN_FINYRID,   ";
                SQL = SQL + " RM_PO_PROJECT_DETAILS.RM_RMM_RM_CODE,  ";
                SQL = SQL + "   MIN(RM_POD_SL_NO) RM_POD_SL_NO_SORT , ";

                SQL = SQL + " RM_PO_PROJECT_DETAILS.SALES_STN_STATION_CODE,RM_PO_PROJECT_DETAILS.RM_SM_SOURCE_CODE,   ";
                SQL = SQL + " RM_po_master.HR_DEPT_DEPT_CODE,  RM_IM_ITEM_DTL_DESCRIPTION,   ";
                SQL = SQL + " sum( RM_POD_QTY)RM_POD_QTY,   ";
                SQL = SQL + " case   when  sum( RM_POD_QTY)  = 0  then 0   ";
                SQL = SQL + " ELSE (SUM (RM_POD_TOTALAMT) / SUM (RM_POD_QTY)) end    RM_POD_UNIT_PRICE,  ";
                SQL = SQL + " RM_UOM_UOM_CODE,   ";
                SQL = SQL + " 0 RM_POD_DISCOUNT_PERCENT,   ";
                SQL = SQL + " sum( RM_POD_DISCOUNT_AMOUNT ) RM_POD_DISCOUNT_AMOUNT ,  ";
                SQL = SQL + " SUM (RM_POD_TOTALAMT)  RM_POD_TOTALAMT,   ";
                SQL = SQL + " SUM (RM_POD_PENDING_QTY) RM_POD_PENDING_QTY ,   ";
                SQL = SQL + " case   when  sum( RM_POD_QTY)  = 0  then 0   ";
                SQL = SQL + " ELSE (SUM (RM_POD_TOTALAMT-RM_POD_DISCOUNT_AMOUNT) / SUM (RM_POD_QTY)) end    RM_POD_NEWPRICE,   ";
                SQL = SQL + " sum(   RM_POD_DISCOUNT_AMOUNT_FC) RM_POD_DISCOUNT_AMOUNT_FC,    ";
                SQL = SQL + " sum( RM_POD_TOTALAMT_FC) RM_POD_TOTALAMT_FC ,  ''PC_BUD_BUDGET_ITEM_CODE ";
                SQL = SQL + "  FROM RM_PO_PROJECT_DETAILS ,RM_po_master   ";
                SQL = SQL + "  where    ";
                SQL = SQL + " RM_PO_PROJECT_DETAILS.RM_PO_PONO =   RM_po_master.RM_PO_PONO   ";
                SQL = SQL + " and RM_PO_PROJECT_DETAILS. AD_FIN_FINYRID  = RM_po_master. AD_FIN_FINYRID   ";


                SQL = SQL + " AND  RM_PO_PROJECT_DETAILS.RM_PO_PONO = '" + LPONo + "'  AND RM_PO_PROJECT_DETAILS. AD_FIN_FINYRID =" + PurChaseOrderEntryFinYearId + "";

                SQL = SQL + " GROUP BY   ";
                SQL = SQL + " RM_PO_PROJECT_DETAILS.RM_PO_PONO, RM_PO_PROJECT_DETAILS.AD_CM_COMPANY_CODE, RM_PO_PROJECT_DETAILS.AD_FIN_FINYRID,   ";
                SQL = SQL + " RM_PO_PROJECT_DETAILS.RM_RMM_RM_CODE,   ";
                SQL = SQL + " RM_PO_PROJECT_DETAILS.SALES_STN_STATION_CODE,RM_PO_PROJECT_DETAILS.RM_SM_SOURCE_CODE,   ";
                SQL = SQL + " RM_po_master.HR_DEPT_DEPT_CODE, RM_IM_ITEM_DTL_DESCRIPTION,   ";
                SQL = SQL + " RM_UOM_UOM_CODE   ";
                SQL = SQL + "         ORDER BY  RM_POD_SL_NO_SORT ASC ";

                sSQLArray.Add(SQL);






                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMO", LPONo, false, Environment.MachineName, "U", sSQLArray);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return sRetun;
        }


        public string PRDateValidation(DateTime dtTransDate, string PRNo)
        {
            string sReturn;
            DataSet dsEntry = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "   select * from RM_PR_MASTER ";
                SQL = SQL + "  where RM_PR_PRNO = '" + PRNo + "'";
                SQL = SQL + "  and RM_PR_PR_DATE >   '" + Convert.ToDateTime(dtTransDate).ToString("dd-MMM-yyyy") + "' ";

                dsEntry = clsSQLHelper.GetDataset(SQL);

                if (dsEntry.Tables[0].Rows.Count > 0)
                {
                    return "Date of Linked Document cant be before the date of base document";
                }


            }
            catch (Exception ex)
            {
                sReturn = System.Convert.ToString(ex.Message);
                return sReturn;

            }
            return "CONTINUE";
        }



        #endregion

        #region Revise PO updated on 3-Sep-24 by Jiju


        public string GetRevisionNo(string pono)
        {
            DataTable dtCode = new DataTable();
            try
            {

                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT RM_PO_RATE_REVISION_NO+1 AD_DN_NEXT_NO FROM RM_PO_MASTER WHERE  RM_PO_PONO='" + pono + "' ";

                dtCode = clsSQLHelper.GetDataTableByCommand(SQL);


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return System.Convert.ToString(dtCode.Rows[0]["AD_DN_NEXT_NO"]);
        }

        /*
         * Created on:13-Sep-24
        *Created By:JIju
        *Modified on:
        *Modified By:
        *Purpose:To Insert Revise the PO Initialverified3 status data
        */
        public string InsertReviseDirectPO(string InitialPO, PurchaseOrderPRCEntity oEntity, List<PurchaseOrderPRCLPO> EntityLPO,
           List<PurchaseOrderPRCTermsAndConditionEntity> objFPSNoteEntity,
           string piNoofSup, object mngrclassobj,
           bool bDocAutogenerated, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " INSERT INTO RM_PO_MASTER (";
                SQL = SQL + " RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                SQL = SQL + " RM_PO_PO_DATE, RM_VM_VENDOR_CODE, RM_PO_PRICETYPE, RM_PO_EXP_DATE, ";

                SQL = SQL + " RM_PO_ENTRY_TYPE , RM_PO_EXP_DATE_TO, ";

                ///FORIEGN CURRENCY CHANGE DONE BY TONY ON 14-NOV-2022
                ///

                SQL = SQL + " RM_PO_GRAND_TOTAL_FC, RM_PO_DISCOUNT_PERC_FC, RM_PO_DISCOUNT_AMOUNT_FC, ";
                SQL = SQL + " RM_PO_NET_AMOUNT_FC,RM_PO_VAT_PERCENTAGE_FC,RM_PO_VAT_AMOUNT_FC,   ";

                ///----------------------------/////////////////////////------
                ///

                SQL = SQL + " RM_PO_GRAND_TOTAL, RM_PO_DISCOUNT_PERC, RM_PO_DISCOUNT_AMOUNT, ";
                SQL = SQL + " RM_PO_NET_AMOUNT,RM_PO_VAT_PERCENTAGE,RM_PO_VAT_AMOUNT,   ";


                SQL = SQL + "RM_PO_PO_STATUS, RM_PO_REMARKS, ";
                SQL = SQL + " AD_CUR_CURRENCY_CODE, RM_PO_EXCHANGE_RATE, RM_PO_APPROVED, ";
                SQL = SQL + " RM_PO_APPROVEDBY, RM_PO_PURENTRY, RM_PO_POTYPE, ";
                SQL = SQL + " RM_PO_CANCEL_REMARKS, RM_PO_CONTAC1_CODE, RM_PO_CONTAC2_CODE, ";

                SQL = SQL + " RM_PR_PRNO, RM_PR_FIN_FINYRID, RM_PO_SUPP_REF, ";
                SQL = SQL + " RM_PO_SUPP_REF_DATE, RM_PO_OUR_REF, RM_PO_CREATEDBY, ";
                SQL = SQL + " RM_PO_CREATEDDATE, RM_PO_UPDATEDBY, RM_PO_UPDATEDDATE, ";
                SQL = SQL + " RM_PO_DELETESTATUS, ";
                SQL = SQL + " RM_PO_VERIFIED, RM_PO_VERIFIEDBY, RM_PO_POSTEDREMARKS, ";

                SQL = SQL + " RM_MPOM_DELIVERY_TIME_RMKS, RM_MPOM_DELIVERY_PLACE_RMKS, ";
                SQL = SQL + " RM_MPOM_VALIDITY, RM_MPOM_PAYMENT_TERMS, RM_MPOM_PAYMENT_MODE,RM_PO_COST_CENTER,";

                SQL = SQL + " RM_PO_UOM_TYPE , AD_BR_CODE,RM_PO_RM_TYPE,RM_PO_CURR_TYPE, ";
                SQL = SQL + " RM_VM_CREDIT_TERMS,RM_PO_QTY_RATE_TYPE,HR_DEPT_DEPT_CODE, ";
                SQL = SQL + " RM_PO_AGG_START_DATE,RM_PO_AGG_END_DATE , RM_PO_AGG_END_DATE_REQUIRED_YN,RM_PO_APPROVEDBY_DATE,";
                SQL = SQL + " RM_PO_VERIFIEDBY_DATE,RM_PO_VERIFIED_MNGR,RM_PO_VERIFIEDBY_MNGR,RM_PO_VERIFIED_MNGR_DATE,RM_PO_STOCK_TYPE_CODE, ";
                SQL = SQL + " RM_PO_RATE_REVISION_APP_YN,RM_PO_RATE_REVISION_BASE_PONO,RM_PO_RATE_REVISION_NO,RM_PO_LAST_REVISED_YN";
                SQL = SQL + " ) ";
                SQL = SQL + " VALUES ('" + oEntity.RM_PO_RATE_REVISION_BASE_PONO + "' ,'" + oEntity.AD_CM_COMPANY_CODE + "' ," + oEntity.AD_FIN_FINYRID + " ,";
                SQL = SQL + "'" + oEntity.RM_PO_PO_DATE.ToString("dd-MMM-yyyy") + "' ,'" + oEntity.RM_VM_VENDOR_CODE + "' ,'" + oEntity.RM_PO_PRICETYPE + "', '" + oEntity.RM_PO_EXP_DATE.ToString("dd-MMM-yyyy") + "' ,";

                SQL = SQL + "'" + oEntity.RM_PO_ENTRY_TYPE + "', '" + oEntity.RM_PO_EXP_DATE_TO.ToString("dd-MMM-yyyy") + "' ,";

                SQL = SQL + " " + Convert.ToDouble(oEntity.RM_PO_GRAND_TOTAL_FC) + ",0,0 ,";
                SQL = SQL + "" + Convert.ToDouble(oEntity.RM_PO_NET_AMOUNT_FC) + ", " + Convert.ToDouble(oEntity.TaxPerc_FC) + "," + Convert.ToDouble(oEntity.TaxAmount_FC) + " , ";


                SQL = SQL + " " + Convert.ToDouble(oEntity.RM_PO_GRAND_TOTAL) + ",0,0 ,";
                SQL = SQL + "" + Convert.ToDouble(oEntity.RM_PO_NET_AMOUNT) + ", " + Convert.ToDouble(oEntity.TaxPerc) + "," + Convert.ToDouble(oEntity.TaxAmount) + " , ";


                SQL = SQL + "'O' ,'" + oEntity.RM_PO_REMARKS + "' ,";
                SQL = SQL + "'" + oEntity.AD_CUR_CURRENCY_CODE + "' ," + oEntity.RM_PO_EXCHANGE_RATE + " ,'" + oEntity.RM_PO_APPROVED + "',";
                SQL = SQL + " '" + oEntity.RM_PO_APPROVEDBY + "','" + oEntity.RM_PO_PURENTRY + "' ,'" + oEntity.RM_PO_POTYPE + "',";
                SQL = SQL + " '', '" + oEntity.RM_PO_CONTAC1_CODE + "' ,'" + oEntity.RM_PO_CONTAC2_CODE + "' ,";
                SQL = SQL + "'" + oEntity.RM_PR_PRNO + "' ,'" + oEntity.RM_PR_FIN_FINYRID + "' ,'" + oEntity.RM_PO_SUPP_REF + "' ,";

                SQL = SQL + "'" + oEntity.RM_PO_SUPP_REF_DATE.ToString("dd-MMM-yyyy") + "' ,'" + oEntity.RM_PO_OUR_REF + "' , '" + mngrclass.UserName + "' ,";
                SQL = SQL + " TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM'), '','' ,";
                SQL = SQL + "0, ";
                SQL = SQL + "'" + oEntity.RM_PO_VERIFIED + "' ,'" + oEntity.RM_PO_VERIFIEDBY + "','" + oEntity.RM_PO_POSTEDREMARKS + "',";

                SQL = SQL + " '" + oEntity.RM_MPOM_DELIVERY_TIME_RMKS + "' , '" + oEntity.RM_MPOM_DELIVERY_PLACE_RMKS + "' , ";
                SQL = SQL + "'" + oEntity.RM_MPOM_VALIDITY + "','" + oEntity.RM_MPOM_PAYMENT_TERMS + "', '" + oEntity.RM_MPOM_PAYMENT_MODE + "', '" + oEntity.sCostCenter + "',";

                SQL = SQL + "'" + oEntity.sPOUOMType + "' ,'" + oEntity.Branch + "','" + oEntity.sLPOReceiptType + "','" + oEntity.SPOCurrType + "',";

                SQL = SQL + " '" + oEntity.PayTerms + "','" + oEntity.QtyType + "','" + oEntity.Dept + "',";
                SQL = SQL + " '" + oEntity.AggStartDate.ToString("dd-MMM-yyyy") + "' ,";
                if (oEntity.AggrementEndDateApplicableYN == "Y")
                {
                    SQL = SQL + "'" + oEntity.AggEndDate.ToString("dd-MMM-yyyy") + "',";
                }
                else
                {
                    SQL = SQL + "'',";
                }
                SQL = SQL + "'" + oEntity.AggrementEndDateApplicableYN + "',TO_DATE( '" + oEntity.ApprovedTime + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + "TO_DATE( '" + oEntity.VerifiedTime + "','DD-MM-YYYY HH:MI:SS AM'),'" + oEntity.ManagerVerified + "',";
                SQL = SQL + "'" + oEntity.ManagerVerifiedBy + "',TO_DATE( '" + oEntity.ManagerVerifiedTime + "','DD-MM-YYYY HH:MI:SS AM') ,'" + oEntity.StockType + "', ";
                SQL = SQL + " 'Y','" + InitialPO + "','" + oEntity.RM_PO_RATE_REVISION_NO + "','Y' )";

                sSQLArray.Add(SQL);



                //if (!string.IsNullOrEmpty(oEntity.RM_PR_PRNO))
                //{ // as soon as created the Po against system should close the PR

                //    SQL = " UPDATE RM_PR_MASTER";
                //    SQL = SQL + " SET RM_PR_PR_STATUS = 'C'";
                //    SQL = SQL + " WHERE RM_pR_pRno = '" + oEntity.RM_PR_PRNO + "' AND ad_fin_finyrid = " + oEntity.RM_PR_FIN_FINYRID + "";

                //    sSQLArray.Add(SQL);
                //}

                sRetun = string.Empty;
                sRetun = InsertPOProjectDetails(oEntity.RM_PO_RATE_REVISION_BASE_PONO, oEntity.AD_FIN_FINYRID, EntityLPO, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;
                sRetun = InsertPODetails(oEntity.RM_PO_RATE_REVISION_BASE_PONO, oEntity.AD_FIN_FINYRID, EntityLPO, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }
                sRetun = string.Empty;

                sRetun = InsertTermsAndConditions(oEntity.RM_PO_RATE_REVISION_BASE_PONO, objFPSNoteEntity, mngrclassobj, oEntity.AD_FIN_FINYRID);

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;
                if (oEntity.RM_PO_APPROVED == "Y")
                {
                    sRetun = InsertApprovalQrs(oEntity.RM_PO_RATE_REVISION_BASE_PONO, oEntity.AD_FIN_FINYRID, oEntity.RM_PO_PO_DATE.ToString("dd-MMM-yyyy"), oEntity.RM_PO_APPROVEDBY);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }
                //sSQLArray.Add(ResSetNextDocNumberStation("RTMO", oEntity.RM_PO_PONO, oEntity.AD_FIN_FINYRID));

                sRetun = string.Empty;
                sRetun = ModifiyPurQuotation("I", oEntity, EntityLPO, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;
                //double PrDetailCount = 0;

                //if (!string.IsNullOrEmpty(oEntity.RM_PR_PRNO))
                //{
                //    PrDetailCount = FillPrGridCount(oEntity.RM_PR_PRNO, oEntity.RM_PR_FIN_FINYRID);

                //    //if (piNoofSup == "1")
                //    if (Convert.ToDouble(piNoofSup.ToString()) == PrDetailCount)
                //    {

                //        sRetun = ModifyPurReqStatus("C", oEntity.RM_PR_PRNO, int.Parse(oEntity.RM_PR_FIN_FINYRID));
                //        if (sRetun != "CONTINUE")
                //        {
                //            return sRetun;
                //        }
                //    }
                //}



                sRetun = string.Empty;
                //WTPO //  RTMO // need to icrease the same sesaril therefor usind this tab number .// jomy 
                sRetun = oTrns.SetTrans(mngrclass.UserName, oEntity.AD_FIN_FINYRID, mngrclass.CompanyCode, DateTime.Now, "WTPO", oEntity.RM_PO_PONO, bDocAutogenerated, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);

                if (sRetun == "Save Successful")
                {

                    //sRetun = string.Empty;
                    double PrDetailCount = 0;

                    if (!string.IsNullOrEmpty(oEntity.RM_PO_RATE_REVISION_BASE_PONO))
                    {
                        PrDetailCount = FillPrGridCount(oEntity.RM_PO_RATE_REVISION_BASE_PONO, oEntity.RM_PR_FIN_FINYRID);

                        //if (piNoofSup == "1")
                        if (PrDetailCount == 0)
                        {
                            sSQLArray.Clear();

                            sRetun = ModifyPurReqStatus("C", oEntity.RM_PO_RATE_REVISION_BASE_PONO, int.Parse(oEntity.RM_PR_FIN_FINYRID));

                            if (sRetun != "CONTINUE")
                            {
                                return sRetun;
                            }
                            //if (sRetun == "CONTINUE")
                            //{
                            //    return "Save Successful";
                            //}
                            else
                            {
                                sRetun = oTrns.SetTrans(mngrclass.UserName, oEntity.AD_FIN_FINYRID, mngrclass.CompanyCode, DateTime.Now, "RTMO", oEntity.RM_PO_RATE_REVISION_BASE_PONO, bDocAutogenerated, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);
                            }
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
            return sRetun;
        } 

        #endregion




    }


    /// <summary>
    /// /////////////////////////////////--------------- ENTITY ----------------------------////////////////////////////////////////////
    /// </summary>

    public class PurchaseOrderPRCEntity
    {


        private string sRM_PO_PONO = string.Empty;
        public string RM_PO_PONO
        {
            get { return sRM_PO_PONO; }
            set { sRM_PO_PONO = value; }
        }


        private string sAD_CM_COMPANY_CODE = string.Empty;
        public string AD_CM_COMPANY_CODE
        {
            get { return sAD_CM_COMPANY_CODE; }
            set { sAD_CM_COMPANY_CODE = value; }
        }

        private int iAD_FIN_FINYRID = 0;
        public int AD_FIN_FINYRID
        {
            get { return iAD_FIN_FINYRID; }
            set { iAD_FIN_FINYRID = value; }
        }

        private DateTime dRM_PO_PO_DATE = DateTime.Now;
        public DateTime RM_PO_PO_DATE
        {
            get { return dRM_PO_PO_DATE; }
            set { dRM_PO_PO_DATE = value; }
        }

        private string sRM_VM_VENDOR_CODE = string.Empty;
        public string RM_VM_VENDOR_CODE
        {
            get { return sRM_VM_VENDOR_CODE; }
            set { sRM_VM_VENDOR_CODE = value; }
        }


        public string StockType
        {
            get;
            set;
        }

        private string sRM_PO_PRICETYPE = string.Empty;
        public string RM_PO_PRICETYPE
        {
            get { return sRM_PO_PRICETYPE; }
            set { sRM_PO_PRICETYPE = value; }
        }

        private DateTime dRM_PO_EXP_DATE = DateTime.Now;
        public DateTime RM_PO_EXP_DATE
        {
            get { return dRM_PO_EXP_DATE; }
            set { dRM_PO_EXP_DATE = value; }
        }

        private string sRM_PO_ENTRY_TYPE = string.Empty;
        public string RM_PO_ENTRY_TYPE
        {
            get { return sRM_PO_ENTRY_TYPE; }
            set { sRM_PO_ENTRY_TYPE = value; }
        }

        private DateTime dRM_PO_EXP_DATE_TO = DateTime.Now;
        public DateTime AggStartDate { get; set; }
        public DateTime AggEndDate { get; set; }

        public string AggrementEndDateApplicableYN { get; set; }
        public DateTime RM_PO_EXP_DATE_TO
        {
            get { return dRM_PO_EXP_DATE_TO; }
            set { dRM_PO_EXP_DATE_TO = value; }
        }

        private string nRM_PO_GRAND_TOTAL = string.Empty;
        public string RM_PO_GRAND_TOTAL
        {
            get { return nRM_PO_GRAND_TOTAL; }
            set { nRM_PO_GRAND_TOTAL = value; }
        }

        private string nRM_PO_DISCOUNT_PERC = string.Empty;
        public string Dept { get; set; }
        public string RM_PO_DISCOUNT_PERC
        {
            get { return nRM_PO_DISCOUNT_PERC; }
            set { nRM_PO_DISCOUNT_PERC = value; }
        }

        private string nRM_PO_DISCOUNT_AMOUNT = string.Empty;
        public string RM_PO_DISCOUNT_AMOUNT
        {
            get { return nRM_PO_DISCOUNT_AMOUNT; }
            set { nRM_PO_DISCOUNT_AMOUNT = value; }
        }

        private string nRM_PO_NET_AMOUNT = string.Empty;
        public string RM_PO_NET_AMOUNT
        {
            get { return nRM_PO_NET_AMOUNT; }
            set { nRM_PO_NET_AMOUNT = value; }
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

        public string RM_PO_GRAND_TOTAL_FC
        {
            get;
            set;
        }


        public string RM_PO_DISCOUNT_PERC_FC
        {
            get;
            set;
        }

        public string RM_PO_DISCOUNT_AMOUNT_FC
        {
            get;
            set;
        }

        public string RM_PO_NET_AMOUNT_FC
        {
            get;
            set;
        }
        public double TaxPerc_FC
        {
            get;
            set;
        }
        public double TaxAmount_FC
        {
            get;
            set;
        }


        private string sRM_PO_PO_STATUS = string.Empty;
        public string RM_PO_PO_STATUS
        {
            get { return sRM_PO_PO_STATUS; }
            set { sRM_PO_PO_STATUS = value; }
        }

        private string sRM_PO_REMARKS = string.Empty;
        public string RM_PO_REMARKS
        {
            get { return sRM_PO_REMARKS; }
            set { sRM_PO_REMARKS = value; }
        }

        private string sAD_CUR_CURRENCY_CODE = string.Empty;
        public string AD_CUR_CURRENCY_CODE
        {
            get { return sAD_CUR_CURRENCY_CODE; }
            set { sAD_CUR_CURRENCY_CODE = value; }
        }

        private string sRM_PO_EXCHANGE_RATE = string.Empty;
        public string RM_PO_EXCHANGE_RATE
        {
            get { return sRM_PO_EXCHANGE_RATE; }
            set { sRM_PO_EXCHANGE_RATE = value; }
        }
        public string ManagerVerified
        {
            get;
            set;
        }
        public string ManagerVerifiedBy
        {
            get;
            set;
        }

        public string ManagerVerifiedTime
        {
            get;
            set;
        }
        private string sRM_PO_APPROVED = string.Empty;
        public string RM_PO_APPROVED
        {
            get { return sRM_PO_APPROVED; }
            set { sRM_PO_APPROVED = value; }
        }

        private string sRM_PO_APPROVEDBY = string.Empty;
        public string ApprovedTime
        {
            get;
            set;
        }
        public string RM_PO_APPROVEDBY
        {
            get { return sRM_PO_APPROVEDBY; }
            set { sRM_PO_APPROVEDBY = value; }
        }

        private string sRM_PO_PURENTRY = string.Empty;
        public string RM_PO_PURENTRY
        {
            get { return sRM_PO_PURENTRY; }
            set { sRM_PO_PURENTRY = value; }
        }

        private string sRM_PO_POTYPE = string.Empty;
        public string RM_PO_POTYPE
        {
            get { return sRM_PO_POTYPE; }
            set { sRM_PO_POTYPE = value; }
        }

        private string sRM_PO_CANCEL_REMARKS = string.Empty;
        public string RM_PO_CANCEL_REMARKS
        {
            get { return sRM_PO_CANCEL_REMARKS; }
            set { sRM_PO_CANCEL_REMARKS = value; }
        }

        private string sRM_PO_CONTAC1_CODE = string.Empty;
        public string RM_PO_CONTAC1_CODE
        {
            get { return sRM_PO_CONTAC1_CODE; }
            set { sRM_PO_CONTAC1_CODE = value; }
        }

        private string sRM_PO_CONTAC2_CODE = string.Empty;
        public string RM_PO_CONTAC2_CODE
        {
            get { return sRM_PO_CONTAC2_CODE; }
            set { sRM_PO_CONTAC2_CODE = value; }
        }

        private string sRM_PR_PRNO = string.Empty;
        public string RM_PR_PRNO
        {
            get { return sRM_PR_PRNO; }
            set { sRM_PR_PRNO = value; }
        }

        private string iRM_PR_FIN_FINYRID = string.Empty;
        public string RM_PR_FIN_FINYRID
        {
            get { return iRM_PR_FIN_FINYRID; }
            set { iRM_PR_FIN_FINYRID = value; }
        }

        private string sRM_PO_SUPP_REF = string.Empty;
        public string RM_PO_SUPP_REF
        {
            get { return sRM_PO_SUPP_REF; }
            set { sRM_PO_SUPP_REF = value; }
        }

        private DateTime dRM_PO_SUPP_REF_DATE = DateTime.Now;
        public DateTime RM_PO_SUPP_REF_DATE
        {
            get { return dRM_PO_SUPP_REF_DATE; }
            set { dRM_PO_SUPP_REF_DATE = value; }
        }

        private string sRM_PO_OUR_REF = string.Empty;
        public string PayTerms
        {
            get;
            set;
        }
        public string RM_PO_OUR_REF
        {
            get { return sRM_PO_OUR_REF; }
            set { sRM_PO_OUR_REF = value; }
        }

        private string sRM_PO_CREATEDBY = string.Empty;
        public string RM_PO_CREATEDBY
        {
            get { return sRM_PO_CREATEDBY; }
            set { sRM_PO_CREATEDBY = value; }
        }

        private string sRM_PO_VERIFIED = string.Empty;
        public string RM_PO_VERIFIED
        {
            get { return sRM_PO_VERIFIED; }
            set { sRM_PO_VERIFIED = value; }
        }

        private string sRM_PO_VERIFIEDBY = string.Empty;
        public string VerifiedTime
        {
            get;
            set;
        }
        public string RM_PO_VERIFIEDBY
        {
            get { return sRM_PO_VERIFIEDBY; }
            set { sRM_PO_VERIFIEDBY = value; }
        }

        private string sRM_PO_POSTEDREMARKS = string.Empty;
        public string RM_PO_POSTEDREMARKS
        {
            get { return sRM_PO_POSTEDREMARKS; }
            set { sRM_PO_POSTEDREMARKS = value; }
        }

        private string sRM_MPOM_DELIVERY_TIME_RMKS = string.Empty;
        public string RM_MPOM_DELIVERY_TIME_RMKS
        {
            get { return sRM_MPOM_DELIVERY_TIME_RMKS; }
            set { sRM_MPOM_DELIVERY_TIME_RMKS = value; }
        }

        private string sRM_MPOM_DELIVERY_PLACE_RMKS = string.Empty;
        public string RM_MPOM_DELIVERY_PLACE_RMKS
        {
            get { return sRM_MPOM_DELIVERY_PLACE_RMKS; }
            set { sRM_MPOM_DELIVERY_PLACE_RMKS = value; }
        }

        private string sRM_MPOM_VALIDITY = string.Empty;
        public string RM_MPOM_VALIDITY
        {
            get { return sRM_MPOM_VALIDITY; }
            set { sRM_MPOM_VALIDITY = value; }
        }

        private string sRM_MPOM_PAYMENT_TERMS = string.Empty;
        public string RM_MPOM_PAYMENT_TERMS
        {
            get { return sRM_MPOM_PAYMENT_TERMS; }
            set { sRM_MPOM_PAYMENT_TERMS = value; }
        }

        private string sRM_MPOM_PAYMENT_MODE = string.Empty;
        public string RM_MPOM_PAYMENT_MODE
        {
            get { return sRM_MPOM_PAYMENT_MODE; }
            set { sRM_MPOM_PAYMENT_MODE = value; }
        }


        private string sRM_PO_RATE_REVISION_BASE_PONO = string.Empty;
        public string RM_PO_RATE_REVISION_BASE_PONO
        {
            get { return sRM_PO_RATE_REVISION_BASE_PONO; }
            set { sRM_PO_RATE_REVISION_BASE_PONO = value; }
        }

        private DateTime sRM_PO_RATE_REVISION_DATE = DateTime.Now;
        public DateTime RM_PO_RATE_REVISION_DATE
        {
            get { return sRM_PO_RATE_REVISION_DATE; }
            set { sRM_PO_RATE_REVISION_DATE = value; }
        }

        private string sRM_PO_RATE_REVISION_NO = string.Empty;
        public string RM_PO_RATE_REVISION_NO
        {
            get { return sRM_PO_RATE_REVISION_NO; }
            set { sRM_PO_RATE_REVISION_NO = value; }
        }
        private string sRM_PO_RATE_REVISION_APP_YN = string.Empty;
        public string RM_PO_RATE_REVISION_APP_YN
        {
            get { return sRM_PO_RATE_REVISION_APP_YN; }
            set { sRM_PO_RATE_REVISION_APP_YN = value; }
        }
        private string sRM_PO_LAST_REVISED_YN = string.Empty;
        public string RM_PO_LAST_REVISED_YN
        {
            get { return sRM_PO_LAST_REVISED_YN; }
            set { sRM_PO_LAST_REVISED_YN = value; }
        }


        public string sCostCenter
        {
            get;
            set;
        }

        public string sPOUOMType
        {
            get;
            set;
        }

        public string sLPOReceiptType
        {
            get;
            set;
        }

        public string SPOCurrType
        {
            get;
            set;
        }


        public string Branch { get; set; }
        public string QtyType { get; set; }

    }

    public class SearchPoSavedItems
    {
        public string PCode { get; set; }
        public int ID { get; set; }

        public string SCode { get; set; }
        public string SName { get; set; }
        public string AD_BR_CODE { get; set; }
        public string RM_MPOM_PAYMENT_TERMS { get; set; }
        public string SOURCE { get; set; }
        public string RM_CODE { get; set; }
        public string RM_NAME { get; set; }
        public string PDate { get; set; }
        public string RM_PO_AGG_START_DATE { get; set; }
        public string RM_PO_AGG_END_DATE { get; set; }
        public string RM_PO_APPROVED { get; set; }
        public string RM_SPECIFICATION { get; set; }
        public string STATUS { get; set; }
        public string RM_PO_REMARKS { get; set; }
        public string FA_FAM_ASSET_DESCRIPTION { get; set; }
        public double NETTOTAL { get; set; }
        public double RM_POD_UNIT_PRICE { get; set; }
        public double RM_PO_NET_AMOUNT { get; set; }
        public double UNIT_PRICE { get; set; }

    }


    public class PurchaseOrderPRCTermsAndConditionEntity
    {
        public int SlNo { get; set; }
        public string sText { get; set; }
        public string sPrintYN { get; set; }
    }



    public class PurchaseOrderPRCLPO
    {
        public object oItemCodeLPO
        {
            get;
            set;
        }

        public object oItemSpecification
        {
            get;
            set;
        }

        public object oBudget_ItemCodeLPO
        {
            get;
            set;
        }

        public object oUOMCodeLPO
        {
            get;
            set;
        }

        public object oProectCostCodeLPO
        {
            get;
            set;
        }

        public object oStcodeLPO
        {
            get;
            set;
        }

        public object oDetailDescLPO
        {
            get;
            set;
        }

        public object oSoruceCodeLPO
        {
            get;
            set;
        }

        public double dOrderQtyLPO
        {
            get;
            set;
        }

        public double dUnitPirceLPO
        {
            get;
            set;
        }

        public double dNewUnitPirceLPO
        {
            get;
            set;
        }

        public double dDiscntPerLPO
        {
            get;
            set;
        }

        public double dDiscAmountLPO
        {
            get;
            set;
        }

        public double dTotalAmountLPO
        {
            get;
            set;
        }

        public double oTollRate
        {
            get;
            set;
        }

        public double AmountBefTax { get; set; }

        public double VatAmount { get; set; }
        public double VatRate { get; set; }

        //foreign currency change done by TONY on 14-NOV-2022

        public double dUnitPirceLPO_FC
        {
            get;
            set;
        }

        public double dNewUnitPirceLPO_FC
        {
            get;
            set;
        }

        public double dDiscntPerLPO_FC
        {
            get;
            set;
        }

        public double dDiscAmountLPO_FC
        {
            get;
            set;
        }

        public double dTotalAmountLPO_FC
        {
            get;
            set;
        }

        public double oTollRate_FC
        {
            get;
            set;
        }

        public double AmountBefTax_FC { get; set; }

        public double VatAmount_FC { get; set; }
        public double VatRate_FC { get; set; }




        //-- NEW FOR GIBN  20 JAN 2014 FOR DUPLICATING ITEMS 

        public object oItemNameLPO
        {
            get;
            set;
        }
        public object oUomDescLPO
        {
            get;
            set;
        }

        public object oStDescLPO
        {
            get;
            set;
        }

        public object oSoruceDescLPO
        {
            get;
            set;
        }

        public object oTollUom
        {
            get;
            set;
        }
        public string VatType { get; set; }
        public string RFQNo { get; set; }
        public string CombinedDocno { get; set; }
        public string RFQslno { get; set; }
        // --- FOR DUPLICATING ITEMS 
    }

    public class PurchaseOrderRMPRCSearchModel
    {

        public string fromdate { get; set; }
        public string todate { get; set; }
        public string DivCode { get; set; }
    }

}