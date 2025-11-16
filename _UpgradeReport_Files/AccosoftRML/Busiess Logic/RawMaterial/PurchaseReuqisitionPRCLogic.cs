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
/// Created On      :   12-Dec-2012
/// © Copyright 2012, All Rights Reserved Terasoft ERP Soulition Technology, KADS Open Market Building , Thodoupuzha- 685 584, Idduki , Kerala, India, Mob- 8281049408
/// </summary>

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class PurchaseReuqisitionPRCLogic
    {
        private const int COL_Stcode = 0;
        private const int COL_StName = 1;
        private const int COL_DeptCode = 2;
        private const int COL_DeptName = 3;

        private const int COL_ICode = 4;
        private const int COL_IDesc = 5;
        private const int COL_UserDescription = 6;

        private const int COL_UOMCode = 7;
        private const int COL_UOMDesc = 8;

        private const int COL_DUQty = 9;
        private const int COL_StkQty = 10;
        private const int COL_TotQty = 11;
        private const int COL_UPrice = 12;
        private const int COL_Amount = 13;
        private const int COL_ItemRemarks = 14;


        private const int COL_SupCode1 = 15;
        private const int COL_SupName1 = 16;
        private const int COL_Qot1 = 17;
        private const int COL_Price1 = 18;
        private const int COL_QODate1 = 19;
        private const int COL_App1 = 20;

        private const int COL_SupCode2 = 21;
        private const int COL_SupName2 = 22;
        private const int COL_Qot2 = 23;
        private const int COL_Price2 = 24;
        private const int COL_QODate2 = 25;
        private const int COL_App2 = 26;

        private const int COL_SupCode3 = 27;
        private const int COL_SupName3 = 28;
        private const int COL_Qot3 = 29;
        private const int COL_Price3 = 30;
        private const int COL_QODate3 = 31;
        private const int COL_App3 = 32;

        private const int COL_Prev10 = 33;
        private const int COL_SlNo = 34;

        private const int COL_AppYN1 = 35;
        private const int COL_AppYN2 = 36;
        private const int COL_AppYN3 = 37;

        private const int COL_PONO = 38;
        private const int COL_PONO_FINYRID = 39;


        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;


        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;


        public string SelectedBranchCode { get; set; }

        public string UserName { get; set; }


        public PurchaseReuqisitionPRCLogic ( )
        {
            //
            // TODO: Add constructor logic here
            //
        }
        #region Fill Combo

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


        #endregion


        #region "FillView"

        public DataTable FillGridView ( string col, string sSelected )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                //if (col == "0")
                //{
                //    SQL = " SELECT SALES_STN_STATION_CODE Code,SALES_STN_STATION_NAME  Name ,'' Id   ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC  FROM ";
                //    SQL = SQL + " SL_STATION_MASTER WHERE SALES_STN_DELETESTATUS=0  and SALES_STN_PRODUCTION_STATION ='Y' ";
                //    SQL = SQL + " ORDER BY SALES_STN_STATION_NAME ";
                //}
                if (col == "2")//STATION CODE
                {

                    //SQL = "  SELECT SALES_STN_STATION_CODE Code,SALES_STN_STATION_NAME Name,'' PartNo ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC  FROM  ";
                    //SQL = SQL + "  SL_STATION_MASTER WHERE  SALES_RAW_MATERIAL_STATION ='Y' AND  SALES_STN_DELETESTATUS=0";
                    ////if (!string.IsNullOrEmpty(strCondition))
                    ////{
                    ////    SQL = SQL + " And" + strCondition;
                    ////}
                    //SQL = SQL + "  ORDER BY SALES_STN_STATION_NAME ";



                    SQL = " SELECT   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE, SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME, ";
                    SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE Code  , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME Name  , ";
                    SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.GL_COSM_ACCOUNT_CODE ,";
                    SQL = SQL + "   '' Id   ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC,''BUDGET_AMOUNT, '' PartNo ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC  ";
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


                    if (sSelected != "")
                    {
                        SQL = SQL + " 	  AND  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE  ='" + sSelected + "'";
                    }


                    SQL = SQL + "  ORDER BY   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE , SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_SORT_ID  , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE   ASC  ";




                }
                //else if (col == "2")
                //{
                //    SQL = "SELECT HR_DEPT_DEPT_CODE Code,HR_DEPT_DEPT_DESC Name , '' PartNo  ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC ";
                //    SQL = SQL + " FROM HR_DEPT_MASTER WHERE HR_DEPT_DELETESTATUS=0";

                //    // SQL = SQL + " and HR_DEPT_DEPT_CODE   in ( SELECT  RM_IP_PARAMETER_VALUE FROM  RM_DEFUALTS_GL_PARAMETERS where RM_IP_PARAMETER_DESC ='DEPARTMENT' ) ";
                //    SQL = SQL + " ORDER BY HR_DEPT_DEPT_CODE";
                //}

                else if (col == "4")//SOURCE CODE
                {
                    SQL = "SELECT RM_SM_SOURCE_CODE Code,RM_SM_SOURCE_DESC Name , '' PartNo  ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC ";
                    SQL = SQL + " FROM RM_SOURCE_MASTER WHERE RM_SM_DELETESTATUS=0";
                    SQL = SQL + " ORDER BY RM_SM_SOURCE_CODE";
                }

                //else if (col == "4")
                //{
                //    SQL = " SELECT ";
                //    SQL = SQL + " RM_RMM_RM_CODE Code,";
                //    SQL = SQL + " WS_IM_SHORT_DESC Name,";
                //    SQL = SQL + " WS_IM_MANU_PARTNO PartNo,";
                //    SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC RM_UM_UOM_DESC,";
                //    SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UOM_UOM_CODE RM_UM_UOM_CODE";
                //    SQL = SQL + " FROM  RM_RAWMATERIAL_MASTER,RM_UOM_MASTER ";
                //    SQL = SQL + " WHERE  RM_UOM_MASTER.RM_UM_UOM_CODE=RM_RAWMATERIAL_MASTER.RM_UOM_UOM_CODE ";
                //    if (!string.IsNullOrEmpty(sSelected.ToString().Trim()))
                //    {
                //        SQL = SQL + "   AND  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE NOT IN  ( '" + sSelected + "') ";
                //    }
                //    SQL = SQL + "  ORDER BY RM_RMM_RM_CODE ";
                //}
                //else if (col == "4")
                //{
                //    SQL = " SELECT RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code , RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION Name, '' Id,    RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE ,  RM_UOM_MASTER.RM_UM_UOM_DESC    ";
                //    SQL = SQL + " FROM RM_RAWMATERIAL_MASTER , RM_UOM_MASTER  ";
                //    SQL = SQL + "  where RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE  =  RM_UOM_MASTER.RM_UM_UOM_CODE (+)";

                //    SQL = SQL + " ORDER BY RM_RMM_RM_CODE  asc ";
                //}
                else if (col == "11")// UOM
                {
                    SQL = " SELECT ";
                    SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_CODE Code, ";
                    SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC Name ,'' PartNo ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC";
                    SQL = SQL + " FROM ";
                    SQL = SQL + " RM_UOM_MASTER";
                }

                else if ((col == "19") || (col == "26") || (col == "33"))// SUPPLIER1 , SUPPLIER2 , SUPPLIER3
                {
                    SQL = " SELECT ";
                    SQL = SQL + " RM_VM_VENDOR_CODE Code , RM_VM_VENDOR_NAME Name,'' PartNo  ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC ";
                    SQL = SQL + " FROM 		";
                    SQL = SQL + " RM_VENDOR_MASTER";
                    SQL = SQL + " WHERE RM_VM_VENDOR_STATUS='A' ";
                    SQL = SQL + " ORDER BY RM_VM_VENDOR_NAME";
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
         



        public DataTable FillViewSearch ( string fromdate, string todate, string sFilterType, object mngrclassobj )
        {
            DataTable dt = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
 

                SQL = " SELECT   RM_PR_PRNO PRNO,  to_char(RM_PR_MASTER.RM_PR_PR_DATE,'DD-MON-YYYY')  PR_DATE,";
                SQL = SQL + " RM_PR_PR_STATUS STATUS, RM_PR_PHYSICAL_NO PHYNO,";
                SQL = SQL + " to_char(AD_FIN_FINYRID) Id,RM_PR_COST_CENTER,GL_COSTING_MASTER_VW.NAME COSTING_NAME, ";
                SQL = SQL + " RM_PR_VERIFIED VERIFIED ,RM_PR_APPROVED APPROVED ";
                SQL = SQL + " FROM RM_PR_MASTER,GL_COSTING_MASTER_VW ";

                SQL = SQL + " WHERE ";
                SQL = SQL + "   RM_PR_MASTER.RM_PR_COST_CENTER=GL_COSTING_MASTER_VW.CODE (+)";
                SQL = SQL + "   AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";
                SQL = SQL + "   AND RM_PR_MASTER.RM_PR_PR_DATE  BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";


                if (mngrclass.UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                    SQL = SQL + "  and     RM_PR_MASTER.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                }
                if (sFilterType == "NOT APPROVED")
                {
                    SQL = SQL + " AND RM_PR_MASTER.RM_PR_APPROVED ='N'";

                    SQL = SQL + " AND RM_PR_MASTER.RM_PR_CANCEL ='N'";
                }
                else if (sFilterType == "APPROVED")
                {
                    SQL = SQL + " AND RM_PR_MASTER.RM_PR_APPROVED ='Y'";
                }
                else if (sFilterType == "CLOSED")
                {
                    SQL = SQL + " AND RM_PR_MASTER.RM_PR_PR_STATUS ='C'";
                }
                else if (sFilterType == "CANCELLED")
                {
                    SQL = SQL + " AND RM_PR_MASTER.RM_PR_CANCEL_REMARKS is not null";
                }

                SQL = SQL + " order by  RM_PR_PRNO  desc";

                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dt;

        }

        public DataSet fillviewCategory ( )
        {
            DataSet dtTable = new DataSet();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  select  WS_CM_CATEGORY_CODE CODE   , WS_CM_CATEGORY_DESC  NAME  from WS_ITEM_GROUP_MASTER order by WS_CM_CATEGORY_DESC asc";



                dtTable = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
            }

            return dtTable;
        }


        public DataTable fillViewBudgetItemCode ( string sSelected )
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
                SQL = SQL + "   AND  PC_BUD_BUDGET_LAST_YN = 'Y'";
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


        #endregion

        public DataTable DefaultRMStation ( )
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

        public DataTable GetAlertData ( string PRno, object mngrclassobj )
        {

            DataTable dsData = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT DISTINCT RM_PRREQN_QTN.RM_VM_VENDOR_CODE , RM_VENDOR_MASTER.RM_VM_VENDOR_NAME, RM_PO_PONO";
                SQL = SQL + " FROM RM_PRREQN_QTN, RM_VENDOR_MASTER";
                SQL = SQL + " WHERE  ";
                SQL = SQL + "  RM_PRREQN_QTN.RM_PRQ_APPROVED = 'Y'";
                SQL = SQL + "  AND RM_PRREQN_QTN.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " AND  RM_PR_PRNO='" + PRno + "' AND AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";
                SQL = SQL + " ORDER BY RM_PO_PONO ";

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

        #region "FetchData"

        public DataSet FetchData ( string prNo, int finyrid )
        {

            DataSet dtType = new DataSet();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT RM_PR_PRNO, AD_FIN_FINYRID, AD_CM_COMPANY_CODE, RM_PR_PHYSICAL_NO,";
                SQL = SQL + "    RM_PR_PR_DATE, RM_PR_EXP_DATE, RM_PR_PURCHASE_TYPE,";
                SQL = SQL + "    RM_PR_GRAND_TOTAL, RM_PR_REMARKS REMARKS, RM_PR_PR_STATUS PR_STATUS, RM_PR_CREATEDBY,";
                SQL = SQL + "    RM_PR_CREATEDDATE, RM_PR_UPDATEDBY, RM_PR_UPDATEDDATE, RM_PR_UPDATEDDATE,";
                SQL = SQL + "    RM_PR_VERIFIEDBY, RM_PR_VERIFIED, RM_PR_APPROVEDBY, RM_PR_APPROVED, RM_PR_CANCEL , RM_PR_CANCEL_REMARKS,";
                SQL = SQL + "    RM_PR_IMMEDIATE, RM_PR_WITHIN_2DAYS, RM_PR_WITHIN_WEEK, ";
                SQL = SQL + "    RM_PR_WITHIN_15DAYS,RM_PR_AVLB_RLSD,RM_PR_NONAVLB_PRSD ,RM_PR_MASTER.AD_BR_CODE,AD_BR_NAME,RM_PR_STOCK_TYPE_CODE ";
                SQL = SQL + " FROM RM_PR_MASTER ,AD_BRANCH_MASTER ";
                SQL = SQL + " WHERE ";
                SQL = SQL + "  RM_PR_MASTER.RM_PR_PRNO='" + prNo + "' AND AD_FIN_FINYRID=" + finyrid + "";
                SQL = SQL + " and RM_PR_MASTER.AD_BR_CODE=AD_BRANCH_MASTER.AD_BR_CODE(+) ";

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

        public DataSet FetchDetailData ( string prNo, int finyrid )
        {

            DataSet dtType = new DataSet();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT RM_PR_DETAILS.RM_PR_PRNO,";
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
                SQL = SQL + " RM_PR_DETAILS.RM_PRD_DIR_USE_QTY,";
                SQL = SQL + " RM_PR_DETAILS.RM_PRD_INV_QTY,";
                SQL = SQL + " RM_PR_DETAILS.RM_PRD_QTY, RM_PRD_APP_RATE ,RM_PRD_APP_AMOUNT  , ";
                SQL = SQL + " RM_PR_DETAILS.RM_UOM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC,";
                SQL = SQL + " RM_PR_DETAILS.RM_PRD_ITEM_APPRV,RM_PR_DETAILS.RM_PRD_ITEM_REMARKS,";
                SQL = SQL + " RM_PR_DETAILS.PC_BUD_BUDGET_ITEM_CODE,0 BUDGET_AMOUNT,";
                SQL = SQL + " RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE ,GL_COSTING_MASTER_VW.NAME  COSTNAME,GL_COSM_BUDGET_VAL_YN ";
                SQL = SQL + " FROM RM_PR_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " SL_STATION_MASTER,";
                SQL = SQL + " HR_DEPT_MASTER,";
                SQL = SQL + " RM_UOM_MASTER,RM_SOURCE_MASTER,";
                SQL = SQL + " GL_COSTING_MASTER_VW";//PC_BUD_BUDGET_DETAILS,PC_BUD_BUDGET_MASTER,
                SQL = SQL + " WHERE     RM_PR_DETAILS.RM_RMM_RM_CODE =RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " AND RM_PR_DETAILS.SALES_STN_STATION_CODE =SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_PR_DETAILS.HR_DEPT_DEPT_CODE =HR_DEPT_MASTER.HR_DEPT_DEPT_CODE(+)";
                SQL = SQL + " AND RM_PR_DETAILS.RM_SM_SOURCE_CODE =RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                SQL = SQL + " AND RM_PR_DETAILS.RM_UOM_UOM_CODE =RM_UOM_MASTER.RM_UM_UOM_CODE";
                //SQL = SQL + " AND RM_PR_DETAILS.PC_BUD_BUDGET_ITEM_CODE=PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_ITEM_CODE (+) ";
                //SQL = SQL + " AND RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE=PC_BUD_BUDGET_DETAILS.SALES_PM_PROJECT_CODE(+)";
                //SQL = SQL + " AND pc_bud_budget_details.PC_BUD_BUDGET_CODE=PC_BUD_BUDGET_MASTER.PC_BUD_BUDGET_CODE ";
                //SQL = SQL + " AND PC_BUD_BUDGET_MASTER.PC_BUD_BUDGET_LAST_YN='Y' ";
                SQL = SQL + " AND RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE=GL_COSTING_MASTER_VW.CODE (+) ";
                SQL = SQL + " AND RM_PR_PRNO='" + prNo + "' AND RM_PR_DETAILS.AD_FIN_FINYRID=" + finyrid + "";
                SQL = SQL + " order by RM_PR_DETAILS.RM_PRD_SL_NO  asc";

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

        public DataSet FetchQuotationDetails ( string sItemCode, int SlNo, int Row, string sPRNo, int FinYr )
        {

            DataSet dtType = new DataSet();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT  RM_PRREQN_QTN.RM_VM_VENDOR_CODE SUP_CODE,";
                SQL = SQL + "   RM_VENDOR_MASTER.RM_VM_VENDOR_NAME";
                SQL = SQL + "   SUP_NAME,";
                SQL = SQL + "   RM_PRQ_UNIT_PRICE U_PRICE,";
                SQL = SQL + "   TO_CHAR (RM_PRQ_QO_DATE, 'DD-MON-YYYY') QO_DATE,";
                SQL = SQL + "   RM_PRQ_QUOTATIONNO";
                SQL = SQL + "   QO_NO, RM_PRQ_APPROVED APP, RM_PO_PONO PONO ,RM_PO_PO_FINYEAR_ID, RM_PRQ_SUPPLIERNO ,RM_PRQ_DISCOUNT";
                SQL = SQL + "   FROM RM_PRREQN_QTN, RM_VENDOR_MASTER";
                SQL = SQL + "   WHERE  ";
                SQL = SQL + "   RM_PRREQN_QTN.RM_VM_VENDOR_CODE =  RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + "   AND  RM_PR_PRNO='" + sPRNo + "' AND RM_PRREQN_QTN.AD_FIN_FINYRID=" + FinYr + " AND ";
                SQL = SQL + "   RM_PRREQN_QTN.RM_RMM_RM_CODE='" + sItemCode + "' AND RM_PRQ_SLNO=" + SlNo + "";

                SQL = SQL + "   ORDER BY RM_PRQ_SLNO";


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


        public DataTable FillGridItemView ( string col, string sSelected, string sBudgetItemCode, string sProjectCode, string StCode )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                if (col == "8")
                {
                    if (sBudgetItemCode != "")
                    {
                        SQL = " SELECT ";

                        SQL = SQL + "   RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code, RM_RMM_RM_DESCRIPTION Name,";

                        SQL = SQL + "   RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE RM_UM_UOM_CODE,RM_UOM_MASTER.RM_UM_UOM_DESC RM_UM_UOM_DESC,";

                        SQL = SQL + "   RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE RESOURCE_CODE,PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_NAME  RESOURCE_NAME,";

                        SQL = SQL + "   PC_BUD_BUDGET_ITEM_CODE,";

                        SQL = SQL + "   PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_AMOUNT BUDGET_AMOUNT  ";

                        SQL = SQL + " FROM  ";
                        SQL = SQL + "   RM_RAWMATERIAL_MASTER,RM_UOM_MASTER , ";
                        SQL = SQL + "   RM_RAWMATERIAL_DETAILS,";
                        SQL = SQL + "   PC_BUD_RESOURCE_MASTER,";
                        SQL = SQL + "   PC_BUD_BUDGET_MASTER,";
                        SQL = SQL + "   PC_BUD_BUDGET_DETAILS  ";

                        SQL = SQL + " WHERE  ";

                        SQL = SQL + "   RM_UOM_MASTER.RM_UM_UOM_CODE=RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE ";
                        SQL = SQL + "   AND  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE=RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE ";
                        SQL = SQL + "   AND  PC_BUD_BUDGET_MASTER.PC_BUD_BUDGET_CODE = PC_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_CODE  ";
                        SQL = SQL + "   AND  PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE= PC_BUD_BUDGET_DETAILS.PC_BUD_RESOURCE_CODE ";
                        SQL = SQL + "   AND  RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE = PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE ";
                        SQL = SQL + "   AND  PC_BUD_BUDGET_MASTER.PC_BUD_BUDGET_LAST_YN='Y' ";

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
                            SQL = SQL + "   AND  RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE= '" + SelectedBranchCode + "' ";
                        }

                        //if (!string.IsNullOrEmpty(sSelected.ToString().Trim()))
                        //{
                        //    SQL = SQL + "   AND  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE NOT IN  ( '" + sSelected + "') ";
                        //}
                    }

                    else
                    {
                        SQL = " SELECT ";
                        SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code,";
                        SQL = SQL + " RM_RMM_RM_DESCRIPTION Name,";
                        SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC RM_UM_UOM_DESC,";
                        SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE RM_UM_UOM_CODE, ";

                        SQL = SQL + " '' PC_BUD_BUDGET_ITEM_CODE,";

                        SQL = SQL + " '' BUDGET_AMOUNT  ";

                        SQL = SQL + " FROM  RM_RAWMATERIAL_MASTER,RM_UOM_MASTER ,RM_RAWMATERIAL_DETAILS";
                        SQL = SQL + " WHERE  RM_UOM_MASTER.RM_UM_UOM_CODE=RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE ";
                        SQL = SQL + " And  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE=RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE ";
                        SQL = SQL + " And  RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE='" + StCode + "' ";
                        SQL = SQL + " And  RM_RAWMATERIAL_DETAILS.AD_BR_CODE='" + SelectedBranchCode + "' ";

                        //if (!string.IsNullOrEmpty(sSelected.ToString().Trim()))
                        //{
                        //    SQL = SQL + "   AND  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE NOT IN  ( '" + sSelected + "') ";
                        //}
                    }

                    SQL = SQL + "  ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
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

        #endregion

        #region "ValidateModification"

        public DataTable ValidateModification ( string PRNo, object mngrclassobj )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT count(RM_PRREQN_QTN.RM_PR_PRNO) cnt";
                SQL = SQL + " FROM RM_PRREQN_QTN, RM_PR_MASTER";
                SQL = SQL + " WHERE     RM_PRREQN_QTN.RM_PR_PRNO = RM_PR_MASTER.RM_PR_PRNO ";
                SQL = SQL + " AND RM_PRREQN_QTN.ad_fin_finyrid = RM_PR_MASTER.ad_fin_finyrid";
                // SQL = SQL +  " and RM_PR_MASTER.RM_PR_PR_STATUS ='O'"
                SQL = SQL + " AND RM_PR_MASTER.RM_PR_PRNO='" + PRNo + "'";
                SQL = SQL + "  AND RM_PR_MASTER.AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";
                SQL = SQL + "AND RM_PRREQN_QTN.RM_PO_PONO IS not NULL";


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

        public DataTable Validatedeletion ( string PRNo, object mngrclassobj )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT count(RM_PRREQN_QTN.RM_PO_PONO) cnt";
                SQL = SQL + " FROM RM_PRREQN_QTN  ";
                SQL = SQL + " WHERE RM_PRREQN_QTN.RM_PR_PRNO='" + PRNo + "'";
                SQL = SQL + "  AND RM_PRREQN_QTN.AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";
                SQL = SQL + "AND RM_PRREQN_QTN.RM_PO_PONO IS not NULL";


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

        public DataSet ValidateApprovedAmountold ( string BudgetItem_Code, object mngrclassobj )
        {

            DataSet dtType = new DataSet();
            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT SUM(RM_PRD_APP_AMOUNT) TOTAL_PR_AMOUNT ";
                SQL = SQL + " FROM  ";
                SQL = SQL + "   RM_PR_MASTER,RM_PR_DETAILS";
                SQL = SQL + " WHERE  ";
                SQL = SQL + "   RM_PR_MASTER.RM_PR_PRNO=RM_PR_DETAILS.RM_PR_PRNO ";
                SQL = SQL + "   AND RM_PR_DETAILS.PC_BUD_BUDGET_ITEM_CODE='" + BudgetItem_Code + "'";

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

        #endregion

        #region "Insert/Update/Delete"

        public string InsertSQL ( PurchaseRequisitionPRCEntity oEntity, List<PurchaseRequisitionPRCGridEntity> objWSGridEntity,
            object mngrclassobj, bool bDocAutogenerated, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber )
        {

            string sRetun = string.Empty;
            // string sApprStatus = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " INSERT INTO RM_PR_MASTER (";
                SQL = SQL + " RM_PR_PRNO, AD_FIN_FINYRID, AD_CM_COMPANY_CODE, ";
                SQL = SQL + " RM_PR_PHYSICAL_NO, RM_PR_PR_DATE, RM_PR_EXP_DATE,RM_PR_EXP_DATE_TO, ";
                SQL = SQL + " RM_PR_PURCHASE_TYPE, RM_PR_GRAND_TOTAL, RM_PR_REMARKS, ";
                SQL = SQL + " RM_PR_PR_STATUS, AD_CUR_CURRENCY_CODE,RM_PR_EXCHANGE_RATE,RM_PR_CREATEDBY, RM_PR_CREATEDDATE, ";
                SQL = SQL + " RM_PR_UPDATEDBY, RM_PR_UPDATEDDATE,RM_PR_DELETESTATUS,  ";
                SQL = SQL + " RM_PR_VERIFIEDBY, RM_PR_VERIFIED, RM_PR_APPROVEDBY, ";
                SQL = SQL + " RM_PR_APPROVED,RM_PR_COST_CENTER,";
                SQL = SQL + " RM_PR_IMMEDIATE, RM_PR_WITHIN_2DAYS, RM_PR_WITHIN_WEEK, ";
                SQL = SQL + " RM_PR_WITHIN_15DAYS,RM_PR_AVLB_RLSD,RM_PR_NONAVLB_PRSD,AD_BR_CODE,RM_PR_STOCK_TYPE_CODE ";
                SQL = SQL + "  ) ";

                SQL = SQL + " VALUES ( '" + oEntity.PRNo + "', " + mngrclass.FinYearID + ", '" + mngrclass.CompanyCode + "',";
                SQL = SQL + "'" + oEntity.Physical + "' , '" + oEntity.PRDate.ToString("dd-MMM-yyyy") + "' ,'" + oEntity.ExpDate.ToString("dd-MMM-yyyy") + "' ,'" + oEntity.ExpDate.ToString("dd-MMM-yyyy") + "' ,";
                SQL = SQL + "'LOCAL' ," + Convert.ToDouble(oEntity.GrandTotal) + " ,'" + oEntity.Remarks + "' ,";
                SQL = SQL + "'O', '01',1,'" + mngrclass.UserName + "',TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM') ,";
                SQL = SQL + "'' ,'' ,0 ,";
                SQL = SQL + "'" + oEntity.VerifiedBy + "','" + oEntity.Verified + "' , '" + oEntity.ApprovedBy + "',";
                SQL = SQL + "'" + oEntity.Approved + "','" + oEntity.CostCenter + "',";
                SQL = SQL + "'" + oEntity.Immediate + "','" + oEntity.Within2Days + "','" + oEntity.WithinWeek + "',";
                SQL = SQL + "'" + oEntity.Within15Days + "','" + oEntity.Available + "','" + oEntity.NonAvailable + "', ";
                SQL = SQL + "'" + oEntity.Branch + "','" + oEntity.StockType + "'";
                SQL = SQL + " )";



                sSQLArray.Add(SQL);


                sRetun = string.Empty;
                sRetun = InsertDetailsSQL(oEntity.PRNo, mngrclass.FinYearID, objWSGridEntity, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }


                //if (oEntity.Approved == "Y")
                //{
                //    sRetun = InsertApprovalQrs(oEntity.PRNo, mngrclassobj, oEntity.PRDate.ToString("dd-MMM-yyyy"));
                //    if (sRetun != "CONTINUE")
                //    {
                //        return sRetun;
                //    }
                //}


                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRQ", oEntity.PRNo, bDocAutogenerated, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);

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

        private string InsertDetailsSQL ( string PRNo, int iFinYr, List<PurchaseRequisitionPRCGridEntity> objWSGridEntity, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                // int slno = 1;

                int iRow = 0;

                foreach (var Data in objWSGridEntity)
                {

                    if (!string.IsNullOrEmpty(Data.PRStCode) & !string.IsNullOrEmpty(Data.PRItemCode) & !string.IsNullOrEmpty(Data.PRDeptCode))
                    {
                        ++iRow;
                        double totQty = Data.PRStkQty + Data.PRDirUseQty;

                        SQL = " INSERT INTO RM_PR_DETAILS (";
                        SQL = SQL + " RM_PR_PRNO, AD_FIN_FINYRID, AD_CM_COMPANY_CODE, ";
                        SQL = SQL + " SALES_STN_STATION_CODE, HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE, RM_RMM_RM_CODE, ";
                        SQL = SQL + " RM_IM_ITEM_DTL_DESCRIPTION, RM_PRD_SL_NO, RM_PRD_DIR_USE_QTY, ";
                        SQL = SQL + " RM_PRD_INV_QTY, RM_PRD_QTY, RM_PRD_APP_RATE ,RM_PRD_APP_AMOUNT  ,  RM_UOM_UOM_CODE, ";
                        SQL = SQL + " RM_PRD_DELETESTATUS, RM_PRD_ITEM_APPRV,RM_PRD_ITEM_REMARKS,PC_BUD_BUDGET_ITEM_CODE,GL_COSM_ACCOUNT_CODE) ";
                        SQL = SQL + " VALUES ( '" + PRNo + "', " + mngrclass.FinYearID + ", '" + mngrclass.CompanyCode + "',";
                        SQL = SQL + "'" + Data.PRStCode + "' , '" + Data.PRDeptCode + "','" + Data.PRSourceCode + "','" + Data.PRItemCode + "' ,";
                        SQL = SQL + "'" + Data.PRDetDesc + "'," + iRow + " ," + Convert.ToDouble(Data.PRDirUseQty) + " ,";
                        SQL = SQL + "" + Convert.ToDouble(Data.PRStkQty) + " ," + Convert.ToDouble(totQty) + "," + Convert.ToDouble(Data.PRPrice) + ", " + Convert.ToDouble(Data.PRAmnt) + ", '" + Data.PRUomCode + "',";
                        SQL = SQL + "0 ,'" + Data.sAppStatus + "','" + Data.PRItemRemarks + "','" + Data.PRBudgetItemCode + "','" + Data.ProjectCostCode + "')";

                        sSQLArray.Add(SQL);
                    }

                    ///================================  RM_PR_DETAILS end 

                }
                //<< supplier one 

                int slno1 = 0;

                #region "Supplier one "

                foreach (var Data in objWSGridEntity)
                {

                    ++slno1;

                    if (string.IsNullOrEmpty(Data.PRPONo))
                    {
                        if (!string.IsNullOrEmpty(Data.PRQtnSupp1))
                        {
                            if (string.IsNullOrEmpty(Data.PRPONo))
                            {

                                SQL = " INSERT INTO RM_PRREQN_QTN (";
                                SQL = SQL + " RM_PR_PRNO, AD_FIN_FINYRID, RM_RMM_RM_CODE, ";
                                SQL = SQL + " AD_CM_COMPANY_CODE, RM_VM_VENDOR_CODE, RM_PRQ_UNIT_PRICE, ";
                                SQL = SQL + " RM_PRQ_QO_DATE, RM_PRQ_APPROVED, RM_PRQ_SLNO, ";
                                SQL = SQL + " RM_PRQ_SUPPLIERNO, RM_PRQ_QUOTATIONNO, RM_PO_PONO, ";
                                SQL = SQL + " RM_PO_PO_FINYEAR_ID, RM_PRQ_DELETESTATUS,RM_PRQ_DISCOUNT,GL_COSM_ACCOUNT_CODE,SALES_STN_STATION_CODE,RM_SM_SOURCE_CODE) ";
                                SQL = SQL + " VALUES ( '" + PRNo + "', " + mngrclass.FinYearID + ", '" + Data.PRItemCode + "',";
                                SQL = SQL + "'" + mngrclass.CompanyCode + "' ,'" + Data.PRQtnSupp1 + "' ," + Convert.ToDouble(Data.PRQtnPrice1) + " ,";
                                SQL = SQL + "'" + Data.PRQtnDate1 + "','" + Data.PRsAppStatus1 + "' , " + slno1 + ",";
                                SQL = SQL + " 1 , '" + Data.PRQtnNo1 + "','' ,";
                                SQL = SQL + " 0, 0," + Convert.ToDouble(Data.PRQtnDiscount1) + ",'" + Data.ProjectCostCode + "','" + Data.PRStCode + "','" + Data.PRSourceCode + "')";

                                sSQLArray.Add(SQL);
                            }
                        }

                    }  // if po LOOP supplier one 
                }


                #endregion
                //<< supplier two 
                int slno2 = 0;


                foreach (var Data in objWSGridEntity)
                {
                    ++slno2;
                    if (string.IsNullOrEmpty(Data.PRPONo))
                    {

                        if (!string.IsNullOrEmpty(Data.PRQtnSupp2))
                        {

                            if (string.IsNullOrEmpty(Data.PRPONo))
                            {

                                SQL = " INSERT INTO RM_PRREQN_QTN (";
                                SQL = SQL + " RM_PR_PRNO, AD_FIN_FINYRID, RM_RMM_RM_CODE, ";
                                SQL = SQL + " AD_CM_COMPANY_CODE, RM_VM_VENDOR_CODE, RM_PRQ_UNIT_PRICE, ";
                                SQL = SQL + " RM_PRQ_QO_DATE, RM_PRQ_APPROVED, RM_PRQ_SLNO, ";
                                SQL = SQL + " RM_PRQ_SUPPLIERNO, RM_PRQ_QUOTATIONNO, RM_PO_PONO, ";
                                SQL = SQL + " RM_PO_PO_FINYEAR_ID, RM_PRQ_DELETESTATUS,RM_PRQ_DISCOUNT,GL_COSM_ACCOUNT_CODE,SALES_STN_STATION_CODE,RM_SM_SOURCE_CODE) ";
                                SQL = SQL + " VALUES ( '" + PRNo + "', " + mngrclass.FinYearID + ", '" + Data.PRItemCode + "',";
                                SQL = SQL + "'" + mngrclass.CompanyCode + "' ,'" + Data.PRQtnSupp2 + "' ," + Convert.ToDouble(Data.PRQtnPrice2) + " ,";
                                SQL = SQL + "'" + Data.PRQtnDate2 + "','" + Data.PRsAppStatus2 + "' , " + slno2 + ",";
                                SQL = SQL + " 2 , '" + Data.PRQtnNo2 + "','' ,";
                                SQL = SQL + " 0, 0," + Convert.ToDouble(Data.PRQtnDiscount2) + ",'" + Data.ProjectCostCode + "','" + Data.PRStCode + "','" + Data.PRSourceCode + "')";

                                sSQLArray.Add(SQL);
                            }
                        }

                    }   // if  purchaser order loop end 


                }


                //<< supplier  three 
                int slno3 = 0;


                foreach (var Data in objWSGridEntity)
                {

                    ++slno3;

                    if (string.IsNullOrEmpty(Data.PRPONo))
                    {

                        if (!string.IsNullOrEmpty(Data.PRQtnSupp3))
                        {
                            if (string.IsNullOrEmpty(Data.PRPONo))
                            {

                                SQL = " INSERT INTO RM_PRREQN_QTN (";
                                SQL = SQL + " RM_PR_PRNO, AD_FIN_FINYRID, RM_RMM_RM_CODE, ";
                                SQL = SQL + " AD_CM_COMPANY_CODE, RM_VM_VENDOR_CODE, RM_PRQ_UNIT_PRICE, ";
                                SQL = SQL + " RM_PRQ_QO_DATE, RM_PRQ_APPROVED, RM_PRQ_SLNO, ";
                                SQL = SQL + " RM_PRQ_SUPPLIERNO, RM_PRQ_QUOTATIONNO, RM_PO_PONO, ";
                                SQL = SQL + " RM_PO_PO_FINYEAR_ID, RM_PRQ_DELETESTATUS,RM_PRQ_DISCOUNT,GL_COSM_ACCOUNT_CODE,SALES_STN_STATION_CODE,RM_SM_SOURCE_CODE) ";
                                SQL = SQL + " VALUES ( '" + PRNo + "', " + mngrclass.FinYearID + ", '" + Data.PRItemCode + "',";
                                SQL = SQL + "'" + mngrclass.CompanyCode + "' ,'" + Data.PRQtnSupp3 + "' ," + Convert.ToDouble(Data.PRQtnPrice3) + " ,";
                                SQL = SQL + "'" + Data.PRQtnDate3 + "','" + Data.PRsAppStatus3 + "' , " + slno3 + ",";
                                SQL = SQL + " 3 , '" + Data.PRQtnNo3 + "','' ,";
                                SQL = SQL + " 0, 0," + Convert.ToDouble(Data.PRQtnDiscount3) + ",'" + Data.ProjectCostCode + "','" + Data.PRStCode + "','" + Data.PRSourceCode + "')";

                                sSQLArray.Add(SQL);
                            }
                        }

                    } // if purchaser order if end 

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

        private string InsertApprovalQrs ( string txtPRNo, object mngrclassobj, string dtpPRDate )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " INSERT INTO RM_PR_TRIGGER (";
                SQL = SQL + " RM_PR_PRNO , AD_FIN_FINYRID, AD_UM_USERID, ";
                SQL = SQL + " RM_PR_PR_DATE, RM_POSTED_DATE) ";

                SQL = SQL + " VALUES ('" + txtPRNo + "', " + mngrclass.FinYearID + ", '" + mngrclass.UserName + "', '" + String.Format("{0:dd/MMM/yyyy}", dtpPRDate) + "' , TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'))";

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

        public string UpdateSQL ( PurchaseRequisitionPRCEntity oEntity, List<PurchaseRequisitionPRCGridEntity> objWSGridEntity, object mngrclassobj )
        {

            string sRetun = string.Empty;
            // string sApprStatus = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = "  UPDATE  RM_PR_MASTER SET ";
                SQL = SQL + " RM_PR_PHYSICAL_NO ='" + oEntity.Physical + "' , RM_PR_PR_DATE ='" + oEntity.PRDate.ToString("dd-MMM-yyyy") + "' , RM_PR_EXP_DATE ='" + oEntity.ExpDate.ToString("dd-MMM-yyyy") + "' , ";
                SQL = SQL + "  RM_PR_GRAND_TOTAL =" + Convert.ToDouble(oEntity.GrandTotal) + "  , RM_PR_REMARKS ='" + oEntity.Remarks + "', ";

                SQL = SQL + " RM_PR_UPDATEDBY ='" + mngrclass.UserName + "' , RM_PR_UPDATEDDATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + " RM_PR_VERIFIEDBY ='" + oEntity.VerifiedBy + "' , RM_PR_VERIFIED ='" + oEntity.Verified + "' , RM_PR_APPROVEDBY ='" + oEntity.ApprovedBy + "' , ";
                SQL = SQL + " RM_PR_APPROVED = '" + oEntity.Approved + "',RM_PR_COST_CENTER='" + oEntity.CostCenter + "',";
                SQL = SQL + " RM_PR_IMMEDIATE = '" + oEntity.Immediate + "', RM_PR_WITHIN_2DAYS = '" + oEntity.Within2Days + "', RM_PR_WITHIN_WEEK = '" + oEntity.WithinWeek + "', ";
                SQL = SQL + " RM_PR_WITHIN_15DAYS = '" + oEntity.Within15Days + "', ";
                SQL = SQL + " RM_PR_AVLB_RLSD = '" + oEntity.Available + "',RM_PR_NONAVLB_PRSD = '" + oEntity.NonAvailable + "', ";
                SQL = SQL + " AD_BR_CODE ='" + oEntity.Branch + "' ";
                SQL = SQL + " WHERE RM_PR_PRNO =  '" + oEntity.PRNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_PR_DETAILS    ";
                SQL = SQL + "WHERE RM_PR_PRNO =  '" + oEntity.PRNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
                sSQLArray.Add(SQL);


                //SQL = " DELETE FROM RM_PR_DETAILS    ";
                //SQL = SQL + " WHERE RM_PR_PRNO in( select RM_PR_PRNO FROM  RM_PRREQN_QTN   ";
                //SQL = SQL + " WHERE  RM_PRREQN_QTN.RM_PO_PONO IS   NULL  AND  RM_PR_PRNO ='" + oEntity.PRNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + ")";
                //SQL = SQL + " and RM_RMM_RM_CODE in(select RM_RMM_RM_CODE FROM  RM_PRREQN_QTN    ";
                //SQL = SQL + " WHERE  RM_PRREQN_QTN.RM_PO_PONO IS   NULL  AND  RM_PR_PRNO ='" + oEntity.PRNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + ")";
                //SQL = SQL + " and GL_COSM_ACCOUNT_CODE in(select GL_COSM_ACCOUNT_CODE FROM  RM_PRREQN_QTN    ";
                //SQL = SQL + " WHERE  RM_PRREQN_QTN.RM_PO_PONO IS   NULL  AND  RM_PR_PRNO ='" + oEntity.PRNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + ")";
                //SQL = SQL + " AND  RM_PR_PRNO ='" + oEntity.PRNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
                //sSQLArray.Add(SQL);//for row wise approval after Master Approval

                SQL = " DELETE FROM  RM_PRREQN_QTN    ";
                SQL = SQL + "WHERE  RM_PRREQN_QTN.RM_PO_PONO IS   NULL AND  RM_PR_PRNO =  '" + oEntity.PRNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
                sSQLArray.Add(SQL);


                sRetun = string.Empty;
                sRetun = InsertDetailsSQL(oEntity.PRNo, mngrclass.FinYearID, objWSGridEntity, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                //if (oEntity.Approved == "Y")
                //{
                //    sRetun = InsertApprovalQrs(oEntity.PRNo, mngrclassobj, oEntity.PRDate.ToString("dd-MMM-yyyy"));
                //    if (sRetun != "CONTINUE")
                //    {
                //        return sRetun;
                //    }
                //}

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRQ", oEntity.PRNo, false, Environment.MachineName, "U", sSQLArray);

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

        public string DeleteSQL ( string PRNo, object mngrclassobj )
        {

            string sRetun = string.Empty;
            // string sApprStatus = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = "  Delete from   RM_PR_MASTER   ";
                SQL = SQL + "WHERE RM_PR_PRNO =  '" + PRNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_PR_DETAILS    ";
                SQL = SQL + "WHERE RM_PR_PRNO =  '" + PRNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
                sSQLArray.Add(SQL);

                SQL = " DELETE FROM  RM_PRREQN_QTN    ";
                SQL = SQL + "WHERE RM_PR_PRNO =  '" + PRNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
                sSQLArray.Add(SQL);


                //sRetun = string.Empty;
                //sRetun = InsertDetailsSQL(PRNo, mngrclass.FinYearID, fpsPRR, mngrclassobj);
                //if (sRetun != "CONTINUE")
                //{
                //    return sRetun;
                //}


                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRQ", PRNo, false, Environment.MachineName, "D", sSQLArray);

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

        public DataSet dsPoChecking ( string txtPRNo, object mngrclassobj )
        {
            DataSet dsPoStatus = new DataSet();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                sSQLArray.Clear();

                SQL = " SELECT count(RM_PO_PONO) cnt";
                SQL = SQL + " FROM RM_PO_MASTER";
                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_PR_PRNO='" + txtPRNo.Trim() + "'AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";


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


        public string UnApproveData ( string PRNO, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();


                SQL = " UPDATE RM_PR_MASTER SET ";
                SQL = SQL + "RM_PR_APPROVED ='N',";
                SQL = SQL + "RM_PR_APPROVEDBY= ''";
                SQL = SQL + " WHERE RM_PR_PRNO = '" + PRNO + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRQ", PRNO, false, Environment.MachineName, "U", sSQLArray);

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

        public DataTable HideUnpprovlControl ( object mngrclassobj, string sItemId )
        {
            DataTable dtHideApprovlControl = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "";
                SQL = " SELECT  count(*) cnt";
                SQL = SQL + "   FROM   ad_mail_approval_users";
                SQL = SQL + "  WHERE  ";
                SQL = SQL + "    ad_mail_approval_users.ad_um_userid = '" + mngrclass.UserName + "'";
                SQL = SQL + "    AND  ad_mail_approval_users.AD_AF_ID  = '" + sItemId + "'";

                dtHideApprovlControl = clsSQLHelper.GetDataTableByCommand(SQL);
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
            return dtHideApprovlControl;
        }


        public DataTable HideApprovlControl ( string userId, string sItemId )
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


        //public string ItemDetailsDelete(string PRNo,string ItemCode ,object mngrclassobj)
        //{

        //    string sRetun = string.Empty;
        //    // string sApprStatus = string.Empty;
        //    try
        //    {
        //        OracleHelper oTrns = new OracleHelper();
        //        SessionManager mngrclass = (SessionManager)mngrclassobj;

        //        sSQLArray.Clear();

        //        //SQL = "  Delete from   RM_PR_MASTER   ";
        //        //SQL = SQL + "WHERE RM_PR_PRNO =  '" + PRNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

        //        //sSQLArray.Add(SQL);

        //        SQL = " DELETE FROM RM_PR_DETAILS    ";
        //        SQL = SQL + " WHERE RM_PR_PRNO =  '" + PRNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
        //        SQL = SQL + " AND RM_RMM_RM_CODE =  '" + ItemCode + "' ";

        //        sSQLArray.Add(SQL);

        //        SQL = " DELETE FROM  RM_PRREQN_QTN    ";
        //        SQL = SQL + " WHERE RM_PR_PRNO =  '" + PRNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
        //        SQL = SQL + " AND RM_RMM_RM_CODE =  '" + ItemCode + "' ";
        //        sSQLArray.Add(SQL);


        //        //sRetun = string.Empty;
        //        //sRetun = InsertDetailsSQL(PRNo, mngrclass.FinYearID, fpsPRR, mngrclassobj);
        //        //if (sRetun != "CONTINUE")
        //        //{
        //        //    return sRetun;
        //        //}


        //        sRetun = string.Empty;

        //        sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRQ", PRNo, false, Environment.MachineName, "D", sSQLArray);

        //    }
        //    catch (Exception ex)
        //    {
        //        sRetun = System.Convert.ToString(ex.Message);

        //        return sRetun;
        //    }
        //    finally
        //    {
        //        // ocConn.Close();
        //        // ocConn.Dispose();
        //    }
        //    return sRetun;
        //}

        #endregion

        #region "Open/Close/Cancel"

        public string REQOpen ( string PRno, object mngrclassobj )
        {

            string sRetun = string.Empty;
            // string sApprStatus = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " UPDATE RM_PR_MASTER   ";
                SQL = SQL + "  SET RM_PR_PR_STATUS ='C'";
                SQL = SQL + "  WHERE   RM_PR_PRNO  = '" + PRno + "'  AND ad_fin_finyrid = " + mngrclass.FinYearID;
                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRQ", PRno, false, Environment.MachineName, "U", sSQLArray);

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

        public string REQClose ( string PRno, object mngrclassobj )
        {

            string sRetun = string.Empty;
            // string sApprStatus = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " UPDATE RM_PR_MASTER   ";
                SQL = SQL + "  SET RM_PR_PR_STATUS ='O'";
                SQL = SQL + "  WHERE   RM_PR_PRNO  = '" + PRno + "'  AND ad_fin_finyrid = " + mngrclass.FinYearID;
                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRQ", PRno, false, Environment.MachineName, "U", sSQLArray);

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

        public string REQCancel ( string PRno, string Cancelremarks, object mngrclassobj )
        {

            string sRetun = string.Empty;
            // string sApprStatus = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " UPDATE RM_PR_MASTER   ";
                SQL = SQL + "  SET RM_PR_PR_STATUS ='C' ,   RM_PR_CANCEL  = 'Y',  RM_PR_CANCEL_REMARKS ='" + Cancelremarks + "'";
                SQL = SQL + "  WHERE   RM_PR_PRNO  = '" + PRno + "'  AND ad_fin_finyrid = " + mngrclass.FinYearID;
                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRQ", PRno, false, Environment.MachineName, "U", sSQLArray);

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

        #region "Generate PO Functions"

        //public string GeneratePO(PurchaseRequisitionPRCEntity oEntity, string SupplierCode, string PONo, object mngrclassobj, List<PurchaseRequisitionPRCGridEntity> objWSGridEntity,
        //      bool bDocAutogenerated, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber)
        //{

        //    string sRetun = string.Empty;
        //    // string sApprStatus = string.Empty;
        //    try
        //    {
        //        OracleHelper oTrns = new OracleHelper();
        //        SessionManager mngrclass = (SessionManager)mngrclassobj;

        //        sSQLArray.Clear();

        //        DataSet DtlData = default(DataSet);
        //        DataTable DtlDataTbl = default(DataTable);
        //        DataTable DtlPayterms = default(DataTable);
        //        DataRow DtlDr = default(DataRow);
        //        string DtlCnd = null;
        //        double grandttotal = 0;
        //        SqlHelper clsSQLHelper = new SqlHelper();
        //        DataTable dtPOTotQty = new DataTable();
        //        double UPricePo = 0;
        //        string paytermCode = string.Empty;
        //        string payterms = string.Empty;
        //        int slno = 0;

        //        DtlPayterms = SupplierPayterms(SupplierCode);
        //        if (DtlPayterms.Rows.Count > 0)
        //        {
        //            payterms = DtlPayterms.Rows[0]["SALES_PAY_TYPE_DESC"].ToString();
        //            paytermCode = DtlPayterms.Rows[0]["RM_VM_CREDIT_TERMS"].ToString(); 
        //        }

        //        DataTable dtData = new DataTable();
        //        double TaxPer = 0;
        //        int TaxAppFinYR = 0;

        //        TaxAppFinYR = FillTaxSetup();

        //        if (mngrclass.FinYearID > TaxAppFinYR)
        //        {

        //            dtData = SupplierData(SupplierCode);

        //            if (dtData.Rows.Count > 0)
        //            {
        //                TaxPer = Convert.ToDouble(dtData.Rows[0]["RM_VM_TAX_VAT_PERCENTAGE"].ToString());
        //            }
        //        }
        //        else
        //        {

        //            TaxPer = 0;
        //        }


        //        SQL = " SELECT RM_PR_DETAILS.RM_RMM_RM_CODE,";
        //        SQL = SQL + " (RM_PR_DETAILS.RM_PRD_DIR_USE_QTY  + RM_PR_DETAILS.RM_PRD_INV_QTY )POQTY,";
        //        SQL = SQL + " RM_PRREQN_QTN.RM_PRQ_UNIT_PRICE,RM_PRREQN_QTN.RM_PRQ_DISCOUNT,";
        //        SQL = SQL + " RM_PR_DETAILS.RM_UOM_UOM_CODE,";
        //        SQL = SQL + " ( ( RM_PR_DETAILS.RM_PRD_DIR_USE_QTY + RM_PR_DETAILS.RM_PRD_INV_QTY ) * (RM_PRREQN_QTN.RM_PRQ_UNIT_PRICE-RM_PRREQN_QTN.RM_PRQ_DISCOUNT) ) TOTALAMOUNT   ,";
        //        SQL = SQL + "   RM_IM_ITEM_DTL_DESCRIPTION DTL_DESCRIPTION,RM_PR_DETAILS.SALES_STN_STATION_CODE, HR_DEPT_DEPT_CODE,RM_PR_DETAILS.RM_SM_SOURCE_CODE,PC_BUD_BUDGET_ITEM_CODE  ,RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE ";
        //        SQL = SQL + " FROM RM_PR_DETAILS, RM_PRREQN_QTN, RM_RAWMATERIAL_MASTER";
        //        SQL = SQL + " WHERE  ";
        //        SQL = SQL + " RM_PRREQN_QTN.RM_PRQ_APPROVED = 'Y'";
        //        SQL = SQL + " AND RM_PRREQN_QTN.RM_RMM_RM_CODE = RM_PR_DETAILS.RM_RMM_RM_CODE";
        //        SQL = SQL + " AND RM_PR_DETAILS.RM_PR_PRNO = RM_PRREQN_QTN.RM_PR_PRNO";
        //        SQL = SQL + " AND RM_PR_DETAILS.RM_RMM_RM_CODE =    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
        //        SQL = SQL + " AND RM_PRREQN_QTN.RM_PRQ_SLNO = RM_PR_DETAILS.RM_PRD_SL_NO";
        //        SQL = SQL + " AND  (RM_PRREQN_QTN.RM_PO_PONO = ''   OR RM_PRREQN_QTN.RM_PO_PONO IS NULL) ";
        //        SQL = SQL + " AND  RM_PRREQN_QTN.RM_VM_VENDOR_CODE='" + SupplierCode + "' AND RM_PR_DETAILS.RM_PR_PRNO='" + oEntity.PRNo + "' AND RM_PRREQN_QTN.AD_FIN_FINYRID=" + mngrclass.FinYearID + "";

        //        DtlData = clsSQLHelper.GetDataset(SQL);


        //        int J = 0;
        //        int K = 0;

        //        double uprice = 0;

        //        for (J = 0; J <= DtlData.Tables[0].Rows.Count - 1; J++)
        //        {
        //            slno++;

        //            grandttotal = grandttotal + Convert.ToDouble(DtlData.Tables[0].Rows[J]["TOTALAMOUNT"].ToString());

        //            uprice = Convert.ToDouble(DtlData.Tables[0].Rows[J]["RM_PRQ_UNIT_PRICE"].ToString()) - Convert.ToDouble(DtlData.Tables[0].Rows[J]["RM_PRQ_DISCOUNT"].ToString());

        //            SQL = " INSERT INTO RM_PO_PROJECT_DETAILS (";
        //            SQL = SQL + " RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
        //            SQL = SQL + " RM_RMM_RM_CODE, RM_POD_SL_NO, SALES_STN_STATION_CODE, ";
        //            SQL = SQL + " HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE,  RM_IM_ITEM_DTL_DESCRIPTION, ";
        //            SQL = SQL + " RM_POD_QTY, RM_POD_UNIT_PRICE, RM_UOM_UOM_CODE, ";
        //            SQL = SQL + " RM_POD_DISCOUNT_PERCENT, RM_POD_DISCOUNT_AMOUNT, RM_POD_TOTALAMT, ";
        //            SQL = SQL + " RM_POD_PENDING_QTY, RM_POD_NEWPRICE,RM_POD_STATUS,PC_BUD_BUDGET_ITEM_CODE,GL_COSM_ACCOUNT_CODE  )";
        //            SQL = SQL + " VALUES ('" + PONo + "' ,'" + mngrclass.CompanyCode + "' ," + mngrclass.FinYearID + " ,";
        //            SQL = SQL + " '" + DtlData.Tables[0].Rows[J]["RM_RMM_RM_CODE"].ToString() + "' , " + slno + ",  '" + DtlData.Tables[0].Rows[J]["SALES_STN_STATION_CODE"].ToString() + "',";
        //            SQL = SQL + " '" + DtlData.Tables[0].Rows[J]["HR_DEPT_DEPT_CODE"].ToString() + "'  ,'" + DtlData.Tables[0].Rows[J]["RM_SM_SOURCE_CODE"].ToString() + "' ,'" + DtlData.Tables[0].Rows[J]["DTL_DESCRIPTION"].ToString() + "' ,";
        //            SQL = SQL + " " + DtlData.Tables[0].Rows[J]["POQTY"].ToString() + " , " + uprice + " , '" + DtlData.Tables[0].Rows[J]["RM_UOM_UOM_CODE"].ToString() + "' ,";
        //            SQL = SQL + " 0, 0, " + DtlData.Tables[0].Rows[J]["TOTALAMOUNT"].ToString() + ",";
        //            SQL = SQL + " " + DtlData.Tables[0].Rows[J]["POQTY"].ToString() + " ," + uprice + " ,'O','" + DtlData.Tables[0].Rows[J]["PC_BUD_BUDGET_ITEM_CODE"].ToString() + "' ,'" + DtlData.Tables[0].Rows[J]["GL_COSM_ACCOUNT_CODE"].ToString() + "' )";

        //            sSQLArray.Add(SQL);


        //            //Update Items in PR with the newly generated PO 
        //            //_______________________________________________
        //            SQL = " UPDATE RM_PRREQN_QTN";
        //            SQL = SQL + "  SET RM_PO_PONO = '" + PONo + "',RM_PO_PO_FINYEAR_ID = " + mngrclass.FinYearID + "";
        //            SQL = SQL + "  WHERE RM_RMM_RM_CODE = '" + DtlData.Tables[0].Rows[J]["RM_RMM_RM_CODE"].ToString() + "'";
        //            SQL = SQL + "  AND rm_vm_vendor_code = '" + SupplierCode + "'";
        //            SQL = SQL + "  AND RM_PRQ_APPROVED = 'Y'";
        //            SQL = SQL + "  AND RM_PRQ_DELETESTATUS = 0";
        //            SQL = SQL + "  AND ad_fin_finyrid = " + mngrclass.FinYearID;
        //            SQL = SQL + "  AND RM_PR_PRNO = '" + oEntity.PRNo + "'";

        //            sSQLArray.Add(SQL);
        //            //_______________________________________________
        //        }

        //        //dtPOTotQty=fnPOTotQty(PONo,mngrclass.FinYearID,DtlData.Tables[0].Rows[J]["RM_RMM_RM_CODE"].ToString(),DtlData.Tables[0].Rows[J]["RM_SM_SOURCE_CODE"].ToString())

        //        //UPricePo=0;

        //        //if(Convert.ToDouble(dtPOTotQty.Rows[0]["RM_POD_QTY"].ToString())>0)
        //        //{
        //        //   UPricePo=Convert.ToDouble(dtPOTotQty.Rows[0]["RM_POD_TOTALAMT"].ToString()) /Convert.ToDouble(dtPOTotQty.Rows[0]["RM_POD_QTY"].ToString());
        //        //}
        //        //else
        //        //{

        //        //    UPricePo=0;
        //        //}



        //        SQL = " INSERT INTO RM_PO_DETAILS (";
        //        SQL = SQL + "  RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
        //        SQL = SQL + "  RM_RMM_RM_CODE, RM_POD_SL_NO, SALES_STN_STATION_CODE, ";
        //        SQL = SQL + "  HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE,  RM_IM_ITEM_DTL_DESCRIPTION, ";
        //        SQL = SQL + "  RM_POD_QTY, RM_POD_UNIT_PRICE, RM_UOM_UOM_CODE, ";
        //        SQL = SQL + "  RM_POD_DISCOUNT_PERCENT, RM_POD_DISCOUNT_AMOUNT, RM_POD_TOTALAMT, ";
        //        SQL = SQL + "  RM_POD_PENDING_QTY, RM_POD_NEWPRICE, RM_POD_STATUS,PC_BUD_BUDGET_ITEM_CODE )";
        //        SQL = SQL + "  select ";
        //        SQL = SQL + "  RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
        //        SQL = SQL + "  RM_RMM_RM_CODE, row_number() over (order by null) AS   RM_POD_SL_NO, SALES_STN_STATION_CODE, ";
        //        SQL = SQL + "  HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE,  RM_IM_ITEM_DTL_DESCRIPTION, ";
        //        SQL = SQL + "  sum(RM_POD_QTY) RM_POD_QTY, ";
        //        SQL = SQL + "   CASE SUM (RM_POD_QTY)";
        //        SQL = SQL + "     WHEN 0 ";
        //        SQL = SQL + "       THEN 0";
        //        SQL = SQL + "    ELSE (SUM (RM_POD_TOTALAMT) / SUM (RM_POD_QTY))";
        //        SQL = SQL + "   END AS RM_POD_UNIT_PRICE,";
        //        SQL = SQL + "   RM_UOM_UOM_CODE, ";
        //        SQL = SQL + "  0 RM_POD_DISCOUNT_PERCENT, 0 RM_POD_DISCOUNT_AMOUNT, sum(RM_POD_TOTALAMT) RM_POD_TOTALAMT, ";
        //        SQL = SQL + "  sum(RM_POD_PENDING_QTY)RM_POD_PENDING_QTY,";
        //        SQL = SQL + "   CASE SUM (RM_POD_QTY)";
        //        SQL = SQL + "     WHEN 0 ";
        //        SQL = SQL + "      THEN 0";
        //        SQL = SQL + "    ELSE (SUM (RM_POD_TOTALAMT) / SUM (RM_POD_QTY))";
        //        SQL = SQL + "   END AS RM_POD_NEWPRICE,";
        //        SQL = SQL + "  'O',PC_BUD_BUDGET_ITEM_CODE";
        //        SQL = SQL + "  from RM_PO_PROJECT_DETAILS ";
        //        SQL = SQL + "  WHERE  RM_PO_PONO ='" + PONo + "' ";
        //        SQL = SQL + "  AND   AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";
        //        SQL = SQL + "  group by  RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
        //        SQL = SQL + "  RM_RMM_RM_CODE, SALES_STN_STATION_CODE, ";
        //        SQL = SQL + "  HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE, RM_IM_ITEM_DTL_DESCRIPTION,";
        //        SQL = SQL + "  RM_UOM_UOM_CODE,PC_BUD_BUDGET_ITEM_CODE";


        //        sSQLArray.Add(SQL);



        //        double TaxAmount = 0;

        //        double NetTotal = 0;

        //        if (TaxPer > 0)
        //        {
        //            TaxAmount = (grandttotal * TaxPer) / 100;

        //            NetTotal = grandttotal + TaxAmount;
        //        }
        //        else
        //        {
        //            NetTotal = grandttotal;
        //        }


        //        SQL = " INSERT INTO RM_PO_MASTER (";
        //        SQL = SQL + " RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
        //        SQL = SQL + " RM_PO_PO_DATE, RM_VM_VENDOR_CODE, RM_PO_PRICETYPE,RM_PO_EXP_DATE, ";
        //        SQL = SQL + " RM_PO_GRAND_TOTAL, RM_PO_DISCOUNT_PERC, RM_PO_DISCOUNT_AMOUNT, ";
        //        SQL = SQL + " RM_PO_NET_AMOUNT, RM_PO_PO_STATUS, RM_PO_REMARKS, ";
        //        SQL = SQL + " AD_CUR_CURRENCY_CODE, RM_PO_EXCHANGE_RATE, RM_PO_APPROVED, ";
        //        SQL = SQL + " RM_PO_APPROVEDBY, RM_PO_PURENTRY, RM_PO_POTYPE, ";
        //        SQL = SQL + " RM_PO_CANCEL_REMARKS, RM_PO_CONTAC1_CODE, RM_PO_CONTAC2_CODE, ";
        //        SQL = SQL + " RM_PR_PRNO, RM_PR_FIN_FINYRID, RM_PO_SUPP_REF, ";
        //        SQL = SQL + " RM_PO_SUPP_REF_DATE, RM_PO_OUR_REF, RM_PO_CREATEDBY, ";
        //        SQL = SQL + " RM_PO_CREATEDDATE, RM_PO_UPDATEDBY, RM_PO_UPDATEDDATE, ";
        //        SQL = SQL + " RM_PO_DELETESTATUS,RM_PO_COST_CENTER,RM_MPOM_PAYMENT_TERMS,AD_BR_CODE, ";
        //        SQL = SQL + " RM_PO_VAT_PERCENTAGE,RM_PO_VAT_AMOUNT  ,RM_VM_CREDIT_TERMS,RM_PO_QTY_RATE_TYPE,HR_DEPT_DEPT_CODE, ";
        //        SQL = SQL + " RM_PO_AGG_START_DATE,RM_PO_AGG_END_DATE,RM_PO_STOCK_TYPE_CODE) ";

        //        SQL = SQL + " VALUES ('" + PONo + "' ,'" + mngrclass.CompanyCode + "' ," + mngrclass.FinYearID + " ,";
        //        SQL = SQL + " '" + DateTime.Now.ToString("dd-MMM-yyyy") + "' ,'" + SupplierCode + "' ,'EX-FACTORY','" + DateTime.Now.ToString("dd-MMM-yyyy") + "'  ,";
        //        SQL = SQL + " " + Convert.ToDouble(grandttotal) + " ,0,0 ,";
        //        SQL = SQL + " " + Convert.ToDouble(NetTotal) + "  ,'O' ,'' ,";
        //        SQL = SQL + "'" + mngrclass.CurrencyCodeBase + "' ," + 1 + " ,'N',";
        //        SQL = SQL + " '','N' ,'CREDIT',";
        //        SQL = SQL + " '', '" + oEntity.Contact1 + "','',";
        //        SQL = SQL + "'" + oEntity.PRNo + "' ," + mngrclass.FinYearID + " ,'' ,";
        //        SQL = SQL + "'' ,'" + oEntity.Physical + "' , '" + mngrclass.UserName + "' ,";
        //        SQL = SQL + " TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'), '','' ,";
        //        SQL = SQL + "0 ,'" + oEntity.CostCenter + "','" + payterms + "', '" + oEntity.Branch + "' ,";
        //        SQL = SQL + " " + Convert.ToDouble(TaxPer) + "," + Convert.ToDouble(TaxAmount) + ",'"+ paytermCode + "','Q','',sysdate,sysdate,'"+ oEntity.StockType +"' )";

        //        sSQLArray.Add(SQL);

        //        SQL = " INSERT INTO RM_PO_NOTES ( ";
        //        SQL = SQL + "RM_PO_PONO, AD_FIN_FINYRID, RM_PO_SL_NO,  ";
        //        SQL = SQL + "   RM_PO_NOTES, RM_PO_PRINT_YN, RM_PO_DELSTATUS ";
        //        SQL = SQL + " ) ";

        //        SQL = SQL + "select '" + PONo + "' RM_PO_PONO," + mngrclass.FinYearID + " AD_FIN_FINYRID,";
        //        SQL = SQL + " RM_PO_DEFAULTS_SL_NO,RM_PO_DEFAULTS_TEXT,'Y' RM_PO_PRINT_YN,0 RM_PO_DELSTATUS from RM_DEFAULTS_PO_TEMRS ";
        //        SQL = SQL + "where RM_PO_DELSTATUS=0 order by  RM_PO_DEFAULTS_SL_NO asc ";
        //        //SQL = SQL + "VALUES ( '" + QTNNo + "'," + mngrclass.FinYearID + ", " + Data.SlNo + ", ";
        //        //SQL = SQL + " '" + Data.sText + "', '" + Data.sPrintYN + "' ,0 ) ";

        //        sSQLArray.Add(SQL);

        //        //sRetun = string.Empty;
        //        //sRetun = InsertPOApprovalMailSQL(PONo, mngrclassobj);
        //        //if (sRetun != "CONTINUE")
        //        //{
        //        //    return sRetun;
        //        //}

        //        sRetun = string.Empty;
        //        sRetun = ModifyPRStatus(oEntity.PRNo, objWSGridEntity, mngrclassobj);
        //        if (sRetun != "CONTINUE")
        //        {
        //            return sRetun;
        //        }

        //        sRetun = string.Empty;
        //        // WTPO should be teh tag // purchase order ws else the dc number will nt increase // jomy  RTMO 
        //        sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "WTPO", PONo, true, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);

        //    }
        //    catch (Exception ex)
        //    {
        //        sRetun = System.Convert.ToString(ex.Message);

        //        return sRetun;
        //    }
        //    finally
        //    {
        //        // ocConn.Close();
        //        // ocConn.Dispose();
        //    }
        //    return sRetun;
        //}


        ////public string GeneratePO ( PurchaseRequisitionPRCEntity oEntity, string SupplierCode, string PONo, object mngrclassobj, List<PurchaseRequisitionPRCGridEntity> objWSGridEntity,
        ////      bool bDocAutogenerated, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber )
        ////{

        ////    string sRetun = string.Empty;
        ////    // string sApprStatus = string.Empty;
        ////    try
        ////    {
        ////        OracleHelper oTrns = new OracleHelper();
        ////        SessionManager mngrclass = (SessionManager)mngrclassobj;

        ////        sSQLArray.Clear();

        ////        DataSet DtlData = default(DataSet);
        ////        DataTable DtlDataTbl = default(DataTable);
        ////        DataTable DtlPayterms = default(DataTable);
        ////        DataRow DtlDr = default(DataRow);
        ////        string DtlCnd = null;
        ////        double grandttotal = 0;
        ////        double TotalAmountBeforeTax = 0;
        ////        double TotalAfterTax = 0;
        ////        double TaxAmountInGrid = 0;
        ////        double TaxAmount = 0;
        ////        double NetTotal = 0;
        ////        SqlHelper clsSQLHelper = new SqlHelper();
        ////        DataTable dtPOTotQty = new DataTable();
        ////        double UPricePo = 0;
        ////        string paytermCode = string.Empty;
        ////        string payterms = string.Empty;
        ////        int slno = 0;

        ////        DtlPayterms = SupplierPayterms(SupplierCode);
        ////        if (DtlPayterms.Rows.Count > 0)
        ////        {
        ////            payterms = DtlPayterms.Rows[0]["SALES_PAY_TYPE_DESC"].ToString();
        ////            paytermCode = DtlPayterms.Rows[0]["RM_VM_CREDIT_TERMS"].ToString();
        ////        }

        ////        DataTable dtData = new DataTable();
        ////        double TaxPercentage = 0;
        ////        double TaxPer = 0;
        ////        int TaxAppFinYR = 0;
        ////        string TaxType = string.Empty;

        ////        TaxAppFinYR = FillTaxSetup();

        ////        if (mngrclass.FinYearID > TaxAppFinYR)
        ////        {

        ////            dtData = SupplierData(SupplierCode);

        ////            if (dtData.Rows.Count > 0)
        ////            {
        ////                TaxPercentage = Convert.ToDouble(dtData.Rows[0]["RM_VM_TAX_VAT_PERCENTAGE"].ToString());
        ////                TaxPer = Convert.ToDouble(dtData.Rows[0]["RM_VM_TAX_VAT_PERCENTAGE"].ToString());
        ////                TaxType = dtData.Rows[0]["GL_TAX_TYPE_CODE"].ToString();
        ////            }
        ////        }
        ////        else
        ////        {

        ////            TaxPer = 0;
        ////        }


        ////        SQL = " SELECT RM_PR_DETAILS.RM_RMM_RM_CODE,";
        ////        SQL = SQL + " (RM_PR_DETAILS.RM_PRD_DIR_USE_QTY  + RM_PR_DETAILS.RM_PRD_INV_QTY )POQTY,";
        ////        SQL = SQL + " RM_PRREQN_QTN.RM_PRQ_UNIT_PRICE,RM_PRREQN_QTN.RM_PRQ_DISCOUNT,";
        ////        SQL = SQL + " RM_PR_DETAILS.RM_UOM_UOM_CODE,";
        ////        SQL = SQL + " ( ( RM_PR_DETAILS.RM_PRD_DIR_USE_QTY + RM_PR_DETAILS.RM_PRD_INV_QTY ) * (RM_PRREQN_QTN.RM_PRQ_UNIT_PRICE-RM_PRREQN_QTN.RM_PRQ_DISCOUNT) ) TOTALAMOUNT   ,";
        ////        SQL = SQL + "   RM_IM_ITEM_DTL_DESCRIPTION DTL_DESCRIPTION,RM_PR_DETAILS.SALES_STN_STATION_CODE, HR_DEPT_DEPT_CODE,RM_PR_DETAILS.RM_SM_SOURCE_CODE,PC_BUD_BUDGET_ITEM_CODE  ,RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE,RM_RMM_VAT_APPLICABLE_YN ";
        ////        SQL = SQL + " FROM RM_PR_DETAILS, RM_PRREQN_QTN, RM_RAWMATERIAL_MASTER";
        ////        SQL = SQL + " WHERE  ";
        ////        SQL = SQL + " RM_PRREQN_QTN.RM_PRQ_APPROVED = 'Y'";
        ////        SQL = SQL + " AND RM_PRREQN_QTN.RM_RMM_RM_CODE = RM_PR_DETAILS.RM_RMM_RM_CODE";
        ////        SQL = SQL + " AND RM_PR_DETAILS.RM_PR_PRNO = RM_PRREQN_QTN.RM_PR_PRNO";
        ////        SQL = SQL + " AND RM_PR_DETAILS.RM_RMM_RM_CODE =    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
        ////        SQL = SQL + " AND RM_PRREQN_QTN.RM_PRQ_SLNO = RM_PR_DETAILS.RM_PRD_SL_NO";
        ////        SQL = SQL + " AND  (RM_PRREQN_QTN.RM_PO_PONO = ''   OR RM_PRREQN_QTN.RM_PO_PONO IS NULL) ";
        ////        SQL = SQL + " AND  RM_PRREQN_QTN.RM_VM_VENDOR_CODE='" + SupplierCode + "' AND RM_PR_DETAILS.RM_PR_PRNO='" + oEntity.PRNo + "' AND RM_PRREQN_QTN.AD_FIN_FINYRID=" + mngrclass.FinYearID + "";

        ////        DtlData = clsSQLHelper.GetDataset(SQL);


        ////        int J = 0;
        ////        int K = 0;

        ////        double uprice = 0;

        ////        for (J = 0; J <= DtlData.Tables[0].Rows.Count - 1; J++)
        ////        {
        ////            slno++;

        ////            grandttotal = grandttotal + Convert.ToDouble(DtlData.Tables[0].Rows[J]["TOTALAMOUNT"].ToString());

        ////            if (DtlData.Tables[0].Rows[J]["RM_RMM_VAT_APPLICABLE_YN"].ToString() == "Y")
        ////            {
        ////                TotalAmountBeforeTax = Convert.ToDouble(DtlData.Tables[0].Rows[J]["TOTALAMOUNT"].ToString());
        ////                TaxAmountInGrid = (TotalAmountBeforeTax * TaxPer) / 100;
        ////                TotalAfterTax = TotalAmountBeforeTax + TaxAmountInGrid;

        ////            }
        ////            else
        ////            {
        ////                TotalAmountBeforeTax = Convert.ToDouble(DtlData.Tables[0].Rows[J]["TOTALAMOUNT"].ToString());
        ////                TaxAmountInGrid = 0;
        ////                TotalAfterTax = TotalAmountBeforeTax + TaxAmountInGrid;
        ////                TaxPer = 0;
        ////                TaxType = "V0";

        ////            }
        ////            TaxAmount = TaxAmount + ((TotalAmountBeforeTax * TaxPer) / 100);



        ////            uprice = Convert.ToDouble(DtlData.Tables[0].Rows[J]["RM_PRQ_UNIT_PRICE"].ToString()) - Convert.ToDouble(DtlData.Tables[0].Rows[J]["RM_PRQ_DISCOUNT"].ToString());

        ////            SQL = " INSERT INTO RM_PO_PROJECT_DETAILS (";
        ////            SQL = SQL + " RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
        ////            SQL = SQL + " RM_RMM_RM_CODE, RM_POD_SL_NO, SALES_STN_STATION_CODE, ";
        ////            SQL = SQL + " HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE,  RM_IM_ITEM_DTL_DESCRIPTION, ";
        ////            SQL = SQL + " RM_POD_QTY, RM_POD_UNIT_PRICE, RM_UOM_UOM_CODE, ";
        ////            SQL = SQL + " RM_POD_DISCOUNT_PERCENT, RM_POD_DISCOUNT_AMOUNT,RM_POD_RM_AMOUNT_BFR_VAT, RM_POD_TOTALAMT,RM_POD_RM_VAT_AMOUNT, ";
        ////            SQL = SQL + " RM_POD_PENDING_QTY, RM_POD_NEWPRICE,RM_POD_STATUS,PC_BUD_BUDGET_ITEM_CODE,GL_COSM_ACCOUNT_CODE,RM_POD_RM_VAT_RATE,";
        ////            SQL = SQL + "  RM_POD_RM_VAT_TYPE , "; "  

        ////            SQL = SQL + "RM_POD_UNIT_PRICE_FC   , ";
        ////            SQL = SQL + "RM_POD_DISCOUNT_PERCENT_FC   , ";
        ////            SQL = SQL + "RM_POD_DISCOUNT_AMOUNT_FC  , ";
        ////            SQL = SQL + "RM_POD_TOTALAMT_FC     , ";
        ////            SQL = SQL + "RM_POD_NEWPRICE_FC  , ";
        ////            SQL = SQL + "RM_POD_RM_AMOUNT_BFR_VAT_FC , ";
        ////            SQL = SQL + "RM_POD_RM_VAT_RATE_FC  , ";
        ////            SQL = SQL + "RM_POD_RM_VAT_AMOUNT_FC   ";

        ////            SQL = SQL + " )";
        ////            SQL = SQL + " VALUES ('" + PONo + "' ,'" + mngrclass.CompanyCode + "' ," + mngrclass.FinYearID + " ,";
        ////            SQL = SQL + " '" + DtlData.Tables[0].Rows[J]["RM_RMM_RM_CODE"].ToString() + "' , " + slno + ",  '" + DtlData.Tables[0].Rows[J]["SALES_STN_STATION_CODE"].ToString() + "',";
        ////            SQL = SQL + " '" + DtlData.Tables[0].Rows[J]["HR_DEPT_DEPT_CODE"].ToString() + "'  ,'" + DtlData.Tables[0].Rows[J]["RM_SM_SOURCE_CODE"].ToString() + "' ,'" + DtlData.Tables[0].Rows[J]["DTL_DESCRIPTION"].ToString() + "' ,";
        ////            SQL = SQL + " " + DtlData.Tables[0].Rows[J]["POQTY"].ToString() + " , " + uprice + " , '" + DtlData.Tables[0].Rows[J]["RM_UOM_UOM_CODE"].ToString() + "' ,";
        ////            SQL = SQL + " 0, 0, " + DtlData.Tables[0].Rows[J]["TOTALAMOUNT"].ToString() + ",'" + TotalAfterTax.ToString() + "','" + TaxAmountInGrid.ToString() + "',";
        ////            SQL = SQL + " " + DtlData.Tables[0].Rows[J]["POQTY"].ToString() + " ," + uprice + " ,'O','" + DtlData.Tables[0].Rows[J]["PC_BUD_BUDGET_ITEM_CODE"].ToString() + "' ,'" + DtlData.Tables[0].Rows[J]["GL_COSM_ACCOUNT_CODE"].ToString() + "', ";
        ////            SQL = SQL + "" + Convert.ToDouble(TaxPer) + ",'" + TaxType + "' )";
        ////            sSQLArray.Add(SQL);


        ////            //Update Items in PR with the newly generated PO 
        ////            //_______________________________________________
        ////            SQL = " UPDATE RM_PRREQN_QTN";
        ////            SQL = SQL + "  SET RM_PO_PONO = '" + PONo + "',RM_PO_PO_FINYEAR_ID = " + mngrclass.FinYearID + "";
        ////            SQL = SQL + "  WHERE RM_RMM_RM_CODE = '" + DtlData.Tables[0].Rows[J]["RM_RMM_RM_CODE"].ToString() + "'";
        ////            SQL = SQL + "  AND rm_vm_vendor_code = '" + SupplierCode + "'";
        ////            SQL = SQL + "  AND RM_PRQ_APPROVED = 'Y'";
        ////            SQL = SQL + "  AND RM_PRQ_DELETESTATUS = 0";
        ////            SQL = SQL + "  AND ad_fin_finyrid = " + mngrclass.FinYearID;
        ////            SQL = SQL + "  AND RM_PR_PRNO = '" + oEntity.PRNo + "'";

        ////            sSQLArray.Add(SQL);
        ////            //_______________________________________________
        ////        }
        ////        NetTotal = grandttotal + TaxAmount;

        ////        SQL = " INSERT INTO RM_PO_DETAILS (";
        ////        SQL = SQL + "  RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
        ////        SQL = SQL + "  RM_RMM_RM_CODE, RM_POD_SL_NO, SALES_STN_STATION_CODE, ";
        ////        SQL = SQL + "  HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE,  RM_IM_ITEM_DTL_DESCRIPTION, ";
        ////        SQL = SQL + "  RM_POD_QTY, RM_POD_UNIT_PRICE, RM_UOM_UOM_CODE, ";
        ////        SQL = SQL + "  RM_POD_DISCOUNT_PERCENT, RM_POD_DISCOUNT_AMOUNT,RM_POD_RM_AMOUNT_BFR_VAT, RM_POD_TOTALAMT,RM_POD_RM_VAT_AMOUNT, ";
        ////        SQL = SQL + "  RM_POD_PENDING_QTY, RM_POD_NEWPRICE, RM_POD_STATUS,PC_BUD_BUDGET_ITEM_CODE, ";
        ////        SQL = SQL + "  RM_POD_RM_VAT_RATE,RM_POD_RM_VAT_TYPE)";
        ////        SQL = SQL + "  select ";
        ////        SQL = SQL + "  RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
        ////        SQL = SQL + "  RM_RMM_RM_CODE, row_number() over (order by null) AS   RM_POD_SL_NO, SALES_STN_STATION_CODE, ";
        ////        SQL = SQL + "  HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE,  RM_IM_ITEM_DTL_DESCRIPTION, ";
        ////        SQL = SQL + "  sum(RM_POD_QTY) RM_POD_QTY, ";
        ////        SQL = SQL + "   CASE SUM (RM_POD_QTY)";
        ////        SQL = SQL + "     WHEN 0 ";
        ////        SQL = SQL + "       THEN 0";
        ////        SQL = SQL + "    ELSE (SUM (RM_POD_TOTALAMT) / SUM (RM_POD_QTY))";
        ////        SQL = SQL + "   END AS RM_POD_UNIT_PRICE,";
        ////        SQL = SQL + "   RM_UOM_UOM_CODE, ";
        ////        SQL = SQL + "  0 RM_POD_DISCOUNT_PERCENT, 0 RM_POD_DISCOUNT_AMOUNT,sum(RM_POD_RM_AMOUNT_BFR_VAT) RM_POD_RM_AMOUNT_BFR_VAT,sum(RM_POD_TOTALAMT)  RM_POD_TOTALAMT,sum(RM_POD_RM_VAT_AMOUNT) RM_POD_RM_VAT_AMOUNT, ";
        ////        SQL = SQL + "  sum(RM_POD_PENDING_QTY)RM_POD_PENDING_QTY,";
        ////        SQL = SQL + "   CASE SUM (RM_POD_QTY)";
        ////        SQL = SQL + "     WHEN 0 ";
        ////        SQL = SQL + "      THEN 0";
        ////        SQL = SQL + "    ELSE (SUM (RM_POD_TOTALAMT) / SUM (RM_POD_QTY))";
        ////        SQL = SQL + "   END AS RM_POD_NEWPRICE,";
        ////        SQL = SQL + "  'O',PC_BUD_BUDGET_ITEM_CODE,RM_POD_RM_VAT_RATE,RM_POD_RM_VAT_TYPE";
        ////        SQL = SQL + "  from RM_PO_PROJECT_DETAILS ";
        ////        SQL = SQL + "  WHERE  RM_PO_PONO ='" + PONo + "' ";
        ////        SQL = SQL + "  AND   AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";
        ////        SQL = SQL + "  group by  RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
        ////        SQL = SQL + "  RM_RMM_RM_CODE, SALES_STN_STATION_CODE, ";
        ////        SQL = SQL + "  HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE, RM_IM_ITEM_DTL_DESCRIPTION,";
        ////        SQL = SQL + "  RM_UOM_UOM_CODE,PC_BUD_BUDGET_ITEM_CODE,RM_POD_RM_VAT_RATE,RM_POD_RM_VAT_TYPE";


        ////        sSQLArray.Add(SQL);


        ////        //if (TaxPer > 0)
        ////        //{
        ////        //    TaxAmount = (grandttotal * TaxPer) / 100;

        ////        //    NetTotal = grandttotal + TaxAmount;
        ////        //}
        ////        //else
        ////        //{
        ////        //    NetTotal = grandttotal;
        ////        //}


        ////        SQL = " INSERT INTO RM_PO_MASTER (";
        ////        SQL = SQL + " RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
        ////        SQL = SQL + " RM_PO_PO_DATE, RM_VM_VENDOR_CODE, RM_PO_PRICETYPE,RM_PO_EXP_DATE, ";
        ////        SQL = SQL + " RM_PO_GRAND_TOTAL, RM_PO_DISCOUNT_PERC, RM_PO_DISCOUNT_AMOUNT, ";
        ////        SQL = SQL + " RM_PO_NET_AMOUNT, RM_PO_PO_STATUS, RM_PO_REMARKS, ";
        ////        SQL = SQL + " AD_CUR_CURRENCY_CODE, RM_PO_EXCHANGE_RATE, RM_PO_APPROVED, ";
        ////        SQL = SQL + " RM_PO_APPROVEDBY, RM_PO_PURENTRY, RM_PO_POTYPE, ";
        ////        SQL = SQL + " RM_PO_CANCEL_REMARKS, RM_PO_CONTAC1_CODE, RM_PO_CONTAC2_CODE, ";
        ////        SQL = SQL + " RM_PR_PRNO, RM_PR_FIN_FINYRID, RM_PO_SUPP_REF, ";
        ////        SQL = SQL + " RM_PO_SUPP_REF_DATE, RM_PO_OUR_REF, RM_PO_CREATEDBY, ";
        ////        SQL = SQL + " RM_PO_CREATEDDATE, RM_PO_UPDATEDBY, RM_PO_UPDATEDDATE, ";
        ////        SQL = SQL + " RM_PO_DELETESTATUS,RM_PO_COST_CENTER,RM_MPOM_PAYMENT_TERMS,AD_BR_CODE, ";
        ////        SQL = SQL + " RM_PO_VAT_PERCENTAGE,RM_PO_VAT_AMOUNT  ,RM_VM_CREDIT_TERMS,RM_PO_QTY_RATE_TYPE,HR_DEPT_DEPT_CODE, ";
        ////        SQL = SQL + " RM_PO_AGG_START_DATE,RM_PO_AGG_END_DATE,RM_PO_STOCK_TYPE_CODE) ";

        ////        SQL = SQL + " VALUES ('" + PONo + "' ,'" + mngrclass.CompanyCode + "' ," + mngrclass.FinYearID + " ,";
        ////        SQL = SQL + " '" + DateTime.Now.ToString("dd-MMM-yyyy") + "' ,'" + SupplierCode + "' ,'EX-FACTORY','" + DateTime.Now.ToString("dd-MMM-yyyy") + "'  ,";
        ////        SQL = SQL + " " + Convert.ToDouble(grandttotal) + " ,0,0 ,";
        ////        SQL = SQL + " " + Convert.ToDouble(NetTotal) + "  ,'O' ,'' ,";
        ////        SQL = SQL + "'" + mngrclass.CurrencyCodeBase + "' ," + 1 + " ,'N',";
        ////        SQL = SQL + " '','N' ,'CREDIT',";
        ////        SQL = SQL + " '', '" + oEntity.Contact1 + "','',";
        ////        SQL = SQL + "'" + oEntity.PRNo + "' ," + mngrclass.FinYearID + " ,'' ,";
        ////        SQL = SQL + "'' ,'" + oEntity.Physical + "' , '" + mngrclass.UserName + "' ,";
        ////        SQL = SQL + " TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'), '','' ,";
        ////        SQL = SQL + "0 ,'" + oEntity.CostCenter + "','" + payterms + "', '" + oEntity.Branch + "' ,";
        ////        SQL = SQL + " " + Convert.ToDouble(TaxPercentage) + "," + Convert.ToDouble(TaxAmount) + ",'" + paytermCode + "','Q','',sysdate,sysdate,'" + oEntity.StockType + "' )";

        ////        sSQLArray.Add(SQL);

        ////        SQL = " INSERT INTO RM_PO_NOTES ( ";
        ////        SQL = SQL + "RM_PO_PONO, AD_FIN_FINYRID, RM_PO_SL_NO,  ";
        ////        SQL = SQL + "   RM_PO_NOTES, RM_PO_PRINT_YN, RM_PO_DELSTATUS ";
        ////        SQL = SQL + " ) ";

        ////        SQL = SQL + "select '" + PONo + "' RM_PO_PONO," + mngrclass.FinYearID + " AD_FIN_FINYRID,";
        ////        SQL = SQL + " RM_PO_DEFAULTS_SL_NO,RM_PO_DEFAULTS_TEXT,'Y' RM_PO_PRINT_YN,0 RM_PO_DELSTATUS from RM_DEFAULTS_PO_TEMRS ";
        ////        SQL = SQL + "where RM_PO_DELSTATUS=0 order by  RM_PO_DEFAULTS_SL_NO asc ";
        ////        //SQL = SQL + "VALUES ( '" + QTNNo + "'," + mngrclass.FinYearID + ", " + Data.SlNo + ", ";
        ////        //SQL = SQL + " '" + Data.sText + "', '" + Data.sPrintYN + "' ,0 ) ";

        ////        sSQLArray.Add(SQL);

        ////        //sRetun = string.Empty;
        ////        //sRetun = InsertPOApprovalMailSQL(PONo, mngrclassobj);
        ////        //if (sRetun != "CONTINUE")
        ////        //{
        ////        //    return sRetun;
        ////        //}

        ////        sRetun = string.Empty;
        ////        sRetun = ModifyPRStatus(oEntity.PRNo, objWSGridEntity, mngrclassobj);
        ////        if (sRetun != "CONTINUE")
        ////        {
        ////            return sRetun;
        ////        }

        ////        sRetun = string.Empty;
        ////        // WTPO should be teh tag // purchase order ws else the dc number will nt increase // jomy  RTMO 
        ////        sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "WTPO", PONo, true, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);

        ////    }
        ////    catch (Exception ex)
        ////    {
        ////        sRetun = System.Convert.ToString(ex.Message);

        ////        return sRetun;
        ////    }
        ////    finally
        ////    {
        ////        // ocConn.Close();
        ////        // ocConn.Dispose();
        ////    }
        ////    return sRetun;
        ////}
        /// <summary>
        /// 
        /// 
        public string GeneratePO ( PurchaseRequisitionPRCEntity oEntity, string SupplierCode, string PONo, object mngrclassobj, List<PurchaseRequisitionPRCGridEntity> objWSGridEntity,
              bool bDocAutogenerated, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber )
        {

            string sRetun = string.Empty;
            // string sApprStatus = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                DataSet DtlData = default(DataSet);
                DataTable DtlDataTbl = default(DataTable);
                DataTable DtlPayterms = default(DataTable);
                DataRow DtlDr = default(DataRow);
                string DtlCnd = null;
                double grandttotal = 0;
                double TotalAmountBeforeTax = 0;
                double TotalAfterTax = 0;
                double TaxAmountInGrid = 0;
                double TaxAmount = 0;
                double NetTotal = 0;
                SqlHelper clsSQLHelper = new SqlHelper();
                DataTable dtPOTotQty = new DataTable();
                double UPricePo = 0;
                string paytermCode = string.Empty;
                string payterms = string.Empty;
                int slno = 0;

                DtlPayterms = SupplierPayterms(SupplierCode, oEntity.Branch);
                if (DtlPayterms.Rows.Count > 0)
                {
                    payterms = DtlPayterms.Rows[0]["SALES_PAY_TYPE_DESC"].ToString();
                    paytermCode = DtlPayterms.Rows[0]["RM_VM_CREDIT_TERMS"].ToString();
                }

                DataTable dtData = new DataTable();
                double TaxPercentage = 0;
                double TaxPer = 0;
                int TaxAppFinYR = 0;
                string TaxType = string.Empty;

                TaxAppFinYR = FillTaxSetup();

                if (mngrclass.FinYearID > TaxAppFinYR)
                {

                    dtData = SupplierData(SupplierCode, oEntity.Branch);

                    if (dtData.Rows.Count > 0)
                    {
                        TaxPercentage = Convert.ToDouble(dtData.Rows[0]["RM_VM_TAX_VAT_PERCENTAGE"].ToString());
                        TaxPer = Convert.ToDouble(dtData.Rows[0]["RM_VM_TAX_VAT_PERCENTAGE"].ToString());
                        TaxType = dtData.Rows[0]["GL_TAX_TYPE_CODE"].ToString();
                    }
                }
                else
                {

                    TaxPer = 0;
                }


                SQL = " SELECT RM_PR_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " (RM_PR_DETAILS.RM_PRD_DIR_USE_QTY  + RM_PR_DETAILS.RM_PRD_INV_QTY )POQTY,";
                SQL = SQL + " RM_PRREQN_QTN.RM_PRQ_UNIT_PRICE,RM_PRREQN_QTN.RM_PRQ_DISCOUNT,";
                SQL = SQL + " RM_PR_DETAILS.RM_UOM_UOM_CODE,";
                SQL = SQL + " ( ( RM_PR_DETAILS.RM_PRD_DIR_USE_QTY + RM_PR_DETAILS.RM_PRD_INV_QTY ) * (RM_PRREQN_QTN.RM_PRQ_UNIT_PRICE-RM_PRREQN_QTN.RM_PRQ_DISCOUNT) ) TOTALAMOUNT   ,";
                SQL = SQL + "   RM_IM_ITEM_DTL_DESCRIPTION DTL_DESCRIPTION,RM_PR_DETAILS.SALES_STN_STATION_CODE, HR_DEPT_DEPT_CODE,RM_PR_DETAILS.RM_SM_SOURCE_CODE,PC_BUD_BUDGET_ITEM_CODE  ,RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE,RM_RMM_VAT_APPLICABLE_YN ";
                SQL = SQL + " FROM RM_PR_DETAILS, RM_PRREQN_QTN, RM_RAWMATERIAL_MASTER";
                SQL = SQL + " WHERE  ";
                SQL = SQL + " RM_PRREQN_QTN.RM_PRQ_APPROVED = 'Y'";
                SQL = SQL + " AND RM_PRREQN_QTN.RM_RMM_RM_CODE = RM_PR_DETAILS.RM_RMM_RM_CODE";
                SQL = SQL + " AND RM_PR_DETAILS.RM_PR_PRNO = RM_PRREQN_QTN.RM_PR_PRNO";
                SQL = SQL + " AND RM_PR_DETAILS.RM_RMM_RM_CODE =    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " AND RM_PRREQN_QTN.RM_PRQ_SLNO = RM_PR_DETAILS.RM_PRD_SL_NO";
                SQL = SQL + " AND  (RM_PRREQN_QTN.RM_PO_PONO = ''   OR RM_PRREQN_QTN.RM_PO_PONO IS NULL) ";
                SQL = SQL + " AND  RM_PRREQN_QTN.RM_VM_VENDOR_CODE='" + SupplierCode + "' AND RM_PR_DETAILS.RM_PR_PRNO='" + oEntity.PRNo + "' AND RM_PRREQN_QTN.AD_FIN_FINYRID=" + mngrclass.FinYearID + "";

                DtlData = clsSQLHelper.GetDataset(SQL);


                int J = 0;
                int K = 0;

                double uprice = 0;

                for (J = 0; J <= DtlData.Tables[0].Rows.Count - 1; J++)
                {
                    slno++;

                    grandttotal = grandttotal + Convert.ToDouble(DtlData.Tables[0].Rows[J]["TOTALAMOUNT"].ToString());

                    if (DtlData.Tables[0].Rows[J]["RM_RMM_VAT_APPLICABLE_YN"].ToString() == "Y")
                    {
                        TotalAmountBeforeTax = Convert.ToDouble(DtlData.Tables[0].Rows[J]["TOTALAMOUNT"].ToString());
                        TaxAmountInGrid = (TotalAmountBeforeTax * TaxPer) / 100;
                        TotalAfterTax = TotalAmountBeforeTax + TaxAmountInGrid;

                    }
                    else
                    {
                        TotalAmountBeforeTax = Convert.ToDouble(DtlData.Tables[0].Rows[J]["TOTALAMOUNT"].ToString());
                        TaxAmountInGrid = 0;
                        TotalAfterTax = TotalAmountBeforeTax + TaxAmountInGrid;
                        TaxPer = 0;
                        TaxType = "V0";

                    }
                    TaxAmount = TaxAmount + ((TotalAmountBeforeTax * TaxPer) / 100);



                    uprice = Convert.ToDouble(DtlData.Tables[0].Rows[J]["RM_PRQ_UNIT_PRICE"].ToString()) - Convert.ToDouble(DtlData.Tables[0].Rows[J]["RM_PRQ_DISCOUNT"].ToString());

                    SQL = " INSERT INTO RM_PO_PROJECT_DETAILS (";
                    SQL = SQL + " RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                    SQL = SQL + " RM_RMM_RM_CODE, RM_POD_SL_NO, SALES_STN_STATION_CODE, ";
                    SQL = SQL + " HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE,  RM_IM_ITEM_DTL_DESCRIPTION, ";
                    SQL = SQL + " RM_POD_QTY, RM_POD_UNIT_PRICE,RM_POD_UNIT_PRICE_FC, RM_UOM_UOM_CODE, ";
                    SQL = SQL + " RM_POD_DISCOUNT_PERCENT,RM_POD_DISCOUNT_PERCENT_FC, RM_POD_DISCOUNT_AMOUNT,RM_POD_DISCOUNT_AMOUNT_FC,RM_POD_RM_AMOUNT_BFR_VAT,RM_POD_RM_AMOUNT_BFR_VAT_FC, RM_POD_TOTALAMT,RM_POD_TOTALAMT_FC,RM_POD_RM_VAT_AMOUNT,RM_POD_RM_VAT_AMOUNT_FC, ";
                    SQL = SQL + " RM_POD_PENDING_QTY, RM_POD_NEWPRICE,RM_POD_NEWPRICE_FC,RM_POD_STATUS,PC_BUD_BUDGET_ITEM_CODE,GL_COSM_ACCOUNT_CODE,RM_POD_RM_VAT_RATE,RM_POD_RM_VAT_RATE_FC,RM_POD_RM_VAT_TYPE  )";
                    SQL = SQL + " VALUES ('" + PONo + "' ,'" + mngrclass.CompanyCode + "' ," + mngrclass.FinYearID + " ,";
                    SQL = SQL + " '" + DtlData.Tables[0].Rows[J]["RM_RMM_RM_CODE"].ToString() + "' , " + slno + ",  '" + DtlData.Tables[0].Rows[J]["SALES_STN_STATION_CODE"].ToString() + "',";
                    SQL = SQL + " '" + DtlData.Tables[0].Rows[J]["HR_DEPT_DEPT_CODE"].ToString() + "'  ,'" + DtlData.Tables[0].Rows[J]["RM_SM_SOURCE_CODE"].ToString() + "' ,'" + DtlData.Tables[0].Rows[J]["DTL_DESCRIPTION"].ToString() + "' ,";
                    SQL = SQL + " " + DtlData.Tables[0].Rows[J]["POQTY"].ToString() + " , " + uprice + " , " + uprice + " , '" + DtlData.Tables[0].Rows[J]["RM_UOM_UOM_CODE"].ToString() + "' ,";
                    SQL = SQL + " 0, 0,0, 0, " + DtlData.Tables[0].Rows[J]["TOTALAMOUNT"].ToString() + "," + DtlData.Tables[0].Rows[J]["TOTALAMOUNT"].ToString() + ",'" + TotalAfterTax.ToString() + "','" + TotalAfterTax.ToString() + "','" + TaxAmountInGrid.ToString() + "','" + TaxAmountInGrid.ToString() + "',";
                    SQL = SQL + " " + DtlData.Tables[0].Rows[J]["POQTY"].ToString() + " ," + uprice + " ," + uprice + " ,'O','" + DtlData.Tables[0].Rows[J]["PC_BUD_BUDGET_ITEM_CODE"].ToString() + "' ,'" + DtlData.Tables[0].Rows[J]["GL_COSM_ACCOUNT_CODE"].ToString() + "', ";
                    SQL = SQL + "" + Convert.ToDouble(TaxPer) + "," + Convert.ToDouble(TaxPer) + ",'" + TaxType + "' )";
                    sSQLArray.Add(SQL);






                    //Update Items in PR with the newly generated PO 
                    //_______________________________________________
                    SQL = " UPDATE RM_PRREQN_QTN";
                    SQL = SQL + "  SET RM_PO_PONO = '" + PONo + "',RM_PO_PO_FINYEAR_ID = " + mngrclass.FinYearID + "";
                    SQL = SQL + "  WHERE RM_RMM_RM_CODE = '" + DtlData.Tables[0].Rows[J]["RM_RMM_RM_CODE"].ToString() + "'";
                    SQL = SQL + "  AND rm_vm_vendor_code = '" + SupplierCode + "'";
                    SQL = SQL + "  AND RM_PRQ_APPROVED = 'Y'";
                    SQL = SQL + "  AND RM_PRQ_DELETESTATUS = 0";
                    SQL = SQL + "  AND ad_fin_finyrid = " + mngrclass.FinYearID;
                    SQL = SQL + "  AND RM_PR_PRNO = '" + oEntity.PRNo + "'";

                    sSQLArray.Add(SQL);
                    //_______________________________________________
                }
                NetTotal = grandttotal + TaxAmount;

                SQL = " INSERT INTO RM_PO_DETAILS (";
                SQL = SQL + "  RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                SQL = SQL + "  RM_RMM_RM_CODE, RM_POD_SL_NO, SALES_STN_STATION_CODE, ";
                SQL = SQL + "  HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE,  RM_IM_ITEM_DTL_DESCRIPTION, ";
                SQL = SQL + "  RM_POD_QTY, RM_POD_UNIT_PRICE,RM_POD_UNIT_PRICE_FC, RM_UOM_UOM_CODE, ";
                SQL = SQL + "  RM_POD_DISCOUNT_PERCENT, RM_POD_DISCOUNT_AMOUNT,RM_POD_DISCOUNT_PERCENT_FC, RM_POD_DISCOUNT_AMOUNT_FC,RM_POD_RM_AMOUNT_BFR_VAT,RM_POD_RM_AMOUNT_BFR_VAT_FC, RM_POD_TOTALAMT,RM_POD_TOTALAMT_FC,RM_POD_RM_VAT_AMOUNT,RM_POD_RM_VAT_AMOUNT_FC, ";
                SQL = SQL + "  RM_POD_PENDING_QTY, RM_POD_NEWPRICE,RM_POD_NEWPRICE_FC, RM_POD_STATUS,PC_BUD_BUDGET_ITEM_CODE, ";
                SQL = SQL + "  RM_POD_RM_VAT_RATE,RM_POD_RM_VAT_RATE_FC,RM_POD_RM_VAT_TYPE)";

                SQL = SQL + "  select ";
                SQL = SQL + "  RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                SQL = SQL + "  RM_RMM_RM_CODE, row_number() over (order by null) AS   RM_POD_SL_NO, SALES_STN_STATION_CODE, ";
                SQL = SQL + "  HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE,  RM_IM_ITEM_DTL_DESCRIPTION, ";

                SQL = SQL + "  sum(RM_POD_QTY) RM_POD_QTY, ";

                SQL = SQL + "   CASE SUM (RM_POD_QTY)";
                SQL = SQL + "     WHEN 0 ";
                SQL = SQL + "       THEN 0";
                SQL = SQL + "    ELSE (SUM (RM_POD_RM_AMOUNT_BFR_VAT) / SUM (RM_POD_QTY))";
                SQL = SQL + "   END AS RM_POD_UNIT_PRICE,";

                SQL = SQL + "   CASE SUM (RM_POD_QTY)";
                SQL = SQL + "     WHEN 0 ";
                SQL = SQL + "       THEN 0";
                SQL = SQL + "    ELSE (SUM (RM_POD_RM_AMOUNT_BFR_VAT_FC) / SUM (RM_POD_QTY))";
                SQL = SQL + "   END AS RM_POD_UNIT_PRICE_FC,";

                SQL = SQL + "   RM_UOM_UOM_CODE, ";
                SQL = SQL + "  0 RM_POD_DISCOUNT_PERCENT, 0 RM_POD_DISCOUNT_AMOUNT, 0 RM_POD_DISCOUNT_PERCENT_FC, 0 RM_POD_DISCOUNT_AMOUNT_FC,sum(RM_POD_RM_AMOUNT_BFR_VAT) RM_POD_RM_AMOUNT_BFR_VAT, ";
                SQL = SQL + "sum(RM_POD_RM_AMOUNT_BFR_VAT_FC) RM_POD_RM_AMOUNT_BFR_VAT_FC,sum(RM_POD_TOTALAMT)  RM_POD_TOTALAMT,sum(RM_POD_TOTALAMT_FC)  RM_POD_TOTALAMT_FC,sum(RM_POD_RM_VAT_AMOUNT) RM_POD_RM_VAT_AMOUNT,sum(RM_POD_RM_VAT_AMOUNT_FC) RM_POD_RM_VAT_AMOUNT_FC,";


                SQL = SQL + "  sum(RM_POD_PENDING_QTY)RM_POD_PENDING_QTY,";

                SQL = SQL + "   CASE SUM (RM_POD_QTY)";
                SQL = SQL + "     WHEN 0 ";
                SQL = SQL + "      THEN 0";
                SQL = SQL + "    ELSE (SUM (RM_POD_RM_AMOUNT_BFR_VAT) / SUM (RM_POD_QTY))";
                SQL = SQL + "   END AS RM_POD_NEWPRICE,";

                SQL = SQL + "   CASE SUM (RM_POD_QTY)";
                SQL = SQL + "     WHEN 0 ";
                SQL = SQL + "      THEN 0";
                SQL = SQL + "    ELSE (SUM (RM_POD_RM_AMOUNT_BFR_VAT_FC) / SUM (RM_POD_QTY))";
                SQL = SQL + "   END AS RM_POD_NEWPRICE_FC,";

                SQL = SQL + "  'O',PC_BUD_BUDGET_ITEM_CODE,RM_POD_RM_VAT_RATE,RM_POD_RM_VAT_RATE_FC,RM_POD_RM_VAT_TYPE";
                SQL = SQL + "  from RM_PO_PROJECT_DETAILS ";
                SQL = SQL + "  WHERE  RM_PO_PONO ='" + PONo + "' ";
                SQL = SQL + "  AND   AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";
                SQL = SQL + "  group by  RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                SQL = SQL + "  RM_RMM_RM_CODE, SALES_STN_STATION_CODE, ";
                SQL = SQL + "  HR_DEPT_DEPT_CODE,RM_SM_SOURCE_CODE, RM_IM_ITEM_DTL_DESCRIPTION,";
                SQL = SQL + "  RM_UOM_UOM_CODE,PC_BUD_BUDGET_ITEM_CODE,RM_POD_RM_VAT_RATE,RM_POD_RM_VAT_RATE_FC,RM_POD_RM_VAT_TYPE";

                sSQLArray.Add(SQL);


                //if (TaxPer > 0)
                //{
                //    TaxAmount = (grandttotal * TaxPer) / 100;

                //    NetTotal = grandttotal + TaxAmount;
                //}
                //else
                //{
                //    NetTotal = grandttotal;
                //}


                SQL = " INSERT INTO RM_PO_MASTER (";
                SQL = SQL + " RM_PO_PONO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                SQL = SQL + " RM_PO_PO_DATE, RM_VM_VENDOR_CODE, RM_PO_PRICETYPE,RM_PO_EXP_DATE, ";
                SQL = SQL + " RM_PO_GRAND_TOTAL,RM_PO_GRAND_TOTAL_FC, RM_PO_DISCOUNT_PERC,RM_PO_DISCOUNT_PERC_FC, RM_PO_DISCOUNT_AMOUNT,RM_PO_DISCOUNT_AMOUNT_FC, ";
                SQL = SQL + " RM_PO_NET_AMOUNT,RM_PO_NET_AMOUNT_FC, RM_PO_PO_STATUS, RM_PO_REMARKS, ";
                SQL = SQL + " AD_CUR_CURRENCY_CODE, RM_PO_EXCHANGE_RATE, RM_PO_APPROVED, ";
                SQL = SQL + " RM_PO_APPROVEDBY, RM_PO_PURENTRY, RM_PO_POTYPE, ";
                SQL = SQL + " RM_PO_CANCEL_REMARKS, RM_PO_CONTAC1_CODE, RM_PO_CONTAC2_CODE, ";
                SQL = SQL + " RM_PR_PRNO, RM_PR_FIN_FINYRID, RM_PO_SUPP_REF, ";
                SQL = SQL + " RM_PO_SUPP_REF_DATE, RM_PO_OUR_REF, RM_PO_CREATEDBY, ";
                SQL = SQL + " RM_PO_CREATEDDATE, RM_PO_UPDATEDBY, RM_PO_UPDATEDDATE, ";
                SQL = SQL + " RM_PO_DELETESTATUS,RM_PO_COST_CENTER,RM_MPOM_PAYMENT_TERMS,AD_BR_CODE, ";
                SQL = SQL + " RM_PO_VAT_PERCENTAGE,RM_PO_VAT_PERCENTAGE_FC,RM_PO_VAT_AMOUNT  ,RM_PO_VAT_AMOUNT_FC  ,RM_VM_CREDIT_TERMS,RM_PO_QTY_RATE_TYPE,HR_DEPT_DEPT_CODE, ";
                SQL = SQL + " RM_PO_AGG_START_DATE,RM_PO_AGG_END_DATE,RM_PO_STOCK_TYPE_CODE) ";

                SQL = SQL + " VALUES ('" + PONo + "' ,'" + mngrclass.CompanyCode + "' ," + mngrclass.FinYearID + " ,";
                SQL = SQL + " '" + DateTime.Now.ToString("dd-MMM-yyyy") + "' ,'" + SupplierCode + "' ,'EX-FACTORY','" + DateTime.Now.ToString("dd-MMM-yyyy") + "'  ,";
                SQL = SQL + " " + Convert.ToDouble(grandttotal) + " ," + Convert.ToDouble(grandttotal) + " ,0,0,0 ,0 ,";
                SQL = SQL + " " + Convert.ToDouble(NetTotal) + "  ," + Convert.ToDouble(NetTotal) + "  ,'O' ,'' ,";
                SQL = SQL + "'" + mngrclass.CurrencyCodeBase + "' ," + 1 + " ,'N',";
                SQL = SQL + " '','N' ,'CREDIT',";
                SQL = SQL + " '', '" + oEntity.Contact1 + "','',";
                SQL = SQL + "'" + oEntity.PRNo + "' ," + mngrclass.FinYearID + " ,'' ,";
                SQL = SQL + "'' ,'" + oEntity.Physical + "' , '" + mngrclass.UserName + "' ,";
                SQL = SQL + " TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'), '','' ,";
                SQL = SQL + "0 ,'" + oEntity.CostCenter + "','" + payterms + "', '" + oEntity.Branch + "' ,";
                SQL = SQL + " " + Convert.ToDouble(TaxPercentage) + "," + Convert.ToDouble(TaxPercentage) + "," + Convert.ToDouble(TaxAmount) + "," + Convert.ToDouble(TaxAmount) + ",'" + paytermCode + "','Q','',sysdate,sysdate,'" + oEntity.StockType + "' )";




                sSQLArray.Add(SQL);

                SQL = " INSERT INTO RM_PO_NOTES ( ";
                SQL = SQL + "RM_PO_PONO, AD_FIN_FINYRID, RM_PO_SL_NO,  ";
                SQL = SQL + "   RM_PO_NOTES, RM_PO_PRINT_YN, RM_PO_DELSTATUS ";
                SQL = SQL + " ) ";

                SQL = SQL + "select '" + PONo + "' RM_PO_PONO," + mngrclass.FinYearID + " AD_FIN_FINYRID,";
                SQL = SQL + " RM_PO_DEFAULTS_SL_NO,RM_PO_DEFAULTS_TEXT,'Y' RM_PO_PRINT_YN,0 RM_PO_DELSTATUS from RM_DEFAULTS_PO_TEMRS where AD_BR_CODE='" + oEntity.Branch + "' ";
                SQL = SQL + " and RM_PO_DELSTATUS=0 order by  RM_PO_DEFAULTS_SL_NO asc ";
                //SQL = SQL + "VALUES ( '" + QTNNo + "'," + mngrclass.FinYearID + ", " + Data.SlNo + ", ";
                //SQL = SQL + " '" + Data.sText + "', '" + Data.sPrintYN + "' ,0 ) ";

                sSQLArray.Add(SQL);

                //sRetun = string.Empty;
                //sRetun = InsertPOApprovalMailSQL(PONo, mngrclassobj);
                //if (sRetun != "CONTINUE")
                //{
                //    return sRetun;
                //}

                sRetun = string.Empty;
                sRetun = ModifyPRStatus(oEntity.PRNo, objWSGridEntity, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;
                // WTPO should be teh tag // purchase order ws else the dc number will nt increase // jomy  RTMO 
                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "WTPO", PONo, true, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);

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

        /// </summary>
        /// <param name="PRNo"></param>
        /// <param name="objWSGridEntity"></param>
        /// <param name="mngrclassobj"></param>
        /// <returns></returns>

        private string ModifyPRStatus ( string PRNo, List<PurchaseRequisitionPRCGridEntity> objWSGridEntity, object mngrclassobj )
        {
            string sRetun = string.Empty;
            string SQL = null;
            int iRowCnt = 0;
            //object oAppr1 = null;
            //object oAppr2 = null;
            //object oAppr3 = null;
            string sStatus = "C";
            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();
                //int slno = 1;

                foreach (var Data in objWSGridEntity)
                {
                    if ((Data.PRQtnApp1 == "false") & (Data.PRQtnApp2 == "false") & (Data.PRQtnApp3 == "false"))
                    {
                        sStatus = "O";
                    }
                }




                sStatus = "C";
                DataSet dsDataset = default(DataSet);



                //SQL = " SELECT   COUNT ( DISTINCT  RM_RMM_RM_CODE )     cnt";
                //SQL = SQL + " FROM RM_PRREQN_QTN, RM_PR_MASTER";
                //SQL = SQL + " WHERE     RM_PRREQN_QTN.RM_PR_PRNO = RM_PR_MASTER.RM_PR_PRNO ";
                //SQL = SQL + " AND RM_PRREQN_QTN.ad_fin_finyrid = RM_PR_MASTER.ad_fin_finyrid"; 
                //SQL = SQL + " and RM_PRREQN_QTN.RM_PRQ_APPROVED ='N' AND RM_PR_MASTER.RM_PR_PRNO='" + PRNo + "'";
                //SQL = SQL + "  AND RM_PR_MASTER.AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";
                ////SQL = SQL + "AND RM_PRREQN_QTN.RM_PO_PONO IS not NULL";

                SQL = "   select  COUNT (RM_RMM_RM_CODE ) cnt ";
                SQL = SQL + "    from RM_PRREQN_QTN ";
                SQL = SQL + "    where     ";
                SQL = SQL + "    RM_PRQ_APPROVED ='N' ";
                SQL = SQL + "     AND    RM_PRREQN_QTN.RM_PR_PRNO='" + PRNo + "'";
                SQL = SQL + "     AND  RM_PRREQN_QTN.AD_FIN_FINYRID= " + mngrclass.FinYearID + " ";
                SQL = SQL + "    AND RM_RMM_RM_CODE NOT IN ";
                SQL = SQL + "    (  ";
                SQL = SQL + "     select     RM_RMM_RM_CODE   from RM_PRREQN_QTN ";
                SQL = SQL + "     where     ";
                SQL = SQL + "     RM_PRQ_APPROVED ='Y' ";
                SQL = SQL + "      AND    RM_PRREQN_QTN.RM_PR_PRNO='" + PRNo + "'";
                SQL = SQL + "     AND  RM_PRREQN_QTN.AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";
                SQL = SQL + "     ) ";

                dsDataset = clsSQLHelper.GetDataset(SQL);

                if (Convert.ToDouble(dsDataset.Tables[0].Rows[0]["cnt"]) == 0)
                {

                    SQL = " UPDATE RM_PR_MASTER";
                    SQL = SQL + "    SET RM_PR_PR_STATUS = '" + sStatus + "'";
                    SQL = SQL + "  WHERE RM_PR_PRNO = '" + PRNo + "' AND ad_fin_finyrid = " + mngrclass.FinYearID + " ";
                    //  SQL = SQL + "    AND RM_PR_UPDATEDDATE = 0";
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

        public DataSet btnPO ( string PRno, object mngrclassobj )
        {

            DataSet dtType = new DataSet();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                //'SQL = " select count (*) from "
                //'SQL = SQL +  "   WS_TBL_PURREQN_QTN  "
                //'SQL = SQL +  " where "
                //'SQL = SQL +  " AD_FIN_FINYRID  =3 AND RM_PR_PRNO  ='110001'   and RM_RMM_RM_CODE not in ("
                //'SQL = SQL +  " select RM_RMM_RM_CODE from WS_TBL_PURREQN_QTN  where "
                //'SQL = SQL +  " AD_FIN_FINYRID  =3 AND RM_PR_PRNO  ='110001' "
                //'SQL = SQL +  " and RM_PO_PONO is not null )"

                SQL = " SELECT count(RM_PRREQN_QTN.RM_PR_PRNO) cnt";
                SQL = SQL + " FROM RM_PRREQN_QTN, RM_PR_MASTER";
                SQL = SQL + " WHERE     RM_PRREQN_QTN.RM_PR_PRNO = RM_PR_MASTER.RM_PR_PRNO ";
                SQL = SQL + " AND RM_PRREQN_QTN.ad_fin_finyrid = RM_PR_MASTER.ad_fin_finyrid";
                SQL = SQL + " and RM_PR_MASTER.RM_PR_PR_STATUS ='O'";
                SQL = SQL + " AND RM_PR_MASTER.RM_PR_PRNO='" + PRno + "'";
                SQL = SQL + "  AND RM_PR_MASTER.AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";
                SQL = SQL + "AND RM_PRREQN_QTN.RM_PO_PONO IS not NULL";

                //SQL = " SELECT count(*) pocnt"
                //SQL = SQL +  " FROM RM_PRREQN_QTN,  RM_PR_MASTER "
                //SQL = SQL +  " WHERE  "

                //SQL = SQL +  " RM_PR_PRNO='" +  Trim(txtPRNo.Text) +  "' AND AD_FIN_FINYRID=" +  miFinYrId +  " AND RM_PO_PONO IS not NULL"


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

        public DataSet GenMData ( string PRno, object mngrclassobj )
        {

            DataSet dtType = new DataSet();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT DISTINCT RM_PRREQN_QTN.rm_vm_vendor_code";
                SQL = SQL + " FROM RM_PRREQN_QTN";
                SQL = SQL + " WHERE  ";
                SQL = SQL + "  RM_PRREQN_QTN.RM_PRQ_APPROVED = 'Y'";
                SQL = SQL + " AND  RM_PR_PRNO='" + PRno + "' AND AD_FIN_FINYRID=" + mngrclass.FinYearID + " AND RM_PO_PONO IS NULL";

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

        private string InsertPOApprovalMailSQL ( string strPONo, object mngrclassobj )
        {
            string sRetun = string.Empty;
            try
            {

                ArrayList lArMailApp = default(ArrayList);
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                Int16 i = default(Int16);
                //lArMail.Clear()
                lArMailApp = SendApprovalMail("RMOTOA", "Purchase Order", strPONo, mngrclass.UserName, " for Approval", mngrclass);
                for (i = 1; i <= lArMailApp.Count; i++)
                {
                    sSQLArray.Add(lArMailApp[i - 1]);
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

        public ArrayList SendApprovalMail ( string FormCode, string FormDescription, string RefNo, string FromUser, string Subject, object mngrclassobj )
        {

            string lsSql = null;
            int i = 0;
            ArrayList lArQueries = new ArrayList();
            string lsSub = null;
            string lsToUser = null;
            DataSet ldsToUser = default(DataSet);
            string dtServerDate = null;
            DataSet fdsFormData = new DataSet();
            ArrayList lsXMLFiles = new ArrayList();
            SqlHelper clsSQLHelper = new SqlHelper();
            SessionManager mngrclass = (SessionManager)mngrclassobj;
            //  dtServerDate = CStr(lobjValidate.fnGetServerDate)
            dtServerDate = DateTime.Now.ToString("dd-MM-yyyy");
            SQL = "SELECT AD_UM_USERID FROM AD_MAIL_APPROVAL_USERS WHERE AD_MI_ITEMID='" + FormCode + "' AND AD_CM_COMPANY_CODE='" + mngrclass.CompanyCode + "'";
            //ldsToUser = mobjQuery.LoadDataSet("SELECT AD_UM_USERID FROM AD_MAIL_APPROVAL_USERS WHERE AD_MI_ITEMID='" + FormCode + "' AND AD_CM_COMPANY_CODE='" + msCompanyCode + "'");
            ldsToUser = clsSQLHelper.GetDataset(SQL);
            for (i = 1; i <= ldsToUser.Tables[0].Rows.Count; i++)
            {
                lsSub = FormCode + "- " + FormDescription + " " + Subject;
                lsToUser = ldsToUser.Tables[0].Rows[i - 1]["AD_UM_USERID"].ToString();
                SQL = "INSERT INTO AD_MAIL_INBOX(AD_INB_SUB, AD_INB_DATE, AD_INB_REFNO, AD_CM_COMPANY_CODE, AD_INB_FROM_USERID,";
                SQL = SQL + "  AD_INB_TO_USERID, AD_INB_MSG,AD_FIN_FINYRID) VALUES('" + lsSub + "',TO_DATE('" + dtServerDate + "','DD-MON-YYYY HH:MI:SS AM'),";
                SQL = SQL + " '" + RefNo + "','" + mngrclass.CompanyCode + "','" + FromUser + "','" + lsToUser + "','System generated message'," + mngrclass.FinYearID + ")";
                lArQueries.Add(SQL);
            }

            return lArQueries;

        }

        private DataTable SupplierPayterms ( string strSupplierNo, string Branch )
        {
            string sRetun = string.Empty;
            DataTable dtType = new DataTable();

            try
            {


                //SessionManager mngrclass = (SessionManager)mngrclassobj;
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                //lArMail.Clear()
                SQL = " SELECT  RM_VENDOR_MASTER.RM_VM_VENDOR_CODE SUPPLIER_CODE,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIER_NAME,RMVENDMSTBRANCHPAYTERMSDATA.SALES_PAY_TYPE_DESC,";
                SQL = SQL + " RMVENDMSTBRANCHPAYTERMSDATA.PAY_TERMS_ID RM_VM_CREDIT_TERMS";
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
                SQL = SQL + "   = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE(+) )RMVENDMSTBRANCHPAYTERMSDATA ";
                SQL = SQL + " WHERE  RM_VENDOR_MASTER.RM_VM_VENDOR_CODE='" + strSupplierNo + "' ";
                SQL = SQL + "  And RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  = RMVENDMSTBRANCHPAYTERMSDATA.RM_VM_VENDOR_CODE  ";
                SQL = SQL + "   order by RM_VM_VENDOR_NAME asc  ";

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);

                // return sRetun;
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            //if (dtType.Rows.Count > 0)
            //{
            //    sRetun = dtType.Rows[0]["SALES_PAY_TYPE_DESC"].ToString();
            //    return sRetun;
            //}
            //else
            //{
            //    return sRetun;
            //}
            return dtType;
        }

        private DataTable SupplierData ( string strSupplierNo, string Branch )
        {
            string sRetun = string.Empty;
            DataTable dtType = new DataTable();

            try
            {


                //SessionManager mngrclass = (SessionManager)mngrclassobj;
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                //lArMail.Clear()
                SQL = " SELECT  RM_VENDOR_MASTER.RM_VM_VENDOR_CODE SUPPLIER_CODE,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIER_NAME , ";
                SQL = SQL + " RMVENDMSTBRANCHTAXTYPEDATA.GL_TAX_TYPE_CODE , GL_TAX_TYPE_PER RM_VM_TAX_VAT_PERCENTAGE  ,";
                SQL = SQL + " RM_VM_TAX_VAT_REG_NUMBER ";
                SQL = SQL + " FROM   RM_VENDOR_MASTER,   ";
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
                SQL = SQL + " WHERE  RM_VENDOR_MASTER.RM_VM_VENDOR_CODE='" + strSupplierNo + "' ";
                SQL = SQL + "    AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE =  RMVENDMSTBRANCHTAXTYPEDATA.RM_VM_VENDOR_CODE ";
                SQL = SQL + " order by RM_VM_VENDOR_NAME asc  ";

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }

            return dtType;

        }

        private int FillTaxSetup ( )
        {
            try
            {
                DataTable dtTax = new DataTable();
                SqlHelper clsSQLHelper = new SqlHelper();
                double TaxPer = 0;
                int TaxApllicableFinYr = 0;

                SQL = "   SELECT  ";
                SQL = SQL + "   GL_PURCHASE_TAX_ACCOUNT_CODE,   ";
                SQL = SQL + "   GL_SALES_TAX_ACCOUNT_CODE,GL_TAX_APPLICBLE_FINYRID  FROM  GL_DEFAULTS_TAX_MASTER  ";

                dtTax = clsSQLHelper.GetDataTableByCommand(SQL);

                if (dtTax.Rows.Count > 0)
                {

                    TaxApllicableFinYr = Convert.ToInt16(dtTax.Rows[0]["GL_TAX_APPLICBLE_FINYRID"].ToString());
                }
                else
                {
                    TaxApllicableFinYr = 0;
                }

                return TaxApllicableFinYr;
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);

                return 0;
            }
        }

        public DataTable fnPOTotQty ( string PoNo, int FinId, string RmCode, string SourceCode )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " select  ";
                SQL = SQL + " RM_PO_PONO,  ";
                SQL = SQL + " sum(RM_POD_QTY) RM_POD_QTY,sum( RM_POD_TOTALAMT)  RM_POD_TOTALAMT ";
                SQL = SQL + " from RM_PO_PROJECT_DETAILS ";
                SQL = SQL + " where RM_PO_PROJECT_DETAILS.RM_PO_PONO= '" + PoNo + "' ";
                SQL = SQL + " and RM_PO_PROJECT_DETAILS.AD_FIN_FINYRID = " + FinId + "";
                SQL = SQL + " and RM_PO_PROJECT_DETAILS.RM_RMM_RM_CODE = '" + RmCode + "'";
                SQL = SQL + " and RM_PO_PROJECT_DETAILS.RM_SM_SOURCE_CODE = '" + SourceCode + "'";
                SQL = SQL + " group by  RM_PO_PONO,RM_RMM_RM_CODE,RM_SM_SOURCE_CODE ";

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



        public DataTable CostAccountView ( string Branch, string UserName )
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

        #region "print"

        public DataSet FetchPR_Quotation_PrintData ( string txtPRNo, object mngrclassobj )
        {
            DataSet dsPoStatus = new DataSet("RMPRPRINT");

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                //SQL = "     ";
                //SQL = " select ";
                //SQL = SQL + " RM_PR_PRNO, AD_FIN_FINYRID, RM_PR_PHYSICAL_NO, RM_PR_PR_DATE, RM_PR_EXP_DATE,  ";
                //SQL = SQL + " RM_PR_PURCHASE_TYPE, RM_PR_GRAND_TOTAL, RM_PR_REMARKS, RM_PR_PR_STATUS, RM_PR_CREATEDBY,  ";
                //SQL = SQL + " RM_PR_CREATEDDATE, RM_PR_UPDATEDBY, RM_PR_UPDATEDDATE, RM_PR_DELETESTATUS, RM_PR_VERIFIED,  ";
                //SQL = SQL + " RM_PR_VERIFIEDBY, RM_PR_APPROVED, RM_PR_APPROVEDBY, RM_PR_CANCEL, RM_PR_CANCEL_REMARKS,  ";
                //SQL = SQL + " SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, HR_DEPT_DEPT_CODE, HR_DEPT_DEPT_DESC, RM_VM_VENDOR_CODE,  ";
                //SQL = SQL + " RM_VM_VENDOR_NAME, RM_VM_POBOX, RM_VM_ADDRESS, RM_VM_CITY, RM_VM_TEL_NO,  ";
                //SQL = SQL + " RM_VM_FAXNO, RM_VM_EMAIL, RM_VM_WEB_SITE, RM_VM_CONT_PERSON1, RM_VM_CONT_DESIG1,  ";
                //SQL = SQL + " RM_VM_CONT_PERSON1_MOBNO, RM_VM_CONT_PERSON2, RM_VM_CONT_DESIG2, RM_VM_CONT_PERSON2_MOBNO, RM_PRD_SL_NO,  ";
                //SQL = SQL + " RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION, RM_IM_ITEM_DTL_DESCRIPTION, RM_UOM_UOM_CODE, RM_UM_UOM_DESC,  ";
                //SQL = SQL + " RM_PRD_DIR_USE_QTY, RM_PRD_INV_QTY, RM_PRD_QTY, RM_PRD_APP_RATE, RM_PRD_APP_AMOUNT ";
                //SQL = SQL + " FROM  ";
                //// SQL = SQL + " rm_pur_req_quote_print_Sum_vw   ";
                //SQL = SQL + " RM_PUR_REQ_QUOTE_PRINT_VW   ";
                //SQL = SQL + " where  RM_PR_PRNO='" + txtPRNo + "'AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";

                //dsPoStatus = clsSQLHelper.GetDataset(SQL);
                //dsPoStatus.Tables[0].TableName = "RM_PUR_REQ_PRINT_VW";

                ////////////////////////As per the request from XIP we excluded Supplier details in Quotation Print

                SQL = "     ";
                SQL = " SELECT   ";
                SQL = SQL + " rm_pr_prno,ad_fin_finyrid,rm_pr_physical_no,rm_pr_pr_date,rm_pr_exp_date, ";
                SQL = SQL + " rm_pr_purchase_type,rm_pr_grand_total,rm_pr_remarks,rm_pr_pr_status,rm_pr_createdby, ";
                SQL = SQL + " rm_pr_createddate,rm_pr_updatedby,rm_pr_updateddate,rm_pr_deletestatus,rm_pr_verified, ";
                SQL = SQL + " rm_pr_verifiedby,rm_pr_approved,rm_pr_approvedby,rm_pr_cancel,rm_pr_cancel_remarks, ";
                SQL = SQL + " gl_cosm_account_code,CostingName,pc_bud_resource_code,pc_bud_resource_name, ";
                SQL = SQL + " sales_stn_station_code,sales_stn_station_name, ";
                SQL = SQL + " hr_dept_dept_code,hr_dept_dept_desc,rm_rmm_rm_code,rm_rmm_rm_description, ";
                SQL = SQL + " rm_im_item_dtl_description,rm_uom_uom_code,rm_um_uom_desc,rm_prd_sl_no, ";
                SQL = SQL + " rm_prd_dir_use_qty,rm_prd_inv_qty,rm_prd_qty,rm_prd_app_rate,rm_prd_app_amount, ";
                SQL = SQL + " rm_prd_deletestatus,rm_prd_item_apprv,rm_prd_item_remarks,rm_pr_cost_center, ";
                SQL = SQL + " rm_pr_immediate,rm_pr_within_2days,rm_pr_within_week,rm_pr_within_15days, ";
                SQL = SQL + " rm_pr_avlb_rlsd,rm_pr_nonavlb_prsd ,";
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
                SQL = SQL + "  RM_PURCHASE_REQUESTPRC_PRINTVW  ";
                SQL = SQL + "  where  RM_PR_PRNO='" + txtPRNo + "'AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";

                dsPoStatus = clsSQLHelper.GetDataset(SQL);
                dsPoStatus.Tables[0].TableName = "RM_PUR_REQ_PRINT_PRC_VW";




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


        public DataSet FetchPR_Request_PrintData ( string txtPRNo, object mngrclassobj )
        {
            DataSet dsPoStatus = new DataSet("RMPRPRINT");

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = "     ";
                SQL = " SELECT   ";
                SQL = SQL + " rm_pr_prno,ad_fin_finyrid,rm_pr_physical_no,rm_pr_pr_date,rm_pr_exp_date, ";
                SQL = SQL + " rm_pr_purchase_type,rm_pr_grand_total,rm_pr_remarks,rm_pr_pr_status,rm_pr_createdby, ";
                SQL = SQL + " rm_pr_createddate,rm_pr_updatedby,rm_pr_updateddate,rm_pr_deletestatus,rm_pr_verified, ";
                SQL = SQL + " rm_pr_verifiedby,rm_pr_approved,rm_pr_approvedby,rm_pr_cancel,rm_pr_cancel_remarks, ";
                SQL = SQL + " gl_cosm_account_code,CostingName,pc_bud_resource_code,pc_bud_resource_name, ";
                SQL = SQL + " sales_stn_station_code,sales_stn_station_name, ";
                SQL = SQL + " hr_dept_dept_code,hr_dept_dept_desc,rm_rmm_rm_code,rm_rmm_rm_description, ";
                SQL = SQL + " rm_im_item_dtl_description,rm_uom_uom_code,rm_um_uom_desc,rm_prd_sl_no, ";
                SQL = SQL + " rm_prd_dir_use_qty,rm_prd_inv_qty,rm_prd_qty,rm_prd_app_rate,rm_prd_app_amount, ";
                SQL = SQL + " rm_prd_deletestatus,rm_prd_item_apprv,rm_prd_item_remarks,rm_pr_cost_center, ";
                SQL = SQL + " rm_pr_immediate,rm_pr_within_2days,rm_pr_within_week,rm_pr_within_15days, ";
                SQL = SQL + " rm_pr_avlb_rlsd,rm_pr_nonavlb_prsd, ";
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
                SQL = SQL + "  RM_PURCHASE_REQUESTPRC_PRINTVW  ";
                SQL = SQL + "  where  RM_PR_PRNO='" + txtPRNo + "'AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";

                dsPoStatus = clsSQLHelper.GetDataset(SQL);
                dsPoStatus.Tables[0].TableName = "RM_PUR_REQ_PRINT_PRC_VW";
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


        public DataSet FetchPR_Comparison_Print ( string txtPRNo, object mngrclassobj )
        {
            DataSet dsPRStatus = new DataSet("RMPRPRINT");
            DataTable dtPRStatus = new DataTable();
            DataTable dtPRStatusNew = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                //SQL = "     SELECT  " ;
                //SQL = SQL  +  "    MAIN_QRY.RM_PR_PRNO ,     MAIN_QRY.AD_FIN_FINYRID, RM_PRD_SL_NO,";
                //SQL = SQL  +  "    MAIN_QRY.RM_RMM_RM_CODE,  ";
                //SQL = SQL  + "     MAIN_QRY.RM_RMM_RM_DESCRIPTION,       RM_IM_ITEM_DTL_DESCRIPTION,   ";
                //SQL = SQL  +  "    RM_PRD_QTY  , ";
                //SQL = SQL  +  "    SUPP_ONE.RM_VM_VENDOR_CODE SUPP_ONE_VENDOR_CODE,     SUPP_ONE.RM_VM_VENDOR_NAME SUPP_ONE_VENDOR_NAME  ,      SUPP_ONE.RM_VM_FULL_NAME SUPP_ONE_FULL_NAME, ";
                //SQL = SQL  + "     SUPP_ONE.RM_PRQ_UNIT_PRICE SUPP_ONE_UNIT_PRICE,SUPP_ONE.RM_PRQ_DISCOUNT SUPP_ONE_DISCOUNT,   (SUPP_ONE.RM_PRQ_UNIT_PRICE-SUPP_ONE.RM_PRQ_DISCOUNT)* RM_PRD_QTY    SUPP_ONE_AMOUNT  ,  ";
                //SQL = SQL  +  "    SUPP_ONE.SALES_PAY_TYPE_DESC SUPP_ONE_PAY_TYPE_DESC, ";
                //SQL = SQL  +  "    SUPP_TWO.RM_VM_VENDOR_CODE SUPP_TWO_VENDOR_CODE,     SUPP_TWO.RM_VM_VENDOR_NAME SUPP_TWO_VENDOR_NAME  ,      SUPP_TWO.RM_VM_FULL_NAME SUPP_TWO_FULL_NAME, ";
                //SQL = SQL  + "     SUPP_TWO.RM_PRQ_UNIT_PRICE SUPP_TWO_UNIT_PRICE, SUPP_TWO.RM_PRQ_DISCOUNT SUPP_TWO_DISCOUNT,  (SUPP_TWO.RM_PRQ_UNIT_PRICE-SUPP_TWO.RM_PRQ_DISCOUNT)* RM_PRD_QTY    SUPP_TWO_AMOUNT  ,  ";
                //SQL = SQL  +  "    SUPP_TWO.SALES_PAY_TYPE_DESC SUPP_TWO_PAY_TYPE_DESC, ";
                //SQL = SQL  +  "    SUPP_THREE.RM_VM_VENDOR_CODE SUPP_THREE_VENDOR_CODE,      SUPP_THREE.RM_VM_VENDOR_NAME SUPP_THREE_VENDOR_NAME  ,       ";
                //SQL = SQL  +  "    SUPP_THREE.RM_VM_FULL_NAME SUPP_THREE_FULL_NAME, ";
                //SQL = SQL  + "     SUPP_THREE.RM_PRQ_UNIT_PRICE SUPP_THREE_UNIT_PRICE,SUPP_THREE.RM_PRQ_DISCOUNT SUPP_THREE_DISCOUNT,   (SUPP_THREE.RM_PRQ_UNIT_PRICE-SUPP_THREE.RM_PRQ_DISCOUNT)* RM_PRD_QTY    SUPP_THREE_AMOUNT  ,  ";
                //SQL = SQL  +  "    SUPP_THREE.SALES_PAY_TYPE_DESC SUPP_THREE_PAY_TYPE_DESC ";
                //SQL = SQL  +  "    FROM    ";
                //SQL = SQL  +  "    (   ";
                //SQL = SQL  +  "    SELECT   ";
                //SQL = SQL  +  "    RM_PR_DETAILS .RM_PR_PRNO ,    RM_PR_DETAILS.AD_FIN_FINYRID,RM_PRD_SL_NO,";
                //SQL = SQL  +  "    RM_PR_DETAILS.RM_RMM_RM_CODE,  ";
                //SQL = SQL + "      RM_RMM_RM_DESCRIPTION,       RM_IM_ITEM_DTL_DESCRIPTION,   ";
                //SQL = SQL  +  "    RM_PRD_QTY  ";
                //SQL = SQL  +  "    FROM   ";
                //SQL = SQL  +  "    RM_PR_MASTER , ";
                //SQL = SQL  +  "    RM_PR_DETAILS , ";
                //SQL = SQL  +  "    RM_RAWMATERIAL_MASTER     ";
                //SQL = SQL  +  "    WHERE  ";
                //SQL = SQL  +  "    RM_PR_DETAILS .RM_RMM_RM_CODE =RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                //SQL = SQL  +  "    AND RM_PR_MASTER.RM_PR_PRNO =  RM_PR_DETAILS.RM_PR_PRNO   (+) ";
                //SQL = SQL  +  "    AND   RM_PR_MASTER.AD_FIN_FINYRID  =    RM_PR_DETAILS.AD_FIN_FINYRID     (+)   ";
                //SQL = SQL  +  "    )     ";
                //SQL = SQL  +  "    MAIN_QRY  ,    ";
                //SQL = SQL  +  "    (   ";
                //SQL = SQL  +  "    SELECT  ";
                //SQL = SQL  +  "    RM_PRREQN_QTN.RM_PR_PRNO,  RM_PRREQN_QTN.AD_FIN_FINYRID,  ";
                //SQL = SQL  +  "    RM_PRREQN_QTN.RM_RMM_RM_CODE,  ";
                //SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,           ";
                //SQL = SQL  +  "    RM_PRREQN_QTN.RM_VM_VENDOR_CODE,RM_VM_VENDOR_NAME, RM_VENDOR_MASTER.RM_VM_FULL_NAME, ";
                //SQL = SQL  +  "    RM_PRREQN_QTN.RM_PRQ_UNIT_PRICE, RM_PRREQN_QTN.RM_PRQ_DISCOUNT,  ";
                //SQL = SQL  +  "    RM_VENDOR_MASTER.RM_VM_CREDIT_LIMIT, ";
                //SQL = SQL  +  "    RM_VENDOR_MASTER.RM_VM_CREDIT_PERIOD, ";
                //SQL = SQL  +  "    RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS, ";
                //SQL = SQL  +  "    SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_DESC ";
                //SQL = SQL  +  "    FROM  ";
                //SQL = SQL  +  "    RM_PRREQN_QTN, ";
                //SQL = SQL  +  "    RM_VENDOR_MASTER,SL_PAY_TYPE_MASTER ,RM_RAWMATERIAL_MASTER ";
                //SQL = SQL  +  "    WHERE   ";
                //SQL = SQL  +  "        RM_PRREQN_QTN.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                //SQL = SQL  +  "        AND  RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS =SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE ";
                //SQL = SQL  +  "        AND  RM_PRREQN_QTN.RM_RMM_RM_CODE  =  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                //SQL = SQL  +  "        AND RM_PRQ_SUPPLIERNO = 1    ";
                //SQL = SQL  +  "     )  ";
                //SQL = SQL  +  "    SUPP_ONE ,  ";
                //SQL = SQL  +  "    (   ";
                //SQL = SQL  +  "    SELECT  ";
                //SQL = SQL  +  "    RM_PRREQN_QTN.RM_PR_PRNO,  RM_PRREQN_QTN.AD_FIN_FINYRID, ";
                //SQL = SQL  +  "    RM_PRREQN_QTN.RM_RMM_RM_CODE,  ";
                //SQL = SQL  + "      RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,           ";
                //SQL = SQL  +  "    RM_PRREQN_QTN.RM_VM_VENDOR_CODE,RM_VM_VENDOR_NAME, RM_VENDOR_MASTER.RM_VM_FULL_NAME, ";
                //SQL = SQL  +  "    RM_PRREQN_QTN.RM_PRQ_UNIT_PRICE, RM_PRREQN_QTN.RM_PRQ_DISCOUNT,   ";
                //SQL = SQL  +  "    RM_VENDOR_MASTER.RM_VM_CREDIT_LIMIT, ";
                //SQL = SQL  +  "    RM_VENDOR_MASTER.RM_VM_CREDIT_PERIOD, ";
                //SQL = SQL  +  "    RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS, ";
                //SQL = SQL  +  "    SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_DESC ";
                //SQL = SQL  +  "    FROM  ";
                //SQL = SQL  +  "    RM_PRREQN_QTN, ";
                //SQL = SQL  +  "    RM_VENDOR_MASTER,SL_PAY_TYPE_MASTER ,RM_RAWMATERIAL_MASTER ";
                //SQL = SQL  +  "    WHERE   ";
                //SQL = SQL  +  "        RM_PRREQN_QTN.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                //SQL = SQL  +  "        AND  RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS =SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE ";
                //SQL = SQL  +  "        AND  RM_PRREQN_QTN.RM_RMM_RM_CODE  =  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                //SQL = SQL  +  "        AND RM_PRQ_SUPPLIERNO =  2     ";
                //SQL = SQL  +  "     )  ";
                //SQL = SQL  +  "    SUPP_TWO,  ";
                //SQL = SQL  +  "    (   ";
                //SQL = SQL  +  "    SELECT  ";
                //SQL = SQL  +  "    RM_PRREQN_QTN.RM_PR_PRNO,  RM_PRREQN_QTN.AD_FIN_FINYRID, ";
                //SQL = SQL  +  "    RM_PRREQN_QTN.RM_RMM_RM_CODE,  ";
                //SQL = SQL + "      RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,           ";
                //SQL = SQL  +  "    RM_PRREQN_QTN.RM_VM_VENDOR_CODE,RM_VM_VENDOR_NAME, RM_VENDOR_MASTER.RM_VM_FULL_NAME, ";
                //SQL = SQL  +  "    RM_PRREQN_QTN.RM_PRQ_UNIT_PRICE, RM_PRREQN_QTN.RM_PRQ_DISCOUNT,  ";
                //SQL = SQL  +  "    RM_VENDOR_MASTER.RM_VM_CREDIT_LIMIT, ";
                //SQL = SQL  +  "    RM_VENDOR_MASTER.RM_VM_CREDIT_PERIOD, ";
                //SQL = SQL  +  "    RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS, ";
                //SQL = SQL  +  "    SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_DESC ";
                //SQL = SQL  +  "    FROM  ";
                //SQL = SQL  +  "    RM_PRREQN_QTN, ";
                //SQL = SQL  +  "    RM_VENDOR_MASTER,SL_PAY_TYPE_MASTER ,RM_RAWMATERIAL_MASTER ";
                //SQL = SQL  +  "    WHERE   ";
                //SQL = SQL  +  "        RM_PRREQN_QTN.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                //SQL = SQL  +  "        AND  RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS =SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE ";
                //SQL = SQL  +  "        AND  RM_PRREQN_QTN.RM_RMM_RM_CODE  =  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                //SQL = SQL  +  "        AND RM_PRQ_SUPPLIERNO =  3     ";
                //SQL = SQL  +  "     )  ";
                //SQL = SQL  +  "    SUPP_THREE ";
                //SQL = SQL  +  "    WHERE    ";
                //SQL = SQL  +  "    MAIN_QRY.RM_PR_PRNO ='"+ txtPRNo +"'";
                //SQL = SQL  +  "    AND  MAIN_QRY.AD_FIN_FINYRID  = "+ mngrclass.FinYearID +"";
                //SQL = SQL  +  "    AND  MAIN_QRY.RM_RMM_RM_CODE = SUPP_ONE.RM_RMM_RM_CODE  (+) ";
                //SQL = SQL  +  "    AND  MAIN_QRY.RM_PR_PRNO = SUPP_ONE.RM_PR_PRNO   (+) ";
                //SQL = SQL  +  "    AND  MAIN_QRY.AD_FIN_FINYRID  = SUPP_ONE.AD_FIN_FINYRID     (+)  ";
                //SQL = SQL  +  "    AND  MAIN_QRY.RM_RMM_RM_CODE = SUPP_TWO.RM_RMM_RM_CODE  (+) ";
                //SQL = SQL  +  "    AND  MAIN_QRY.RM_PR_PRNO = SUPP_TWO.RM_PR_PRNO   (+) ";
                //SQL = SQL  +  "    AND  MAIN_QRY.AD_FIN_FINYRID  = SUPP_TWO.AD_FIN_FINYRID     (+)  ";
                //SQL = SQL  +  "    AND  MAIN_QRY.RM_RMM_RM_CODE = SUPP_THREE.RM_RMM_RM_CODE  (+) ";
                //SQL = SQL  +  "    AND  MAIN_QRY.RM_PR_PRNO = SUPP_THREE.RM_PR_PRNO  (+) ";
                //SQL = SQL  +  "    AND  MAIN_QRY.AD_FIN_FINYRID = SUPP_THREE.AD_FIN_FINYRID   (+) ";
                //SQL = SQL  +  " ORDER BY RM_PRD_SL_NO ASC";

                SQL = "  SELECT  ";
                SQL = SQL + "    RM_PR_PRNO,AD_FIN_FINYRID,RM_PRD_SL_NO, ";
                SQL = SQL + "    RM_RMM_RM_CODE,RM_RMM_RM_DESCRIPTION,RM_IM_ITEM_DTL_DESCRIPTION, ";
                SQL = SQL + "    RM_PRD_QTY,SUPP_ONE_VENDOR_CODE,SUPP_ONE_VENDOR_NAME,SUPP_ONE_FULL_NAME, ";
                SQL = SQL + "    SUPP_ONE_UNIT_PRICE,SUPP_ONE_DISCOUNT,SUPP_ONE_AMOUNT,SUPP_ONE_PAY_TYPE_DESC, ";
                SQL = SQL + "    SUPP_TWO_VENDOR_CODE,SUPP_TWO_VENDOR_NAME,SUPP_TWO_FULL_NAME,SUPP_TWO_UNIT_PRICE, ";
                SQL = SQL + "    SUPP_TWO_DISCOUNT,SUPP_TWO_AMOUNT,SUPP_TWO_PAY_TYPE_DESC, ";
                SQL = SQL + "    SUPP_THREE_VENDOR_CODE,SUPP_THREE_VENDOR_NAME,SUPP_THREE_FULL_NAME, ";
                SQL = SQL + "    SUPP_THREE_UNIT_PRICE,SUPP_THREE_DISCOUNT,SUPP_THREE_AMOUNT, ";
                SQL = SQL + "    SUPP_THREE_PAY_TYPE_DESC,  ";

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

                SQL = SQL + "    FROM RM_PUR_REQCOMPSHEETPRC_PRINTVW ";
                SQL = SQL + "    WHERE rm_pr_prno = '" + txtPRNo + "' ";
                SQL = SQL + "    AND ad_fin_finyrid = " + mngrclass.FinYearID + " ";


                dsPRStatus = clsSQLHelper.GetDataset(SQL);

                dsPRStatus.Tables[0].TableName = "RM_PUR_REQ_COMP_PRINT_PRC_VW";


                SQL = "  SELECT  ";
                SQL = SQL + " A.MAXPODATE ,B.RM_VM_VENDOR_CODE ,B.RM_VM_VENDOR_NAME ,B.RM_RMM_RM_CODE ,B.RM_RMM_RM_DESCRIPTION ,B.RM_POD_UNIT_PRICE  ";
                SQL = SQL + " FROM  ";
                SQL = SQL + " (  ";
                SQL = SQL + " SELECT MAX(PODATE) MAXPODATE, RM_RMM_RM_CODE MAXRM_RMM_RM_CODE  ";
                SQL = SQL + " FROM (  ";
                SQL = SQL + " SELECT  ";
                SQL = SQL + " MAX(RM_PO_MASTER.RM_PO_PO_DATE) PODATE, RM_PO_MASTER.RM_VM_VENDOR_CODE , RM_VENDOR_MASTER.RM_VM_VENDOR_NAME ,  ";
                SQL = SQL + " RM_PO_DETAILS.RM_RMM_RM_CODE , RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION , RM_PO_DETAILS.RM_POD_UNIT_PRICE  ";
                SQL = SQL + " FROM  ";
                SQL = SQL + " RM_PO_MASTER, RM_PO_DETAILS, RM_VENDOR_MASTER, RM_RAWMATERIAL_MASTER  ";
                SQL = SQL + " WHERE RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO  ";
                SQL = SQL + " AND RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID  ";
                SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE IN (  ";
                SQL = SQL + " SELECT RM_RMM_RM_CODE  ";
                SQL = SQL + " FROM  ";
                SQL = SQL + " RM_PR_DETAILS  ";
                SQL = SQL + " WHERE RM_PR_PRNO ='" + txtPRNo + "' )  ";
                SQL = SQL + " GROUP BY RM_PO_MASTER.RM_VM_VENDOR_CODE ,  ";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME , RM_PO_DETAILS.RM_RMM_RM_CODE ,RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION , RM_PO_DETAILS.RM_POD_UNIT_PRICE  ";
                SQL = SQL + " )  ";
                SQL = SQL + " GROUP BY RM_RMM_RM_CODE ) A ,  ";
                SQL = SQL + " (SELECT  ";
                SQL = SQL + " MAX(RM_PO_MASTER.RM_PO_PO_DATE) PODATE,RM_PO_MASTER.RM_VM_VENDOR_CODE ,RM_VENDOR_MASTER.RM_VM_VENDOR_NAME ,  ";
                SQL = SQL + "RM_PO_DETAILS.RM_RMM_RM_CODE ,RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION ,RM_PO_DETAILS.RM_POD_UNIT_PRICE  ";
                SQL = SQL + " FROM  ";
                SQL = SQL + " RM_PO_MASTER, RM_PO_DETAILS, RM_VENDOR_MASTER, RM_RAWMATERIAL_MASTER  ";
                SQL = SQL + " WHERE RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO  ";
                SQL = SQL + " AND RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID  ";
                SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE IN (  ";
                SQL = SQL + " SELECT RM_RMM_RM_CODE  ";
                SQL = SQL + " FROM  ";
                SQL = SQL + " RM_PR_DETAILS  ";
                SQL = SQL + " WHERE RM_PR_PRNO ='" + txtPRNo + "' )  ";
                SQL = SQL + " GROUP BY RM_PO_MASTER.RM_VM_VENDOR_CODE ,  ";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME , RM_PO_DETAILS.RM_RMM_RM_CODE ,RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION , RM_PO_DETAILS.RM_POD_UNIT_PRICE  ";
                SQL = SQL + " ) B  ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + " B.PODATE = A.MAXPODATE  ";
                SQL = SQL + " AND B.RM_RMM_RM_CODE = A.MAXRM_RMM_RM_CODE  ";


                dtPRStatus = clsSQLHelper.GetDataTableByCommand(SQL);

                dtPRStatus.TableName = "RM_PUR_REQ_PREV_DATA_PRINT_PRC_VW";

                dsPRStatus.Tables.Add(dtPRStatus);


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
            return dsPRStatus;

        }

        #endregion


        #region" Validation"

        public DataSet ValidateApprovedAmount ( string ProjectCode, string BudgetItem_Code, object mngrclassobj, string InsertionType, string ponO )
        {

            DataSet dtType = new DataSet();
            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                //if (InsertionType == "I")
                //{


                SQL = " SELECT NVL(SUM(RM_POD_TOTALAMT),0) TOTAL_PO_AMOUNT ";
                SQL = SQL + " FROM  ";
                SQL = SQL + "   RM_PO_MASTER,rm_po_project_details";
                SQL = SQL + " WHERE  ";
                SQL = SQL + "   RM_PO_MASTER.RM_PO_PONO=rm_po_project_details.RM_PO_PONO ";
                SQL = SQL + "   AND rm_po_project_details.PC_BUD_BUDGET_ITEM_CODE='" + BudgetItem_Code + "'";
                SQL = SQL + "   AND rm_po_project_details.GL_COSM_ACCOUNT_CODE='" + ProjectCode + "'";
                SQL = SQL + "   AND RM_PO_PO_STATUS<>'N' ";


                //}

                //else if (InsertionType == "U")
                //{
                //SQL = " SELECT NVL(SUM(WS_POD_TOTALAMT),0) TOTAL_PO_AMOUNT ";
                //SQL = SQL + " FROM  ";
                //SQL = SQL + "   WS_PO_MASTER,WS_PO_PROJECT_DETAILS";
                //SQL = SQL + " WHERE  ";
                //SQL = SQL + "   WS_PO_MASTER.WS_PO_PONO=WS_PO_PROJECT_DETAILS.WS_PO_PONO ";
                //SQL = SQL + "   AND WS_PO_PROJECT_DETAILS.PC_BUD_BUDGET_ITEM_CODE='" + BudgetItem_Code + "'";
                //SQL = SQL + "   AND WS_PO_PROJECT_DETAILS.GL_COSM_ACCOUNT_CODE='" + ProjectCode + "'";
                //SQL = SQL + "   AND WS_PO_PO_STATUS<>'N' ";
                //    SQL = SQL + "   and PO_MASTER.WS_PO_PONO <> '" + ponO + "' ";
                //}

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

        public DataSet TotalBudgetAmount ( string ProjectCode, string BudgetItem_Code, object mngrclassobj )
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


        #endregion

    }

    #region"Entity"

    public class PurchaseRequisitionPRCEntity
    {

        public PurchaseRequisitionPRCEntity ( )
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public string PRNo
        {
            get;
            set;
        }

        public DateTime PRDate
        {
            get;
            set;
        }

        public string Physical
        {
            get;
            set;
        }

        public DateTime ExpDate
        {
            get;
            set;
        }

        public string Remarks
        {
            get;
            set;
        }

        public string CancelRemarks
        {
            get;
            set;
        }

        public string StockType
        {
            get;
            set;
        }

        public double GrandTotal
        {
            get;
            set;
        }

        public string Created
        {
            get;
            set;
        }

        public string CreatedBy
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

        public string ApprovalRemarks
        {
            get;
            set;
        }

        public string HdlbCreateddBy
        {
            get;
            set;
        }

        public string HdlblVerifiedBy
        {
            get;
            set;
        }

        public string HdlblApprovedBy
        {
            get;
            set;
        }

        public string HdlblPRStatus
        {
            get;
            set;
        }

        public string CostCenter
        {
            get;
            set;
        }

        public string AssetCode
        {
            get;
            set;
        }

        public string Immediate
        {
            get;
            set;
        }

        public string Within2Days
        {
            get;
            set;
        }

        public string WithinWeek
        {
            get;
            set;
        }

        public string Within15Days
        {
            get;
            set;
        }


        public string Available
        {
            get;
            set;
        }

        public string NonAvailable
        {
            get;
            set;
        }

        public string Contact1
        {
            get;
            set;
        }
        public string Branch { get; set; }

    }

    public class PurchaseRequisitionPRCGridEntity
    {


        public string ProjectCostCode
        {
            get;
            set;
        }

        public string PRStCode
        {
            get;
            set;
        }
        public string PRDeptCode
        {
            get;
            set;
        }
        public string PRSourceCode
        {
            get;
            set;
        }

        public string PRBudgetItemCode
        {
            get;
            set;
        }

        public string PRItemCode
        {
            get;
            set;
        }

        public string PRUomCode
        {
            get;
            set;
        }

        public string PRDetDesc
        {
            get;
            set;
        }

        public double PRStkQty
        {
            get;
            set;
        }

        public double PRDirUseQty
        {
            get;
            set;
        }

        public double PRPrice
        {
            get;
            set;
        }

        public double PRAmnt
        {
            get;
            set;
        }

        public string PRItemRemarks
        {
            get;
            set;
        }

        public string PRPONo
        {
            get;
            set;
        }



        public double PRQtnPrice1
        {
            get;
            set;
        }

        public double PRQtnDiscount1
        {
            get;
            set;
        }

        public string PRQtnSupp1
        {
            get;
            set;
        }

        public object PRQtnDate1
        {
            get;
            set;
        }

        public string PRQtnApp1
        {
            get;
            set;
        }

        public string PRQtnNo1
        {
            get;
            set;
        }



        public double PRQtnPrice2
        {
            get;
            set;
        }

        public double PRQtnDiscount2
        {
            get;
            set;
        }


        public string PRQtnSupp2
        {
            get;
            set;
        }

        public object PRQtnDate2
        {
            get;
            set;
        }

        public string PRQtnApp2
        {
            get;
            set;
        }

        public string PRQtnNo2
        {
            get;
            set;
        }



        public double PRQtnPrice3
        {
            get;
            set;
        }

        public double PRQtnDiscount3
        {
            get;
            set;
        }

        public string PRQtnSupp3
        {
            get;
            set;
        }

        public object PRQtnDate3
        {
            get;
            set;
        }

        public string PRQtnApp3
        {
            get;
            set;
        }

        public string PRQtnNo3
        {
            get;
            set;
        }


        public string PRsAppStatus1
        {
            get;
            set;
        }

        public string PRsAppStatus2
        {
            get;
            set;
        }

        public string PRsAppStatus3
        {
            get;
            set;
        }


        public string sAppStatus
        {
            get;
            set;
        }

    }



    #endregion
}
