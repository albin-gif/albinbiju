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
using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Collections;
using System.Collections.Generic;

using System.Globalization;
using System.IO;
using System.Linq;
/// <summary>
/// Summary description for GeneralRmLogic
/// </summary>
/// 

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class GeneralRmLogic
    {


        static string sConString = Utilities.cnnstr;
        //   string scon = sConString; 
        OracleConnection ocConn = new OracleConnection(sConString.ToString());


        //OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList SQLArray = new ArrayList();
        String SQL = string.Empty;


        public GeneralRmLogic ( )
        {
            //
            // TODO: Add constructor logic here
            //

        }

        public object gtbudget_ItemCode = "";

        public string Projectcode
        {
            get;
            set;
        }

        public string Stcode
        {
            get;
            set;
        }


        #region "Validate FinYear"


        public string MonthClosingBranch ( DateTime dtTransDate, int iTransFinYr, string ModuleId, string BranchCode )
        { // jomy added 12 sep 2021 7.29 pm
            string sReturn;
            DataSet dsMonth = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT ad_mc_month, ad_mc_createdby, ad_mc_createddate";
                SQL = SQL + "   FROM ad_month_closing";
                SQL = SQL + "  WHERE AD_MD_MODULEID = '" + ModuleId + "' and AD_BR_CODE ='" + BranchCode + "'";
                SQL = SQL + "   AND ad_fin_finyrid = " + iTransFinYr + "";
                SQL = SQL + "   AND ad_mc_month = " + dtTransDate.Month + "";


                dsMonth = clsSQLHelper.GetDataset(SQL);

                if (dsMonth == null)
                {

                    return "Error Occured while Month Close Validation ";

                }


                if (dsMonth.Tables[0].Rows.Count > 0)
                {
                    // MsgBox("Month Closing is already done for " + MonthName(Month(dtTransDate)) + ".", MsgBoxStyle.Information, msMessageCaption)
                    return "Month Closing is already done for " + dtTransDate.Month + ".";

                }


            }
            catch (Exception ex)
            {
                sReturn = System.Convert.ToString(ex.Message);
                return sReturn;

            }
            return "CONTINUE";

        }


        //public string MonthClosing(DateTime dtTransDate, int iTransFinYr, string ModuleId)
        //{
        //    string sReturn;
        //    DataSet dsMonth = new DataSet();
        //    try
        //    {
        //        SqlHelper clsSQLHelper = new SqlHelper();


        //        SQL = " SELECT ad_mc_month, ad_mc_createdby, ad_mc_createddate";
        //        SQL = SQL + "   FROM ad_month_closing";
        //        SQL = SQL + "  WHERE AD_MD_MODULEID ='" + ModuleId + "'";
        //        SQL = SQL + "   AND ad_fin_finyrid = " + iTransFinYr + "";
        //        SQL = SQL + "   AND ad_mc_month = " + dtTransDate.Month + "";


        //        dsMonth = clsSQLHelper.GetDataset(SQL);

        //        if (dsMonth == null)
        //        {

        //            return "Error Occured while Month Close Validation 1.";

        //        }


        //        if (dsMonth.Tables[0].Rows.Count > 0)
        //        {


        //            // MsgBox("Month Closing is already done for " + MonthName(Month(dtTransDate)) + ".", MsgBoxStyle.Information, msMessageCaption)
        //            return "Month Closing is already done for " + dtTransDate.Month + ".";



        //        }


        //    }
        //    catch (Exception ex)
        //    {
        //        sReturn = System.Convert.ToString(ex.Message);
        //        return sReturn;

        //    }
        //    return "CONTINUE";

        //}



        //public string fnChkMonthClosing(DateTime dtTransDate, int iTransFinYr, string ModuleId)
        //{
        //    string sReturn;
        //    DataSet dsMonth = new DataSet();
        //    try
        //    {
        //        SqlHelper clsSQLHelper = new SqlHelper();


        //        SQL = " SELECT ad_mc_month, ad_mc_createdby, ad_mc_createddate";
        //        SQL = SQL + "   FROM ad_month_closing";
        //        SQL = SQL + "  WHERE AD_MD_MODULEID ='" + ModuleId + "'";
        //        SQL = SQL + "   AND ad_fin_finyrid = " + iTransFinYr + "";
        //        SQL = SQL + "   AND ad_mc_month = " + dtTransDate.Month + "";


        //        dsMonth = clsSQLHelper.GetDataset(SQL);

        //        if (dsMonth == null)
        //        {

        //            return "Error Occured while Month Close Validation 1.";

        //        }


        //        if (dsMonth.Tables[0].Rows.Count > 0)
        //        {


        //            // MsgBox("Month Closing is already done for " + MonthName(Month(dtTransDate)) + ".", MsgBoxStyle.Information, msMessageCaption)
        //            return "Month Closing is already done for " + dtTransDate.Month + ".";



        //        }


        //    }
        //    catch (Exception ex)
        //    {
        //        sReturn = System.Convert.ToString(ex.Message);
        //        return sReturn;

        //    }
        //    return "CONTINUE";

        //    //throw new NotImplementedException();
        //}

        public string fnChkMonthClosingTransaction ( DateTime dtTransDate, int iTransFinYr, string ModuleId, string BranchCode )
        {
            string sReturn;
            DataSet dsMonth = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT AD_MC_MONTH, AD_MC_CREATEDBY, AD_MC_CREATEDDATE";
                SQL = SQL + "   FROM AD_MONTH_CLOSING_ITEMS";
                SQL = SQL + "  WHERE AD_MI_ITEMID ='" + ModuleId + "' and AD_BR_CODE ='" + BranchCode + "'";
                SQL = SQL + "   AND ad_fin_finyrid = " + iTransFinYr + "";
                SQL = SQL + "   AND ad_mc_month = " + dtTransDate.Month + "";


                dsMonth = clsSQLHelper.GetDataset(SQL);

                if (dsMonth == null)
                {

                    return "Error Occured while Month Close Validation 1.";

                }


                if (dsMonth.Tables[0].Rows.Count > 0)
                {


                    // MsgBox("Month Closing is already done for " + MonthName(Month(dtTransDate)) + ".", MsgBoxStyle.Information, msMessageCaption)
                    return "Month Closing is already done for " + dtTransDate.Month + ".";



                }


            }
            catch (Exception ex)
            {
                sReturn = System.Convert.ToString(ex.Message);
                return sReturn;

            }
            return "CONTINUE";

            //throw new NotImplementedException();
        }


        #endregion

        #region "Validation Function Physucal stock entry  " 

        public string PhysicalstockEntryExists ( string sTcode, string sEntryDate, string BranchCode )
        {
            // JOmy P chacko 2 / jan 2013 checking the Physical stock entry against the selected period
            DataSet dsCnt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the  opening 

                SQL = " SELECT MAX(RM_PSEM_ENTRY_DATE) PSDATE ";
                SQL = SQL + " FROM RM_PHYSICAL_STOCK_MASTER ";

                //SQL = " SELECT count(*) StkNo from  RM_PHYSICAL_STOCK_master ";
                //SQL = SQL + " where RM_PSEM_ENTRY_DATE <= '" + System.Convert.ToDateTime(sEntryDate).ToString("dd-MMM-yyyy") + "' ";

                //'" + System.Convert.ToDateTime(sEntryDate).ToString("dd-MMM-yyyy") + "' between RM_PSEM_FROM_DATE  and   RM_PSEM_TO_DATE" ; 

                SQL = SQL + " where  SALES_STN_STATION_CODE ='" + sTcode + "'";
                SQL = SQL + " and AD_BR_CODE= '" + BranchCode + "' ";

                dsCnt = clsSQLHelper.GetDataset(SQL);

                //if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                //{
                //    return "Unable to create the entry since physcial stock entry exists against the selected period  kindly change the date and try ";
                //}
                if (!string.IsNullOrWhiteSpace(dsCnt.Tables[0].Rows[0]["PSDATE"].ToString()))
                {

                    if (Convert.ToDateTime(dsCnt.Tables[0].Rows[0]["PSDATE"]) >= Convert.ToDateTime(sEntryDate))
                    {
                        return "Unable to create the entry since physcial stock entry exists against the selected period  kindly change the date and try ";
                    }

                }



            }
            catch (Exception ex)
            {
                return System.Convert.ToString(ex.Message);
            }
            return "CONTINUE";
        }


        public string PhysicalstockEntryExistsWithoutStation ( string sEntryDate, string BranchCode )
        {
            DataSet dsCnt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the  opening 
                // form receit approval  allowing mulitple station its difficulat to maange however just trying 

                SQL = " select  count(*) StkNo from  RM_PHYSICAL_STOCK_master ";
                SQL = SQL + " where     '" + System.Convert.ToDateTime(sEntryDate).ToString("dd-MMM-yyyy") + "' between RM_PSEM_FROM_DATE  and   RM_PSEM_TO_DATE";
                SQL = SQL + " and AD_BR_CODE= '" + BranchCode + "' ";




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

        public string PhysicalstockEntryProjectExistsWithoutStation(string sEntryDate, string BranchCode)
        {
            DataSet dsCnt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the  opening 
                // form receit approval  allowing mulitple station its difficulat to maange however just trying 

                SQL = " select  count(*) StkNo from  RM_PHYSICAL_STOCK_PROJ_MASTER ";
                SQL = SQL + " where     '" + System.Convert.ToDateTime(sEntryDate).ToString("dd-MMM-yyyy") + "' between RM_PSPR_FROM_DATE  and   RM_PSPR_TO_DATE";
                SQL = SQL + " and AD_BR_CODE= '" + BranchCode + "' ";




                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "Unable to create the entry since project consumptions stock entry exists  against the selected period  kindly change the date and try ";
                }






            }
            catch (Exception ex)
            {
                return System.Convert.ToString(ex.Message);
            }
            return "CONTINUE";
        }




        #endregion


        #region "WeighBridge Integration"

        public DataTable FetchWeighAPIData(string station)//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT   ";
                SQL = SQL + "    SALES_STN_STATION_CODE, API_CONNECTION_LINK ";
                SQL = SQL + " FROM  ";
                SQL = SQL + "    PR_DEFAULTS_IMP_PATH ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + " IMP_ID='WEIGHT_BRIDGE' ";
                SQL = SQL + " AND SALES_STN_STATION_CODE='" + station + "' ";


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
        public double GetGrantedProjectCount ( string UserName )
        {

            DataTable dtReturn = new DataTable();
            double dGrantedProjectCount = 0;
            try
            {

                String SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "    Select count(*)      GRANTEDPROJECTCOUNT   FROM SL_GRANTED_PROJECT_USERS ";
                SQL = SQL + "  WHERE AD_UM_USERID =  '" + UserName + "'";

                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);
                dGrantedProjectCount = double.Parse(dtReturn.Rows[0]["GRANTEDPROJECTCOUNT"].ToString());



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
            return dGrantedProjectCount;

        }
        
        public DataTable FillDepartmentUserGranted(string UserName)
        {

            DataTable dt = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                ///SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = "  SELECT  HR_DEPT_MASTER.HR_DEPT_DEPT_CODE CODE , HR_DEPT_DEPT_DESC NAME ";
                SQL = SQL + "   FROM     HR_DEPT_MASTER  ";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + "  WHERE  HR_DEPT_DEPT_CODE IN ( SELECT     HR_DEPT_DEPT_CODE FROM ";
                    SQL = SQL + "     AD_WS_GRANTED_BR_DEPT_USERS WHERE    AD_UM_USERID   ='" + UserName + "') ";
                    // Abin added on 03 mar 2023// two grant option removal

                }
                SQL = SQL + "    ORDER BY HR_DEPT_DEPT_CODE ASC ";

                dt = clsSQLHelper.GetDataTableByCommand(SQL);

            }

            catch (Exception Exception)
            { }
            return dt;
        }



        public DataSet FillStockType ( )
        {
            DataSet dtTable = new DataSet();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "  select   RM_PO_STOCK_TYPE_CODE  CODE, RM_PO_STOCK_TYPE_NAME  NAME from  RM_STOCK_TYPE_VW     order by RM_po_stock_type_sortid   asc";



                dtTable = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
            }

            return dtTable;
        }

        public DataTable GetBranchData ( )
        {
            DataTable dtBranch = new DataTable();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL
                  = "SELECT  " +
                      " AD_BR_CODE, AD_BR_NAME, AD_BR_DOC_PREFIX,  " +
                      "   AD_BR_SORT_ID, AD_BR_POBOX, AD_BR_ADDRESS,  " +
                      "   AD_BR_CITY, AD_BR_PHONE, AD_BR_FAX,  " +
                      "   AD_BR_SPONSER_NAME, AD_BR_TRADE_LICENSE_NO, AD_BR_LIC_ISSUE_DATE,  " +
                      "   AD_BR_LIC_EXPIRY_DATE, AD_BR_EMAIL_ID, AD_BR_ACCOUNT_PERIOD,  " +
                      "   AD_BR_POSTING_METHOD, AD_BR_AUDITOR_NAME, AD_BR_AUDITOR_ADDRESS,  " +
                      "   AD_BR_REMARKS, AD_BR_CREATEDBY, AD_BR_CREATEDATE,  " +
                      "   AD_BR_DELETESTATUS, AD_BR_FINYRFROM, AD_BR_UPDATEDDATE,  " +
                      "   AD_BR_UPDATEDBY, AD_BR_WEB_SITE, AD_BR_TAX_VAT_REG_NUMBER,  " +
                      "   AD_BR_WS_VISIBILE_YN, AD_BR_PROJECT_VISIBILE_YN, AD_BR_ACTIVESTATUS_YN,  " +
                      "   AD_BR_READYMIX_VISIBILE_YN, AD_BR_BLOCK_VISIBILE_YN, AD_BR_PRECAST_VISIBILE_YN,  " +
                      "   GL_BM_BNK_CODE " +
                      "   FROM AD_BRANCH_MASTER  ORDER  BY  AD_BR_SORT_ID ASC   ";



                dtBranch = clsSQLHelper.GetDataTableByCommand(SQL);
                dtBranch.TableName = "AD_BRANCH_MASTER";


                DataTable dtRptPath = new DataTable();
                SQL = "SELECT   AD_REPORTPATH, AD_PHOTOPATH, AD_XLEXPORT_PATH,  " +
                 "   AD_ATTACHMENT_PATH, AD_DATA_EXPORT_PATH, AD_DATA_IMPORT_PATH,  " +
                 "   AD_DATA_EXPORT_MRTK_PATH, AD_DATA_IMPORT_MRTK_PATH, AD_BRANCH_RM_DOC_NO_YN,  " +
                 "   AD_BRANCH_GL_DOC_NO_YN, AD_BRANCH_WS_DOC_NO_YN, AD_BRANCH_HR_DOC_NO_YN,  " +
                 "   AD_BRANCH_SL_DOC_NO_YN, AD_BRANCH_PR_DOC_NO_YN, AD_BRANCH_FA_DOC_NO_YN " +
                 "FROM  AD_DOCUMENT_DEFAULTS ";

                dtRptPath = clsSQLHelper.GetDataTableByCommand(SQL);
                string ReportPath = dtRptPath.Rows[0]["AD_REPORTPATH"].ToString();

                FileStream fs;
                BinaryReader br;

                dtBranch.Columns.Add("Branch_Logo_Image", System.Type.GetType("System.Byte[]"));

                string sBrachCodeImage = "";

                //   fs = new FileStream(mngrclass.ReportPath + "Report_Logo.jpg", FileMode.Open);



                for (int i = 0; i < dtBranch.Rows.Count; i++)
                {
                    sBrachCodeImage = dtBranch.Rows[i]["AD_BR_CODE"].ToString();
                    fs = new FileStream(ReportPath + sBrachCodeImage + ".jpg", FileMode.Open);

                    br = new BinaryReader(fs);
                    byte[] imgbyte = new byte[fs.Length + 1];
                    imgbyte = br.ReadBytes(Convert.ToInt32((fs.Length)));
                    dtBranch.Rows[i]["Branch_Logo_Image"] = imgbyte;
                    br.Close();
                    fs.Close();
                    fs = null;
                    br = null;
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
            return dtBranch;
        }
        //get branch wise documnt number 
        public string GetDocumnentNoBranchWise ( string sFromCode, string sBranchcode, int FinYearID )
        {
            DataSet dsReturn = new DataSet();
            string JSONString = "";
            try
            {


                SQL = string.Empty;


                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  SELECT AD_DN_NEXT_NO ,AD_BRANCH_MASTER.AD_BR_DOC_PREFIX   FROM AD_DOC_NUMBER_BRANCH ,  AD_BRANCH_MASTER  WHERE  ";
                SQL = SQL + "   AD_DOC_NUMBER_BRANCH.AD_BR_CODE =   AD_BRANCH_MASTER.AD_BR_CODE  ";
                SQL = SQL + "  and    AD_BRANCH_MASTER.AD_BR_CODE = '" + sBranchcode + "'";
                SQL = SQL + "  and   AD_MI_ITEMID='" + sFromCode + "' AND AD_FIN_FINYRID=" + FinYearID + "";

                dsReturn = clsSQLHelper.GetDataset(SQL);

                dsReturn.Tables[0].TableName = "AD_DOC_NUMBER";


                JSONString = JsonConvert.SerializeObject(dsReturn, Formatting.None);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return JSONString;
        }


        public string GetCode ( string Code )
        {
            DataTable dsCnt = new DataTable();

            string Subcategorycode = null;
            int SeqNo = 0;

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "  SELECT nvl( MAX(SUBSTR(RM_SCM_SUBCATEGORY_CODE,3,2)),0)+1  RM_SCM_SUBCATEGORY_CODE  ";
                SQL = SQL + "  FROM RM_CATEGORY_SUB_MASTER   WHERE RM_CM_CATEGORY_CODE =  '" + Code + "' and RM_SCM_SUBCAT_CODE_PATTERN_YN='Y' ";

                dsCnt = clsSQLHelper.GetDataTableByCommand(SQL);

                SeqNo = int.Parse(dsCnt.Rows[0].ItemArray[0].ToString());

                if (SeqNo < 9)
                {
                    Subcategorycode = "0" + SeqNo;
                }
                else if (SeqNo < 99)
                {
                    Subcategorycode = SeqNo.ToString();
                }
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return Subcategorycode;
        }



        // GET DOCUMENT NUMBER 
        public string GetDocumnentNo ( string sFromCode, object mngrclassobj )
        {
            DataTable dtCode = new DataTable();
            try
            {


                SQL = string.Empty;
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT AD_DN_NEXT_NO FROM AD_DOC_NUMBER WHERE  AD_MI_ITEMID='" + sFromCode + "' AND AD_FIN_FINYRID=" + mngrclass.FinYearID + "";

                dtCode = clsSQLHelper.GetDataTableByCommand(SQL);


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return System.Convert.ToString(dtCode.Rows[0]["AD_DN_NEXT_NO"]);
        }


        public DataTable GetPayTemrs ( )
        {
            DataTable dtCode = new DataTable();
            try
            {


                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  ";
                SQL = SQL + " SELECT  ";
                SQL = SQL + " SALES_PAY_TYPE_CODE CODE , SALES_PAY_TYPE_DESC  NAME,SALES_PAY_NO_DAYS   ";
                SQL = SQL + " FROM SL_PAY_TYPE_MASTER ORDER BY SALES_PAY_TYPE_DESC  ASC  ";

                dtCode = clsSQLHelper.GetDataTableByCommand(SQL);


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dtCode;
        }



        public DataTable FillView ( string TypeID, string SuppCode )
        {
            DataTable dtType = new DataTable();
            try
            //-- this look i up is using various places os take care // Jomy + jins  11 / jan 23013
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                if (TypeID == "SUPPLLIERAPPRVL")
                {
                    SQL = " SELECT DISTINCT ";
                    SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name ";
                    SQL = SQL + " FROM RM_VENDOR_MASTER, RM_PO_MASTER ";
                    SQL = SQL + " WHERE RM_PO_MASTER.RM_PO_ENTRY_TYPE = 'PURCHASE'";
                    SQL = SQL + " AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_PO_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " ORDER BY RM_VM_VENDOR_NAME ASC ";
                }
                else if (TypeID == "SUPPLLIER")
                {
                    SQL = " SELECT ";
                    SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name ";
                    SQL = SQL + " FROM RM_VENDOR_MASTER";
                    //SQL = SQL + " WHERE RM_PO_MASTER.RM_PO_ENTRY_TYPE = 'PURCHASE'";
                    SQL = SQL + " ORDER BY RM_VM_VENDOR_NAME ASC ";
                }
                else if (TypeID == "FPSALL")
                {
                    SQL = " SELECT ";
                    SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name ";
                    SQL = SQL + " FROM RM_VENDOR_MASTER";
                    //SQL = SQL + " WHERE RM_PO_MASTER.RM_PO_ENTRY_TYPE = 'PURCHASE'";
                    SQL = SQL + " ORDER BY RM_VM_VENDOR_NAME ASC ";
                }

                else if (TypeID == "ACCCODE" || TypeID == "ADJACCODE")
                {
                    SQL = " SELECT GL_COAM_ACCOUNT_CODE Code, GL_COAM_ACCOUNT_NAME Name";
                    SQL = SQL + " FROM GL_COA_MASTER WHERE GL_COAM_DELETESTATUS = 0 AND ";
                    SQL = SQL + " GL_COAM_LEDGER_TYPE = 'SUB' AND GL_COAM_ACCOUNT_STATUS = 'ACTIVE'";
                    SQL = SQL + " ORDER BY GL_COAM_ACCOUNT_CODE ";
                }
                else if (TypeID == "FPSSTATION")
                {
                    SQL = " SELECT ";
                    SQL = SQL + " SALES_STN_STATION_CODE Code, SALES_STN_STATION_NAME Name ";
                    SQL = SQL + " FROM SL_STATION_MASTER";
                }
                else if (TypeID == "FPSRM")
                {
                    SQL = " SELECT DISTINCT ";
                    SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code , RM_RMM_RM_DESCRIPTION Name ";
                    SQL = SQL + " FROM RM_RAWMATERIAL_MASTER ";

                    SQL = SQL + " ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC ";
                }
                else if (TypeID == "FPSTRANS")
                {
                    SQL = " SELECT ";
                    SQL = SQL + " RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME  Name ";
                    SQL = SQL + " FROM  RM_VENDOR_MASTER  where ";
                    SQL = SQL + " UPPER(RM_VM_VENDOR_TYPE) IN ( 'TRANSPORTER','BOTH')  ORDER BY RM_VM_VENDOR_NAME ";
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




        #region "Branch View"

        public List<BranchList> GetFirstDefaultBranch ( string username )
        {
            DataTable dtReturn = new DataTable();
            List<BranchList> ietmsListRet = new List<BranchList>();

            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();





                SQL = "SELECT    CODE ,    NAME  , AD_BR_SORT_ID  FROM  ";
                SQL = SQL + "    ( SELECT AD_BR_CODE CODE , AD_BR_NAME  NAME  ,  AD_BR_SORT_ID AD_BR_SORT_ID FROM AD_BRANCH_MASTER  ";
                SQL = SQL + " WHERE  AD_BR_ACTIVESTATUS_YN ='Y' ";
                if (username != "ADMIN")
                {
                    SQL = SQL + "  AND  AD_BR_CODE in (SELECT AD_BR_CODE FROM AD_USER_GRANTED_BRANCH WHERE  AD_UM_USERID = '" + username + "') ";


                }
                SQL = SQL + "    ORDER BY  AD_BR_SORT_ID  )  ";
                SQL = SQL + "    WHERE ROWNUM =1  ";



                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);

                ietmsListRet = (from DataRow dr in dtReturn.Rows
                                select new BranchList()
                                {
                                    AD_BR_CODE = dr["CODE"].ToString(),
                                    AD_BR_NAME = dr["NAME"].ToString(),
                                    AD_BR_SORT_ID = double.Parse(dr["AD_BR_SORT_ID"].ToString())
                                }).ToList();


                //   var minValue = ietmsListRet.Min(p => p.AD_BR_SORT_ID);

                //var  oItem = ietmsListRet.Where(p => p.AD_BR_SORT_ID ==minValue)
                // .FirstOrDefault();

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
            return ietmsListRet;
        }


        public List<BranchSationList> GetFirstDefaultBranchStation ( string username )
        {
            DataTable dtReturn = new DataTable();
            List<BranchSationList> ietmsListRet = new List<BranchSationList>();

            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();




                SQL = "SELECT    CODE ,    NAME  , AD_BR_SORT_ID ,SALES_GL_STATION_SORT_ID , SALES_STN_STATION_CODE ,    SALES_STN_STATION_NAME   FROM  ";
                SQL = SQL + " ( ";
                SQL = SQL + " SELECT   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE CODE , SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME NAME , AD_BR_SORT_ID ,  SALES_GL_STATION_SORT_ID , ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE    , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME    , ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.GL_COSM_ACCOUNT_CODE  ";

                SQL = SQL + " FROM ";
                SQL = SQL + "   SL_STATION_BRANCH_MAPP_DTLS_VW WHERE   ";
                SQL = SQL + "    SALES_STATION_BRANCH_STATUS_AI ='A' and SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_GL_STATION ='Y'    ";


                if (username != "ADMIN")
                {
                    SQL = SQL + "   AND SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE in (select AD_BR_CODE from  ";
                    SQL = SQL + "  AD_USER_GRANTED_BRANCH where AD_UM_USERID = '" + username + "')   ";
                }
                if (username != "ADMIN")
                {
                    SQL = SQL + "  AND SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE IN ( SELECT     SALES_STN_STATION_CODE FROM ";
                    SQL = SQL + "     WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + username + "') ";
                }
                SQL = SQL + "    ORDER BY  AD_BR_SORT_ID ,SALES_GL_STATION_SORT_ID    )  ";
                SQL = SQL + "    WHERE ROWNUM =1  ";


                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);

                ietmsListRet = (from DataRow dr in dtReturn.Rows
                                select new BranchSationList()
                                {
                                    AD_BR_CODE = dr["CODE"].ToString(),
                                    AD_BR_NAME = dr["NAME"].ToString(),
                                    AD_BR_SORT_ID = double.Parse(dr["AD_BR_SORT_ID"].ToString()),
                                    SALES_STN_STATION_CODE = dr["SALES_STN_STATION_CODE"].ToString(),
                                    SALES_STN_STATION_NAME = dr["SALES_STN_STATION_NAME"].ToString(),
                                    SALES_GL_STATION_SORT_ID = double.Parse(dr["AD_BR_SORT_ID"].ToString())

                                }).ToList();


                //   var minValue = ietmsListRet.Min(p => p.AD_BR_SORT_ID);

                //var  oItem = ietmsListRet.Where(p => p.AD_BR_SORT_ID ==minValue)
                // .FirstOrDefault();

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
            return ietmsListRet;
        }


        public DataTable BranchView ( string UserName )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT AD_BR_CODE CODE, AD_BR_NAME NAME ";
                SQL = SQL + " FROM AD_BRANCH_MASTER ";
                if (UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                    SQL = SQL + "  where     AD_BR_CODE  IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                }
                SQL = SQL + " order by CODE asc ";
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


        public string BranchType ( string sBranchCode )
        {
            string JSONString = "";
            DataSet dsReturn = new DataSet();
            string sRetVal = "";
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT   AD_BR_READYMIX_VISIBILE_YN , AD_BR_BLOCK_VISIBILE_YN ,  AD_BR_PRECAST_VISIBILE_YN   ";
                SQL = SQL + " FROM AD_BRANCH_MASTER where AD_BR_CODE ='" + sBranchCode + "'";

                dsReturn = clsSQLHelper.GetDataset(SQL);
                dsReturn.Tables[0].TableName = "BRANCH_TYPE";

                ////sRetVal = dtType.Rows[0]["AD_BR_READYMIX_VISIBILE_YN"].ToString();
                ////if (sRetVal == "Y")   
                ////        { return "READYMIX";
                ////}

                ////sRetVal = dtType.Rows[0]["AD_BR_BLOCK_VISIBILE_YN"].ToString();
                ////if (sRetVal == "Y")
                ////{
                ////    return "BLOCK";
                ////}
                ////sRetVal = dtType.Rows[0]["AD_BR_PRECAST_VISIBILE_YN"].ToString();
                ////if (sRetVal == "Y")
                ////{
                ////    return "PRECAST";
                ////}
                ///
                JSONString = JsonConvert.SerializeObject(dsReturn, Formatting.None);


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
            return JSONString;

        }

        // jomy adddeed 04 aug 2021
        public List<BranchSationList> GetBranchStationGrantedList ( string SelectedBranchCode, string UserName )
        {
            DataTable dtReturn = new DataTable();
            List<BranchSationList> ietmsListRet = new List<BranchSationList>();

            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE, SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME, AD_BR_SORT_ID , ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE    , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME    , ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.GL_COSM_ACCOUNT_CODE  ";

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



                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);

                ietmsListRet = (from DataRow dr in dtReturn.Rows
                                select new BranchSationList()
                                {
                                    AD_BR_CODE = dr["AD_BR_CODE"].ToString(),
                                    AD_BR_NAME = dr["AD_BR_NAME"].ToString(),
                                    AD_BR_SORT_ID = double.Parse(dr["AD_BR_SORT_ID"].ToString()),
                                    SALES_STN_STATION_CODE = dr["SALES_STN_STATION_CODE"].ToString(),
                                    SALES_STN_STATION_NAME = dr["SALES_STN_STATION_NAME"].ToString(),
                                    GL_COSM_ACCOUNT_CODE = dr["GL_COSM_ACCOUNT_CODE"].ToString()
                                }).ToList();



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
            return ietmsListRet;
        }



        public DataTable GetBranchStationGrantedDatatable ( string SelectedBranchCode, string UserName )
        {  // jomy and jerring for the form filling 03 jun 2022 
            DataTable dtReturn = new DataTable();
            List<BranchSationList> ietmsListRet = new List<BranchSationList>();

            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT  distinct   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE, SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME  ";

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

        #region "Get last day o the month "
        public DataSet GetLastDay ( DateTime dtLastDepDate )
        {
            string strCondition = string.Empty;
            DataSet dt = new DataSet();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "Select To_Char(Last_Day('" + Convert.ToDateTime(dtLastDepDate).ToString("dd-MMM-yyyy") + "'),'DD-MON-YYYY') LASTDAY From Dual";

                dt = clsSQLHelper.GetDataset(SQL);
            }

            catch (Exception ex)
            { }
            return dt;
        }



        #endregion

        #region "approval Checking " 

        public int HideApprovlControlRetunInt ( string UserName, string sItemId )
        {
            DataTable dtHideApprovlControl = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                /// SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "";
                SQL = " SELECT  count(*) CNT";
                SQL = SQL + "   FROM   ad_mail_approval_users";
                SQL = SQL + "  WHERE  ";
                SQL = SQL + "      ad_mail_approval_users.ad_um_userid = '" + UserName + "'";
                SQL = SQL + "    AND  ad_mail_approval_users.AD_AF_ID  = '" + sItemId + "'";

                dtHideApprovlControl = clsSQLHelper.GetDataTableByCommand(SQL);
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return 0;
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return int.Parse(dtHideApprovlControl.Rows[0]["CNT"].ToString());
        }



        public DataTable HideApprovlControl ( string UserName, string sItemId )
        {
            DataTable dtHideApprovlControl = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                //   SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "";
                SQL = " SELECT  count(*) cnt";
                SQL = SQL + "   FROM   ad_mail_approval_users";
                SQL = SQL + "  WHERE  ";
                SQL = SQL + "      ad_mail_approval_users.ad_um_userid = '" + UserName + "'";
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

        #endregion

        #region "CostAccount"

        public DataTable CostAccountView ( )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT CODE,NAME ,GROUPNAME,GL_COSM_BUDGET_VAL_YN ";
                SQL = SQL + " FROM GL_COSTING_MASTER_VW ";
                SQL = SQL + " WHERE GL_COSM_ACCOUNT_STATUS ='ACTIVE'";// Active Project only needs to come 
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

        public DataTable CostAccountView ( string Branch, string UserName, string PoStockType )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                double dGrantedProjectCount = 0;

                dGrantedProjectCount = GetGrantedProjectCount(UserName);

                // this   .PK_GET_GRANTED_PROJECT 

                //<<<<<<<<<<<<<<<<<< this is uspplose to be like this what we have done in workship  

                //////   sSQL:=
                //////   ' SELECT CODE,      NAME,   CUSTCODE,  CUSTNAME,  AD_BR_CODE, AD_BR_NAME,'
                //////|| '   SALES_STN_STATION_CODE,  SALES_STN_STATION_NAME,  HR_DEPT_DEPT_CODE,    HR_DEPT_DEPT_DESC,'
                //////|| ' GL_COSM_ACCOUNT_STATUS,   GL_COSM_BUDGET_VAL_YN, TYPECODE, GROUPCODE, GROUPNAME,'
                //////|| '     GL_COSM_ENTRY_DR_VISIBILE_YN,   SORTID,  LINKED_FROM_PROJECT_YN,   LINKED_TO_DEPT_YN'
                //////|| '  FROM GL_COSTING_MASTER_VW'
                //////|| ' WHERE  GL_COSM_ACCOUNT_STATUS =''ACTIVE''';

                //////       IF(ws_po_stock_type = 'SI') then
                //////        -- - SITE SRV - (PROJECT)ALWAYS WILL COME PRJECT THERE FOR THE BRANCH LINKING SHOULD BE APPLICED AND WE CAN APPLY-- JOMY 22 JUN 2021


                //////      sSQl:= sSQl || ' AND ENQUIRY_STATUS = ''LIVE''    AND (   linked_to_Framwork_yn = ''Y'' OR linked_from_project_yn = ''Y'')';

                //////       IF(dGrantedProjectCount > 0)  THEN
                //////     sSQl := sSQl || ' AND CODE IN (SELECT SALES_PM_PROJECT_CODE   FROM SL_GRANTED_PROJECT_USERS WHERE AD_UM_USERID =' || CHR(39) || m_user_id || CHR(39) || ')';
                //////       END IF;
                //////       IF(dGrantedProjectCount > 0)
                //////       THEN
                //////       sSQl := sSQl || ' AND GROUPCODE IN (SELECT GL_COSM_ACCOUNT_GROUP   FROM SL_GRANTED_PROJECT_GROUP_USERS WHERE AD_UM_USERID =' || CHR(39) || m_user_id || CHR(39) || ')';
                //////       END IF;

                //////       IF(m_user_id <> 'ADMIN')
                //////   THEN
                //////           sSQl := sSQl || '   and AD_BR_CODE in (SELECT AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID = ' || CHR(39) || m_user_id || CHR(39) || ')';
                //////       END IF;



                //////  else
                //////           --JOMY COMMENTD SINCE IF THE DIVSION NOT SELECTEDTHEN SHOULD BE ALLOW THE DIVISION IN THE VISION
                //////         -- JOMY AND GIBN corrected in costing master  if the  prioject and framwork linked then only we can do the branch mapping else case we could not compare
                //////    sSQl:= sSQl || '    AND CODE IN'
                //////      || ' (SELECT GL_COSM_ACCOUNT_CODE    FROM WS_DEFAULTS_PR_PO_LOOK_UP  WHERE WS_PO_STOCK_TYPE_CODE = ' || CHR(39) || ws_po_stock_type || CHR(39) || ')';
                //////       end if ;

                ////////////////////////////////////////////////
                SQL = " SELECT CODE,NAME ,GROUPNAME,GL_COSM_BUDGET_VAL_YN ,AD_BR_CODE ";
                SQL = SQL + " FROM GL_COSTING_MASTER_VW ";
                SQL = SQL + " WHERE GL_COSM_ACCOUNT_STATUS ='ACTIVE'";
                //SQL = SQL + "   AND LINKED_FROM_PROJECT_YN = 'Y'  AND LINKED_TO_DEPT_YN = 'Y' AND AD_BR_CODE ='" + Branch + "'";
                if (PoStockType == "ST")
                {
                    SQL = SQL + " and   GL_COSTING_MASTER_VW.CODE    ";
                    SQL = SQL + "IN  (  SELECT GL_COSM_ACCOUNT_CODE FROM RM_DEFAULTS_PR_PO_LOOK_UP ";
                    SQL = SQL + "WHERE  PARAMETER_DESC ='PR_PO_COST_AND_PO_TYPE_MAPP' and RM_PO_STOCK_TYPE_CODE='ST')      ";
                }
                else
                {
                    SQL = SQL + " AND ENQUIRY_STATUS = 'LIVE'    AND (   linked_to_Framwork_yn = 'Y' OR linked_from_project_yn = 'Y')";
                    if (UserName != "ADMIN")
                    {
                        //SQL = SQL + "   AND CODE IN (SELECT SALES_PM_PROJECT_CODE   FROM SL_GRANTED_PROJECT_USERS WHERE AD_UM_USERID ='" + UserName + "')";

                        //SQL = SQL + "  AND GROUPCODE IN (SELECT GL_COSM_ACCOUNT_GROUP   FROM SL_GRANTED_PROJECT_GROUP_USERS WHERE AD_UM_USERID ='" + UserName + "')";


                        if (dGrantedProjectCount > 0)
                        {
                            SQL = SQL + " AND CODE IN (SELECT SALES_PM_PROJECT_CODE   FROM SL_GRANTED_PROJECT_USERS WHERE AD_UM_USERID  ='" + UserName + "')";
                        }
                        if (dGrantedProjectCount > 0)
                        {
                            SQL = SQL + " AND GROUPCODE IN (SELECT GL_COSM_ACCOUNT_GROUP   FROM SL_GRANTED_PROJECT_GROUP_USERS WHERE AD_UM_USERID  ='" + UserName + "')";

                        }

                        SQL = SQL + "  and AD_BR_CODE in (SELECT AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "')";
                    }
                }


                ////if (UserName != "ADMIN")
                ////{  // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                ////    SQL = SQL + "  and     AD_BR_CODE IN  ";
                ////    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                ////}
                ////SQL = SQL + "     UNION ALL  ";
                ////SQL = SQL + "    SELECT CODE,NAME ,GROUPNAME,GL_COSM_BUDGET_VAL_YN  ,AD_BR_CODE ";
                ////SQL = SQL + " FROM GL_COSTING_MASTER_VW  ";
                ////SQL = SQL + " WHERE GL_COSM_ACCOUNT_STATUS ='ACTIVE' ";
                ////SQL = SQL + " AND   LINKED_FROM_PROJECT_YN  ='N' AND    GL_COSM_ENTRY_DR_VISIBILE_YN ='Y' AND  LINKED_TO_DEPT_YN='Y' ";
                ////SQL = SQL + " and   GL_COSTING_MASTER_VW.CODE    ";
                ////SQL = SQL + "IN  (  SELECT GL_COSM_ACCOUNT_CODE FROM RM_DEFAULTS_PR_PO_LOOK_UP ";
                ////SQL = SQL + "WHERE  PARAMETER_DESC ='PR_PO_COST_AND_PO_TYPE_MAPP' and RM_PO_STOCK_TYPE_CODE='ST')      ";


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


        #endregion

        #region "keypress Enter"
        public string FetchStationGridKeypressCodeGrantBranchcode ( string sCode, string UserName, string SelectedBranchCode )
        {
            string sRetName = "";

            DataTable dtType = new DataTable();

            try
            {

                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                //SQL = "   SELECT  ";

                //SQL = SQL + "   SL_STATION_MASTER.SALES_STN_STATION_CODE  ,SL_STATION_MASTER.SALES_STN_STATION_NAME    ";

                //SQL = SQL + "   FROM  SL_STATION_MASTER";

                //SQL = SQL + "  where   SL_STATION_MASTER.SALES_STN_STATION_CODE   ='" + sCode + "'";


                SQL = " SELECT   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE, SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME, ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME , ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.GL_COSM_ACCOUNT_CODE  ";
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
                SQL = SQL + "  and    SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE   ='" + sCode + "'";

                SQL = SQL + "  ORDER BY   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE , SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_SORT_ID    ASC  ";




                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

                if (dtType.Rows.Count > 0)
                {
                    sRetName = dtType.Rows[0]["SALES_STN_STATION_NAME"].ToString();
                }

                else
                {
                    sRetName = "Incorrect Station Code.Kindly Enter Correct Code";
                }
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();

                objLogWriter.WriteLog(ex);

                sRetName = "Error";
            }

            finally
            {
                //ocConn.Close();

                //ocConn.Dispose();
            }

            return sRetName;
        }

        public String FetchStationGridKeypressCode ( string sCode )
        {
            string sRetName = "";

            DataTable dtType = new DataTable();

            try
            {

                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "   SELECT  ";

                SQL = SQL + "   SL_STATION_MASTER.SALES_STN_STATION_CODE CODE,SL_STATION_MASTER.SALES_STN_STATION_NAME NAME  ";

                SQL = SQL + "   FROM  SL_STATION_MASTER";

                SQL = SQL + "  where   SL_STATION_MASTER.SALES_STN_STATION_CODE   ='" + sCode + "'";

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

                if (dtType.Rows.Count > 0)
                {
                    sRetName = dtType.Rows[0]["NAME"].ToString();
                }

                else
                {
                    sRetName = "Incorrect Station Code.Kindly Enter Correct Code";
                }
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();

                objLogWriter.WriteLog(ex);

                sRetName = "Error";
            }

            finally
            {
                //ocConn.Close();

                //ocConn.Dispose();
            }

            return sRetName;
        }

        public String FetchDeptGridKeypressCode ( string sCode )
        {

            string sRetName = "";
            DataTable dtType = new DataTable();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  select  HR_DEPT_MASTER.HR_DEPT_DEPT_CODE CODE , HR_DEPT_DEPT_DESC NAME ";

                SQL = SQL + "   FROM     HR_DEPT_MASTER  ";

                SQL = SQL + "  where   HR_DEPT_MASTER.HR_DEPT_DEPT_CODE  ='" + sCode + "'";

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

                if (dtType.Rows.Count > 0)
                {
                    sRetName = dtType.Rows[0]["NAME"].ToString();
                }

                else
                {
                    sRetName = "Incorrect Department Code.Kindly Enter Correct Code";
                }
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();

                objLogWriter.WriteLog(ex);

                sRetName = "Error";
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return sRetName;
        }

        public String FetchSourceGridKeypressCode ( string sCode )
        {

            string sRetName = "";
            DataTable dtType = new DataTable();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  select  RM_SOURCE_MASTER.RM_SM_SOURCE_CODE CODE , RM_SM_SOURCE_DESC NAME ";

                SQL = SQL + "   FROM     RM_SOURCE_MASTER  ";

                SQL = SQL + "  where   RM_SOURCE_MASTER.RM_SM_SOURCE_CODE  ='" + sCode + "'";


                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

                if (dtType.Rows.Count > 0)
                {
                    sRetName = dtType.Rows[0]["NAME"].ToString();
                }

                else
                {
                    sRetName = "Incorrect Department Code.Kindly Enter Correct Code";
                }
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();

                objLogWriter.WriteLog(ex);

                sRetName = "Error";
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return sRetName;
        }

        public DataTable FetchItemGridKeypressCode ( string sCode )
        {

            DataTable dtType = new DataTable();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                if (gtbudget_ItemCode != null)
                {
                    //SQL = SQL + "SELECT RM_RMM_RM_CODE Code,RM_RMM_RM_DESCRIPTION Name,RM_RMM_RM_SHORT_NAME PartNo,";

                    //SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC RM_UM_UOM_DESC,RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE RM_UM_UOM_CODE, ";

                    //SQL = SQL + " RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE RESOURCE_CODE,PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_NAME RESOURCE_NAME, ";

                    //SQL = SQL + " CN_BUD_BUDGET_MASTER.PC_BUD_BUDGET_CODE BUDGET_CODE,CN_BUD_BUDGET_MASTER.PC_BUD_BUDGET_NAME BUDGET_NAME ";

                    //SQL = SQL + " FROM  RM_RAWMATERIAL_MASTER,RM_UOM_MASTER,PC_BUD_RESOURCE_MASTER,CN_BUD_BUDGET_MASTER,CN_BUD_BUDGET_DETAILS ";

                    //SQL = SQL + " WHERE  ";
                    //SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_CODE=RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE  ";
                    //SQL = SQL + " AND  RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE = PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE  ";
                    //SQL = SQL + " AND  PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE= CN_BUD_BUDGET_DETAILS.PC_BUD_RESOURCE_CODE  ";
                    //SQL = SQL + " AND  RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE =CN_BUD_BUDGET_DETAILS.PC_BUD_RESOURCE_CODE  ";
                    //SQL = SQL + " AND RM_UOM_MASTER.RM_UM_UOM_CODE=CN_BUD_BUDGET_DETAILS.RM_UM_UOM_CODE  ";
                    //SQL = SQL + " AND CN_BUD_BUDGET_MASTER.PC_BUD_BUDGET_CODE=CN_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_CODE ";
                    //SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ='" + sCode + "'";

                    SQL = "  SELECT  ";
                    SQL = SQL + " rm_rawmaterial_master.rm_rmm_rm_code  Code, ";
                    SQL = SQL + " rm_rmm_rm_description Name, ";
                    SQL = SQL + " rm_rmm_rm_short_name partno, ";
                    SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC RM_UM_UOM_DESC, ";
                    SQL = SQL + " rm_rawmaterial_master.rm_um_uom_code RM_UM_UOM_CODE, ";
                    SQL = SQL + " rm_rawmaterial_master.PC_BUD_RESOURCE_CODE RESOURCE_CODE,PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_NAME  RESOURCE_NAME, ";
                    SQL = SQL + " PC_BUD_BUDGET_ITEM_CODE, ";
                    SQL = SQL + " CN_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_AMOUNT BUDGET_AMOUNT   ";
                    SQL = SQL + " FROM  ";
                    SQL = SQL + " rm_rawmaterial_master,RM_RAWMATERIAL_DETAILS, ";
                    SQL = SQL + " RM_UOM_MASTER , ";
                    SQL = SQL + " CN_BUD_BUDGET_MASTER,   CN_BUD_BUDGET_DETAILS , ";
                    SQL = SQL + " PC_BUD_RESOURCE_MASTER   ";
                    SQL = SQL + " WHERE  RM_UOM_MASTER.RM_UM_UOM_CODE=rm_rawmaterial_master.rm_um_uom_code ";
                    SQL = SQL + " AND  rm_rawmaterial_master. rm_rmm_rm_code= RM_RAWMATERIAL_DETAILS. rm_rmm_rm_code ";
                    SQL = SQL + " AND  CN_BUD_BUDGET_MASTER.PC_BUD_BUDGET_CODE = CN_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_CODE   ";
                    SQL = SQL + " AND  PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE= CN_BUD_BUDGET_DETAILS.PC_BUD_RESOURCE_CODE  ";
                    SQL = SQL + " AND  rm_rawmaterial_master.PC_BUD_RESOURCE_CODE = PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE  ";
                    SQL = SQL + " AND  CN_BUD_BUDGET_MASTER.PC_BUD_BUDGET_LAST_YN='Y'  ";

                    SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ='" + sCode + "'";


                    if (!string.IsNullOrEmpty(Stcode))
                    {
                        SQL = SQL + " AND RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE  ='" + Stcode + "'";
                    }

                    if (!string.IsNullOrEmpty(gtbudget_ItemCode.ToString().Trim()))
                    {
                        SQL = SQL + "   AND  CN_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_ITEM_CODE= '" + gtbudget_ItemCode + "' ";
                    }

                    if (!string.IsNullOrEmpty(Projectcode.ToString().Trim()))
                    {
                        SQL = SQL + "   AND  CN_BUD_BUDGET_DETAILS.SALES_PM_PROJECT_CODE= '" + Projectcode + "' ";
                    }


                }
                else
                {
                    SQL = SQL + "SELECT RM_RMM_RM_CODE Code,RM_RMM_RM_DESCRIPTION Name,RM_RMM_RM_SHORT_NAME PartNo,";

                    SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC RM_UM_UOM_DESC,RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE RM_UM_UOM_CODE,";

                    SQL = SQL + " rm_rawmaterial_master.PC_BUD_RESOURCE_CODE RESOURCE_CODE,''  RESOURCE_NAME, ";
                    SQL = SQL + " '' PC_BUD_BUDGET_ITEM_CODE, ";
                    SQL = SQL + " 0 BUDGET_AMOUNT   ";

                    SQL = SQL + " FROM  RM_RAWMATERIAL_MASTER,RM_UOM_MASTER ";

                    SQL = SQL + " WHERE  RM_UOM_MASTER.RM_UM_UOM_CODE=RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE ";

                    SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ='" + sCode + "'";
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

        public String FetchUOMGridKeypressCode ( string sCode )
        {

            string sRetName = "";
            DataTable dtType = new DataTable();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  select  RM_UM_UOM_CODE CODE , RM_UM_UOM_DESC NAME ";

                SQL = SQL + "   FROM     RM_UOM_MASTER  ";

                SQL = SQL + "  where  RM_UM_UOM_CODE  ='" + sCode + "'";

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

                if (dtType.Rows.Count > 0)
                {
                    sRetName = dtType.Rows[0]["NAME"].ToString();
                }

                else
                {
                    sRetName = "Incorrect UOM Code.Kindly Enter Correct Code";
                }
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();

                objLogWriter.WriteLog(ex);

                sRetName = "Error";
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return sRetName;
        }

        #endregion 

        #region "Tax Setup "

        public DataTable FillTaxTypeSetup ( )
        {
            DataTable dtTax = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = "   select  GL_TAX_TYPE_CODE, GL_TAX_TYPE_NAME, GL_TAX_TYPE_PER,  ";
                SQL = SQL + "   GL_TAX_TYPE_SORT_ID  ";
                SQL = SQL + "   from GL_DEFAULTS_TAX_TYPE_MASTER order by   GL_TAX_TYPE_SORT_ID asc ";

                dtTax = clsSQLHelper.GetDataTableByCommand(SQL);

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
            return dtTax;
        }

        public DataTable FillTaxSetup ( )
        {
            DataTable dtTax = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = "   SELECT  ";
                SQL = SQL + "    GL_PURCHASE_TAX_ACCOUNT_CODE,  ";
                SQL = SQL + "   GL_SALES_TAX_ACCOUNT_CODE  ,GL_TAX_APPLICBLE_FINYRID FROM  GL_DEFAULTS_TAX_MASTER  ";
                dtTax = clsSQLHelper.GetDataTableByCommand(SQL);

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
            return dtTax;
        }


        #endregion

        #region "Generals function " 

        public DataTable FillBranch ( string UserName )
        {
            DataTable dtReturn = new DataTable();

            try
            {
                String SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT  AD_BR_CODE, AD_BR_NAME ";
                SQL = SQL + "FROM AD_BRANCH_MASTER ";
                if (UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                    SQL = SQL + "  where     AD_BR_CODE  IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                }

                SQL = SQL + "ORDER BY AD_BR_CODE ASC  ";

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

        public DataTable FillStation ( string UserName )
        {

            DataTable dtReturn = new DataTable();

            try
            {

                String SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT   sales_stn_station_code, sales_stn_station_name";
                SQL = SQL + "    FROM SL_STATION_MASTER";
                SQL = SQL + "    WHERE  SALES_RAW_MATERIAL_STATION  =  'Y'";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + "  AND SALES_STN_STATION_CODE IN ( SELECT     SALES_STN_STATION_CODE FROM ";
                    SQL = SQL + "     WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + UserName + "') ";
                }

                SQL = SQL + "    ORDER BY sales_stn_station_code ASC ";

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

        public DataTable FillPlantOrDestination ( string UserName )
        {

            DataTable dtReturn = new DataTable();

            try
            {

                String SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                // where TECH_PLM_RM_RCPT_DESITINATION ='


                SQL = " SELECT    TECH_PLM_PLANT_CODE,  TECH_PLM_PLANT_NAME   FROM TECH_PLANT_MASTER  order by   TECH_PLM_PLANT_CODE asc ";


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

        public DataTable FillBranchUserGranted ( string UserName )
        {

            DataTable dt = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                // SessionManager mngrclass = (SessionManager)mngrclassobj;



                SQL = "SELECT  AD_BR_CODE, AD_BR_NAME  ";
                SQL = SQL + "  FROM AD_BRANCH_MASTER  ";
                SQL = SQL + "  where  (  AD_BR_READYMIX_VISIBILE_YN ='Y' OR  AD_BR_BLOCK_VISIBILE_YN ='Y' OR AD_BR_PRECAST_VISIBILE_YN ='Y' ) ";

                if (UserName != "ADMIN")
                {
                    SQL = SQL + "   AND AD_BR_CODE in (select AD_BR_CODE from  ";
                    SQL = SQL + "  AD_USER_GRANTED_BRANCH where AD_UM_USERID = '" + UserName + "')   ";
                }

                SQL = SQL + "  ORDER BY AD_BR_NAME  ASC   ";


                dt = clsSQLHelper.GetDataTableByCommand(SQL);

            }

            catch (Exception Exception)
            { }
            return dt;
        }

        public DataTable FillStationUserGranted ( string UserName )
        {

            DataTable dt = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                //   SessionManager mngrclass = (SessionManager)mngrclassobj;



                SQL = "  SELECT SALES_STN_STATION_CODE  sales_stn_station_code ,SALES_STN_STATION_NAME  sales_stn_station_name   FROM  ";
                SQL = SQL + "  SL_STATION_MASTER WHERE  SALES_RAW_MATERIAL_STATION ='Y' ";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + "  AND SALES_STN_STATION_CODE IN ( SELECT     SALES_STN_STATION_CODE FROM ";
                    SQL = SQL + "     WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + UserName + "') ";
                }
                SQL = SQL + "  ORDER BY SALES_STN_STATION_NAME ";


                dt = clsSQLHelper.GetDataTableByCommand(SQL);

            }

            catch (Exception Exception)
            { }
            return dt;
        }


        public DataTable FillBranchAndStationUserGranted ( string UserName, string SelectedBranchCode = "" )
        {

            DataTable dt = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                //   SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE, SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME, ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME , ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.GL_COSM_ACCOUNT_CODE  ";
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
                SQL = SQL + "  ORDER BY   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE , SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_SORT_ID    ASC  ";



                dt = clsSQLHelper.GetDataTableByCommand(SQL);

            }

            catch (Exception Exception)
            { }
            return dt;
        }


        #endregion

    }

    public class RootObjRMDocNumber
    {
        public List<ADDOCNUMBER> AD_DOC_NUMBER { get; set; } // sjon , table name AD_DOC_NUMBER 
        public List<BRANCHTYPE> BRANCH_TYPE { get; set; } // sjon , table name AD_DOC_NUMBER 
    }


    public class ADDOCNUMBER
    {
        public string AD_DN_NEXT_NO { get; set; }
        public string AD_BR_DOC_PREFIX { get; set; }

    }

    public class BRANCHTYPE
    {
        public string AD_BR_READYMIX_VISIBILE_YN { get; set; }
        public string AD_BR_BLOCK_VISIBILE_YN { get; set; }
        public string AD_BR_PRECAST_VISIBILE_YN { get; set; }

    }


    public class BranchList
    {
        public string AD_BR_CODE { get; set; }

        public string AD_BR_NAME { get; set; }
        public double AD_BR_SORT_ID { get; set; }


    }


    public class BranchSationList
    {
        public string AD_BR_CODE { get; set; }

        public string AD_BR_NAME { get; set; }
        public double AD_BR_SORT_ID { get; set; }
        public double SALES_GL_STATION_SORT_ID { get; set; }
        public string SALES_STN_STATION_CODE { get; set; }
        public string SALES_STN_STATION_NAME { get; set; }
        public string GL_COSM_ACCOUNT_CODE { get; set; }


    }


}