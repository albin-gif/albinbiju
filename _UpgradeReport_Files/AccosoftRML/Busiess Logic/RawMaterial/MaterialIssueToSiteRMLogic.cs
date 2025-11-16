using AccosoftLogWriter;
using AccosoftNineteenCDL;
using AccosoftUtilities;
using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

/// <summary>
/// Created By      :   Jins Mathew // jomy 
/// Created On      :     03 mar 2023 
/// Updated By      : Alex
/// Updated On      : 28 Feb 2025
/// </summary>

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class MaterialIssueToSiteRMLogic
    {
        #region "Global Declaration"

        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        #endregion

        #region "Fill View"
        public DataTable FillViewReqEntry ( object mngrclassobj )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                //SQL = SQL + " SELECT  RM_STORE_REQUEST_MASTER.RM_SRQ_ENTRY_NO ENTRYNO, to_char(RM_SRQ_ENTRY_DATE) ENTRY_DATE,RM_STORE_REQUEST_MASTER.AD_FIN_FINYRID,  ";
                //SQL = SQL + "         RM_STORE_REQUEST_MASTER.SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, ";
                //SQL = SQL + "         RM_STORE_REQUEST_MASTER.HR_DEPT_DEPT_CODE,HR_DEPT_DEPT_DESC,  ";
                //SQL = SQL + "         RM_SRQ_APPROVED,";
                //SQL = SQL + "         RM_STORE_REQUEST_MASTER.GL_COSM_ACCOUNT_CODE CUSTOMER_CODE,  ";
                //SQL = SQL + "         GL_COSTING_MASTER.GL_COSM_ACCOUNT_NAME CUSTOMER_NAME ,  ";
                //SQL = SQL + "         RM_STORE_REQUEST_MASTER.PC_SALES_INQM_ENQUIRY_NO ";
                //SQL = SQL + "    FROM RM_STORE_REQUEST_MASTER, ";
                //SQL = SQL + "         GL_COSTING_MASTER,  ";
                //SQL = SQL + "         HR_DEPT_MASTER ,";
                //SQL = SQL + "         SL_STATION_MASTER ";
                //SQL = SQL + "   WHERE  RM_STORE_REQUEST_MASTER.GL_COSM_ACCOUNT_CODE = GL_COSTING_MASTER.GL_COSM_ACCOUNT_CODE(+)  ";
                //SQL = SQL + "   AND RM_STORE_REQUEST_MASTER.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE(+)  ";
                //SQL = SQL + "   AND RM_STORE_REQUEST_MASTER.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE(+)  ";
                //SQL = SQL + "     AND RM_STORE_REQUEST_MASTER.AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";
                //SQL = SQL + "     AND RM_STORE_REQUEST_MASTER.RM_SRQ_ISSUE_STATUS_OC ='O' ";


                SQL = "    SELECT  RM_STORE_REQUEST_MASTER.RM_SRQ_ENTRY_NO ENTRYNO, to_char(RM_SRQ_ENTRY_DATE) ENTRY_DATE,RM_STORE_REQUEST_MASTER.AD_FIN_FINYRID,   ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.SALES_STN_STATION_CODE, SALES_STN_STATION_NAME,  ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.HR_DEPT_DEPT_CODE,HR_DEPT_DEPT_DESC,   ";
                SQL = SQL + "         RM_SRQ_APPROVED, ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.PC_SALES_INQM_ENQUIRY_NO CUSTOMER_CODE,   ";
                SQL = SQL + "         PC_ENQUIRY_MASTER.SALES_PM_PROJECT_NAME CUSTOMER_NAME ,   ";
                SQL = SQL + "         PC_ENQUIRY_MASTER.SALES_INQM_ENQUIRY_NO PC_SALES_INQM_ENQUIRY_NO  ";
                SQL = SQL + "    FROM RM_STORE_REQUEST_MASTER,  ";
                SQL = SQL + "         PC_ENQUIRY_MASTER,   ";
                SQL = SQL + "         HR_DEPT_MASTER , ";
                SQL = SQL + "         SL_STATION_MASTER  ";
                SQL = SQL + "    WHEre  PC_ENQUIRY_MASTER.SALES_INQM_ENQUIRY_NO = RM_STORE_REQUEST_MASTER.GL_COSM_ACCOUNT_CODE   ";
                SQL = SQL + "    AND RM_STORE_REQUEST_MASTER.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE(+)   ";
                SQL = SQL + "    AND RM_STORE_REQUEST_MASTER.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE(+)   ";
                SQL = SQL + "       AND PC_ENQUIRY_MASTER.SALES_INQM_LIVE_APPROVED = 'Y' ";
                SQL = SQL + "    AND PC_ENQUIRY_MASTER.SALES_INQM_ENQUIRY_STATUS = 'LIVE'   ";
                 SQL = SQL + "     AND RM_STORE_REQUEST_MASTER.RM_SRQ_ISSUE_STATUS_OC ='O' ";




                if (mngrclass.UserName != "ADMIN")
                {
                    SQL = SQL + " AND RM_STORE_REQUEST_MASTER.AD_BR_CODE in ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
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

                SQL = SQL + " SELECT   RM_STISU_ENTRY_NO ENTRYNO, to_char( RM_STISU_ENTRY_DATE) ENTRY_DATE,AD_FIN_FINYRID,RM_SRQ_ENTRY_NO,  ";
                SQL = SQL + "         RM_STORE_ISSUE_MASTER.SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_STORE_ISSUE_MASTER.HR_DEPT_DEPT_CODE,HR_DEPT_DEPT_DESC,  ";
                SQL = SQL + "          RM_STISU_APPROVED,";
                SQL = SQL + "         RM_STORE_ISSUE_MASTER.GL_COSM_ACCOUNT_CODE CUSTOMER_CODE,  ";
                SQL = SQL + "         GL_COSTING_MASTER.GL_COSM_ACCOUNT_NAME CUSTOMER_NAME ,  ";
                SQL = SQL + "         RM_STORE_ISSUE_MASTER.PC_SALES_INQM_ENQUIRY_NO ";
                SQL = SQL + "    FROM RM_STORE_ISSUE_MASTER,  ";
                SQL = SQL + "         GL_COSTING_MASTER,  ";
                SQL = SQL + "         HR_DEPT_MASTER ,";
                SQL = SQL + "         SL_STATION_MASTER ";
                SQL = SQL + "   WHERE RM_STORE_ISSUE_MASTER.GL_COSM_ACCOUNT_CODE = GL_COSTING_MASTER.GL_COSM_ACCOUNT_CODE(+)  ";
                SQL = SQL + "   AND RM_STORE_ISSUE_MASTER.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE(+)  ";
                SQL = SQL + "   AND RM_STORE_ISSUE_MASTER.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE(+)  ";
                SQL = SQL + "     AND RM_STORE_ISSUE_MASTER.AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";


                if (EntryType == "Y")
                {
                    SQL = SQL + " AND  RM_STISU_APPROVED ='Y'";
                }
                else if (EntryType == "N")
                {
                    SQL = SQL + " AND  RM_STISU_APPROVED ='N'";
                }
                SQL = SQL + " AND  RM_STISU_ENTRY_DATE BETWEEN '" + Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";

                if (mngrclass.UserName != "ADMIN")
                {
                    SQL = SQL + " AND RM_STORE_ISSUE_MASTER.AD_BR_CODE in ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
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

        public DataTable FillViewEmployee ( )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT   hr_emp_employee_code Code, hr_emp_employee_name Name,";
                SQL = SQL + "  hr_dm_desg_desc FIELDTHREE";
                SQL = SQL + "  FROM hr_employee_master, hr_desg_master";
                SQL = SQL + "  WHERE ";
                SQL = SQL + "  hr_employee_master.hr_dm_designation_code =   hr_desg_master.hr_dm_designation_code";
                SQL = SQL + "  AND HR_EMP_STATUS='A' ";
                //SQL = SQL + " AND HR_EMP_COMPANY_CODE IN (      ";
                //SQL = SQL + "     SELECT  ws_ip_parameter_value from  WS_DEFAULTS_GL_INVENTORY ";
                //SQL = SQL + "  WHERE  WS_IP_PARAMETER_DESC ='STORE_REQUEST_EMP_COMP_CODE' ) ";


                SQL = SQL + "  ORDER BY hr_emp_employee_code";

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


        public DataTable ProjectCostCodeLookUpContrledPermission ( string sUSERNAME )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();



                //pleads ot no un commen jomy 02 jan 2018 

                ////OracleCommand ocCommand = new OracleCommand("PK_GET_GRANTED_PROJECT.Get_GrantedProjectsForRequest");
                ////ocCommand.Connection = ocConn;
                ////ocCommand.CommandType = CommandType.StoredProcedure;


                //////ocCommand.Parameters.Add("m_user_id", OracleType.VarChar).Direction = ParameterDirection.Input;
                //////ocCommand.Parameters.Add("m_user_id", OracleType.VarChar, 25).Value = sUSERNAME;

                ////ocCommand.Parameters.Add("m_user_id", OracleDbType.Varchar2, sUSERNAME, ParameterDirection.Input);


                ////ocCommand.Parameters.Add("cur_Return", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ////OracleDataAdapter odAdpt = new OracleDataAdapter(ocCommand);
                ////odAdpt.Fill(dtType);


                SQL = " ";
                SQL = SQL + "SELECT    ";
                SQL = SQL + "    SALES_INQM_ENQUIRY_NO         CODE, ";
                SQL = SQL + "    SALES_PM_PROJECT_NAME    NAME, ";
                SQL = SQL + "    PC_ENQUIRY_MASTER.SALES_CUS_CUSTOMER_CODE      CUSTCODE, ";
                SQL = SQL + "    SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_NAME     CUSTNAME, ";
                SQL = SQL + "    PC_ENQUIRY_MASTER.AD_BR_CODE, ";
                SQL = SQL + "    AD_BRANCH_MASTER.AD_BR_NAME, ";
                SQL = SQL + "    PC_ENQUIRY_MASTER.SALES_STN_STATION_CODE, ";
                SQL = SQL + "    SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "    PC_ENQUIRY_MASTER.HR_DEPT_DEPT_CODE, ";
                SQL = SQL + "    HR_DEPT_MASTER.HR_DEPT_DEPT_DESC, ";
                SQL = SQL + "    PC_ENQUIRY_MASTER.GL_COSM_ACCOUNT_GROUP ";
                SQL = SQL + "FROM  ";
                SQL = SQL + "    AD_BRANCH_MASTER, ";
                SQL = SQL + "    SL_STATION_MASTER, ";
                SQL = SQL + "    HR_DEPT_MASTER, ";
                SQL = SQL + "    PC_ENQUIRY_MASTER, ";
                SQL = SQL + "    SL_CUSTOMER_MASTER ";
                SQL = SQL + "WHERE      ";
                SQL = SQL + "    PC_ENQUIRY_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE(+) ";
                SQL = SQL + "    AND PC_ENQUIRY_MASTER.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE(+) ";
                SQL = SQL + "    AND PC_ENQUIRY_MASTER.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE(+)  ";
                SQL = SQL + "    AND PC_ENQUIRY_MASTER.SALES_CUS_CUSTOMER_CODE =   SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_CODE(+)  ";
                SQL = SQL + "    AND SALES_INQM_LIVE_APPROVED = 'Y' ";
                SQL = SQL + "    AND SALES_INQM_ENQUIRY_STATUS = 'LIVE'   ";
                SQL = SQL + "    order by SALES_INQM_ENQUIRY_NO       asc";


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

        public DataTable FillPopUp(string Branch, string UserName)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL =    " SELECT TECH_PRODUCT_MASTER.TECH_PM_PRODUCT_CODE  CODE ,";

                SQL = SQL + " TECH_PRODUCT_MASTER.TECH_PM_PRODUCT_DESCRIPTION   NAME  ,decode( TECH_PM_APPROVED_STATUS,'Y', 'Yes','No')APPROVED,  TECH_PM_STATUS FIELD_THREE, TECH_PRODUCT_MASTEr.AD_BR_CODE,AD_BR_NAME  ";
               
                SQL = SQL + " 	FROM   ";

                SQL = SQL + " 	TECH_PRODUCT_MASTEr,AD_BRANCH_MASTER  where  TECH_PT_PRODUCT_TYPE  ='CNCR'";
               
                SQL = SQL + "AND TECH_PM_APPROVED_STATUS='Y' AND   TECH_PM_STATUS ='A'";
               
                if (UserName != "ADMIN")
                { 
                    SQL = SQL + "  AND     TECH_PRODUCT_MASTER.AD_BR_CODE  IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                }

                SQL = SQL + "   AND TECH_PRODUCT_MASTER.AD_BR_CODE = '"+Branch+"'  ";

                SQL = SQL + " 	AND TECH_PRODUCT_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE (+)   ";

                SQL = SQL + " 	ORDER BY TECH_PRODUCT_MASTER.TECH_PM_PRODUCT_CODE  ASC  ";

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

        public DataTable FetchDetail(string strProductCode)
        {
            DataTable dtData = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "";
                SQL = SQL + " SELECT  ";
                SQL = SQL + "  	     TECH_PRODUCT_COMP_DETAILS.RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "  	     RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE, ";
                SQL = SQL + "  	     TECH_PRODUCT_COMP_DETAILS.RM_UOM_UOM_CODE,	RM_UM_UOM_DESC,RM_UCM_FACTOR, ";
                SQL = SQL + "  	     TECH_PCD_WEIGHT_FROM, ";
                SQL = SQL + "  	     TECH_PCD_WEIGHT,TECH_PCD_WEIGHT_ACTUAL_PLANT,TECH_PRODUCT_COMP_DETAILS.RM_RMM_MAT_DENSITY, ";
                SQL = SQL + "        TECH_PRODUCT_COMP_DETAILS.RM_RMM_MAT_ABSORPTION,TECH_PRODUCT_COMP_DETAILS.RM_RMM_SULPHATE_RATIO,TECH_PRODUCT_COMP_DETAILS.RM_RMM_CHLORIDE_RATIO  ";
                SQL = SQL + "   FROM TECH_PRODUCT_COMP_DETAILS,RM_RAWMATERIAL_MASTER, RM_UOM_MASTER,RM_UOM_CONVERSION  ";
                SQL = SQL + "  where TECH_PRODUCT_COMP_DETAILS.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "  	 AND TECH_PRODUCT_COMP_DETAILS.RM_UOM_UOM_CODE  = RM_UOM_MASTER.RM_UM_UOM_CODE(+)   ";
                SQL = SQL + "    AND TECH_PRODUCT_COMP_DETAILS.RM_UOM_UOM_CODE  = RM_UOM_MASTER.RM_UM_UOM_CODE(+)   ";
                SQL = SQL + "    AND TECH_PRODUCT_COMP_DETAILS.RM_RMM_RM_CODE = RM_UOM_CONVERSION.RM_RMM_RM_CODE(+) ";
                SQL = SQL + "    AND TECH_PRODUCT_COMP_DETAILS.RM_UOM_UOM_CODE = RM_UOM_CONVERSION.RM_UM_UOM_CODE_TO(+) ";
                SQL = SQL + "   AND RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE = RM_UOM_CONVERSION.RM_UM_UOM_CODE_FROM(+)  "; 
                SQL = SQL + "    AND TECH_PM_PRODUCT_CODE ='" + strProductCode + "'";
                SQL = SQL + "ORDER BY RM_RMM_RM_CODE ";

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);


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
            return dtData;

        }

        public string FillProductType ( )
        {
            DataTable dtData = new DataTable();
            DataSet dsData = new DataSet();
            string JSONString = string.Empty;
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT GL_PRODUCT_TYPE_CODE CODE, ";
                SQL = SQL + "       GL_PRODUCT_TYPE_DESC NAME, ";
                SQL = SQL + "       PC_ELM_GROUP_TYPECODE  ";
                SQL = SQL + "  FROM GL_PRODUCT_TYPE_MASTER ";
                 
                dtData = clsSQLHelper.GetDataTableByCommand(SQL);
                dtData.TableName = "GL_PRODUCT_TYPE_MASTER";
                dsData.Tables.Add(dtData);
                JSONString = JsonConvert.SerializeObject(dsData, Formatting.None);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return JSONString;
        }
         
        #endregion

        #region "Insert/Update/Delete"

        public string InsertMasterSql ( MaterialIssueToSiteRMEntity oEntity, List<MaterialIssueToSiteRMEntityDetails> EntityDetails, bool Autogen, object mngrclassobj, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber )
         {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = "INSERT INTO RM_STORE_ISSUE_MASTER ( ";
                SQL = SQL + "   RM_STISU_ENTRY_NO, RM_STISU_ENTRY_DATE, AD_FIN_FINYRID,  ";
                SQL = SQL + "   RM_SRQ_ENTRY_NO, RM_SRQ_AD_FIN_FINYRID, AD_BR_CODE,  ";
                SQL = SQL + "   PC_SALES_INQM_ENQUIRY_NO, GL_COSM_ACCOUNT_CODE, SALES_STN_STATION_CODE,  ";
                SQL = SQL + "   HR_DEPT_DEPT_CODE, RM_STISU_TOTAL_AMOUNT, RM_STISU_REMARKS,  ";
                SQL = SQL + "   RM_STISU_CREATEDBY, RM_STISU_CREATEDDATE, RM_STISU_VERIFIED, RM_STISU_VERIFIED_BY,  ";
                SQL = SQL + "   RM_STISU_VERIFIED_DATE,RM_STISU_APPROVED, RM_STISU_APPROVED_BY, RM_STISU_APPROVED_DATE,  ";
                SQL = SQL + "   RM_STISU_RECEIVED_EMPCODE,RM_STISU_RECEIVED_EMPNAME,RM_STISU_DELETESTATUS, AD_CM_COMPANY_CODE, ";
                SQL = SQL + "   RM_STISU_ISSUETYPE_RM_CNCR, TECH_PM_PRODUCT_CODE, RM_STISU_CNCR_BATCHED_QTY,RM_STISU_ONSITE_OR_FACTORY )  ";

                SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "', '" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "', " + mngrclass.FinYearID + ",";
                SQL = SQL + " '" + oEntity.txtReqEntryNo + "'," + oEntity.txtReqFinyr + ",'" + oEntity.cboBranch + "',";
                SQL = SQL + " '" + oEntity.EnquiryNo + "','" + oEntity.CostCenter + "','" + oEntity.cboStation + "', ";
                SQL = SQL + " '" + oEntity.txtDept + "'," + Convert.ToDouble(oEntity.txtGradndTotalAmount) + ", '" + oEntity.txtRemarks + "',  ";
                SQL = SQL + " '" + mngrclass.UserName + "', TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " '" + oEntity.chkVerify + "','" + oEntity.txtVerifiedBy + "',TO_DATE('" + oEntity.txtVerifiedDate + "','DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + " '" + oEntity.chkApprv + "','" + oEntity.txtAppdBy + "',TO_DATE('" + oEntity.txtAppdDate + "','DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + " '" + oEntity.EmpCode + "', '" + oEntity.EmpName + "', 0,'" + mngrclass.CompanyCode + "', ";
                SQL = SQL + " '" +oEntity.IssueType+"' , '" +oEntity.ProductCode+"' , " +oEntity.ProductQty+", '" +oEntity.IssueTo+"' )";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertDetails(oEntity, EntityDetails, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                if (oEntity.chkApprv == "Y")
                {
                    sRetun = InsertApprovalQrs(oEntity, mngrclassobj);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTRMISUTOSIT", oEntity.txtEntryNo, Autogen, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.cboBranch, sAtuoGenBranchDocNumber);

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

        private string InsertDetails ( MaterialIssueToSiteRMEntity oEntity, List<MaterialIssueToSiteRMEntityDetails> EntityDetails, object mngrclassobj )
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
                        if (!string.IsNullOrEmpty(Data.rmCodeDetails.ToString()))
                        {
                            ++i;
                            SQL = "INSERT INTO RM_STORE_ISSUE_DETAILS ( ";
                            SQL = SQL + "   RM_STISU_ENTRY_NO, RM_RMM_RM_CODE, AD_FIN_FINYRID,  ";
                            SQL = SQL + "   RM_STISUD_SL_NO, PC_BUD_BUDGET_ITEM_CODE, RM_UM_UOM_CODE,  ";
                            SQL = SQL + "   RM_STISUD_AVG_COST, RM_STISUD_QTY, RM_STISUD_AMOUNT,GL_PRODUCT_TYPE_CODE)  ";

                            SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "','" + Data.rmCodeDetails + "', " + mngrclass.FinYearID + ",";
                            SQL = SQL + " " + i + ", '" + Data.BudgetItemCodeDetails + "','" + Data.uomCodeDetails + "',";
                            SQL = SQL + " " + Convert.ToDouble(Data.avgCostDetails) + " , " + Convert.ToDouble(Data.qtyDetails) + ", " + Convert.ToDouble(Data.AmountDetails) + ",'" + Data.ProductType + "' ";
                            SQL = SQL + "  )";


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

        private string InsertApprovalQrs ( MaterialIssueToSiteRMEntity oEntity, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " INSERT INTO RM_STORE_ISSUE_TRIGGER ( ";
                SQL = SQL + "  RM_STISU_ENTRY_NO, AD_FIN_FINYRID,   RM_STISU_APPROVED_BY) ";
                SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "', " + mngrclass.FinYearID + ", '" + mngrclass.UserName + "'";
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

        public string UpdateMasterSql ( MaterialIssueToSiteRMEntity oEntity, List<MaterialIssueToSiteRMEntityDetails> EntityDetails, bool Autogen, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " UPDATE RM_STORE_ISSUE_MASTER";

                SQL = SQL + "   SET  RM_STISU_ENTRY_DATE          = '" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "', ";
                SQL = SQL + "       AD_BR_CODE                 = '" + oEntity.cboBranch + "', ";
                SQL = SQL + "       RM_SRQ_ENTRY_NO   = '" + oEntity.txtReqEntryNo + "', ";
                SQL = SQL + "       RM_SRQ_AD_FIN_FINYRID   = " + oEntity.txtReqFinyr + ", ";
                SQL = SQL + "       PC_SALES_INQM_ENQUIRY_NO   = '" + oEntity.EnquiryNo + "', ";
                SQL = SQL + "       GL_COSM_ACCOUNT_CODE       = '" + oEntity.CostCenter + "', ";
                SQL = SQL + "       SALES_STN_STATION_CODE     = '" + oEntity.cboStation + "', ";
                SQL = SQL + "       HR_DEPT_DEPT_CODE          = '" + oEntity.txtDept + "', ";
                SQL = SQL + "        RM_STISU_TOTAL_AMOUNT        = " + Convert.ToDouble(oEntity.txtGradndTotalAmount) + ", ";
                SQL = SQL + "        RM_STISU_REMARKS             = '" + oEntity.txtRemarks + "', ";
                SQL = SQL + "        RM_STISU_RECEIVED_EMPCODE    = '" + oEntity.EmpCode + "', ";
                SQL = SQL + "        RM_STISU_RECEIVED_EMPNAME    = '" + oEntity.EmpName + "', ";
                SQL = SQL + "        RM_STISU_CNCR_BATCHED_QTY    =  "+oEntity.ProductQty+" ,  ";
                SQL = SQL + "        RM_STISU_UPDATEDBY           = '" + mngrclass.UserName + "', ";
                SQL = SQL + "        RM_STISU_UPDATEDDATE         =  TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + "        RM_STISU_VERIFIED            = '" + oEntity.chkVerify + "', ";
                SQL = SQL + "        RM_STISU_VERIFIED_BY         = '" + oEntity.txtVerifiedBy + "', ";
                SQL = SQL + "        RM_STISU_VERIFIED_DATE       =  TO_DATE('" + oEntity.txtVerifiedDate + "','DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + "        RM_STISU_APPROVED            = '" + oEntity.chkApprv + "', ";
                SQL = SQL + "        RM_STISU_APPROVED_BY         = '" + oEntity.txtAppdBy + "', ";
                SQL = SQL + "        RM_STISU_APPROVED_DATE       =  TO_DATE('" + oEntity.txtAppdDate + "','DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + "        RM_STISU_ONSITE_OR_FACTORY   =  '" + oEntity.IssueTo+ "'";
                SQL = SQL + " WHERE  RM_STISU_ENTRY_NO = '" + oEntity.txtEntryNo + "' AND ad_fin_finyrid = " + mngrclass.FinYearID + " AND  RM_STISU_DELETESTATUS = 0";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_STORE_ISSUE_DETAILS";
                SQL = SQL + " WHERE  RM_STISU_ENTRY_NO = '" + oEntity.txtEntryNo + "'";
                SQL = SQL + " AND ad_fin_finyrid = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertDetails(oEntity, EntityDetails, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                if (oEntity.chkApprv == "Y")
                {
                    sRetun = InsertApprovalQrs(oEntity, mngrclassobj);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTRMISUTOSIT", oEntity.txtEntryNo, Autogen, Environment.MachineName, "U", sSQLArray);

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

                SQL = " Delete from RM_STORE_ISSUE_MASTER";
                SQL = SQL + " WHERE  RM_STISU_ENTRY_NO = '" + txtEntryNo + "' AND ad_fin_finyrid = " + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);

                SQL = " Delete from RM_STORE_ISSUE_DETAILS";
                SQL = SQL + " WHERE  RM_STISU_ENTRY_NO ='" + txtEntryNo + "' AND ad_fin_finyrid = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                SQL = " Delete from RM_STORE_ISSUE_TRIGGER";
                SQL = SQL + " WHERE  RM_STISU_ENTRY_NO ='" + txtEntryNo + "' AND ad_fin_finyrid = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTRMISUTOSIT", txtEntryNo, false, Environment.MachineName, "D", sSQLArray);
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

        public DataSet FetchReqData ( string Entry_No, int FinYr, string UserName )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = SQL + " SELECT  RM_SRQ_ENTRY_NO ENTRYNO, RM_SRQ_ENTRY_DATE,  ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.HR_DEPT_DEPT_CODE,HR_DEPT_DEPT_DESC,  ";
                SQL = SQL + "         RM_SRQ_VERIFIED, RM_SRQ_VERIFIED_BY, RM_SRQ_VERIFIED_DATE, ";
                SQL = SQL + "         RM_SRQ_TOTAL_AMOUNT, RM_SRQ_APPROVED, RM_SRQ_APPROVED_BY, RM_SRQ_APPROVED_DATE, ";
                SQL = SQL + "         RM_SRQ_REMARKS, RM_SRQ_CREATEDBY,RM_SRQ_CREATEDDATE,RM_SRQ_CANCELLED_YN,RM_SRQ_CANCELLED_REMARKS,  ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.GL_COSM_ACCOUNT_CODE,  ";
                SQL = SQL + "         GL_COSTING_MASTER.GL_COSM_ACCOUNT_NAME ,  ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.PC_SALES_INQM_ENQUIRY_NO, ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.AD_BR_CODE ,AD_BR_NAME    ";
                SQL = SQL + "    FROM RM_STORE_REQUEST_MASTER,  ";
                SQL = SQL + "         GL_COSTING_MASTER,  ";
                SQL = SQL + "         AD_BRANCH_MASTER,HR_DEPT_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER ";
                SQL = SQL + "   WHERE RM_STORE_REQUEST_MASTER.AD_FIN_FINYRID = " + FinYr + "";
                SQL = SQL + "     AND RM_SRQ_ENTRY_NO = '" + Entry_No + "'";
                SQL = SQL + "     AND RM_STORE_REQUEST_MASTER.GL_COSM_ACCOUNT_CODE = GL_COSTING_MASTER.GL_COSM_ACCOUNT_CODE(+)  ";
                SQL = SQL + "     AND RM_STORE_REQUEST_MASTER.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "     AND RM_STORE_REQUEST_MASTER.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE ";
                SQL = SQL + "     AND RM_STORE_REQUEST_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE ";
                SQL = SQL + "     AND RM_STORE_REQUEST_MASTER.RM_SRQ_ISSUE_STATUS_OC ='O'  ";

                if (UserName != "ADMIN")
                {
                    SQL = SQL + " AND RM_STORE_REQUEST_MASTER.AD_BR_CODE in ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
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

        public DataSet FetchReqDetailData ( string Entry_No, int FinYr )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " ";
                SQL = SQL + "SELECT Request.RM_SRQ_ENTRY_NO, Request.RMCODE,0 RM_STISUD_QTY,1 RM_UCM_FACTOR ,";
                SQL = SQL + "       RM_RMM_RM_DESCRIPTION,Request.RM_SRQD_SL_NO, ";
                SQL = SQL + "       RM_UM_UOM_CODE, RM_UM_UOM_DESC, ";
                SQL = SQL + "       RM_SRQD_AVG_COST,RM_SRQD_AMOUNT, RM_SRQD_QTY, ";
                SQL = SQL + "       PC_BUD_BUDGET_ITEM_CODE, RM_SRQD_ISSUE_QTY_BAL - NVL (Bal_Qty, 0)     RM_SRQD_ISSUE_QTY_BAL, RM_SRQD_EXPECTED_DATE ";
                SQL = SQL + "  FROM (SELECT RM_SRQ_ENTRY_NO, ";
                SQL = SQL + "               RM_STORE_REQUEST_DETAILS.AD_FIN_FINYRID, ";
                SQL = SQL + "               RM_STORE_REQUEST_DETAILS.RM_RMM_RM_CODE     RMCODE, ";
                SQL = SQL + "               RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, RM_SRQD_SL_NO, ";
                SQL = SQL + "               RM_STORE_REQUEST_DETAILS.RM_UM_UOM_CODE,  RM_UOM_MASTER.RM_UM_UOM_DESC, ";
                SQL = SQL + "               RM_SRQD_AVG_COST, ";
                SQL = SQL + "               RM_SRQD_AMOUNT, ";
                SQL = SQL + "               RM_SRQD_QTY, ";
                SQL = SQL + "               PC_BUD_BUDGET_ITEM_CODE, ";
                SQL = SQL + "               RM_SRQD_ISSUE_QTY_BAL, ";
                SQL = SQL + "               RM_STORE_REQUEST_DETAILS.RM_SRQD_EXPECTED_DATE ";
                SQL = SQL + "          FROM RM_STORE_REQUEST_DETAILS, RM_RAWMATERIAL_MASTER, RM_UOM_MASTER ";
                SQL = SQL + "         WHERE     RM_STORE_REQUEST_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "               AND RM_STORE_REQUEST_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_STORE_REQUEST_DETAILS.AD_FIN_FINYRID = " + FinYr + "";
                SQL = SQL + " AND RM_STORE_REQUEST_DETAILS.RM_SRQ_ENTRY_NO = '" + Entry_No + "'";
                SQL = SQL + "               AND RM_STORE_REQUEST_DETAILS.RM_SRQD_ISSUE_QTY_BAL > 0) ";
                SQL = SQL + "       Request, ";
                SQL = SQL + "       (  SELECT RM_STORE_ISSUE_MASTER.RM_SRQ_ENTRY_NO, ";
                SQL = SQL + "                 RM_STORE_ISSUE_MASTER.RM_SRQ_AD_FIN_FINYRID, ";
                SQL = SQL + "                 RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE          RMCODE, ";
                SQL = SQL + "                 RM_STORE_ISSUE_DETAILS.RM_STISUD_SL_NO, ";
                SQL = SQL + "                 SUM (RM_STORE_ISSUE_DETAILS.RM_STISUD_QTY)     Bal_Qty ";
                SQL = SQL + "            FROM RM_STORE_ISSUE_DETAILS, RM_STORE_ISSUE_MASTER ";
                SQL = SQL + "           WHERE     RM_STORE_ISSUE_MASTER.RM_STISU_ENTRY_NO =RM_STORE_ISSUE_DETAILS.RM_STISU_ENTRY_NO ";
                SQL = SQL + "                 AND RM_STORE_ISSUE_MASTER.AD_FIN_FINYRID = RM_STORE_ISSUE_DETAILS.AD_FIN_FINYRID ";
                SQL = SQL + "                 AND RM_STORE_ISSUE_MASTER.RM_STISU_APPROVED = 'N' ";
                SQL = SQL + " AND RM_STORE_ISSUE_MASTER.RM_SRQ_AD_FIN_FINYRID = " + FinYr + "";
                SQL = SQL + " AND RM_STORE_ISSUE_MASTER.RM_SRQ_ENTRY_NO= '" + Entry_No + "'";
                SQL = SQL + "        GROUP BY RM_SRQ_ENTRY_NO, ";
                SQL = SQL + "                 RM_SRQ_AD_FIN_FINYRID, ";
                SQL = SQL + "                 RM_RMM_RM_CODE, ";
                SQL = SQL + "                 RM_STISUD_SL_NO) Issue ";
                SQL = SQL + " WHERE     Request.RM_SRQ_ENTRY_NO = Issue.RM_SRQ_ENTRY_NO(+) ";
                SQL = SQL + "       AND Request.AD_FIN_FINYRID = Issue.RM_SRQ_AD_FIN_FINYRID(+) ";
                SQL = SQL + "       AND Request.RMCODE = Issue.RMCODE(+) ";
                //SQL = SQL + "       AND Request.RM_SRQD_SL_NO = Issue.RM_STISUD_SL_NO(+) ";




                //SQL = " SELECT RM_SRQ_ENTRY_NO,RM_STORE_REQUEST_DETAILS.RM_RMM_RM_CODE RMCODE, ";
                //SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,RM_SRQD_SL_NO, ";
                //SQL = SQL + " RM_STORE_REQUEST_DETAILS.RM_UM_UOM_CODE, ";
                //SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC, RM_SRQD_AVG_COST, RM_SRQD_AMOUNT, ";
                //SQL = SQL + " RM_SRQD_QTY,  RM_SRQD_SL_NO,PC_BUD_BUDGET_ITEM_CODE,RM_SRQD_ISSUE_QTY_BAL, ";
                //SQL = SQL + " RM_STORE_REQUEST_DETAILS.RM_SRQD_EXPECTED_DATE  ";
                //SQL = SQL + " FROM RM_STORE_REQUEST_DETAILS,";
                //SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                //SQL = SQL + " RM_UOM_MASTER";
                //SQL = SQL + " WHERE ";
                //SQL = SQL + "  RM_STORE_REQUEST_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                //SQL = SQL + " AND RM_STORE_REQUEST_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE";
                //SQL = SQL + " AND RM_STORE_REQUEST_DETAILS.AD_FIN_FINYRID = " + FinYr + "";
                //SQL = SQL + " AND RM_STORE_REQUEST_DETAILS.RM_SRQ_ENTRY_NO = '" + Entry_No + "'";
                //SQL = SQL + " AND RM_STORE_REQUEST_DETAILS.RM_SRQD_ISSUE_QTY_BAL > 0 ";
                //SQL = SQL + " ORDER BY RM_STORE_REQUEST_DETAILS.RM_SRQD_SL_NO ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        public DataSet FetchMasterData ( string Entry_No, int FinYr, string UserName )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = SQL + " SELECT   RM_STISU_ENTRY_NO ENTRYNO,  RM_STISU_ENTRY_DATE, RM_SRQ_ENTRY_NO,RM_SRQ_AD_FIN_FINYRID, ";
                SQL = SQL + "         RM_STORE_ISSUE_MASTER.SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_STORE_ISSUE_MASTER.HR_DEPT_DEPT_CODE,HR_DEPT_DEPT_DESC,  ";
                SQL = SQL + "          RM_STISU_VERIFIED,  RM_STISU_VERIFIED_BY,  RM_STISU_VERIFIED_DATE, ";
                SQL = SQL + "          RM_STISU_TOTAL_AMOUNT,  RM_STISU_APPROVED,  RM_STISU_APPROVED_BY,  RM_STISU_APPROVED_DATE, ";
                SQL = SQL + "          RM_STISU_REMARKS,  RM_STISU_CREATEDBY, RM_STISU_CREATEDDATE, ";
                SQL = SQL + "         RM_STORE_ISSUE_MASTER.GL_COSM_ACCOUNT_CODE,  ";
                SQL = SQL + "         GL_COSTING_MASTER.GL_COSM_ACCOUNT_NAME ,  ";
                SQL = SQL + "         RM_STORE_ISSUE_MASTER.PC_SALES_INQM_ENQUIRY_NO, ";
                SQL = SQL + "         RM_STORE_ISSUE_MASTER.AD_BR_CODE ,AD_BR_NAME ,RM_STISU_RECEIVED_EMPCODE,RM_STISU_RECEIVED_EMPNAME,   ";
                SQL = SQL + "         RM_STISU_ISSUETYPE_RM_CNCR, RM_STORE_ISSUE_MASTER.TECH_PM_PRODUCT_CODE, TECH_PM_PRODUCT_DESCRIPTION, RM_STISU_CNCR_BATCHED_QTY, ";
                SQL = SQL + "         RM_STISU_ONSITE_OR_FACTORY ";
                SQL = SQL + "    FROM RM_STORE_ISSUE_MASTER,  ";
                SQL = SQL + "         GL_COSTING_MASTER,  ";
                SQL = SQL + "         AD_BRANCH_MASTER,HR_DEPT_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, TECH_PRODUCT_MASTER  ";
                SQL = SQL + "   WHERE RM_STORE_ISSUE_MASTER.AD_FIN_FINYRID = " + FinYr + "";
                SQL = SQL + "     AND  RM_STISU_ENTRY_NO = '" + Entry_No + "'";
                SQL = SQL + "     AND RM_STORE_ISSUE_MASTER.GL_COSM_ACCOUNT_CODE = GL_COSTING_MASTER.GL_COSM_ACCOUNT_CODE(+)  ";
                SQL = SQL + "     AND RM_STORE_ISSUE_MASTER.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "     AND RM_STORE_ISSUE_MASTER.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE ";
                SQL = SQL + "     AND RM_STORE_ISSUE_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE ";
                SQL = SQL + "     AND RM_STORE_ISSUE_MASTER.TECH_PM_PRODUCT_CODE = TECH_PRODUCT_MASTER.TECH_PM_PRODUCT_CODE(+)  ";

                if (UserName != "ADMIN")
                {
                    SQL = SQL + " AND RM_STORE_ISSUE_MASTER.AD_BR_CODE in ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
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

        public DataSet FetchPurchaseDetailData ( string Entry_No, int FinYr, string IssueType, string ReqNo )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                if(IssueType=="RM")
                {
                    SQL = "SELECT RM_STORE_ISSUE_DETAILS.RM_STISU_ENTRY_NO,RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE RMCODE,     ";
                    SQL = SQL + "          RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, RM_STISUD_SL_NO,     ";
                    SQL = SQL + "          RM_STORE_ISSUE_DETAILS.RM_UM_UOM_CODE,     ";
                    SQL = SQL + "          RM_UOM_MASTER.RM_UM_UOM_DESC,RM_STORE_ISSUE_DETAILS.GL_PRODUCT_TYPE_CODE,GL_PRODUCT_TYPE_DESC,  RM_STISUD_AVG_COST,  RM_STISUD_AMOUNT,     ";
                    SQL = SQL + "          RM_STISUD_QTY,   RM_STISUD_SL_NO,RM_STORE_ISSUE_DETAILS.PC_BUD_BUDGET_ITEM_CODE,      ";
                    SQL = SQL + "          RM_STORE_REQUEST_DETAILS.RM_SRQD_QTY Req_Qty,    ";
                    SQL = SQL + "         NVL((  (RM_STORE_REQUEST_DETAILS.RM_SRQD_ISSUE_QTY_BAL - Bal_Qty) + RM_STISUD_QTY),0) Bal_Qty   ";
                    SQL = SQL + "     FROM RM_STORE_ISSUE_DETAILS,RM_STORE_ISSUE_MASTER,    ";
                    SQL = SQL + "          RM_RAWMATERIAL_MASTER,GL_PRODUCT_TYPE_MASTER,   ";
                    SQL = SQL + "          RM_UOM_MASTER,RM_STORE_REQUEST_DETAILS,  ";
                    SQL = SQL + "          ( SELECT RM_SRQ_ENTRY_NO ,RM_RMM_RM_CODE,SUM (RM_STORE_ISSUE_DETAILS.RM_STISUD_QTY) Bal_Qty  ";
                    SQL = SQL + "            FROM RM_STORE_ISSUE_DETAILS,RM_STORE_ISSUE_MASTER  ";
                    SQL = SQL + "           WHERE RM_STORE_ISSUE_MASTER.RM_STISU_ENTRY_NO=RM_STORE_ISSUE_DETAILS.RM_STISU_ENTRY_NO  ";
                    SQL = SQL + "             AND RM_STORE_ISSUE_MASTER.RM_STISU_APPROVED = 'N'   ";
                    SQL = SQL + "             AND RM_STORE_ISSUE_MASTER.RM_SRQ_ENTRY_NO=  '" + ReqNo + "'  ";
                    SQL = SQL + "        GROUP BY RM_SRQ_ENTRY_NO ,RM_RMM_RM_CODE ) Issue    ";
                    SQL = SQL + "    WHERE RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                    SQL = SQL + "      AND RM_STORE_ISSUE_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE   ";
                    SQL = SQL + "      AND RM_STORE_ISSUE_DETAILS.GL_PRODUCT_TYPE_CODE = GL_PRODUCT_TYPE_MASTER.GL_PRODUCT_TYPE_CODE   ";
                    SQL = SQL + "      AND RM_STORE_ISSUE_MASTER.RM_SRQ_ENTRY_NO=Issue.RM_SRQ_ENTRY_NO(+)  ";
                    SQL = SQL + "      AND RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE=Issue.RM_RMM_RM_CODE(+)  ";
                    SQL = SQL + "      and RM_STORE_ISSUE_MASTER.RM_STISU_ENTRY_NO=RM_STORE_ISSUE_DETAILS.RM_STISU_ENTRY_NO   ";
                    SQL = SQL + "      and RM_STORE_ISSUE_MASTER.AD_FIN_FINYRID=RM_STORE_ISSUE_DETAILS.AD_FIN_FINYRID   ";
                    SQL = SQL + "      and RM_STORE_ISSUE_MASTER.RM_SRQ_ENTRY_NO = RM_STORE_REQUEST_DETAILS.RM_SRQ_ENTRY_NO   ";
                    SQL = SQL + "      and RM_STORE_ISSUE_MASTER.RM_SRQ_AD_FIN_FINYRID = RM_STORE_REQUEST_DETAILS.AD_FIN_FINYRID   ";
                    SQL = SQL + "      and RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE = RM_STORE_REQUEST_DETAILS.RM_RMM_RM_CODE  ";
                    SQL = SQL + "      AND RM_STORE_ISSUE_DETAILS.AD_FIN_FINYRID =" + FinYr + "";
                    SQL = SQL + "      AND RM_STORE_ISSUE_DETAILS. RM_STISU_ENTRY_NO = '" + Entry_No + "' ";
                    SQL = SQL + " ORDER BY RM_STORE_ISSUE_DETAILS. RM_STISUD_SL_NO  ";
                }
                else if (IssueType=="CN")
                {
                    SQL = "SELECT RM_STORE_ISSUE_DETAILS.RM_STISU_ENTRY_NO,   ";
                    SQL = SQL + "         RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE RMCODE,   ";
                    SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,   ";
                    SQL = SQL + "         RM_STISUD_SL_NO,   ";
                    SQL = SQL + "         RM_STORE_ISSUE_DETAILS.RM_UM_UOM_CODE,   ";
                    SQL = SQL + "         RM_UOM_MASTER.RM_UM_UOM_DESC,RM_UCM_FACTOR,    ";
                    SQL = SQL + "         RM_STISUD_AVG_COST,   ";
                    SQL = SQL + "         RM_STISUD_AMOUNT,   ";
                    SQL = SQL + "         RM_STISUD_QTY,   ";
                    SQL = SQL + "         RM_STISUD_SL_NO,   ";
                    SQL = SQL + "         RM_STORE_ISSUE_DETAILS.PC_BUD_BUDGET_ITEM_CODE,RM_STORE_ISSUE_DETAILS.GL_PRODUCT_TYPE_CODE, ";
                    SQL = SQL + "         GL_PRODUCT_TYPE_DESC   ";
                    SQL = SQL + "    FROM RM_STORE_ISSUE_DETAILS,RM_STORE_ISSUE_MASTER,   ";
                    SQL = SQL + "         RM_RAWMATERIAL_MASTER,TECH_PRODUCT_COMP_DETAILS,   ";
                    SQL = SQL + "         RM_UOM_MASTER ,RM_UOM_CONVERSION,GL_PRODUCT_TYPE_MASTER  ";
                    SQL = SQL + "   WHERE RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE =  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                    SQL = SQL + "     AND RM_STORE_ISSUE_DETAILS.RM_UM_UOM_CODE =  RM_UOM_MASTER.RM_UM_UOM_CODE   ";
                    SQL = SQL + "     AND RM_STORE_ISSUE_DETAILS.GL_PRODUCT_TYPE_CODE =  GL_PRODUCT_TYPE_MASTER.GL_PRODUCT_TYPE_CODE   ";
                    SQL = SQL + "     AND RM_STORE_ISSUE_MASTER.RM_STISU_ENTRY_NO =  RM_STORE_ISSUE_DETAILS.RM_STISU_ENTRY_NO   ";
                    SQL = SQL + "     AND RM_STORE_ISSUE_MASTER.AD_FIN_FINYRID =RM_STORE_ISSUE_DETAILS.AD_FIN_FINYRID   ";
                    SQL = SQL + "     AND RM_STORE_ISSUE_MASTER.TECH_PM_PRODUCT_CODE =TECH_PRODUCT_COMP_DETAILS.TECH_PM_PRODUCT_CODE   ";
                    SQL = SQL + "     AND TECH_PRODUCT_COMP_DETAILS.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                    SQL = SQL + "     AND TECH_PRODUCT_COMP_DETAILS.RM_UOM_UOM_CODE  =RM_UOM_MASTER.RM_UM_UOM_CODE(+)     ";
                    SQL = SQL + "     AND RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE = RM_UOM_CONVERSION.RM_RMM_RM_CODE(+)   ";
                    SQL = SQL + "    AND RM_STORE_ISSUE_DETAILS.RM_UM_UOM_CODE = RM_UOM_CONVERSION.RM_UM_UOM_CODE_TO(+)   ";
                    SQL = SQL + "   AND RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE = RM_UOM_CONVERSION.RM_UM_UOM_CODE_FROM(+)  "; 
                    SQL = SQL + "       AND RM_STORE_ISSUE_DETAILS.AD_FIN_FINYRID = " + FinYr + "";
                    SQL = SQL + "       AND RM_STORE_ISSUE_DETAILS. RM_STISU_ENTRY_NO = '" + Entry_No + "'";
                    SQL = SQL + "   ORDER BY RM_STORE_ISSUE_DETAILS.RM_STISUD_SL_NO " ;

                }else if (IssueType=="FA")
                {
                    SQL = "SELECT RM_STORE_ISSUE_DETAILS.RM_STISU_ENTRY_NO, RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE RMCODE,   ";
                    SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, RM_STISUD_SL_NO, RM_STORE_ISSUE_DETAILS.RM_UM_UOM_CODE,   ";
                    SQL = SQL + "         RM_UOM_MASTER.RM_UM_UOM_DESC, RM_STISUD_AVG_COST, RM_STISUD_AMOUNT, RM_STISUD_QTY, RM_STISUD_SL_NO,   ";
                    SQL = SQL + "         RM_STORE_ISSUE_DETAILS.PC_BUD_BUDGET_ITEM_CODE,GL_PRODUCT_TYPE_MASTER.GL_PRODUCT_TYPE_CODE,GL_PRODUCT_TYPE_DESC   ";
                    SQL = SQL + "    FROM RM_STORE_ISSUE_DETAILS, RM_STORE_ISSUE_MASTER, RM_RAWMATERIAL_MASTER,RM_UOM_MASTER,GL_PRODUCT_TYPE_MASTER   ";
                    SQL = SQL + "   WHERE RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                    SQL = SQL + "     AND RM_STORE_ISSUE_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE   ";
                    SQL = SQL + "     AND RM_STORE_ISSUE_DETAILS.GL_PRODUCT_TYPE_CODE = GL_PRODUCT_TYPE_MASTER.GL_PRODUCT_TYPE_CODE   ";
                    SQL = SQL + "     AND RM_STORE_ISSUE_MASTER.RM_STISU_ENTRY_NO = RM_STORE_ISSUE_DETAILS.RM_STISU_ENTRY_NO   ";
                    SQL = SQL + "     AND RM_STORE_ISSUE_DETAILS.AD_FIN_FINYRID = " + FinYr + "";
                    SQL = SQL + "     AND RM_STORE_ISSUE_DETAILS. RM_STISU_ENTRY_NO = '" + Entry_No + "'";
                    SQL = SQL + "   ORDER BY RM_STORE_ISSUE_DETAILS.RM_STISUD_SL_NO " ;

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

        #endregion

        #region "Physcial entry details XL "
         
        public DataTable RawMaterialQtyCurrentStock ( string stCode,string BrCode,string EntryNo, DateTime dtpTo,string RmCode, int finYrId )
        { 
            DataTable sReturn = new DataTable();
             
            try 
            { 
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                 
                SQL = " SELECT MAINQUERY.SALES_STN_STATION_CODE STATION_CODE, ";
                SQL = SQL + "         MAINQUERY.SALES_STN_STATION_NAME STATION_NAME, ";
                SQL = SQL + "         MAINQUERY.RM_RMM_RM_CODE RM_CODE, ";
                SQL = SQL + "         MAINQUERY.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION, ";
                SQL = SQL + "         MAINQUERY.RM_RMM_RM_TYPE, ";
                SQL = SQL + "         NVL (OPENING.OPQTY, 0) FINYROPTQTY, ";
                SQL = SQL + "         NVL (PREVMTHBAL.PRVGRVQTY, 0) PRVGRVQTY, ";
                SQL = SQL + "         NVL (PREVMTHBAL.PRVISSUEQTY, 0) PRVISSUEVQTY, ";
                SQL = SQL + "        (NVL (OPENING.OPQTY, 0)+ NVL(PREVMTHBAL.PRVGRVQTY,0) - NVL (PREVMTHBAL.PRVISSUEQTY,0)) prevClosing, ";
                SQL = SQL + "         NVL(GRVQry.GRVQty, 0)GRVQty, ";
                SQL = SQL + "         NVL(issueQry.cnvrtIssueQty, 0) cnvrtIssueQty,  ";
                SQL = SQL + "        ((NVL(OPENING.OPQTY, 0)+ NVL(PREVMTHBAL.PRVGRVQTY,0) + NVL (GRVQry.GRVQty,0)) - (NVL (PREVMTHBAL.PRVISSUEQTY, 0) )-NVL(issueQry.cnvrtIssueQty, 0)) CurrClosing,AllowMinusQty ";
                SQL = SQL + "    FROM (SELECT RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE,RM_RMM_RM_DESCRIPTION,SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE, ";
                SQL = SQL + "                 SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME,SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE, ";
                SQL = SQL + "                 SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME, ";
                SQL = SQL + "                 RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE ,";
                SQL = SQL + "                 RM_RMM_ALLOW_MINUS_QTY_YN AllowMinusQty " ;
                SQL = SQL + "            FROM RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "                 RM_RAWMATERIAL_DETAILS, ";
                SQL = SQL + "                 SL_STATION_BRANCH_MAPP_DTLS_VW ";
                SQL = SQL + "           WHERE     RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE = ";
                SQL = SQL + "                     RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE ";
                SQL = SQL + "                 AND RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE = ";
                SQL = SQL + "                     SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE ";
                SQL = SQL + "                 AND RM_RAWMATERIAL_DETAILS.AD_BR_CODE = ";
                SQL = SQL + "                     SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE ";
                SQL = SQL + "                 AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE = ";
                SQL = SQL + "                     RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE ";
                SQL = SQL + "                 AND RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE IN ('" + stCode + "')  ";
                SQL = SQL + "                 AND RM_RAWMATERIAL_DETAILS.AD_BR_CODE='" + BrCode + "') MAINQUERY, ";
                SQL = SQL + "         (  SELECT AD_BR_CODE, ";
                SQL = SQL + "                   SALES_STN_STATION_CODE, ";
                SQL = SQL + "                   RM_RMM_RM_CODE, ";
                SQL = SQL + "                   SUM (RM_OB_QTY)        OPQTY, ";
                SQL = SQL + "                   SUM (RM_OB_AMOUNT)     OPAMOUNT ";
                SQL = SQL + "              FROM RM_OPEN_BALANCES ";
                SQL = SQL + "             WHERE AD_FIN_FINYRID =" + finYrId + " ";
                SQL = SQL + "             AND SALES_STN_STATION_CODE IN ('" + stCode + "')";
                SQL = SQL + "             AND AD_BR_CODE = '" + BrCode + "' ";
                SQL = SQL + "          GROUP BY AD_BR_CODE, SALES_STN_STATION_CODE, RM_RMM_RM_CODE) OPENING, ";
                SQL = SQL + "         (  SELECT AD_BR_CODE, ";
                SQL = SQL + "                   SALES_STN_STATION_CODE, ";
                SQL = SQL + "                   RM_IM_ITEM_CODE, ";
                SQL = SQL + "                   SUM (RM_STKL_RECD_QTY)      PRVGRVQTY, ";
                SQL = SQL + "                   SUM (RM_STKL_ISSUE_QTY)     PRVISSUEQTY ";
                SQL = SQL + "              FROM RM_STOCK_LEDGER ";
                SQL = SQL + "             WHERE     ad_fin_finyrid = " + finYrId + " ";
                SQL = SQL + "                   AND TO_DATE (TO_CHAR (RM_STKL_TRANS_DATE, 'DD-MON-YYYY')) <= ";
                SQL = SQL + "                       (CASE ";
                SQL = SQL + "                            WHEN (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                    FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                   WHERE     RM_PSEM_APPROVED = 'Y' ";
                SQL = SQL + "                                         AND SALES_STN_STATION_CODE IN ('" + stCode + "') ";
                SQL = SQL + "                                         AND AD_BR_CODE='" + BrCode + "' ";
                SQL = SQL + "                                         AND AD_FIN_FINYRID = " + finYrId + ") ";
                SQL = SQL + "                                     IS NULL ";
                SQL = SQL + "                            THEN ";
                SQL = SQL + "                                (SELECT AD_FIN_FROM_DATE ";
                SQL = SQL + "                                   FROM AD_FINANCIAL_YEAR ";
                SQL = SQL + "                                  WHERE AD_FIN_FINYRID = " + finYrId + ") ";
                SQL = SQL + "                            ELSE ";
                SQL = SQL + "                                (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                   FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                  WHERE     RM_PSEM_APPROVED = 'Y' ";
                SQL = SQL + "                                        AND SALES_STN_STATION_CODE IN ('" + stCode + "') ";
                SQL = SQL + "                                        AND AD_BR_CODE='" + BrCode + "' ";
                SQL = SQL + "                                        AND AD_FIN_FINYRID = " + finYrId + ") ";
                SQL = SQL + "                        END) ";
                SQL = SQL + "          GROUP BY AD_BR_CODE, SALES_STN_STATION_CODE, RM_IM_ITEM_CODE) ";
                SQL = SQL + "         PREVMTHBAL, ";
                SQL = SQL + "         (SELECT AD_BR_CODE, ";
                SQL = SQL + "                 RM_IM_ITEM_CODE, ";
                SQL = SQL + "                 SALES_STN_STATION_CODE, ";
                SQL = SQL + "                 GrvQTY ";
                SQL = SQL + "            FROM (  SELECT AD_BR_CODE, ";
                SQL = SQL + "                           RM_IM_ITEM_CODE, ";
                SQL = SQL + "                           SALES_STN_STATION_CODE, ";
                SQL = SQL + "                           SUM (GrvQTY)     GrvQTY ";
                SQL = SQL + "                      FROM (  SELECT AD_BR_CODE, ";
                SQL = SQL + "                                     RM_RMM_RM_CODE               RM_IM_ITEM_CODE, ";
                SQL = SQL + "                                     SALES_STN_STATION_CODE, ";
                SQL = SQL + "                                     SUM (RM_MRD_APPROVE_QTY)     GrvQTY ";
                SQL = SQL + "                                FROM RM_RECEPIT_DETAILS ";
                SQL = SQL + "                               WHERE     SALES_STN_STATION_CODE IN ('" + stCode + "') ";
                SQL = SQL + "                                     AND AD_BR_CODE='" + BrCode + "' ";
                SQL = SQL + "                                     AND ad_fin_finyrid = " + finYrId + " ";
                SQL = SQL + "                                     AND RM_MRM_RECEIPT_DATE > ";
                SQL = SQL + "                                         (CASE ";
                SQL = SQL + "                                              WHEN (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                                      FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                                     WHERE     RM_PSEM_APPROVED = ";
                SQL = SQL + "                                                               'Y' ";
                SQL = SQL + "                                                           AND SALES_STN_STATION_CODE IN  ('" + stCode + "') ";
                SQL = SQL + "                                                           AND AD_BR_CODE='" + BrCode + "' ";
                SQL = SQL + "                                                           AND AD_FIN_FINYRID =" + finYrId + ") ";
                SQL = SQL + "                                                       IS NULL ";
                SQL = SQL + "                                              THEN ";
                SQL = SQL + "                                                  (SELECT AD_FIN_FROM_DATE ";
                SQL = SQL + "                                                     FROM AD_FINANCIAL_YEAR ";
                SQL = SQL + "                                                    WHERE AD_FIN_FINYRID =" + finYrId + ") ";
                SQL = SQL + "                                              ELSE ";
                SQL = SQL + "                                                  (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                                     FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                                    WHERE     RM_PSEM_APPROVED = ";
                SQL = SQL + "                                                              'Y' ";
                SQL = SQL + "                                                          AND SALES_STN_STATION_CODE IN  ('" + stCode + "') ";
                SQL = SQL + "                                                          AND AD_BR_CODE='" + BrCode + "' ";
                SQL = SQL + "                                                          AND AD_FIN_FINYRID = " + finYrId + ") ";
                SQL = SQL + "                                          END) ";
                SQL = SQL + "                                     AND RM_MRM_RECEIPT_DATE <=  '" + System.Convert.ToDateTime(dtpTo).ToString("dd-MMM-yyyy") + "' ";
                SQL = SQL + "                            GROUP BY AD_BR_CODE, ";
                SQL = SQL + "                                     RM_RMM_RM_CODE, ";
                SQL = SQL + "                                     SALES_STN_STATION_CODE ";
                SQL = SQL + "                            UNION ALL ";
                SQL = SQL + "                              SELECT AD_BR_CODE, ";
                SQL = SQL + "                                     RM_IM_ITEM_CODE, ";
                SQL = SQL + "                                     SALES_STN_STATION_CODE, ";
                SQL = SQL + "                                     SUM (RM_STKL_RECD_QTY - RM_STKL_ISSUE_QTY)    StockTransferQty ";
                SQL = SQL + "                                FROM RM_STOCK_LEDGER ";
                SQL = SQL + "                               WHERE     RM_STKL_TRANS_TYPE = 'STK_TRANS' ";
                SQL = SQL + "                                     AND ad_fin_finyrid = " + finYrId + " ";
                SQL = SQL + "                                     AND SALES_STN_STATION_CODE IN ('" + stCode + "') ";
                SQL = SQL + "                                     AND AD_BR_CODE='" + BrCode + "'";
                SQL = SQL + "                                     AND ad_fin_finyrid = " + finYrId + " ";
                SQL = SQL + "                                     AND RM_STKL_TRANS_DATE > ";
                SQL = SQL + "                                         (CASE ";
                SQL = SQL + "                                              WHEN (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                                      FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                                     WHERE     RM_PSEM_APPROVED = ";
                SQL = SQL + "                                                               'Y' ";
                SQL = SQL + "                                                           AND SALES_STN_STATION_CODE IN  ('" + stCode + "') ";
                SQL = SQL + "                                                           AND AD_BR_CODE='" + BrCode + "' ";
                SQL = SQL + "                                                           AND AD_FIN_FINYRID = " + finYrId + ") ";
                SQL = SQL + "                                                       IS NULL ";
                SQL = SQL + "                                              THEN ";
                SQL = SQL + "                                                  (SELECT AD_FIN_FROM_DATE ";
                SQL = SQL + "                                                     FROM AD_FINANCIAL_YEAR ";
                SQL = SQL + "                                                    WHERE AD_FIN_FINYRID =" + finYrId + ") ";
                SQL = SQL + "                                              ELSE ";
                SQL = SQL + "                                                  (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                                     FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                                    WHERE     RM_PSEM_APPROVED = ";
                SQL = SQL + "                                                              'Y' ";
                SQL = SQL + "                                                          AND SALES_STN_STATION_CODE IN ('" + stCode + "') ";
                SQL = SQL + "                                                          AND AD_BR_CODE='" + BrCode + "'";
                SQL = SQL + "                                                          AND AD_FIN_FINYRID = " + finYrId + ") ";
                SQL = SQL + "                                          END) ";
                SQL = SQL + "                                     AND RM_STKL_TRANS_DATE <=  '" + System.Convert.ToDateTime(dtpTo).ToString("dd-MMM-yyyy") + "' ";
                SQL = SQL + "                            GROUP BY AD_BR_CODE, ";
                SQL = SQL + "                                     RM_IM_ITEM_CODE, ";
                SQL = SQL + "                                     SALES_STN_STATION_CODE) InQry ";
                SQL = SQL + "                  GROUP BY AD_BR_CODE, RM_IM_ITEM_CODE, SALES_STN_STATION_CODE)) ";
                SQL = SQL + "         GRVQry, ";
                SQL = SQL + "         (select RM_RMM_RM_CODE, ";
                SQL = SQL + "                   AD_BR_CODE, ";
                SQL = SQL + "                   SALES_STN_STATION_CODE, Sum(IssueQty) IssueQty, ";
                SQL = SQL + "                   SUM (cnvrtIssueQty)cnvrtIssueQty  ";
                SQL = SQL + "        FROM( SELECT  RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE, AD_BR_CODE,SALES_STN_STATION_CODE,sum(RM_STISUD_QTY) IssueQty,RM_UCM_FACTOR , ";
                SQL = SQL + "               CASE WHEN NVL (RM_UCM_FACTOR, 0) > 0 ";
                SQL = SQL + "               THEN (sum(RM_STISUD_QTY) / RM_UCM_FACTOR) ";
                SQL = SQL + "               ELSE sum(RM_STISUD_QTY) ";
                SQL = SQL + "                END AS  cnvrtIssueQty  ";
                SQL = SQL + "          FROM RM_STORE_ISSUE_MASTER,RM_STORE_ISSUE_DETAILS,RM_UOM_CONVERSION ";
                SQL = SQL + "         WHERE RM_STORE_ISSUE_MASTER.RM_STISU_ENTRY_NO =RM_STORE_ISSUE_DETAILS.RM_STISU_ENTRY_NO ";
                SQL = SQL + "           AND RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE=RM_UOM_CONVERSION.RM_RMM_RM_CODE (+)  ";
                SQL = SQL + "           AND RM_STORE_ISSUE_DETAILS.RM_UM_UOM_CODE = RM_UOM_CONVERSION.RM_UM_UOM_CODE_TO(+)  ";
                SQL = SQL + "           and RM_STORE_ISSUE_MASTER.AD_FIN_FINYRID=" + finYrId + " ";
                SQL = SQL + "           AND SALES_STN_STATION_CODE IN ('" + stCode + "') ";
                SQL = SQL + "           AND AD_BR_CODE='" + BrCode + "'";
                SQL = SQL + "           AND RM_STORE_ISSUE_MASTER.RM_STISU_ENTRY_NO NOT IN ('" + EntryNo + "')";
                SQL = SQL + "           and RM_STISU_ENTRY_DATE >  (CASE ";
                SQL = SQL + "                                      WHEN (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                                   FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                                  WHERE RM_PSEM_APPROVED = 'Y' ";
                SQL = SQL + "                                                    AND SALES_STN_STATION_CODE IN ('" + stCode + "') ";
                SQL = SQL + "                                                    AND AD_BR_CODE='" + BrCode + "' ";
                SQL = SQL + "                                                    AND AD_FIN_FINYRID = " + finYrId + ") ";
                SQL = SQL + "                                                IS NULL ";
                SQL = SQL + "                                        THEN ";
                SQL = SQL + "                                            (SELECT AD_FIN_FROM_DATE ";
                SQL = SQL + "                                            FROM AD_FINANCIAL_YEAR ";
                SQL = SQL + "                                            WHERE AD_FIN_FINYRID = " + finYrId + ") ";
                SQL = SQL + "                                        ELSE ";
                SQL = SQL + "                                            (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                            FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                            WHERE     RM_PSEM_APPROVED = 'Y' ";
                SQL = SQL + "                                                    AND SALES_STN_STATION_CODE IN ('" + stCode + "') ";
                SQL = SQL + "                                                    AND AD_BR_CODE='" + BrCode + "' ";
                SQL = SQL + "                                                    AND AD_FIN_FINYRID =" + finYrId + ") ";
                SQL = SQL + "                                    END)  ";
                SQL = SQL + "      group by  RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE, AD_BR_CODE,SALES_STN_STATION_CODE,RM_UCM_FACTOR ";
                SQL = SQL + "      ) ";
                SQL = SQL + "      group by RM_RMM_RM_CODE, ";
                SQL = SQL + "      AD_BR_CODE, ";
                SQL = SQL + "      SALES_STN_STATION_CODE ";
                SQL = SQL + "      ) issueQry ";
                SQL = SQL + "       WHERE MAINQUERY.RM_RMM_RM_CODE = OPENING.RM_RMM_RM_CODE(+) ";
                SQL = SQL + "         AND MAINQUERY.SALES_STN_STATION_CODE = OPENING.SALES_STN_STATION_CODE(+) ";
                SQL = SQL + "         AND MAINQUERY.AD_BR_CODE = OPENING.AD_BR_CODE(+) ";
                SQL = SQL + "         AND MAINQUERY.RM_RMM_RM_CODE = PREVMTHBAL.RM_IM_ITEM_CODE(+) ";
                SQL = SQL + "         AND MAINQUERY.SALES_STN_STATION_CODE = PREVMTHBAL.SALES_STN_STATION_CODE(+) ";
                SQL = SQL + "         AND MAINQUERY.AD_BR_CODE = PREVMTHBAL.AD_BR_CODE(+) ";
                SQL = SQL + "         AND MAINQUERY.RM_RMM_RM_CODE = GRVQry.RM_IM_ITEM_CODE(+) ";
                SQL = SQL + "         AND MAINQUERY.SALES_STN_STATION_CODE = GRVQry.SALES_STN_STATION_CODE(+) ";
                SQL = SQL + "         AND MAINQUERY.AD_BR_CODE = GRVQry.AD_BR_CODE(+) ";
                SQL = SQL + "         AND MAINQUERY.RM_RMM_RM_CODE = issueQry.RM_RMM_RM_CODE(+) ";
                SQL = SQL + "         AND MAINQUERY.SALES_STN_STATION_CODE = issueQry.SALES_STN_STATION_CODE(+) ";
                SQL = SQL + "         AND MAINQUERY.AD_BR_CODE = issueQry.AD_BR_CODE(+)   ";
                SQL = SQL + "         AND MAINQUERY.RM_RMM_RM_CODE in ( "+RmCode+" ) ";
                SQL = SQL + "ORDER BY MAINQUERY.AD_BR_CODE, MAINQUERY.SALES_STN_STATION_CODE, MAINQUERY.RM_RMM_RM_CODE ";


                sReturn = clsSQLHelper.GetDataTableByCommand(SQL);


            }
            catch (Exception ex)

            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();

                objLogWriter.WriteLog(ex);

                objLogWriter.WriteSQL(SQL);

            }

            return sReturn;

        }

        #endregion

        #region Get Item"

        public DataTable FillGridItemView ( string sSelected )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT  ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code, ";
                SQL = SQL + " RM_RMM_RM_DESCRIPTION Name, ";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC RM_UM_UOM_DESC, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE RM_UM_UOM_CODE,  ";
                SQL = SQL + " '' PC_BUD_BUDGET_ITEM_CODE   ";
                SQL = SQL + " FROM  RM_RAWMATERIAL_MASTER,RM_UOM_MASTER   ";
                SQL = SQL + " WHERE  RM_UOM_MASTER.RM_UM_UOM_CODE=RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE ";

                if (!string.IsNullOrEmpty(sSelected))
                {
                    SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE NOT IN ('" + sSelected + "') ";
                }

                SQL = SQL + "  ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";


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

        public DataSet FetchVoucherPrintData ( string voucherno,string IssueType, object mngrclassobj )
        {
            DataSet dSPrint = new DataSet("VOUCHERPRINT");

            DataTable dtMaster = new DataTable();
            DataTable dtMatchDetails = new DataTable();
            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

               // if(IssueType=="CN")
               // {
                    SQL = " SELECT   SLNO, ENTRY_NO, ENTRY_DATE,RQ_ENTRY_NO, FINID, ACCOUNT_CODE, ACCOUNT_NAME,   ";
                    SQL = SQL + " STATION_CODE, STATION_NAME, DEPT_CODE, DEPT_DESC,REMARKS, MATAMOUNT,  " ;
                    SQL = SQL + "  RM_STISU_CREATEDBY, RM_STISU_APPROVED_BY, RMCODE, RMDESC, PRODUCT_CODE,PRODUCT_DESC,  " ;
                    SQL = SQL + " UOM_CODE, UOM_DESC, AVG_COST,  QUANTITY, EXT_AMNT, Req_Qty, bal_qty,   " ;
                    SQL = SQL + " PC_SALES_INQM_ENQUIRY_NO,ad_br_code,   " ;
                    SQL = SQL + " MASTER_BRANCH_CODE,    MASTER_BRANCH_NAME,    MASTER_BRANCHDOC_PREFIX,   " ;
                    SQL = SQL + " MASTER_BRANCH_POBOX,    MASTER_BRANCH_ADDRESS,    MASTER_BRANCH_CITY,   " ;
                    SQL = SQL + "MASTER_BRANCH_PHONE,    MASTER_BRANCH_FAX,    MASTER_BRANCH_SPONSER_NAME,   " ;
                    SQL = SQL + "   MASTER_BRANCH_TRADE_LICENSE_NO,    MASTER_BRANCH_EMAIL_ID,    MASTER_BRANCH_WEB_SITE,   " ;
                    SQL = SQL + "   MASTER_BRANCH_VAT_REG_NUMBER   " ;
                    SQL = SQL + " FROM RM_STORE_ISSUE_VOUCHER_PRINT  ";
                    SQL = SQL + " WHERE  " ;
                    SQL = SQL + " ENTRY_NO='" + voucherno + "' AND FINID=" + mngrclass.FinYearID + " ";
                    SQL = SQL + " ORDER BY ENTRY_NO, SLNO  " ;


                //}
                //else
                //{
                //    SQL = " SELECT   SLNO, ENTRY_NO, ENTRY_DATE,RQ_ENTRY_NO, FINID, ACCOUNT_CODE, ACCOUNT_NAME,  ";
                //    SQL = SQL + " STATION_CODE, STATION_NAME, DEPT_CODE, DEPT_DESC,REMARKS, MATAMOUNT, ";
                //    SQL = SQL + "  RM_STISU_CREATEDBY, RM_STISU_APPROVED_BY, RMCODE, RMDESC,'' PRODUCT_CODE,'' PRODUCT_DESC,  ";
                //    SQL = SQL + " UOM_CODE, UOM_DESC, AVG_COST,  QUANTITY, EXT_AMNT, Req_Qty, bal_qty,  ";
                //    SQL = SQL + " PC_SALES_INQM_ENQUIRY_NO,ad_br_code,  ";
                //    SQL = SQL + " MASTER_BRANCH_CODE,    MASTER_BRANCH_NAME,    MASTER_BRANCHDOC_PREFIX,  ";
                //    SQL = SQL + " MASTER_BRANCH_POBOX,    MASTER_BRANCH_ADDRESS,    MASTER_BRANCH_CITY,  ";
                //    SQL = SQL + "MASTER_BRANCH_PHONE,    MASTER_BRANCH_FAX,    MASTER_BRANCH_SPONSER_NAME,  ";
                //    SQL = SQL + "   MASTER_BRANCH_TRADE_LICENSE_NO,    MASTER_BRANCH_EMAIL_ID,    MASTER_BRANCH_WEB_SITE,  ";
                //    SQL = SQL + "   MASTER_BRANCH_VAT_REG_NUMBER  ";

                //    SQL = SQL + " FROM RM_STORE_ISSUE_VOUCHER_PRINT ";
                //    SQL = SQL + " WHERE ";
                //    SQL = SQL + " ENTRY_NO='" + voucherno + "' AND FINID=" + mngrclass.FinYearID + " ";
                //    SQL = SQL + " ORDER BY ENTRY_NO, SLNO ";
                //}
                

                dtMaster = clsSQLHelper.GetDataTableByCommand(SQL);
                dtMaster.TableName = "RM_STORE_ISSUE_VOUCHER_PRINT";
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


    }

    //SalesRMLogic  MaterialRequestRMLogic   //MaterialIssueToSiteRMLogicEntity 
    //MaterialRequestRMEntityDetails 
    public class MaterialIssueToSiteRMEntity
    {
        public string txtEntryNo
        {
            get;
            set;
        }
        public string txtReqEntryNo
        {
            get;
            set;
        }
        public int txtReqFinyr
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

        public string EnquiryNo
        {
            get;
            set;
        }
        public string EmpCode
        {
            get;
            set;
        }

        public string EmpName
        {
            get;
            set;
        }

        public string CostCenter
        {
            get;
            set;
        }

        public string IssueType
        {
            get;
            set;
        }
        
        public string IssueTo
        {
            get;
            set;
        }

        public string ProductCode
        {
            get;
            set;
        }

        public double ProductQty
        {
            get;
            set;
        }

        public string txtDept
        {
            get;
            set;
        }

        public string txtRemarks
        {
            get;
            set;
        }

        public string chkVerify
        {
            get;
            set;
        }

        public string txtVerifiedBy
        {
            get;
            set;
        }

        public string txtVerifiedDate
        {
            get;
            set;
        }

        public string chkApprv
        {
            get;
            set;
        }

        public string txtAppdBy
        {
            get;
            set;
        }

        public string txtAppdDate
        {
            get;
            set;
        }

        public DateTime dtpEntryDate
        {
            get;
            set;
        }

        public double txtGradndTotalAmount
        {
            get;
            set;
        }


    }
    //MaterialIssueToSiteRMEntity  //MaterialIssueToSiteRMEntityDetails
    public class MaterialIssueToSiteRMEntityDetails
    {
        public string slNoDetails { get; set; }
        public string BudgetItemCodeDetails { get; set; }
        public string ProductType { get; set; }
        public string rmCodeDetails { get; set; }
        public string uomCodeDetails { get; set; }
        public double avgCostDetails { get; set; }
        public double qtyDetails { get; set; }
        public double AmountDetails { get; set; }
        public string ExpDateDetails { get; set; }
        public double BalQtyDetails { get; set; }
    }
}