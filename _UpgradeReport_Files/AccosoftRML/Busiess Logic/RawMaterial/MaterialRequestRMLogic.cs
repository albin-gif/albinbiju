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
    public class MaterialRequestRMLogic
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

        public DataTable FillViewSalesEntry ( string fromdate, string todate, string EntryType, object mngrclassobj )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                double dGrantedMaterialReqSiteCount = GetGrantedDepartment(mngrclass.UserName, "RTSTRREQDEPVWALL");


                SQL = SQL + " SELECT  RM_SRQ_ENTRY_NO ENTRYNO, to_char(RM_SRQ_ENTRY_DATE) ENTRY_DATE,AD_FIN_FINYRID,  ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.HR_DEPT_DEPT_CODE,HR_DEPT_DEPT_DESC,  ";
                SQL = SQL + "         RM_SRQ_APPROVED,";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.GL_COSM_ACCOUNT_CODE CUSTOMER_CODE,  ";
                SQL = SQL + "         GL_COSTING_MASTER.GL_COSM_ACCOUNT_NAME CUSTOMER_NAME ,  ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.PC_SALES_INQM_ENQUIRY_NO ";
                SQL = SQL + "    FROM RM_STORE_REQUEST_MASTER,  ";
                SQL = SQL + "         GL_COSTING_MASTER,  ";
                SQL = SQL + "         HR_DEPT_MASTER ,";
                SQL = SQL + "         SL_STATION_MASTER ";
                SQL = SQL + "   WHERE RM_STORE_REQUEST_MASTER.GL_COSM_ACCOUNT_CODE = GL_COSTING_MASTER.GL_COSM_ACCOUNT_CODE(+)  ";
                SQL = SQL + "   AND RM_STORE_REQUEST_MASTER.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE(+)  ";
                SQL = SQL + "   AND RM_STORE_REQUEST_MASTER.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE(+)  ";
                SQL = SQL + "     AND RM_STORE_REQUEST_MASTER.AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";


                if (EntryType == "Y")
                {
                    SQL = SQL + " AND RM_SRQ_APPROVED ='Y' AND RM_SRQ_CANCELLED_YN='N'";
                }
                else if (EntryType == "N")
                {
                    SQL = SQL + " AND RM_SRQ_APPROVED ='N' AND RM_SRQ_CANCELLED_YN ='N'";
                }
                else if (EntryType == "C")
                {
                    SQL = SQL + " AND RM_SRQ_CANCELLED_YN ='Y'";
                }
                SQL = SQL + " AND RM_SRQ_ENTRY_DATE BETWEEN '" + Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";

                if (mngrclass.UserName != "ADMIN")
                {
                    SQL = SQL + " AND RM_STORE_REQUEST_MASTER.AD_BR_CODE in ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";

                    if (dGrantedMaterialReqSiteCount == 0)
                    {
                        SQL = SQL + " AND    RM_STORE_REQUEST_MASTER.AD_BR_CODE || RM_STORE_REQUEST_MASTER.HR_DEPT_DEPT_CODE IN  ";
                        SQL = SQL + "   (   ";
                        SQL = SQL + " SELECT  AD_BR_CODE||HR_DEPT_DEPT_CODE   FROM    AD_WS_GRANTED_BR_DEPT_USERS ";
                        SQL = SQL + "  WHERE  WS_GDU_DELETESTATUS = 0 and ad_um_userid ='" + mngrclass.UserName + "' )";// Abin added on 05/Jul/2023 
                    }

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


        public double GetGrantedDepartment ( string UserName, string ApprovalTypeID )
        {

            DataTable dtReturn = new DataTable();
            double dGrantedProjectCount = 0;
            try
            {

                String SQLQ = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQLQ = "    Select count(*)      GRANTEDApprovalCOUNT   FROM ad_mail_approval_users ";
                SQLQ = SQLQ + "  WHERE AD_UM_USERID =  '" + UserName + "' and AD_AF_ID='" + ApprovalTypeID + "'";

                dtReturn = clsSQLHelper.GetDataTableByCommand(SQLQ);
                dGrantedProjectCount = double.Parse(dtReturn.Rows[0]["GRANTEDApprovalCOUNT"].ToString());



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
        #endregion

        #region "Insert/Update/Delete"

        public string InsertMasterSql ( MaterialRequestRMEntity oEntity, List<MaterialRequestRMEntityDetails> EntityDetails, bool Autogen, object mngrclassobj, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = "INSERT INTO RM_STORE_REQUEST_MASTER ( ";
                SQL = SQL + "   RM_SRQ_ENTRY_NO, RM_SRQ_ENTRY_DATE, AD_FIN_FINYRID,  ";
                SQL = SQL + "   AD_BR_CODE, PC_SALES_INQM_ENQUIRY_NO, GL_COSM_ACCOUNT_CODE,  ";
                SQL = SQL + "   SALES_STN_STATION_CODE, HR_DEPT_DEPT_CODE, RM_SRQ_TOTAL_AMOUNT,  ";
                SQL = SQL + "   RM_SRQ_REMARKS, RM_SRQ_CREATEDBY, RM_SRQ_CREATEDDATE,  ";
                SQL = SQL + "   RM_SRQ_VERIFIED,RM_SRQ_VERIFIED_BY, RM_SRQ_VERIFIED_DATE,   ";
                SQL = SQL + "   RM_SRQ_APPROVED, RM_SRQ_APPROVED_BY,RM_SRQ_APPROVED_DATE,   ";
                SQL = SQL + "   RM_SRQ_DELETESTATUS, AD_CM_COMPANY_CODE)  ";

                SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "', '" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "', " + mngrclass.FinYearID + ",";
                SQL = SQL + " '" + oEntity.cboBranch + "','" + oEntity.EnquiryNo + "','" + oEntity.CostCenter + "',";
                SQL = SQL + " '" + oEntity.cboStation + "', '" + oEntity.txtDept + "'," + Convert.ToDouble(oEntity.txtGradndTotalAmount) + ",  ";
                SQL = SQL + " '" + oEntity.txtRemarks + "', '" + mngrclass.UserName + "', TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " '" + oEntity.chkVerify + "','" + oEntity.txtVerifiedBy + "',TO_DATE('" + oEntity.txtVerifiedDate + "','DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + " '" + oEntity.chkApprv + "','" + oEntity.txtAppdBy + "',TO_DATE('" + oEntity.txtAppdDate + "','DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + "  0,'" + mngrclass.CompanyCode + "'";
                SQL = SQL + ")";

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

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTSTRREQ", oEntity.txtEntryNo, Autogen, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.cboBranch, sAtuoGenBranchDocNumber);

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

        private string InsertDetails ( MaterialRequestRMEntity oEntity, List<MaterialRequestRMEntityDetails> EntityDetails, object mngrclassobj )
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
                            SQL = "INSERT INTO RM_STORE_REQUEST_DETAILS ( ";
                            SQL = SQL + "   RM_SRQ_ENTRY_NO, RM_RMM_RM_CODE, AD_FIN_FINYRID,  ";
                            SQL = SQL + "   RM_SRQD_SL_NO, PC_BUD_BUDGET_ITEM_CODE, RM_UM_UOM_CODE,  ";
                            SQL = SQL + "   RM_SRQD_AVG_COST, RM_SRQD_QTY, RM_SRQD_AMOUNT,  ";
                            SQL = SQL + "   RM_SRQD_EXPECTED_DATE, RM_SRQD_ISSUE_QTY_BAL)  ";

                            SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "','" + Data.rmCodeDetails + "', " + mngrclass.FinYearID + ",";
                            SQL = SQL + " " + i + ", '" + Data.BudgetItemCodeDetails + "','" + Data.uomCodeDetails + "',";
                            SQL = SQL + " " + Convert.ToDouble(Data.avgCostDetails) + " , " + Convert.ToDouble(Data.qtyDetails) + ", " + Convert.ToDouble(Data.AmountDetails) + ",";
                            SQL = SQL + " '" + Convert.ToDateTime(Data.ExpDateDetails).ToString("dd-MMM-yyyy") + "'," + Convert.ToDouble(Data.BalQtyDetails) + "  ";

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

        private string InsertApprovalQrs ( MaterialRequestRMEntity oEntity, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " INSERT INTO RM_STORE_REQUEST_TRIGGER ( ";
                SQL = SQL + " RM_SRQ_ENTRY_NO, AD_FIN_FINYRID,  RM_SRQ_APPROVED_BY) ";
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

        public string UpdateMasterSql ( MaterialRequestRMEntity oEntity, List<MaterialRequestRMEntityDetails> EntityDetails, bool Autogen, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " UPDATE RM_STORE_REQUEST_MASTER";

                SQL = SQL + "   SET RM_SRQ_ENTRY_DATE          = '" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "', ";
                SQL = SQL + "       AD_BR_CODE                 = '" + oEntity.cboBranch + "', ";
                SQL = SQL + "       PC_SALES_INQM_ENQUIRY_NO   = '" + oEntity.EnquiryNo + "', ";
                SQL = SQL + "       GL_COSM_ACCOUNT_CODE       = '" + oEntity.CostCenter + "', ";
                SQL = SQL + "       SALES_STN_STATION_CODE     = '" + oEntity.cboStation + "', ";
                SQL = SQL + "       HR_DEPT_DEPT_CODE          = '" + oEntity.txtDept + "', ";
                SQL = SQL + "       RM_SRQ_TOTAL_AMOUNT        = " + Convert.ToDouble(oEntity.txtGradndTotalAmount) + ", ";
                SQL = SQL + "       RM_SRQ_REMARKS             = '" + oEntity.txtRemarks + "', ";
                SQL = SQL + "       RM_SRQ_UPDATEDBY           = '" + mngrclass.UserName + "', ";
                SQL = SQL + "       RM_SRQ_UPDATEDDATE         =  TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + "       RM_SRQ_VERIFIED            = '" + oEntity.chkVerify + "', ";
                SQL = SQL + "       RM_SRQ_VERIFIED_BY         = '" + oEntity.txtVerifiedBy + "', ";
                SQL = SQL + "       RM_SRQ_VERIFIED_DATE       =  TO_DATE('" + oEntity.txtVerifiedDate + "','DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + "       RM_SRQ_APPROVED            = '" + oEntity.chkApprv + "', ";
                SQL = SQL + "       RM_SRQ_APPROVED_BY         = '" + oEntity.txtAppdBy + "', ";
                SQL = SQL + "       RM_SRQ_APPROVED_DATE       =  TO_DATE('" + oEntity.txtAppdDate + "','DD-MM-YYYY HH:MI:SS AM') ";
                SQL = SQL + " WHERE RM_SRQ_ENTRY_NO = '" + oEntity.txtEntryNo + "' AND ad_fin_finyrid = " + mngrclass.FinYearID + " AND RM_SRQ_DELETESTATUS = 0";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_STORE_REQUEST_DETAILS";
                SQL = SQL + " WHERE RM_SRQ_ENTRY_NO = '" + oEntity.txtEntryNo + "'";
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

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTSTRREQ", oEntity.txtEntryNo, Autogen, Environment.MachineName, "U", sSQLArray);

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

                SQL = " Delete from RM_STORE_REQUEST_MASTER";
                SQL = SQL + " WHERE RM_SRQ_ENTRY_NO = '" + txtEntryNo + "' AND ad_fin_finyrid = " + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);

                SQL = " Delete from RM_STORE_REQUEST_DETAILS";
                SQL = SQL + " WHERE RM_SRQ_ENTRY_NO ='" + txtEntryNo + "' AND ad_fin_finyrid = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                SQL = " Delete from RM_STORE_REQUEST_TRIGGER";
                SQL = SQL + " WHERE RM_SRQ_ENTRY_NO ='" + txtEntryNo + "' AND ad_fin_finyrid = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTSTRREQ", txtEntryNo, false, Environment.MachineName, "D", sSQLArray);
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

        public string REQCancel ( string PRno, string Cancelremarks, object mngrclassobj )
        {

            string sRetun = string.Empty;
            // string sApprStatus = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " UPDATE RM_STORE_REQUEST_MASTER   ";
                SQL = SQL + "  SET  RM_SRQ_CANCELLED_YN  = 'Y',RM_SRQ_CANCELLED_BY='" + mngrclass.UserName + "' ,";
                SQL = SQL + "RM_SRQ_CANCELLED_DATE  = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'), RM_SRQ_CANCELLED_REMARKS ='" + Cancelremarks + "'";
                SQL = SQL + "  WHERE   RM_SRQ_ENTRY_NO  = '" + PRno + "'  AND ad_fin_finyrid = " + mngrclass.FinYearID;
                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTSTRREQ", PRno, false, Environment.MachineName, "U", sSQLArray);

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


        public string openclose ( string requestno, int FinYr, object mngrclassobj )
        {


            string sReturn = string.Empty;
            SQL = string.Empty;
            try
            {

                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                sSQLArray.Clear();

                SQL = "UPDATE RM_STORE_REQUEST_MASTER  set  RM_SRQ_ISSUE_STATUS_OC ='C'   where RM_SRQ_ENTRY_NO='" + requestno + "' and AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                //do jomy 13 oct 2016 

                SQL = "UPDATE RM_STORE_REQUEST_DETAILS   set  RM_SRQD_ISSUE_STATUS_OC ='C'   where RM_SRQ_ENTRY_NO='" + requestno + "' and AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
                sSQLArray.Add(SQL);

                sReturn = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTSTRREQ", requestno, false, Environment.MachineName, "U", sSQLArray);

            }
            catch (Exception ex)
            {
                sReturn = System.Convert.ToString(ex.Message);
            }
            return sReturn;
        }

        public string UnApproveSQL ( string requestno, object mngrclassobj )
        {


            string sReturn = string.Empty;
            SQL = string.Empty;
            try
            {
                //RM_STORE_REQUEST_TRIGGER
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                sSQLArray.Clear();
                DataSet dsCnt = new DataSet();
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  ";
                SQL = SQL + " COUNT(RM_SRQ_ENTRY_NO) CNT  ";
                SQL = SQL + " FROM ";
                SQL = SQL + "  RM_STORE_ISSUE_MASTER   ";
                SQL = SQL + " WHERE   RM_SRQ_ENTRY_NO='" + requestno + "' ";

                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "Referernce exists in Material Issue";
                }

                SQL = "UPDATE RM_STORE_REQUEST_MASTER  set  RM_SRQ_APPROVED ='N', ";
                SQL = SQL + "       RM_SRQ_UPDATEDBY           = '" + mngrclass.UserName + "', ";
                SQL = SQL + "       RM_SRQ_UPDATEDDATE         =  TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM') ";
                SQL = SQL + "   WHERE RM_SRQ_ENTRY_NO='" + requestno + "' and AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                SQL = " Delete from RM_STORE_REQUEST_TRIGGER";
                SQL = SQL + " WHERE RM_SRQ_ENTRY_NO ='" + requestno + "' AND ad_fin_finyrid = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                sReturn = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTSTRREQ", requestno, false, Environment.MachineName, "U", sSQLArray);

            }
            catch (Exception ex)
            {
                sReturn = System.Convert.ToString(ex.Message);
            }
            return sReturn;
        }

        public string closeopen ( string requestno, object mngrclassobj )
        {


            string sReturn = string.Empty;
            SQL = string.Empty;
            try
            {

                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                sSQLArray.Clear();

                SQL = "UPDATE RM_STORE_REQUEST_MASTER  set  RM_SRQ_ISSUE_STATUS_OC ='O'   where RM_SRQ_ENTRY_NO='" + requestno + "' and AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);


                SQL = "UPDATE RM_STORE_REQUEST_DETAILS   set  RM_SRQD_ISSUE_STATUS_OC ='O'   where RM_SRQ_ENTRY_NO='" + requestno + "' and AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
                sSQLArray.Add(SQL);

                sReturn = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTSTRREQ", requestno, false, Environment.MachineName, "U", sSQLArray);

            }
            catch (Exception ex)
            {
                sReturn = System.Convert.ToString(ex.Message);
            }
            return sReturn;
        }

        public double StoreReqIssueBal ( string requestno, object mngrclassobj )
        {


            string sReturn = string.Empty;
            SQL = string.Empty;
            double QtyCount = 0;
            try
            {
                DataTable dtData = new DataTable();

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = "select NVL(count(RM_SRQD_ISSUE_QTY_BAL),0) cnt from  RM_STORE_REQUEST_DETAILS  where RM_SRQ_ENTRY_NO='" + requestno + "' and AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";

                SQL = SQL + " AND RM_SRQD_ISSUE_QTY_BAL > 0 ";


                dtData = clsSQLHelper.GetDataTableByCommand(SQL);


                if (dtData.Rows.Count > 0)
                {
                    QtyCount = Convert.ToDouble(dtData.Rows[0]["cnt"].ToString());
                }
                else
                {
                    QtyCount = 0;
                }


            }
            catch (Exception ex)
            {
                sReturn = System.Convert.ToString(ex.Message);
            }
            return QtyCount;
        }

        #endregion

        #region "Fetch Datas"
        public DataTable ProjectCostCodeLookUpContrledPermission ( string sUSERNAME )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();



                ////pleads ot no un commen jomy 02 jan 2018 

                //OracleCommand ocCommand = new OracleCommand("PK_GET_GRANTED_PROJECT.Get_GrantedProjectsForRequest");
                //ocCommand.Connection = ocConn;
                //ocCommand.CommandType = CommandType.StoredProcedure;


                ////ocCommand.Parameters.Add("m_user_id", OracleType.VarChar).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("m_user_id", OracleType.VarChar, 25).Value = sUSERNAME;

                //ocCommand.Parameters.Add("m_user_id", OracleDbType.Varchar2, sUSERNAME, ParameterDirection.Input);


                //ocCommand.Parameters.Add("cur_Return", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                //OracleDataAdapter odAdpt = new OracleDataAdapter(ocCommand);
                //odAdpt.Fill(dtType);

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



               /// AND SALES_INQM_BUDGET_APP_YN = 'Y'

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


        public DataSet FetchMasterData ( string Entry_No, int FinYr, string UserName )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = SQL + " SELECT  RM_SRQ_ENTRY_NO ENTRYNO, RM_SRQ_ENTRY_DATE,RM_STORE_REQUEST_MASTER.AD_FIN_FINYRID ,  ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.HR_DEPT_DEPT_CODE,HR_DEPT_DEPT_DESC,  ";
                SQL = SQL + "         RM_SRQ_VERIFIED, RM_SRQ_VERIFIED_BY, RM_SRQ_VERIFIED_DATE, ";
                SQL = SQL + "         RM_SRQ_TOTAL_AMOUNT, RM_SRQ_APPROVED, RM_SRQ_APPROVED_BY, RM_SRQ_APPROVED_DATE, ";
                SQL = SQL + "         RM_SRQ_REMARKS, RM_SRQ_CREATEDBY,RM_SRQ_CREATEDDATE,RM_SRQ_CANCELLED_YN,RM_SRQ_CANCELLED_REMARKS,  ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.GL_COSM_ACCOUNT_CODE,  ";
                SQL = SQL + "         GL_COSTING_MASTER.GL_COSM_ACCOUNT_NAME ,  ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.PC_SALES_INQM_ENQUIRY_NO, ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.AD_BR_CODE ,AD_BR_NAME,RM_SRQ_ISSUE_STATUS_OC    ";
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


        public DataSet FetchCostCenter ( string Code, string UserName )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " ";
                SQL = SQL + "SELECT MASTER.GL_COSM_ACCOUNT_CODE     CODE, ";
                SQL = SQL + "       MASTER.GL_COSM_ACCOUNT_NAME     NAME, ";
                SQL = SQL + "       MASTER.AD_BR_CODE, ";
                SQL = SQL + "       AD_BRANCH_MASTER.AD_BR_NAME, ";
                SQL = SQL + "       MASTER.SALES_STN_STATION_CODE, ";
                SQL = SQL + "       SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "       MASTER.HR_DEPT_DEPT_CODE, ";
                SQL = SQL + "       HR_DEPT_MASTER.HR_DEPT_DEPT_DESC,PC_SALES_INQM_ENQUIRY_NO ";
                SQL = SQL + "  FROM GL_COSTING_MASTER  MASTER, ";
                SQL = SQL + "       AD_BRANCH_MASTER, ";
                SQL = SQL + "       SL_STATION_MASTER, ";
                SQL = SQL + "       HR_DEPT_MASTER ";
                SQL = SQL + " WHERE    MASTER.GL_COSM_ACCOUNT_CODE = '" + Code + "' ";
                SQL = SQL + "       AND MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE(+) ";
                SQL = SQL + "       AND MASTER.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE(+) ";
                SQL = SQL + "       AND MASTER.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE(+) ";



                if (UserName != "ADMIN")
                {
                    SQL = SQL + " AND MASTER.AD_BR_CODE in ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
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

        public DataSet FetchPurchaseDetailData ( string Entry_No, int FinYr )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_SRQ_ENTRY_NO,RM_STORE_REQUEST_DETAILS.RM_RMM_RM_CODE RMCODE, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,RM_SRQD_SL_NO, ";
                SQL = SQL + " RM_STORE_REQUEST_DETAILS.RM_UM_UOM_CODE, ";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC, RM_SRQD_AVG_COST, RM_SRQD_AMOUNT, ";
                SQL = SQL + " RM_SRQD_QTY,  RM_SRQD_SL_NO,PC_BUD_BUDGET_ITEM_CODE,RM_SRQD_ISSUE_QTY_BAL, ";
                SQL = SQL + " RM_STORE_REQUEST_DETAILS.RM_SRQD_EXPECTED_DATE  ";
                SQL = SQL + " FROM RM_STORE_REQUEST_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER";
                SQL = SQL + " WHERE ";
                SQL = SQL + "  RM_STORE_REQUEST_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " AND RM_STORE_REQUEST_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE";
                SQL = SQL + " AND RM_STORE_REQUEST_DETAILS.AD_FIN_FINYRID = " + FinYr + "";
                SQL = SQL + " AND RM_STORE_REQUEST_DETAILS.RM_SRQ_ENTRY_NO = '" + Entry_No + "'";
                SQL = SQL + " ORDER BY RM_STORE_REQUEST_DETAILS.RM_SRQD_SL_NO ";

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
                    SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE NOT IN('" + sSelected.Replace(",", "','") + "')";
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

                SQL = " SELECT   SLNO, ENTRY_NO, ENTRY_DATE, FINID, ACCOUNT_CODE, ACCOUNT_NAME,  ";
                SQL = SQL + " STATION_CODE, STATION_NAME, DEPT_CODE, DEPT_DESC,REMARKS, MATAMOUNT, ";
                SQL = SQL + "  RM_SRQ_CREATEDBY, RM_SRQ_APPROVED_BY, RMCODE, RMDESC,  ";
                SQL = SQL + " UOM_CODE, UOM_DESC, AVG_COST,  QUANTITY, EXT_AMNT, exp_date, bal_qty,  ";
                SQL = SQL + " PC_SALES_INQM_ENQUIRY_NO,ad_br_code,  ";
                SQL = SQL + " MASTER_BRANCH_CODE,    MASTER_BRANCH_NAME,    MASTER_BRANCHDOC_PREFIX,  ";
                SQL = SQL + " MASTER_BRANCH_POBOX,    MASTER_BRANCH_ADDRESS,    MASTER_BRANCH_CITY,  ";
                SQL = SQL + "MASTER_BRANCH_PHONE,    MASTER_BRANCH_FAX,    MASTER_BRANCH_SPONSER_NAME,  ";
                SQL = SQL + "   MASTER_BRANCH_TRADE_LICENSE_NO,    MASTER_BRANCH_EMAIL_ID,    MASTER_BRANCH_WEB_SITE,  ";
                SQL = SQL + "   MASTER_BRANCH_VAT_REG_NUMBER  ";

                SQL = SQL + " FROM RM_STORE_REQUEST_VOUCHER_PRINT ";
                SQL = SQL + " WHERE ";
                SQL = SQL + " ENTRY_NO='" + voucherno + "' AND FINID=" + mngrclass.FinYearID + " ";
                SQL = SQL + " ORDER BY ENTRY_NO, SLNO ";

                dtMaster = clsSQLHelper.GetDataTableByCommand(SQL);
                dtMaster.TableName = "RM_STORE_REQUEST_DETAILS";
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

    //SalesRMLogic  MaterialRequestRMLogic
    public class MaterialRequestRMEntity
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

        public string EnquiryNo
        {
            get;
            set;
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
        public string CancelRmks
        {
            get;
            set;
        }

    }
    //MaterialRequestRMEntity  //MaterialRequestRMEntityDetails
    public class MaterialRequestRMEntityDetails
    {
        public string slNoDetails { get; set; }
        public string BudgetItemCodeDetails { get; set; }
        public string rmCodeDetails { get; set; }
        public string uomCodeDetails { get; set; }
        public double avgCostDetails { get; set; }
        public double qtyDetails { get; set; }
        public double AmountDetails { get; set; }
        public string ExpDateDetails { get; set; }
        public double BalQtyDetails { get; set; }
    }
}