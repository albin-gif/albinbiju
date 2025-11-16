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
using System.Globalization;
using System.Collections.Generic;
using Newtonsoft.Json;
//using AccosoftRML.Entity_Classes.RawMaterial; 

/// <summary>
/// Summary description for VendorMasterLogic
/// </summary>
/// 

//----------------------------------------------------------------

//CREATED BY : NEENU JOY

//CREATED ON : 06-04-2024

//CREATED PAGE DETAILS : VendorExpiryDocumentEntryLogic

//----------------------------------------------------------------

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class VendorExpiryDocumentEntryLogic
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());


        // OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public VendorExpiryDocumentEntryLogic( )
        {
            //
            // TODO: Add constructor logic here
            //
        }

        #region "API Data list Funtino jomy 18 mar 2022 8.06 am  " 
        public string FillVendorDataWithNoBalanceList ()
        {
            string JSONString = null;
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_VM_VENDOR_CODE CODE, RM_VM_VENDOR_NAME NAME, RM_VM_FULL_NAME FULL_NAME, RM_VM_VENDOR_NAME_IN_CHEQUE ,  ";
                SQL = SQL + " RM_VM_VENDOR_TYPE, GL_COAM_ACCOUNT_GROUP, RM_VM_VENDOR_STATUS, ";
                SQL = SQL + " RM_VM_POBOX, RM_VM_ADDRESS, RM_VM_CITY, ";
                SQL = SQL + " RM_VM_TEL_NO, RM_VM_FAXNO, RM_VM_EMAIL, ";
                SQL = SQL + " RM_VM_WEB_SITE, RM_VM_CONT_PERSON1, RM_VM_CONT_DESIG1, ";
                SQL = SQL + " RM_VM_CONT_PERSON2, RM_VM_CONT_DESIG2, RM_VM_BUSINESS_TYPE, ";
                SQL = SQL + " RM_VM_TRADE_LICENSE_NO, RM_VM_LICENSE_EXP_DATE, RM_VM_CREDIT_LIMIT, ";
                SQL = SQL + " RM_VM_CREDIT_PERIOD, RM_VM_CREDIT_TERMS, GL_BM_BNK1_CODE,RM_VM_BANK1_NAME, ";
                SQL = SQL + " RM_VM_BANK1_ACC_NO,RM_VM_IBAN1_CODE, ";
                SQL = SQL + "   RM_VM_IBAN1_SWIFT_CODE  SWIFT_CODE1,  RM_VM_IBAN1_BANK_ADDRESS  BANK_ADDRESS1, ";
                SQL = SQL + "     RM_VM_IBAN2_SWIFT_CODE  SWIFT_CODE2,  RM_VM_IBAN2_BANK_ADDRESS  BANK_ADDRESS2,";

                SQL = SQL + " GL_BM_BNK2_CODE,RM_VM_BANK2_NAME, RM_VM_BANK2_ACC_NO, RM_VM_IBAN2_CODE,";
                SQL = SQL + " RM_VM_PRICING_CATEGORY, RM_VM_PRICE_REMARKS, RM_VM_REMARKS, ";
                SQL = SQL + " RM_VENDOR_MASTER.AD_CM_COMPANY_CODE, RM_VM_CREATEDBY, RM_VM_CREATEDDATE, ";
                SQL = SQL + " RM_VM_UPDATEDBY, RM_VM_UPDATEDDATE, RM_VM_DELETESTATUS, ";
                SQL = SQL + " RM_VM_CONT_PERSON1_MOBNO, RM_VM_CONT_PERSON2_MOBNO, GL_COAM_ACCOUNT_CODE, ";
                SQL = SQL + " HR_DEPT_DEPT_CODE,RM_VM_INVOICE_QTY,RM_VM_EVAL_EXP_DATE,RM_EVAL_EXP_CAT_CODE,RM_VM_EVAL_EXP_REMARKS, ";
                SQL = SQL + " GL_TAX_TYPE_CODE ,RM_VM_TAX_VAT_REG_NUMBER  ,RM_VM_TAX_VAT_PERCENTAGE,SALES_EM_EMIRATE_CODE,";
                SQL = SQL + " AD_BR_CODE,RM_VM_VENDOR_CODE,RM_VENDOR_MASTER.RM_CO_ORIGIN_CODE,RM_CO_ORIGIN_DESC, ";
                SQL = SQL + " RM_VM_CONT_EMAIL1 , ";
                SQL = SQL + " RM_VM_CONT_EMAIL2 , RM_VM_TRN_DATE,RM_VM_VERIFIED_YN,RM_VM_VERIFIED_BY,RM_VM_VERIFIED_DATETIME,RM_VM_APPROVED_YN,RM_VM_APPROVED_BY,RM_VM_APPROVED_DATETIME ";
                SQL = SQL + " FROM RM_VENDOR_MASTER,GL_BANK_MASTER bank1, GL_BANK_MASTER bank2,RM_COUNTRY_ORIGIN_MASTER";
                SQL = SQL + " WHERE RM_VENDOR_MASTER.GL_BM_BNK1_CODE = bank1.GL_BM_BNK_CODE(+)";
                SQL = SQL + " AND RM_VENDOR_MASTER.GL_BM_BNK2_CODE = bank2.GL_BM_BNK_CODE(+)";
                SQL = SQL + " AND RM_VENDOR_MASTER.RM_CO_ORIGIN_CODE = RM_COUNTRY_ORIGIN_MASTER.RM_CO_ORIGIN_CODE(+)";
                SQL = SQL + " ORDER BY  RM_VENDOR_MASTER.RM_VM_VENDOR_CODE DESC";


                dtType = clsSQLHelper.GetDataTableByCommand(SQL);
                dtType.TableName = "VENDOR_MASTER";

                JSONString = JsonConvert.SerializeObject(dtType, Formatting.None);
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
        #endregion

        #region "Filldata"

         
        public DataSet fillCombo ( )
        {
            DataSet dsData = new DataSet(); 
            DataTable dtEval = new DataTable(); 
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();

                 

                SQL = " SELECT RM_EVAL_EXP_CAT_CODE  CODE, RM_EVAL_EXP_CAT_NAME NAME";
                SQL = SQL + "   FROM RM_EVAL_EXP_CATEGORY_VW  ";

                dtEval = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtEval);
                   

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

        public DataTable FetchBank ( )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "Select GL_BM_BNK_CODE  GL_BM_BNK_CODE  ,GL_BM_BNK_NAME , ";
                SQL = SQL + " GL_BM_BANK_FULL_NAME,GL_BM_AGENT_ID ,GL_BM_BRANCH,GL_BM_SWIFT_CODE, GL_BM_BANK_ADDRESS";
                SQL = SQL + "  From GL_BANK_MASTER  ";
                SQL = SQL + "  Order By  GL_BM_BNK_CODE asc ";

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

        #region "DML "
             
        public string FetchData ( string sCode )
        {
            string JSONString = string.Empty ;
            DataTable dtReturn = new DataTable();
            DataSet dsReturn = new DataSet();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the company type master recrods 
                SQL = " SELECT  RM_VM_VENDOR_NAME, RM_VM_FULL_NAME, RM_VM_VENDOR_NAME_IN_CHEQUE ,  ";
                SQL = SQL + " RM_VM_VENDOR_TYPE, GL_COAM_ACCOUNT_GROUP, RM_VM_VENDOR_STATUS, ";
                SQL = SQL + " RM_VM_POBOX, RM_VM_ADDRESS, RM_VM_CITY, ";
                SQL = SQL + " RM_VM_TEL_NO, RM_VM_FAXNO, RM_VM_EMAIL, ";
                SQL = SQL + " RM_VM_WEB_SITE, RM_VM_CONT_PERSON1, RM_VM_CONT_DESIG1, ";
                SQL = SQL + " RM_VM_CONT_PERSON2, RM_VM_CONT_DESIG2, RM_VM_BUSINESS_TYPE, ";
                SQL = SQL + " RM_VM_TRADE_LICENSE_NO, RM_VM_LICENSE_EXP_DATE, RM_VM_CREDIT_LIMIT, ";
                SQL = SQL + " RM_VM_CREDIT_PERIOD, RM_VM_CREDIT_TERMS, GL_BM_BNK1_CODE,RM_VM_BANK1_NAME, ";
                SQL = SQL + " RM_VM_BANK1_ACC_NO,RM_VM_IBAN1_CODE, ";
                SQL = SQL + " RM_VM_IBAN1_SWIFT_CODE  SWIFT_CODE1,  RM_VM_IBAN1_BANK_ADDRESS  BANK_ADDRESS1, ";
                SQL = SQL + " RM_VM_IBAN2_SWIFT_CODE  SWIFT_CODE2,  RM_VM_IBAN2_BANK_ADDRESS  BANK_ADDRESS2,";
                SQL = SQL + " GL_BM_BNK2_CODE,RM_VM_BANK2_NAME, RM_VM_BANK2_ACC_NO, RM_VM_IBAN2_CODE,";
                SQL = SQL + " RM_VM_PRICING_CATEGORY, RM_VM_PRICE_REMARKS, RM_VM_REMARKS, ";
                SQL = SQL + " RM_VENDOR_MASTER.AD_CM_COMPANY_CODE, RM_VM_CREATEDBY, RM_VM_CREATEDDATE, ";
                SQL = SQL + " RM_VM_UPDATEDBY, RM_VM_UPDATEDDATE, RM_VM_DELETESTATUS, ";
                SQL = SQL + " RM_VM_CONT_PERSON1_MOBNO, RM_VM_CONT_PERSON2_MOBNO, GL_COAM_ACCOUNT_CODE, ";
                SQL = SQL + " HR_DEPT_DEPT_CODE,RM_VM_INVOICE_QTY,RM_VM_EVAL_EXP_DATE,RM_EVAL_EXP_CAT_CODE,RM_VM_EVAL_EXP_REMARKS, ";
                SQL = SQL + " GL_TAX_TYPE_CODE ,RM_VM_TAX_VAT_REG_NUMBER  ,RM_VM_TAX_VAT_PERCENTAGE,SALES_EM_EMIRATE_CODE,";
                SQL = SQL + " AD_BR_CODE,RM_VM_VENDOR_CODE,RM_VENDOR_MASTER.RM_CO_ORIGIN_CODE,RM_CO_ORIGIN_DESC, ";
                SQL = SQL + " RM_VM_CONT_EMAIL1 ,RM_VM_CONT_EMAIL2 , RM_VM_TRN_DATE,RM_VM_VERIFIED_YN, ";
                SQL = SQL + " RM_VM_VERIFIED_BY,RM_VM_VERIFIED_DATETIME,RM_VM_APPROVED_YN,RM_VM_APPROVED_BY,RM_VM_APPROVED_DATETIME ";
                SQL = SQL + " FROM RM_VENDOR_MASTER,GL_BANK_MASTER bank1, GL_BANK_MASTER bank2,RM_COUNTRY_ORIGIN_MASTER";
                SQL = SQL + " WHERE RM_VENDOR_MASTER.GL_BM_BNK1_CODE = bank1.GL_BM_BNK_CODE(+)";
                SQL = SQL + " AND RM_VENDOR_MASTER.GL_BM_BNK2_CODE = bank2.GL_BM_BNK_CODE(+)";
                SQL = SQL + " AND RM_VENDOR_MASTER.RM_CO_ORIGIN_CODE = RM_COUNTRY_ORIGIN_MASTER.RM_CO_ORIGIN_CODE(+)";
                SQL = SQL + " AND RM_VM_VENDOR_CODE ='" + sCode + "'";

                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);
                dtReturn.TableName = "VENDOR_MASTER";
                dsReturn.Tables.Add(dtReturn);
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

        public string UpdateSQL(VendorMasterExpiryEntity oVMEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();


                SQL = "UPDATE RM_VENDOR_MASTER SET ";
                 
                SQL = SQL + " RM_VM_BUSINESS_TYPE ='" + oVMEntity.BusinessType + "' ,";
                SQL = SQL + " RM_VM_TRADE_LICENSE_NO='" + oVMEntity.LicenseNo + "' ,";
                SQL = SQL + " RM_VM_LICENSE_EXP_DATE ='" + oVMEntity.ExpriyDate.ToString("dd-MMM-yyyy") + "',";
                if (oVMEntity.TRNDate != DateTime.MinValue)
                {
                    SQL = SQL + " RM_VM_TRN_DATE ='" + oVMEntity.TRNDate.ToString("dd-MMM-yyyy") + "',";
                }
                SQL = SQL + " RM_VM_EVAL_EXP_DATE = '" + oVMEntity.EvalExpDate.ToString("dd-MMM-yyyy") + "',";
                SQL = SQL + " RM_EVAL_EXP_CAT_CODE ='" + oVMEntity.EvalCateCode + "',"; 
                SQL = SQL + "  RM_VM_TAX_VAT_REG_NUMBER='" + oVMEntity.VATRegistrationNumber + "' "; 
                SQL = SQL + " WHERE RM_VM_VENDOR_CODE ='" + oVMEntity.VendorCode + "'";

                sSQLArray.Add(SQL);                

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMVMDOCEXP", oVMEntity.VendorCode, false, Environment.MachineName, "U", sSQLArray);

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
            return sRetun;

        }

        #endregion 

        #region  "Doc Attach funtions - Insert - Fill - delete Path "

        public void InsertAttachFile ( string lblCVPath, string AttachRemarks, string txtEmployeeCode )
        {
            DataSet sReturn = new DataSet();
            Int32 SLNO = 0;

            try
            {
                SQL = string.Empty;
                OracleHelper oTrns = new OracleHelper();
                SqlHelper clsSQLHelper = new SqlHelper();
                //SessionManager mngrclass = (SessionManager)mngrclassobj;
                //DataTable dtCnt = new DataTable();
                sSQLArray.Clear();


                SQL = "select max(RM_VM_VENDOR_SLNO  ) slno From RM_VENDOR_MASTER_ATTACH_DTLS where RM_VM_VENDOR_CODE  ='" + txtEmployeeCode + "'";

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

                SQL = " INSERT INTO RM_VENDOR_MASTER_ATTACH_DTLS";

                SQL = SQL + " (RM_VM_VENDOR_CODE  , RM_VM_VENDOR_SLNO  , RM_VM_VENDOR_REMARKS    , RM_VM_VENDOR_FILE_PATH  )";

                SQL = SQL + " Values('" + txtEmployeeCode + "'," + SLNO + ",'" + AttachRemarks + "','" + lblCVPath + "')";

                oTrns.GetExecuteNonQueryBySQL(SQL);


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return;
        }

        public DataSet fillAttachGrid ( string txtEmployeeCode )
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " select RM_VM_VENDOR_CODE  , RM_VM_VENDOR_REMARKS    , RM_VM_VENDOR_FILE_PATH  ";

                SQL = SQL + " from RM_VENDOR_MASTER_ATTACH_DTLS ";

                SQL = SQL + " where RM_VM_VENDOR_CODE  ='" + txtEmployeeCode + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public void DeletePath ( string txtEmployeeCode, string Path )
        {
            try
            {
                SQL = string.Empty;
                OracleHelper oTrns = new OracleHelper();
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " delete from RM_VENDOR_MASTER_ATTACH_DTLS";
                SQL = SQL + " where RM_VM_VENDOR_CODE  ='" + txtEmployeeCode + "' AND RM_VM_VENDOR_FILE_PATH  ='" + Path + "'";

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
         
    }

    #region "Entity"

    public class VendorMasterExpiryEntity
    {
        public VendorMasterExpiryEntity ( )
        {
            //
            // TODO: Add constructor logic here
            //
        }        
        public string VendorCode
        {
            get;
            set;
        }   
        public string BusinessType
        {
            get;
            set;
        }

        private string sTrLisNo = string.Empty;
        public string LicenseNo
        {
            get;
            set;
        }

       
        private string sBankCode1 = string.Empty;
        public string BankCode1
        {
            get;
            set;
        }

        private string sBankName1 = string.Empty;
        public string BankName1
        {
            get;
            set;
        }

        private string sBankAccountNo1 = string.Empty;
        public string BankAccountNo1
        {
            get;
            set;
        }

        private string sIBANNo1 = string.Empty;
        public string IBANNo1
        {
            get;
            set;
        }

        private string sSwiftCode1 = string.Empty;
        public string SwiftCode1
        {
            get;
            set;
        }

        private string sBankAddress1 = string.Empty;
        public string BankAddress1
        {
            get;
            set;
        }

        private string sBankCode2 = string.Empty;
        public string BankCode2
        {
            get;
            set;
        }

        private string sBankName2 = string.Empty;
        public string BankName2
        {
            get;
            set;
        }

        private string sBankAccountNo2 = string.Empty;
        public string BankAccountNo2
        {
            get;
            set;
        }

        private string sIBANNo2 = string.Empty;
        public string IBANNo2
        {
            get;
            set;
        }

        private string sSwiftCode2 = string.Empty;
        public string SwiftCode2
        {
            get;
            set;
        }

        private string sBankAddress2 = string.Empty;
        public string BankAddress2
        {
            get;
            set;
        }

        private string sRemarks = string.Empty;
        public string Remarks
        {
            get;
            set;
        }

        public DateTime TRNDate
        {
            get;
            set;
        }
        private DateTime dExpDate = DateTime.Now;
        public DateTime ExpriyDate
        {
            get;
            set;
        }
        public DateTime EvalExpDate
        {
            get;
            set;
        }
        public string EvalCateCode
        {
            get;
            set;

        }
        public string VATRegistrationNumber
        {
            get;
            set;
        }
    }

     
    #endregion
      

}