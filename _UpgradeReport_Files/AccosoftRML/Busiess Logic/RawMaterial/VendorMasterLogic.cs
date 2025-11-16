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

//UPDATED BY : JISHNU

//UPDATED ON : 22-03-2019

//UPDATED PAGE DETAILS : VENDOR MASTER LOGIC

//----------------------------------------------------------------

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class VendorMasterLogic
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());


        // OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;
        string TempSchema = "ACCDELLOG";

        public VendorMasterLogic()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        #region "API Data list Funtino jomy 18 mar 2022 8.06 am  " 

        public string FillVendorDataWithBalanceList(VendorMasterSearchModel omdl, string UserName, int FinId)
        {
            string JSONString = null;
            DataTable dtType = new DataTable();
            CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
            try
            {
                string sRetun = string.Empty;

                OracleCommand ocCommand = new OracleCommand("pk_cust_supply_lookup.supplyer_bal_lookup");
                ocCommand.Connection = ocConn;
                ocCommand.CommandType = CommandType.StoredProcedure;

                ocCommand.Parameters.Add("finyr", OracleDbType.Decimal, FinId, ParameterDirection.Input);
                ocCommand.Parameters.Add("asondate", OracleDbType.Date, System.Convert.ToDateTime(omdl.todate, culture), ParameterDirection.Input);
                ocCommand.Parameters.Add("cur_return", OracleDbType.RefCursor).Direction = ParameterDirection.Output;


                OracleDataAdapter odAdpt = new OracleDataAdapter(ocCommand);

                odAdpt.Fill(dtType);

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


        public string FillVendorDataWithNoBalanceList(VendorMasterSearchModel omdl, string UserName)
        {
            string JSONString = null;
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  rm_vendor_master.rm_vm_vendor_code Code, rm_vm_vendor_name Name,";
                SQL = SQL + " rm_vm_vendor_status status, ";
                SQL = SQL + " RM_VENDOR_TYPE_MASTER.RM_VM_VENDOR_TYPE_NAME rm_vm_vendor_type, rm_vm_tel_no tel_no, ";
                SQL = SQL + "  RM_VM_ADDRESS , RM_VM_LOCMAP , RM_VM_CONT_PERSON1 , RM_VM_CONT_DESIG1 ,  RM_VM_CONT_PERSON1_MOBNO ,  RM_VM_TAX_VAT_REG_NUMBER VAT_REG_NUMBER , ";

                SQL = SQL + " sales_pay_type_desc, ";
                SQL = SQL + " TO_CHAR (rm_vm_credit_limit) credit_limit ,";
                SQL = SQL + " DECODE(ATTACHDTLS.CNT, '', 'N', 'Y') DocAttachYN ";
                SQL = SQL + " FROM rm_vendor_master,sl_pay_type_master ,RM_VENDOR_TYPE_MASTER ,";
                SQL = SQL + " (SELECT RM_VM_VENDOR_CODE , COUNT(RM_VM_VENDOR_CODE) CNT ";
                SQL = SQL + " FROM RM_VENDOR_MASTER_ATTACH_DTLS ";
                SQL = SQL + " GROUP BY RM_VM_VENDOR_CODE )ATTACHDTLS ";
                SQL = SQL + "  where rm_vendor_master.rm_vm_credit_terms = sl_pay_type_master.sales_pay_type_code(+)";
                SQL = SQL + " AND rm_vendor_master.rm_vm_vendor_type=RM_VENDOR_TYPE_MASTER.RM_VM_VENDOR_TYPE(+)";
                SQL = SQL + " AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE =ATTACHDTLS.RM_VM_VENDOR_CODE(+) ";
                SQL = SQL + " order by  RM_VENDOR_MASTER.rm_vm_vendor_code desc";


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


        public string FillVendorDataWithTransactionList(VendorMasterSearchModel omdl, string UserName, int FinYearID)
        {   //jomy 05 apr 2022 
            string JSONString = null;
            DataTable dtVendorMast = new DataTable();
            DataSet dsReturn = new DataSet("VENDOR_DATA");
            DataTable dtLPOData = new DataTable();
            DataTable dtPEData = new DataTable();
            DataTable dtStatement = new DataTable();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                ////SQL = " ";
                ////SQL = SQL + "select  AD_FIN_FROM_DATE , AD_FIN_TO_DATE   from AD_FINANCIAL_YEAR  where  '" + System.Convert.ToDateTime(omdl.todate).ToString("dd-MMM-yyyy") + "'  between  AD_FIN_FROM_DATE and AD_FIN_TO_DATE  ";
                ////DataTable dtGetfinyeaDates = new DataTable();

                ////dtGetfinyeaDates = clsSQLHelper.GetDataTableByCommand(SQL);
                ////if ( dtGetfinyeaDates.Rows.Count >0 )
                ////{  // jomy added the po and pe entry should be teh current financial Year 
                ////    omdl.fromdate = dtGetfinyeaDates.Rows[0]["AD_FIN_FROM_DATE"].ToString();
                ////        omdl.todate = dtGetfinyeaDates.Rows[0]["AD_FIN_TO_DATE"].ToString();
                ////}

                SQL = " SELECT  rm_vendor_master.rm_vm_vendor_code  , rm_vm_vendor_name Name,";
                SQL = SQL + " rm_vm_vendor_status status, ";
                SQL = SQL + " RM_VENDOR_TYPE_MASTER.RM_VM_VENDOR_TYPE_NAME rm_vm_vendor_type, rm_vm_tel_no tel_no, ";
                SQL = SQL + "  RM_VM_ADDRESS , RM_VM_LOCMAP , RM_VM_CONT_PERSON1 , RM_VM_CONT_DESIG1 ,  RM_VM_CONT_PERSON1_MOBNO ,  RM_VM_TAX_VAT_REG_NUMBER VAT_REG_NUMBER , ";

                SQL = SQL + " sales_pay_type_desc, ";
                SQL = SQL + " TO_CHAR (rm_vm_credit_limit) credit_limit ,";
                SQL = SQL + " DECODE(ATTACHDTLS.CNT, '', 'N', 'Y') DocAttachYN ";
                SQL = SQL + " FROM rm_vendor_master,sl_pay_type_master ,RM_VENDOR_TYPE_MASTER ,";
                SQL = SQL + " (SELECT RM_VM_VENDOR_CODE , COUNT(RM_VM_VENDOR_CODE) CNT ";
                SQL = SQL + " FROM RM_VENDOR_MASTER_ATTACH_DTLS ";
                SQL = SQL + " GROUP BY RM_VM_VENDOR_CODE )ATTACHDTLS ";
                SQL = SQL + "  where rm_vendor_master.rm_vm_credit_terms = sl_pay_type_master.sales_pay_type_code(+)";
                SQL = SQL + " AND rm_vendor_master.rm_vm_vendor_type=RM_VENDOR_TYPE_MASTER.RM_VM_VENDOR_TYPE(+)";
                SQL = SQL + " AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE =ATTACHDTLS.RM_VM_VENDOR_CODE(+) ";
                SQL = SQL + " order by  RM_VENDOR_MASTER.rm_vm_vendor_code desc";


                dtVendorMast = clsSQLHelper.GetDataTableByCommand(SQL);
                dsReturn.Tables.Add(dtVendorMast);
                dsReturn.Tables[0].TableName = "VENDOR_MASTER";


                GeneralRmLogic objwsgen = new GeneralRmLogic();
                double dGrantedProjectCount = objwsgen.GetGrantedProjectCount(UserName);

                SQL = " SELECT ";
                SQL = SQL + "    WS_PO_MASTER.WS_PO_PONO pcode,  AD_FIN_FINYRID  Id,     ";
                SQL = SQL + "    WS_PO_MASTER.WS_PO_PO_DATE PDate,   ";
                SQL = SQL + "    WS_PO_MASTER.RM_VM_VENDOR_CODE  ,   ";
                SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SName,   ";
                SQL = SQL + "    WS_PO_MASTER.WS_PO_GRAND_TOTAL GRANDTOTAL,   ";
                SQL = SQL + "    to_char(WS_PO_MASTER.WS_PO_NET_AMOUNT ) NETTOTAL, ";
                SQL = SQL + "    WS_PO_MASTER.WS_PO_APPROVED, WS_PO_APPROVEDBY , WS_PO_MASTER.WS_PO_VERIFIED,  WS_PO_VERIFIEDBY ,   ";
                SQL = SQL + "    WS_PO_MASTER.WS_PO_PO_STATUS STATUS,WS_PO_MASTER.WS_PO_CANCEL_REMARKS,   ";
                SQL = SQL + "    WS_PR_PRNO_CONCATENATE_NO  PRNO, WS_PO_SUPP_REF SUPPREF, WS_PO_OUR_REF OURREF,   ";
                SQL = SQL + "    WS_PO_MASTER.GL_COSM_ACCOUNT_CODE PRCODE,GL_COSTING_MASTER_VW.NAME PRNAME,   ";
                SQL = SQL + "    WS_PO_MASTER.WS_PO_PAYMENT_TERMS PayTerms, WS_PO_MASTER.WS_PO_EXP_DATE DDate ,  ";
                SQL = SQL + "    WS_PO_MASTER.WS_PO_REQUEST_TYPE_CODE,WS_PO_REQUEST_TYPE_NAME PO_TYPE,  ";
                SQL = SQL + "    WS_PO_MASTER.SALES_INQM_ENQUIRY_NO  ERCProject,  ";
                SQL = SQL + "    WS_PO_MASTER.WS_PO_REMARKS,AD_BRANCH_MASTER.AD_BR_NAME ,  ";
                SQL = SQL + "    101 SortID,  'WTPO' AD_MI_ITEMID , '../Presentation/WorkshopInventory/PurchaseOrderWS.aspx'  AS NAVIGATEURL       ";
                SQL = SQL + "FROM  ";
                SQL = SQL + "    RM_VENDOR_MASTER,WS_PO_MASTER,GL_COSTING_MASTER_VW ,WS_PO_REQUEST_TYPE_VW,AD_BRANCH_MASTER   ";
                SQL = SQL + "WHERE   ";
                SQL = SQL + "    WS_PO_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE   ";
                SQL = SQL + "    AND WS_PO_MASTER.GL_COSM_ACCOUNT_CODE=GL_COSTING_MASTER_VW.CODE (+)   ";
                SQL = SQL + "    AND WS_PO_MASTER.WS_PO_REQUEST_TYPE_CODE=WS_PO_REQUEST_TYPE_VW.WS_PO_REQUEST_TYPE_CODE(+)  ";
                SQL = SQL + "    AND WS_PO_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE(+)   ";



                SQL = SQL + " AND WS_PO_MASTER.WS_PO_PO_DATE  BETWEEN '" + System.Convert.ToDateTime(omdl.fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(omdl.todate).ToString("dd-MMM-yyyy") + "'";
                //SQL = SQL + " AND WS_PO_MASTER.AD_BR_CODE = '" + omdl.DivCode + "' ";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + "  AND  WS_PO_MASTER.AD_BR_CODE  in (SELECT AD_BR_CODE FROM AD_USER_GRANTED_BRANCH WHERE  AD_UM_USERID = '" + UserName + "') ";
                }

                // CONTROL ACCES BY PROJECT WISE // JOMY 

                if (dGrantedProjectCount > 0)
                {
                    SQL = SQL + "     and  WS_PO_MASTER.GL_COSM_ACCOUNT_CODE in ( ";
                    SQL = SQL + " SELECT SALES_PM_PROJECT_CODE  FROM SL_GRANTED_PROJECT_USERS ";
                    SQL = SQL + " WHERE AD_UM_USERID ='" + UserName + "')";

                }

                //------------------rawmteiral 
                SQL = SQL + "    UNION ALL  ";
                SQL = SQL + " SELECT  ";
                SQL = SQL + "     RM_PO_MASTER.RM_PO_PONO   pcode, RM_PO_MASTER.AD_FIN_FINYRID Id,     ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_PO_DATE PDate,   ";
                SQL = SQL + "    RM_PO_MASTER.RM_VM_VENDOR_CODE  ,   ";
                SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SName,   ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_GRAND_TOTAL GRANDTOTAL,   ";
                SQL = SQL + "    to_char(RM_PO_MASTER.RM_PO_NET_AMOUNT ) NETTOTAL, ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_APPROVED, RM_PO_APPROVEDBY , RM_PO_MASTER.RM_PO_VERIFIED,  RM_PO_VERIFIEDBY ,   ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_PO_STATUS STATUS,RM_PO_MASTER.RM_PO_CANCEL_REMARKS,   ";
                SQL = SQL + "    RM_PR_PRNO  PRNO, RM_PO_SUPP_REF  SUPPREF, RM_PO_OUR_REF OURREF,   ";
                SQL = SQL + "    '' PRCODE,  RM_PO_COST_CENTER  PRNAME,   ";
                SQL = SQL + "   RM_MPOM_PAYMENT_TERMS  PayTerms, RM_PO_EXP_DATE DDate ,  ";
                SQL = SQL + "    '' WS_PO_REQUEST_TYPE_CODE,''  PO_TYPE,  ";
                SQL = SQL + "    ''  ERCProject,  ";
                SQL = SQL + "    RM_PO_REMARKS,AD_BRANCH_MASTER.AD_BR_NAME  , ";
                SQL = SQL + "     201 SortID,   'RTMO' AD_MI_ITEMID  , '../Presentation/RawMaterial/PurchaseOrderRMPRC.aspx'  AS NAVIGATEURL  ";
                SQL = SQL + " FROM  ";
                SQL = SQL + "        RM_VENDOR_MASTER,RM_PO_MASTER  ,AD_BRANCH_MASTER  ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + "        RM_PO_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "        AND RM_PO_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE(+)   ";

                //SQL = SQL + " WHERE RM_PO_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + "  AND RM_PO_MASTER.RM_PO_PO_DATE  ";
                SQL = SQL + " BETWEEN '" + System.Convert.ToDateTime(omdl.fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(omdl.todate).ToString("dd-MMM-yyyy") + "'";


                if (UserName != "ADMIN")
                {
                    SQL = SQL + "  and     RM_PO_MASTER.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                }


                SQL = SQL + " order by   pcode   desc";
                DataTable dtPOMaster = new DataTable();
                dtPOMaster = clsSQLHelper.GetDataTableByCommand(SQL);

                dsReturn.Tables.Add(dtPOMaster);
                dsReturn.Tables[1].TableName = "PO_MASTER";



                SQL = " SELECT  ";
                SQL = SQL + "    WS_PE_MASTER.WS_PE_PENO ENTRYNO, WS_PE_MASTER.AD_FIN_FINYRID,  ";
                SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE , RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIER,  ";
                SQL = SQL + "    WS_PE_INVOICENO INVOICENO, WS_PE_PEDATE   PEDATE ,  ";
                SQL = SQL + "    WS_PE_PETYPE PETYPE, WS_PE_GRAND_TOTAL GRDNDTOAL,  ";
                SQL = SQL + "    WS_PE_DISCOUNT_AMOUNT DISCAMOUNT,  WS_PE_NET_AMOUNT  NETAMOUNT ,  ";
                SQL = SQL + "    WS_PE_INVOICE_AMOUNT INVOICEAMOUNT,  ";
                SQL = SQL + "    WS_PE_APPROVED APPROVED,WS_PE_APPROVEDBY APPROVED_BY,  ";
                SQL = SQL + "    WS_PE_MASTER.AD_BR_CODE,AD_BRANCH_MASTER.AD_BR_NAME,  ";
                SQL = SQL + "    WS_PE_MASTER.SALES_INQM_ENQUIRY_NO, NAME SALES_INQM_ENQUIRY_NAME  , ";
                SQL = SQL + "     101 SortID,   'WTPE' AD_MI_ITEMID  , '../Presentation/WorkshopInventory/PurchaseEntryWS.aspx'  AS NAVIGATEURL  ";
                SQL = SQL + " FROM ";
                SQL = SQL + "    WS_PE_MASTER, RM_VENDOR_MASTER,AD_BRANCH_MASTER,GL_COSTING_MASTER_VW  ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + "     WS_PE_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                SQL = SQL + "     AND WS_PE_MASTER.AD_BR_CODE=AD_BRANCH_MASTER.AD_BR_CODE  ";
                SQL = SQL + "     AND WS_PE_MASTER.SALES_INQM_ENQUIRY_NO=GL_COSTING_MASTER_VW.CODE  ";

                SQL = SQL + "   AND WS_PE_MASTER.WS_PE_PEDATE ";
                SQL = SQL + " BETWEEN '" + System.Convert.ToDateTime(omdl.fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(omdl.todate).ToString("dd-MMM-yyyy") + "'";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + "    and  WS_PE_MASTER.AD_BR_CODE in (SELECT AD_BR_CODE FROM AD_USER_GRANTED_BRANCH WHERE  AD_UM_USERID = '" + UserName + "')  ";

                }


                SQL = SQL + "   UNION ALL  ";

                SQL = SQL + "      SELECT  ";
                SQL = SQL + "        RM_PE_MASTER.RM_MPEM_ENTRY_NO  ENTRYNO, RM_PE_MASTER.AD_FIN_FINYRID,  ";
                SQL = SQL + "       RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ,   RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIER,  ";
                SQL = SQL + "        RM_MPEM_VENDOR_INV_NO INVOICENO, RM_PE_MASTER.RM_MPEM_ENTRY_DATE   PEDATE ,  ";
                SQL = SQL + "        'RM PURCHASE' PETYPE, RM_MPEM_GRAND_TOTAL GRDNDTOAL,  ";
                SQL = SQL + "        RM_MPEM_DISC_AMT DISCAMOUNT,  RM_MPEM_NET_TOTAL  NETAMOUNT ,  ";
                SQL = SQL + "        RM_MPEM_INVOICE_TOTAL INVOICEAMOUNT,  ";
                SQL = SQL + "        RM_MPEM_APPROVED APPROVED,RM_MPEM_APPROVED_BY APPROVED_BY,  ";
                SQL = SQL + "        RM_PE_MASTER.AD_BR_CODE,AD_BRANCH_MASTER.AD_BR_NAME,  ";
                SQL = SQL + "        '' SALES_INQM_ENQUIRY_NO, ''  SALES_INQM_ENQUIRY_NAME ,  ";
                SQL = SQL + "     201 SortID,   'RTMPE' AD_MI_ITEMID  , '../Presentation/RawMaterial/PurchaseEtnryRM.aspx'  AS NAVIGATEURL  ";
                SQL = SQL + "    FROM ";
                SQL = SQL + "    RM_PE_MASTER, RM_VENDOR_MASTER,AD_BRANCH_MASTER  ";
                SQL = SQL + "    WHERE ";
                SQL = SQL + "    RM_PE_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "    AND RM_PE_MASTER.AD_BR_CODE=AD_BRANCH_MASTER.AD_BR_CODE  ";
                SQL = SQL + "    AND RM_PE_MASTER.RM_MPEM_ENTRY_DATE ";
                SQL = SQL + " BETWEEN '" + System.Convert.ToDateTime(omdl.fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(omdl.todate).ToString("dd-MMM-yyyy") + "'";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + "   and   RM_PE_MASTER.AD_BR_CODE  IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                }


                SQL = SQL + "   UNION ALL  ";
                SQL = SQL + "SELECT   ";
                SQL = SQL + "    GL_MISC_PURCHASE_MASTER.GL_MPEM_ENTRY_NO    ENTRYNO, GL_MISC_PURCHASE_MASTER.AD_FIN_FINYRID,  ";
                SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ,  RM_VM_VENDOR_NAME  SUPPLIER,  ";
                SQL = SQL + "    GL_MPEM_VEN_INV_NO  INVOICENO, GL_MPEM_ENTRY_DATE   PEDATE ,  ";
                SQL = SQL + "    'GL PURCHASE' PETYPE, GL_MPEM_GRAND_TOTAL GRDNDTOAL,  ";
                SQL = SQL + "    GL_MPEM_DISC_AMT DISCAMOUNT,  GL_MPEM_NET_TOTAL  NETAMOUNT ,  ";
                SQL = SQL + "    GL_MPEM_NET_TOTAL INVOICEAMOUNT,  ";
                SQL = SQL + "    GL_POSTED APPROVED,GL_POSTEDBY APPROVED_BY,  ";
                SQL = SQL + "    GL_MISC_PURCHASE_MASTER.AD_BR_CODE,AD_BRANCH_MASTER.AD_BR_NAME,  ";
                SQL = SQL + "    GL_MISC_PURCHASE_MASTER.SALES_INQM_ENQUIRY_NO,  NAME  SALES_INQM_ENQUIRY_NAME ,  ";
                SQL = SQL + "     301 SortID,  'GLMISCPUR'  AD_MI_ITEMID,  '../Presentation/Accounts/MiscPurchase.aspx'  AS NAVIGATEURL  ";
                SQL = SQL + " FROM  ";
                SQL = SQL + " GL_MISC_PURCHASE_MASTER,  GL_COSTING_MASTER_VW,RM_VENDOR_MASTER    ,AD_BRANCH_MASTER ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + " GL_MISC_PURCHASE_MASTER.GL_MPEM_CR_ACC_CODE = RM_VENDOR_MASTER.GL_COAM_ACCOUNT_CODE ";
                SQL = SQL + " AND GL_MISC_PURCHASE_MASTER.SALES_INQM_ENQUIRY_NO = GL_COSTING_MASTER_VW.CODE(+) ";
                SQL = SQL + " AND GL_MISC_PURCHASE_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE(+)   ";
                SQL = SQL + "    AND GL_MISC_PURCHASE_MASTER.GL_MPEM_ENTRY_DATE ";
                SQL = SQL + " BETWEEN '" + System.Convert.ToDateTime(omdl.fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(omdl.todate).ToString("dd-MMM-yyyy") + "'";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + "   and   GL_MISC_PURCHASE_MASTER.AD_BR_CODE  IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + UserName + "' )";
                }

                SQL = SQL + " ORDER BY  SortID , ENTRYNO ASC ";


                DataTable dtPEMaster = new DataTable();
                dtPEMaster = clsSQLHelper.GetDataTableByCommand(SQL);

                dsReturn.Tables.Add(dtPEMaster);
                dsReturn.Tables[2].TableName = "PE_MASTER";




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


        #endregion

        #region "Filldata"


        public DataTable fillCheckList()
        {
            DataSet dsReturn = new DataSet();

            DataTable dtdata = new DataTable();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  RM_CHK_CHECK_LIST_CODE CODE , RM_CHK_CHKLIST_DESC NAME, ";
                SQL = SQL + " RM_CHK_REMARKS ";
                SQL = SQL + " FROM RM_CHECK_LIST_VENDOR_MASTER ";
                SQL = SQL + " ORDER BY RM_CHK_CHECK_LIST_CODE ASC ";

                dtdata = clsSQLHelper.GetDataTableByCommand(SQL);


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
            return dtdata;

        }

        public DataSet fillCombo()
        {
            DataSet dsData = new DataSet();
            DataTable dtAdd = new DataTable();
            DataTable dtEval = new DataTable();
            DataTable dtEmirates = new DataTable();
            DataTable dtBranch = new DataTable();
            DataTable dtSupplier = new DataTable();
            DataTable dtVendorType = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT GL_COAM_ACCOUNT_CODE  CODE, GL_COAM_ACCOUNT_NAME NAME";
                SQL = SQL + "   FROM GL_COA_MASTER  WHERE  GL_COAM_LEDGER_TYPE = 'MAIN'";
                SQL = SQL + "    AND GL_COAM_ACCOUNT_STATUS = 'ACTIVE'";
                SQL = SQL + "    AND GL_COAM_ACC_TYPE_CODE = '20'";
                SQL = SQL + "    AND GL_COAM_CUST_SUPP_SHOW_GRP_YN = 'Y'";
                SQL = SQL + "  order by GL_COAM_ACCOUNT_CODE asc ";

                dtAdd = clsSQLHelper.GetDataTableByCommand(SQL);

                dsData.Tables.Add(dtAdd);

                SQL = " SELECT RM_EVAL_EXP_CAT_CODE  CODE, RM_EVAL_EXP_CAT_NAME NAME";
                SQL = SQL + "   FROM RM_EVAL_EXP_CATEGORY_VW  ";

                dtEval = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtEval);

                SQL = "SELECT SALES_EM_EMIRATE_CODE  CODE , SALES_EM_EMIRATE_NAME  NAME FROM SL_EMIRATES_MASTER";
                dtEmirates = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtEmirates);

                SQL = "SELECT AD_BR_CODE CODE,AD_BR_NAME NAME FROM  AD_BRANCH_MASTER ORDER BY AD_BR_CODE";
                dtBranch = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtBranch);

                SQL = "SELECT RM_CO_ORIGIN_CODE CODE, RM_CO_ORIGIN_DESC NAME FROM RM_COUNTRY_ORIGIN_MASTER";
                dtSupplier = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtSupplier);

                SQL = "SELECT RM_VM_VENDOR_TYPE CODE, RM_VM_VENDOR_TYPE_NAME NAME FROM RM_VENDOR_TYPE_MASTER";
                dtVendorType = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtVendorType);

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

        public DataTable FetchBank()
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

        public DataTable FillView(string sFillterType, int FinId, DateTime asonDate)
        {
            DataTable dtType = new DataTable();

            try
            {
                string sRetun = string.Empty;

                OracleCommand ocCommand = new OracleCommand("pk_cust_supply_lookup.supplyer_bal_lookup");
                ocCommand.Connection = ocConn;
                ocCommand.CommandType = CommandType.StoredProcedure;

                ////ocCommand.Parameters.Add("finyr", OracleType.VarChar).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("finyr", OracleType.VarChar).Value = FinId;

                ////ocCommand.Parameters.Add("asondate", OracleType.DateTime).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("asondate", OracleType.DateTime).Value = asonDate;

                ////ocCommand.Parameters.Add("cur_return", OracleType.Cursor).Direction = ParameterDirection.Output;


                ocCommand.Parameters.Add("finyr", OracleDbType.Decimal, FinId, ParameterDirection.Input);
                ocCommand.Parameters.Add("asondate", OracleDbType.Date, asonDate, ParameterDirection.Input);
                ocCommand.Parameters.Add("cur_return", OracleDbType.RefCursor).Direction = ParameterDirection.Output;


                OracleDataAdapter odAdpt = new OracleDataAdapter(ocCommand);

                odAdpt.Fill(dtType);
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


        public DataTable FillViewnobalance(string sFillterType)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  rm_vendor_master.rm_vm_vendor_code Code, rm_vm_vendor_name Name,";
                SQL = SQL + " rm_vm_vendor_status status, ";
                SQL = SQL + " RM_VENDOR_TYPE_MASTER.RM_VM_VENDOR_TYPE_NAME rm_vm_vendor_type, rm_vm_tel_no tel_no,";
                SQL = SQL + " sales_pay_type_desc, ";
                SQL = SQL + " TO_CHAR (rm_vm_credit_limit) credit_limit ,";
                SQL = SQL + " DECODE(ATTACHDTLS.CNT, '', 'N', 'Y') DocAttachYN,RM_VM_VERIFIED_YN verified,RM_VM_APPROVED_YN approved ";
                SQL = SQL + " FROM rm_vendor_master,sl_pay_type_master ,RM_VENDOR_TYPE_MASTER ,";
                SQL = SQL + " (SELECT RM_VM_VENDOR_CODE , COUNT(RM_VM_VENDOR_CODE) CNT ";
                SQL = SQL + " FROM RM_VENDOR_MASTER_ATTACH_DTLS ";
                SQL = SQL + " GROUP BY RM_VM_VENDOR_CODE )ATTACHDTLS ";
                SQL = SQL + "  where rm_vendor_master.rm_vm_credit_terms = sl_pay_type_master.sales_pay_type_code(+)";
                SQL = SQL + " AND rm_vendor_master.rm_vm_vendor_type=RM_VENDOR_TYPE_MASTER.RM_VM_VENDOR_TYPE(+)";
                SQL = SQL + " AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE =ATTACHDTLS.RM_VM_VENDOR_CODE(+) ";

                if (sFillterType == "ACTIVE")
                {
                    SQL = SQL + " AND RM_VM_VENDOR_STATUS = 'A'";
                }
                else if (sFillterType == "IN ACTIVE")
                {
                    SQL = SQL + " AND RM_VM_VENDOR_STATUS = 'I'";
                }
                else if (sFillterType == "VERIFIED")
                {
                    SQL = SQL + " AND rm_vendor_master.RM_VM_VERIFIED_YN = 'Y'";
                }
                else if (sFillterType == "NOT VERIFIED")
                {
                    SQL = SQL + " AND rm_vendor_master.RM_VM_VERIFIED_YN = 'N'";
                }
                else if (sFillterType == "APPROVED")
                {
                    SQL = SQL + " AND rm_vendor_master.RM_VM_APPROVED_YN = 'Y'";
                }
                else if (sFillterType == "NOT APPROVED")
                {
                    SQL = SQL + " AND rm_vendor_master.RM_VM_APPROVED_YN = 'N'";
                }


                SQL = SQL + " order by  RM_VENDOR_MASTER.rm_vm_vendor_code desc";
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

        public string Getvendorcode(string sPreFix)
        {


            DataTable dtCode = new DataTable();
            try
            {

                String strName = string.Empty;
                strName = sPreFix.Trim();

                //if (strName.Substring(0, 2) == "AL")
                ////(( strName.Substring(2, 1) == string.Empty )  &&
                //{
                //    sPreFix = strName.Substring(3, 1);
                //}
                //else
                //{
                //    sPreFix = strName.Substring(0, 1);
                //}

                OracleCommand ocCommand = new OracleCommand("PK_CODE_CREATION.GetsupplierCode");

                OracleConnection ocConn = new OracleConnection(sConString.ToString());

                //OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());

                ocCommand.Connection = ocConn;

                ocCommand.CommandType = CommandType.StoredProcedure;

                //ocCommand.Parameters.Add("sPreFix", OracleType.VarChar, 10).Direction = ParameterDirection.Input;
                //ocCommand.Parameters.Add("sPreFix", OracleType.VarChar).Value = sPreFix;

                //  ocCommand.Parameters.Add("new_cus_code", OracleType. ).Direction = ParameterDirection.Output;
                //ocCommand.Parameters.Add("new_cus_code", OracleType.t).Value = nModuleID;


                //  ocCommand.Parameters.Add("cur_return", OracleType.Cursor).Direction = ParameterDirection.Output;

                ocCommand.Parameters.Add("cur_return", OracleDbType.RefCursor).Direction = ParameterDirection.Output;


                OracleDataAdapter odAdpt = new OracleDataAdapter(ocCommand);
                odAdpt.Fill(dtCode);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return System.Convert.ToString(dtCode.Rows[0].ItemArray[0]);
        }

        public DataTable GetDocNumberWith31()
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT max(RM_VM_VENDOR_CODE) Code";
                SQL = SQL + " FROM RM_VENDOR_MASTER ";
                SQL = SQL + " where RM_VM_VENDOR_CODE like'31%'";

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
        public string GetDocNumberBasedGLGroupRealEstae(string sGlGrup)
        {
            DataTable dtType = new DataTable();
            string sCode = "0";
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                //hard code for reals estate/ jomy 02 fev 2019 
                if (sGlGrup == "2101011")
                {
                    SQL = " SELECT  nvl(max(RM_VM_VENDOR_CODE),0)  Code";
                    SQL = SQL + " FROM RM_VENDOR_MASTER ";
                    SQL = SQL + " where RM_VM_VENDOR_CODE like '21%' and  GL_COAM_ACCOUNT_GROUP    ='" + sGlGrup + "'";
                    dtType = clsSQLHelper.GetDataTableByCommand(SQL);
                    if (dtType.Rows[0]["Code"].ToString() == "0")
                    {
                        sCode = "210001";// as per khlaid
                    }
                    else
                    {
                        sCode = (Convert.ToDouble(dtType.Rows[0]["Code"].ToString()) + 1).ToString();
                    }


                }
                else
                {

                    SQL = " SELECT  nvl(max(RM_VM_VENDOR_CODE),0)  Code";
                    SQL = SQL + " FROM RM_VENDOR_MASTER ";
                    SQL = SQL + " where  RM_VM_VENDOR_CODE like '51%' and GL_COAM_ACCOUNT_GROUP     not in ( '2101011')";
                    dtType = clsSQLHelper.GetDataTableByCommand(SQL);
                    if (dtType.Rows[0]["Code"].ToString() == "0")
                    {
                        sCode = "610001";// as per khlaid
                    }
                    else
                    {
                        sCode = (Convert.ToDouble(dtType.Rows[0]["Code"].ToString()) + 1).ToString();
                    }

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
            return sCode;

        }

        public string GetDocNumberBasedGLGroupAERM(string sGlGrup)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT max(RM_VM_VENDOR_CODE) Code";
                SQL = SQL + " FROM RM_VENDOR_MASTER ";
                SQL = SQL + " where GL_COAM_ACCOUNT_GROUP    ='" + sGlGrup + "'";

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
            return dtType.Rows[0]["Code"].ToString();

        }
        public string GetDocNumber()
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT nvl(max(RM_VM_VENDOR_CODE),0)+1 Code";
                SQL = SQL + " FROM RM_VENDOR_MASTER ";

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
            return dtType.Rows[0]["Code"].ToString();

        }


        public DataSet FetchVendorBranchMapping(string CustCode)
        {

            DataTable dtReturn = new DataTable();
            DataTable dtBranchdata = new DataTable();
            DataTable dtSubStatus = new DataTable();
            DataTable dtSalesRep = new DataTable();
            DataSet dsData = new DataSet();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT  ";
                SQL = SQL + "SORT_ID,  RM_VM_LABLE_ID,  RM_VM_LABLE_NAME,  ";
                SQL = SQL + "   RM_VM_LABLE_TYPE,  RM_VM_LABLE_VALUE ";
                SQL = SQL + "FROM  RM_VENDOR_MAST_BRANCH_LIN_VW ";
                SQL = SQL + "order by SORT_ID asc ";


                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtReturn);

                SQL = " SELECT AD_BR_CODE CODE,AD_BR_NAME NAME   ";
                SQL = SQL + "  FROM AD_BRANCH_MASTER   ";
                SQL = SQL + "  ORDER BY AD_BR_CODE ASC  ";

                dtBranchdata = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtBranchdata);




                DataTable dtPayTerms = new DataTable();

                SQL = "   SELECT SALES_PAY_TYPE_CODE  CODE, ";
                SQL = SQL + "            SALES_PAY_TYPE_DESC    NAME ";
                SQL = SQL + "    FROM SL_PAY_TYPE_MASTER ";
                SQL = SQL + " ORDER BY  ";
                SQL = SQL + "         SALES_PAY_TYPE_DESC  ASC ";

                dtPayTerms = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtPayTerms);

                DataTable dtTaxType = new DataTable();

                SQL = "   select  GL_TAX_TYPE_CODE, GL_TAX_TYPE_NAME, GL_TAX_TYPE_PER,  ";
                SQL = SQL + "   GL_TAX_TYPE_SORT_ID  ";
                SQL = SQL + "   from GL_DEFAULTS_TAX_TYPE_MASTER order by   GL_TAX_TYPE_SORT_ID asc ";

                dtTaxType = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtTaxType);



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

        #region "Branch Grand"
        public DataSet FetchBranch(string UserName)
        {

            DataSet dtReturn = new DataSet();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "   SELECT  AD_BR_CODE CODE,  AD_BR_NAME NAME   ";
                SQL = SQL + " FROM AD_BRANCH_MASTER  ";

                if (UserName != "ADMIN")
                {
                    SQL = SQL + "  where AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT AD_BR_CODE FROM AD_USER_GRANTED_BRANCH WHERE AD_UM_USERID ='" + UserName + "' )";
                }

                dtReturn = clsSQLHelper.GetDataset(SQL);

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


        #region "DML "

        public string InsertSQL(VendorMasterEntity oVMEntity, bool bDocAutogenerated, object mngrclassobj,
            List<VendorBranchMapppingEntity> objVendorBranchGridEntity, List<VendorMasteCheckListGridEntity> objCheckListEntity)
        {
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " INSERT INTO RM_VENDOR_MASTER (";
                SQL = SQL + "  RM_VM_VENDOR_CODE, RM_VM_VENDOR_NAME, RM_VM_FULL_NAME, RM_VM_VENDOR_NAME_IN_CHEQUE ,  ";
                SQL = SQL + " RM_VM_VENDOR_TYPE, GL_COAM_ACCOUNT_GROUP, RM_VM_VENDOR_STATUS, ";
                SQL = SQL + " RM_VM_POBOX, RM_VM_ADDRESS, RM_VM_CITY, ";
                SQL = SQL + " RM_VM_TEL_NO, RM_VM_FAXNO, RM_VM_EMAIL, ";
                SQL = SQL + " RM_VM_WEB_SITE, RM_VM_CONT_PERSON1, RM_VM_CONT_DESIG1, ";
                SQL = SQL + " RM_VM_CONT_PERSON2, RM_VM_CONT_DESIG2, RM_VM_BUSINESS_TYPE, ";
                SQL = SQL + " RM_VM_TRADE_LICENSE_NO, RM_VM_LICENSE_EXP_DATE, RM_VM_CREDIT_LIMIT, ";
                SQL = SQL + " RM_VM_CREDIT_PERIOD, RM_VM_CREDIT_TERMS,GL_BM_BNK1_CODE,RM_VM_BANK1_NAME, ";
                SQL = SQL + " RM_VM_BANK1_ACC_NO,RM_VM_IBAN1_CODE,GL_BM_BNK2_CODE, RM_VM_BANK2_NAME,  ";
                SQL = SQL + " RM_VM_BANK2_ACC_NO,RM_VM_IBAN2_CODE, RM_VM_REMARKS, ";
                SQL = SQL + " AD_CM_COMPANY_CODE, RM_VM_CREATEDBY, RM_VM_CREATEDDATE, ";
                SQL = SQL + " RM_VM_UPDATEDBY, RM_VM_UPDATEDDATE, RM_VM_DELETESTATUS, ";
                SQL = SQL + " RM_VM_CONT_EMAIL1,RM_VM_CONT_EMAIL2 ,";
                SQL = SQL + " RM_VM_CONT_PERSON1_MOBNO, RM_VM_CONT_PERSON2_MOBNO, GL_COAM_ACCOUNT_CODE, ";
                SQL = SQL + " HR_DEPT_DEPT_CODE,RM_VM_INVOICE_QTY,";
                SQL = SQL + " RM_VM_EVAL_EXP_DATE,RM_EVAL_EXP_CAT_CODE,RM_VM_EVAL_EXP_REMARKS , ";
                SQL = SQL + " GL_TAX_TYPE_CODE ,RM_VM_TAX_VAT_REG_NUMBER  ,RM_VM_TAX_VAT_PERCENTAGE,";
                SQL = SQL + " SALES_EM_EMIRATE_CODE,AD_BR_CODE,RM_CO_ORIGIN_CODE ,RM_VM_TRN_DATE, ";
                SQL = SQL + "  RM_VM_IBAN1_SWIFT_CODE ,  RM_VM_IBAN1_BANK_ADDRESS ,  RM_VM_IBAN2_SWIFT_CODE  ,RM_VM_IBAN2_BANK_ADDRESS, ";
                SQL = SQL + "  RM_VM_VERIFIED_YN ,  RM_VM_VERIFIED_BY ,  RM_VM_VERIFIED_DATETIME  ,  ";
                SQL = SQL + "  RM_VM_APPROVED_YN ,  RM_VM_APPROVED_BY ,  RM_VM_APPROVED_DATETIME ,   ";
                SQL = SQL + "  RM_VM_PREV_APPROVED_YN ,  RM_VM_PREV_APPROVED_BY ,  RM_VM_PREV_APPROVED_DATETIME    ";
                SQL = SQL + "  ) ";

                SQL = SQL + " VALUES ('" + oVMEntity.VendorCode + "' ,'" + oVMEntity.VendorName + "' ,'" + oVMEntity.VendorFullName + "' ,'" + oVMEntity.Print_CHQ_Name + "' ,";
                SQL = SQL + "'" + oVMEntity.VendorType + "','" + oVMEntity.AccGroup + "' ,'" + oVMEntity.Status + "' ,";
                SQL = SQL + "'" + oVMEntity.POBox + "' ,'" + oVMEntity.Address + "' ,'" + oVMEntity.City + "' ,";
                SQL = SQL + "'" + oVMEntity.TelNo + "' ,'" + oVMEntity.FaxNo + "' ,'" + oVMEntity.Email + "' ,";
                SQL = SQL + "'" + oVMEntity.Website + "' ,'" + oVMEntity.ContactPerson1 + "' ,'" + oVMEntity.CtPerDesig1 + "' ,";
                SQL = SQL + "'" + oVMEntity.ContactPerson2 + "','" + oVMEntity.CtPerDesig2 + "' ,'" + oVMEntity.BusinessType + "' ,";
                SQL = SQL + "'" + oVMEntity.LicenseNo + "' , '" + oVMEntity.ExpriyDate.ToString("dd-MMM-yyyy") + "'," + oVMEntity.CreditLimit + " ,";
                SQL = SQL + "" + oVMEntity.CreditPeriod + " , '" + oVMEntity.CreditTemrs + "','" + oVMEntity.BankCode1 + "','" + oVMEntity.BankName1 + "' ,";
                SQL = SQL + "'" + oVMEntity.BankAccountNo1 + "' ,'" + oVMEntity.IBANNo1 + "', '" + oVMEntity.BankCode2 + "','" + oVMEntity.BankName2 + "',";
                SQL = SQL + " '" + oVMEntity.BankAccountNo2 + "','" + oVMEntity.IBANNo2 + "','" + oVMEntity.Remarks + "' ,";
                SQL = SQL + "'" + mngrclass.CompanyCode + "' ,'" + mngrclass.UserName + "' ,TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy hh:mm:ss tt") + "', 'DD-MM-YYYY HH:MI:SS AM'),";// '" + DateTime.Now.ToString("dd-MMM-yyyy") + "', ";
                SQL = SQL + "'' ,'' ,0 ,";
                SQL = SQL + "'" + oVMEntity.CtPeremail1 + "', '" + oVMEntity.CtPeremail2 + "', ";
                SQL = SQL + "'" + oVMEntity.CtPerMobile1 + "', '" + oVMEntity.CtPerMobile2 + "','" + oVMEntity.AccCode + "' ,'','" + oVMEntity.InvQty + "',";
                SQL = SQL + " '" + oVMEntity.EvalExpDate.ToString("dd-MMM-yyyy") + "','" + oVMEntity.EvalCateCode + "','" + oVMEntity.EvalRemarks + "',";
                SQL = SQL + "'" + oVMEntity.VATType + "','" + oVMEntity.VATRegistrationNumber + "','" + oVMEntity.VATPercentage + "','" + oVMEntity.EmiratesId + "',";
                SQL = SQL + " '" + oVMEntity.BranchID + "','" + oVMEntity.SupplierCountry + "',";
                if (oVMEntity.TRNDate == DateTime.MinValue)
                {
                    SQL = SQL + "'',";
                }
                else
                {
                    SQL = SQL + "'" + oVMEntity.TRNDate.ToString("dd-MMM-yyyy") + "',";
                }
                SQL = SQL + "'" + oVMEntity.SwiftCode1 + "','" + oVMEntity.BankAddress1 + "',";
                SQL = SQL + "'" + oVMEntity.SwiftCode2 + "','" + oVMEntity.BankAddress2 + "',";
                SQL = SQL + "    '" + oVMEntity.sChkVarifiedYN + "' ,'" + oVMEntity.sVarifiedby + "'  ,    TO_DATE( '" + oVMEntity.VerifiedbyTime + "','DD-MM-YYYY HH:MI:SS AM') , ";
                SQL = SQL + "    '" + oVMEntity.Approved + "' ,'" + oVMEntity.ApprovedBy + "'  ,    TO_DATE( '" + oVMEntity.ApprovedbyTime + "','DD-MM-YYYY HH:MI:SS AM')  ,";
                SQL = SQL + "    '" + oVMEntity.PrevApproved + "' ,'" + oVMEntity.PrevApprovedBy + "'  ,    TO_DATE( '" + oVMEntity.PrevApprovedbyTime + "','DD-MM-YYYY HH:MI:SS AM')  ";

                SQL = SQL + " )";


                sSQLArray.Add(SQL);

                if (oVMEntity.Approved == "Y")
                {

                    string HeadoffAccountcode = ""; // GL_HO_ACCOUNT_CODE  hardcode since its linked with hyprerion jomy 16 07 2023 // while chaign the any group must be corrected this as well 
                    if (mngrclass.CompanyCode == "EBRM")
                    {
                        if (oVMEntity.AccGroup == "210205")
                        {
                            HeadoffAccountcode = "22010112"; //22010112	Amount Due To Suppliers - Non Related
                        }
                        else if (oVMEntity.AccGroup == "210206" || oVMEntity.AccGroup == "210207")
                        {   //210207	RELATED-PARTY DUE // 210206	CREDITORS - SISTER CONCREN //                        // 22020100   Due to related parties Current 
                            HeadoffAccountcode = "22020100";
                        }
                        else
                        {
                            HeadoffAccountcode = "22010112"; //22010112	Amount Due To Suppliers - Non Related
                        }
                    }

                    SQL = " INSERT INTO GL_COA_MASTER";
                    SQL = SQL + "             (gl_coam_account_code, gl_coam_account_name,";
                    SQL = SQL + "              gl_coam_account_full_name, gl_coam_acc_type_code,";
                    SQL = SQL + "              gl_coam_account_status, gl_coam_account_group,";
                    SQL = SQL + "              gl_coam_ledger_type, gl_coam_remove, ad_cur_currency_code,";
                    SQL = SQL + "              gl_coam_updatedby, gl_coam_updateddate, ad_cm_company_code,";
                    SQL = SQL + "              gl_coam_deletestatus, gl_coam_createddate, gl_coam_createdby,";
                    SQL = SQL + "              gl_coam_ref_code,GL_COAM_LEDGER_REPORT_TYPE,GL_COAM_MIS_BS_TYPE ,GL_HO_ACCOUNT_CODE )";
                    SQL = SQL + "      VALUES ('" + oVMEntity.AccCode + "','" + oVMEntity.VendorName + "',";
                    SQL = SQL + "              '" + oVMEntity.VendorFullName + "',  '20' ,";
                    SQL = SQL + "               'ACTIVE','" + oVMEntity.AccGroup + "',";
                    SQL = SQL + "               'SUB','Y','01',";
                    SQL = SQL + "               '" + mngrclass.UserName + "','" + DateTime.Now.ToString("dd-MMM-yyyy") + "','" + mngrclass.CompanyCode + "',";
                    SQL = SQL + "               0" + ",'" + DateTime.Now.ToString("dd-MMM-yyyy") + "','" + mngrclass.UserName + "','" + oVMEntity.VendorCode + "',";
                    SQL = SQL + "  'TRADEPAY','BS','" + HeadoffAccountcode + "')";

                    sSQLArray.Add(SQL);
                }


                sRetun = InsertvendorBranchMappingDetailsSQL(oVMEntity.VendorCode, objVendorBranchGridEntity, mngrclass, "INSERT", 0, oVMEntity.Approved);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }


                sRetun = InsertCheckListSQL(oVMEntity.VendorCode, objCheckListEntity, mngrclass);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMVM", oVMEntity.VendorCode, bDocAutogenerated, Environment.MachineName, "I", sSQLArray);

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

        public string InsertCheckListSQL(string vendor, List<VendorMasteCheckListGridEntity> objCheckListEntity, object mngrclassobj)
        {

            string sRetun = string.Empty;
            try
            {
                // object oCode;
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                Int16 i = default(Int16);

                Int32 SLNO = 0;

                SQL = " DELETE FROM  RM_VENDOR_MASTER_CHECK_LIST  WHERE   RM_VM_VENDOR_CODE ='" + vendor + "' ";
                sSQLArray.Add(SQL);


                i = 0;
                foreach (var Data in objCheckListEntity)
                {
                    SLNO = ++i;

                    SQL = " INSERT INTO RM_VENDOR_MASTER_CHECK_LIST (";
                    SQL = SQL + "   RM_VM_VENDOR_CODE, AD_FIN_FINYRID, RM_CHK_CHECK_LIST_CODE,";
                    SQL = SQL + "   RM_VM_CHK_REMRAKS,RM_VM_CHK_SELECT_YN, RM_VM_CHK_SLNO)  ";

                    SQL = SQL + "VALUES ( '" + vendor + "', " + mngrclass.FinYearID + ",  '" + Data.CheckListCode + "',";
                    SQL = SQL + "   '" + Data.CheckListRmk + "','" + Data.CheckListYN + "', '" + SLNO + "' ) ";


                    sSQLArray.Add(SQL);

                }

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
            return "CONTINUE";


        }


        public string InsertvendorBranchMappingDetailsSQL(string Code, List<VendorBranchMapppingEntity> objVendorBranchGridEntity, object mngrclassobj, string Type,
            double SeqNoGenerated,string Approved)
        {

            string sRetun = string.Empty;

            DataTable dtReturn = new DataTable();
            try
            {
                // object oCode;
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                Int16 i = default(Int16);
                SqlHelper clsSQLHelper = new SqlHelper();

                //SQL = " DELETE FROM  RM_VENDOR_MASTER_BRANCH_DATA  WHERE   RM_VM_VENDOR_CODE ='" + Code + "' ";
                //  sSQLArray.Add(SQL);
                if (Type == "INSERT")
                {
                    i = 0;
                    foreach (var Data in objVendorBranchGridEntity)
                    {
                        ++i;

                        SQL = " INSERT INTO RM_VENDOR_MASTER_BRANCH_DATA ( ";
                        SQL = SQL + "   RM_VM_VENDOR_CODE,AD_BR_CODE, RM_VM_SORT_ID,  ";
                        SQL = SQL + "   RM_VM_LABLE_ID, RM_VM_LABLE_TYPE, RM_VM_LABLE_VALUE,RM_VM_APPROVED_YN)  ";
                        SQL = SQL + "VALUES ( '" + Code + "','" + Data.mVendorBranchCode + "','" + Data.mSlNo + "', ";
                        SQL = SQL + "   '" + Data.mLableId + "','" + Data.mLableType + "', '" + Data.mLableValue + "','"+ Approved + "' ) ";

                        sSQLArray.Add(SQL);
                    }
                }
                else
                {
                    foreach (var Data in objVendorBranchGridEntity)
                    {

                        SQL = " select Count(*) CNT from RM_VENDOR_MASTER_BRANCH_DATA   where  ";
                        SQL = SQL + "   RM_VM_VENDOR_CODE='" + Code + "' and AD_BR_CODE='" + Data.mVendorBranchCode + "'   ";
                        SQL = SQL + "  and  RM_VM_LABLE_ID= '" + Data.mLableId + "' and RM_VM_LABLE_TYPE='" + Data.mLableType + "'  ";
                        dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);

                        if (Convert.ToDouble(dtReturn.Rows[0]["CNT"].ToString()) > 0)
                        {

                            SQL = " update RM_VENDOR_MASTER_BRANCH_DATA set RM_VM_LABLE_VALUE='" + Data.mLableValue + "' , ";
                            SQL = SQL + " RM_VM_UPDATEDBY='" + mngrclass.UserName + "' ,";
                            SQL = SQL + " RM_VM_VENDOR_SESSION_ID=" + SeqNoGenerated + " ,";
                            SQL = SQL + " RM_VM_UPDATEDDATE ='" + DateTime.Now.ToString("dd-MMM-yyyy") + "',RM_VM_APPROVED_YN = '" + Approved + "'  ";
                            SQL = SQL + "  where  ";
                            SQL = SQL + "   RM_VM_VENDOR_CODE='" + Code + "' and AD_BR_CODE='" + Data.mVendorBranchCode + "'   ";
                            SQL = SQL + "  and  RM_VM_LABLE_ID= '" + Data.mLableId + "' and RM_VM_LABLE_TYPE='" + Data.mLableType + "'  ";

                            sSQLArray.Add(SQL);
                        }
                        else
                        {
                            SQL = " INSERT INTO RM_VENDOR_MASTER_BRANCH_DATA ( ";
                            SQL = SQL + "   RM_VM_VENDOR_CODE,AD_BR_CODE, RM_VM_SORT_ID,  ";
                            SQL = SQL + "   RM_VM_LABLE_ID, RM_VM_LABLE_TYPE, RM_VM_LABLE_VALUE,RM_VM_APPROVED_YN)  ";
                            SQL = SQL + "VALUES ( '" + Code + "','" + Data.mVendorBranchCode + "','" + Data.mSlNo + "', ";
                            SQL = SQL + "   '" + Data.mLableId + "','" + Data.mLableType + "', '" + Data.mLableValue + "','" + Approved + "' ) ";

                            sSQLArray.Add(SQL);
                        }



                    }
                }

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
            return "CONTINUE";

        }

        public string DeleteSQL(string sCode, string sAccCode, object mngrclassobj)
        {
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();
                sRetun = DeleteCheck(sCode, sAccCode);

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                SQL = "DELETE FROM  RM_VENDOR_MASTER   ";
                SQL = SQL + " WHERE RM_VM_VENDOR_CODE ='" + sCode + "'";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM   GL_COA_MASTER";
                SQL = SQL + " WHERE gl_coam_account_code ='" + sAccCode + "'";
                sSQLArray.Add(SQL);

                SQL = " DELETE FROM  RM_VENDOR_MASTER_BRANCH_DATA  WHERE   RM_VM_VENDOR_CODE ='" + sCode + "' ";
                sSQLArray.Add(SQL);


                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMVM", sCode, false, Environment.MachineName, "D", sSQLArray);

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

        private string DeleteCheck(string Code, string AccCode)
        {
            DataSet dsCnt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the  opening 
                SQL = "select count(*) CNT from gl_coa_balances where GL_COAM_ACCOUNT_CODE='" + Code + "'";
                // SQL = " select count(*) CNT from gl_coa_balances where GL_COAM_ACCOUNT_CODE='" + AccCode + "' And  GL_COAB_OPN_BAL > 0 ";

                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "Opening Balance amount is greater than zero Against this customer";
                }

                SQL = " select count(*) CNT from gl_coa_balances where GL_COAM_ACCOUNT_CODE='" + AccCode + "' And  GL_COAB_OPN_BAL > 0 ";
                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "Opening Balance amount is greater than zero Against this customer";
                }

            }
            catch (Exception ex)
            {
                return System.Convert.ToString(ex.Message);
            }
            return "CONTINUE";
        }

        public DataTable FetchData(string sCode)
        {
            DataTable dtReturn = new DataTable();
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
                //SQL = SQL + "  bank1.GL_BM_SWIFT_CODE SWIFT_CODE1, bank1.GL_BM_BANK_ADDRESS BANK_ADDRESS1, bank2.GL_BM_SWIFT_CODE SWIFT_CODE2, bank2.GL_BM_BANK_ADDRESS BANK_ADDRESS2,";

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
                SQL = SQL + " RM_VM_CONT_EMAIL2 , RM_VM_TRN_DATE,RM_VM_VERIFIED_YN,RM_VM_VERIFIED_BY,RM_VM_VERIFIED_DATETIME,RM_VM_APPROVED_YN,RM_VM_APPROVED_BY,RM_VM_APPROVED_DATETIME ,RM_VM_PREV_APPROVED_YN ";
                SQL = SQL + " FROM RM_VENDOR_MASTER,GL_BANK_MASTER bank1, GL_BANK_MASTER bank2,RM_COUNTRY_ORIGIN_MASTER";
                SQL = SQL + " WHERE RM_VENDOR_MASTER.GL_BM_BNK1_CODE = bank1.GL_BM_BNK_CODE(+)";
                SQL = SQL + " AND RM_VENDOR_MASTER.GL_BM_BNK2_CODE = bank2.GL_BM_BNK_CODE(+)";
                SQL = SQL + " AND RM_VENDOR_MASTER.RM_CO_ORIGIN_CODE = RM_COUNTRY_ORIGIN_MASTER.RM_CO_ORIGIN_CODE(+)";
                SQL = SQL + " AND RM_VM_VENDOR_CODE ='" + sCode + "'";

                //  dtReturn = clsSQLHelper.GetDataset(SQL);
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
        public DataSet FetchAuditDetils(string DocNo)
        {

            DataSet dt = new DataSet();

            try
            {

                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT  ";
                SQL = SQL + "   AD_DOC_NO DOC_NO  ,  AD_UM_USERID USERID,";
                SQL = SQL + "      to_char( AD_AT_AUDIT_DATE,'DD-MON-YYYY HH12:MI:SS AM')  AUDIT_DATE ,AD_MI_ERP_LABEL_NAME, ";
                SQL = SQL + "  AD_MI_ERP_OLD_VALUE OLD_VALUE,AD_MI_ERP_NEW_VALUE NEW_VALUE, AD_BR_CODE  ";
                SQL = SQL + "   FROM  " + TempSchema + ".AD_DOCUMENT_AUDIT_TRIAL ";
                SQL = SQL + "    where  ";
                SQL = SQL + "   AD_MI_ITEMID ='RMVM' ";
                if (!string.IsNullOrEmpty(DocNo))
                {
                    SQL = SQL + "  and   AD_DOC_NO = '" + DocNo + "'";
                }


                SQL = SQL + "    order by AD_DOC_NO ,AD_AT_AUDIT_DATE desc ";



                dt = clsSQLHelper.GetDataset(SQL);
                dt.Tables[0].TableName = "AUDIT";

            }

            catch (Exception ex)
            { }
            return dt;
        }


        public DataTable FetchCheckListDetails(string Vendorcode, object mngrclassobj)
        {
            DataSet dsReturn = new DataSet();
            // DataTable dtMaster = new DataTable();
            DataTable dtOffense = new DataTable();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT RM_VM_VENDOR_CODE , RM_VENDOR_MASTER_CHECK_LIST.RM_CHK_CHECK_LIST_CODE, ";
                SQL = SQL + " RM_VM_CHK_SLNO, RM_VM_CHK_SELECT_YN,  ";
                SQL = SQL + " RM_VM_CHK_REMRAKS, RM_CHK_CHKLIST_DESC  ";
                SQL = SQL + " FROM  RM_VENDOR_MASTER_CHECK_LIST , RM_CHECK_LIST_VENDOR_MASTER  ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + " RM_VENDOR_MASTER_CHECK_LIST.RM_CHK_CHECK_LIST_CODE= RM_CHECK_LIST_VENDOR_MASTER.RM_CHK_CHECK_LIST_CODE ";
                SQL = SQL + " AND RM_VM_VENDOR_CODE ='" + Vendorcode + "' order by RM_VM_CHK_SLNO asc  ";


                dtOffense = clsSQLHelper.GetDataTableByCommand(SQL);

                ////  dsReturn.Tables.Add(dtOffense);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                ocConn.Close();
                ocConn.Dispose();
            }
            return dtOffense;

        }


        public DataSet fetchVendorBranchMappingDetails(string VendorCode, object mngrclassobj)
        {
            DataSet dsReturn = new DataSet();
            DataTable dtMaster = new DataTable();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT RM_VENDOR_MASTER_BRANCH_DATA.RM_VM_VENDOR_CODE,   ";
                SQL = SQL + "       RM_VENDOR_MASTER_BRANCH_DATA.AD_BR_CODE,   ";
                SQL = SQL + "       RM_VENDOR_MASTER_BRANCH_DATA.RM_VM_SORT_ID,   ";
                SQL = SQL + "       RM_VENDOR_MASTER_BRANCH_DATA.RM_VM_LABLE_ID,   ";
                SQL = SQL + "       RM_VENDOR_MAST_BRANCH_LIN_VW.RM_VM_LABLE_NAME,   ";
                SQL = SQL + "       RM_VENDOR_MASTER_BRANCH_DATA.RM_VM_LABLE_TYPE,   ";
                SQL = SQL + "       RM_VENDOR_MASTER_BRANCH_DATA.RM_VM_LABLE_VALUE  ";
                SQL = SQL + "  FROM RM_VENDOR_MASTER_BRANCH_DATA,   ";
                SQL = SQL + "       RM_VENDOR_MAST_BRANCH_LIN_VW  ";
                SQL = SQL + "     WHERE RM_VENDOR_MASTER_BRANCH_DATA.RM_VM_LABLE_ID = RM_VENDOR_MAST_BRANCH_LIN_VW.RM_VM_LABLE_ID   ";
                SQL = SQL + "       AND RM_VENDOR_MASTER_BRANCH_DATA.RM_VM_VENDOR_CODE ='" + VendorCode + "'  ";
                SQL = SQL + "       ORDER BY RM_VENDOR_MASTER_BRANCH_DATA.AD_BR_CODE,RM_VENDOR_MASTER_BRANCH_DATA.RM_VM_SORT_ID ASC   ";


                dtMaster = clsSQLHelper.GetDataTableByCommand(SQL);
                dsReturn.Tables.Add(dtMaster);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                ocConn.Close();
                ocConn.Dispose();
            }
            return dsReturn;

        }



        public double GetNextSeqNo()
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = " SELECT SEQ_DOCUMENT_AUDIT.NEXTVAL next_seq_no  FROM DUAL";

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


        public string UpdateSQL(VendorMasterEntity oVMEntity, object mngrclassobj, List<VendorBranchMapppingEntity> objVendorBranchGridEntity, List<VendorMasteCheckListGridEntity> objCheckListEntity)
        {
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SqlHelper clsSQLHelper = new SqlHelper();
                DataTable dtAccounts = new DataTable();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                double SeqNoGenerated = GetNextSeqNo();

                sSQLArray.Clear();


                SQL = "UPDATE RM_VENDOR_MASTER SET ";

                SQL = SQL + " RM_VM_VENDOR_NAME= '" + oVMEntity.VendorName + "' ,";
                SQL = SQL + " RM_VM_FULL_NAME ='" + oVMEntity.VendorFullName + "' ,";
                SQL = SQL + " RM_VM_VENDOR_NAME_IN_CHEQUE  ='" + oVMEntity.Print_CHQ_Name + "',";
                SQL = SQL + " RM_VM_VENDOR_TYPE= '" + oVMEntity.VendorType + "',";
                SQL = SQL + "  RM_VM_VENDOR_STATUS ='" + oVMEntity.Status + "' ,";
                SQL = SQL + " RM_VM_POBOX ='" + oVMEntity.POBox + "' ,";
                SQL = SQL + " RM_VM_ADDRESS = '" + oVMEntity.Address + "' ,";
                SQL = SQL + " RM_VM_CITY ='" + oVMEntity.City + "' ,";
                SQL = SQL + " RM_VM_TEL_NO ='" + oVMEntity.TelNo + "' ,";
                SQL = SQL + " RM_VM_FAXNO ='" + oVMEntity.FaxNo + "' ,";
                SQL = SQL + "  RM_VM_EMAIL ='" + oVMEntity.Email + "' ,";
                SQL = SQL + " RM_VM_WEB_SITE='" + oVMEntity.Website + "' ,";
                SQL = SQL + " RM_VM_CONT_PERSON1 ='" + oVMEntity.ContactPerson1 + "' ,";
                SQL = SQL + " RM_VM_CONT_DESIG1 ='" + oVMEntity.CtPerDesig1 + "' ,";
                SQL = SQL + " RM_VM_CONT_PERSON2='" + oVMEntity.ContactPerson2 + "',";
                SQL = SQL + " RM_VM_CONT_DESIG2 ='" + oVMEntity.CtPerDesig2 + "' ,";
                SQL = SQL + " RM_VM_BUSINESS_TYPE ='" + oVMEntity.BusinessType + "' ,";
                SQL = SQL + " RM_VM_TRADE_LICENSE_NO='" + oVMEntity.LicenseNo + "' ,";
                SQL = SQL + " RM_VM_LICENSE_EXP_DATE ='" + oVMEntity.ExpriyDate.ToString("dd-MMM-yyyy") + "',";
                if (oVMEntity.TRNDate != DateTime.MinValue)
                {
                    SQL = SQL + " RM_VM_TRN_DATE ='" + oVMEntity.TRNDate.ToString("dd-MMM-yyyy") + "',";
                }
                SQL = SQL + " RM_VM_CREDIT_LIMIT =" + oVMEntity.CreditLimit + " ,";
                SQL = SQL + " RM_VM_CREDIT_PERIOD=" + oVMEntity.CreditPeriod + " ,";
                SQL = SQL + " RM_VM_CREDIT_TERMS = '" + oVMEntity.CreditTemrs + "',";
                SQL = SQL + " GL_BM_BNK1_CODE = '" + oVMEntity.BankCode1 + "' ,";
                SQL = SQL + " RM_VM_BANK1_NAME = '" + oVMEntity.BankName1 + "' ,";
                SQL = SQL + " RM_VM_BANK1_ACC_NO='" + oVMEntity.BankAccountNo1 + "' ,";
                SQL = SQL + " RM_VM_IBAN1_CODE = '" + oVMEntity.IBANNo1 + "' ,";
                SQL = SQL + " GL_BM_BNK2_CODE = '" + oVMEntity.BankCode2 + "' ,";
                SQL = SQL + " RM_VM_BANK2_NAME = '" + oVMEntity.BankName2 + "',";
                SQL = SQL + " RM_VM_BANK2_ACC_NO = '" + oVMEntity.BankAccountNo2 + "',";
                SQL = SQL + " RM_VM_IBAN2_CODE = '" + oVMEntity.IBANNo2 + "' ,";
                SQL = SQL + " RM_VM_IBAN1_SWIFT_CODE = '" + oVMEntity.SwiftCode1 + "',RM_VM_IBAN1_BANK_ADDRESS = '" + oVMEntity.BankAddress1 + "',";
                SQL = SQL + "RM_VM_IBAN2_SWIFT_CODE = '" + oVMEntity.SwiftCode2 + "',RM_VM_IBAN2_BANK_ADDRESS = '" + oVMEntity.BankAddress2 + "',";

                SQL = SQL + " RM_VM_REMARKS= '" + oVMEntity.Remarks + "' ,";
                SQL = SQL + " SALES_EM_EMIRATE_CODE ='" + oVMEntity.EmiratesId + "',";
                SQL = SQL + " RM_VM_UPDATEDBY='" + mngrclass.UserName + "' ,";
                SQL = SQL + " RM_VM_UPDATEDDATE ='" + DateTime.Now.ToString("dd-MMM-yyyy") + "', ";
                SQL = SQL + " RM_VM_CONT_PERSON1_MOBNO='" + oVMEntity.CtPerMobile1 + "',";
                SQL = SQL + " RM_VM_CONT_PERSON2_MOBNO = '" + oVMEntity.CtPerMobile2 + "',";
                SQL = SQL + " RM_VM_CONT_EMAIL1='" + oVMEntity.CtPeremail1 + "',";
                SQL = SQL + " RM_VM_CONT_EMAIL2='" + oVMEntity.CtPeremail2 + "',";
                SQL = SQL + " RM_VM_INVOICE_QTY = '" + oVMEntity.InvQty + "',";
                SQL = SQL + " RM_VM_EVAL_EXP_DATE = '" + oVMEntity.EvalExpDate.ToString("dd-MMM-yyyy") + "',";
                SQL = SQL + "RM_VM_EVAL_EXP_REMARKS='" + oVMEntity.EvalRemarks + "' ,";
                SQL = SQL + " RM_EVAL_EXP_CAT_CODE ='" + oVMEntity.EvalCateCode + "',";
                SQL = SQL + "  GL_TAX_TYPE_CODE = '" + oVMEntity.VATType + "',";
                SQL = SQL + "  RM_VM_TAX_VAT_REG_NUMBER='" + oVMEntity.VATRegistrationNumber + "',";
                SQL = SQL + " RM_VM_TAX_VAT_PERCENTAGE ='" + oVMEntity.VATPercentage + "',";
                SQL = SQL + "  AD_BR_CODE ='" + oVMEntity.BranchID + "',";
                SQL = SQL + " RM_CO_ORIGIN_CODE = '" + oVMEntity.SupplierCountry + "',";

                SQL = SQL + "       RM_VM_VERIFIED_YN            ='" + oVMEntity.sChkVarifiedYN + "', ";
                SQL = SQL + "       RM_VM_VERIFIED_BY         = '" + oVMEntity.sVarifiedby + "', ";
                SQL = SQL + "       RM_VM_VENDOR_SESSION_ID         = " + SeqNoGenerated + ", ";

                SQL = SQL + "      RM_VM_VERIFIED_DATETIME  = TO_DATE( '" + oVMEntity.VerifiedbyTime + "','DD-MM-YYYY HH:MI:SS AM') ,";

                if(oVMEntity.PrevApproved=="Y")
                {
                    SQL = SQL + "       RM_VM_PREV_APPROVED_YN            = '" + oVMEntity.PrevApproved + "', ";
                    SQL = SQL + "       RM_VM_PREV_APPROVED_BY          = '" + oVMEntity.PrevApprovedBy + "', ";
                    SQL = SQL + "      RM_VM_PREV_APPROVED_DATETIME  = TO_DATE( '" + oVMEntity.PrevApprovedbyTime + "','DD-MM-YYYY HH:MI:SS AM') , ";
                }

                SQL = SQL + "       RM_VM_APPROVED_YN            = '" + oVMEntity.Approved + "', ";
                SQL = SQL + "       RM_VM_APPROVED_BY          = '" + oVMEntity.ApprovedBy + "', ";
                SQL = SQL + "      RM_VM_APPROVED_DATETIME  = TO_DATE( '" + oVMEntity.ApprovedbyTime + "','DD-MM-YYYY HH:MI:SS AM') ";


                SQL = SQL + " WHERE RM_VM_VENDOR_CODE ='" + oVMEntity.VendorCode + "'";



                sSQLArray.Add(SQL);


                SQL = " SELECT  nvl(count(*),0)  cnt ";
                SQL = SQL + " FROM GL_COA_MASTER";
                SQL = SQL + " WHERE  gl_coam_account_code ='" + oVMEntity.AccCode + "'   ";

                dtAccounts = clsSQLHelper.GetDataTableByCommand(SQL);

                if (int.Parse(dtAccounts.Rows[0]["CNT"].ToString()) <= 0)
                {
                    if (oVMEntity.Approved == "Y" && oVMEntity.ApprovedEnabled == "TRUE")
                    {
                        string HeadoffAccountcode = ""; // GL_HO_ACCOUNT_CODE  hardcode since its linked with hyprerion jomy 16 07 2023 // while chaign the any group must be corrected this as well 
                        if (mngrclass.CompanyCode == "EBRM")
                        {
                            if (oVMEntity.AccGroup == "210205")
                            {
                                HeadoffAccountcode = "22010112"; //22010112	Amount Due To Suppliers - Non Related
                            }
                            else if (oVMEntity.AccGroup == "210206" || oVMEntity.AccGroup == "210207")
                            {   //210207	RELATED-PARTY DUE // 210206	CREDITORS - SISTER CONCREN //                        // 22020100   Due to related parties Current 
                                HeadoffAccountcode = "22020100";
                            }
                            else
                            {
                                HeadoffAccountcode = "22010112"; //22010112	Amount Due To Suppliers - Non Related
                            }
                        }

                        SQL = " INSERT INTO GL_COA_MASTER";
                        SQL = SQL + "             (gl_coam_account_code, gl_coam_account_name,";
                        SQL = SQL + "              gl_coam_account_full_name, gl_coam_acc_type_code,";
                        SQL = SQL + "              gl_coam_account_status, gl_coam_account_group,";
                        SQL = SQL + "              gl_coam_ledger_type, gl_coam_remove, ad_cur_currency_code,";
                        SQL = SQL + "              gl_coam_updatedby, gl_coam_updateddate, ad_cm_company_code,";
                        SQL = SQL + "              gl_coam_deletestatus, gl_coam_createddate, gl_coam_createdby,";
                        SQL = SQL + "              gl_coam_ref_code,GL_COAM_LEDGER_REPORT_TYPE,GL_COAM_MIS_BS_TYPE ,GL_HO_ACCOUNT_CODE )";
                        SQL = SQL + "      VALUES ('" + oVMEntity.AccCode + "','" + oVMEntity.VendorName + "',";
                        SQL = SQL + "              '" + oVMEntity.VendorFullName + "',  '20' ,";
                        SQL = SQL + "               'ACTIVE','" + oVMEntity.AccGroup + "',";
                        SQL = SQL + "               'SUB','Y','01',";
                        SQL = SQL + "               '" + mngrclass.UserName + "','" + DateTime.Now.ToString("dd-MMM-yyyy") + "','" + mngrclass.CompanyCode + "',";
                        SQL = SQL + "               0" + ",'" + DateTime.Now.ToString("dd-MMM-yyyy") + "','" + mngrclass.UserName + "','" + oVMEntity.VendorCode + "',";
                        SQL = SQL + "  'TRADEPAY','BS','" + HeadoffAccountcode + "')";

                        sSQLArray.Add(SQL);
                    }
                }
                else
                {
                    SQL = " UPDATE  GL_COA_MASTER ";
                    SQL = SQL + "  SET gl_coam_account_name ='" + oVMEntity.VendorName + "'  , ";
                    SQL = SQL + "  gl_coam_account_full_name ='" + oVMEntity.VendorFullName + "'";
                    SQL = SQL + " WHERE gl_coam_account_code ='" + oVMEntity.AccCode + "'";
                    sSQLArray.Add(SQL);
                }



                sRetun = InsertvendorBranchMappingDetailsSQL(oVMEntity.VendorCode, objVendorBranchGridEntity, mngrclass, "UPDATE", SeqNoGenerated, oVMEntity.Approved);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }




                sRetun = InsertCheckListSQL(oVMEntity.VendorCode, objCheckListEntity, mngrclass);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }


                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMVM", oVMEntity.VendorCode, false, Environment.MachineName, "U", sSQLArray);

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

        #region "insert hisotry"
        public string InsertHistoryNameSQL(VendorMasterHistoryEntity oEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;
            try
            {

                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " INSERT INTO RM_VENDOR_MASTER_NAME_HISTORY (";
                SQL = SQL + " RM_VM_VENDOR_CODE, RM_VM_HISTORY_NAME, RM_VM_HISTORY_FULL_NAME, ";
                SQL = SQL + " RM_VM_HISTORY_IN_CHEQUE_NAME, RM_VM_HISTORY_slno, RM_VM_HISTORY_CHANGE_DATE ";

                SQL = SQL + " ) ";
                SQL = SQL + " VALUES ('" + oEntity.sVendorCode + "' , '" + oEntity.sVendorHisName + "','" + oEntity.sVendorHisFullName + "' ,";
                SQL = SQL + "'" + oEntity.sChqName + "' , '" + oEntity.sSLNO + "','" + Convert.ToDateTime(oEntity.NameRepDate).ToString("dd-MMM-yyyy") + "' )";


                sSQLArray.Add(SQL);


                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMVM", oEntity.sVendorCode, false, Environment.MachineName, "U", sSQLArray);
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

        public string UpdateGridDetailsSQL(VendorMasterHistoryGridEntity oRtnEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                string IssueCode = string.Empty;

                sSQLArray.Clear();

                SQL = " UPDATE RM_VENDOR_MASTER_NAME_HISTORY SET  RM_VM_HISTORY_CHANGE_DATE='" + Convert.ToDateTime(oRtnEntity.dNameRepDate).ToString("dd-MMM-yyyy") + "',";
                SQL = SQL + "  RM_VM_HISTORY_FULL_NAME='" + oRtnEntity.sVendorHisFullName + "',";
                SQL = SQL + " RM_VM_HISTORY_IN_CHEQUE_NAME= '" + oRtnEntity.sChqName + "'";
                SQL = SQL + "WHERE RM_VM_VENDOR_CODE='" + oRtnEntity.sVendorCode + "' and RM_VM_HISTORY_slno=" + oRtnEntity.sSLNO + "";
                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMVM", oRtnEntity.sVendorCode, false, Environment.MachineName, "U", sSQLArray);

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

        public int HistoryMaxRowCount(string EmpCode)
        {
            DataTable dtType = new DataTable();
            int Count = 0;
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT COUNT(*) COUNT FROM  RM_VENDOR_MASTER_NAME_HISTORY WHERE  RM_VM_VENDOR_CODE='" + EmpCode + "'";

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);
                Count = Convert.ToInt32(dtType.Rows[0]["COUNT"]);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return Count;
        }

        #endregion

        #region "hissotory"

        public DataSet FillGrid(string sCode)
        {
            DataSet dsData = new DataSet();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT   ";
                SQL = SQL + " RM_VENDOR_MASTER_NAME_HISTORY.RM_VM_VENDOR_CODE,  RM_VENDOR_MASTER_NAME_HISTORY.RM_VM_HISTORY_NAME,";
                SQL = SQL + " RM_VENDOR_MASTER_NAME_HISTORY.RM_VM_HISTORY_FULL_NAME,RM_VENDOR_MASTER_NAME_HISTORY.RM_VM_HISTORY_IN_CHEQUE_NAME,";
                SQL = SQL + " RM_VENDOR_MASTER_NAME_HISTORY.RM_VM_HISTORY_slno,RM_VENDOR_MASTER_NAME_HISTORY.RM_VM_HISTORY_CHANGE_DATE";
                SQL = SQL + " FROM RM_VENDOR_MASTER_NAME_HISTORY ";
                SQL = SQL + "  where  RM_VENDOR_MASTER_NAME_HISTORY.RM_VM_VENDOR_CODE ='" + sCode + "'   ORDER BY RM_VM_HISTORY_slno ASC ";

                dsData = clsSQLHelper.GetDataset(SQL);

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

        public DataTable FillHistoryView()
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT VENDOR_CODE,VENDOR_NAME,RM_VM_HISTORY_NAME,FIELD_THREE  ";
                SQL = SQL + " FROM (";
                SQL = SQL + " SELECT ";
                SQL = SQL + "   RM_VENDOR_MASTER.RM_VM_VENDOR_CODE VENDOR_CODE, RM_VM_VENDOR_NAME VENDOR_NAME,''RM_VM_HISTORY_NAME , RM_VM_VENDOR_TYPE_NAME  FIELD_THREE ";
                SQL = SQL + " FROM ";
                SQL = SQL + "   RM_VENDOR_MASTER ,RM_VENDOR_TYPE_MASTER ";
                SQL = SQL + " WHERE RM_VENDOR_MASTER.RM_VM_VENDOR_TYPE=RM_VENDOR_TYPE_MASTER.RM_VM_VENDOR_TYPE(+)";
                SQL = SQL + " UNION ALL ";
                SQL = SQL + " SELECT ";
                SQL = SQL + "   RM_VENDOR_MASTER_NAME_HISTORY.RM_VM_VENDOR_CODE VENDOR_CODE,  RM_VENDOR_MASTER_NAME_HISTORY.RM_VM_HISTORY_NAME    VENDOR_NAME,RM_VENDOR_MASTER_NAME_HISTORY.RM_VM_HISTORY_NAME  RM_VM_HISTORY_NAME ,''FIELD_THREE";
                SQL = SQL + " FROM ";
                SQL = SQL + "   RM_VENDOR_MASTER_NAME_HISTORY )";
                SQL = SQL + " order by  VENDOR_CODE ASC ";

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

        #region  "Doc Attach funtions - Insert - Fill - delete Path "

        public void InsertAttachFile(string lblCVPath, string AttachRemarks, string txtEmployeeCode)
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

        public DataSet fillAttachGrid(string txtEmployeeCode)
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

        public void DeletePath(string txtEmployeeCode, string Path)
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

        public int FetchPayemntDays(string PaymentTermCode)
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT   SALES_PAY_NO_DAYS  FROM  SL_PAY_TYPE_MASTER  where SALES_PAY_TYPE_CODE = '" + PaymentTermCode + "'";
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
            return int.Parse(dsData.Tables[0].Rows[0]["SALES_PAY_NO_DAYS"].ToString());
        }


        #region "print Data " 

        public DataSet FetchVoucherPrintData(string voucherno, object mngrclassobj)
        {
            DataSet dSPrint = new DataSet("VOUCHERPRINT");
            DataTable dtMaster = new DataTable();
            DataTable dtMatchDetails = new DataTable();
            DataTable dtMatchRVDetails = new DataTable();

            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = " SELECT * FROM RM_VENDOR_VOUCHER_RPT_VW ";
                SQL = SQL + " WHERE VENDORCODE	 ='" + voucherno + "'";

                dtMaster = clsSQLHelper.GetDataTableByCommand(SQL);
                dtMaster.TableName = "RM_VENDOR_VOUCHER_RPT_VW";
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

        #region Fetch Country
        public DataTable FetchCountry()
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT RM_CO_ORIGIN_CODE, RM_CO_ORIGIN_DESC,RM_CO_ALPHA_3_CODE,RM_CO_NUMERIC_CODE ";
                SQL = SQL + "    FROM RM_COUNTRY_ORIGIN_MASTER ";
                SQL = SQL + "ORDER BY RM_CO_ORIGIN_CODE ASC ";

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
    }

    #region "Entity"

    public class VendorMasterEntity
    {
        public VendorMasterEntity()
        {
            //
            // TODO: Add constructor logic here
            //
        }
        private string vendorCode = string.Empty;
        private string vendorName = string.Empty;

        /// <summary>
        /// UserName
        /// </summary>
        public string VendorCode
        {
            get { return vendorCode; }
            set { vendorCode = value; }
        }
        /// <summary>
        /// Password
        /// </summary>
        public string VendorName
        {
            get { return vendorName; }
            set { vendorName = value; }
        }

        public string Print_CHQ_Name
        {
            get;
            set;
        }
        public string SupplierCountry
        {
            get;
            set;
        }



        public string VendorFullName
        {
            get;
            set;
        }

        private string sAccGroup = string.Empty;
        public string AccGroup
        {
            get { return sAccGroup; }
            set { sAccGroup = value; }
        }

        private string sAccCode = string.Empty;

        public string AccCode
        {
            get { return sAccCode; }
            set { sAccCode = value; }
        }


        private string vendorType = string.Empty;
        public string VendorType
        {
            get { return vendorType; }
            set { vendorType = value; }
        }


        private string sActive = string.Empty;
        public string Status
        {
            get { return sActive; }
            set { sActive = value; }
        }


        private string sAddress = string.Empty;
        public string Address
        {
            get { return sAddress; }
            set { sAddress = value; }
        }

        private string sPOBox = string.Empty;
        public string POBox
        {
            get { return sPOBox; }
            set { sPOBox = value; }
        }
        public string EmiratesId
        {
            get;
            set;
        }
        public string BranchID
        {
            get;
            set;
        }
        private string sCity = string.Empty;
        public string City
        {
            get { return sCity; }
            set { sCity = value; }
        }

        private string sTelNo = string.Empty;
        public string TelNo
        {
            get { return sTelNo; }
            set { sTelNo = value; }
        }


        private string sFaxNo = string.Empty;
        public string FaxNo
        {
            get { return sFaxNo; }
            set { sFaxNo = value; }
        }
        private string sEmail = string.Empty;
        public string Email
        {
            get { return sEmail; }
            set { sEmail = value; }
        }


        private string sWebsite = string.Empty;
        public string Website
        {
            get { return sWebsite; }
            set { sWebsite = value; }
        }
        private string sContactPerson1 = string.Empty;
        public string ContactPerson1
        {
            get { return sContactPerson1; }
            set { sContactPerson1 = value; }
        }

        private string sCtPerDesig1 = string.Empty;
        public string CtPerDesig1
        {
            get { return sCtPerDesig1; }
            set { sCtPerDesig1 = value; }
        }

        private string sCtPerMobile1 = string.Empty;
        public string CtPerMobile1
        {
            get { return sCtPerMobile1; }
            set { sCtPerMobile1 = value; }
        }


        private string sCtPeremail1 = string.Empty;
        public string CtPeremail1
        {
            get { return sCtPeremail1; }
            set { sCtPeremail1 = value; }
        }

        private string sCtPeremail2 = string.Empty;
        public string CtPeremail2
        {
            get { return sCtPeremail2; }
            set { sCtPeremail2 = value; }
        }


        private string sContactPerson2 = string.Empty;
        public string ContactPerson2
        {
            get { return sContactPerson2; }
            set { sContactPerson2 = value; }
        }

        private string sCtPerDesig2 = string.Empty;
        public string CtPerDesig2
        {
            get { return sCtPerDesig2; }
            set { sCtPerDesig2 = value; }
        }

        private string sCtPerMobile2 = string.Empty;
        public string CtPerMobile2
        {
            get { return sCtPerMobile2; }
            set { sCtPerMobile2 = value; }
        }

        private string sBusinessType = string.Empty;
        public string BusinessType
        {
            get { return sBusinessType; }
            set { sBusinessType = value; }
        }

        private string sTrLisNo = string.Empty;
        public string LicenseNo
        {
            get { return sTrLisNo; }
            set { sTrLisNo = value; }
        }

        private double iCreditLimit = 0.0;
        public double CreditLimit
        {
            get { return iCreditLimit; }
            set { iCreditLimit = value; }
        }

        private double iCreditPeriod = 0.0;
        public double CreditPeriod
        {
            get { return System.Convert.ToDouble(iCreditPeriod); }
            set { iCreditPeriod = value; }
        }


        private string sCreditTerms = string.Empty;
        public string CreditTemrs
        {
            get { return sCreditTerms; }
            set { sCreditTerms = value; }
        }

        private string sBankCode1 = string.Empty;
        public string BankCode1
        {
            get { return sBankCode1; }
            set { sBankCode1 = value; }
        }

        private string sBankName1 = string.Empty;
        public string BankName1
        {
            get { return sBankName1; }
            set { sBankName1 = value; }
        }

        private string sBankAccountNo1 = string.Empty;
        public string BankAccountNo1
        {
            get { return sBankAccountNo1; }
            set { sBankAccountNo1 = value; }
        }

        private string sIBANNo1 = string.Empty;
        public string IBANNo1
        {
            get { return sIBANNo1; }
            set { sIBANNo1 = value; }
        }

        private string sSwiftCode1 = string.Empty;
        public string SwiftCode1
        {
            get { return sSwiftCode1; }
            set { sSwiftCode1 = value; }
        }

        private string sBankAddress1 = string.Empty;
        public string BankAddress1
        {
            get { return sBankAddress1; }
            set { sBankAddress1 = value; }
        }

        private string sBankCode2 = string.Empty;
        public string BankCode2
        {
            get { return sBankCode2; }
            set { sBankCode2 = value; }
        }

        private string sBankName2 = string.Empty;
        public string BankName2
        {
            get { return sBankName2; }
            set { sBankName2 = value; }
        }

        private string sBankAccountNo2 = string.Empty;
        public string BankAccountNo2
        {
            get { return sBankAccountNo2; }
            set { sBankAccountNo2 = value; }
        }

        private string sIBANNo2 = string.Empty;
        public string IBANNo2
        {
            get { return sIBANNo2; }
            set { sIBANNo2 = value; }
        }

        private string sSwiftCode2 = string.Empty;
        public string SwiftCode2
        {
            get { return sSwiftCode2; }
            set { sSwiftCode2 = value; }
        }

        private string sBankAddress2 = string.Empty;
        public string BankAddress2
        {
            get { return sBankAddress2; }
            set { sBankAddress2 = value; }
        }

        private string sRemarks = string.Empty;
        public string Remarks
        {
            get { return sRemarks; }
            set { sRemarks = value; }
        }

        public DateTime TRNDate
        {
            get;
            set;
        }
        private DateTime dExpDate = DateTime.Now;
        public DateTime ExpriyDate
        {
            get { return dExpDate; }
            set { dExpDate = value; }
        }

        public string InvQty
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
        public string EvalRemarks
        {
            get;
            set;
        }

        public string VATType
        {
            get;
            set;
        }
        public string VATRegistrationNumber
        {
            get;
            set;
        }

        public double VATPercentage
        {
            get;
            set;
        }
        public string sChkVarifiedYN { get; set; }
        public string sVarifiedby { get; set; }
        public string VerifiedbyTime { get; set; }

        public string ApprovedbyTime { get; set; }
        public string PrevApprovedbyTime { get; set; }

        public string Approved
        {
            get;
            set;
        }
        
        public string PrevApproved
        {
            get;
            set;
        }
        public string ApprovedEnabled
        {
            get;
            set;
        }

        public string ApprovedBy
        {
            get;
            set;
        } 
        
        public string PrevApprovedBy
        {
            get;
            set;
        }

    }



    public class VendorMasteCheckListGridEntity
    {

        public string CheckListYN { get; set; }
        public string CheckListCode { get; set; }
        public string CheckListValue { get; set; }
        public string CheckListRmk { get; set; }

    }

    #endregion

    #region " vendor  master "


    public class VendorMasterHistoryEntity
    {

        public VendorMasterHistoryEntity()
        {
            //
            // TODO: Add constructor logic here
            //
        }


        public string sVendorCode
        {
            get;
            set;
        }


        public string sVendorHisName
        {
            get;
            set;
        }


        public string sVendorHisFullName
        {
            get;
            set;
        }

        public string sChqName
        {
            get;
            set;
        }




        public int sSLNO
        {
            get;
            set;
        }


        public DateTime NameRepDate
        {
            get;
            set;
        }
    }

    public class VendorMasterHistoryGridEntity
    {
        public DateTime dNameRepDate
        {
            get;
            set;
        }

        public string sVendorCode
        {
            get;
            set;
        }

        public int sSLNO
        {
            get;
            set;
        }

        public string sVendorHisName
        {
            get;
            set;
        }

        public string sVendorHisFullName
        {
            get;
            set;
        }

        public string sChqName
        {
            get;
            set;
        }

    }

    public class VendorBranchMapppingEntity
    {
        public string mVendorBranchCode { get; set; }
        public string mSlNo { get; set; }
        public string mLableId { get; set; }
        public string mLableType { get; set; }
        public string mLableValue { get; set; }
    }
    #endregion

    #region SErch module " 
    public class VendorMasterSearchModel
    {

        public string fromdate { get; set; }
        public string todate { get; set; }
        public string DivCode { get; set; }
    }
    #endregion 

}