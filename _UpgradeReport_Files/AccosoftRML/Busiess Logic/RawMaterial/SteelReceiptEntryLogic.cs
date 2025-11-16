using System;
using System.Data;
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
using AccosoftUtilities;
using AccosoftLogWriter;
using AccosoftNineteenCDL;
using System.Collections.Generic;

//---------------------------------------------------------------

//------ CREATED BY      : NISHA ALOISIOUS                ------
//------ CREATED DATE    : 09-12-2015                     ------
//------ CREATED DETAILS : RM - STEEL RECEIPT ENTRY LOGIC ------ 

//--------------------------------------------------------------- 

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class SteelReceiptEntryLogic
    {
        static string sConString = Utilities.cnnstr;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public SteelReceiptEntryLogic ( )
        {
            //
            // TODO: Add constructor logic here
            //
        }

        #region " FUNCTIONS FOR FILL VIEW "

        public DataTable FillViewPurchaseOrder ( )
        {
            DataTable dt = new DataTable();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + "  RM_PO_PONO pcode,  to_char(RM_PO_MASTER.AD_FIN_FINYRID) id,";
                SQL = SQL + "  RM_PO_MASTER.RM_VM_VENDOR_CODE scode ,";
                SQL = SQL + "  RM_VENDOR_MASTER.RM_VM_VENDOR_NAME sname, ";
                SQL = SQL + "  RM_PO_DISCOUNT_PERC DISPER, to_char(RM_PO_NET_AMOUNT) NETAMOUNT,";
                SQL = SQL + "  to_char(RM_PO_PO_DATE,'DD-MON-YYYY') pdate,RM_PO_SUPP_REF SUPPREF, RM_PO_OUR_REF OURREF";

                SQL = SQL + " FROM ";
                SQL = SQL + "  RM_PO_MASTER, RM_VENDOR_MASTER   ";

                SQL = SQL + " WHERE ";
                SQL = SQL + "  RM_VENDOR_MASTER.RM_VM_VENDOR_CODE =   RM_PO_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + "  AND RM_PO_PO_STATUS = 'O'";
                SQL = SQL + "  AND RM_PO_APPROVED = 'Y'";
                SQL = SQL + "  AND RM_PO_RM_TYPE='ST' ";
                SQL = SQL + " ORDER BY RM_PO_PONO desc";

                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }

        public DataTable FillStationView ( object mngrclassobj )
        {
            DataTable dt = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                if (mngrclass.UserName == "ADMIN")
                {
                    SQL = "  SELECT ";
                    SQL = SQL + "   SALES_STN_STATION_CODE Code,SALES_STN_STATION_NAME Name  ";

                    SQL = SQL + "  FROM  ";
                    SQL = SQL + "   SL_STATION_MASTER WHERE  SALES_STN_DELETESTATUS=0";
                    SQL = SQL + "   AND SALES_RAW_MATERIAL_STATION ='Y' ";

                    SQL = SQL + "  ORDER BY SALES_STN_STATION_NAME ";
                }
                else
                {
                    SQL = "  SELECT ";
                    SQL = SQL + "   SALES_STN_STATION_CODE Code,SALES_STN_STATION_NAME Name  ";
                    SQL = SQL + " FROM  ";
                    SQL = SQL + "   SL_STATION_MASTER ";
                    SQL = SQL + " WHERE  ";
                    SQL = SQL + "   SALES_STN_DELETESTATUS=0";
                    SQL = SQL + "   AND  SALES_STN_STATION_CODE IN ( SELECT     SALES_STN_STATION_CODE FROM    WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + mngrclass.UserName + "')";
                    SQL = SQL + "   AND SALES_RAW_MATERIAL_STATION ='Y' ";

                    SQL = SQL + "  ORDER BY SALES_STN_STATION_NAME ";
                }

                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }

        public DataTable FillViewReceiptDetSearch ( string fromdate, string todate, string sFilterType, object mngrclassobj )
        {
            DataTable dt = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT ";
                SQL = SQL + "   DISTINCT RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO pcode, to_char(RM_RECEIPT_MASTER.AD_FIN_FINYRID) Id, ";
                SQL = SQL + "   to_char(RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY')  PDate, RM_RECEIPT_MASTER.RM_MPOM_ORDER_NO PONO,";
                SQL = SQL + "   RM_RECEIPT_MASTER.RM_VM_VENDOR_CODE  Vendorcode,RM_RECEIPT_MASTER.RM_MRM_APPROVED_STATUS, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SName, ";
                SQL = SQL + "   RM_RECEPIT_DETAILS.RM_MRM_VEHICLE_DESCRIPTION scode , ";
                SQL = SQL + "   RM_MRD_VENDOR_DOC_NO,to_char( RM_MRM_GRAND_TOTAL) RM_MRM_FINAL_TOTAL_AMOUNT ,RM_MRD_VENDOR_DOC_NO Supp_Doc_No,";
                SQL = SQL + " RM_MRM_VEHICLE_DESCRIPTION Vehicle_No ,DECODE(RM_MRM_RECEIPT_VERIFIED,'Y','YES','NO')VerifiedYN,DECODE(RM_MRM_RECEIPT_APPROVED,'Y','YES','NO') ApprovedYN ";

                SQL = SQL + " FROM ";
                SQL = SQL + "   RM_RECEIPT_MASTER, RM_VENDOR_MASTER, RM_RECEPIT_DETAILS ";

                SQL = SQL + " WHERE  ";
                SQL = SQL + "   RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO = RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO";
                SQL = SQL + "   AND RM_RECEIPT_MASTER.AD_FIN_FINYRID=RM_RECEPIT_DETAILS.AD_FIN_FINYRID  ";
                SQL = SQL + "   AND RM_RECEIPT_MASTER.RM_MPOM_ORDER_NO = RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO  ";
                SQL = SQL + "   AND RM_RECEIPT_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "   AND RM_RECEIPT_MASTER.AD_FIN_FINYRID =" + mngrclass.FinYearID + "";
                SQL = SQL + "   AND RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "   AND RM_RECEIPT_MASTER.RM_PO_RM_TYPE ='ST'";
                if (sFilterType == "NOT APPROVED")
                {
                    SQL = SQL + " AND RM_RECEIPT_MASTER.RM_MRM_APPROVED_STATUS ='N'";
                }
                else if (sFilterType == "APPROVED")
                {
                    SQL = SQL + " AND RM_RECEIPT_MASTER.RM_MRM_APPROVED_STATUS ='Y'";
                }

                SQL = SQL + "   ORDER BY  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO desc";

                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }


        public DataTable FillViewVendor ( )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT   ";
                SQL = SQL + " RM_VM_VENDOR_CODE CODE ,";
                SQL = SQL + "RM_VM_VENDOR_NAME NAME ";
                SQL = SQL + " FROM RM_VENDOR_MASTER";
                SQL = SQL + " ORDER BY RM_VM_VENDOR_CODE asc ";

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

        public DataTable FillViewOtherItemsDirectExp ( )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_RMM_RM_CODE                            CODE, ";
                SQL = SQL + "         RM_RMM_RM_DESCRIPTION                     NAME, ";
                SQL = SQL + "         RM_UOM_MASTER.RM_UM_UOM_DESC, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE     UOM_CODE ";
                SQL = SQL + "    FROM RM_RAWMATERIAL_MASTER, RM_UOM_MASTER ";
                SQL = SQL + "   WHERE     RM_RAWMATERIAL_MASTER.RM_RMM_PRODUCT_TYPE IN ('OTHER', 'TOLL') ";
                SQL = SQL + "         AND RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE = ";
                SQL = SQL + "             RM_UOM_MASTER.RM_UM_UOM_CODE(+) ";
                SQL = SQL + "ORDER BY RM_RMM_RM_CODE ASC ";



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


        public DataTable FillViewStation ( string Branch )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE, SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME, ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE Code  , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME Name  , ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.GL_COSM_ACCOUNT_CODE ,";
                SQL = SQL + "   '' Id   ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC,''BUDGET_AMOUNT ";
                SQL = SQL + " FROM ";
                SQL = SQL + "   SL_STATION_BRANCH_MAPP_DTLS_VW WHERE   ";
                SQL = SQL + "    SALES_STATION_BRANCH_STATUS_AI ='A' and SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_RAW_MATERIAL_STATION ='Y'    ";
                SQL = SQL + "  and  (AD_BR_READYMIX_VISIBILE_YN = 'Y' OR AD_BR_BLOCK_VISIBILE_YN = 'Y' OR AD_BR_PRECAST_VISIBILE_YN = 'Y' ) ";

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + "   AND SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE ='" + Branch + "'";
                }


                SQL = SQL + "  ORDER BY   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE , SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_SORT_ID  , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE   ASC  ";

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
        public DataTable FillViewSource ( )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + "RM_SM_SOURCE_CODE Code , RM_SM_SOURCE_DESC Name ,'' Id ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC,''BUDGET_AMOUNT ";
                SQL = SQL + "FROM RM_SOURCE_MASTER ";
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

        #region " FUNCTIONS FOR FETCH DATA "

        public DataSet FetchReceiptData ( string DocNo, string iFinYear )
        {
            DataSet dt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + "    RM_MRM_RECEIPT_NO, RM_RECEIPT_MASTER.AD_CM_COMPANY_CODE, RM_RECEIPT_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL + "    RM_MRM_RECEIPT_DATE, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, ";
                SQL = SQL + "    RM_RECEIPT_MASTER.RM_VM_VENDOR_CODE SUPP_CODE,RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPP_NAME,";
                SQL = SQL + "    RM_MRM_REMARKS, RM_MRM_GRAND_TOTAL,";
                SQL = SQL + "    RM_MRM_APPROVED_STATUS, RM_MPOM_PRICE_TYPE, RM_MRM_CREATEDBY, ";
                SQL = SQL + "    RM_MRM_CREATEDDATE, RM_MRM_UPDATEDBY, RM_MRM_UPDATEDDATE, ";
                SQL = SQL + "    RM_MRM_DELETESTATUS,RM_MRM_STOCK_YN ,RM_PO_RM_TYPE ,";
                SQL = SQL + " RM_RECEIPT_MASTER.AD_BR_CODE, AD_BRANCH_MASTER.AD_BR_NAME, ";
                SQL = SQL + "    RM_RECEIPT_MASTER.RM_MRM_RECEIPT_VERIFIED, RM_RECEIPT_MASTER.RM_MRM_RECEIPT_VERIFIED_BY, ";
                SQL = SQL + "    RM_RECEIPT_MASTER.RM_MRM_RECEIPT_APPROVED, RM_RECEIPT_MASTER.RM_MRM_RECEIPT_APPROVE_BY,  ";
                SQL = SQL + "    RM_RECEIPT_MASTER.RM_MRM_RECEIPT_VERIFIEDDT,RM_RECEIPT_MASTER.RM_MRM_RECEIPT_APPROVEDDT,RM_MRM_RECEIPT_DATE_TIME,RM_PO_RATE_REVISION_APP_YN ";

                SQL = SQL + " FROM ";
                SQL = SQL + "    RM_RECEIPT_MASTER,RM_VENDOR_MASTER,AD_BRANCH_MASTER ";

                SQL = SQL + " WHERE ";
                SQL = SQL + "    RM_MRM_RECEIPT_NO ='" + DocNo + "' and  AD_FIN_FINYRID =" + iFinYear + "";
                SQL = SQL + "    AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE =  RM_RECEIPT_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + "    AND RM_RECEIPT_MASTER.AD_BR_CODE =  AD_BRANCH_MASTER.AD_BR_CODE(+)";

                dt = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }

        public DataSet FetchReceiptGridDetails ( string DocNo, int FINYEAR, int PoFinYrId )
        {
            DataSet dt = new DataSet();
            DataTable dtMaster = new DataTable();
            DataTable dtTollDetails = new DataTable();
            DataTable dtTollDetailWithOUTLPO = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + "  RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_MRM_RECEIPT_DATE, RM_MPOM_ORDER_NO,";
                SQL = SQL + "  RM_MPOM_FIN_FINYRID, RM_POD_SL_NO,  RM_MPOM_PRICE_TYPE, RM_MRD_SL_NO,";
                SQL = SQL + "  RM_RECEPIT_DETAILS.AD_CM_COMPANY_CODE,";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME, ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + "  rm_recepit_details.HR_DEPT_DEPT_CODE,";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_UM_UOM_CODE,RM_UOM_MASTER.RM_UM_UOM_DESC, ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE,RM_SOURCE_MASTER.RM_SM_SOURCE_DESC,";
                SQL = SQL + "  RM_INTERNALOREXTENAL,RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME,";
                SQL = SQL + "  RM_MRD_VENDOR_DOC_NO, RM_MRD_ORDER_QTY, RM_MRD_SUPPLY_QTY,";
                SQL = SQL + "  RM_MRD_WEIGH_QTY, RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY,";
                SQL = SQL + "  RM_MRD_SUPP_UNIT_PRICE, RM_MRD_SUPP_AMOUNT,";
                SQL = SQL + "  RM_MRD_APPROVED, RM_MRM_APPROVED_NO,";
                SQL = SQL + "  RM_MRD_APPROVED_FINYRID, RM_MRD_SUPP_ENTRY, RM_MRD_SUPP_PENO,";
                SQL = SQL + "  RM_MPED_VFINYR_ID, RM_MRD_TRANS_ENTRY, RM_MRD_TRANS_PENO,";
                SQL = SQL + "  RM_MRM_RECEIPT_UNIQE_NO ,";
                SQL = SQL + "  RM_RECEPIT_DETAILS.TECH_PLM_PLANT_CODE,TECH_PLM_PLANT_NAME,RM_MRD_REJECTED_YN  ,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_PO_UOM_TYPE,RM_RECEPIT_DETAILS.RM_VM_INVOICE_QTY,RM_MRM_STOCK_YN ,";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_VAT_TYPE_CODE, RM_RECEPIT_DETAILS.RM_MRM_SUPP_VAT_PERCENTAGE";

                SQL = SQL + " FROM ";
                SQL = SQL + "  RM_RECEPIT_DETAILS,RM_RAWMATERIAL_MASTER,RM_UOM_MASTER,SL_STATION_MASTER,";
                SQL = SQL + "  RM_SOURCE_MASTER,RM_VENDOR_MASTER,TECH_PLANT_MASTER";

                SQL = SQL + " WHERE ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + "  AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE";
                SQL = SQL + "  AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + "  AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                SQL = SQL + "  AND RM_RECEPIT_DETAILS.TECH_PLM_PLANT_CODE = TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE";
                SQL = SQL + "  AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE =SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + "  AND  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ='" + DocNo + "' AND  RM_RECEPIT_DETAILS.AD_FIN_FINYRID =" + FINYEAR + "";

                SQL = SQL + " ORDER BY  RM_MRD_SL_NO ASC  ";

                dtMaster = clsSQLHelper.GetDataTableByCommand(SQL);
                dt.Tables.Add(dtMaster);

                SQL = "SELECT  ";
                SQL = SQL + "RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE, RM_VM_VENDOR_NAME,RM_MPOM_ORDER_NO,";
                SQL = SQL + "   RM_MRM_TOLL_NO, RM_MRM_TOLL_RATE, RM_MRM_TOLL_AMOUNT,RM_RECEPIT_TOLL.RM_TL_LOCATION_CODE,  ";
                SQL = SQL + "   RM_TL_LOCATION_DESC,RM_MRM_TOLL_PE_DONE_YN, RM_MRM_TOLL_PE_NO,RM_MRM_TOLL_Date , ";
                SQL = SQL + "   RM_RECEPIT_TOLL.SALES_STN_STATION_CODE,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE,RM_SM_SOURCE_DESC,";
                SQL = SQL + "  RM_RECEPIT_TOLL.RM_RMM_RM_CODE,RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_UOM_UOM_CODE ,RM_UM_UOM_DESC, RM_MRM_TOLL_QTY,RM_MRM_VAT_TYPE_CODE, RM_MRM_VAT_PERCENTAGE ";
                SQL = SQL + "FROM RM_RECEPIT_TOLL ,RM_VENDOR_MASTER,RM_RECEPIT_TOLL_LOCATION_MAST , ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER ,RM_SOURCE_MASTER,RM_UOM_MASTER ";
                SQL = SQL + "    where ";
                SQL = SQL + "    RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE=RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_UOM_UOM_CODE=RM_UOM_MASTER.RM_UM_UOM_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_TL_LOCATION_CODE =RM_RECEPIT_TOLL_LOCATION_MAST.RM_TL_LOCATION_CODE(+)  ";
                SQL = SQL + "  AND  RM_MRM_RECEIPT_NO ='" + DocNo + "' and  AD_FIN_FINYRID =" + FINYEAR + "  and RM_MRM_TOLL_ENTRYTYPE='WITHLPO' ";
                dtTollDetails = clsSQLHelper.GetDataTableByCommand(SQL);
                dt.Tables.Add(dtTollDetails);

                SQL = "SELECT   ";
                SQL = SQL + "RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE, RM_VM_VENDOR_NAME,RM_MPOM_ORDER_NO, ";
                SQL = SQL + "   RM_MRM_TOLL_NO, RM_MRM_TOLL_RATE, RM_MRM_TOLL_AMOUNT,RM_RECEPIT_TOLL.RM_TL_LOCATION_CODE,   ";
                SQL = SQL + "   RM_TL_LOCATION_DESC,RM_MRM_TOLL_PE_DONE_YN, RM_MRM_TOLL_PE_NO,RM_MRM_TOLL_Date ,  ";
                SQL = SQL + "   RM_RECEPIT_TOLL.SALES_STN_STATION_CODE,SL_STATION_MASTER.SALES_STN_STATION_NAME,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE,RM_SM_SOURCE_DESC, ";
                SQL = SQL + "  RM_RECEPIT_TOLL.RM_RMM_RM_CODE,RM_RMM_RM_DESCRIPTION,  ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_UOM_UOM_CODE ,RM_UM_UOM_DESC, RM_MRM_TOLL_QTY,RM_MRM_VAT_TYPE_CODE, RM_MRM_VAT_PERCENTAGE   ";
                SQL = SQL + "FROM RM_RECEPIT_TOLL ,RM_VENDOR_MASTER,RM_RECEPIT_TOLL_LOCATION_MAST ,  ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER ,RM_SOURCE_MASTER,RM_UOM_MASTER,SL_STATION_MASTER ";
                SQL = SQL + "    where  ";
                SQL = SQL + "    RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+) ";
                SQL = SQL + "    AND  RM_RECEPIT_TOLL.SALES_STN_STATION_CODE=SL_STATION_MASTER.SALES_STN_STATION_CODE(+)    ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE(+)   ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE=RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+)   ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_UOM_UOM_CODE=RM_UOM_MASTER.RM_UM_UOM_CODE(+)   ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_TL_LOCATION_CODE =RM_RECEPIT_TOLL_LOCATION_MAST.RM_TL_LOCATION_CODE(+)  ";
                SQL = SQL + "  AND  RM_MRM_RECEIPT_NO ='" + DocNo + "' and  AD_FIN_FINYRID =" + FINYEAR + "  and RM_MRM_TOLL_ENTRYTYPE='WITHOUTLPO' ";
                dtTollDetailWithOUTLPO = clsSQLHelper.GetDataTableByCommand(SQL);
                dt.Tables.Add(dtTollDetailWithOUTLPO);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }

        public DataSet FetchPOData ( string pono, string id )
        {
            DataSet dt = new DataSet();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + "  RM_PO_PONO PONO, RM_PO_MASTER.AD_FIN_FINYRID FINYR,";
                SQL = SQL + "  RM_PO_MASTER.RM_VM_VENDOR_CODE SUPPCODE ,";
                SQL = SQL + "  RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIERNAME, ";
                SQL = SQL + "  RM_PO_DISCOUNT_PERC DISPER, RM_PO_NET_AMOUNT NETAMOUNT,";
                SQL = SQL + "  RM_PO_PO_DATE ,RM_PO_SUPP_REF SUPPREF, RM_PO_OUR_REF OURREF,RM_PO_RM_TYPE,";
                SQL = SQL + " RM_PO_MASTER.AD_BR_CODE, AD_BRANCH_MASTER.AD_BR_NAME,RM_PO_RATE_REVISION_APP_YN ";

                SQL = SQL + " FROM ";
                SQL = SQL + "   RM_PO_MASTER, RM_VENDOR_MASTER,AD_BRANCH_MASTER   ";

                SQL = SQL + " WHERE ";
                SQL = SQL + "   RM_VENDOR_MASTER.RM_VM_VENDOR_CODE =   RM_PO_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + "   AND RM_PO_MASTER.AD_BR_CODE =   AD_BRANCH_MASTER.AD_BR_CODE(+)";
                SQL = SQL + "   AND RM_PO_PO_STATUS = 'O'";
                SQL = SQL + "   AND RM_PO_APPROVED = 'Y'";
                SQL = SQL + "   AND RM_PO_MASTER.RM_PO_PONO = '" + pono + "' AND RM_PO_MASTER.AD_FIN_FINYRID = " + id + "";

                SQL = SQL + " ORDER BY RM_PO_PONO DESC";

                dt = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }

        public DataSet FetchPOGridData ( string pono, string id )
        {
            DataSet dt = new DataSet();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + "   RM_POD_SL_NO SL_NO,RM_PO_DETAILS.RM_RMM_RM_CODE ITEM_CODE,RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION ITEM_DESC , ";
                SQL = SQL + "   RM_IM_ITEM_DTL_DESCRIPTION DTLDESC,RM_PO_DETAILS.HR_DEPT_DEPT_CODE,HR_DEPT_MASTER.HR_DEPT_DEPT_DESC,";
                SQL = SQL + "   RM_PO_DETAILS.RM_SM_SOURCE_CODE , RM_SOURCE_MASTER.RM_SM_SOURCE_DESC ,";
                SQL = SQL + "   RM_PO_DETAILS.RM_UOM_UOM_CODE UOM_CODE,RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC,";
                SQL = SQL + "   RM_POD_QTY ORD_QTY,RM_POD_PENDING_QTY PND_QTY,RM_POD_UNIT_PRICE PO_UNIT_PRICE, ";
                SQL = SQL + "  RM_PO_DETAILS.SALES_STN_STATION_CODE STATION_CODE, SALES_STN_STATION_NAME STATION_NAME,RM_POD_RM_VAT_TYPE,RM_POD_RM_VAT_RATE";

                SQL = SQL + " FROM ";
                SQL = SQL + "   RM_PO_DETAILS, RM_RAWMATERIAL_MASTER, RM_UOM_MASTER,HR_DEPT_MASTER , RM_SOURCE_MASTER,SL_STATION_MASTER ";

                SQL = SQL + " WHERE ";
                SQL = SQL + "   RM_PO_PONO='" + pono + "' AND AD_FIN_FINYRID=" + id + "";
                SQL = SQL + "   AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE (+)";
                SQL = SQL + "   AND RM_PO_DETAILS.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE (+)";
                SQL = SQL + "   AND RM_PO_DETAILS.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE (+)";
                SQL = SQL + "   AND RM_PO_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE (+)";
                SQL = SQL + "   AND RM_PO_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE (+)";
                SQL = SQL + "  And  RM_RMM_RM_TYPE IN ('STEEL','WIRE MESH','OTHER STEEL')  ";
                SQL = SQL + " ORDER BY RM_POD_SL_NO";

                dt = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }

        public DataTable FillViewRM ( string PoNum, string Branch, string SelectedRM = "" )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT   RM_PO_MASTER.RM_PO_PONO ORDER_NO,  RM_PO_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";
                SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE,RM_VENDOR_MASTER.RM_VM_VENDOR_NAME ,";

                SQL = SQL + " RM_PO_DETAILS.RM_RMM_RM_CODE RM_CODE ,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_NAME ,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC , ";
                SQL = SQL + " RM_PO_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_NAME,";
                SQL = SQL + " RM_PO_DETAILS.RM_UOM_UOM_CODE UOM_CODE,RM_POD_RM_VAT_TYPE,RM_POD_RM_VAT_RATE, ";
                SQL = SQL + " RM_POD_QTY ,RM_POD_UNIT_PRICE, RM_POD_NEWPRICE ,  ";
                SQL = SQL + "  RM_PO_DETAILS.RM_UOM_TOLL_CODE ,  RM_PO_DETAILS.RM_POD_TOLL_RATE ";

                SQL = SQL + " FROM RM_PO_MASTER,";
                SQL = SQL + " RM_PO_DETAILS,";
                SQL = SQL + " SL_STATION_MASTER,RM_UOM_MASTER,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER , RM_SOURCE_MASTER , RM_VENDOR_MASTER";

                SQL = SQL + " WHERE  ";
                SQL = SQL + "  RM_PO_DETAILS.RM_SM_SOURCE_CODE =  RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " AND RM_PO_DETAILS.RM_UOM_UOM_CODE  =  RM_UOM_MASTER.RM_UM_UOM_CODE(+) ";
                SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE =  SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+) ";
                SQL = SQL + " AND RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO ";
                SQL = SQL + " AND RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID";

                SQL = SQL + " AND RM_PO_DETAILS.RM_PO_PONO ='" + PoNum + "'";
                //SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE='" + StnCode + "'";
                SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE='" + Branch + "'";
                SQL = SQL + " And RM_RAWMATERIAL_MASTER.RM_RMM_PRODUCT_TYPE IN ('OTHER','TOLL')";

                if (!string.IsNullOrEmpty(SelectedRM))
                {
                    SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE||RM_PO_MASTER.RM_PO_PONO ORDER_NO|| ";
                    SQL = SQL + " RM_PO_DETAILS.SALES_STN_STATION_CODE ||RM_PO_DETAILS.RM_SM_SOURCE_CODE ";
                    SQL = SQL + " NOT IN ('" + SelectedRM.Replace(",", "','") + "')";
                }

                SQL = SQL + " ORDER BY RM_PO_DETAILS.RM_RMM_RM_CODE  asc ";


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

        public DataTable FillTollViewRM ( string PoNum, string Branch, string SelectedRM = "" )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT   RM_PO_MASTER.RM_PO_PONO ORDER_NO,  RM_PO_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";
                SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE,RM_VENDOR_MASTER.RM_VM_VENDOR_NAME ,";

                SQL = SQL + " RM_PO_DETAILS.RM_RMM_RM_CODE RM_CODE ,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_NAME ,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC , ";
                SQL = SQL + " RM_PO_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_NAME,";
                SQL = SQL + " RM_PO_DETAILS.RM_UOM_UOM_CODE UOM_CODE,RM_POD_RM_VAT_TYPE,RM_POD_RM_VAT_RATE, ";
                SQL = SQL + " RM_POD_QTY ,RM_POD_UNIT_PRICE, RM_POD_NEWPRICE ,  ";
                SQL = SQL + "  RM_PO_DETAILS.RM_UOM_TOLL_CODE ,  RM_PO_DETAILS.RM_POD_TOLL_RATE ";

                SQL = SQL + " FROM RM_PO_MASTER,";
                SQL = SQL + " RM_PO_DETAILS,";
                SQL = SQL + " SL_STATION_MASTER,RM_UOM_MASTER,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER , RM_SOURCE_MASTER , RM_VENDOR_MASTER";

                SQL = SQL + " WHERE  ";
                SQL = SQL + "  RM_PO_DETAILS.RM_SM_SOURCE_CODE =  RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " AND RM_PO_DETAILS.RM_UOM_UOM_CODE  =  RM_UOM_MASTER.RM_UM_UOM_CODE(+) ";
                SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE =  SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+) ";
                SQL = SQL + " AND RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO ";
                SQL = SQL + " AND RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID";

                SQL = SQL + " AND RM_PO_DETAILS.RM_PO_PONO ='" + PoNum + "'";
                //SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE='" + StnCode + "'";
                SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE='" + Branch + "'";
                SQL = SQL + " And RM_RAWMATERIAL_MASTER.RM_RMM_PRODUCT_TYPE IN ('OTHER','TOLL')";

                if (!string.IsNullOrEmpty(SelectedRM))
                {
                    SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE||RM_PO_MASTER.RM_PO_PONO ORDER_NO|| ";
                    SQL = SQL + " RM_PO_DETAILS.SALES_STN_STATION_CODE ||RM_PO_DETAILS.RM_SM_SOURCE_CODE ";
                    SQL = SQL + " NOT IN ('" + SelectedRM.Replace(",", "','") + "')";
                }

                SQL = SQL + " ORDER BY RM_PO_DETAILS.RM_RMM_RM_CODE  asc ";


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

        #region " COMMON GET FUNCTIONS "

        public DataTable GetStationName ( string StnCode, object mngrclassobj )
        {
            DataTable dt = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "SELECT ";
                SQL = SQL + "   SALES_STN_STATION_NAME ";

                SQL = SQL + " FROM ";
                SQL = SQL + "   SL_STATION_MASTER ";

                SQL = SQL + " WHERE ";
                SQL = SQL + "   AD_CM_COMPANY_CODE='" + mngrclass.CompanyCode + "' AND SALES_STN_DELETESTATUS=0 AND SALES_STN_STATION_CODE='" + StnCode + "'";

                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }

        public DataTable GetDepartmentName ( string DeptCode, object mngrclassobj )
        {
            DataTable dt = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "SELECT ";
                SQL = SQL + "   HR_DEPT_DEPT_DESC ";

                SQL = SQL + " FROM ";
                SQL = SQL + "   HR_DEPT_MASTER ";

                SQL = SQL + " WHERE ";
                SQL = SQL + "   AD_CM_COMPANY_CODE='" + mngrclass.CompanyCode + "' AND HR_DEPT_DELETESTATUS=0 AND HR_DEPT_DEPT_CODE='" + DeptCode + "'";

                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }

        public DataSet getPendingQty ( string PONo, string ItemCode, string POFinYrId )
        {
            DataSet dt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + "   RM_POD_PENDING_QTY";

                SQL = SQL + " FROM ";
                SQL = SQL + "   RM_PO_DETAILS";

                SQL = SQL + " WHERE   ";
                SQL = SQL + "   RM_PO_PONO ='" + PONo + "' AND AD_FIN_FINYRID = " + int.Parse(POFinYrId) + " AND RM_RMM_RM_CODE ='" + ItemCode + "'";

                dt = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }

        public DataSet getOrdQty ( string PONo, string ItemCode, string POFinYrId )
        {
            DataSet dt = new DataSet();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + "   RM_POD_QTY";

                SQL = SQL + " FROM ";
                SQL = SQL + "   RM_PO_DETAILS";

                SQL = SQL + " WHERE  ";
                SQL = SQL + "   RM_PO_PONO ='" + PONo + "' AND AD_FIN_FINYRID = " + int.Parse(POFinYrId) + " AND RM_RMM_RM_CODE ='" + ItemCode + "'";

                dt = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }

        public DataSet UOMData ( )
        {
            DataSet dt = new DataSet();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + "   rm_um_uom_code uom_code, rm_um_uom_desc uom_desc ";

                SQL = SQL + " FROM ";
                SQL = SQL + "   rm_uom_master ";

                SQL = SQL + " WHERE ";
                SQL = SQL + "   rm_um_deletestatus = 0 ";

                dt = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }

        public DataSet getDefaultDepartmentRM ( )
        {
            DataSet dt = new DataSet();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = SQL + " SELECT   ";
                SQL = SQL + "    RM_IP_PARAMETER_VALUE HR_DEPT_DEPT_CODE , HR_DEPT_DEPT_DESC   ";

                SQL = SQL + " FROM ";
                SQL = SQL + "   RM_DEFUALTS_GL_PARAMETERS ,HR_DEPT_MASTER ";

                SQL = SQL + " WHERE ";
                SQL = SQL + "   RM_DEFUALTS_GL_PARAMETERS . RM_IP_PARAMETER_VALUE =  HR_DEPT_MASTER . HR_DEPT_DEPT_CODE  ";
                SQL = SQL + "   AND RM_IP_PARAMETER_DESC='DEPARTMENT' ";

                dt = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }

        public DataSet getDefaultStationRM ( )
        {
            DataSet dt = new DataSet();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  SELECT ";
                SQL = SQL + "    RM_IP_PARAMETER_VALUE SALES_STN_STATION_CODE , SALES_STN_STATION_NAME    ";

                SQL = SQL + " FROM ";
                SQL = SQL + "   RM_DEFUALTS_GL_PARAMETERS  ,    SL_STATION_MASTER    ";

                SQL = SQL + " WHERE ";
                SQL = SQL + "   RM_DEFUALTS_GL_PARAMETERS . RM_IP_PARAMETER_VALUE =  SL_STATION_MASTER . SALES_STN_STATION_CODE   ";
                SQL = SQL + "   AND RM_IP_PARAMETER_DESC='STATION'";

                dt = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dt;
        }

        #endregion

        #region " INSERT / UPDATE / DELETE "

        public string InsertSQL ( SteelReceiptEntryEntity oEntity, List<SteelReceiptGridDetailsEntity> objRMGridEntity, List<SteelReceiptEntryRmTollGridEntity> objTollGridEntity,
            object mngrclassobj, bool bDocAutogenerated, string gtEntryNo, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                DateTime dReceiptTime = System.Convert.ToDateTime(oEntity.EntryDate.ToString("dd-MMM-yyyy") + " " + oEntity.EntryTime);

                sSQLArray.Clear();

                SQL = " INSERT INTO RM_RECEIPT_MASTER (";
                SQL = SQL + " RM_MRM_RECEIPT_NO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                SQL = SQL + " RM_MRM_RECEIPT_DATE, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, ";
                SQL = SQL + " RM_VM_VENDOR_CODE, RM_MRM_REMARKS, RM_MRM_GRAND_TOTAL, ";
                SQL = SQL + " RM_MRM_APPROVED_STATUS, RM_MPOM_PRICE_TYPE, RM_MRM_CREATEDBY, ";
                SQL = SQL + " RM_MRM_CREATEDDATE, RM_MRM_UPDATEDBY, RM_MRM_UPDATEDDATE, ";
                SQL = SQL + " RM_MRM_RECEIPT_VERIFIED ,RM_MRM_RECEIPT_VERIFIED_BY,RM_MRM_RECEIPT_VERIFIEDDT,RM_MRM_RECEIPT_APPROVED,RM_MRM_RECEIPT_APPROVE_BY,RM_MRM_RECEIPT_APPROVEDDT  ,";
                SQL = SQL + " RM_MRM_DELETESTATUS,RM_MRM_STOCK_YN ,RM_PO_RM_TYPE,AD_BR_CODE,RM_MRM_RECEIPT_DATE_TIME,RM_PO_RATE_REVISION_APP_YN) ";

                SQL = SQL + " VALUES ('" + oEntity.EntryNo + "', '" + mngrclass.CompanyCode + "'," + mngrclass.FinYearID + " ,";
                SQL = SQL + "'" + oEntity.EntryDate.ToString("dd-MMM-yyyy") + "'  ,'" + oEntity.OrderNo + "' ,  " + oEntity.orderfinYrId + " ,";
                SQL = SQL + "'" + oEntity.SupplierCode + "' ,'" + oEntity.Remarks + "' ," + Convert.ToDouble(oEntity.TotalAmount) + " ,";
                SQL = SQL + "'N' ,'" + oEntity.PriceType + "' ,'" + mngrclass.UserName + "' ,";
                SQL = SQL + "TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM') ,'' ,'', ";
                SQL = SQL + "'" + oEntity.Verified + "','" + oEntity.VerifiedBy + "', TO_DATE('" + oEntity.VerifiedTime + "', 'DD-MM-YYYY HH:MI:SS AM'),'" + oEntity.Approved + "','" + oEntity.ApprovedBy + "',TO_DATE('" + oEntity.ApprovedTime + "', 'DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + "  0 ,'" + oEntity.TempStock + "','" + oEntity.PoReceiptType + "','" + oEntity.Branch + "',";
                SQL = SQL + "  TO_DATE('" + dReceiptTime.ToString("dd-MMM-yyyy hh:mm:ss tt") + "', 'DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + " '" + oEntity.RateRevisionApplicable + "'   )";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertDetailsSQL(oEntity, objRMGridEntity, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }


                SQL = " INSERT INTO RM_RECEIPT_MASTER_trigger  (";

                SQL = SQL + " RM_MRM_RECEIPT_NO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID,RM_MRM_RECEIPT_DATE )";

                SQL = SQL + " VALUES( ";

                SQL = SQL + "'" + oEntity.EntryNo + "', '" + mngrclass.CompanyCode + "'," + mngrclass.FinYearID + " ,'" + oEntity.EntryDate.ToString("dd-MMM-yyyy") + "' ";

                SQL = SQL + ")";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertTollDetailsSQL(oEntity.EntryNo, Convert.ToInt16(mngrclass.FinYearID), objTollGridEntity, "I");
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }
                sRetun = string.Empty;
                //  sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRC", gtEntryNo, bDocAutogenerated, Environment.MachineName, "I", sSQLArray);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRC", gtEntryNo, bDocAutogenerated, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);


            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);

                return sRetun;
            }

            return sRetun;
        }

        public string UpdateSQL ( SteelReceiptEntryEntity oEntity, List<SteelReceiptGridDetailsEntity> objRMGridEntity, List<SteelReceiptEntryRmTollGridEntity> objTollGridEntity, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                DateTime dReceiptTime = System.Convert.ToDateTime(oEntity.EntryDate.ToString("dd-MMM-yyyy") + " " + oEntity.EntryTime);
                sSQLArray.Clear();

                SQL = " DELETE FROM  RM_RECEIPT_MASTER_trigger ";
                SQL = SQL + " WHERE RM_MRM_RECEIPT_NO = '" + oEntity.EntryNo + "' AND  AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);

                SQL = " UPDATE  RM_RECEIPT_MASTER SET ";
                SQL = SQL + "   RM_MRM_RECEIPT_DATE = '" + oEntity.EntryDate.ToString("dd-MMM-yyyy") + "', ";
                SQL = SQL + "  RM_MRM_RECEIPT_DATE_TIME = TO_DATE('" + dReceiptTime.ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM') ,";
                SQL = SQL + "   RM_MRM_REMARKS='" + oEntity.Remarks + "' ,  RM_MRM_UPDATEDBY = '" + mngrclass.UserName + "' , ";
                SQL = SQL + "   RM_MRM_UPDATEDDATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " RM_MRM_RECEIPT_VERIFIED ='" + oEntity.Verified + "' ,RM_MRM_RECEIPT_VERIFIED_BY = '" + oEntity.VerifiedBy + "',RM_MRM_RECEIPT_VERIFIEDDT = TO_DATE('" + oEntity.VerifiedTime + "', 'DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " RM_MRM_RECEIPT_APPROVED = '" + oEntity.Approved + "' , RM_MRM_RECEIPT_APPROVE_BY = '" + oEntity.ApprovedBy + "', RM_MRM_RECEIPT_APPROVEDDT= TO_DATE('" + oEntity.ApprovedTime + "', 'DD-MM-YYYY HH:MI:SS AM')";


                SQL = SQL + " WHERE  ";
                SQL = SQL + "   RM_MRM_RECEIPT_NO = '" + oEntity.EntryNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM  RM_RECEPIT_DETAILS ";
                SQL = SQL + " WHERE RM_MRM_RECEIPT_NO = '" + oEntity.EntryNo + "' AND  AD_FIN_FINYRID = " + mngrclass.FinYearID + " AND RM_MRD_APPROVED ='N'";

                sSQLArray.Add(SQL);

                sRetun = InsertDetailsSQL(oEntity, objRMGridEntity, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                SQL = " INSERT INTO RM_RECEIPT_MASTER_trigger  (";

                SQL = SQL + " RM_MRM_RECEIPT_NO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID,RM_MRM_RECEIPT_DATE )";

                SQL = SQL + " VALUES( ";

                SQL = SQL + "'" + oEntity.EntryNo + "', '" + mngrclass.CompanyCode + "'," + mngrclass.FinYearID + " ,'" + oEntity.EntryDate.ToString("dd-MMM-yyyy") + "' ";

                SQL = SQL + ")";

                sSQLArray.Add(SQL);

                sRetun = InsertTollDetailsSQL(oEntity.EntryNo, Convert.ToInt16(mngrclass.FinYearID), objTollGridEntity, "U");
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }
                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRC", oEntity.EntryNo, false, Environment.MachineName, "U", sSQLArray);
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

        public string DeleteSql ( string EntryNo, object mngrclassobj )
        {
            string sRetun = string.Empty;

            OracleHelper oTrns = new OracleHelper();
            SessionManager mngrclass = (SessionManager)mngrclassobj;

            sSQLArray.Clear();

            try
            {

                sSQLArray.Clear();

                sRetun = DeleteCheck(EntryNo, mngrclass.FinYearID);

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }


                sSQLArray.Clear();

                SQL = " DELETE FROM  RM_RECEIPT_MASTER_trigger ";
                SQL = SQL + " WHERE RM_MRM_RECEIPT_NO = '" + EntryNo + "' AND  AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);

                SQL = " DELETE from   RM_RECEIPT_MASTER   ";
                SQL = SQL + " WHERE  RM_MRM_RECEIPT_NO = '" + EntryNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM  RM_RECEPIT_DETAILS ";
                SQL = SQL + " WHERE RM_MRM_RECEIPT_NO = '" + EntryNo + "' AND  AD_FIN_FINYRID = " + mngrclass.FinYearID + " AND RM_MRD_APPROVED ='N'";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM  RM_RECEPIT_TOLL ";
                SQL = SQL + " WHERE RM_MRM_RECEIPT_NO = '" + EntryNo + "' AND  AD_FIN_FINYRID = " + mngrclass.FinYearID + " AND RM_MRD_APPROVED ='N'";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRC", EntryNo, false, Environment.MachineName, "D", sSQLArray);
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

        private string DeleteCheck ( string Code, int FinID )
        {
            DataSet dsCnt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();



                SQL = " select count(*) CNT from RM_RECEPIT_DETAILS ";

                SQL = SQL + " WHERE RM_MRM_RECEIPT_NO = '" + Code + "' AND  AD_FIN_FINYRID = " + FinID + " AND RM_MRD_APPROVED ='Y' ";

                dsCnt = clsSQLHelper.GetDataset(SQL);


                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return " Receipt Approved Already. Kindly delete approved entry and try";
                }

            }
            catch (Exception ex)
            {
                return System.Convert.ToString(ex.Message);
            }
            return "CONTINUE";
        }

        private string InsertDetailsSQL ( SteelReceiptEntryEntity oEntity, List<SteelReceiptGridDetailsEntity> objRMGridEntity, object mngrclassobj )
        {
            string sRetun = string.Empty;

            DataSet dept = new DataSet();

            try
            {
                int iRow = 0;

                string sInternalExternal = "No";

                SessionManager mngrclass = (SessionManager)mngrclassobj;
                DateTime dReceiptTime = System.Convert.ToDateTime(oEntity.EntryDate.ToString("dd-MMM-yyyy") + " " + oEntity.EntryTime);

                foreach (var Data in objRMGridEntity)
                {
                    if (!string.IsNullOrEmpty(Data.RmCode))
                    {
                        ++iRow;

                        SQL = " INSERT INTO RM_RECEPIT_DETAILS (";
                        SQL = SQL + " RM_MRM_RECEIPT_UNIQE_NO, RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, ";
                        SQL = SQL + " RM_MRM_RECEIPT_DATE, RM_VM_VENDOR_CODE,RM_MPOM_ORDER_NO,  ";
                        SQL = SQL + " RM_MPOM_FIN_FINYRID, RM_POD_SL_NO, RM_MPOM_PRICE_TYPE,RM_MRD_SL_NO, AD_CM_COMPANY_CODE, ";
                        SQL = SQL + " SALES_STN_STATION_CODE,HR_DEPT_DEPT_CODE, RM_RMM_RM_CODE, RM_UM_UOM_CODE, ";
                        SQL = SQL + " RM_SM_SOURCE_CODE,     RM_INTERNALOREXTENAL , RM_MRD_TRIPNO,    ";
                        SQL = SQL + " RM_MRM_VEHICLE_DESCRIPTION,  RM_MRD_DRIVER_NAME, RM_MRD_VENDOR_DOC_NO,";
                        SQL = SQL + " RM_MRD_ORDER_QTY, RM_MRD_SUPPLY_QTY,RM_MRD_WEIGH_QTY,  ";
                        SQL = SQL + " RM_MRD_APPROVE_QTY,RM_MRD_SUPP_UNIT_PRICE,RM_MRD_SUPP_AMOUNT,  ";
                        SQL = SQL + " RM_MRD_TOTAL_AMOUNT,RM_MRD_SUPP_UNIT_PRICE_NEW,RM_MRD_SUPP_AMOUNT_NEW,  ";
                        SQL = SQL + " RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_APPROVED, RM_MRM_APPROVED_NO,RM_MRD_APPROVED_FINYRID, RM_MRD_SUPP_ENTRY, RM_MRD_SUPP_PENO, ";
                        SQL = SQL + " RM_MPED_VFINYR_ID,TECH_PLM_PLANT_CODE,RM_MRM_STOCK_YN ,RM_PO_RM_TYPE,AD_BR_CODE,RM_MRM_RECEIPT_DATE_TIME,";
                        SQL = SQL + " RM_MRD_SUPP_VAT_TYPE_CODE, RM_MRM_SUPP_VAT_PERCENTAGE ,SALES_STN_STATION_CODE_PO_SUP ) ";

                        SQL = SQL + " VALUES ( ";

                        SQL = SQL + " RECEPT_SEQ.NEXTVAL,'" + oEntity.EntryNo + "', " + mngrclass.FinYearID + ", ";
                        SQL = SQL + " '" + oEntity.EntryDate.ToString("dd-MMM-yyyy") + "','" + oEntity.SupplierCode + "','" + oEntity.OrderNo + "' ,";
                        SQL = SQL + " " + oEntity.orderfinYrId + " ," + Data.PoSLNO + ", '" + oEntity.PriceType + "'," + iRow + " ,'" + mngrclass.CompanyCode + "' ,";
                        SQL = SQL + " '" + Data.StnCode + "' ,'" + oEntity.DeptCode + "','" + Data.RmCode + "' ,'" + Data.UomCode + "' ,";
                        SQL = SQL + " '" + Data.SourceCode + "' ,'" + sInternalExternal + "',1,";
                        SQL = SQL + " '" + oEntity.TrailerName + "','" + oEntity.Drivername + "','" + oEntity.SuppDocno + "' ,";
                        SQL = SQL + " " + Convert.ToDouble(Data.OrderQty) + " , " + Convert.ToDouble(Data.SuppQty) + " ," + Convert.ToDouble(Data.MeasuredQty) + " ,";
                        SQL = SQL + " " + Convert.ToDouble(Data.ApprovedQty) + " ," + Convert.ToDouble(Data.UnitRate) + "," + Convert.ToDouble(Data.UnitAmount) + " , ";
                        SQL = SQL + " " + Convert.ToDouble(Data.UnitAmount) + "," + Convert.ToDouble(Data.UnitRateNew) + "," + Convert.ToDouble(Data.UnitAmountNew) + ",";
                        SQL = SQL + " " + Convert.ToDouble(Data.UnitAmountNew) + ",'N' ,'' ,0 ,'N' ,'' ,";
                        SQL = SQL + " 0 ,'" + oEntity.PlantCode + "','" + oEntity.TempStock + "','" + oEntity.PoReceiptType + "','" + oEntity.Branch + "',  ";
                        SQL = SQL + " TO_DATE('" + dReceiptTime.ToString("dd-MMM-yyyy hh:mm:ss tt") + "', 'DD-MM-YYYY HH:MI:SS AM') ,'" + Data.VatType + "','" + Data.VatRate + "','" + Data.StnCode + "' )";

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

        private string InsertTollDetailsSQL ( string sEntryNumber, int iFinYear, List<SteelReceiptEntryRmTollGridEntity> objTollGridEntity, string Status )
        {
            string sRetun = string.Empty;
            int Row = 0;

            try
            {
                if (Status == "U")
                {
                    SQL = " Delete from  RM_RECEPIT_TOLL where RM_MRM_RECEIPT_NO ='" + sEntryNumber + "' and AD_FIN_FINYRID= " + iFinYear + " ";
                    sSQLArray.Add(SQL);
                }

                foreach (var Data in objTollGridEntity)
                {
                    Row++;
                    SQL = "INSERT INTO RM_RECEPIT_TOLL ( ";
                    SQL = SQL + "   RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_VM_VENDOR_CODE, SALES_STN_STATION_CODE,RM_SM_SOURCE_CODE, ";
                    SQL = SQL + "   RM_RMM_RM_CODE, RM_UOM_UOM_CODE, RM_MRM_TOLL_QTY, RM_MRM_TOLL_SLNO, ";
                    SQL = SQL + "   RM_MRM_TOLL_NO, RM_MRM_TOLL_RATE, RM_MRM_TOLL_AMOUNT,RM_TL_LOCATION_CODE,  ";
                    SQL = SQL + "   RM_MRM_TOLL_PE_DONE_YN, RM_MRM_TOLL_PE_NO,RM_MRM_TOLL_Date ,RM_MPOM_ORDER_NO,RM_MRM_APPROVED_QTY,  ";
                    SQL = SQL + "   RM_MRM_APRPOVED_RATE,RM_MRM_APPROVED_AMOUNT,RM_MRM_TOLL_ENTRYTYPE ,  ";
                    SQL = SQL + "   RM_MRM_VAT_TYPE_CODE, RM_MRM_VAT_PERCENTAGE )  ";
                    SQL = SQL + "VALUES ( '" + sEntryNumber + "'," + iFinYear + " ,'" + Data.oVenderCode + "' , '" + Data.oStnCode + "','" + Data.oSourceCode + "',";
                    SQL = SQL + "   '" + Data.oRMCode + "','" + Data.oUOMCode + "'," + Data.oTollQTY + "," + Row + ",";
                    SQL = SQL + "   '" + Data.oTollNo + "' , " + Data.oTollRate + "," + Data.oTollamount + " ,'" + Data.oTollLoc + "' , ";
                    SQL = SQL + "   'N' ,'','" + Data.oTollDATE.ToString("dd-MMM-yyyy") + "','" + Data.oPONo + "', ";
                    SQL = SQL + "   " + Data.oTollQTY + " ," + Data.oTollRate + "," + Data.oTollamount + " ,'" + Data.ENTRYTYPE + "' , ";
                    SQL = SQL + "   '" + Data.oTaxType + "' ,'" + Data.VatRate + "'  ) ";

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

         

        #endregion

        
         #region "UnAprrove"


        private string DeleteCheckReceiptDetails(string txtEntryNo)
        {
            DataSet dsCnt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                 

                SQL = " select count(*) CNT from RM_RECEPIT_DETAILS  where   RM_MRD_APPROVED ='Y' AND  RM_MRM_RECEIPT_NO = '" + txtEntryNo + "'";

                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "Receipt details alreaday approved";
                }

            }
            catch (Exception ex)
            {
                return System.Convert.ToString(ex.Message);
            }
            return "CONTINUE";
        }


        public string UnApprove ( string txtEntryNo, object mngrclassobj )
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sRetun = DeleteCheckReceiptDetails(txtEntryNo);

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }


                sSQLArray.Clear();

                SQL = " update RM_RECEIPT_MASTER set ";
                SQL = SQL + " RM_MRM_UPDATEDBY = '" + mngrclass.UserName + "' , RM_MRM_UPDATEDDATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " RM_MRM_RECEIPT_APPROVED = 'N' , ";
                SQL = SQL + " RM_MRM_RECEIPT_APPROVE_BY = ''";
                SQL = SQL + " where RM_MRM_RECEIPT_NO = '" + txtEntryNo + "' and AD_FIN_FINYRID =" + mngrclass.FinYearID + " and RM_MRM_APPROVED_STATUS='N' ";

                sSQLArray.Add(SQL);


                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTPSEP", txtEntryNo, false, Environment.MachineName, "U", sSQLArray);

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



        #region " FUNCTION FOR PRINT "

        public DataSet FetchRceiptPrintData ( string sEntryNo, object mngrclassobj )
        {
            DataSet dsPoStatus = new DataSet("RMRECEIPTPRINT");
            DataTable dtMaster = new DataTable();
            DataTable dtTollDetails = new DataTable();
            DataTable dtTollDetailWithOUTLPO = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT  ";
                SQL = SQL + "   RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_MRM_RECEIPT_DATE, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID,  ";
                SQL = SQL + "   RM_POD_SL_NO, RM_MPOM_PRICE_TYPE, RM_MRD_SL_NO, AD_CM_COMPANY_CODE, RM_VM_VENDOR_CODE,  ";
                SQL = SQL + "   RM_VM_VENDOR_NAME, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, HR_DEPT_DEPT_CODE, RM_RMM_RM_CODE,  ";
                SQL = SQL + "   RM_UM_UOM_CODE, RM_RMM_RM_DESCRIPTION, RM_UM_UOM_DESC, RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC,  ";
                SQL = SQL + "   RM_INTERNALOREXTENAL, RM_MTRANSPO_ORDER_NO, RM_MTRANSPO_FIN_FINYRID, RM_POD_SL_NO_TRANS, RM_MRM_TRANSPORTER_CODE,  ";
                SQL = SQL + "   TRANSPORTER_NAME, RM_MRM_TRNSPORTER_DOC_NO, RM_MRD_TRIPNO,  RM_MRM_VEHICLE_DESCRIPTION,  ";
                SQL = SQL + "   RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME, RM_MRD_VENDOR_DOC_NO, RM_MRD_ORDER_QTY, RM_MRD_SUPPLY_QTY,  ";
                SQL = SQL + "   RM_MRD_WEIGH_QTY, RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY, RM_MRD_SUPP_UNIT_PRICE, RM_MRD_SUPP_AMOUNT,  ";
                SQL = SQL + "   RM_MRD_TRANS_RATE, RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT, RM_MRD_SUPP_UNIT_PRICE_NEW, RM_MRD_SUPP_AMOUNT_NEW,  ";
                SQL = SQL + "   RM_MRD_TRANS_RATE_NEW, RM_MRD_TRANS_AMOUNT_NEW, RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_APPROVED, RM_MRM_APPROVED_NO,  ";
                SQL = SQL + "   RM_MRD_APPROVED_FINYRID, RM_MRD_SUPP_ENTRY, RM_MRD_SUPP_PENO, RM_MPED_VFINYR_ID, RM_MRD_TRANS_ENTRY,  ";
                SQL = SQL + "   RM_MRD_TRANS_PENO, RM_MPED_TFINYR_ID, RM_MRM_RECEIPT_UNIQE_NO, RM_MRM_REMARKS, RM_MRM_CREATEDBY,  ";
                SQL = SQL + "   RM_MRM_UPDATEDBY,RM_MRD_FIRST_WEIGHT_TIME, RM_MRD_FRIST_WEIGHT_QTY,RM_MRD_SECOND_WEIGHT_TIME,";
                SQL = SQL + "   RM_MRD_SECOND_WEIGHT_QTY, RM_MRD_READING_STATUS,RM_MRD_ENTRY_MODE,RM_MRD_WEIGH_BRIDGE_ID,TECH_PLM_PLANT_CODE,TECH_PLM_PLANT_NAME,";
                SQL = SQL + "   MASTER_BRANCH_CODE,RM_MRM_RECEIPT_VERIFIED_BY, RM_MRM_RECEIPT_APPROVE_BY ";
                SQL = SQL + " FROM  ";
                SQL = SQL + "   RM_RECEIPT_VOUCHER_PRINT_VW ";
                SQL = SQL + " WHERE  RM_RECEIPT_VOUCHER_PRINT_VW.RM_MRM_RECEIPT_NO = '" + sEntryNo + "' AND  RM_RECEIPT_VOUCHER_PRINT_VW.AD_FIN_FINYRID =" + mngrclass.FinYearID + "";

                dtMaster = clsSQLHelper.GetDataTableByCommand(SQL);
                dtMaster.TableName = "RM_RECEPIT_DETAILS";
                dsPoStatus.Tables.Add(dtMaster);

                SQL = "SELECT  ";
                SQL = SQL + "RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE, RM_VM_VENDOR_NAME,RM_MPOM_ORDER_NO,";
                SQL = SQL + "   RM_MRM_TOLL_NO, RM_MRM_TOLL_RATE, RM_MRM_TOLL_AMOUNT,RM_RECEPIT_TOLL.RM_TL_LOCATION_CODE,  ";
                SQL = SQL + "   RM_TL_LOCATION_DESC,RM_MRM_TOLL_PE_DONE_YN, RM_MRM_TOLL_PE_NO,RM_MRM_TOLL_Date , ";
                SQL = SQL + "   RM_RECEPIT_TOLL.SALES_STN_STATION_CODE,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE,RM_SM_SOURCE_DESC,";
                SQL = SQL + "  RM_RECEPIT_TOLL.RM_RMM_RM_CODE,RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_UOM_UOM_CODE ,RM_UM_UOM_DESC, RM_MRM_TOLL_QTY,RM_MRM_VAT_TYPE_CODE, RM_MRM_VAT_PERCENTAGE ";
                SQL = SQL + "FROM RM_RECEPIT_TOLL ,RM_VENDOR_MASTER,RM_RECEPIT_TOLL_LOCATION_MAST , ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER ,RM_SOURCE_MASTER,RM_UOM_MASTER ";
                SQL = SQL + "    where ";
                SQL = SQL + "    RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE=RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_UOM_UOM_CODE=RM_UOM_MASTER.RM_UM_UOM_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_TL_LOCATION_CODE =RM_RECEPIT_TOLL_LOCATION_MAST.RM_TL_LOCATION_CODE(+)  ";
                SQL = SQL + "  AND  RM_MRM_RECEIPT_NO ='" + sEntryNo + "' and  AD_FIN_FINYRID =" + mngrclass.FinYearID + "  and RM_MRM_TOLL_ENTRYTYPE='WITHLPO' ";

                dtTollDetails = clsSQLHelper.GetDataTableByCommand(SQL);
                dtTollDetails.TableName = "RM_RECEPIT_TOLL_WITHLOP";
                dsPoStatus.Tables.Add(dtTollDetails);

                SQL = "SELECT   ";
                SQL = SQL + "RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE, RM_VM_VENDOR_NAME,RM_MPOM_ORDER_NO, ";
                SQL = SQL + "   RM_MRM_TOLL_NO, RM_MRM_TOLL_RATE, RM_MRM_TOLL_AMOUNT,RM_RECEPIT_TOLL.RM_TL_LOCATION_CODE,   ";
                SQL = SQL + "   RM_TL_LOCATION_DESC,RM_MRM_TOLL_PE_DONE_YN, RM_MRM_TOLL_PE_NO,RM_MRM_TOLL_Date ,  ";
                SQL = SQL + "   RM_RECEPIT_TOLL.SALES_STN_STATION_CODE,SL_STATION_MASTER.SALES_STN_STATION_NAME,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE,RM_SM_SOURCE_DESC, ";
                SQL = SQL + "  RM_RECEPIT_TOLL.RM_RMM_RM_CODE,RM_RMM_RM_DESCRIPTION,  ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_UOM_UOM_CODE ,RM_UM_UOM_DESC, RM_MRM_TOLL_QTY,RM_MRM_VAT_TYPE_CODE, RM_MRM_VAT_PERCENTAGE   ";
                SQL = SQL + "FROM RM_RECEPIT_TOLL ,RM_VENDOR_MASTER,RM_RECEPIT_TOLL_LOCATION_MAST ,  ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER ,RM_SOURCE_MASTER,RM_UOM_MASTER,SL_STATION_MASTER ";
                SQL = SQL + "    where  ";
                SQL = SQL + "    RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+) ";
                SQL = SQL + "    AND  RM_RECEPIT_TOLL.SALES_STN_STATION_CODE=SL_STATION_MASTER.SALES_STN_STATION_CODE(+)    ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE(+)   ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE=RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+)   ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_UOM_UOM_CODE=RM_UOM_MASTER.RM_UM_UOM_CODE(+)   ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_TL_LOCATION_CODE =RM_RECEPIT_TOLL_LOCATION_MAST.RM_TL_LOCATION_CODE(+)  ";
                SQL = SQL + "  AND  RM_MRM_RECEIPT_NO ='" + sEntryNo + "' and  AD_FIN_FINYRID =" + mngrclass.FinYearID + "  and RM_MRM_TOLL_ENTRYTYPE='WITHOUTLPO' ";

                dtTollDetailWithOUTLPO = clsSQLHelper.GetDataTableByCommand(SQL);
                dtTollDetailWithOUTLPO.TableName = "RM_RECEPIT_TOLL_WITHOUTLOP";
                dsPoStatus.Tables.Add(dtTollDetailWithOUTLPO);

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
    }

    #region " ENTITY "

    public class SteelReceiptEntryEntity
    {
        public SteelReceiptEntryEntity ( )
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public string EntryNo
        {
            get;
            set;
        }

        public string OrderNo
        {
            get;
            set;
        }

        public DateTime EntryDate
        {
            get;
            set;
        }
        public string EntryTime
        {
            get;
            set;
        }

        public string orderfinYrId
        {
            get;
            set;
        }

        public string SupplierCode
        {
            get;
            set;
        }

        public string SupplierName
        {
            get;
            set;
        }
        public string RateRevisionApplicable { get; set; }
        public string Branch { get; set; }
        public string Remarks
        {
            get;
            set;
        }

        public string PriceType
        {
            get;
            set;
        }

        public string TempStock
        {
            get;
            set;
        }

        //public string StationCode
        //{
        //    get;
        //    set;
        //}

        //public string StationName
        //{
        //    get;
        //    set;
        //}

        public string PlantCode
        {
            get;
            set;
        }

        public string DeptCode
        {
            get;
            set;
        }

        public string TrailerName
        {
            get;
            set;
        }

        public string Drivername
        {
            get;
            set;
        }

        public string SuppDocno
        {
            get;
            set;
        }

        public double TotalAmount
        {
            get;
            set;
        }

        public string PoReceiptType
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

        public string VerifiedTime
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
        public string ApprovedTime
        {
            get;
            set;
        }
    }

    public class SteelReceiptGridDetailsEntity
    {
        public SteelReceiptGridDetailsEntity ( )
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public string PoSLNO
        {
            get;
            set;
        }

        public string RmCode
        {
            get;
            set;
        }

        public string DriverType
        {
            get;
            set;
        }

        public string UomCode
        {
            get;
            set;
        }

        public string StnCode
        {
            get;
            set;
        }

        public string StnName
        {
            get;
            set;
        }

        public string SourceCode
        {
            get;
            set;
        }

        public string SourceName
        {
            get;
            set;
        }
        public string VatType
        {
            get;
            set;
        }
        public string VatRate
        {
            get;
            set;
        }

        public double OrderQty
        {
            get;
            set;
        }

        public double SuppQty
        {
            get;
            set;
        }

        public double MeasuredQty
        {
            get;
            set;
        }

        public double ApprovedQty
        {
            get;
            set;
        }

        public double RejectedQty
        {
            get;
            set;
        }

        public double UnitRate
        {
            get;
            set;
        }

        public double UnitAmount
        {
            get;
            set;
        }

        public double UnitRateNew
        {
            get;
            set;
        }

        public double UnitAmountNew
        {
            get;
            set;
        }
    }

    public class SteelReceiptEntryRmTollGridEntity
    {
        public string oPONo { get; set; }
        public string oVenderCode { get; set; }
        public string oStnCode { get; set; }
        public string oSourceCode { get; set; }
        public string oRMCode { get; set; }
        public string oUOMCode { get; set; }
        public DateTime oTollDATE { get; set; }
        public string oTollNo { get; set; }
        public double oTollQTY { get; set; }
        public double oTollRate { get; set; }
        public double oTollamount { get; set; }
        public string oTollLoc { get; set; }
        public string ENTRYTYPE { get; set; }
        public string oTaxType { get; set; }
        public double VatRate { get; set; }
    }
    #endregion
}

