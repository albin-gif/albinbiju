using System;
using System.Data;
using AccosoftUtilities;
using AccosoftLogWriter;
using AccosoftNineteenCDL;
using System.Configuration;
using System.Linq;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Collections;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.HtmlControls;
using System.Web.UI.WebControls;
using System.Web.UI.WebControls.WebParts;
using System.Xml.Linq;
using System.Globalization;
using System.Collections.Generic;

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class ReceiptEntryRmLogic
    {


        //public string msBaseCurrency = "01";
        //public double mdExchangeRate = 1.0;
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public ReceiptEntryRmLogic ( )
        {
            //
            // TODO: Add constructor logic here
            //
        }


        #region "LookUpFunctions"

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
        public DataTable FillViewRM ( string PoNum, string StnCode, string Branch, string SelectedRM = "" )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT    ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_PONO ORDER_NO,  RM_PO_DETAILS.SALES_STN_STATION_CODE STATION_CODE, " ;
                SQL = SQL + "    SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME, " ;
                SQL = SQL + "    RM_PO_MASTER.RM_VM_VENDOR_CODE,RM_VENDOR_MASTER.RM_VM_VENDOR_NAME , " ;
                SQL = SQL + "    RM_PO_DETAILS.RM_RMM_RM_CODE RM_CODE , " ;
                SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_NAME , " ;
                SQL = SQL + "    RM_UOM_MASTER.RM_UM_UOM_DESC , INVOICE_CALC_CODE, " ;
                SQL = SQL + "    RM_PO_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_NAME, " ;
                SQL = SQL + "    RM_PO_DETAILS.RM_UOM_UOM_CODE UOM_CODE,RM_POD_RM_VAT_TYPE,RM_POD_RM_VAT_RATE,  " ;
                SQL = SQL + "    RM_POD_QTY ,RM_POD_UNIT_PRICE, RM_POD_NEWPRICE ,   " ;
                SQL = SQL + "    RM_PO_DETAILS.RM_UOM_TOLL_CODE ,  RM_PO_DETAILS.RM_POD_TOLL_RATE  " ;
                SQL = SQL + " FROM " ;
                SQL = SQL + "    RM_PO_MASTER, RM_PO_DETAILS, " ;
                SQL = SQL + "    SL_STATION_MASTER,RM_UOM_MASTER,RM_UOM_CATEGORY_MASTER, " ;
                SQL = SQL + "    RM_RAWMATERIAL_MASTER , RM_SOURCE_MASTER , RM_VENDOR_MASTER " ;
                SQL = SQL + " WHERE   " ;
                SQL = SQL + "    RM_PO_DETAILS.RM_SM_SOURCE_CODE =  RM_SOURCE_MASTER.RM_SM_SOURCE_CODE " ;
                SQL = SQL + "    AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE " ;
                SQL = SQL + "    AND RM_PO_DETAILS.RM_UOM_UOM_CODE  =  RM_UOM_MASTER.RM_UM_UOM_CODE(+)  " ;
                SQL = SQL + "    AND  RM_UOM_MASTER.RM_UOM_CAT_CODE= RM_UOM_CATEGORY_MASTER.RM_UOM_CAT_CODE " ;
                SQL = SQL + "    AND RM_PO_DETAILS.SALES_STN_STATION_CODE =  SL_STATION_MASTER.SALES_STN_STATION_CODE " ;
                SQL = SQL + "    AND RM_PO_MASTER.RM_VM_VENDOR_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+)  " ;
                SQL = SQL + "    AND RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO  " ;
                SQL = SQL + "    AND RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID  " ;


                SQL = SQL + " AND RM_PO_DETAILS.RM_PO_PONO in ('" + PoNum.Replace(",", "','") + "') ";
                SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE='" + StnCode + "'";
                SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE='" + Branch + "'";
                SQL = SQL + " And RM_RAWMATERIAL_MASTER.RM_RMM_PRODUCT_TYPE IN ('OTHER','TOLL')";

                if (!string.IsNullOrEmpty(SelectedRM))
                {
                    SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE||RM_PO_MASTER.RM_PO_PONO|| ";
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


        public DataTable FillPlantView ( string stCode )//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE CAT_CODE, TECH_PLANT_MASTER.TECH_PLM_PLANT_NAME CAT_NAME ";
                SQL = SQL + " FROM TECH_PLANT_MASTER  ";
                SQL = SQL + " WHERE  TECH_PLM_DELETESTATUS ='0' ";
                SQL = SQL + " AND TECH_PLANT_MASTER.SALES_STN_STATION_CODE = '" + stCode + "'";
                SQL = SQL + " ORDER BY TECH_PLM_PLANT_CODE ASC";

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


        public DataTable FillViewSupplier ( )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();



                SQL = " SELECT  RM_VM_VENDOR_CODE SUPPLIER_CODE,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIER_NAME,SALES_PAY_TYPE_DESC";
                SQL = SQL + " FROM   RM_VENDOR_MASTER,SL_PAY_TYPE_MASTER";
                SQL = SQL + " WHERE RM_VM_VENDOR_STATUS='A' ";
                SQL = SQL + " AND RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS  = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE  ";
                SQL = SQL + " AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE in (SELECT RM_IP_PARAMETER_VALUE FROM RM_DEFUALTS_DRIVER_TRAILER ";
                SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TOLLSUPPLIER') ";
                SQL = SQL + " order by  RM_VM_VENDOR_CODE asc";

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
        
        public DataTable FillTokenView ( string Supplier,string Station,string RMCode)//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + "        RM_RT_TOKEN_NO,AD_FIN_FINYRID,RM_RT_TOKEN_DATE TOKENDATE,RM_RT_TOKEN_DATE_TIME TOKENTIME,      ";
                SQL = SQL + "        RM_RECEPIT_TOKEN_MST.RM_VM_VENDOR_CODE VENDORCODE,RM_VM_VENDOR_NAME VENDORNAME,      ";
                SQL = SQL + "        FA_FAM_ASSET_CODE VEHICLECODE,RM_RT_VEHICLE_DESCRIPTION VEHICLENAME,      ";
                SQL = SQL + "        RM_RT_DRIVER_CODE DRIVERCODE,RM_RT_DRIVER_NAME DRIVERNAME,RM_RT_CREATEDBY,RM_RT_CREATEDDATE,  ";
                SQL = SQL + "        RM_RT_UPDATEDBY,RM_RT_UPDATEDDATE,RM_MRM_RECEIPT_NO,RM_MRM_FINYRID,  ";
                SQL = SQL + "        RM_RT_STATUS,RM_RECEPIT_TOKEN_MST.AD_BR_CODE         ";
                SQL = SQL + "   FROM RM_RECEPIT_TOKEN_MST,rm_vendor_master      ";
                SQL = SQL + "  WHERE RM_RECEPIT_TOKEN_MST.RM_VM_VENDOR_CODE=rm_vendor_master.RM_VM_VENDOR_CODE   ";
                SQL = SQL + "    AND RM_RT_STATUS='O'    ";
                SQL = SQL + "    AND RM_RECEPIT_TOKEN_MST.RM_VM_VENDOR_CODE='"+Supplier+"' ";
                SQL = SQL + "    AND RM_RECEPIT_TOKEN_MST.SALES_STN_STATION_CODE='"+Station+"' ";
                SQL = SQL + "    AND RM_RECEPIT_TOKEN_MST.RM_RMM_RM_CODE='" + RMCode + "' ";
                SQL = SQL + "    AND RM_RT_TOKEN_NO NOT IN(select RM_RT_TOKEN_NO from RM_RECEIPT_MASTER where RM_RT_TOKEN_NO is not Null)  ";

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

        public DataTable FillOrderNoView ( string approvalstatus, string FilterBranch, object mngrclassobj )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                //string fromdate, string todate, 
                SQL = "  ";
                    SQL = "     SELECT   ";
                    SQL = SQL + "        RM_PO_MASTER.RM_PO_PONO orderno, RM_PO_MASTER.RM_VM_VENDOR_CODE code , RM_VENDOR_MASTER.RM_VM_VENDOR_NAME vendorname, " ;
                    SQL = SQL + "        SL_STATION_MASTER.SALES_STN_STATION_NAME stationname,SL_STATION_MASTER.SALES_STN_STATION_CODE stationcode, " ;
                    SQL = SQL + "        RM_PO_DETAILS.RM_RMM_RM_CODE rmcode,RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION rmname,RM_PO_PRICETYPE potype ,  " ;
                    SQL = SQL + "        RM_PO_DETAILS.RM_SM_SOURCE_CODE sourcecode, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC sourcename, " ;
                    SQL = SQL + "        to_char(RM_PO_PO_DATE ,'DD-MON-YYYY') orderdate, RM_PO_MASTER.AD_FIN_FINYRID finyrid, RM_PO_DETAILS.RM_POD_SL_NO  slno   " ;
                    SQL = SQL + "     FROM  " ;
                    SQL = SQL + "         RM_PO_MASTER, RM_VENDOR_MASTER,  RM_PO_DETAILS, SL_STATION_MASTER, " ;
                    SQL = SQL + "         RM_RAWMATERIAL_MASTER , RM_SOURCE_MASTER " ;
                    SQL = SQL + "     WHERE   " ;
                    SQL = SQL + "            RM_PO_APPROVED ='Y' and RM_PO_MASTER.RM_PO_ENTRY_TYPE ='PURCHASE' " ;
                    SQL = SQL + "            AND   RM_PO_DETAILS.RM_SM_SOURCE_CODE =  RM_SOURCE_MASTER.RM_SM_SOURCE_CODE " ;
                    SQL = SQL + "            AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE " ;
                    SQL = SQL + "            AND RM_PO_DETAILS.SALES_STN_STATION_CODE =  SL_STATION_MASTER.SALES_STN_STATION_CODE " ;
                    SQL = SQL + "            AND RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO  " ;
                    SQL = SQL + "            AND RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID " ;
                    SQL = SQL + "            AND RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE " ;
                    SQL = SQL + "            AND RM_RAWMATERIAL_MASTER.RM_RMM_PRODUCT_TYPE  in('PR', 'TRADE')     " ; 

                //  SQL = SQL + " AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE IN('" + FilterBranch.Replace(",", "','") + "')";
                if (mngrclass.UserName != "ADMIN")
                {
                    SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE in  ";
                    SQL = SQL + "( SELECT     SALES_STN_STATION_CODE FROM    WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + mngrclass.UserName + "')";
                    /// jomy and alex , added // 17 05 2025 // po staion overriding 
                    /// Fetch all POs where any of the PO’s stations are in the user’s granted stations — and once that condition is true, 
                    /// return the entire PO including all its station-wise line items.
                    //SQL = SQL + "       AND EXISTS ( ";
                    //SQL = SQL + "        SELECT 1  " ;
                    //SQL = SQL + "        FROM RM_PO_DETAILS D " ;
                    //SQL = SQL + "        WHERE D.RM_PO_PONO = RM_PO_MASTER.RM_PO_PONO " ;
                    //SQL = SQL + "          AND D.AD_FIN_FINYRID = RM_PO_MASTER.AD_FIN_FINYRID " ;
                    //SQL = SQL + "          AND D.SALES_STN_STATION_CODE IN ( " ;
                    //SQL = SQL + "              SELECT SALES_STN_STATION_CODE  " ;
                    //SQL = SQL + "              FROM WS_GRANTED_STATION_USERS  " ;
                    //SQL = SQL + "              WHERE AD_UM_USERID ='" + mngrclass.UserName + "'";
                    //SQL = SQL + "          ) " ;
                    //SQL = SQL + "       )  " ;



                    SQL = SQL + "  AND     RM_PO_MASTER.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                }

                if (approvalstatus == "O")// APPROVAL STATUS 'A' MEANS CONDITION 'ALL'(i.e, INCLUDES APPROVED AND NOT APPROVED)
                {
                    //  SQL = SQL + "  AND RM_PO_MASTER.RM_PO_PO_STATUS ='O'";
                    SQL = SQL + "  AND RM_PO_DETAILS.RM_POD_STATUS ='O'";
                }
                else if (approvalstatus == "C")// APPROVAL STATUS 'A' MEANS CONDITION 'ALL'(i.e, INCLUDES APPROVED AND NOT APPROVED)
                {
                    SQL = SQL + "  AND RM_PO_DETAILS.RM_POD_STATUS ='C'";
                }

                SQL = SQL + " ORDER BY RM_PO_MASTER.RM_PO_PONO DESC";

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

        public DataTable FillSearchView ( string fromdate, string todate, string FilterType, string FilterBranch, object mngrclassobj )//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SQL = "     SELECT  ";
                SQL = SQL + "        RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO Receiptno, ";
                SQL = SQL + "        RM_RECEIPT_MASTER.AD_FIN_FINYRID Finyrid, ";
                SQL = SQL + "        to_char(RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY')  Receiptdate, ";
                SQL = SQL + "        RM_RECEIPT_MASTER.RM_MPOM_ORDER_NO Orderno, ";
                SQL = SQL + "        RM_RECEPIT_DETAILS.RM_MRM_VEHICLE_DESCRIPTION  Vendorcode, RM_RECEIPT_MASTER.RM_MRM_APPROVED_STATUS, ";
                SQL = SQL + "        RM_VENDOR_MASTER.RM_VM_VENDOR_NAME Vendorname , RM_MRD_VENDOR_DOC_NO ,  ";
                SQL = SQL + "        RM_MRM_TRNSPORTER_DOC_NO  RM_MRM_TRNSPORTER_DOC_NOC , TRANSPORTER.RM_VM_VENDOR_NAME TRANPORTERNAME ,  ";
                SQL = SQL + "         RM_MRD_WEIGH_BRIDGE_DOC_NO,RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,RM_RMM_RM_DESCRIPTION RM_DESCRIPTION, ";
                SQL = SQL + "        RM_MRM_RECEIPT_VERIFIED VERIFIED ,  RM_MRM_RECEIPT_APPROVED APPROVED , ";
                SQL = SQL + "        CASE WHEN RM_MRD_ENTRY_MODE='A'  THEN 'AUTO' ELSE 'MANUAL' END AS ENTRY_MODE  ";
                SQL = SQL + "     FROM  ";
                SQL = SQL + "        RM_RECEIPT_MASTER, RM_VENDOR_MASTER, RM_RECEPIT_DETAILS , ";
                SQL = SQL + "        RM_VENDOR_MASTER TRANSPORTER,RM_RAWMATERIAL_MASTER ";
                SQL = SQL + "      WHERE  ";
                SQL = SQL + "        RM_RECEIPT_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE = TRANSPORTER.RM_VM_VENDOR_CODE(+) ";
                SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE =RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "        AND  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO = RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO   ";
                SQL = SQL + "        AND  RM_RECEIPT_MASTER.AD_FIN_FINYRID = RM_RECEPIT_DETAILS.AD_FIN_FINYRID   "; 
                SQL = SQL + "       and   RM_RECEIPT_MASTER.AD_FIN_FINYRID =" + mngrclass.FinYearID + "";
                SQL = SQL + "       AND RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " and   RM_RECEIPT_MASTER.RM_PO_RM_TYPE ='RM'";
                if (FilterType == "OPEN")
                {
                    SQL = SQL + "AND RM_MRD_READING_STATUS='O'";
                }
                else if (FilterType == "APPROVED")
                {
                    SQL = SQL + "AND RM_MRM_RECEIPT_APPROVED='Y'";
                }

                else if (FilterType == "VERIFIED")
                {
                    SQL = SQL + "AND RM_MRM_RECEIPT_VERIFIED='Y'";
                }
                
                else if (FilterType == "NOT VERIFIED")
                {
                    SQL = SQL + "AND RM_MRM_RECEIPT_VERIFIED='N'";
                }

                SQL = SQL + " and   RM_RECEIPT_MASTER.AD_BR_CODE IN('" + FilterBranch.Replace(",", "','") + "')";

                if (mngrclass.UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021

                    SQL = SQL + "  AND     RM_RECEIPT_MASTER.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )"; 

                    SQL = SQL + "    AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE IN ( SELECT     SALES_STN_STATION_CODE FROM  ";
                    SQL = SQL + "     WS_GRANTED_STATION_USERS  WHERE    AD_UM_USERID   ='" + mngrclass.UserName + "')  ";
                }

                SQL = SQL + "   ORDER BY  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO DESC";


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

        public DataTable FillTransPOView ( string stationCode, string sourcecode, string rmcode )//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  ";
                SQL = " SELECT   RM_PO_MASTER.RM_PO_PONO Orderno,  ";
                SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE  Code ,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME Vendorname,";
                //< SQL = SQL & " RM_PO_DETAILS.SALES_STN_STATION_CODE STATION_CODE,"
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME Stationname,";
                SQL = SQL + " RM_PO_DETAILS.RM_RMM_RM_CODE Rmcode,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_NAME,RM_POD_RM_VAT_TYPE,RM_POD_RM_VAT_RATE,";
                SQL = SQL + " RM_PO_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_NAME , ";
                SQL = SQL + "RM_PO_DETAILS.RM_POD_NEWPRICE Transprice,";
                SQL = SQL + "RM_PO_MASTER.AD_FIN_FINYRID Finyrid, ";
                SQL = SQL + "RM_PO_PO_DATE ORDER_DATE  ,";
                SQL = SQL + " RM_PO_DETAILS.RM_POD_SL_NO  Slno , ";
                SQL = SQL + "  RM_PO_DETAILS.RM_UOM_TOLL_CODE ,  RM_UOM_TOLL_NAME ,  RM_PO_DETAILS.RM_POD_TOLL_RATE ";

                SQL = SQL + " FROM RM_PO_MASTER,";
                SQL = SQL + " RM_VENDOR_MASTER,";
                SQL = SQL + " RM_PO_DETAILS,";
                SQL = SQL + " SL_STATION_MASTER,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER , RM_SOURCE_MASTER , RM_UOM_TOLL_MASTER ";
                SQL = SQL + " WHERE  ";
                //RM_PO_MASTER.RM_PO_PO_STATUS="O"
                SQL = SQL + "  RM_PO_DETAILS.RM_POD_STATUS = 'O' and RM_PO_APPROVED ='Y' and RM_PO_MASTER.RM_PO_ENTRY_TYPE ='TRANSPORTER'";
                SQL = SQL + "  AND   RM_PO_DETAILS.RM_SM_SOURCE_CODE =  RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE =  SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO ";
                SQL = SQL + " AND RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID";
                SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " AND RM_PO_DETAILS.RM_UOM_TOLL_CODE = RM_UOM_TOLL_MASTER.RM_UOM_TOLL_CODE (+)";

                SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE ='" + stationCode + "'";

                SQL = SQL + " AND RM_PO_DETAILS.RM_SM_SOURCE_CODE ='" + sourcecode + "'";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE ='" + rmcode + "'";

                SQL = SQL + " ORDER BY RM_PO_MASTER.RM_PO_PONO DESC";


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

        #region "LookupCodeSearch"

        public DataTable FillVenderView ( string VenderCode )
        {
            DataTable dtdata = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  RM_VM_VENDOR_CODE Code,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME Name,SALES_PAY_TYPE_DESC";
                SQL = SQL + " FROM   RM_VENDOR_MASTER,SL_PAY_TYPE_MASTER";
                SQL = SQL + " WHERE RM_VM_VENDOR_STATUS='A' and RM_VM_VENDOR_CODE='" + VenderCode + "'";
                SQL = SQL + " AND RM_VENDOR_MASTER.RM_VM_CREDIT_TERMS  = SL_PAY_TYPE_MASTER.SALES_PAY_TYPE_CODE  ";
                SQL = SQL + " AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE in (SELECT RM_IP_PARAMETER_VALUE FROM RM_DEFUALTS_DRIVER_TRAILER ";
                SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TOLLSUPPLIER') ";
                SQL = SQL + " order by  RM_VM_VENDOR_CODE asc";

                dtdata = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dtdata;
        }

        #endregion

        #region "FillFpsCombo"

        public DataTable FillFpsCombo ( )
        {
            DataTable dtdata = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  RM_TL_LOCATION_CODE CODE, RM_TL_LOCATION_DESC NAME ";
                SQL = SQL + " FROM  RM_RECEPIT_TOLL_LOCATION_MAST";
                SQL = SQL + " order by  RM_TL_LOCATION_CODE asc";

                dtdata = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dtdata;
        }

        public DataTable FillViewStation (string UserName,string PoNo,string RmCode, string SrcCode, Double UnitPrice)
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "      SELECT RM_PO_DETAILS.SALES_STN_STATION_CODE STATION_CODE,SALES_STN_STATION_NAME STATION_NAME,RM_POD_NEWPRICE ";
                SQL = SQL + "  FROM RM_PO_master,RM_PO_DETAILS,SL_STATION_MASTER  ";
                SQL = SQL + " WHERE RM_PO_master.RM_PO_PONO=RM_PO_DETAILS.RM_PO_PONO ";
                SQL = SQL + "   AND RM_PO_DETAILS.SALES_STN_STATION_CODE=SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "   AND RM_PO_master.RM_PO_PONO='" + PoNo + "'  ";
                SQL = SQL + "   AND RM_RMM_RM_CODE='" + RmCode + "' ";
                SQL = SQL + "   AND RM_SM_SOURCE_CODE='" + SrcCode + "' ";
                //SQL = SQL + " and round(RM_POD_NEWPRICE,4)='"+UnitPrice+"' ";

                if (UserName != "ADMIN")
                {
                    SQL = SQL + "   AND RM_PO_DETAILS.SALES_STN_STATION_CODE IN ( SELECT SALES_STN_STATION_CODE FROM  ";
                    SQL = SQL + "       WS_GRANTED_STATION_USERS WHERE AD_UM_USERID ='" + UserName + "')      ";
                }
                if (UserName != "ADMIN")
                {
                    SQL = SQL + "   AND RM_PO_master.AD_BR_CODE in (select AD_BR_CODE from   ";
                    SQL = SQL + "       AD_USER_GRANTED_BRANCH where AD_UM_USERID = '" + UserName + "')   ";
                }
                 
                SQL = SQL + "  ORDER BY RM_PO_DETAILS.SALES_STN_STATION_CODE ASC  ";

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

        #region "FetchData"

        public DataTable FetchViewData ( string sCode, string id )//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                //SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO RECEIPTNO,";
                SQL = SQL + " RM_RECEIPT_MASTER.AD_FIN_FINYRID FINYRID,";
                SQL = SQL + " RM_RECEIPT_MASTER.RM_MRM_RECEIPT_DATE RECEIPTDATE,";
                SQL = SQL + " RM_RECEIPT_MASTER.RM_MPOM_ORDER_NO ORDERNO,";
                SQL = SQL + " RM_RECEIPT_MASTER.RM_VM_VENDOR_CODE VENDORCODE,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME VENDORNAME,  ";
                SQL = SQL + " RM_RECEIPT_MASTER.RM_MRM_RECEIPT_VERIFIED, RM_RECEIPT_MASTER.RM_MRM_RECEIPT_VERIFIED_BY, ";
                SQL = SQL + "  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_APPROVED, RM_RECEIPT_MASTER.RM_MRM_RECEIPT_APPROVE_BY ,";
                SQL = SQL + "  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_VERIFIEDDT,RM_RECEIPT_MASTER.RM_MRM_RECEIPT_APPROVEDDT,RM_RT_TOKEN_NO ";
                SQL = SQL + " FROM RM_RECEIPT_MASTER, RM_VENDOR_MASTER";
                SQL = SQL + " WHERE  RM_RECEIPT_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " and     RM_RECEIPT_MASTER.AD_FIN_FINYRID =" + id + "";
                SQL = SQL + " and     RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO = '" + sCode + "'";
                SQL = SQL + "   ORDER BY  RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO DESC";


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

        public DataSet FetchData ( string DocNo, int iFinYear )
        {
            DataSet dtReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT RM_MRM_RECEIPT_NO, RM_RECEIPT_MASTER.AD_CM_COMPANY_CODE, RM_RECEIPT_MASTER.AD_FIN_FINYRID, ";
                SQL = SQL + "    RM_MRM_RECEIPT_DATE, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, ";
                SQL = SQL + "    RM_RECEIPT_MASTER.RM_VM_VENDOR_CODE, RM_MRM_REMARKS, RM_MRM_GRAND_TOTAL, ";
                SQL = SQL + "    RM_MRM_APPROVED_STATUS, RM_MPOM_PRICE_TYPE, RM_MRM_CREATEDBY, ";
                SQL = SQL + "    RM_MRM_CREATEDDATE, RM_MRM_UPDATEDBY, RM_MRM_UPDATEDDATE, ";
                SQL = SQL + "    RM_RECEIPT_MASTER.RM_MRM_RECEIPT_VERIFIED, RM_RECEIPT_MASTER.RM_MRM_RECEIPT_VERIFIED_BY, ";
                SQL = SQL + "    RM_RECEIPT_MASTER.RM_MRM_RECEIPT_APPROVED, RM_RECEIPT_MASTER.RM_MRM_RECEIPT_APPROVE_BY,  ";
                SQL = SQL + "    RM_RECEIPT_MASTER.RM_MRM_RECEIPT_VERIFIEDDT,RM_RECEIPT_MASTER.RM_MRM_RECEIPT_APPROVEDDT, ";
                SQL = SQL + "    RM_MRM_DELETESTATUS,RM_MRM_STOCK_YN,RM_PO_MASTER.RM_PO_QTY_RATE_TYPE,RM_RECEIPT_MASTER.RM_PO_RATE_REVISION_APP_YN,RM_RT_TOKEN_NO   FROM RM_RECEIPT_MASTER,RM_PO_MASTER";
                SQL = SQL + "     where ";
                SQL = SQL + "     RM_MRM_RECEIPT_NO ='" + DocNo + "' and  RM_RECEIPT_MASTER.AD_FIN_FINYRID =" + iFinYear + "";
                SQL = SQL + "  and   RM_RECEIPT_MASTER.RM_MPOM_ORDER_NO=RM_PO_MASTER.RM_PO_PONO ";

                dtReturn = clsSQLHelper.GetDataset(SQL);
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
            return dtReturn;

        }

        public DataSet FetchReceiptDetalis ( string DocNo, int iFinYear )
        {
            DataSet dtReturn = new DataSet();
            DataTable dtMaster = new DataTable();
            DataTable dtTollDetails = new DataTable();
            DataTable dtTollDetailWithOUTLPO = new DataTable();

            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                // SessionManager mngrclass = (SessionManager)mngrclassobj;
                SQL = " ";
                SQL = " SELECT RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_MRM_RECEIPT_DATE, RM_MRM_RECEIPT_DATE_TIME ,  RM_MPOM_ORDER_NO,";
                SQL = SQL + " RM_MPOM_FIN_FINYRID, RM_POD_SL_NO,  RM_MPOM_PRICE_TYPE, RM_MRD_SL_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_CM_COMPANY_CODE,";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME, ";
                SQL = SQL + " RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME, rm_recepit_details.HR_DEPT_DEPT_CODE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE, RM_RECEPIT_DETAILS.RM_UM_UOM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,RM_RMM_MRD_QTY_VARIENCE_BASEUM,RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE, ";

                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC, RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE,";
                SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC, RM_INTERNALOREXTENAL,";
                SQL = SQL + " RM_MTRANSPO_ORDER_NO, RM_MTRANSPO_FIN_FINYRID,  RM_POD_SL_NO_TRANS ,  RM_MRM_TRANSPORTER_CODE,";
                SQL = SQL + " RM_MRM_TRNSPORTER_DOC_NO, RM_MRD_TRIPNO,RM_RECEPIT_DETAILS.FA_FAM_ASSET_CODE,";
                SQL = SQL + " RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME,";
                SQL = SQL + " RM_MRD_VENDOR_DOC_NO, RM_MRD_ORDER_QTY, RM_MRD_SUPPLY_QTY,";
                SQL = SQL + " RM_MRD_MOIST_LOSS_PERCENTAGE,RM_MRD_MOIST_LOSS_QTY, ";
                SQL = SQL + " RM_MRD_WEIGH_QTY, RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY,";
                SQL = SQL + " RM_MRD_SUPP_UNIT_PRICE, RM_MRD_SUPP_AMOUNT, RM_MRD_TRANS_RATE,";
                SQL = SQL + " RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT, RM_MRD_SUPP_UNIT_PRICE_NEW,";

                SQL = SQL + " RM_MRD_SUPP_AMOUNT_NEW, RM_MRD_TRANS_RATE_NEW, RM_MRD_TRANS_AMOUNT_NEW,";
                SQL = SQL + " RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_APPROVED, RM_MRM_APPROVED_NO,";
                SQL = SQL + " RM_MRD_APPROVED_FINYRID, RM_MRD_SUPP_ENTRY, RM_MRD_SUPP_PENO,";
                SQL = SQL + " RM_MPED_VFINYR_ID, RM_MRD_TRANS_ENTRY, RM_MRD_TRANS_PENO,";
                SQL = SQL + " RM_MPED_TFINYR_ID, RM_MRM_RECEIPT_UNIQE_NO , RM_MRD_WEIGH_BRIDGE_DOC_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.TECH_PLM_PLANT_CODE,TECH_PLM_PLANT_NAME,RM_MRD_WEIGH_BRIDGE_DOC_NO,RM_MRD_REJECTED_YN  ,  ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_PO_UOM_TYPE,RM_RECEPIT_DETAILS.RM_VM_INVOICE_QTY,RM_MRM_STOCK_YN, ";
                SQL = SQL + " RM_MRD_FIRST_WEIGHT_TIME,RM_MRD_FRIST_WEIGHT_QTY,RM_MRD_SECOND_WEIGHT_TIME,RM_MRD_SECOND_WEIGHT_QTY, ";
                SQL = SQL + " RM_MRD_READING_STATUS,RM_MRD_ENTRY_MODE,RM_MRD_WEIGH_BRIDGE_ID ,";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_UOM_TOLL_CODE  ,  RM_UOM_TOLL_NAME , RM_RECEPIT_DETAILS.RM_POD_TOLL_RATE  , RM_MRD_TOLL_AMOUNT	,  ";
                SQL = SQL + "  RM_POD_TOLL_RATE_NEW  , RM_MRD_TOLL_AMOUNT_NEW	 , ";
                SQL = SQL + "    RM_MRD_TOLL_PE_ENTRY_YN	,   RM_MRD_TOLL_PE_ENTRY_NO,  RM_MRD_TOLL_PE_ENTRY_FINYR_ID	 ,  ";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_BR_CODE, AD_BRANCH_MASTER.AD_BR_NAME, ";
                SQL = SQL + " RM_MRD_SUPP_VAT_TYPE_CODE,RM_MRM_SUPP_VAT_PERCENTAGE,RM_MRD_TRANS_VAT_TYPE_CODE,RM_MRM_TRANS_VAT_PERCENTAGE,  ";
                SQL = SQL + " SALES_STN_STATION_CODE_PO_SUP RECEDSTN_CODE,RECEIVEDSTATION.SALES_STN_STATION_NAME RECEDSTN_NAME ";
                 
                SQL = SQL + " FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";

                SQL = SQL + " RM_UOM_MASTER,";
                SQL = SQL + " SL_STATION_MASTER,";
                SQL = SQL + " SL_STATION_MASTER RECEIVEDSTATION,";
                SQL = SQL + " RM_SOURCE_MASTER,AD_BRANCH_MASTER, ";
                SQL = SQL + " RM_VENDOR_MASTER,TECH_PLANT_MASTER  , RM_UOM_TOLL_MASTER ";
                SQL = SQL + " WHERE RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.TECH_PLM_PLANT_CODE = TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE (+)";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE =SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE_PO_SUP =RECEIVEDSTATION.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UOM_TOLL_CODE =RM_UOM_TOLL_MASTER.RM_UOM_TOLL_CODE (+) ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE =AD_BRANCH_MASTER.AD_BR_CODE (+) ";

                SQL = SQL + "   AND  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ='" + DocNo + "' AND  RM_RECEPIT_DETAILS.AD_FIN_FINYRID =" + iFinYear + "";
                dtMaster = clsSQLHelper.GetDataTableByCommand(SQL);
                dtReturn.Tables.Add(dtMaster);

                SQL = "SELECT  ";
                SQL = SQL + "RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE, RM_VM_VENDOR_NAME,RM_MPOM_ORDER_NO,";
                SQL = SQL + "   RM_MRM_TOLL_NO, RM_MRM_TOLL_RATE, RM_MRM_TOLL_AMOUNT,RM_RECEPIT_TOLL.RM_TL_LOCATION_CODE,  ";
                SQL = SQL + "   RM_TL_LOCATION_DESC,RM_MRM_TOLL_PE_DONE_YN, RM_MRM_TOLL_PE_NO,RM_MRM_TOLL_Date , ";
                SQL = SQL + "   RM_RECEPIT_TOLL.SALES_STN_STATION_CODE,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE,RM_SM_SOURCE_DESC,";
                SQL = SQL + "  RM_RECEPIT_TOLL.RM_RMM_RM_CODE,RM_RMM_RM_DESCRIPTION,INVOICE_CALC_CODE, ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_UOM_UOM_CODE ,RM_UM_UOM_DESC, RM_MRM_TOLL_QTY,RM_MRM_VAT_TYPE_CODE, RM_MRM_VAT_PERCENTAGE ";
                SQL = SQL + "FROM RM_RECEPIT_TOLL ,RM_VENDOR_MASTER,RM_RECEPIT_TOLL_LOCATION_MAST , ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER ,RM_SOURCE_MASTER,RM_UOM_MASTER,RM_UOM_CATEGORY_MASTER ";
                SQL = SQL + "    where ";
                SQL = SQL + "    RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE=RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_UOM_UOM_CODE=RM_UOM_MASTER.RM_UM_UOM_CODE(+)  ";
                SQL = SQL + "    AND  RM_UOM_MASTER.RM_UOM_CAT_CODE= RM_UOM_CATEGORY_MASTER.RM_UOM_CAT_CODE " ;
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_TL_LOCATION_CODE =RM_RECEPIT_TOLL_LOCATION_MAST.RM_TL_LOCATION_CODE(+)  ";
                SQL = SQL + "  AND  RM_MRM_RECEIPT_NO ='" + DocNo + "' and  AD_FIN_FINYRID =" + iFinYear + "  and RM_MRM_TOLL_ENTRYTYPE='WITHLPO' ";
                dtTollDetails = clsSQLHelper.GetDataTableByCommand(SQL);
                dtReturn.Tables.Add(dtTollDetails);


                SQL = "SELECT  ";
                SQL = SQL + "RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE, RM_VM_VENDOR_NAME,RM_MPOM_ORDER_NO,";
                SQL = SQL + "   RM_MRM_TOLL_NO, RM_MRM_TOLL_RATE, RM_MRM_TOLL_AMOUNT,RM_RECEPIT_TOLL.RM_TL_LOCATION_CODE,  ";
                SQL = SQL + "   RM_TL_LOCATION_DESC,RM_MRM_TOLL_PE_DONE_YN, RM_MRM_TOLL_PE_NO,RM_MRM_TOLL_Date , ";
                SQL = SQL + "   RM_RECEPIT_TOLL.SALES_STN_STATION_CODE,RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE,RM_SM_SOURCE_DESC,";
                SQL = SQL + "  RM_RECEPIT_TOLL.RM_RMM_RM_CODE,RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + " RM_RECEPIT_TOLL.RM_UOM_UOM_CODE ,RM_UM_UOM_DESC, RM_MRM_TOLL_QTY,RM_MRM_VAT_TYPE_CODE, RM_MRM_VAT_PERCENTAGE  ";
                SQL = SQL + "FROM RM_RECEPIT_TOLL ,RM_VENDOR_MASTER,RM_RECEPIT_TOLL_LOCATION_MAST , ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER ,RM_SOURCE_MASTER,RM_UOM_MASTER";
                SQL = SQL + "    where ";
                SQL = SQL + "    RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_SM_SOURCE_CODE=RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_UOM_UOM_CODE=RM_UOM_MASTER.RM_UM_UOM_CODE(+)  ";
                SQL = SQL + "  AND  RM_RECEPIT_TOLL.RM_TL_LOCATION_CODE =RM_RECEPIT_TOLL_LOCATION_MAST.RM_TL_LOCATION_CODE(+)  ";
                SQL = SQL + "  AND  RM_MRM_RECEIPT_NO ='" + DocNo + "' and  AD_FIN_FINYRID =" + iFinYear + "  and RM_MRM_TOLL_ENTRYTYPE='WITHOUTLPO' ";
                dtTollDetailWithOUTLPO = clsSQLHelper.GetDataTableByCommand(SQL);
                dtReturn.Tables.Add(dtTollDetailWithOUTLPO);
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
            return dtReturn;

        }

        public DataTable GetVendorName ( string ItemCode )//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                //SessionManager mngrclass = (SessionManager)mngrclassobj;
                SQL = " select    RM_VM_VENDOR_NAME from  RM_VENDOR_MASTER \t where RM_VM_VENDOR_CODE   ='" + ItemCode + "'";
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

        public DataTable FetchOrderNoData ( string oCode, string soCode, string rmCode, string stCode, int finyr, object mngrclassobj )//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "  ";
                SQL = " SELECT   RM_PO_MASTER.RM_PO_PONO orderno, ";  
                SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE code ,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME vendorname,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME stationname,";
                SQL = SQL + " RM_PO_DETAILS.RM_RMM_RM_CODE rmcode,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION rmname,";
                SQL = SQL + " RM_PO_PRICETYPE potype ,RM_POD_RM_VAT_TYPE,RM_POD_RM_VAT_RATE, ";
                SQL = SQL + " RM_PO_DETAILS.RM_SM_SOURCE_CODE sourcecode, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC sourcename,";
                SQL = SQL + "RM_PO_PO_DATE orderdate, ";
                SQL = SQL + " RM_PO_MASTER.AD_FIN_FINYRID finyrid, ";
                SQL = SQL + " RM_PO_DETAILS.RM_POD_SL_NO  slno, RM_RMM_RECEIPT_QTY_VARIENCE, ";
                SQL = SQL + "  RM_PO_DETAILS.RM_UOM_TOLL_CODE ,  RM_UOM_TOLL_NAME ,  RM_PO_DETAILS.RM_POD_TOLL_RATE,RM_PO_MASTER.RM_PO_QTY_RATE_TYPE,RM_PO_MASTER.RM_PO_RATE_REVISION_APP_YN  ";

                SQL = SQL + " FROM RM_PO_MASTER,";
                SQL = SQL + " RM_VENDOR_MASTER,";
                SQL = SQL + " RM_PO_DETAILS,";
                SQL = SQL + " SL_STATION_MASTER,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER , RM_SOURCE_MASTER , RM_UOM_TOLL_MASTER ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + "   RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO ";
                SQL = SQL + "    AND RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID ";
                SQL = SQL + "      AND RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE  ";
                SQL = SQL + "     AND RM_PO_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE  ";
                SQL = SQL + "     AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                SQL = SQL + "       AND RM_PO_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE  ";
                SQL = SQL + " AND RM_PO_DETAILS.RM_UOM_TOLL_CODE = RM_UOM_TOLL_MASTER.RM_UOM_TOLL_CODE (+) ";
                SQL = SQL + " AND  RM_PO_MASTER.RM_PO_PO_STATUS = 'O' and RM_PO_APPROVED ='Y' and RM_PO_MASTER.RM_PO_ENTRY_TYPE ='PURCHASE'";
                SQL = SQL + "  AND   RM_PO_DETAILS.RM_SM_SOURCE_CODE =  '" + soCode + "'";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE = '" + rmCode + "'";
                SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE = '" + stCode + "'";
                SQL = SQL + " AND RM_PO_MASTER.AD_FIN_FINYRID = '" + finyr + "'";
                SQL = SQL + " AND RM_PO_MASTER.RM_PO_PONO = '" + oCode + "' ";

                /// Disscued with jomy sir and alex , Comented // 17 05 2025  
                if (mngrclass.UserName != "ADMIN")
                {
                    SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE in  ";
                    SQL = SQL + "( SELECT     SALES_STN_STATION_CODE FROM    WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + mngrclass.UserName + "')";

                }
                SQL = SQL + " ORDER BY RM_PO_MASTER.RM_PO_PONO DESC";

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

        public DataSet FetchPOData ( string DocNo, int iFinYear, int iSlno, string RmCode )
        {
            DataSet dtReturn = new DataSet();

            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                // SessionManager mngrclass = (SessionManager)mngrclassobj;
                SQL = " SELECT   RM_PO_MASTER.RM_PO_PONO ORDER_NO,  RM_PO_MASTER.AD_FIN_FINYRID FINYRID,  RM_POD_SL_NO, ";
                SQL = SQL + "RM_PO_PO_DATE ORDER_DATE, ";
                SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE VENDOR_CODE ,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME VENDOR_NAME,";
                SQL = SQL + " RM_PO_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";

                SQL = SQL + " RM_PO_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_NAME,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC                    UOMNAME, ";
                SQL = SQL + " RM_PO_PRICETYPE POTYPE ,RM_POD_RM_VAT_TYPE,RM_POD_RM_VAT_RATE, ";
                // DECODE(RM_PO_MASTER.RM_MPOM_PRICETYPE,'O' , 'ONSITE', 'EX-FACTORY') POTYPE, "

                SQL = SQL + " RM_PO_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_NAME,";
                SQL = SQL + " RM_PO_DETAILS.RM_UOM_UOM_CODE UOM_CODE, ";
                SQL = SQL + " RM_PO_DETAILS.HR_DEPT_DEPT_CODE DEPT_CODE,";
                SQL = SQL + " RM_POD_QTY ,RM_POD_UNIT_PRICE, RM_POD_NEWPRICE , RM_PO_MASTER.RM_PO_UOM_TYPE,RM_VENDOR_MASTER.RM_VM_INVOICE_QTY, RM_PO_RM_TYPE ,";
                SQL = SQL + "  RM_PO_DETAILS.RM_UOM_TOLL_CODE ,  RM_UOM_TOLL_NAME ,  RM_PO_DETAILS.RM_POD_TOLL_RATE, ";
                SQL = SQL + " RM_PO_MASTER.AD_BR_CODE, AD_BRANCH_MASTER.AD_BR_NAME, RM_RMM_PRODUCT_TYPE ,RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE ";

                SQL = SQL + " FROM RM_PO_MASTER,";
                SQL = SQL + " RM_VENDOR_MASTER,";
                SQL = SQL + " RM_PO_DETAILS,";
                SQL = SQL + " SL_STATION_MASTER,AD_BRANCH_MASTER,RM_UOM_MASTER,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER , RM_SOURCE_MASTER , RM_UOM_TOLL_MASTER ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + "  RM_PO_DETAILS.RM_POD_STATUS = 'O'";
                SQL = SQL + "  AND RM_PO_DETAILS.RM_SM_SOURCE_CODE =  RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " AND RM_PO_DETAILS.RM_UOM_TOLL_CODE = RM_UOM_TOLL_MASTER.RM_UOM_TOLL_CODE (+)";

                SQL = SQL + "  AND RM_PO_DETAILS.RM_UOM_UOM_CODE  =  RM_UOM_MASTER.RM_UM_UOM_CODE(+) ";
                SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE =  SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO ";
                SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE(+) ";
                SQL = SQL + " AND RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID";
                SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " AND RM_PO_DETAILS.RM_POD_SL_NO =" + iSlno + "";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE = '" + RmCode + "'";
                SQL = SQL + " AND RM_PO_DETAILS.RM_PO_PONO ='" + DocNo + "'";
                SQL = SQL + " AND RM_PO_DETAILS.AD_FIN_FINYRID =" + iFinYear + "";


                dtReturn = clsSQLHelper.GetDataset(SQL);

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
            return dtReturn;

        }

        public DataSet FetchBalanceQty ( string DocNo, int iFinYear, Int16 iorderSlno, string RmCode,string SourceCode )
        {
            DataSet dtReturn = new DataSet();

            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                // SessionManager mngrclass = (SessionManager)mngrclassobj;
                //SQL = " SELECT  NVL(SUM(RM_MRD_SUPPLY_QTY),0) SUPPLY_QTY  FROM RM_RECEPIT_DETAILS where  RM_MPOM_ORDER_NO = '" + DocNo + "'  ";
                //SQL = SQL + "  and  RM_POD_SL_NO   = " + iorderSlno + " and  RM_MPOM_FIN_FINYRID = " + iFinYear + " and  RM_RMM_RM_CODE = '" + RmCode + "' ";

                //TONY CORRECTECD THIS ON  22/05/2025

                SQL = " SELECT  NVL(SUM(RM_MRD_SUPPLY_QTY),0) SUPPLY_QTY  FROM RM_RECEPIT_DETAILS where  RM_MPOM_ORDER_NO = '" + DocNo + "'  ";
                SQL = SQL + "  and  RM_SM_SOURCE_CODE   = '" + SourceCode + "' and  RM_MPOM_FIN_FINYRID = " + iFinYear + " and  RM_RMM_RM_CODE = '" + RmCode + "' ";

                dtReturn = clsSQLHelper.GetDataset(SQL);

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
            return dtReturn;

        }

        public DataSet FetchBalanceExcludeCurrentReceiptQty(string DocNo, int iFinYear, Int16 iorderSlno, string RmCode,string ReceiptNo,string SourceCode)
        {
            DataSet dtReturn = new DataSet();

            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                // SessionManager mngrclass = (SessionManager)mngrclassobj;
                //SQL = " SELECT  NVL(SUM(RM_MRD_SUPPLY_QTY),0) SUPPLY_QTY  FROM RM_RECEPIT_DETAILS where  RM_MPOM_ORDER_NO = '" + DocNo + "'";
                //SQL = SQL + "  and  RM_POD_SL_NO   = " + iorderSlno + " and  RM_MPOM_FIN_FINYRID = " + iFinYear + " and  RM_RMM_RM_CODE = '" + RmCode + "'";


                SQL = " SELECT  NVL(SUM(RM_MRD_SUPPLY_QTY),0) SUPPLY_QTY  FROM RM_RECEPIT_DETAILS where  RM_MPOM_ORDER_NO = '" + DocNo + "'";
                SQL = SQL + "  and  RM_SM_SOURCE_CODE   = '"+ SourceCode +"' and  RM_MPOM_FIN_FINYRID = " + iFinYear + " and  RM_RMM_RM_CODE = '" + RmCode + "'";


                if (!string.IsNullOrEmpty(ReceiptNo))
                {
                    SQL = SQL + "and RM_MRM_RECEIPT_NO<>'" + ReceiptNo + "' ";
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
                ocConn.Close();
                ocConn.Dispose();
            }
            return dtReturn;

        }
        
        public DataSet FetchPoOrderedQty(string DocNo,string RmCode,string SrcCode)
        {
            DataSet dtReturn = new DataSet();

            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                // SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "   select Sum(RM_POD_QTY) RM_POD_QTY ";
                SQL = SQL + "            from RM_PO_DETAILS  ";
                SQL = SQL + "           where RM_PO_PONO='"+DocNo+"'   ";
                SQL = SQL + "             AND RM_RMM_RM_CODE='"+RmCode+"'  ";
                SQL = SQL + "             AND RM_SM_SOURCE_CODE='"+SrcCode+"'  ";
                //SQL = SQL + " and round(RM_POD_NEWPRICE,4)='"+UnitPrice+"' ";

                dtReturn = clsSQLHelper.GetDataset(SQL);

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
            return dtReturn;

        }

        private string CloseOpenCheck ( ReceiptEntryRmEntity oEntity )
        {
            DataSet dsCnt = new DataSet();
            SqlHelper clsSQLHelper = new SqlHelper();
            try
            {


                // SQL Statment for fethiing the  opening 


                SQL = "  select  count(*) CNT  FROM   RM_PO_DETAILS  ";

                SQL = SQL + " WHERE RM_POD_STATUS ='O' AND  RM_PO_DETAILS.RM_PO_PONO='" + oEntity.OrderNo + "'";
                SQL = SQL + " AND RM_PO_DETAILS.AD_FIN_FINYRID='" + oEntity.orderfinYrId + "'";
                //SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE='" + oEntity.StationCode + "'";
                //SQL = SQL + " AND RM_PO_DETAILS.RM_SM_SOURCE_CODE='" + oEntity.SourceCode + "'";
                //SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE='" + oEntity.RmCode + "'";

                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "cannot  CLOSE SINCE PENDING ITEMS EIXSTS";
                }






            }
            catch (Exception ex)
            {
                return System.Convert.ToString(ex.Message);
            }
            return "CONTINUE";
        }

        public DataTable FetchWeighBridgeData ( string Id )//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                //SQL = "SELECT  WEIGHBRIDGEID, Max(TRNSDATE) TRNSDATE,";
                //SQL = SQL + " sum(WEIGHTDATA) WEIGHTDATA";                
                //SQL = SQL + " FROM   TBL_WEIGHTTRACK  ";
                //SQL = SQL + " WHERE  WEIGHBRIDGEID = '"+Id+"'";
                //SQL = SQL + " group by WEIGHBRIDGEID";

                SQL = "SELECT  WEIGHBRIDGEID, TRNSDATE ,WEIGHTDATA";
                SQL = SQL + " FROM  (SELECT  WEIGHBRIDGEID, TRNSDATE ,WEIGHTDATA from TBL_WEIGHTTRACK";
                SQL = SQL + " WHERE  WEIGHBRIDGEID = '" + Id + "' order by TRNSDATE desc)";
                SQL = SQL + " where rownum = 1";

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


        #region "Desity of Raw material"
        public DataTable FetchRMDensity(string RMCode)
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT   ";
                SQL = SQL + "   NVL(RM_RMM_MAT_DENSITY,0) RM_RMM_MAT_DENSITY ";
                SQL = SQL + "FROM   ";
                SQL = SQL + "   RM_RAWMATERIAL_MASTER ";
                SQL = SQL + "WHERE RM_RMM_RM_CODE ='" + RMCode + "'  ";


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

        #region "Validate Vendor document number " 


        public DataSet ValidateDocumentNumber ( string vendocmentnumber, string vendorCode, string receiptNumber, string transdocumentnumber, string transporterCode, DateTime entryDate, int FinYearId, string sweightdocNo, string StationCode )
        {
            DataSet dsReturn = new DataSet("RM_DETAILS");
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();

                OracleCommand ocCommand = new OracleCommand("PK_RM_RECEPIT_DOC_VALID.ValidateVendorDocumentNumber");
                ocCommand.Connection = ocConn;
                ocCommand.CommandType = CommandType.StoredProcedure;

                ////ocCommand.Parameters.Add("vendocmentnumber", OracleType.VarChar).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("vendocmentnumber", OracleType.VarChar).Value = vendocmentnumber;


                ////ocCommand.Parameters.Add("vendorCode", OracleType.VarChar).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("vendorCode", OracleType.VarChar).Value = vendorCode;



                ////ocCommand.Parameters.Add("receiptNumber", OracleType.VarChar).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("receiptNumber", OracleType.VarChar).Value = receiptNumber;

                ////ocCommand.Parameters.Add("transdocumentnumber", OracleType.VarChar).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("transdocumentnumber", OracleType.VarChar).Value = transdocumentnumber;


                ////ocCommand.Parameters.Add("transporterCode", OracleType.VarChar).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("transporterCode", OracleType.VarChar).Value = transporterCode;


                ////ocCommand.Parameters.Add("entryDate", OracleType.DateTime).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("entryDate", OracleType.DateTime).Value = entryDate.ToString("dd-MMM-yyyy");

                ////ocCommand.Parameters.Add("finYearId", OracleType.Int32).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("finYearId", OracleType.Int32).Value = FinYearId;

                ////ocCommand.Parameters.Add("weightdocNo", OracleType.VarChar).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("weightdocNo", OracleType.VarChar).Value = sweightdocNo;

                //=========================


                ocCommand.Parameters.Add("vendocmentnumber", OracleDbType.Varchar2, vendocmentnumber, ParameterDirection.Input);
                ocCommand.Parameters.Add("vendorCode", OracleDbType.Varchar2, vendorCode, ParameterDirection.Input);
                ocCommand.Parameters.Add("StationCode", OracleDbType.Varchar2, StationCode, ParameterDirection.Input);
                ocCommand.Parameters.Add("receiptNumber", OracleDbType.Varchar2, receiptNumber, ParameterDirection.Input);
                ocCommand.Parameters.Add("transdocumentnumber", OracleDbType.Varchar2, transdocumentnumber, ParameterDirection.Input);
                ocCommand.Parameters.Add("transporterCode", OracleDbType.Varchar2, transporterCode, ParameterDirection.Input);
                ocCommand.Parameters.Add("entryDate", OracleDbType.Date, entryDate, ParameterDirection.Input);
                ocCommand.Parameters.Add("finYearId", OracleDbType.Decimal, FinYearId, ParameterDirection.Input);
                ocCommand.Parameters.Add("weightdocNo", OracleDbType.Varchar2, sweightdocNo, ParameterDirection.Input);
                ocCommand.Parameters.Add("cur_return", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                OracleDataAdapter odAdpt = new OracleDataAdapter(ocCommand);

                odAdpt.Fill(dsReturn);
            }
            catch (Exception ex)
            {

            }
            return dsReturn;
        }
       
        public int ValidateToken ( string Supplier,string Station,string RMCode )//
        { 
            DataTable dt = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper(); 
                 

                SQL = "SELECT count(*) CNT FROM RM_RECEPIT_TOKEN_MST WHERE RM_RT_STATUS='O' And RM_VM_VENDOR_CODE='"+Supplier+"' And SALES_STN_STATION_CODE='"+Station+ "' AND RM_RMM_RM_CODE='" + RMCode + "'  ";

                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return int.Parse(dt.Rows[0]["CNT"].ToString());
        }
        
        #endregion 

        #region "Insert/Update/Delete UML"

        public string InsertSql ( ReceiptEntryRmEntity oEntity, List<ReceiptEntryRmTollGridEntity> objTollGridEntity, object mngrclassobj, string sDocNo, 
            bool autogen, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber )
        {
            //string fsESOBProv = string.Empty;
            //string fsESOBExp = string.Empty;
            //string fsLSProv = string.Empty;
            //string fsLSExp = string.Empty;
            //string fsATProv = string.Empty;
            //string fsATExp = string.Empty;

            string sRetun = string.Empty;

            string sAppFlag = "N";
            // object oCode;
            OracleHelper oTrns = new OracleHelper();
            SessionManager mngrclass = (SessionManager)mngrclassobj;

            sSQLArray.Clear();

            try
            {



                string sInternalExternal = null;
                if (oEntity.DriverType == "I")
                {
                    sInternalExternal = "Yes";
                }
                else
                {
                    sInternalExternal = "No";
                }

                DateTime dReceiptTime = System.Convert.ToDateTime(oEntity.EntryDate.ToString("dd-MMM-yyyy") + " " + oEntity.EntryTime);



                sSQLArray.Clear();

                SQL = " INSERT INTO RM_RECEIPT_MASTER (";
                SQL = SQL + " RM_MRM_RECEIPT_NO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID, ";
                SQL = SQL + " RM_MRM_RECEIPT_DATE,  ";
                SQL = SQL + " RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, ";
                SQL = SQL + " RM_VM_VENDOR_CODE, RM_MRM_REMARKS, RM_MRM_GRAND_TOTAL, ";
                SQL = SQL + " RM_MRM_APPROVED_STATUS, RM_MPOM_PRICE_TYPE, RM_MRM_CREATEDBY, ";
                SQL = SQL + " RM_MRM_CREATEDDATE, RM_MRM_UPDATEDBY, RM_MRM_UPDATEDDATE, ";
                SQL = SQL + " RM_MRM_RECEIPT_VERIFIED ,RM_MRM_RECEIPT_VERIFIED_BY,RM_MRM_RECEIPT_VERIFIEDDT,RM_MRM_RECEIPT_APPROVED,RM_MRM_RECEIPT_APPROVE_BY,RM_MRM_RECEIPT_APPROVEDDT  ,";
                SQL = SQL + " RM_MRM_DELETESTATUS,RM_MRM_STOCK_YN,RM_PO_RM_TYPE,AD_BR_CODE,RM_MRM_RECEIPT_DATE_TIME,RM_PO_RATE_REVISION_APP_YN,RM_RT_TOKEN_NO) ";
                SQL = SQL + " VALUES ('" + oEntity.EntryNo + "', '" + mngrclass.CompanyCode + "'," + mngrclass.FinYearID + " ,";
                SQL = SQL + "'" + oEntity.EntryDate.ToString("dd-MMM-yyyy") + "'  ,";

                SQL = SQL + "'" + oEntity.OrderNo + "' ,  " + oEntity.orderfinYrId + " ,";
                SQL = SQL + "'" + oEntity.SupplierCode + "' ,'" + oEntity.Remarks + "' ,0 ,";
                SQL = SQL + "'N' ,'" + oEntity.PriceType + "' ,'" + mngrclass.UserName + "' ,";
                SQL = SQL + "TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM') ,'' , '',";
                SQL = SQL + "'" + oEntity.Verified + "','" + oEntity.VerifiedBy + "', TO_DATE('" + oEntity.VerifiedTime + "', 'DD-MM-YYYY HH:MI:SS AM'),'" + oEntity.Approved + "','" + oEntity.ApprovedBy + "',TO_DATE('" + oEntity.ApprovedTime + "', 'DD-MM-YYYY HH:MI:SS AM'), ";
                SQL = SQL + "  0 ,'" + oEntity.TempStock + "','" + oEntity.PoReceiptType + "' ,'" + oEntity.Branch + "',TO_DATE('" + dReceiptTime.ToString("dd-MMM-yyyy hh:mm:ss tt") + "', 'DD-MM-YYYY HH:MI:SS AM')  ,";
                SQL = SQL + "  '"+oEntity.RateRevisionApplicable+"','"+oEntity.TokenNo+"'   )";

                sSQLArray.Add(SQL);


                SQL = " INSERT INTO RM_RECEPIT_DETAILS (";
                SQL = SQL + " RM_MRM_RECEIPT_UNIQE_NO, RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_MRM_RECEIPT_DATE,  RM_MRM_RECEIPT_DATE_TIME , RM_VM_VENDOR_CODE,  ";
                SQL = SQL + " RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, RM_POD_SL_NO, RM_MPOM_PRICE_TYPE, ";
                SQL = SQL + " RM_MRD_SL_NO, AD_CM_COMPANY_CODE, SALES_STN_STATION_CODE, ";
                SQL = SQL + " HR_DEPT_DEPT_CODE, RM_RMM_RM_CODE, RM_UM_UOM_CODE,RM_RMM_MRD_QTY_VARIENCE_BASEUM,  ";
                SQL = SQL + " RM_SM_SOURCE_CODE,     RM_INTERNALOREXTENAL ,RM_MTRANSPO_ORDER_NO, RM_MTRANSPO_FIN_FINYRID,  RM_POD_SL_NO_TRANS , ";

                SQL = SQL + " RM_MRM_TRANSPORTER_CODE, RM_MRM_TRNSPORTER_DOC_NO, RM_MRD_TRIPNO, ";
                SQL = SQL + " FA_FAM_ASSET_CODE, RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME,";

                SQL = SQL + " RM_MRD_VENDOR_DOC_NO, RM_MRD_ORDER_QTY, RM_MRD_SUPPLY_QTY,";
                SQL = SQL + " RM_MRD_MOIST_LOSS_PERCENTAGE,RM_MRD_MOIST_LOSS_QTY, ";
                SQL = SQL + " RM_MRD_WEIGH_QTY, RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY, ";

                SQL = SQL + " RM_MRD_SUPP_UNIT_PRICE, RM_MRD_SUPP_AMOUNT, RM_MRD_TRANS_RATE, ";
                SQL = SQL + " RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT, RM_MRD_SUPP_UNIT_PRICE_NEW, ";

                SQL = SQL + " RM_MRD_SUPP_AMOUNT_NEW, RM_MRD_TRANS_RATE_NEW, RM_MRD_TRANS_AMOUNT_NEW, ";
                SQL = SQL + " RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_APPROVED, RM_MRM_APPROVED_NO, ";
                SQL = SQL + " RM_MRD_APPROVED_FINYRID, RM_MRD_SUPP_ENTRY, RM_MRD_SUPP_PENO, ";
                SQL = SQL + " RM_MPED_VFINYR_ID, RM_MRD_TRANS_ENTRY, RM_MRD_TRANS_PENO, ";
                SQL = SQL + " RM_MPED_TFINYR_ID , RM_MRD_WEIGH_BRIDGE_DOC_NO,TECH_PLM_PLANT_CODE,";
                SQL = SQL + " RM_MRD_REJECTED_YN  ,RM_PO_UOM_TYPE,RM_VM_INVOICE_QTY,RM_MRM_STOCK_YN,";
                SQL = SQL + " RM_MRD_FIRST_WEIGHT_TIME,RM_MRD_FRIST_WEIGHT_QTY,RM_MRD_SECOND_WEIGHT_TIME,";
                SQL = SQL + " RM_MRD_SECOND_WEIGHT_QTY,RM_MRD_READING_STATUS,RM_MRD_ENTRY_MODE,RM_MRD_WEIGH_BRIDGE_ID ,RM_PO_RM_TYPE, ";
                SQL = SQL + " RM_UOM_TOLL_CODE  , RM_POD_TOLL_RATE  , RM_MRD_TOLL_AMOUNT	 ,  ";
                SQL = SQL + "    RM_POD_TOLL_RATE_NEW  , RM_MRD_TOLL_AMOUNT_NEW	 ,";
                SQL = SQL + "   RM_MRD_TOLL_PE_ENTRY_NO , RM_MRD_TOLL_PE_ENTRY_FINYR_ID,RM_MRD_TOLL_PE_ENTRY_YN ,AD_BR_CODE, ";
                SQL = SQL + "   RM_MRD_SUPP_VAT_TYPE_CODE,RM_MRM_SUPP_VAT_PERCENTAGE,RM_MRD_TRANS_VAT_TYPE_CODE,RM_MRM_TRANS_VAT_PERCENTAGE,SALES_STN_STATION_CODE_PO_SUP ";

                SQL = SQL + " ) ";



                SQL = SQL + " VALUES ( ";
                SQL = SQL + " RECEPT_SEQ.NEXTVAL, ";

                SQL = SQL + "'" + oEntity.EntryNo + "', " + mngrclass.FinYearID + ", '" + oEntity.EntryDate.ToString("dd-MMM-yyyy") + "',  ";
                SQL = SQL + " TO_DATE('" + dReceiptTime.ToString("dd-MMM-yyyy hh:mm:ss tt") + "', 'DD-MM-YYYY HH:MI:SS AM') ,";
                SQL = SQL + "'" + oEntity.SupplierCode + "',";
                SQL = SQL + "  '" + oEntity.OrderNo + "' ," + oEntity.orderfinYrId + " ," + oEntity.PoSLNO + ", '" + oEntity.PriceType + "',";
                SQL = SQL + "1 ,'" + mngrclass.CompanyCode + "' ,'" + oEntity.StationCode + "' ,";
                SQL = SQL + "'' ,'" + oEntity.RmCode + "' ,'" + oEntity.UomCode + "' , '"+oEntity.VarienceQty+"', ";
                SQL = SQL + "'" + oEntity.SourceCode + "' ,'" + sInternalExternal + "', '" + oEntity.TransPoNo + "' ,'" + oEntity.TransPoFinYrId + "' ,";
                SQL = SQL + " '" + oEntity.TranPoSLno + "', ";

                SQL = SQL + "'" + oEntity.Transportercode + "', '" + oEntity.TransDocNO + "', 1,";
                SQL = SQL + "'" + oEntity.AssetCode + "' , '" + oEntity.TrailerName + "', '" + oEntity.DriverCode + "','" + oEntity.Drivername + "',";

                SQL = SQL + "'" + oEntity.SuppDocno + "' ," + Convert.ToDouble(oEntity.OrderQty) + " , " + Convert.ToDouble(oEntity.SuppQty) + " ,";
                SQL = SQL + " " + Convert.ToDouble(oEntity.MoistureLossPercentage) + " ," + Convert.ToDouble(oEntity.MoistureLossQTY) + " ,";
                SQL = SQL + "" + Convert.ToDouble(oEntity.MeasuredQty) + " , " + Convert.ToDouble(oEntity.ApprovedQty) + " ,";
                SQL = SQL + " " + Convert.ToDouble(oEntity.RejectedQty) + " ,";

                SQL = SQL + "" + Convert.ToDouble(oEntity.UnitRate) + ", " + Convert.ToDouble(oEntity.UnitAmount) + " ,  " + Convert.ToDouble(oEntity.TransUnitRate) + " ,";
                SQL = SQL + "" + Convert.ToDouble(oEntity.TransUnitAmount) + " , " + Convert.ToDouble(oEntity.TotalAmount) + " ," + Convert.ToDouble(oEntity.UnitRate) + " ,";

                SQL = SQL + "" + Convert.ToDouble(oEntity.UnitAmount) + "  , " + Convert.ToDouble(oEntity.TransUnitRate) + "," + Convert.ToDouble(oEntity.TransUnitAmount) + ",";
                SQL = SQL + " " + Convert.ToDouble(oEntity.TotalAmount) + " ,'N' ,'' ,";
                SQL = SQL + "0 ,'N' ,'' ,";
                SQL = SQL + "0 ,'N' ,'' ,0 ,'" + oEntity.txtWeightBridgeNo + "','" + oEntity.PlantCode + "', '" + oEntity.rejectedYN + "','" + oEntity.RM_PO_UOM_TYPE + "',";
                SQL = SQL + " '" + oEntity.Inv_Qty_Type + "','" + oEntity.TempStock + "', TO_DATE('" + oEntity.FirstTime.ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM') ," + oEntity.FirstQty + ",";
                SQL = SQL + " TO_DATE('" + oEntity.SecondTime.ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM') ," + oEntity.SecondQty + ",'" + oEntity.ReadingStatus + "',";
                SQL = SQL + " '" + oEntity.Entrymode + "','" + oEntity.WeighBridgeID + "','" + oEntity.PoReceiptType + "',";

                SQL = SQL + "'" + oEntity.TollUOMCode + "'," + Convert.ToDouble(oEntity.TollUnitRate) + ", " + Convert.ToDouble(oEntity.TollUnitAmount) + ",";
                SQL = SQL + "" + Convert.ToDouble(oEntity.TollUnitRate) + ", " + Convert.ToDouble(oEntity.TollUnitAmount) + ",";
                SQL = SQL + " '', 0, 'N','" + oEntity.Branch + "',";
                SQL = SQL + " '" + oEntity.VendorMRTaxType + "', '" + oEntity.VendorMRTaxPerc + "','" + oEntity.TollTaxType + "','" + oEntity.TollTaxPerc + "','" + oEntity.PoStationCode + "' ";

                SQL = SQL + " )";


                sSQLArray.Add(SQL);

                SQL = "UPDATE RM_RECEPIT_TOKEN_MST  ";
                SQL = SQL + " SET RM_RT_STATUS = 'C'   ";
                SQL = SQL + " WHERE TO_DATE(RM_RT_TOKEN_DATE_TIME , 'DD-MM-YY HH24:MI:SS') < SYSDATE - INTERVAL '48' HOUR  ";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertTollDetailsSQL(oEntity.EntryNo, Convert.ToInt16(mngrclass.FinYearID), objTollGridEntity, "I");
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                if (!string.IsNullOrWhiteSpace(oEntity.TokenNo))
                {
                    sRetun = string.Empty;

                    sRetun = UpdateTokenStatus(oEntity.TokenNo,oEntity.EntryNo,oEntity.TokenStatus,mngrclass.FinYearID);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }
                sRetun = string.Empty;

                //sSQLArray.Add(ResSetNextDocNumberStation("RTMRC", sDocNo, mngrclass.FinYearID, oEntity.StationCode));

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRC", sDocNo, autogen, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);


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

        public string UpdateSql ( ReceiptEntryRmEntity oEntity, List<ReceiptEntryRmTollGridEntity> objTollGridEntity, object mngrclassobj )
        {
            //string fsESOBProv = string.Empty;
            //string fsESOBExp = string.Empty;
            //string fsLSProv = string.Empty;
            //string fsLSExp = string.Empty;
            //string fsATProv = string.Empty;
            //string fsATExp = string.Empty;

            string sRetun = string.Empty;

            string sAppFlag = "N";
            // object oCode;
            OracleHelper oTrns = new OracleHelper();
            SessionManager mngrclass = (SessionManager)mngrclassobj;

            sSQLArray.Clear();

            try
            {
                string sInternalExternal = null;
                if (oEntity.DriverType == "I")
                {
                    sInternalExternal = "Yes";
                }
                else
                {
                    sInternalExternal = "No";
                }

                sSQLArray.Clear();
                DateTime dReceiptTime = System.Convert.ToDateTime(oEntity.EntryDate.ToString("dd-MMM-yyyy") + " " + oEntity.EntryTime);

                SQL = " UPDATE  RM_RECEIPT_MASTER SET ";

                SQL = SQL + " RM_MRM_RECEIPT_DATE = '" + oEntity.EntryDate.ToString("dd-MMM-yyyy") + "', ";
               // SQL = SQL + " RM_PO_RATE_REVISION_APP_YN = '" + oEntity.RateRevisionApplicable + "', ";
                SQL = SQL + "  RM_MRM_RECEIPT_DATE_TIME = TO_DATE('" + dReceiptTime.ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM') ,";


                //' , RM_MPOM_ORDER_NO =, RM_MPOM_FIN_FINYRID, "
                //'SQL = SQL & " RM_VM_VENDOR_CODE, RM_MRM_REMARKS, RM_MRM_GRAND_TOTAL, "
                //'SQL = SQL & " RM_MRM_APPROVED_STATUS, RM_MPOM_PRICE_TYPE, RM_MRM_CREATEDBY, "

                SQL = SQL + " RM_MRM_RECEIPT_VERIFIED ='" + oEntity.Verified + "' ,RM_MRM_RECEIPT_VERIFIED_BY = '" + oEntity.VerifiedBy + "',RM_MRM_RECEIPT_VERIFIEDDT = TO_DATE('" + oEntity.VerifiedTime + "', 'DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " RM_MRM_RECEIPT_APPROVED = '" + oEntity.Approved + "' , RM_MRM_RECEIPT_APPROVE_BY = '" + oEntity.ApprovedBy + "', RM_MRM_RECEIPT_APPROVEDDT= TO_DATE('" + oEntity.ApprovedTime + "', 'DD-MM-YYYY HH:MI:SS AM'),";

                SQL = SQL + "  RM_MRM_REMARKS='" + oEntity.Remarks + "' ,RM_MRM_STOCK_YN ='" + oEntity.TempStock + "' ,  RM_MRM_UPDATEDBY = '" + mngrclass.UserName + "' , RM_MRM_UPDATEDDATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + "  RM_RT_TOKEN_NO='" + oEntity.TokenNo + "' ";

                SQL = SQL + " WHERE  RM_MRM_RECEIPT_NO = '" + oEntity.EntryNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";


                //' SQL = SQL & " ,'" & lblorderno.text & "' ,  " & lblorderfinYrId.text & " ,"
                //' SQL = SQL & "'" & txtSupplierCode.text & "' ,'" & txtRemarks.Text & "' ,0 ,"
                //' SQL = SQL & "'N' ,'" & lblPriceType.text & "' , ,"


                sSQLArray.Add(SQL);

                // SQL = SQL & " HR_DEPT_DEPT_CODE, 
                SQL = " UPDATE    RM_RECEPIT_DETAILS  SET ";
                SQL = SQL + " RM_MRM_RECEIPT_DATE = '" + oEntity.EntryDate.ToString("dd-MMM-yyyy") + "' , ";
                SQL = SQL + "  RM_MRM_RECEIPT_DATE_TIME = TO_DATE('" + dReceiptTime.ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM') ,";



                //  SQL = SQL & " RM_MPOM_ORDER_NO= '" & lblOrderNo.Text & "' , RM_MPOM_FIN_FINYRID =" & lblorderfinYrId.Text & " , RM_MPOM_PRICE_TYPE = '" & lblPriceType.Text & "', "
                SQL = SQL + " RM_MRD_SL_NO =1 ,   SALES_STN_STATION_CODE ='" + oEntity.StationCode + "', ";
                SQL = SQL + "  RM_RMM_RM_CODE ='" + oEntity.RmCode + "', RM_UM_UOM_CODE ='" + oEntity.UomCode + "', ";
                SQL = SQL + " RM_SM_SOURCE_CODE ='" + oEntity.SourceCode + "',     RM_INTERNALOREXTENAL ='" + sInternalExternal + "' ,";
                SQL = SQL + "RM_MTRANSPO_ORDER_NO ='" + oEntity.TransPoNo + "' , RM_MTRANSPO_FIN_FINYRID ='" + oEntity.TransPoFinYrId + "',   RM_POD_SL_NO_TRANS ='" + oEntity.TranPoSLno + "',  ";

                SQL = SQL + " RM_MRM_TRANSPORTER_CODE ='" + oEntity.Transportercode + "', RM_MRM_TRNSPORTER_DOC_NO ='" + oEntity.TransDocNO + "',";
                SQL = SQL + " RM_MRD_TRIPNO =1 , ";
                SQL = SQL + " FA_FAM_ASSET_CODE ='" + oEntity.AssetCode + "', RM_MRM_VEHICLE_DESCRIPTION ='" + oEntity.TrailerName + "', ";
                SQL = SQL + " RM_MRD_DRIVER_CODE  ='" + oEntity.DriverCode + "' , RM_MRD_DRIVER_NAME ='" + oEntity.Drivername + "',";

                SQL = SQL + " RM_MRD_VENDOR_DOC_NO ='" + oEntity.SuppDocno + "', RM_MRD_ORDER_QTY =" + Convert.ToDouble(oEntity.OrderQty) + ", ";
                SQL = SQL + " RM_MRD_SUPPLY_QTY = " + Convert.ToDouble(oEntity.SuppQty) + ", ";
                SQL = SQL + " RM_MRD_MOIST_LOSS_PERCENTAGE = " + Convert.ToDouble(oEntity.MoistureLossPercentage) + ", ";
                SQL = SQL + " RM_MRD_MOIST_LOSS_QTY = " + Convert.ToDouble(oEntity.MoistureLossQTY) + ", ";
                SQL = SQL + " RM_MRD_WEIGH_QTY =" + Convert.ToDouble(oEntity.MeasuredQty) + ", RM_MRD_APPROVE_QTY =" + Convert.ToDouble(oEntity.ApprovedQty) + ",";
                SQL = SQL + " RM_MRD_REJECTED_QTY =" + Convert.ToDouble(oEntity.RejectedQty) + " , ";

                SQL = SQL + " RM_MRD_SUPP_UNIT_PRICE =" + Convert.ToDouble(oEntity.UnitRate) + ", RM_MRD_SUPP_AMOUNT = " + Convert.ToDouble(oEntity.UnitAmount) + " , ";
                SQL = SQL + " RM_MRD_TRANS_RATE = " + Convert.ToDouble(oEntity.TransUnitRate) + ", ";
                SQL = SQL + " RM_MRD_TRANS_AMOUNT =" + Convert.ToDouble(oEntity.TransUnitAmount) + " , ";
                SQL = SQL + " RM_MRD_TOTAL_AMOUNT =" + Convert.ToDouble(oEntity.TotalAmount) + ", ";

                SQL = SQL + " RM_UOM_TOLL_CODE  ='" + oEntity.TollUOMCode + "',";
                SQL = SQL + " RM_POD_TOLL_RATE= '" + oEntity.TollUnitRate + "', RM_MRD_TOLL_AMOUNT = " + Convert.ToDouble(oEntity.TollUnitAmount) + ",";

                SQL = SQL + " RM_POD_TOLL_RATE_NEW ='" + oEntity.TollUnitRate + "', RM_MRD_TOLL_AMOUNT_NEW = " + Convert.ToDouble(oEntity.TollUnitAmount) + ",";


                SQL = SQL + " RM_MRD_SUPP_UNIT_PRICE_NEW =" + Convert.ToDouble(oEntity.UnitRate) + ",  ";

                SQL = SQL + " RM_MRD_SUPP_AMOUNT_NEW =" + Convert.ToDouble(oEntity.UnitAmount) + ", ";
                SQL = SQL + " RM_MRD_TRANS_RATE_NEW =" + Convert.ToDouble(oEntity.TransUnitRate) + ", ";
                SQL = SQL + " RM_MRD_TRANS_AMOUNT_NEW =" + Convert.ToDouble(oEntity.TransUnitAmount) + ", ";
                SQL = SQL + " RM_MRD_TOTAL_AMOUNT_NEW =" + Convert.ToDouble(oEntity.TotalAmount) + ", ";  
                SQL = SQL + " RM_MRD_WEIGH_BRIDGE_DOC_NO = '" + oEntity.txtWeightBridgeNo + "',"; 


                //RM_MRD_SUPP_VAT_TYPE_CODE,RM_MRM_SUPP_VAT_PERCENTAGE,RM_MRD_TRANS_VAT_TYPE_CODE,RM_MRM_TRANS_VAT_PERCENTAGE  
                 SQL = SQL + " RM_MRD_SUPP_VAT_TYPE_CODE = '" + oEntity.VendorMRTaxType + "',";
                 SQL = SQL + " RM_MRM_SUPP_VAT_PERCENTAGE = '" + oEntity.VendorMRTaxPerc + "',";
                 SQL = SQL + " RM_MRD_TRANS_VAT_TYPE_CODE = '" + oEntity.TollTaxType + "',";
                 SQL = SQL + " RM_MRM_TRANS_VAT_PERCENTAGE = '" + oEntity.TollTaxPerc + "',";



                SQL = SQL + " TECH_PLM_PLANT_CODE = '" + oEntity.PlantCode + "',";
                SQL = SQL + " RM_MRD_REJECTED_YN = '" + oEntity.rejectedYN + "',";
                SQL = SQL + " RM_VM_INVOICE_QTY = '" + oEntity.Inv_Qty_Type + "',RM_MRM_STOCK_YN ='" + oEntity.TempStock + "',";
                SQL = SQL + " RM_MRD_FIRST_WEIGHT_TIME = TO_DATE('" + oEntity.FirstTime.ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM'),RM_MRD_FRIST_WEIGHT_QTY = " + oEntity.FirstQty + ",";
                SQL = SQL + " RM_MRD_SECOND_WEIGHT_TIME  = TO_DATE('" + oEntity.SecondTime.ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM'),RM_MRD_SECOND_WEIGHT_QTY = " + oEntity.SecondQty + ",";
                SQL = SQL + " RM_MRD_READING_STATUS = '" + oEntity.ReadingStatus + "',RM_MRD_ENTRY_MODE = '" + oEntity.Entrymode + "', ";
                SQL = SQL + " RM_MRD_WEIGH_BRIDGE_ID = '" + oEntity.WeighBridgeID + "',";
                SQL = SQL + " SALES_STN_STATION_CODE_PO_SUP = '" + oEntity.PoStationCode + "'";
                SQL = SQL + "  WHERE  RM_MRM_RECEIPT_NO = '" + oEntity.EntryNo + "' AND  AD_FIN_FINYRID = " + mngrclass.FinYearID + " AND RM_MRD_APPROVED ='N'";


                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertTollDetailsSQL(oEntity.EntryNo, Convert.ToInt16(mngrclass.FinYearID), objTollGridEntity, "U");
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }
                 
                if (!string.IsNullOrWhiteSpace(oEntity.TokenNo))
                {
                    sRetun = string.Empty;

                    sRetun = UpdateTokenStatus(oEntity.TokenNo,oEntity.EntryNo,oEntity.TokenStatus,mngrclass.FinYearID);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                } 
                 
                sRetun = string.Empty;

                //Strings.Mid(oEntity.TrailerName, 1, 150) 
                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRC", oEntity.EntryNo, false, Environment.MachineName, "U", sSQLArray);

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

        private string InsertTollDetailsSQL ( string sEntryNumber, int iFinYear, List<ReceiptEntryRmTollGridEntity> objTollGridEntity, string Status )
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

        private string UpdateTokenStatus ( string TokenNo,string EntryNo, string Status,int FinYearID )
        {
            string sRetun = string.Empty;
            int Row = 0;

            try
            {

                SQL = " Update RM_RECEPIT_TOKEN_MST Set RM_RT_STATUS='" + Status + "',";
                SQL = SQL + "RM_MRM_RECEIPT_NO='" + EntryNo + "',";
                SQL = SQL + " RM_MRM_FINYRID='" + FinYearID + "' ";
                SQL = SQL + "where RM_RT_TOKEN_NO='" + TokenNo + "'";

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


        public string DeleteSql ( string EntryNo, object mngrclassobj )

        {
            //string fsESOBProv = string.Empty;
            //string fsESOBExp = string.Empty;
            //string fsLSProv = string.Empty;
            //string fsLSExp = string.Empty;
            //string fsATProv = string.Empty;
            //string fsATExp = string.Empty;

            string sRetun = string.Empty;

            // string sAppFlag = "N";
            // object oCode;
            OracleHelper oTrns = new OracleHelper();
            SessionManager mngrclass = (SessionManager)mngrclassobj;

            sSQLArray.Clear();

            try
            {
                sSQLArray.Clear();

                SQL = " DELETE from   RM_RECEIPT_MASTER   ";
                SQL = SQL + " WHERE  RM_MRM_RECEIPT_NO = '" + EntryNo + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
                sSQLArray.Add(SQL);

                SQL = " DELETE FROM  RM_RECEPIT_DETAILS ";
                SQL = SQL + " WHERE RM_MRM_RECEIPT_NO = '" + EntryNo + "' AND  AD_FIN_FINYRID = " + mngrclass.FinYearID + " AND RM_MRD_APPROVED ='N'";
                sSQLArray.Add(SQL);

                SQL = " DELETE FROM  rm_recepit_toll ";
                SQL = SQL + " WHERE rm_mrm_receipt_no = '" + EntryNo + "' AND  AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
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

        #endregion

        #region "Update Toll Details "

        public string UpdateTollDetails ( string EntryNo, int iFinYear, List<ReceiptEntryRmTollGridEntity> objTollGridEntity, object mngrclassobj )

        {
            string sRetun = string.Empty;
            OracleHelper oTrns = new OracleHelper();
            SessionManager mngrclass = (SessionManager)mngrclassobj;

            sSQLArray.Clear();
            int Row = 0;
            try
            {
                sSQLArray.Clear();

                SQL = " Delete from  RM_RECEPIT_TOLL where RM_MRM_RECEIPT_NO ='" + EntryNo + "' and AD_FIN_FINYRID= " + iFinYear + " ";
                sSQLArray.Add(SQL);

                foreach (var Data in objTollGridEntity)
                {
                    Row++;
                    SQL = "INSERT INTO RM_RECEPIT_TOLL ( ";
                    SQL = SQL + "   RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_VM_VENDOR_CODE, SALES_STN_STATION_CODE,RM_SM_SOURCE_CODE, ";
                    SQL = SQL + "   RM_RMM_RM_CODE, RM_UOM_UOM_CODE, RM_MRM_TOLL_QTY, RM_MRM_TOLL_SLNO, ";
                    SQL = SQL + "   RM_MRM_TOLL_NO, RM_MRM_TOLL_RATE, RM_MRM_TOLL_AMOUNT,RM_TL_LOCATION_CODE,  ";
                    SQL = SQL + "   RM_MRM_TOLL_PE_DONE_YN, RM_MRM_TOLL_PE_NO,RM_MRM_TOLL_Date ,RM_MPOM_ORDER_NO,RM_MRM_APPROVED_QTY,  ";
                    SQL = SQL + " RM_MRM_APRPOVED_RATE,RM_MRM_APPROVED_AMOUNT,RM_MRM_TOLL_ENTRYTYPE )  ";
                    SQL = SQL + "VALUES ( '" + EntryNo + "'," + iFinYear + " ,'" + Data.oVenderCode + "' , '" + Data.oStnCode + "','" + Data.oSourceCode + "',";
                    SQL = SQL + "   '" + Data.oRMCode + "','" + Data.oUOMCode + "'," + Data.oTollQTY + "," + Row + ",";
                    SQL = SQL + "   '" + Data.oTollNo + "' , " + Data.oTollRate + "," + Data.oTollamount + " ,'" + Data.oTollLoc + "' , ";
                    SQL = SQL + "   'N' ,'','" + Data.oTollDATE.ToString("dd-MMM-yyyy") + "','" + Data.oPONo + "', ";
                    SQL = SQL + "   " + Data.oTollQTY + " ," + Data.oTollRate + "," + Data.oTollamount + ",'" + Data.ENTRYTYPE + "' ) ";

                    sSQLArray.Add(SQL);
                }


                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRC", EntryNo, false, Environment.MachineName, "U", sSQLArray);

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

        #region "upodate PO details Status "

        public void UpdatePOPendingQty ( ReceiptEntryRmEntity oEntity, double dTotalSuppliedQty )
        {
            try
            {
                OracleHelper oTrns = new OracleHelper();



                // REDUCE THE PENDING QTY 
                SQL = " UPDATE RM_PO_DETAILS SET";

                SQL = SQL + " RM_POD_PENDING_QTY= RM_POD_QTY- " + dTotalSuppliedQty + "";

                SQL = SQL + " WHERE RM_PO_DETAILS.RM_PO_PONO='" + oEntity.OrderNo + "'";
                SQL = SQL + " AND RM_PO_DETAILS.AD_FIN_FINYRID='" + oEntity.orderfinYrId + "'";
                SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE='" + oEntity.PoStationCode + "'";
                SQL = SQL + " AND RM_PO_DETAILS.RM_SM_SOURCE_CODE='" + oEntity.SourceCode + "'";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE='" + oEntity.RmCode + "'";

                oTrns.GetExecuteNonQueryBySQL(SQL);


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
            return;
        }


        public void UpdatePODStatus ( string Status, ReceiptEntryRmEntity oEntity, object mngrclassobj )
        {
            DataSet dsCnt = new DataSet();
            SqlHelper clsSQLHelper = new SqlHelper();
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                string sRM_PO_QTY_RATE_TYPE = "N";

                SQL = "  select   RM_PO_QTY_RATE_TYPE  FROM   RM_PO_MASTER  ";

                SQL = SQL + " WHERE  RM_PO_PONO='" + oEntity.OrderNo + "'";
                SQL = SQL + " AND  AD_FIN_FINYRID='" + oEntity.orderfinYrId + "'";

                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows.Count) > 0)
                {
                    sRM_PO_QTY_RATE_TYPE = dsCnt.Tables[0].Rows[0]["RM_PO_QTY_RATE_TYPE"].ToString();
                }
                if (Status == "C")
                {
                    if (sRM_PO_QTY_RATE_TYPE == "N")
                    {
                        ////N NORMAL  1
                        ////Q OPEN QTY    2
                        ////R OPEN RATE   3

                        // if the qty type is normal only need to close the po else users will do manual closing 
                        SQL = " UPDATE RM_PO_DETAILS SET";
                        SQL = SQL + " RM_POD_STATUS='C'";
                        SQL = SQL + " WHERE RM_PO_DETAILS.RM_PO_PONO='" + oEntity.OrderNo + "'";
                        SQL = SQL + " AND RM_PO_DETAILS.AD_FIN_FINYRID='" + oEntity.orderfinYrId + "'";
                        //SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE='" + oEntity.StationCode + "'";
                        SQL = SQL + " AND RM_PO_DETAILS.RM_SM_SOURCE_CODE='" + oEntity.SourceCode + "'";
                        SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE='" + oEntity.RmCode + "'";
                        //SQL = SQL + " AND  round(RM_POD_NEWPRICE,4)='" + oEntity.hdnUnitRate + "'"; 

                    }

                }
                else if (Status == "O")
                {
                    SQL = " UPDATE RM_PO_DETAILS SET";

                    SQL = SQL + " RM_POD_STATUS='O'";

                    SQL = SQL + " WHERE RM_PO_DETAILS.RM_PO_PONO='" + oEntity.OrderNo + "'";
                    SQL = SQL + " AND RM_PO_DETAILS.AD_FIN_FINYRID='" + oEntity.orderfinYrId + "'";
                    //SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE='" + oEntity.StationCode + "'";
                    SQL = SQL + " AND RM_PO_DETAILS.RM_SM_SOURCE_CODE='" + oEntity.SourceCode + "'";
                    SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE='" + oEntity.RmCode + "'";
                     //SQL = SQL + " AND  round(RM_POD_NEWPRICE,4)='" + oEntity.hdnUnitRate + "'"; 
                }

                oTrns.GetExecuteNonQueryBySQL(SQL);

                string sRetun;
                sRetun = CloseOpenCheck(oEntity);

                if (sRetun != "CONTINUE")
                {



                    SQL = " UPDATE RM_PO_MASTER";
                    SQL = SQL + " SET RM_po_po_status = 'O'";
                    SQL = SQL + " WHERE RM_po_pono = '" + oEntity.OrderNo + "' AND ad_fin_finyrid = " + oEntity.orderfinYrId;

                    oTrns.GetExecuteNonQueryBySQL(SQL);
                }
                else
                {
                    SQL = " UPDATE RM_PO_MASTER";
                    SQL = SQL + " SET RM_po_po_status = 'C'";
                    SQL = SQL + " WHERE RM_po_pono = '" + oEntity.OrderNo + "' AND ad_fin_finyrid = " + oEntity.orderfinYrId;
                    oTrns.GetExecuteNonQueryBySQL(SQL);
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
            return;
        }

        public void ResetPODStatus ( ReceiptEntryRmEntity oEntity, object mngrclassobj )
        {
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " UPDATE RM_PO_DETAILS SET";

                SQL = SQL + " RM_POD_STATUS='O'";

                SQL = SQL + " WHERE RM_PO_DETAILS.RM_PO_PONO='" + oEntity.OrderNo + "'";
                SQL = SQL + " AND RM_PO_DETAILS.AD_FIN_FINYRID='" + oEntity.orderfinYrId + "'";
              //  SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE='" + oEntity.StationCode + "'";
                SQL = SQL + " AND RM_PO_DETAILS.RM_SM_SOURCE_CODE='" + oEntity.SourceCode + "'";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE='" + oEntity.RmCode + "'";
               // SQL = SQL + " AND round(RM_POD_NEWPRICE,4)='"+oEntity.hdnUnitRate+"' ";

                oTrns.GetExecuteNonQueryBySQL(SQL);


                string sRetun;
                sRetun = CloseOpenCheck(oEntity);

                if (sRetun != "CONTINUE")
                {



                    SQL = " UPDATE RM_PO_MASTER";
                    SQL = SQL + " SET RM_po_po_status = 'O'";
                    SQL = SQL + " WHERE RM_po_pono = '" + oEntity.OrderNo + "' AND ad_fin_finyrid = " + oEntity.orderfinYrId;

                    oTrns.GetExecuteNonQueryBySQL(SQL);
                }
                else
                {
                    SQL = " UPDATE RM_PO_MASTER";
                    SQL = SQL + " SET RM_po_po_status = 'C'";
                    SQL = SQL + " WHERE RM_po_pono = '" + oEntity.OrderNo + "' AND ad_fin_finyrid = " + oEntity.orderfinYrId;
                    oTrns.GetExecuteNonQueryBySQL(SQL);
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
            return;
        }


        #endregion 

        #region "print"

        public DataSet FetchRceiptPrintData ( string sEntryNo, object mngrclassobj )
        {
            DataSet dsPoStatus = new DataSet("RMRECEIPTPRINT");

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "   ";
                ////    SQL = " SELECT  ";       
                ////SQL = SQL  + "  RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_MRM_RECEIPT_DATE, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID,  " ;
                ////SQL = SQL  + "  RM_POD_SL_NO, RM_MPOM_PRICE_TYPE, RM_MRD_SL_NO, AD_CM_COMPANY_CODE, RM_VM_VENDOR_CODE,  ";
                ////SQL = SQL  + "  RM_VM_VENDOR_NAME, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, HR_DEPT_DEPT_CODE, RM_RMM_RM_CODE,  ";
                ////SQL = SQL  + "  RM_UM_UOM_CODE, RM_RMM_RM_DESCRIPTION, RM_UM_UOM_DESC, RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC,  ";
                ////SQL = SQL  + "  RM_INTERNALOREXTENAL, RM_MTRANSPO_ORDER_NO, RM_MTRANSPO_FIN_FINYRID, RM_POD_SL_NO_TRANS, RM_MRM_TRANSPORTER_CODE,  ";
                ////SQL = SQL  + "  TRANSPORTER_NAME, RM_MRM_TRNSPORTER_DOC_NO, RM_MRD_TRIPNO, FA_FAM_ASSET_CODE, RM_MRM_VEHICLE_DESCRIPTION,  ";
                ////SQL = SQL  + "  RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME, RM_MRD_VENDOR_DOC_NO, RM_MRD_ORDER_QTY, RM_MRD_SUPPLY_QTY,  ";
                ////SQL = SQL  + "  RM_MRD_WEIGH_QTY, RM_MRD_APPROVE_QTY, RM_MRD_REJECTED_QTY, RM_MRD_SUPP_UNIT_PRICE, RM_MRD_SUPP_AMOUNT,  ";
                ////SQL = SQL  + "  RM_MRD_TRANS_RATE, RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT, RM_MRD_SUPP_UNIT_PRICE_NEW, RM_MRD_SUPP_AMOUNT_NEW,  ";
                ////SQL = SQL  + "  RM_MRD_TRANS_RATE_NEW, RM_MRD_TRANS_AMOUNT_NEW, RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_APPROVED, RM_MRM_APPROVED_NO,  ";
                ////SQL = SQL  + "  RM_MRD_APPROVED_FINYRID, RM_MRD_SUPP_ENTRY, RM_MRD_SUPP_PENO, RM_MPED_VFINYR_ID, RM_MRD_TRANS_ENTRY,  ";
                ////SQL = SQL  + "  RM_MRD_TRANS_PENO, RM_MPED_TFINYR_ID, RM_MRM_RECEIPT_UNIQE_NO, RM_MRM_REMARKS, RM_MRM_CREATEDBY,  ";
                ////SQL = SQL + "   RM_MRM_UPDATEDBY,RM_MRD_FIRST_WEIGHT_TIME, RM_MRD_FRIST_WEIGHT_QTY,RM_MRD_SECOND_WEIGHT_TIME,";
                ////SQL = SQL + "   RM_MRD_SECOND_WEIGHT_QTY, RM_MRD_READING_STATUS,RM_MRD_ENTRY_MODE,";
                ////SQL = SQL + "  RM_MRD_WEIGH_BRIDGE_ID,TECH_PLM_PLANT_CODE,TECH_PLM_PLANT_NAME ,";
                ////    SQL = SQL + "   RM_UOM_TOLL_CODE  , RM_POD_TOLL_RATE  , RM_MRD_TOLL_AMOUNT	 , " ;
                ////    SQL = SQL + " RM_POD_TOLL_RATE_NEW  , RM_MRD_TOLL_AMOUNT_NEW	 , ";
                ////    SQL = SQL + "  RM_MRD_TOLL_PE_ENTRY_YN	,   RM_MRD_TOLL_PE_ENTRY_NO,  RM_MRD_TOLL_PE_ENTRY_FINYR_ID ";
                ////    SQL = SQL + "  FROM  RM_RECEIPT_VOUCHER_PRINT_VW ";
                ////    SQL = SQL + "   WHERE ";  
                ///

                SQL = " SELECT RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO, RM_RECEPIT_DETAILS.AD_FIN_FINYRID,RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE, ";
                SQL = SQL + "           RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO, RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID, RM_POD_SL_NO, RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE, ";
                SQL = SQL + "           RM_MRD_SL_NO,RM_RECEPIT_DETAILS.AD_CM_COMPANY_CODE,RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE,RM_VENDOR_MASTER.RM_VM_VENDOR_NAME, ";
                SQL = SQL + "           RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,SL_STATION_MASTER.SALES_STN_STATION_NAME,rm_recepit_details.HR_DEPT_DEPT_CODE, ";
                SQL = SQL + "           RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,RM_RECEPIT_DETAILS.RM_UM_UOM_CODE, RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "           RM_UOM_MASTER.RM_UM_UOM_DESC,RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE,RM_SOURCE_MASTER.RM_SM_SOURCE_DESC, ";
                SQL = SQL + "           RM_RECEPIT_DETAILS.TECH_PLM_PLANT_CODE,TECH_PLM_PLANT_NAME,RM_INTERNALOREXTENAL, RM_MTRANSPO_ORDER_NO,RM_MTRANSPO_FIN_FINYRID, ";
                SQL = SQL + "           RM_POD_SL_NO_TRANS,RM_MRM_TRANSPORTER_CODE,TRANSPORTER.RM_VM_VENDOR_NAME     TRANSPORTER_NAME,RM_MRM_TRNSPORTER_DOC_NO, ";
                SQL = SQL + "           RM_MRD_TRIPNO, RM_RECEPIT_DETAILS.FA_FAM_ASSET_CODE,RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME, ";
                SQL = SQL + "           RM_MRD_VENDOR_DOC_NO,RM_MRD_ORDER_QTY, RM_MRD_SUPPLY_QTY, RM_MRD_MOIST_LOSS_PERCENTAGE,RM_MRD_MOIST_LOSS_QTY,";
                SQL = SQL + "           RM_MRD_WEIGH_QTY, RM_MRD_APPROVE_QTY,RM_MRD_REJECTED_QTY, ";
                SQL = SQL + "           RM_MRD_SUPP_UNIT_PRICE, RM_MRD_SUPP_AMOUNT, RM_MRD_TRANS_RATE, RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT,RM_MRD_SUPP_UNIT_PRICE_NEW, ";
                SQL = SQL + "           RM_MRD_SUPP_AMOUNT_NEW,RM_MRD_TRANS_RATE_NEW,RM_MRD_TRANS_AMOUNT_NEW,RM_MRD_TOTAL_AMOUNT_NEW, ";
                SQL = SQL + "           RM_MRD_APPROVED,RM_MRM_APPROVED_NO, RM_MRD_APPROVED_FINYRID,RM_MRD_SUPP_ENTRY,RM_MRD_SUPP_PENO, RM_MPED_VFINYR_ID, ";
                SQL = SQL + "           RM_MRD_TRANS_ENTRY,RM_MRD_TRANS_PENO,RM_MPED_TFINYR_ID,RM_MRM_RECEIPT_UNIQE_NO,RM_MRM_REMARKS,RM_MRM_CREATEDBY, RM_MRM_UPDATEDBY, ";
                SQL = SQL + "           RM_MRD_FIRST_WEIGHT_TIME,RM_MRD_FRIST_WEIGHT_QTY,RM_MRD_SECOND_WEIGHT_TIME,RM_MRD_SECOND_WEIGHT_QTY,RM_MRD_READING_STATUS, ";
                SQL = SQL + "           RM_MRD_ENTRY_MODE, RM_MRD_WEIGH_BRIDGE_ID,RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE,RM_MRD_TOLL_AMOUNT, ";
                SQL = SQL + "           RM_POD_TOLL_RATE_NEW,RM_MRD_TOLL_AMOUNT_NEW,RM_MRD_TOLL_PE_ENTRY_YN,RM_MRD_TOLL_PE_ENTRY_NO,RM_MRD_TOLL_PE_ENTRY_FINYR_ID, ";
                SQL = SQL + "            MASTER_BRANCH.AD_BR_CODE MASTER_BRANCH_CODE,   ";
                SQL = SQL + "            MASTER_BRANCH.AD_BR_NAME MASTER_BRANCH_NAME,   ";
                SQL = SQL + "            MASTER_BRANCH.AD_BR_DOC_PREFIX MASTER_BRANCHDOC_PREFIX,   ";
                SQL = SQL + "            MASTER_BRANCH.AD_BR_POBOX MASTER_BRANCH_POBOX,   ";
                SQL = SQL + "            MASTER_BRANCH.AD_BR_ADDRESS MASTER_BRANCH_ADDRESS,   ";
                SQL = SQL + "            MASTER_BRANCH.AD_BR_CITY MASTER_BRANCH_CITY,   ";
                SQL = SQL + "            MASTER_BRANCH.AD_BR_PHONE MASTER_BRANCH_PHONE,   ";
                SQL = SQL + "            MASTER_BRANCH.AD_BR_FAX MASTER_BRANCH_FAX,   ";
                SQL = SQL + "            MASTER_BRANCH.AD_BR_SPONSER_NAME MASTER_BRANCH_SPONSER_NAME,   ";
                SQL = SQL + "            MASTER_BRANCH.AD_BR_TRADE_LICENSE_NO MASTER_BRANCH_TRADE_LICENSE_NO,   ";
                SQL = SQL + "            MASTER_BRANCH.AD_BR_EMAIL_ID MASTER_BRANCH_EMAIL_ID,   ";
                SQL = SQL + "            MASTER_BRANCH.AD_BR_WEB_SITE MASTER_BRANCH_WEB_SITE,   ";
                SQL = SQL + "            MASTER_BRANCH.AD_BR_TAX_VAT_REG_NUMBER MASTER_BRANCH_VAT_REG_NUMBER , RM_MRM_RECEIPT_VERIFIED_BY, RM_MRM_RECEIPT_APPROVE_BY ";
                SQL = SQL + "      FROM RM_RECEPIT_DETAILS, RM_RECEIPT_MASTER, RM_RAWMATERIAL_MASTER, RM_UOM_MASTER,SL_STATION_MASTER, ";
                SQL = SQL + "           RM_SOURCE_MASTER,TECH_PLANT_MASTER, RM_VENDOR_MASTER, RM_VENDOR_MASTER  TRANSPORTER,  ad_branch_master MASTER_BRANCH ";
                SQL = SQL + "     WHERE     RM_RECEIPT_MASTER.RM_MRM_RECEIPT_NO = RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ";
                SQL = SQL + "           AND RM_RECEIPT_MASTER.AD_FIN_FINYRID = RM_RECEPIT_DETAILS.AD_FIN_FINYRID ";
                SQL = SQL + "           AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "           AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + "           AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "           AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "           AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE =SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "           AND RM_RECEPIT_DETAILS.TECH_PLM_PLANT_CODE =TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE ";
                SQL = SQL + "           AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE = TRANSPORTER.RM_VM_VENDOR_CODE(+) ";
                SQL = SQL + "           AND RM_RECEIPT_MASTER.AD_BR_CODE = MASTER_BRANCH.ad_br_code(+) ";


                ////   SQL = SQL + "    RM_RECEIPT_VOUCHER_PRINT_VW.RM_MRM_RECEIPT_NO = '" + sEntryNo + "'  AND  RM_RECEIPT_VOUCHER_PRINT_VW.AD_FIN_FINYRID =" + mngrclass.FinYearID + ""; 
                SQL = SQL + "   AND  RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO = '" + sEntryNo + "'  AND  RM_RECEPIT_DETAILS.AD_FIN_FINYRID =" + mngrclass.FinYearID + "";

                dsPoStatus = clsSQLHelper.GetDataset(SQL);
                dsPoStatus.Tables[0].TableName = "RM_RECEPIT_DETAILS";
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
    }

    /// <summary>
    /// // --------------------------- ENTITY ----------------------------------------------------------
    /// </summary>
    public class ReceiptEntryRmTollGridEntity
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

    public class ReceiptEntryRmEntity
    {

        public ReceiptEntryRmEntity ( )
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public string OrderNo
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

        public string EntryNo
        {
            get;
            set;
        }
        
        public string TokenNo
        {
            get;
            set;
        }
        
        public string TokenStatus
        {
            get;
            set;
        }


        public string PriceType
        {
            get;
            set;
        }
        public string RateRevisionApplicable
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

        public string PoStationCode
        {
            get;
            set;
        }

        
        public string StationCode
        {
            get;
            set;
        }


        public string StationName
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


        public string RmCode
        {
            get;
            set;
        }


        public string RmName
        {
            get;
            set;
        }

        public string Branch { get; set; }
        public string DriverType
        {
            get;
            set;
        }



        public string TransPoNo
        {
            get;
            set;
        }

        public string TranPoSLno
        {
            get;
            set;
        }

        public string Transportercode
        {
            get;
            set;
        }


        public string TransporterName
        {
            get;
            set;
        }


        public string TransDocNO
        {
            get;
            set;
        }


        public double TransUnitRate
        {
            get;
            set;
        }

        public string DriverCode
        {
            get;
            set;
        }


        public string Drivername
        {
            get;
            set;
        }

        public string AssetCode
        {
            get;
            set;
        }


        public string TrailerName
        {
            get;
            set;
        }

        public string Remarks
        {
            get;
            set;
        }



        public string TransPoFinYrId
        {
            get;
            set;
        }

        public string UomCode
        {
            get;
            set;
        }


        public string orderfinYrId
        {
            get;
            set;
        }

        public string PoSLNO
        {
            get;
            set;
        }



        public string DeptCode
        {
            get;
            set;
        }

        public string PlantCode
        {
            get;
            set;
        }


        public double OrderQty
        {
            get;
            set;
        }



        public string txtWeightBridgeNo
        {
            get;
            set;
        }


        public double BalanceQty
        {
            get;
            set;
        }
        public string SuppDocno
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
        public double MoistureLossPercentage { get; set; }
        public double MoistureLossQTY { get; set; }
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

        
        public double hdnUnitRate
        {
            get;
            set;
        }


        public double TollUnitRate
        {
            get;
            set;
        }

        public double TollUnitAmount
        {
            get;
            set;
        }

        public string TollUOMCode
        {
            get;
            set;
        }


        public double UnitAmount
        {
            get;
            set;
        }

        public double TransUnitAmount
        {
            get;
            set;
        }
        public double TotalAmount
        {
            get;
            set;
        }

        public string rejectedYN
        {
            get;
            set;
        }
        public string RM_PO_UOM_TYPE
        {
            get;
            set;
        }

        public string Inv_Qty_Type
        {
            get;
            set;
        }
        public string TempStock
        {
            get;
            set;
        }


        public string ReadingStatus
        {
            get;
            set;
        }

        public string Entrymode
        {
            get;
            set;
        }

        public DateTime FirstTime
        {
            get;
            set;
        }
        public double FirstQty
        {
            get;
            set;
        }

        public DateTime SecondTime
        {
            get;
            set;
        }
        public double SecondQty
        {
            get;
            set;
        }

        public string WeighBridgeID
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
        
        public string VendorMRTaxType
        {
            get;
            set;
        } public string VendorMRTaxPerc
        {
            get;
            set;
        } 
        public string TollTaxType
        {
            get;
            set;
        } public string TollTaxPerc
        {
            get;
            set;
        }
        public double VarienceQty
        {
            get;
            set;
        }
    }



    public class WeighbridgeData
    {
        public double weight { get; set; }
        public string date_time { get; set; }
        public string terminal_id { get; set; }

    }

}
