using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Data;
using AccosoftUtilities;
using AccosoftLogWriter;
using AccosoftNineteenCDL;
using System.Globalization;
using System.Collections;

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class ReportsListLogicRM
    {

        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());
        String SQL = string.Empty;
        ArrayList sSQLArray = new ArrayList();

        #region "poroperty " 

        public DateTime ReceptFromDateTime
        {
            get;
            set;
        }
        public DateTime ReceptToDateTime
        {
            get;
            set;
        }

        public string DateTimeFilterType
        {
            get;
            set;
        }

        public string Type
        {
            get;
            set;
        }
        public string SelecedPlant
        {
            get;
            set;
        }

        public string SelectedType
        {
            get;
            set;
        }
        public string SelectedVehicle
        {
            get;
            set;
        }

        public string Filter
        {
            get;
            set;
        }

        public string Branch
        {
            get;
            set;
        }
        public string ToBranch
        {
            get;
            set;
        }

        public string ReceiptType
        {
            get;
            set;
        }
        public string SlectedStation
        {
            get;
            set;
        }
        public string SelectedRM
        {
            get;
            set;
        }

        public string ApprovedStatus
        {
            get;
            set;
        }

        public string SelectedProject
        {
            get;
            set;
        }


        #endregion

        #region "Fil view
        public DataTable FillView(string TypeID, string SuppCode, string sFromdate, string sTodate)
        {
            DataTable dtType = new DataTable();
            try
            //-- this look i up is using various places os take care // Jomy & jins  11 / jan 23013
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                if (TypeID == "SUPPLLIERRECEIPTS")
                {


                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE ";
                    // SQL = SQL  + "    AND  RM_MRM_RECEIPT_DATE BETWEEN  "
                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                    SQL = SQL + "    ORDER BY RM_VM_VENDOR_NAME ASC  ";

                }
                else if (TypeID == "RECEIPTTRANSPORTER")
                {
                    // THIS IS FOR  trsporter  PURCHASE ENTRY NOT YET DONE 

                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE  ";
                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                    SQL = SQL + "    ORDER BY RM_VM_VENDOR_NAME ASC  ";

                }

                else if (TypeID == "RAWMATERIAL")
                {

                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "     RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code, RM_RMM_RM_DESCRIPTION Name  ";
                    SQL = SQL + "    FROM RM_RAWMATERIAL_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE =     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE ";
                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    // since ths is  vendor cod is coming form the grid very difficult the manage so using date filter
                    //if (SuppCode != null)
                    //{
                    //    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = '" + SuppCode + "'";

                    //}
                    SQL = SQL + "    ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC  ";

                }

                else if (TypeID == "CUSTOMER")
                {
                    // THIS IS FOR  CUSTOMER  SALES ENTRY REPORT

                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_CODE Code, SALES_CUS_CUSTOMER_NAME Name  ";
                    SQL = SQL + "    FROM SL_CUSTOMER_MASTER, RM_SALES_MASTER  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_CODE = RM_SALES_MASTER.SALES_CUS_CUSTOMER_CODE  ";
                    SQL = SQL + " AND  RM_MSM_ENTRY_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + "    ORDER BY SALES_CUS_CUSTOMER_NAME ASC  ";

                }

                else if (TypeID == "SUPPLLIER")
                {
                    // THIS IS FOR  CUSTOMER  SALES ENTRY REPORT

                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "     RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_SALES_MASTER  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_SALES_MASTER.RM_VM_VENDOR_CODE  ";
                    SQL = SQL + " AND  RM_MSM_ENTRY_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + "    ORDER BY RM_VM_VENDOR_NAME ASC  ";

                }

                else if (TypeID == "RAWMATERIALSALES")
                {

                    // THIS IS FOR    SALES ENTRY REPORT
                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code, RM_RMM_RM_DESCRIPTION Name  ";
                    SQL = SQL + "    FROM RM_RAWMATERIAL_MASTER, RM_SALES_DETAILS,RM_SALES_MASTER  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE = RM_SALES_DETAILS.RM_RMM_RM_CODE ";
                    SQL = SQL + "    and RM_SALES_MASTER.RM_MSM_ENTRY_NO =  RM_SALES_DETAILS.RM_MSM_ENTRTY_NO ";
                    SQL = SQL + "    and RM_SALES_MASTER.AD_FIN_FINYRID =   RM_SALES_DETAILS.AD_FIN_FINYRID ";
                    SQL = SQL + " AND  RM_MSM_ENTRY_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + "    ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC  ";

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



        //------------------------ MASTER DATA LIST LOGIC ------------------------------------
        #region "Master data report logoc " 

        public DataTable RMList()
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();
                SQL = " select * from RM_LIST_PRINT_RM ";

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }


        public DataTable RMListwithPrecastBudgetAndCostAccounts()
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();
                SQL = " select * from RM_LIST_PRINT_PRC_ACC_CODE_VW ";

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }



        
        public DataTable VendorList()
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();
                SQL = " select VENDORCODE, VENDORNAME, VENDORFULLNAME,  ";
                SQL = SQL + "   VENDOR_NAME_IN_CHEQUE, VENDOR_ACCOUNT_GROUP, VENDOR_ACCOUNT_CODE,  ";
                SQL = SQL + "   VENDOR_TYPE, VENDORSTATUS, EMIRATE_CODE,  ";
                SQL = SQL + "   EMIRATE_NAME, POBOX, ADDRESS,  ";
                SQL = SQL + "   CITY, TELNO, FAXNO,  ";
                SQL = SQL + "   EMAIL, WEBSITE, CONTACTPERSON1,  ";
                SQL = SQL + "   CONTACTDESIG1, CONT_PERSON1_MOBNO, CONTACTPERSON2,  ";
                SQL = SQL + "   CONTACTDESIG2, CONT_PERSON2_MOBNO, CREDITLIMIT,  ";
                SQL = SQL + "   CREIDTPERIOD, CREDITTERMS, TRADE_LICENSE_NO,  ";
                SQL = SQL + "   LICENSE_EXP_DATE, EXP_CAT_CODE, EVAL_EXP_DATE,  ";
                SQL = SQL + "   EVAL_EXP_REMARKS, PRICING_CATEGORY, PRICE_REMARKS,  ";
                SQL = SQL + "   REMARKS, TYPE_CODE, VAT_REG_NUMBER,  ";
                SQL = SQL + "   VAT_PERCENTAGE, BANK1_ACC_NO, IBAN1_CODE,  ";
                SQL = SQL + "   SWIFT_CODE1, BANK2_ACC_NO, IBAN2_CODE,  ";
                SQL = SQL + "   SWIFT_CODE2,CREATEDBY,  to_char( CREATEDDATE,'DD-MON-YYYY HH12:MI:SS AM')  CREATEDDATE, VENDOR_ORCL_ID,AD_BR_CODE BRANCH_CODE, AD_BR_NAME BRANCH_NAME from RM_VENDOR_LIST_RPT_VW ";


                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable VendorListBranchwise()
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();
                SQL = " select VENDORCODE, VENDORNAME, VENDORFULLNAME,  ";
                SQL = SQL + "   VENDOR_NAME_IN_CHEQUE, VENDOR_ACCOUNT_GROUP, VENDOR_ACCOUNT_CODE,  ";
                SQL = SQL + "   VENDOR_TYPE, VENDORSTATUS, EMIRATE_CODE,  ";
                SQL = SQL + "   EMIRATE_NAME, POBOX, ADDRESS,  ";
                SQL = SQL + "   CITY, TELNO, FAXNO,  ";
                SQL = SQL + "   EMAIL, WEBSITE, CONTACTPERSON1,  ";
                SQL = SQL + "   CONTACTDESIG1, CONT_PERSON1_MOBNO, CONTACTPERSON2,  ";
                SQL = SQL + "   CONTACTDESIG2, CONT_PERSON2_MOBNO, CREDITLIMIT,  ";
                SQL = SQL + "   CREIDTPERIOD, CREDITTERMS, TRADE_LICENSE_NO,  ";
                SQL = SQL + "   LICENSE_EXP_DATE, EXP_CAT_CODE, EVAL_EXP_DATE,  ";
                SQL = SQL + "   EVAL_EXP_REMARKS, PRICING_CATEGORY, PRICE_REMARKS,  ";
                SQL = SQL + "   REMARKS, TYPE_CODE, VAT_REG_NUMBER,  ";
                SQL = SQL + "   VAT_PERCENTAGE, BANK1_ACC_NO, IBAN1_CODE,  ";
                SQL = SQL + "   SWIFT_CODE1, BANK2_ACC_NO, IBAN2_CODE,  ";
                SQL = SQL + "   SWIFT_CODE2,CREATEDBY, to_char( CREATEDDATE,'DD-MON-YYYY HH12:MI:SS AM') CREATEDDATE, VENDOR_ORCL_ID,AD_BR_CODE BRANCH_CODE, AD_BR_NAME BRANCH_NAME from RM_VENDOR_BRANCHWISE_LIST_RPT_VW  ";


                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable UOMList()
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();
                SQL = " SELECT  RM_UM_UOM_CODE UOM_CODE, RM_UM_UOM_DESC UOM_DESC  FROM  RM_UOM_MASTER  ORDER BY RM_UM_UOM_CODE ASC";


                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable SourceList()
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();
                SQL = " SELECT   RM_SM_SOURCE_CODE SOURCE_CODE, RM_SM_SOURCE_DESC  SOURCE_DESC    FROM  RM_SOURCE_MASTER ORDER BY RM_SM_SOURCE_CODE ASC ";


                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        #endregion

        //---------------------- MASTER DATA LIST LOGIC END 

        //-- RATE CARD LIST LOGIC 

        #region "Rate Card"

        public DataTable FetchRateCard(string sStation, string sSupplier, string sRawmaterial, string sFromDate, string sTodate, string sType, string sSortType)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();

                SQL = "  ";
                SQL = "SELECT ";
                //SQL = SQL + "   RM_PO_MASTER.RM_PO_PONO, ";
                //SQL = SQL + "   RM_PO_MASTER.AD_FIN_FINYRID,   ";
                SQL = SQL + "     RM_PO_MASTER.RM_VM_VENDOR_CODE VENDOR_CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME VENDOR_NAME, ";
                //   SQL = SQL + "     RM_VENDOR_MASTER.RM_VM_VENDOR_TYPE,   ";
                SQL = SQL + "     RM_PO_MASTER.RM_PO_PRICETYPE PRICETYPE,  "; // RM_PO_MASTER.RM_PO_ENTRY_TYPE,   ";
                SQL = SQL + "       RM_PO_DETAILS.SALES_STN_STATION_CODE STATION_CODE ,  ";
                SQL = SQL + "     SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME ,  ";
                SQL = SQL + "     RM_PO_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE , RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_DESC , "; ;
                SQL = SQL + "     RM_PO_DETAILS.RM_RMM_RM_CODE RM_CODE , RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION , ";
                SQL = SQL + "     CASE WHEN RM_PO_PRICETYPE  = 'ONSITE'  THEN  RM_PO_DETAILS.RM_POD_UNIT_PRICE  ";
                SQL = SQL + "     ELSE 0 END AS ONSITE_PRICE , ";
                SQL = SQL + "         CASE WHEN RM_PO_PRICETYPE  = 'EX-FACTORY'  THEN  RM_PO_DETAILS.RM_POD_UNIT_PRICE  ";
                SQL = SQL + "     ELSE 0 END AS EXFACTORY_PRICE , ";
                SQL = SQL + "      RM_PO_MASTER.RM_PO_PO_DATE EFFECTIVE_DATE,    RM_PO_DETAILS.RM_UOM_UOM_CODE UOM_CODE ,  ";
                SQL = SQL + "     RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC   ";
                SQL = SQL + " FROM  ";
                SQL = SQL + "     RM_PO_MASTER , RM_PO_DETAILS  ";
                SQL = SQL + "     , RM_RAWMATERIAL_MASTER , RM_SOURCE_MASTER ,  ";
                SQL = SQL + "     SL_STATION_MASTER ,  ";
                SQL = SQL + "     RM_VENDOR_MASTER , RM_UOM_MASTER  ";
                SQL = SQL + " WHERE (RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO) ";
                SQL = SQL + " AND(RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID) ";
                SQL = SQL + " AND(RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE) ";
                SQL = SQL + " AND(RM_PO_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE) ";
                SQL = SQL + " AND(RM_PO_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE)  ";
                SQL = SQL + " AND(RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE) ";
                SQL = SQL + " AND(RM_PO_DETAILS.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE) ";

                //  <asp:ListItem>PURCHASE</asp:ListItem>
                //<asp:ListItem>TRANSPORTER</asp:ListItem>
                //<asp:ListItem>STOCKTRANSFER</asp:ListItem>

                if (sType == "PURCHASE")
                {
                    SQL = SQL + "  AND RM_PO_MASTER.RM_PO_ENTRY_TYPE  = 'PURCHASE'";
                }
                else if (sType == "TRANSPORTER")
                {
                    SQL = SQL + "   AND RM_PO_MASTER.RM_PO_ENTRY_TYPE  = 'TRANSPORTER'";
                }
                else if (sType == "STOCKTRANSFER")
                {
                    SQL = SQL + "    AND RM_PO_MASTER.RM_PO_ENTRY_TYPE  = 'STOCKTRANSFER'";
                }


                SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE IN ( '" + sStation + "') ";

                if (!string.IsNullOrEmpty(sSupplier))
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE IN ( '" + sSupplier + "') ";

                }



                if (!string.IsNullOrEmpty(sRawmaterial))
                {
                    SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE IN ( '" + sRawmaterial + "') ";

                }


                SQL = SQL + "  AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                // order by 

                //<asp:ListItem>Date</asp:ListItem>
                //<asp:ListItem>Supplier</asp:ListItem>
                //<asp:ListItem>Rawmaterial</asp:ListItem>





                if (sSortType == "Supplier")
                {
                    SQL = SQL + "  ORDER BY  RM_PO_MASTER.RM_VM_VENDOR_CODE ASC  ";
                }

                else if (sSortType == "Rawmaterial")
                {
                    SQL = SQL + "  ORDER BY  RM_PO_DETAILS.RM_RMM_RM_CODE ASC";
                }
                SQL = SQL + "     , RM_PO_MASTER.RM_PO_PO_DATE ASC ";

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        #endregion 

        //---------------------- rate card end 



        // REGION PURCHASE REQUEST / PURCHASE ORDER LIST LOGIC 

        #region "Purchase requeest and pruchase order list 

        public DataTable FetchPurchaseRequest(string sStation, string sBranch, string sSupplier, string sRawmaterial, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();


                //SQL = " SELECT RM_PR_PRNO PRNO ,  RM_PR_PR_DATE PR_DATE ,";
                //SQL = SQL + " RM_PR_MASTER.RM_VM_VENDOR_CODE VENDOR_CODE ,";
                //SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIERNAME, RM_PR_EXP_DATE EXP_DATE ,";
                //SQL = SQL + " RM_PR_EXP_DATE_TO EXP_DATE_TO , ";

                //SQL = SQL + " RM_PR_PR_STATUS STATUS , RM_PR_REMARKS REMARKS, ";

                //SQL = SQL + " RM_PR_CANCEL_REMARKS CANCEL_REMARKS , ";
                //SQL = SQL + " RM_PR_CREATEDBY CREATEDBY ,  RM_PR_VERIFIEDBY VERIFIEDBY , RM_PR_APPROVED APPROVED , RM_PR_APPROVEDBY APPROVEDBY   ";
                //SQL = SQL + " FROM RM_PR_MASTER, RM_VENDOR_MASTER ";

                //SQL = SQL + " WHERE ";
                //SQL = SQL + " RM_PR_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE (+)";

                //if (!string.IsNullOrEmpty(sSupplier))
                //{
                //    SQL = SQL + " AND RM_PR_MASTER.RM_VM_VENDOR_CODE IN ( '" + sSupplier + "') ";

                //}

                //SQL = SQL + "  AND RM_PR_MASTER.RM_PR_PR_DATE BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                //SQL = SQL + "     ORDER BY RM_PR_MASTER.RM_PR_PRNO ASC ";

                SQL = "SELECT   RM_PR_MASTER.RM_PR_PRNO PRNO, RM_PR_PR_DATE PR_DATE, ";
                SQL = SQL + "         RM_PRREQN_QTN.RM_VM_VENDOR_CODE VENDOR_CODE, ";
                SQL = SQL + "         RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIERNAME, ";
                SQL = SQL + "         RM_PR_EXP_DATE EXP_DATE, RM_PR_EXP_DATE_TO EXP_DATE_TO, ";
                SQL = SQL + "         RM_PR_PR_STATUS STATUS, RM_PR_REMARKS REMARKS, ";
                SQL = SQL + "         RM_PR_CANCEL_REMARKS CANCEL_REMARKS, RM_PR_CREATEDBY CREATEDBY, ";
                SQL = SQL + "         RM_PR_VERIFIEDBY VERIFIEDBY, RM_PR_APPROVED APPROVED, ";
                SQL = SQL + "         RM_PR_APPROVEDBY APPROVEDBY ";
                SQL = SQL + "    FROM RM_PR_MASTER, RM_VENDOR_MASTER, RM_PRREQN_QTN ";
                SQL = SQL + "   WHERE RM_PR_MASTER.RM_PR_PRNO = RM_PRREQN_QTN.RM_PR_PRNO(+) ";
                SQL = SQL + "     AND RM_PRREQN_QTN.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+) ";

                if (!string.IsNullOrEmpty(sBranch))
                {
                    SQL = SQL + " AND RM_PR_MASTER.AD_BR_CODE IN ( '" + sBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSupplier))
                {
                    SQL = SQL + " AND RM_PRREQN_QTN.RM_VM_VENDOR_CODE IN ( '" + sSupplier + "') ";

                }

                if (!string.IsNullOrEmpty(sStation))
                {
                    SQL = SQL + " AND RM_PRREQN_QTN.SALES_STN_STATION_CODE IN ( '" + sStation + "') ";

                }

                if (!string.IsNullOrEmpty(sRawmaterial))
                {
                    SQL = SQL + " AND RM_PRREQN_QTN.RM_RMM_RM_CODE IN ( '" + sRawmaterial + "') ";

                }
                SQL = SQL + "  AND RM_PR_MASTER.RM_PR_PR_DATE BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "ORDER BY RM_PR_MASTER.RM_PR_PRNO ASC ";


                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable FetchPurchaseOrderSummary(string sStation, string sBranch, string sSupplier, string sRawmaterial, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();

                SQL = " SELECT RM_PO_PONO PONO , RM_PO_PO_DATE PO_DATE , RM_PR_PRNO PR_PRNO,";
                SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE VENDOR_CODE,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIERNAME, RM_PO_PRICETYPE PRICETYPE ,";
                SQL = SQL + " RM_PO_ENTRY_TYPE  ENTRY_TYPE  ,";
                // RM_PO_EXP_DATE EXP_DATE ,  , RM_PO_EXP_DATE_TO EXP_DATE_TO ,
                //   SQL = SQL + " RM_PO_GRAND_TOTAL, RM_PO_DISCOUNT_PERC, RM_PO_DISCOUNT_AMOUNT,";

                SQL = SQL + " RM_PO_NET_AMOUNT NET_AMOUNT , ";

                SQL = SQL + "	CASE   RM_PO_MASTER.RM_PO_PO_STATUS  WHEN 'O' THEN 'OPEN' ";
                SQL = SQL + "	WHEN 'N' THEN 'CANCEL'";
                SQL = SQL + " 	WHEN 'C' THEN 'CLOSE' END PO_STATUS, ";

                SQL = SQL + " 	 RM_PO_MASTER.AD_CUR_CURRENCY_CODE CURRENCY_CODE , AD_CURRENCY_MASTER.AD_CUR_CURRENCY_NAME CURRENCY_NAME  , ";
                SQL = SQL + " RM_PO_EXCHANGE_RATE EXCHANGE_RATE , ";

                SQL = SQL + " RM_PO_CREATEDBY CREATEDBY,   RM_PO_VERIFIEDBY VERIFIEDBY , ";

                SQL = SQL + "  RM_PO_APPROVED APPROVED , RM_PO_APPROVEDBY APPROVEDBY,  ";

                SQL = SQL + "  RM_PO_CANCEL_REMARKS CANCEL_REMARKS , RM_PO_CONTAC1_CODE CONTAC1_CODE, C1.HR_EMP_EMPLOYEE_NAME C1NAME,";
                SQL = SQL + " RM_PO_CONTAC2_CODE CONTAC2_COD, C2.HR_EMP_EMPLOYEE_NAME C2NAME , ";

                SQL = SQL + "    RM_PO_SUPP_REF SUPPLIER_REF,";
                SQL = SQL + " RM_PO_SUPP_REF_DATE SUPPLIER_REF_DATE, RM_PO_OUR_REF  INTERNAL_REF, ";


                SQL = SQL + " RM_MPOM_DELIVERY_TIME_RMKS DELIVERY_TIME_RMKS, RM_MPOM_DELIVERY_PLACE_RMKS DELIVERY_PLACE_RMKS, ";
                SQL = SQL + " RM_MPOM_VALIDITY VALIDITY, RM_MPOM_PAYMENT_TERMS PAYMENT_TERMS, RM_MPOM_PAYMENT_MODE PAYMENT_MODE,RM_PO_REMARKS ";


                SQL = SQL + " FROM RM_PO_MASTER, RM_VENDOR_MASTER, AD_CURRENCY_MASTER , HR_EMPLOYEE_MASTER C1 , ";
                SQL = SQL + " HR_EMPLOYEE_MASTER C2  ";
                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + "  AND RM_PO_MASTER.AD_CUR_CURRENCY_CODE =AD_CURRENCY_MASTER.AD_CUR_CURRENCY_CODE";

                SQL = SQL + " AND RM_PO_MASTER.RM_PO_CONTAC1_CODE=C1.HR_EMP_EMPLOYEE_CODE (+)";

                SQL = SQL + " AND RM_PO_MASTER.RM_PO_CONTAC2_CODE=C2.HR_EMP_EMPLOYEE_CODE (+)";

                if (!string.IsNullOrEmpty(sSupplier))
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE IN ( '" + sSupplier + "') ";

                }

                if (!string.IsNullOrEmpty(sBranch))
                {
                    SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE IN ( '" + sBranch + "') ";

                }
                //if (!string.IsNullOrEmpty(sStation))
                //{
                //    SQL = SQL + " AND C1.SALES_STN_STATION_CODE IN ( '" + sStation + "') ";

                //}

                //if (!string.IsNullOrEmpty(sRawmaterial))
                //{
                //    SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE IN ( '" + sRawmaterial + "') ";

                //}

                SQL = SQL + "  AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                SQL = SQL + "     ORDER BY    RM_PO_MASTER.RM_PO_PO_DATE ASC ";

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable FetchPurchaseOrderDetails(string sStation, string sBranch, string sSupplier, string sRawmaterial, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();



                SQL = " SELECT RM_PO_MASTER.RM_PO_PONO PONO , RM_PO_PO_DATE PO_DATE , RM_PR_PRNO PR_PRNO,";
                SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE VENDOR_CODE,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIERNAME, RM_PO_PRICETYPE PRICETYPE ,";
                SQL = SQL + " RM_PO_ENTRY_TYPE  ENTRY_TYPE  ,";

                SQL = SQL + " RM_PO_NET_AMOUNT NET_AMOUNT , ";

                SQL = SQL + "	CASE   RM_PO_MASTER.RM_PO_PO_STATUS  WHEN 'O' THEN 'OPEN' ";
                SQL = SQL + "	WHEN 'N' THEN 'CANCEL'";
                SQL = SQL + " 	WHEN 'C' THEN 'CLOSE' END PO_STATUS, ";

                SQL = SQL + " 	 RM_PO_MASTER.AD_CUR_CURRENCY_CODE CURRENCY_CODE , AD_CURRENCY_MASTER.AD_CUR_CURRENCY_NAME CURRENCY_NAME  , ";
                SQL = SQL + " RM_PO_EXCHANGE_RATE EXCHANGE_RATE , ";

                SQL = SQL + " RM_PO_CREATEDBY CREATEDBY,   RM_PO_VERIFIEDBY VERIFIEDBY , ";

                SQL = SQL + "  RM_PO_APPROVED APPROVED , RM_PO_APPROVEDBY APPROVEDBY,  ";


                SQL = SQL + " 	 '--->' DETAILS,";
                SQL = SQL + " 	RM_PO_DETAILS.RM_POD_SL_NO SL_NO, RM_PO_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + " 	SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME, ";
                SQL = SQL + " 	RM_PO_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_DESC,";
                SQL = SQL + " 	RM_PO_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                SQL = SQL + " 	RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";

                SQL = SQL + " 	RM_PO_DETAILS.RM_UOM_UOM_CODE UOM_CODE,";
                SQL = SQL + " 	RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC,RM_PO_DETAILS.RM_POD_QTY POD_QTY,";
                SQL = SQL + " 	RM_PO_DETAILS.RM_POD_UNIT_PRICE UNIT_PRICE,    RM_PO_DETAILS.RM_POD_TOTALAMT	  POD_TOTALAMT, RM_PO_REMARKS";
                SQL = SQL + " FROM RM_PO_MASTER,";
                SQL = SQL + " 	RM_PO_DETAILS, AD_CURRENCY_MASTER,";
                SQL = SQL + " 	RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " 	RM_SOURCE_MASTER,";
                SQL = SQL + " 	SL_STATION_MASTER,";
                SQL = SQL + " 	HR_DEPT_MASTER,";
                SQL = SQL + " 	RM_VENDOR_MASTER,";
                SQL = SQL + " 	RM_UOM_MASTER";
                SQL = SQL + " WHERE RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO";
                SQL = SQL + " 	AND RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID";
                SQL = SQL + " 	AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " 	AND RM_PO_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                SQL = SQL + " 	AND RM_PO_DETAILS.SALES_STN_STATION_CODE =  SL_STATION_MASTER.SALES_STN_STATION_CODE    ";
                SQL = SQL + " 	AND RM_PO_DETAILS.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE";
                SQL = SQL + " 	AND RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " 	AND RM_PO_DETAILS.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE";
                SQL = SQL + "  and RM_PO_MASTER.AD_CUR_CURRENCY_CODE =AD_CURRENCY_MASTER.AD_CUR_CURRENCY_CODE";

                if (!string.IsNullOrEmpty(sBranch))
                {
                    SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE IN ( '" + sBranch + "') ";

                }
                if (!string.IsNullOrEmpty(sStation))
                {
                    SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE IN ( '" + sStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSupplier))
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE IN ( '" + sSupplier + "') ";

                }

                if (!string.IsNullOrEmpty(sRawmaterial))
                {
                    SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE IN ( '" + sRawmaterial + "') ";

                }


                SQL = SQL + "  AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                SQL = SQL + "     ORDER BY     RM_PO_DETAILS.RM_PO_PONO ASC ";

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable FetchPendingPurchaseOrderDetails(string sStation, string sBranch, string sSupplier, string sRawmaterial, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");
            //JOMY DONT KNOW THE QUERY WILL MAKE DEALAY OTHER WISE  WE 

            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();



                SQL = " SELECT RM_PO_MASTER.RM_PO_PONO PONO , RM_PO_PO_DATE PO_DATE , RM_PR_PRNO PR_PRNO,";
                SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE VENDOR_CODE,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIERNAME, RM_PO_PRICETYPE PRICETYPE ,";
                SQL = SQL + " RM_PO_ENTRY_TYPE  ENTRY_TYPE  ,";

                SQL = SQL + " RM_PO_NET_AMOUNT NET_AMOUNT , ";

                SQL = SQL + "	CASE   RM_PO_MASTER.RM_PO_PO_STATUS  WHEN 'O' THEN 'OPEN' ";
                SQL = SQL + "	WHEN 'N' THEN 'CANCEL'";
                SQL = SQL + " 	WHEN 'C' THEN 'CLOSE' END PO_STATUS, ";

                SQL = SQL + " 	 RM_PO_MASTER.AD_CUR_CURRENCY_CODE CURRENCY_CODE , AD_CURRENCY_MASTER.AD_CUR_CURRENCY_NAME CURRENCY_NAME  , ";
                SQL = SQL + " RM_PO_EXCHANGE_RATE EXCHANGE_RATE , ";

                SQL = SQL + " RM_PO_CREATEDBY CREATEDBY,   RM_PO_VERIFIEDBY VERIFIEDBY , ";

                SQL = SQL + "  RM_PO_APPROVED APPROVED , RM_PO_APPROVEDBY APPROVEDBY,  ";


                SQL = SQL + " 	 '--->' DETAILS,";
                SQL = SQL + " 	RM_PO_DETAILS.RM_POD_SL_NO SL_NO, RM_PO_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + " 	SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME, ";
                SQL = SQL + " 	RM_PO_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_DESC,";
                SQL = SQL + " 	RM_PO_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                SQL = SQL + " 	RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";

                SQL = SQL + " 	RM_PO_DETAILS.RM_UOM_UOM_CODE UOM_CODE,";
                SQL = SQL + " 	RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC,RM_PO_DETAILS.RM_POD_QTY POD_QTY,";
                SQL = SQL + " 	RM_PO_DETAILS.RM_POD_UNIT_PRICE UNIT_PRICE,    RM_PO_DETAILS.RM_POD_TOTALAMT	  POD_TOTALAMT";
                SQL = SQL + " FROM RM_PO_MASTER,";
                SQL = SQL + " 	RM_PO_DETAILS, AD_CURRENCY_MASTER,";
                SQL = SQL + " 	RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " 	RM_SOURCE_MASTER,";
                SQL = SQL + " 	SL_STATION_MASTER,";
                SQL = SQL + " 	HR_DEPT_MASTER,";
                SQL = SQL + " 	RM_VENDOR_MASTER,";
                SQL = SQL + " 	RM_UOM_MASTER";
                SQL = SQL + " WHERE RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO";
                SQL = SQL + " 	AND RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID";
                SQL = SQL + " 	AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " 	AND RM_PO_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                SQL = SQL + " 	AND RM_PO_DETAILS.SALES_STN_STATION_CODE =  SL_STATION_MASTER.SALES_STN_STATION_CODE    ";
                SQL = SQL + " 	AND RM_PO_DETAILS.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE";
                SQL = SQL + " 	AND RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " 	AND RM_PO_DETAILS.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE";
                SQL = SQL + "  and RM_PO_MASTER.AD_CUR_CURRENCY_CODE =AD_CURRENCY_MASTER.AD_CUR_CURRENCY_CODE";
                SQL = SQL + "  and RM_PO_MASTER.RM_PO_APPROVED ='Y' AND  RM_PO_MASTER.RM_PO_PO_STATUS ='O'";
                SQL = SQL + "    and  RM_PO_MASTER.RM_PO_ENTRY_TYPE  IN ( 'PURCHASE' ) ";
                SQL = SQL + "    and  RM_POD_QTY  = RM_POD_PENDING_QTY ";

                // SINCE THIS QUERY WILL MAKE DELAY PERORMANCE , I HAVE ADDED ONE MORE FLAG IN RECEIPT DETAILS WHEHTER THE RECEIPTS DFONE ORNOT 
                //SQL = SQL + "    AND (  RM_PO_MASTER.RM_PO_PONO  ,RM_PO_MASTER.AD_FIN_FINYRID  , ";
                //SQL = SQL + "    RM_PO_DETAILS.RM_RMM_RM_CODE  , ";
                //SQL = SQL + "    RM_PO_DETAILS.RM_SM_SOURCE_CODE  , ";
                //SQL = SQL + "    RM_PO_DETAILS.SALES_STN_STATION_CODE  )  ";
                //SQL = SQL + "    NOT IN  ";
                //SQL = SQL + "    ( ";
                //SQL = SQL + "    SELECT RM_MPOM_ORDER_NO,    RM_MPOM_FIN_FINYRID  , RM_RMM_RM_CODE, RM_SM_SOURCE_CODE, SALES_STN_STATION_CODE FROM RM_RECEPIT_DETAILS )  ";

                if (!string.IsNullOrEmpty(sBranch))
                {
                    SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE IN ( '" + sBranch + "') ";

                }
                if (!string.IsNullOrEmpty(sStation))
                {
                    SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE IN ( '" + sStation + "') ";
                }

                if (!string.IsNullOrEmpty(sSupplier))
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE IN ( '" + sSupplier + "') ";

                }

                if (!string.IsNullOrEmpty(sRawmaterial))
                {
                    SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE IN ( '" + sRawmaterial + "') ";

                }


                SQL = SQL + "  AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                SQL = SQL + "     ORDER BY     RM_PO_DETAILS.RM_PO_PONO ASC ";

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable FetchPurchaseOrderBalanceDetails(string sStation, string sBranch, string sSupplier, string sRawmaterial, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");
            //JOMY DONT KNOW THE QUERY WILL MAKE DEALAY OTHER WISE  WE 

            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();



                SQL = " SELECT RM_PO_MASTER.RM_PO_PONO PONO , RM_PO_PO_DATE PO_DATE , RM_PR_PRNO PR_PRNO,";
                SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE VENDOR_CODE,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPLIERNAME, RM_PO_PRICETYPE PRICETYPE ,";
                SQL = SQL + " RM_PO_ENTRY_TYPE  ENTRY_TYPE  ,";

                SQL = SQL + " RM_PO_NET_AMOUNT NET_AMOUNT , ";

                SQL = SQL + "	CASE   RM_PO_MASTER.RM_PO_PO_STATUS  WHEN 'O' THEN 'OPEN' ";
                SQL = SQL + "	WHEN 'N' THEN 'CANCEL'";
                SQL = SQL + " 	WHEN 'C' THEN 'CLOSE' END PO_STATUS, ";

                SQL = SQL + " 	 RM_PO_MASTER.AD_CUR_CURRENCY_CODE CURRENCY_CODE , AD_CURRENCY_MASTER.AD_CUR_CURRENCY_NAME CURRENCY_NAME  , ";
                SQL = SQL + " RM_PO_EXCHANGE_RATE EXCHANGE_RATE , ";

                SQL = SQL + " RM_PO_CREATEDBY CREATEDBY,   RM_PO_VERIFIEDBY VERIFIEDBY , ";

                SQL = SQL + "  RM_PO_APPROVED APPROVED , RM_PO_APPROVEDBY APPROVEDBY,  ";


                SQL = SQL + " 	 '--->' DETAILS,";
                SQL = SQL + " 	RM_PO_DETAILS.RM_POD_SL_NO SL_NO, RM_PO_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + " 	SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME, ";
                SQL = SQL + " 	RM_PO_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_DESC,";
                SQL = SQL + " 	RM_PO_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                SQL = SQL + " 	RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";

                SQL = SQL + " 	RM_PO_DETAILS.RM_UOM_UOM_CODE UOM_CODE,";
                SQL = SQL + " 	RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC,RM_PO_DETAILS.RM_POD_QTY POD_QTY,";
                SQL = SQL + " 	RM_PO_DETAILS.RM_POD_UNIT_PRICE UNIT_PRICE,    RM_PO_DETAILS.RM_POD_TOTALAMT	  POD_TOTALAMT , RM_POD_PENDING_QTY  PENDING_QTY";
                SQL = SQL + " FROM RM_PO_MASTER,";
                SQL = SQL + " 	RM_PO_DETAILS, AD_CURRENCY_MASTER,";
                SQL = SQL + " 	RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " 	RM_SOURCE_MASTER,";
                SQL = SQL + " 	SL_STATION_MASTER,";
                SQL = SQL + " 	HR_DEPT_MASTER,";
                SQL = SQL + " 	RM_VENDOR_MASTER,";
                SQL = SQL + " 	RM_UOM_MASTER";
                SQL = SQL + " WHERE RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO";
                SQL = SQL + " 	AND RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID";
                SQL = SQL + " 	AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " 	AND RM_PO_DETAILS.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                SQL = SQL + " 	AND RM_PO_DETAILS.SALES_STN_STATION_CODE =  SL_STATION_MASTER.SALES_STN_STATION_CODE    ";
                SQL = SQL + " 	AND RM_PO_DETAILS.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE";
                SQL = SQL + " 	AND RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " 	AND RM_PO_DETAILS.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE";
                SQL = SQL + "  and RM_PO_MASTER.AD_CUR_CURRENCY_CODE =AD_CURRENCY_MASTER.AD_CUR_CURRENCY_CODE";
                SQL = SQL + "  and RM_PO_MASTER.RM_PO_APPROVED ='Y' AND  RM_PO_MASTER.RM_PO_PO_STATUS ='O'";
                SQL = SQL + "    and  RM_PO_MASTER.RM_PO_ENTRY_TYPE  IN ( 'PURCHASE' ) ";
                SQL = SQL + "    and   RM_POD_PENDING_QTY  > 0 ";

                // SINCE THIS QUERY WILL MAKE DELAY PERORMANCE , I HAVE ADDED ONE MORE FLAG IN RECEIPT DETAILS WHEHTER THE RECEIPTS DFONE ORNOT 
                //SQL = SQL + "    AND (  RM_PO_MASTER.RM_PO_PONO  ,RM_PO_MASTER.AD_FIN_FINYRID  , ";
                //SQL = SQL + "    RM_PO_DETAILS.RM_RMM_RM_CODE  , ";
                //SQL = SQL + "    RM_PO_DETAILS.RM_SM_SOURCE_CODE  , ";
                //SQL = SQL + "    RM_PO_DETAILS.SALES_STN_STATION_CODE  )  ";
                //SQL = SQL + "    NOT IN  ";
                //SQL = SQL + "    ( ";
                //SQL = SQL + "    SELECT RM_MPOM_ORDER_NO,    RM_MPOM_FIN_FINYRID  , RM_RMM_RM_CODE, RM_SM_SOURCE_CODE, SALES_STN_STATION_CODE FROM RM_RECEPIT_DETAILS )  ";




                if (!string.IsNullOrEmpty(sBranch))
                {
                    SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE IN ( '" + sBranch + "') ";

                }
                if (!string.IsNullOrEmpty(sStation))
                {
                    SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE IN ( '" + sStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSupplier))
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE IN ( '" + sSupplier + "') ";

                }

                if (!string.IsNullOrEmpty(sRawmaterial))
                {
                    SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE IN ( '" + sRawmaterial + "') ";

                }


                SQL = SQL + "  AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                SQL = SQL + "     ORDER BY     RM_PO_DETAILS.RM_PO_PONO ASC ";

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        #endregion
        ///-----  end REGION PURCHASE REQUEST / PURCHASE ORDER LIST LOGIC 

        #region "Graph Reports '

        public DataSet FetchPurchaseSummaryGraph(string sStation, string sBranch, string sSupplier, string sRawmaterial, string sFromDate, string sTodate)
        {
            DataSet dtData = new DataSet("RM_PO_PROJECT_DETAILS");
            //JOMY DONT KNOW THE QUERY WILL MAKE DEALAY OTHER WISE  WE 

            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();


                SQL = "         SELECT GL_COSTING_MASTER.GL_COSM_ACCOUNT_CODE,  TO_CHAR(RM_PO_PO_DATE,'MM-YYYY') PODATE  , ";
                SQL = SQL + "     RM_PO_MASTER.AD_BR_CODE , ";
                SQL = SQL + "     MASTER_BRANCH.AD_BR_CODE MASTER_BRANCH_CODE, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_NAME MASTER_BRANCH_NAME, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_DOC_PREFIX MASTER_BRANCHDOC_PREFIX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_POBOX MASTER_BRANCH_POBOX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_ADDRESS MASTER_BRANCH_ADDRESS, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_CITY MASTER_BRANCH_CITY, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_PHONE MASTER_BRANCH_PHONE, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_FAX MASTER_BRANCH_FAX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_SPONSER_NAME MASTER_BRANCH_SPONSER_NAME, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TRADE_LICENSE_NO MASTER_BRANCH_TRADE_LICENSE_NO, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_EMAIL_ID MASTER_BRANCH_EMAIL_ID, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_WEB_SITE MASTER_BRANCH_WEB_SITE, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TAX_VAT_REG_NUMBER MASTER_BRANCH_VAT_REG_NUMBER ,";

                SQL = SQL + "        SUM(   RM_PO_PROJECT_DETAILS.RM_POD_TOTALAMT ) RM_POD_TOTALAMT  ";
                SQL = SQL + "      FROM RM_PO_MASTER, RM_PO_PROJECT_DETAILS, GL_COSTING_MASTER, ";
                SQL = SQL + "      ad_branch_master MASTER_BRANCH ";
                SQL = SQL + "      WHERE  RM_PO_MASTER.RM_PO_PONO = RM_PO_PROJECT_DETAILS.RM_PO_PONO   ";
                SQL = SQL + "          AND  RM_PO_MASTER.AD_FIN_FINYRID =   RM_PO_PROJECT_DETAILS.AD_FIN_FINYRID ";

                SQL = SQL + "          AND  RM_PO_PROJECT_DETAILS.GL_COSM_ACCOUNT_CODE =     GL_COSTING_MASTER.GL_COSM_ACCOUNT_CODE ";
                SQL = SQL + "  AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "  AND RM_PO_MASTER.AD_BR_CODE =  MASTER_BRANCH.AD_BR_CODE(+) ";

                if (!string.IsNullOrEmpty(sBranch))
                {
                    SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE IN ( '" + sBranch + "') ";

                }
                if (!string.IsNullOrEmpty(sStation))
                {
                    SQL = SQL + " AND RM_PO_PROJECT_DETAILS.SALES_STN_STATION_CODE IN ( '" + sStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSupplier))
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE IN ( '" + sSupplier + "') ";

                }

                if (!string.IsNullOrEmpty(sRawmaterial))
                {
                    SQL = SQL + " AND RM_PO_PROJECT_DETAILS.RM_RMM_RM_CODE IN ( '" + sRawmaterial + "') ";

                }
                SQL = SQL + "         GROUP BY GL_COSTING_MASTER.GL_COSM_ACCOUNT_CODE  ,TO_CHAR(RM_PO_PO_DATE,'MM-YYYY')  , ";
                SQL = SQL + "     RM_PO_MASTER.AD_BR_CODE , ";
                SQL = SQL + "     MASTER_BRANCH.AD_BR_CODE  ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_NAME , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_DOC_PREFIX , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_POBOX , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_ADDRESS , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_CITY , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_PHONE , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_FAX , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_SPONSER_NAME , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TRADE_LICENSE_NO , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_EMAIL_ID , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_WEB_SITE , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TAX_VAT_REG_NUMBER  ";

                SQL = SQL + "         order by  TO_CHAR(RM_PO_PO_DATE,'MM-YYYY')   asc  ";

                dtData = clsSQLHelper.GetDataset(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }


        #endregion

        #region "REsource wise report"

        public DataSet FetchPOResourceWiseDetails(string sStation, string sBranch, string sSupplier, string sRawmaterial, string sProject, string sResource, string sFromDate, string sTodate)
        {
            DataSet dtData = new DataSet("RM_PO_PROJECT_DETAILS");
            //JOMY DONT KNOW THE QUERY WILL MAKE DEALAY OTHER WISE  WE 

            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();


                SQL = " SELECT RM_PO_MASTER.RM_PO_PONO, RM_PO_MASTER.RM_PO_PO_DATE, RM_PO_MASTER.RM_PR_PRNO,  ";
                SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE,RM_VENDOR_MASTER.RM_VM_VENDOR_NAME, ";
                SQL = SQL + " CN_BUD_BUDGET_MASTER.SALES_PM_PROJECT_CODE,  ";
                SQL = SQL + " CN_ENQUIRY_MASTER.SALES_PM_PROJECT_NAME,  ";
                SQL = SQL + " RM_PO_PROJECT_DETAILS.RM_POD_QTY RM_POD_QTY,  ";
                SQL = SQL + " RM_PO_PROJECT_DETAILS.RM_POD_PENDING_QTY,  ";
                SQL = SQL + " RM_PO_PROJECT_DETAILS.RM_POD_TOTALAMT,  ";
                SQL = SQL + " CN_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_ITEM_CODE,  ";
                SQL = SQL + " CN_BUD_BUDGET_DETAILS.PC_BUD_RESOURCE_CODE,  ";
                SQL = SQL + " PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_NAME,  ";
                SQL = SQL + " CN_BUD_BUDGET_MASTER.PC_BUD_BUDGET_CODE,  ";
                SQL = SQL + " CN_BUD_BUDGET_MASTER.PC_BUD_BUDGET_NAME,  ";
                SQL = SQL + " CN_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_QTY,  ";
                SQL = SQL + " CN_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_AMOUNT , ";
                SQL = SQL + "     RM_PO_MASTER.ad_br_code, ";
                SQL = SQL + "     MASTER_BRANCH.AD_BR_CODE MASTER_BRANCH_CODE ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_NAME MASTER_BRANCH_NAME, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_DOC_PREFIX MASTER_BRANCHDOC_PREFIX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_POBOX MASTER_BRANCH_POBOX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_ADDRESS MASTER_BRANCH_ADDRESS, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_CITY MASTER_BRANCH_CITY, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_PHONE MASTER_BRANCH_PHONE, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_FAX MASTER_BRANCH_FAX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_SPONSER_NAME MASTER_BRANCH_SPONSER_NAME, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TRADE_LICENSE_NO MASTER_BRANCH_TRADE_LICENSE_NO, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_EMAIL_ID MASTER_BRANCH_EMAIL_ID, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_WEB_SITE MASTER_BRANCH_WEB_SITE, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TAX_VAT_REG_NUMBER MASTER_BRANCH_VAT_REG_NUMBER ";

                SQL = SQL + " FROM RM_PO_MASTER,RM_VENDOR_MASTER, RM_PO_PROJECT_DETAILS,CN_BUD_BUDGET_DETAILS,  ";
                SQL = SQL + " CN_BUD_BUDGET_MASTER,CN_ENQUIRY_MASTER,PC_BUD_RESOURCE_MASTER , ";
                SQL = SQL + " ad_branch_master MASTER_BRANCH ";

                SQL = SQL + " WHERE RM_PO_MASTER.RM_PO_PONO = RM_PO_PROJECT_DETAILS.RM_PO_PONO ";
                SQL = SQL + " and RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + " and CN_BUD_BUDGET_DETAILS.SALES_PM_PROJECT_CODE = CN_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE  ";
                SQL = SQL + " AND CN_BUD_BUDGET_DETAILS.SALES_PM_PROJECT_CODE = RM_PO_PROJECT_DETAILS.GL_COSM_ACCOUNT_CODE ";
                SQL = SQL + " AND CN_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_ITEM_CODE = RM_PO_PROJECT_DETAILS.PC_BUD_BUDGET_ITEM_CODE ";
                SQL = SQL + " AND CN_BUD_BUDGET_MASTER.PC_BUD_BUDGET_CODE = CN_BUD_BUDGET_DETAILS.PC_BUD_BUDGET_CODE  ";
                SQL = SQL + " AND CN_BUD_BUDGET_DETAILS.PC_BUD_RESOURCE_CODE = PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE  ";
                SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE =  MASTER_BRANCH.AD_BR_CODE(+) ";
                SQL = SQL + " AND RM_PO_MASTER.RM_PO_PO_STATUS <> 'N' ";

                if (!string.IsNullOrEmpty(sBranch))
                {
                    SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE IN ( '" + sBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSupplier))
                {
                    SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE IN ( '" + sSupplier + "') ";

                }

                if (!string.IsNullOrEmpty(sRawmaterial))
                {
                    SQL = SQL + " AND RM_PO_PROJECT_DETAILS.RM_RMM_RM_CODE IN ( '" + sRawmaterial + "') ";

                }

                if (!string.IsNullOrEmpty(sProject))
                {
                    SQL = SQL + " AND CN_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE IN ( '" + sProject + "') ";

                }

                if (!string.IsNullOrEmpty(sResource))
                {
                    SQL = SQL + " AND PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE IN ( '" + sResource + "') ";

                }

                SQL = SQL + " AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " ORDER BY  TO_CHAR(RM_PO_PO_DATE,'MM-YYYY')   ASC  ";

                dtData = clsSQLHelper.GetDataset(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        #endregion


        ///////////------------- end of receipt amount wise 

        #region "Fetch Receipt Amoun wise Details " 

        public DataTable FectchReceiptDetails(string sSlectedStation, string sSlectedBranch, string sSlectedVendor, string sSelectedTrans, string sSelectedRM, string sApprovedStatus, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();



                SQL = "SELECT    ";
                SQL = SQL + "    RECEIPT_NO,RECEIPT_DATE,RECEIPT_DATE_TIME,VENDOR_CODE, ";
                SQL = SQL + "    VENDOR_NAME,ORDER_NO,PRICE_TYPE, ";
                SQL = SQL + "    STATION_CODE,STATION_NAME,RM_CODE, ";
                SQL = SQL + "    RM_DESCRIPTION,UOM_CODE,UOM_DESC, ";
                SQL = SQL + "    SOURCE_CODE,SOURCE_DESC,TRANSPORTER_CODE, ";
                SQL = SQL + "    TRNSPORTERNAME,TRANSPO_ORDER_NO,TRNSPORTER_DOC_NO,ASSET_CODE,VEHICLE_DESCRIPTION,DRIVER_CODE, ";
                SQL = SQL + "    DRIVER_NAME,VENDOR_DOC_NO, ";
                SQL = SQL + "    SUPPLY_QTY,WEIGH_QTY,REJECTED_QTY,APPROVE_QTY, ";
                SQL = SQL + "    SUPP_UNIT_PRICE,SUPP_AMOUNT,TRANS_RATE,TRANS_AMOUNT,TOTAL_AMOUNT, ";
                SQL = SQL + "    SUPP_UNIT_PRICE_NEW,SUPP_AMOUNT_NEW,TRANS_RATE_NEW,TRANS_AMOUNT_NEW,TOTAL_AMOUNT_NEW, ";
                SQL = SQL + "    RM_MRD_APPROVED,RM_MRM_APPROVED_NO,RM_MRD_SUPP_ENTRY,RM_MRD_SUPP_PENO, ";
                SQL = SQL + "    RM_MRD_TRANS_ENTRY,RM_MRD_TRANS_PENO, ";
                SQL = SQL + "    WEIGH_BRIDGE_DOC_NO,RM_MRD_REJECTED_YN, ";
                SQL = SQL + "    NVL(TOLL_TOTAL_AMOUNT,0 ) TOLL_TOTAL_AMOUNT,NVL(OTHER_CHARGE_TOTAL_AMOUNT,0) OTHER_CHARGE_TOTAL_AMOUNT,";
                SQL = SQL + "    RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE,RM_MRD_TOLL_AMOUNT,RM_POD_TOLL_RATE_NEW, ";
                SQL = SQL + "    RM_MRD_TOLL_AMOUNT_NEW,RM_MRD_TOLL_PE_ENTRY_YN,RM_MRD_TOLL_PE_ENTRY_NO, ";
                SQL = SQL + "    RM_MRD_TOLL_PE_ENTRY_FINYR_ID,AD_BR_CODE, ";
                SQL = SQL + "    MASTER_BRANCH_CODE ";
                SQL = SQL + " FROM RM_RECEIPT_QTY_AMOUNT_RPT_VW  ";


                if (DateTimeFilterType == "Date")
                {

                    SQL = SQL + " WHERE  TO_DATE(TO_CHAR(RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                }
                else if (DateTimeFilterType == "Time")
                {
                    SQL = SQL + "    WHERE   RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                }


                if (!string.IsNullOrEmpty(sSlectedBranch))
                {
                    SQL = SQL + " AND AD_BR_CODE IN ( '" + sSlectedBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND STATION_CODE IN ( '" + sSlectedStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedTrans))
                {
                    SQL = SQL + " AND  TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_CODE IN ( '" + sSelectedRM + "') ";

                }

                if (sApprovedStatus != "ALL")
                {
                    SQL = SQL + " AND  RM_MRD_APPROVED = '" + sApprovedStatus + "'";

                }






                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable FectchReceiptDetailsTransporter(string sSlectedStation, string sSlectedBranch, string sSlectedVendor, string sSelectedTrans, string sSelectedRM, string sApprovedStatus, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();
                //actaully we can  do the  add one more  parameter , howeve just tryn t abvvoide the confusion 

                //ClearScreen();



                //////////////    SQL = " SELECT  ";
                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO RECEIPT_NO ,";

                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE RECEIPT_DATE,";
                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  VENDOR_CODE,";
                //////////////    SQL = SQL + "  RM_VENDOR_MASTER.RM_VM_VENDOR_NAME    VENDOR_NAME,";

                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO,";

                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE PRICE_TYPE,";

                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                //////////////    SQL = SQL + "SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";
                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                //////////////    SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";
                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_UM_UOM_CODE UOM_CODE ,";
                //////////////    SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC,";
                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,";
                //////////////    SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_DESC ,";
                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE TRANSPORTER_CODE, ";
                //////////////    SQL = SQL + "  TRANSVENDOR.RM_VM_VENDOR_NAME TRNSPORTERNAME, ";
                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO TRANSPO_ORDER_NO, ";

                //////////////    SQL = SQL + "RM_MRM_TRNSPORTER_DOC_NO  TRNSPORTER_DOC_NO ,";


                //////////////    SQL = SQL + " FA_FAM_ASSET_CODE ASSET_CODE ,RM_MRM_VEHICLE_DESCRIPTION VEHICLE_DESCRIPTION, ";
                //////////////    SQL = SQL + "  RM_MRD_DRIVER_CODE DRIVER_CODE ,RM_MRD_DRIVER_NAME DRIVER_NAME, ";

                //////////////    SQL = SQL + "RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO,";
                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY SUPPLY_QTY,";
                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY WEIGH_QTY,  RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY REJECTED_QTY  ,";
                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY APPROVE_QTY, ";
                //////////////    SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE SUPP_UNIT_PRICE,  ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT SUPP_AMOUNT,  ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE TRANS_RATE,  ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT TRANS_AMOUNT , ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT TOTAL_AMOUNT,  ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW SUPP_UNIT_PRICE_NEW,  ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW SUPP_AMOUNT_NEW,  ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW TRANS_RATE_NEW,  ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW TRANS_AMOUNT_NEW,  ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW TOTAL_AMOUNT_NEW,   ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_APPROVED  ,   ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRM_APPROVED_NO,  ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY    ,  ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO   ,   ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRD_TRANS_ENTRY   , ";
                //////////////    SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO  , ";

                //////////////    SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_WEIGH_BRIDGE_DOC_NO  WEIGH_BRIDGE_DOC_NO  ,   ";
                //////////////    SQL = SQL + "    RM_MRD_REJECTED_YN     ,  RM_UOM_TOLL_CODE  ,  RM_POD_TOLL_RATE , ";
                //////////////    SQL = SQL + "   RM_MRD_TOLL_AMOUNT ,  RM_POD_TOLL_RATE_NEW ,  RM_MRD_TOLL_AMOUNT_NEW , ";
                //////////////    SQL = SQL + "   RM_MRD_TOLL_PE_ENTRY_YN ,  RM_MRD_TOLL_PE_ENTRY_NO    ,  RM_MRD_TOLL_PE_ENTRY_FINYR_ID, ";

                //////////////    SQL = SQL + "     rm_recepit_details.ad_br_code, ";
                //////////////    SQL = SQL + "     MASTER_BRANCH.AD_BR_CODE MASTER_BRANCH_CODE ";



                //////////////    SQL = SQL + "FROM RM_RECEPIT_DETAILS,";
                //////////////    SQL = SQL + "RM_RAWMATERIAL_MASTER,";
                //////////////    SQL = SQL + "RM_UOM_MASTER,";
                //////////////    SQL = SQL + "RM_VENDOR_MASTER,";
                //////////////    SQL = SQL + "SL_STATION_MASTER , ";
                //////////////    SQL = SQL + "RM_SOURCE_MASTER, ";
                //////////////    SQL = SQL + " RM_VENDOR_MASTER TRANSVENDOR ,ad_branch_master MASTER_BRANCH ";
                //////////////    SQL = SQL + "WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                //////////////    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE =  MASTER_BRANCH.AD_BR_CODE(+) ";
                //////////////    SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE    ";
                //////////////    SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE    ";

                //////////////    SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                //////////////    SQL = SQL + "AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                //////////////    SQL = SQL + "  AND     RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =  TRANSVENDOR.RM_VM_VENDOR_CODE (+)";
                //////////////    ///     ' SQL = SQL  +  "AND RM_MRD_APPROVED = 'N'"

                //////////////    SQL = SQL + "  AND     RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE  is not null "; // e required else all   other records wil lcome  on siter cases 

                //////////////    if (!string.IsNullOrEmpty(sSlectedBranch))
                //////////////    {
                //////////////        SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + sSlectedBranch + "') ";

                //////////////    }

                //////////////    if (!string.IsNullOrEmpty(sSlectedStation))
                //////////////    {
                //////////////        SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE IN ( '" + sSlectedStation + "') ";

                //////////////    }

                //////////////    if (!string.IsNullOrEmpty(sSlectedVendor))
                //////////////    {
                //////////////        SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                //////////////    }

                //////////////    if (!string.IsNullOrEmpty(sSelectedTrans))
                //////////////    {
                //////////////        SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                //////////////    }

                //////////////    if (!string.IsNullOrEmpty(sSelectedRM))
                //////////////    {
                //////////////        SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                //////////////    }

                //////////////    if (sApprovedStatus != "ALL")
                //////////////    {
                //////////////        SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MRD_APPROVED = '" + sApprovedStatus + "'";

                //////////////    }

                ////////////////    SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";



                //////////////    if (DateTimeFilterType == "Date")
                //////////////    {

                //////////////        SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                //////////////    }
                //////////////    else if (DateTimeFilterType == "Time")
                //////////////    {
                //////////////        SQL = SQL + "    AND   RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                //////////////        SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                //////////////    }






                SQL = "SELECT    ";
                SQL = SQL + "    RECEIPT_NO,RECEIPT_DATE,RECEIPT_DATE_TIME,VENDOR_CODE, ";
                SQL = SQL + "    VENDOR_NAME,ORDER_NO,PRICE_TYPE, ";
                SQL = SQL + "    STATION_CODE,STATION_NAME,RM_CODE, ";
                SQL = SQL + "    RM_DESCRIPTION,UOM_CODE,UOM_DESC, ";
                SQL = SQL + "    SOURCE_CODE,SOURCE_DESC,TRANSPORTER_CODE, ";
                SQL = SQL + "    TRNSPORTERNAME,TRANSPO_ORDER_NO,TRNSPORTER_DOC_NO,ASSET_CODE,VEHICLE_DESCRIPTION,DRIVER_CODE, ";
                SQL = SQL + "    DRIVER_NAME,VENDOR_DOC_NO, ";
                SQL = SQL + "    SUPPLY_QTY,WEIGH_QTY,REJECTED_QTY,APPROVE_QTY, ";
                SQL = SQL + "    SUPP_UNIT_PRICE,SUPP_AMOUNT,TRANS_RATE,TRANS_AMOUNT,TOTAL_AMOUNT, ";
                SQL = SQL + "    SUPP_UNIT_PRICE_NEW,SUPP_AMOUNT_NEW,TRANS_RATE_NEW,TRANS_AMOUNT_NEW,TOTAL_AMOUNT_NEW, ";
                SQL = SQL + "    RM_MRD_APPROVED,RM_MRM_APPROVED_NO,RM_MRD_SUPP_ENTRY,RM_MRD_SUPP_PENO, ";
                SQL = SQL + "    RM_MRD_TRANS_ENTRY,RM_MRD_TRANS_PENO, ";
                SQL = SQL + "    WEIGH_BRIDGE_DOC_NO,RM_MRD_REJECTED_YN, ";
                SQL = SQL + "    TOLL_TOTAL_AMOUNT ,OTHER_CHARGE_TOTAL_AMOUNT ,";
                SQL = SQL + "   RM_UOM_TOLL_CODE,RM_POD_TOLL_RATE,RM_MRD_TOLL_AMOUNT,RM_POD_TOLL_RATE_NEW, ";
                SQL = SQL + "    RM_MRD_TOLL_AMOUNT_NEW,RM_MRD_TOLL_PE_ENTRY_YN,RM_MRD_TOLL_PE_ENTRY_NO, ";
                SQL = SQL + "    RM_MRD_TOLL_PE_ENTRY_FINYR_ID,AD_BR_CODE, ";
                SQL = SQL + "    MASTER_BRANCH_CODE ";
                SQL = SQL + " FROM RM_RECEIPT_QTY_AMOUNT_RPT_VW  ";


                if (DateTimeFilterType == "Date")
                {

                    SQL = SQL + " WHERE  TO_DATE(TO_CHAR(RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                }
                else if (DateTimeFilterType == "Time")
                {
                    SQL = SQL + "    WHERE   RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                }

                SQL = SQL + "  AND     TRANSPORTER_CODE  is not null "; // e required else all   other records wil lcome  on siter cases 


                if (!string.IsNullOrEmpty(sSlectedBranch))
                {
                    SQL = SQL + " AND AD_BR_CODE IN ( '" + sSlectedBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND STATION_CODE IN ( '" + sSlectedStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedTrans))
                {
                    SQL = SQL + " AND  TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_CODE IN ( '" + sSelectedRM + "') ";

                }

                if (sApprovedStatus != "ALL")
                {
                    SQL = SQL + " AND  RM_MRD_APPROVED = '" + sApprovedStatus + "'";

                }



                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable FectchReceiptDetailsXLSpecifedColumns(string sSlectedStation, string sSlectedBranch, string sSlectedVendor, string sSelectedTrans, string sSelectedRM, string sApprovedStatus, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();



                SQL = " SELECT  ";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO RECEIPT_NO ,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO,";

                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE RECEIPT_DATE,";
                SQL = SQL + "RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO,";

                SQL = SQL + "RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  VENDOR_CODE,";
                SQL = SQL + "  RM_VENDOR_MASTER.RM_VM_VENDOR_NAME    VENDOR_NAME,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";

                SQL = SQL + "RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,";
                SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_DESC ,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY APPROVE_QTY, ";

                SQL = SQL + "RM_RECEPIT_DETAILS.RM_UM_UOM_CODE UOM_CODE ,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC,";

                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW SUPP_UNIT_PRICE_NEW,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW SUPP_AMOUNT_NEW,  ";
                SQL = SQL + "RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + "SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";

                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE PRICE_TYPE,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO TRANSPO_ORDER_NO, ";

                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE TRANSPORTER_CODE, ";
                SQL = SQL + "  TRANSVENDOR.RM_VM_VENDOR_NAME TRNSPORTERNAME, ";
                SQL = SQL + "  RM_MRM_VEHICLE_DESCRIPTION VEHICLE_DESCRIPTION, ";

                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW TRANS_RATE_NEW,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW TRANS_AMOUNT_NEW,  ";

                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW TOTAL_AMOUNT_NEW,   ";

                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_APPROVED  ,   ";
                SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRM_APPROVED_NO,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY    ,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO   ,   ";
                SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRD_TRANS_ENTRY   , ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO    ";


                SQL = SQL + "FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + "RM_RAWMATERIAL_MASTER,";
                SQL = SQL + "RM_UOM_MASTER,";
                SQL = SQL + "RM_VENDOR_MASTER,";
                SQL = SQL + "SL_STATION_MASTER , ";
                SQL = SQL + "RM_SOURCE_MASTER, ";
                SQL = SQL + " RM_VENDOR_MASTER TRANSVENDOR ";
                SQL = SQL + "WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE    ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE    ";

                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + "  AND     RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =  TRANSVENDOR.RM_VM_VENDOR_CODE (+)";
                ///     ' SQL = SQL  +  "AND RM_MRD_APPROVED = 'N'"

                if (!string.IsNullOrEmpty(sSlectedBranch))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + sSlectedBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE IN ( '" + sSlectedStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedTrans))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                }

                if (sApprovedStatus != "ALL")
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='" + sApprovedStatus + "'";

                }

                //SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";



                if (DateTimeFilterType == "Date")
                {

                    SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                }
                else if (DateTimeFilterType == "Time")
                {
                    SQL = SQL + "    AND   RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                }


                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable FectchReceiptSummaryRMWISE(string sSlectedStation, string sSlectedBranch, string sSlectedVendor, string sSelectedTrans, string sSelectedRM, string sApprovedStatus, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();




                SQL = " SELECT  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                SQL = SQL + "  RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";


                SQL = SQL + "  sum( RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY ) SUPPLY_QTY,";
                SQL = SQL + "   sum(RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY  ) WEIGH_QTY, ";
                SQL = SQL + "  sum( RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY  ) REJECTED_QTY  ,";
                SQL = SQL + "   sum(RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY  ) APPROVE_QTY , ";

                //' SQL = SQL +  "   RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE SUPP_UNIT_PRICE,  ";
                //'SQL = SQL & "RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT SUPP_AMOUNT,  "
                //' SQL = SQL & "RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE TRANS_RATE,  "
                //' SQL = SQL & "RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT TRANS_AMOUNT , "
                //' SQL = SQL & "RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT TOTAL_AMOUNT,  "
                SQL = SQL + "   case when  sum  (RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY  )   > 0 then ";
                SQL = SQL + "    trunc ( SUM(RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW ) /  sum  (RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY  ) ,2) ";
                SQL = SQL + "   else 0 end AvgRate, ";
                SQL = SQL + "  SUM(RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW ) SUPP_UNIT_PRICE_NEW,  ";
                SQL = SQL + "  SUM(RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW ) SUPP_AMOUNT_NEW,  ";
                //'SQL = SQL & "RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW TRANS_RATE_NEW,  "
                SQL = SQL + "  SUM(RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW ) TRANS_AMOUNT_NEW,  ";
                SQL = SQL + "  SUM(RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW ) TOTAL_AMOUNT_NEW   ";

                SQL = SQL + "  FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + "  RM_RAWMATERIAL_MASTER ";


                SQL = SQL + "  WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                // ' SQL = SQL +  "  AND RM_MRD_APPROVED = 'N'"


                //SQL = SQL + "  AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))";
                //SQL = SQL + "    BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";



                if (DateTimeFilterType == "Date")
                {

                    SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                }
                else if (DateTimeFilterType == "Time")
                {
                    SQL = SQL + "    AND   RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                }

                if (!string.IsNullOrEmpty(sSlectedBranch))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + sSlectedBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE IN ( '" + sSlectedStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedTrans))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                }

                //if ( sApprovedStatus =="N")
                //   {
                //   SQL = SQL + " AND RM_RECEPIT_DETAILS RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='N'";

                //   } 

                SQL = SQL + "  GROUP BY RM_RECEPIT_DETAILS.RM_RMM_RM_CODE  ,";
                SQL = SQL + "  RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION  ";





                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable FectchReceiptSummaryVENDORWISE(string sSlectedStation, string sSlectedBranch, string sSlectedVendor, string sSelectedTrans, string sSelectedRM, string sApprovedStatus, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();

                SQL = " SELECT  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  VENDOR_CODE,";
                SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_NAME    VENDOR_NAME,";

                SQL = SQL + "  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + "  SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                SQL = SQL + "  RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";
                SQL = SQL + "  sum( RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY ) SUPPLY_QTY,";
                SQL = SQL + "   sum(RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY  ) WEIGH_QTY, ";
                SQL = SQL + "  sum( RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY  ) REJECTED_QTY  ,";
                SQL = SQL + "   sum(RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY  ) APPROVE_QTY , ";

                SQL = SQL + "   case when  sum  (RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY  )   > 0 then ";
                SQL = SQL + "  trunc (  SUM(RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW ) /  sum  (RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY  ),2)  ";
                SQL = SQL + "   else 0 end AvgRate, ";
                SQL = SQL + "  SUM(RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW ) SUPP_UNIT_PRICE_NEW,  ";
                SQL = SQL + "  SUM(RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW ) SUPP_AMOUNT_NEW ,  ";
                //  'SQL = SQL & "RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW TRANS_RATE_NEW,  "
                SQL = SQL + "  SUM(RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW ) TRANS_AMOUNT_NEW,  ";
                SQL = SQL + "  SUM(RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW ) TOTAL_AMOUNT_NEW   ";


                SQL = SQL + "  FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + "  RM_RAWMATERIAL_MASTER,";

                SQL = SQL + "  RM_VENDOR_MASTER,";
                SQL = SQL + "  SL_STATION_MASTER ";
                SQL = SQL + "  WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + "  AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE    ";
                SQL = SQL + "  AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";


                // ' SQL = SQL +  "  AND RM_MRD_APPROVED = 'N'"


                //SQL = SQL + "  AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))";
                //SQL = SQL + "    BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";




                if (DateTimeFilterType == "Date")
                {

                    SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                }
                else if (DateTimeFilterType == "Time")
                {
                    SQL = SQL + "    AND   RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                }

                if (!string.IsNullOrEmpty(sSlectedBranch))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + sSlectedBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE IN ( '" + sSlectedStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedTrans))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                }

                //if ( sApprovedStatus =="N")
                //   {
                //   SQL = SQL + " AND RM_RECEPIT_DETAILS RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='N'";

                //   }  

                SQL = SQL + " GROUP BY RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE   ,";
                SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_NAME     ,";

                SQL = SQL + "  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE  ,";
                SQL = SQL + "  SL_STATION_MASTER.SALES_STN_STATION_NAME  ,";
                SQL = SQL + "   RM_RECEPIT_DETAILS.RM_RMM_RM_CODE  ,";
                SQL = SQL + "  RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION ";


                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable FectchReceiptSummarySTATIONWISE(string sSlectedStation, string sSelectedBranch, string sSlectedVendor, string sSelectedTrans, string sSelectedRM, string sApprovedStatus, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();

                SQL = " SELECT  ";

                SQL = SQL + "  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + "  SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                SQL = SQL + "  RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";
                SQL = SQL + "  rm_recepit_details.ad_br_code,";
                SQL = SQL + "     MASTER_BRANCH.AD_BR_CODE MASTER_BRANCH_CODE, ";

                SQL = SQL + "  sum( RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY ) SUPPLY_QTY,";
                SQL = SQL + "   sum(RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY  ) WEIGH_QTY, ";
                SQL = SQL + "  sum( RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY  ) REJECTED_QTY  ,";
                SQL = SQL + "   sum(RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY  ) APPROVE_QTY, ";


                SQL = SQL + "   case when  sum  (RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY  )   > 0 then ";
                SQL = SQL + "   trunc ( SUM(RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW ) /  sum  (RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY  ) ,2) ";
                SQL = SQL + "   else 0 end AvgRate, ";
                SQL = SQL + "   case when  sum  (RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY  )   > 0 then ";
                SQL = SQL + "   trunc ( SUM(RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW ) /  sum  (RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY  ) ,2) ";
                SQL = SQL + "   else 0 end SUPP_UNIT_PRICE_NEW, ";
                SQL = SQL + "  SUM(RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW ) SUPP_AMOUNT_NEW,  ";
                //   'SQL = SQL & "RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW TRANS_RATE_NEW,  "
                SQL = SQL + "  SUM(RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW ) TRANS_AMOUNT_NEW,  ";
                SQL = SQL + "  SUM(RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW ) TOTAL_AMOUNT_NEW   ";




                SQL = SQL + "  FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + "  RM_RAWMATERIAL_MASTER,";


                SQL = SQL + "  SL_STATION_MASTER,  ad_branch_master MASTER_BRANCH  ";
                SQL = SQL + "  WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";

                SQL = SQL + "  AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE =  MASTER_BRANCH.AD_BR_CODE(+) ";


                // ' SQL = SQL +  "  AND RM_MRD_APPROVED = 'N'"

                //SQL = SQL + "   AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))";
                //SQL = SQL + "    BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";




                if (DateTimeFilterType == "Date")
                {

                    SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                }
                else if (DateTimeFilterType == "Time")
                {
                    SQL = SQL + "    AND   RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                }

                if (!string.IsNullOrEmpty(sSelectedBranch))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + sSelectedBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE IN ( '" + sSlectedStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedTrans))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                }

                //if ( sApprovedStatus =="N")
                //   {
                //   SQL = SQL + " AND RM_RECEPIT_DETAILS RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='N'";

                //   }  

                SQL = SQL + " GROUP BY     ";

                SQL = SQL + " RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE  ,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME  ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE  ,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + " rm_recepit_details.ad_br_code, ";
                SQL = SQL + "     MASTER_BRANCH.AD_BR_CODE  ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_NAME , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_DOC_PREFIX , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_POBOX , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_ADDRESS , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_CITY , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_PHONE , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_FAX , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_SPONSER_NAME , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TRADE_LICENSE_NO , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_EMAIL_ID , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_WEB_SITE , ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TAX_VAT_REG_NUMBER  ";



                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataSet FetchTollSupplierDetails(string sSlectedStation, string sSlectedVendor, string sSelectedTrans, string sSelectedRM, string sApprovedStatus, string sFromDate, string sTodate)
        {

            DataSet dsTollSupplierDtls = new DataSet();

            try
            {

                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT  ";
                SQL = SQL + "RM_MRM_RECEIPT_NO, AD_FIN_FINYRID, RM_MRM_RECEIPT_DATE,  ";
                SQL = SQL + "   RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID, RM_MPOM_PRICE_TYPE,  ";
                SQL = SQL + "   RM_MRD_SL_NO, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME,  ";
                SQL = SQL + "   RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION, RM_UM_UOM_CODE,  ";
                SQL = SQL + "   RM_UM_UOM_DESC, RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC,  ";
                SQL = SQL + "   RM_MTRANSPO_ORDER_NO, RM_MTRANSPO_FIN_FINYRID, RM_MRM_TRNSPORTER_DOC_NO,  ";
                SQL = SQL + "   RM_MRD_ORDER_QTY, RM_MRD_VENDOR_DOC_NO, FA_FAM_ASSET_CODE,  ";
                SQL = SQL + "   RM_MRM_VEHICLE_DESCRIPTION, RM_MRD_DRIVER_CODE, RM_MRD_DRIVER_NAME,  ";
                SQL = SQL + "   RM_MRD_SUPPLY_QTY, RM_MRD_WEIGH_QTY, RM_MRD_APPROVE_QTY,  ";
                SQL = SQL + "   RM_MRD_REJECTED_QTY, RM_MRD_SUPP_UNIT_PRICE, RM_MRD_SUPP_AMOUNT,  ";
                SQL = SQL + "   RM_MRD_TRANS_RATE, RM_MRD_TRANS_AMOUNT, RM_MRD_TOTAL_AMOUNT,  ";
                SQL = SQL + "   RM_MRD_SUPP_UNIT_PRICE_NEW, RM_MRD_SUPP_AMOUNT_NEW, RM_MRD_TRANS_RATE_NEW,  ";
                SQL = SQL + "   RM_MRD_TRANS_AMOUNT_NEW, RM_MRD_TOTAL_AMOUNT_NEW, RM_MRD_SUPP_PENO,  ";
                SQL = SQL + "   RM_MRD_TRANS_PENO, RM_MRM_RECEIPT_UNIQE_NO, RM_VM_VENDOR_CODE,  ";
                SQL = SQL + "   RM_VM_VENDOR_NAME, RM_MRM_TOLL_NO, RM_MRM_TOLL_RATE,RM_TL_LOCATION_CODE,RM_TL_LOCATION_DESC,  ";
                SQL = SQL + "   RM_MRM_TOLL_AMOUNT, RM_MRM_TOLL_PE_DONE_YN, RM_MRM_TOLL_PE_NO,RM_MRM_TOLL_DATE ,";
                SQL = SQL + "       ad_br_code, ";
                SQL = SQL + "       MASTER_BRANCH_CODE   ";

                SQL = SQL + "FROM RM_RECEIPT_TOLL_DTLS_RPT_VW ";
                SQL = SQL + " where RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromDate, culture).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + " AND '" + System.Convert.ToDateTime(sTodate, culture).ToString("dd-MMM-yyyy") + "'";



                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND   RM_VM_VENDOR_CODE  IN ( '" + sSlectedVendor + "') ";
                }

                SQL = SQL + "order by rm_mrm_receipt_no,RM_VM_VENDOR_CODE asc ";

                dsTollSupplierDtls = clsSQLHelper.GetDataset(SQL);
                dsTollSupplierDtls.Tables[0].TableName = "TollSupplierDetails";
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dsTollSupplierDtls;
        }

        #endregion


        #region "report Generation fetch statment " 
        public DataSet FetchIssueAllDetails()
        {
            DataSet dsVouchers = new DataSet();

            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "   ";
                SQL = "SELECT ISSUETYPE, DOC_NO, FINYRID, DOC_DATE, ENQUIRY_NO, PROJECT_NAME, SRTYPE, ";
                SQL = SQL + "       SUBISSUETYPE, ITEM_CODE, ITEM_DESC, SL_NO, QTY, PRICE, AMOUNT, ";
                SQL = SQL + "       RETURN_AMOUNT, DIVISION_CODE, DIVISION_NAME, PC_BUD_RESOURCE_CODE, ";
                SQL = SQL + "       PC_BUD_RESOURCE_NAME ";
                SQL = SQL + "  FROM WS_ISSUEALL_PROJECT_REPORT_VW ";



                SQL = SQL + " WHERE  ";


                SQL = SQL + "   ISSUETYPE in ( 'RM')";

                if (!string.IsNullOrEmpty(SelectedProject))
                {
                    SQL = SQL + " AND ENQUIRY_NO in('" + SelectedProject + "')";

                }
                if (!string.IsNullOrEmpty(SelectedRM))
                {
                    SQL = SQL + " AND ITEM_CODE in('" + SelectedRM + "')";

                }

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND DIVISION_CODE in('" + Branch + "')";

                }


                ///   string Project,string RmCode,string Station,string Branch, string sFromDate, string sTodate


                SQL = SQL + "  AND   DOC_DATE BETWEEN  '" + ReceptFromDateTime.ToString("dd-MMM-yyyy") + "' AND '" + ReceptToDateTime.ToString("dd-MMM-yyyy") + "'";




                //if (!string.IsNullOrEmpty(ResourceCode))
                //{
                //    SQL = SQL + " AND PC_BUD_RESOURCE_CODE in ( '" + ResourceCode + "')";

                //}


                //if (!string.IsNullOrEmpty(sProjectCode))
                //{
                //    SQL = SQL + " AND ENQUIRY_NO in ( '" + sProjectCode + "')";
                //}

                SQL = SQL + " ORDER BY ENQUIRY_NO ASC ";




                dsVouchers = clsSQLHelper.GetDataset(SQL);

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
            return dsVouchers;
        }



        public DataSet FetchIssueAllDetailsGLTransactions()
        {
            DataSet dsVouchers = new DataSet();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "   ";
                SQL = "SELECT ISSUETYPE, DOC_NO, FINYRID, DOC_DATE, ENQUIRY_NO, PROJECT_NAME, SRTYPE, ";
                SQL = SQL + "       SUBISSUETYPE, ITEM_CODE, ITEM_DESC, SL_NO, QTY, PRICE, AMOUNT, ";
                SQL = SQL + "       RETURN_AMOUNT, DIVISION_CODE, DIVISION_NAME, PC_BUD_RESOURCE_CODE, ";
                SQL = SQL + "       PC_BUD_RESOURCE_NAME  ,ACC_CODE ,    ACC_NAME ";
                SQL = SQL + "  FROM WS_ISSUEALL_PROJECT_REPORT_VW ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + "   ISSUETYPE in ( 'RM')";
                SQL = SQL + "  AND   DOC_DATE BETWEEN  '" + ReceptFromDateTime.ToString("dd-MMM-yyyy") + "' AND '" + ReceptToDateTime.ToString("dd-MMM-yyyy") + "'";



                //if (!string.IsNullOrEmpty(ResourceCode))
                //{
                //    SQL = SQL + " AND PC_BUD_RESOURCE_CODE in ( '" + ResourceCode + "')";

                //}


                //if (!string.IsNullOrEmpty(sProjectCode))
                //{
                //    SQL = SQL + " AND ENQUIRY_NO in ( '" + sProjectCode + "')";
                //}

                SQL = SQL + " ORDER BY ENQUIRY_NO ASC ";




                dsVouchers = clsSQLHelper.GetDataset(SQL);

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
            return dsVouchers;
        }




        #endregion



        #region  " mateireal issue regions jomy 11 02 2025" 

        public DataTable FillMaterialIssueToSiteDetailsXLList()
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();

                SQL = " ";
                SQL = SQL + "  SELECT ";
                SQL = SQL + "        ENTRY_NO,ENTRY_DATE,FINID,  RQ_ENTRY_NO,ENQUIRY_NO,COSM_ACCOUNT_CODE,  COSM_ACCOUNT_NAME,STATION_CODE,STATION_NAME, ";
                SQL = SQL + "        PRODUCT_CODE,     PRODUCT_DESC,CNCR_BATCHED_QTY,REMARKS, ";
                SQL = SQL + "        MATAMOUNT,MASTER_BRANCH_CODE,CREATEDBY,   VERIFIED,VERIFIED_BY,VERIFIED_DATE, ";
                SQL = SQL + "        APPROVED,APPROVED_BY,APPROVED_DATE,   RECEIVED_EMPCODE,RECEIVED_EMPNAME, ";
                SQL = SQL + "        SLNO,    RM_TYPE,RMCODE,   RMDESC,UOM_CODE,   UOM_DESC,AVG_COST, ";

                SQL = SQL + "  MATERIAL_ISSUE_QTY  , ";   //,     MATERIAL_ISSUE_AMNT,     MATERIAL_REQUESTED_QTY,     MATERIAL_REQUESTEDBAL_QTY

                SQL = SQL + "         TO_BASIC_UOM , TO_UOM_DESC, CONVERSION_FACTOR  ,QUANTITY_CONVRTD  ";


                SQL = SQL + "   FROM RM_STORE_ISSUE_RPT_VW  ";
                SQL = SQL + "   where   ENTRY_DATE   BETWEEN '" + System.Convert.ToDateTime(ReceptFromDateTime).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(ReceptToDateTime).ToString("dd-MMM-yyyy") + "'";

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND MASTER_BRANCH_CODE IN ( '" + Branch + "') ";

                }

                if (!string.IsNullOrEmpty(SlectedStation))
                {
                    SQL = SQL + " AND  STATION_CODE  IN ( '" + SlectedStation + "') ";

                }



                if (!string.IsNullOrEmpty(SelectedRM))
                {
                    SQL = SQL + " AND  RMCODE  IN ( '" + SelectedRM + "') ";

                }

                if (ApprovedStatus != "ALL")
                {
                    SQL = SQL + " AND  APPROVED  ='" + ApprovedStatus + "'";

                }


                if (!string.IsNullOrEmpty(SelectedProject))
                {
                    SQL = SQL + " AND  ENQUIRY_NO  IN ( '" + SelectedProject + "') ";

                }
                SQL = SQL + "  order by  ENQUIRY_NO ,  ENTRY_NO,ENTRY_DATE ,SLNO  asc   ";




                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }


        public DataTable FillMaterialIssueToSiteSummaryXLList()
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();


                SQL = "SELECT  ";
                SQL = SQL + "    ENQUIRY_NO,COSM_ACCOUNT_CODE, COSM_ACCOUNT_NAME, MASTER_BRANCH_CODE,  ";
                SQL = SQL + "    RM_TYPE,RMCODE, RMDESC,UOM_CODE,UOM_DESC  ,  ";
                SQL = SQL + "    sum(MATERIAL_ISSUE_QTY  ) MATERIAL_ISSUE_QTY   ,  SUM (QUANTITY_CONVRTD)     QUANTITY_CONVRTD     ";
                SQL = SQL + "    FROM RM_STORE_ISSUE_RPT_VW   ";

                //, sum(MATERIAL_REQUESTED_QTY  ) MATERIAL_REQUESTED_QTY ,  sum(MATERIAL_REQUESTEDBAL_QTY  ) MATERIAL_REQUESTEDBAL_QTY


                SQL = SQL + " where   ENTRY_DATE   BETWEEN '" + System.Convert.ToDateTime(ReceptFromDateTime).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(ReceptToDateTime).ToString("dd-MMM-yyyy") + "'";





                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND MASTER_BRANCH_CODE IN ( '" + Branch + "') ";

                }

                if (!string.IsNullOrEmpty(SlectedStation))
                {
                    SQL = SQL + " AND  STATION_CODE  IN ( '" + SlectedStation + "') ";

                }



                if (!string.IsNullOrEmpty(SelectedRM))
                {
                    SQL = SQL + " AND  RMCODE  IN ( '" + SelectedRM + "') ";

                }

                if (ApprovedStatus != "ALL")
                {
                    SQL = SQL + " AND  APPROVED  ='" + ApprovedStatus + "'";

                }


                if (!string.IsNullOrEmpty(SelectedProject))
                {
                    SQL = SQL + " AND  ENQUIRY_NO  IN ( '" + SelectedProject + "') ";

                }
                SQL = SQL + "group by  ";
                SQL = SQL + "    ENQUIRY_NO,COSM_ACCOUNT_CODE,  COSM_ACCOUNT_NAME, MASTER_BRANCH_CODE,  ";
                SQL = SQL + "    RM_TYPE,RMCODE, RMDESC,UOM_CODE,UOM_DESC   ";


                SQL = SQL + "  order by  ENQUIRY_NO ,   RMCODE   asc   ";




                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }


        public DataTable FillMaterialIssueToSiteDetailsPrint()
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();

                SQL = " ";
                SQL = SQL + "  SELECT ";
                SQL = SQL + "        ENTRY_NO,ENTRY_DATE,FINID, ";
                SQL = SQL + "        RQ_ENTRY_NO,ENQUIRY_NO,COSM_ACCOUNT_CODE, ";
                SQL = SQL + "        COSM_ACCOUNT_NAME,STATION_CODE,STATION_NAME, ";
                SQL = SQL + "        DEPT_CODE,DEPT_DESC,PRODUCT_CODE, ";
                SQL = SQL + "        PRODUCT_DESC,CNCR_BATCHED_QTY,REMARKS, ";
                SQL = SQL + "        MATAMOUNT,MASTER_BRANCH_CODE,CREATEDBY, ";
                SQL = SQL + "        VERIFIED,VERIFIED_BY,VERIFIED_DATE, ";
                SQL = SQL + "        APPROVED,APPROVED_BY,APPROVED_DATE, ";
                SQL = SQL + "        RECEIVED_EMPCODE,RECEIVED_EMPNAME, ";
                SQL = SQL + "        ISSUETYPE_RM_CNCR,SLNO, ";
                SQL = SQL + "        CATEGORY_CODE,CATEGORY_DESC, ";
                SQL = SQL + "        SUBCATEGORY_CODE,SUBCATEGORY_DESC, ";
                SQL = SQL + "        RM_TYPE,RMCODE, ";
                SQL = SQL + "        RMDESC,UOM_CODE, ";
                SQL = SQL + "        UOM_DESC,AVG_COST, ";

                SQL = SQL + "  MATERIAL_ISSUE_QTY,     MATERIAL_ISSUE_AMNT,     MATERIAL_REQUESTED_QTY,     MATERIAL_REQUESTEDBAL_QTY  , ";
                SQL = SQL + "         TO_BASIC_UOM , TO_UOM_DESC, CONVERSION_FACTOR  ,QUANTITY_CONVRTD  ";

                SQL = SQL + "   FROM RM_STORE_ISSUE_RPT_VW  ";


                SQL = SQL + " where   ENTRY_DATE   BETWEEN '" + System.Convert.ToDateTime(ReceptFromDateTime).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(ReceptToDateTime).ToString("dd-MMM-yyyy") + "'";





                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND MASTER_BRANCH_CODE IN ( '" + Branch + "') ";

                }

                if (!string.IsNullOrEmpty(SlectedStation))
                {
                    SQL = SQL + " AND  STATION_CODE  IN ( '" + SlectedStation + "') ";

                }



                if (!string.IsNullOrEmpty(SelectedRM))
                {
                    SQL = SQL + " AND  RMCODE  IN ( '" + SelectedRM + "') ";

                }

                if (ApprovedStatus != "ALL")
                {
                    SQL = SQL + " AND  APPROVED  ='" + ApprovedStatus + "'";

                }


                if (!string.IsNullOrEmpty(SelectedProject))
                {
                    SQL = SQL + " AND  ENQUIRY_NO  IN ( '" + SelectedProject + "') ";

                }
                SQL = SQL + "  order by  ENQUIRY_NO ,  ENTRY_NO,ENTRY_DATE ,SLNO  asc   ";




                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }


        public DataTable FillMaterialIssueToSiteRMSummaryXLList()
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();

                SQL = "SELECT  ";
                SQL = SQL + "    ";
                SQL = SQL + "    RM_TYPE,RMCODE, RMDESC,UOM_CODE,UOM_DESC  , ";
                SQL = SQL + "   sum(MATERIAL_ISSUE_QTY  ) MATERIAL_ISSUE_QTY  ,     SUM (QUANTITY_CONVRTD)     QUANTITY_CONVRTD     ";
                SQL = SQL + "    FROM RM_STORE_ISSUE_RPT_VW   ";


                SQL = SQL + " where   ENTRY_DATE   BETWEEN '" + System.Convert.ToDateTime(ReceptFromDateTime).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(ReceptToDateTime).ToString("dd-MMM-yyyy") + "'";

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND MASTER_BRANCH_CODE IN ( '" + Branch + "') ";

                }

                if (!string.IsNullOrEmpty(SlectedStation))
                {
                    SQL = SQL + " AND  STATION_CODE  IN ( '" + SlectedStation + "') ";

                }



                if (!string.IsNullOrEmpty(SelectedRM))
                {
                    SQL = SQL + " AND  RMCODE  IN ( '" + SelectedRM + "') ";

                }

                if (ApprovedStatus != "ALL")
                {
                    SQL = SQL + " AND  APPROVED  ='" + ApprovedStatus + "'";

                }


                if (!string.IsNullOrEmpty(SelectedProject))
                {
                    SQL = SQL + " AND  ENQUIRY_NO  IN ( '" + SelectedProject + "') ";

                }


                SQL = SQL + "group by  ";
                SQL = SQL + "    RM_TYPE,RMCODE, RMDESC,UOM_CODE,UOM_DESC   ";

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }


        public DataTable FillMaterialIssueToSiteSummaryPrint()
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();


                SQL = "SELECT  ";
                SQL = SQL + "    ENQUIRY_NO,COSM_ACCOUNT_CODE, COSM_ACCOUNT_NAME, MASTER_BRANCH_CODE,  ";
                SQL = SQL + "    RM_TYPE,RMCODE, RMDESC,UOM_CODE,UOM_DESC  , ";
                SQL = SQL + "   sum(MATERIAL_ISSUE_QTY  ) MATERIAL_ISSUE_QTY  ,     SUM (QUANTITY_CONVRTD)     QUANTITY_CONVRTD,  sum(MATERIAL_REQUESTED_QTY  ) MATERIAL_REQUESTED_QTY ,  sum(MATERIAL_REQUESTEDBAL_QTY  ) MATERIAL_REQUESTEDBAL_QTY   ";
                SQL = SQL + "    FROM RM_STORE_ISSUE_RPT_VW   ";




                SQL = SQL + " where   ENTRY_DATE   BETWEEN '" + System.Convert.ToDateTime(ReceptFromDateTime).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(ReceptToDateTime).ToString("dd-MMM-yyyy") + "'";





                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND MASTER_BRANCH_CODE IN ( '" + Branch + "') ";

                }

                if (!string.IsNullOrEmpty(SlectedStation))
                {
                    SQL = SQL + " AND  STATION_CODE  IN ( '" + SlectedStation + "') ";

                }



                if (!string.IsNullOrEmpty(SelectedRM))
                {
                    SQL = SQL + " AND  RMCODE  IN ( '" + SelectedRM + "') ";

                }

                if (ApprovedStatus != "ALL")
                {
                    SQL = SQL + " AND  APPROVED  ='" + ApprovedStatus + "'";

                }


                if (!string.IsNullOrEmpty(SelectedProject))
                {
                    SQL = SQL + " AND  ENQUIRY_NO  IN ( '" + SelectedProject + "') ";

                }
                SQL = SQL + "group by  ";
                SQL = SQL + "    ENQUIRY_NO,COSM_ACCOUNT_CODE,  COSM_ACCOUNT_NAME, MASTER_BRANCH_CODE,  ";
                SQL = SQL + "    RM_TYPE,RMCODE, RMDESC,UOM_CODE,UOM_DESC   ";


                SQL = SQL + "  order by  ENQUIRY_NO ,   RMCODE   asc   ";




                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }



        #endregion

        ///////////------------- end of receipt amount wise 

        ///  begin receipt qyt wise 
        ///  

        #region "QTy Wise 

        public DataTable FectchReceiptDetailsQTYWise(string sSlectedStation, string sSlectedBranch, string sSlectedVendor, string sSelectedTrans, string sSelectedRM, string sApprovedStatus, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();



                ////SQL = " SELECT  ";
                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO RECEIPT_NO ,";

                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE RECEIPT_DATE,";
                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  VENDOR_CODE,";
                ////SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME    VENDOR_NAME,";

                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO,";

                ////SQL = SQL + " CASE ";
                ////SQL = SQL + " WHEN RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='ONSITE' AND RM_MRM_STOCK_YN='Y' THEN 'ONSITESTOCK' ";
                ////SQL = SQL + " WHEN RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='ONSITE' AND RM_MRM_STOCK_YN='N' THEN 'ONSITE'";
                ////SQL = SQL + " WHEN RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='EX-FACTORY' AND RM_MRM_STOCK_YN='Y' THEN 'EX-FACTORYSTOCK' ";
                ////SQL = SQL + " WHEN RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE ='EX-FACTORY' AND RM_MRM_STOCK_YN='N' THEN 'EX-FACTORY'";
                ////SQL = SQL + " END PRICE_TYPE,";
                ////SQL = SQL + " RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                ////SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";
                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                ////SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";
                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_UM_UOM_CODE UOM_CODE ,";
                ////SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC,";
                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,";
                ////SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_DESC ,";
                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE TRANSPORTER_CODE, ";
                ////SQL = SQL + " TRANSVENDOR.RM_VM_VENDOR_NAME TRNSPORTERNAME, ";
                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO TRANSPO_ORDER_NO, ";

                ////SQL = SQL + " RM_MRM_TRNSPORTER_DOC_NO  TRNSPORTER_DOC_NO ,";

                ////SQL = SQL + " RM_RECEPIT_DETAILS.FA_FAM_ASSET_CODE ASSET_CODE ,FA_FAM_ASSET_DESCRIPTION ASSET_DESCRIPTION, ";
                ////SQL = SQL + " RM_MRM_VEHICLE_DESCRIPTION VEHICLE_DESCRIPTION, ";
                ////SQL = SQL + " RM_MRD_DRIVER_CODE DRIVER_CODE ,RM_MRD_DRIVER_NAME DRIVER_NAME, ";
                ////SQL = SQL + " RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO,";
                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY SUPPLY_QTY,";
                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY WEIGH_QTY,  RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY REJECTED_QTY  ,";
                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY APPROVE_QTY  , ";
                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_WEIGH_BRIDGE_DOC_NO  WEIGH_BRIDGE_DOC_NO ,   ";
                ////SQL = SQL + " TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE ,  TECH_PLANT_MASTER.TECH_PLM_PLANT_NAME  , ";
                ////SQL = SQL + "  TOLL_VENDOR_NAME  ,  NVL(TOLL_AMOUNT,0)   TOLL_AMOUNT , ";
                ////SQL = SQL + "     RM_RECEPIT_DETAILS.AD_BR_CODE , ";
                ////SQL = SQL + "     MASTER_BRANCH.AD_BR_CODE MASTER_BRANCH_CODE "; 

                ////SQL = SQL + " FROM RM_RECEPIT_DETAILS, RM_RAWMATERIAL_MASTER,";
                ////SQL = SQL + " RM_UOM_MASTER, RM_VENDOR_MASTER,";
                ////SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER,ad_branch_master MASTER_BRANCH ,";
                ////SQL = SQL + " RM_VENDOR_MASTER TRANSVENDOR , TECH_PLANT_MASTER ,FA_FIXED_ASSET_MASTER ,  ";

                ////SQL = SQL + "   (SELECT RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO, ";
                ////SQL = SQL + "  LISTAGG(RM_VM_VENDOR_NAME, ',') WITHIN GROUP(ORDER BY RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO) TOLL_VENDOR_NAME, ";
                ////SQL = SQL + "  LISTAGG(RM_MRM_TOLL_AMOUNT, ', ') WITHIN GROUP(ORDER BY RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO) TOLL_AMOUNT ";
                ////SQL = SQL + "  FROM RM_RECEPIT_TOLL, RM_VENDOR_MASTER";
                ////SQL = SQL + "  WHERE  RM_RECEPIT_TOLL.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                ////SQL = SQL + "  GROUP BY ";
                ////SQL = SQL + "  RM_RECEPIT_TOLL.RM_MRM_RECEIPT_NO  )TOLLQRY ";


                ////SQL = SQL + " WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                ////SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE    ";
                ////SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE    ";

                ////SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                ////SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                ////SQL = SQL + " AND     RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =  TRANSVENDOR.RM_VM_VENDOR_CODE (+)";
                ////SQL = SQL + " AND     RM_RECEPIT_DETAILS. TECH_PLM_PLANT_CODE = TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE   (+) ";
                ////SQL = SQL + " AND     RM_RECEPIT_DETAILS. FA_FAM_ASSET_CODE = FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE   (+) ";
                ////SQL = SQL + " AND     RM_RECEPIT_DETAILS. RM_MRM_RECEIPT_NO  = TOLLQRY.RM_MRM_RECEIPT_NO    (+) ";
                ////SQL = SQL + " AND     RM_RECEPIT_DETAILS. AD_BR_CODE  =  MASTER_BRANCH.AD_BR_CODE    (+) ";


                string SQL
                = " SELECT  " +
                "    RECEIPT_NO,RECEIPT_DATE,RECEIPT_DATE_TIME, " +
                "    VENDOR_CODE,VENDOR_NAME,ORDER_NO, " +
                "    PRICE_TYPE,STATION_CODE,STATION_NAME, " +
                "    RM_CODE,RM_DESCRIPTION,UOM_CODE, " +
                "    UOM_DESC,SOURCE_CODE,SOURCE_DESC, " +
                "    TRANSPORTER_CODE,TRNSPORTERNAME,TRANSPO_ORDER_NO, " +
                "    TRNSPORTER_DOC_NO,ASSET_CODE,ASSET_DESCRIPTION, " +
                "    VEHICLE_DESCRIPTION,DRIVER_CODE,DRIVER_NAME, " +
                "    VENDOR_DOC_NO,SUPPLY_QTY,WEIGH_QTY,REJECTED_QTY,APPROVE_QTY, " +
                "    WEIGH_BRIDGE_DOC_NO,TECH_PLM_PLANT_CODE,TECH_PLM_PLANT_NAME, " +
                "    TOLL_VENDOR_NAME,TOLL_AMOUNT,AD_BR_CODE,MASTER_BRANCH_CODE " +
                " FROM RM_RECEIPT_QTY_RPT_VW  ";

                SQL = SQL + " WHERE  RM_VM_DELETESTATUS = 0 ";

                if (!string.IsNullOrEmpty(sSlectedBranch))
                {
                    SQL = SQL + " AND  AD_BR_CODE IN ('" + sSlectedBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND STATION_CODE IN ('" + sSlectedStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND VENDOR_CODE IN ('" + sSlectedVendor + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedTrans))
                {
                    SQL = SQL + " AND TRANSPORTER_CODE IN ('" + sSelectedTrans + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_CODE IN ('" + sSelectedRM + "') ";

                }

                if (sApprovedStatus != "ALL")
                {
                    SQL = SQL + " AND  RM_MRD_APPROVED ='" + sApprovedStatus + "'";

                }

                if (Type == "Varience")
                {
                    SQL = SQL + " AND (SUPPLY_QTY-WEIGH_QTY) > 0.47 ";
                }

                if (!string.IsNullOrEmpty(SelecedPlant)) // jomy added 31 oct 2014
                {

                    SQL = SQL + "    AND     TECH_PLM_PLANT_CODE IN ('" + SelecedPlant + "')";
                }

                if (ReceiptType != "ALL")
                {

                    SQL = SQL + "    AND     PRICE_TYPE ='" + ReceiptType + "'";
                }


                if (DateTimeFilterType == "Date")
                {

                    SQL = SQL + " AND TO_DATE(TO_CHAR(RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                }
                else if (DateTimeFilterType == "Time")
                {
                    SQL = SQL + "    AND   RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                }


                SQL = SQL + " order by RECEIPT_NO";

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }


        public DataTable FectchRawMaterialReceiptsDetailedReport(string sSlectedStation, string sSlectedBranch, string sSlectedVendor, string sSelectedTrans, string sSelectedRM, string sApprovedStatus, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = string.Empty;

                SQL = " SELECT  RECEIPT_NO, ";
                SQL = SQL + "    RECEIPT_DATE, ";
                SQL = SQL + "    RECEIPT_DATE_TIME, ";
                SQL = SQL + "    VENDOR_CODE,VENDOR_NAME,SOURCE_CODE,SOURCE_DESC, ";
                SQL = SQL + "    ORDER_NO, ";
                SQL = SQL + "    PRICE_TYPE,VENDOR_DOC_NO, ";
                SQL = SQL + "    STATION_CODE,STATION_NAME, ";
                SQL = SQL + "    RM_CODE,RM_DESCRIPTION,  ";
                SQL = SQL + "    SUPPLY_QTY,WEIGH_QTY,REJECTED_QTY,APPROVE_QTY, ";
                SQL = SQL + "    WEIGH_BRIDGE_DOC_NO, ";
                SQL = SQL + "    TECH_PLM_PLANT_CODE, ";
                SQL = SQL + "    TECH_PLM_PLANT_NAME, ";
                SQL = SQL + "    RM_RECEIPT_QTY_RPT_VW.AD_BR_CODE, ";
                SQL = SQL + "    RM_VM_DELETESTATUS, ";
                SQL = SQL + "    RM_MRD_APPROVED,CREATED_BY,VERIFIED_BY, ";
                SQL = SQL + "    APPROVED_BY,REMARKS ,AD_BR_NAME BRANCH,TRANSPORTER_CODE,  ";
                SQL = SQL + " TRNSPORTERNAME,";
                SQL = SQL + " TRANSPO_ORDER_NO,";
                SQL = SQL + " TRNSPORTER_DOC_NO";
                SQL = SQL + "    FROM RM_RECEIPT_QTY_RPT_VW,AD_BRANCH_MASTER  ";

                SQL = SQL + "  WHERE RM_RECEIPT_QTY_RPT_VW.AD_BR_CODE=AD_BRANCH_MASTER.AD_BR_CODE AND  RM_VM_DELETESTATUS = 0";
                if (!string.IsNullOrEmpty(sSlectedBranch))
                {
                    SQL = SQL + " AND  RM_RECEIPT_QTY_RPT_VW.AD_BR_CODE IN ('" + sSlectedBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND STATION_CODE IN ('" + sSlectedStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND VENDOR_CODE IN ('" + sSlectedVendor + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedTrans))
                {
                    SQL = SQL + " AND TRANSPORTER_CODE IN ('" + sSelectedTrans + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_CODE IN ('" + sSelectedRM + "') ";

                }

                if (sApprovedStatus != "ALL")
                {
                    SQL = SQL + " AND  RM_MRD_APPROVED ='" + sApprovedStatus + "'";

                }

                if (Type == "Varience")
                {
                    SQL = SQL + " AND (SUPPLY_QTY-WEIGH_QTY) > 0.47 ";
                }

                if (!string.IsNullOrEmpty(SelecedPlant)) // jomy added 31 oct 2014
                {

                    SQL = SQL + "    AND     TECH_PLM_PLANT_CODE IN ('" + SelecedPlant + "')";
                }

                if (ReceiptType != "ALL")
                {

                    SQL = SQL + "    AND     PRICE_TYPE ='" + ReceiptType + "'";
                }


                if (DateTimeFilterType == "Date")
                {

                    SQL = SQL + " AND TO_DATE(TO_CHAR(RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                }
                else if (DateTimeFilterType == "Time")
                {
                    SQL = SQL + "    AND   RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                }


                SQL = SQL + " order by RECEIPT_NO";

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }



            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }


        public DataTable FectchReceiptSummaryRMWISEQTYWise(string sSlectedStation, string sSlectedBranch, string sSlectedVendor, string sSelectedTrans, string sSelectedRM, string sApprovedStatus, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();




                ////SQL = " SELECT  ";
                ////SQL = SQL + "  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                ////SQL = SQL + "  RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";


                ////SQL = SQL + "  sum( RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY ) SUPPLY_QTY,";
                ////SQL = SQL + "   sum(RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY  ) WEIGH_QTY, ";
                ////SQL = SQL + "  sum( RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY  ) REJECTED_QTY  ,";
                ////SQL = SQL + "   sum(RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY  ) APPROVE_QTY  ";


                ////SQL = SQL + "  FROM RM_RECEPIT_DETAILS,";
                ////SQL = SQL + "  RM_RAWMATERIAL_MASTER ";


                ////SQL = SQL + "  WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                ////// ' SQL = SQL +  "  AND RM_MRD_APPROVED = 'N'"
                ////SQL = SQL + "  AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))";
                ////SQL = SQL + "    BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                ////if (!string.IsNullOrEmpty(sSlectedBranch))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + sSlectedBranch + "') ";

                ////}

                ////if (!string.IsNullOrEmpty(sSlectedStation))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE IN ( '" + sSlectedStation + "') ";

                ////}

                ////if (!string.IsNullOrEmpty(sSlectedVendor))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                ////}

                ////if (!string.IsNullOrEmpty(sSelectedTrans))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                ////}

                ////if (!string.IsNullOrEmpty(sSelectedRM))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                ////}

                //////if ( sApprovedStatus =="N")
                //////   {
                //////   SQL = SQL + " AND RM_RECEPIT_DETAILS RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='N'";

                //////   } 

                ////SQL = SQL + "  GROUP BY RM_RECEPIT_DETAILS.RM_RMM_RM_CODE  ,";
                ////SQL = SQL + "  RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION  ";




                SQL = " SELECT   " +
                "    RM_CODE, " +
                "    RM_DESCRIPTION, " +
                "    sum(  SUPPLY_QTY ) SUPPLY_QTY, " +
                "    sum( WEIGH_QTY  ) WEIGH_QTY,  " +
                "    sum(  REJECTED_QTY  ) REJECTED_QTY  , " +
                "    sum( APPROVE_QTY  ) APPROVE_QTY   " +
                "    from RM_RECEIPT_QTY_RPT_VW  ";

                SQL = SQL + " WHERE  RM_VM_DELETESTATUS = 0 ";

                if (!string.IsNullOrEmpty(sSlectedBranch))
                {
                    SQL = SQL + " AND  AD_BR_CODE IN ('" + sSlectedBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND STATION_CODE IN ('" + sSlectedStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND VENDOR_CODE IN ('" + sSlectedVendor + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedTrans))
                {
                    SQL = SQL + " AND TRANSPORTER_CODE IN ('" + sSelectedTrans + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_CODE IN ('" + sSelectedRM + "') ";

                }

                if (sApprovedStatus != "ALL")
                {
                    SQL = SQL + " AND  RM_MRD_APPROVED ='" + sApprovedStatus + "'";

                }

                if (Type == "Varience")
                {
                    SQL = SQL + " AND (RM_MRD_SUPPLY_QTY-RM_MRD_WEIGH_QTY) > 0.47 ";
                }
                if (!string.IsNullOrEmpty(SelecedPlant)) // jomy added 31 oct 2014
                {

                    SQL = SQL + "    AND     TECH_PLM_PLANT_CODE IN ('" + SelecedPlant + "')";
                }

                if (ReceiptType != "ALL")
                {

                    SQL = SQL + "    AND     PRICE_TYPE ='" + ReceiptType + "'";
                }


                if (DateTimeFilterType == "Date")
                {

                    SQL = SQL + " AND TO_DATE(TO_CHAR(RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                }
                else if (DateTimeFilterType == "Time")
                {
                    SQL = SQL + "    AND  RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                }



                SQL = SQL + "    group by     RM_CODE,   RM_DESCRIPTION  ";




                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable FectchReceiptSummaryVENDORWISEQTYWise(string sSlectedStation, string sSlectedBranch, string sSlectedVendor, string sSelectedTrans, string sSelectedRM, string sApprovedStatus, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();

                ////SQL = " SELECT  ";
                ////SQL = SQL + "  RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  VENDOR_CODE,";
                ////SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_NAME    VENDOR_NAME,";

                ////SQL = SQL + "  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                ////SQL = SQL + "  SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";
                ////SQL = SQL + "  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                ////SQL = SQL + "  RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";
                ////SQL = SQL + "  sum( RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY ) SUPPLY_QTY,";
                ////SQL = SQL + "   sum(RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY  ) WEIGH_QTY, ";
                ////SQL = SQL + "  sum( RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY  ) REJECTED_QTY  ,";
                ////SQL = SQL + "   sum(RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY  ) APPROVE_QTY  ";



                ////SQL = SQL + "  FROM RM_RECEPIT_DETAILS,";
                ////SQL = SQL + "  RM_RAWMATERIAL_MASTER,";

                ////SQL = SQL + "  RM_VENDOR_MASTER,";
                ////SQL = SQL + "  SL_STATION_MASTER ";
                ////SQL = SQL + "  WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                ////SQL = SQL + "  AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE    ";
                ////SQL = SQL + "  AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";


                ////// ' SQL = SQL +  "  AND RM_MRD_APPROVED = 'N'"
                ////SQL = SQL + "  AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))";
                ////SQL = SQL + "    BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                ////if (!string.IsNullOrEmpty(sSlectedBranch))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + sSlectedBranch + "') ";

                ////}


                ////if (!string.IsNullOrEmpty(sSlectedStation))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE IN ( '" + sSlectedStation + "') ";

                ////}

                ////if (!string.IsNullOrEmpty(sSlectedVendor))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                ////}

                ////if (!string.IsNullOrEmpty(sSelectedTrans))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                ////}

                ////if (!string.IsNullOrEmpty(sSelectedRM))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                ////}

                //////if ( sApprovedStatus =="N")
                //////   {
                //////   SQL = SQL + " AND RM_RECEPIT_DETAILS RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='N'";

                //////   }  

                ////SQL = SQL + " GROUP BY RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE   ,";
                ////SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_NAME     ,";

                ////SQL = SQL + "  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE  ,";
                ////SQL = SQL + "  SL_STATION_MASTER.SALES_STN_STATION_NAME  ,";
                ////SQL = SQL + "   RM_RECEPIT_DETAILS.RM_RMM_RM_CODE  ,";
                ////SQL = SQL + "  RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION ";




                SQL = " SELECT  VENDOR_CODE,  VENDOR_NAME ,STATION_CODE  ,STATION_NAME  " +
                "    RM_CODE, " +
                "    RM_DESCRIPTION, " +
                "    sum(  SUPPLY_QTY ) SUPPLY_QTY, " +
                "    sum( WEIGH_QTY  ) WEIGH_QTY,  " +
                "    sum(  REJECTED_QTY  ) REJECTED_QTY  , " +
                "    sum( APPROVE_QTY  ) APPROVE_QTY   " +
                "    from RM_RECEIPT_QTY_RPT_VW  ";

                SQL = SQL + " WHERE  RM_VM_DELETESTATUS = 0 ";

                if (!string.IsNullOrEmpty(sSlectedBranch))
                {
                    SQL = SQL + " AND  AD_BR_CODE IN ('" + sSlectedBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND STATION_CODE IN ('" + sSlectedStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND VENDOR_CODE IN ('" + sSlectedVendor + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedTrans))
                {
                    SQL = SQL + " AND TRANSPORTER_CODE IN ('" + sSelectedTrans + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_CODE IN ('" + sSelectedRM + "') ";

                }

                if (sApprovedStatus != "ALL")
                {
                    SQL = SQL + " AND  RM_MRD_APPROVED ='" + sApprovedStatus + "'";

                }

                if (Type == "Varience")
                {
                    SQL = SQL + " AND (RM_MRD_SUPPLY_QTY-RM_MRD_WEIGH_QTY) > 0.47 ";
                }
                if (!string.IsNullOrEmpty(SelecedPlant)) // jomy added 31 oct 2014
                {

                    SQL = SQL + "    AND     TECH_PLM_PLANT_CODE IN ('" + SelecedPlant + "')";
                }

                if (ReceiptType != "ALL")
                {

                    SQL = SQL + "    AND     PRICE_TYPE ='" + ReceiptType + "'";
                }


                if (DateTimeFilterType == "Date")
                {

                    SQL = SQL + " AND TO_DATE(TO_CHAR(RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                }
                else if (DateTimeFilterType == "Time")
                {
                    SQL = SQL + "    AND  RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                }



                SQL = SQL + "    group by   VENDOR_CODE,  VENDOR_NAME ,STATION_CODE  ,STATION_NAME,  RM_CODE,   RM_DESCRIPTION  ";



                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable FectchReceiptSummarySTATIONWISEQTYWise(string sSlectedStation, string sSlectedBranch, string sSlectedVendor, string sSelectedTrans, string sSelectedRM, string sApprovedStatus, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();

                ////SQL = " SELECT  ";

                ////SQL = SQL + "  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                ////SQL = SQL + "  SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";
                ////SQL = SQL + "  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                ////SQL = SQL + "  RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";
                ////SQL = SQL + "  sum( RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY ) SUPPLY_QTY,";
                ////SQL = SQL + "   sum(RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY  ) WEIGH_QTY, ";
                ////SQL = SQL + "  sum( RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY  ) REJECTED_QTY  ,";
                ////SQL = SQL + "   sum(RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY  ) APPROVE_QTY ";

                ////SQL = SQL + "  FROM RM_RECEPIT_DETAILS,";
                ////SQL = SQL + "  RM_RAWMATERIAL_MASTER,";


                ////SQL = SQL + "  SL_STATION_MASTER ";
                ////SQL = SQL + "  WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";

                ////SQL = SQL + "  AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";



                ////// ' SQL = SQL +  "  AND RM_MRD_APPROVED = 'N'"
                ////SQL = SQL + "   AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))";
                ////SQL = SQL + "    BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                ////if (!string.IsNullOrEmpty(sSlectedBranch))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + sSlectedBranch + "') ";

                ////}

                ////if (!string.IsNullOrEmpty(sSlectedStation))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE IN ( '" + sSlectedStation + "') ";

                ////}

                ////if (!string.IsNullOrEmpty(sSlectedVendor))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                ////}

                ////if (!string.IsNullOrEmpty(sSelectedTrans))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                ////}

                ////if (!string.IsNullOrEmpty(sSelectedRM))
                ////{
                ////    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                ////}

                //////if ( sApprovedStatus =="N")
                //////   {
                //////   SQL = SQL + " AND RM_RECEPIT_DETAILS RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='N'";

                //////   }  

                ////SQL = SQL + " GROUP BY     ";

                ////SQL = SQL + " RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE  ,";
                ////SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME  ,";
                ////SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE  ,";
                ////SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION ";






                SQL = " SELECT  STATION_CODE  ,STATION_NAME  " +
                "    RM_CODE, " +
                "    RM_DESCRIPTION, " +
                "    sum(  SUPPLY_QTY ) SUPPLY_QTY, " +
                "    sum( WEIGH_QTY  ) WEIGH_QTY,  " +
                "    sum(  REJECTED_QTY  ) REJECTED_QTY  , " +
                "    sum( APPROVE_QTY  ) APPROVE_QTY   " +
                "    from RM_RECEIPT_QTY_RPT_VW  ";

                SQL = SQL + " WHERE  RM_VM_DELETESTATUS = 0 ";

                if (!string.IsNullOrEmpty(sSlectedBranch))
                {
                    SQL = SQL + " AND  AD_BR_CODE IN ('" + sSlectedBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND STATION_CODE IN ('" + sSlectedStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND VENDOR_CODE IN ('" + sSlectedVendor + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedTrans))
                {
                    SQL = SQL + " AND TRANSPORTER_CODE IN ('" + sSelectedTrans + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_CODE IN ('" + sSelectedRM + "') ";

                }

                if (sApprovedStatus != "ALL")
                {
                    SQL = SQL + " AND  RM_MRD_APPROVED ='" + sApprovedStatus + "'";

                }

                if (Type == "Varience")
                {
                    SQL = SQL + " AND (RM_MRD_SUPPLY_QTY-RM_MRD_WEIGH_QTY) > 0.47 ";
                }
                if (!string.IsNullOrEmpty(SelecedPlant)) // jomy added 31 oct 2014
                {

                    SQL = SQL + "    AND     TECH_PLM_PLANT_CODE IN ('" + SelecedPlant + "')";
                }

                if (ReceiptType != "ALL")
                {

                    SQL = SQL + "    AND     PRICE_TYPE ='" + ReceiptType + "'";
                }


                if (DateTimeFilterType == "Date")
                {

                    SQL = SQL + " AND TO_DATE(TO_CHAR(RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                }
                else if (DateTimeFilterType == "Time")
                {
                    SQL = SQL + "    AND  RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(ReceptFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(ReceptToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                }



                SQL = SQL + "    group by   STATION_CODE  ,STATION_NAME,  RM_CODE,   RM_DESCRIPTION  ";




                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataSet FectchVechicleWiseReceiptDetails(string sSlectedStation, string sSlectedBranch, string sSlectedVendor, string sSelectedTrans, string sSelectedRM, string sApprovedStatus, DateTime dDnFromDate, DateTime dToDate)
        {
            DataSet dsData = new DataSet("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();



                SQL = " SELECT  ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO RECEIPT_NO ,";

                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE RECEIPT_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  VENDOR_CODE,";
                SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_NAME    VENDOR_NAME,";

                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO,";

                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE PRICE_TYPE,";

                SQL = SQL + " RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_UM_UOM_CODE UOM_CODE ,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,";
                SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_DESC ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE TRANSPORTER_CODE, ";
                SQL = SQL + " TRANSVENDOR.RM_VM_VENDOR_NAME TRNSPORTERNAME, ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO TRANSPO_ORDER_NO, ";

                SQL = SQL + " RM_MRM_TRNSPORTER_DOC_NO  TRNSPORTER_DOC_NO ,";

                SQL = SQL + " RM_RECEPIT_DETAILS.FA_FAM_ASSET_CODE ASSET_CODE ,FA_FAM_ASSET_DESCRIPTION VEHICLE_DESCRIPTION, ";
                SQL = SQL + " RM_MRD_DRIVER_CODE DRIVER_CODE ,RM_MRD_DRIVER_NAME DRIVER_NAME, ";
                SQL = SQL + " RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY SUPPLY_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY WEIGH_QTY,  RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY REJECTED_QTY  ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY APPROVE_QTY  , ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_WEIGH_BRIDGE_DOC_NO  WEIGH_BRIDGE_DOC_NO ,   ";
                SQL = SQL + " TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE ,  TECH_PLANT_MASTER.TECH_PLM_PLANT_NAME  , ";
                SQL = SQL + "     RM_RECEPIT_DETAILS.AD_BR_CODE , ";
                SQL = SQL + "     MASTER_BRANCH.AD_BR_CODE MASTER_BRANCH_CODE ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_NAME MASTER_BRANCH_NAME, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_DOC_PREFIX MASTER_BRANCHDOC_PREFIX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_POBOX MASTER_BRANCH_POBOX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_ADDRESS MASTER_BRANCH_ADDRESS, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_CITY MASTER_BRANCH_CITY, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_PHONE MASTER_BRANCH_PHONE, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_FAX MASTER_BRANCH_FAX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_SPONSER_NAME MASTER_BRANCH_SPONSER_NAME, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TRADE_LICENSE_NO MASTER_BRANCH_TRADE_LICENSE_NO, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_EMAIL_ID MASTER_BRANCH_EMAIL_ID, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_WEB_SITE MASTER_BRANCH_WEB_SITE, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TAX_VAT_REG_NUMBER MASTER_BRANCH_VAT_REG_NUMBER ";

                SQL = SQL + " FROM RM_RECEPIT_DETAILS, RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER, RM_VENDOR_MASTER,";
                SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER, FA_FIXED_ASSET_MASTER,";
                SQL = SQL + " RM_VENDOR_MASTER TRANSVENDOR , TECH_PLANT_MASTER ,";
                SQL = SQL + " ad_branch_master MASTER_BRANCH ";

                SQL = SQL + " WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE    ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE    ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE = MASTER_BRANCH.AD_BR_CODE(+) ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.FA_FAM_ASSET_CODE = FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =  TRANSVENDOR.RM_VM_VENDOR_CODE (+)";
                SQL = SQL + " AND  RM_RECEPIT_DETAILS. TECH_PLM_PLANT_CODE = TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE   (+) ";

                if (!string.IsNullOrEmpty(SelectedVehicle))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.FA_FAM_ASSET_CODE IN ('" + SelectedVehicle + "') ";
                }
                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE IN ( '" + sSlectedStation + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedBranch))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + sSlectedBranch + "') ";

                }

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedTrans))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";
                }

                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + " order by RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO";

                dsData = clsSQLHelper.GetDataset(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dsData;
        }

        #endregion

        //---------------- start PURCHASE ENTRY / PENDING ENTRY PURCHASE 

        #region "PURCHASE ENTRY " 

        public DataTable FectchPurchaseEntrySummary(string sSlectedVendor, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();


                SQL = " SELECT RM_PE_MASTER.RM_MPEM_ENTRY_NO ENTRY_NO, RM_PE_MASTER.RM_MPEM_ENTRY_DATE ENTRY_DATE,";
                SQL = SQL + "	 CASE RM_PE_MASTER.RM_MPEM_ENTRY_TYPE ";
                SQL = SQL + "	WHEN  'V' THEN 'VENDOR' ";
                SQL = SQL + "	WHEN 'T'  THEN 'TRANSPORTER' END PETYPE , ";
                SQL = SQL + "	CASE RM_PE_MASTER.RM_MPEM_TRANS_TYPE ";
                SQL = SQL + "	WHEN 'V' THEN 'RECEIPTS'";
                SQL = SQL + "	WHEN 'R' THEN 'RECEIPTS'";
                SQL = SQL + "	WHEN  'T' THEN 'STOCK-TRANSFER' END TRANSTYPE,";
                SQL = SQL + "	 RM_PE_MASTER.RM_VM_VENDOR_CODE VENDOR_CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME VENDOR_NAME,";
                SQL = SQL + "	 RM_PE_MASTER.RM_MPEM_VENDOR_INV_NO VENDOR_INV_NO,";
                SQL = SQL + "	 RM_PE_MASTER.RM_MPEM_VENDOR_INV_DATE VENDOR_INV_DATE, RM_PE_MASTER.RM_MPEM_GRAND_TOTAL GRAND_TOTAL,";
                SQL = SQL + "	 RM_PE_MASTER.RM_MPEM_DISC_AMT  DISC_AMT, RM_PE_MASTER.RM_MPEM_STK_ADJ_AMT STK_ADJ_AMT,";
                SQL = SQL + "	 RM_PE_MASTER.RM_MPEM_INVOICE_TOTAL INVOICE_TOTAL, RM_PE_MASTER.RM_MPEM_NET_TOTAL NET_TOTAL,";
                SQL = SQL + "	 RM_PE_MASTER.RM_MPEM_BALANCE_TOTAL BALANCE_TOTAL, RM_PE_MASTER.RM_MPEM_APPROVED APPROVED,";
                SQL = SQL + "	 RM_PE_MASTER.RM_MPEM_APPROVED_BY	 APPROVED_BY,";
                SQL = SQL + "     RM_PE_MASTER.AD_BR_CODE , ";
                SQL = SQL + "     MASTER_BRANCH.AD_BR_CODE MASTER_BRANCH_CODE ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_NAME MASTER_BRANCH_NAME, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_DOC_PREFIX MASTER_BRANCHDOC_PREFIX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_POBOX MASTER_BRANCH_POBOX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_ADDRESS MASTER_BRANCH_ADDRESS, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_CITY MASTER_BRANCH_CITY, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_PHONE MASTER_BRANCH_PHONE, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_FAX MASTER_BRANCH_FAX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_SPONSER_NAME MASTER_BRANCH_SPONSER_NAME, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TRADE_LICENSE_NO MASTER_BRANCH_TRADE_LICENSE_NO, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_EMAIL_ID MASTER_BRANCH_EMAIL_ID, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_WEB_SITE MASTER_BRANCH_WEB_SITE, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TAX_VAT_REG_NUMBER MASTER_BRANCH_VAT_REG_NUMBER ";

                SQL = SQL + "  FROM RM_PE_MASTER, RM_VENDOR_MASTER, ad_branch_master MASTER_BRANCH  ";
                SQL = SQL + "  WHERE RM_PE_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "  AND RM_PE_MASTER.AD_BR_CODE =  MASTER_BRANCH.AD_BR_CODE(+) ";



                SQL = SQL + "   AND TO_DATE(TO_CHAR( RM_PE_MASTER.RM_MPEM_ENTRY_DATE,'DD-MON-YYYY')) ";
                SQL = SQL + "   BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND RM_PE_MASTER.RM_VM_VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                }
                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND RM_PE_MASTER.AD_BR_CODE IN ( '" + Branch + "') ";

                }

                SQL = SQL + " ORDER BY  RM_PE_MASTER.RM_MPEM_ENTRY_NO";


                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable FetchPendingPurchaseEntryVendorWiseAson(string sSlectedVendor, string sSelectedRM, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();



                SQL = " SELECT  ";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO RECEIPT_NO ,";

                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE RECEIPT_DATE,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  VENDOR_CODE,";
                SQL = SQL + "  RM_VENDOR_MASTER.RM_VM_VENDOR_NAME    VENDOR_NAME,";

                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO,";

                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE PRICE_TYPE,";

                SQL = SQL + "RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + "SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_UM_UOM_CODE UOM_CODE ,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,";
                SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_DESC ,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE TRANSPORTER_CODE, ";
                SQL = SQL + "  TRANSVENDOR.RM_VM_VENDOR_NAME TRNSPORTERNAME, ";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO TRANSPO_ORDER_NO, ";

                SQL = SQL + "RM_MRM_TRNSPORTER_DOC_NO  TRNSPORTER_DOC_NO ,";


                SQL = SQL + " FA_FAM_ASSET_CODE ASSET_CODE ,RM_MRM_VEHICLE_DESCRIPTION VEHICLE_DESCRIPTION, ";
                SQL = SQL + "  RM_MRD_DRIVER_CODE DRIVER_CODE ,RM_MRD_DRIVER_NAME DRIVER_NAME, ";

                SQL = SQL + "RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY SUPPLY_QTY,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY WEIGH_QTY,  RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY REJECTED_QTY  ,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY APPROVE_QTY, ";
                //SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE SUPP_UNIT_PRICE,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT SUPP_AMOUNT,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE TRANS_RATE,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT TRANS_AMOUNT , ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT TOTAL_AMOUNT,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW SUPP_UNIT_PRICE_NEW,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW SUPP_AMOUNT_NEW,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW TRANS_RATE_NEW,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW TRANS_AMOUNT_NEW,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW TOTAL_AMOUNT_NEW,   ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_APPROVED  ,   ";
                SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRM_APPROVED_NO,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY SUPP_ENTRY    ,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO SUPP_PENO   ,   ";
                SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRD_TRANS_ENTRY TRANS_ENTRY   , ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO  TRANS_PENO  ";


                SQL = SQL + "FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + "RM_RAWMATERIAL_MASTER,";
                SQL = SQL + "RM_UOM_MASTER,";
                SQL = SQL + "RM_VENDOR_MASTER,";
                SQL = SQL + "SL_STATION_MASTER , ";
                SQL = SQL + "RM_SOURCE_MASTER, ";
                SQL = SQL + " RM_VENDOR_MASTER TRANSVENDOR,RM_RECEIPT_APPL_MASTER ";
                SQL = SQL + "WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE    ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE    ";

                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + "  AND     RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =  TRANSVENDOR.RM_VM_VENDOR_CODE (+)";
                SQL = SQL + "  AND     RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO =  RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO  ";
                SQL = SQL + "AND RM_MRD_APPROVED = 'Y' ";

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                }
                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                }
                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + Branch + "') ";

                }
                SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY ='N' and RM_MRMA_APPROVED_STATUS ='Y'";

                SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE,'DD-MON-YYYY')) <= '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + " UNION ALL SELECT  ";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO RECEIPT_NO ,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE RECEIPT_DATE,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  VENDOR_CODE,";
                SQL = SQL + "  RM_VENDOR_MASTER.RM_VM_VENDOR_NAME    VENDOR_NAME,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE PRICE_TYPE,";
                SQL = SQL + "RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + "SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_UM_UOM_CODE UOM_CODE ,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,";
                SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_DESC ,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE TRANSPORTER_CODE, ";
                SQL = SQL + "  TRANSVENDOR.RM_VM_VENDOR_NAME TRNSPORTERNAME, ";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO TRANSPO_ORDER_NO, ";
                SQL = SQL + "RM_MRM_TRNSPORTER_DOC_NO  TRNSPORTER_DOC_NO ,";
                SQL = SQL + " FA_FAM_ASSET_CODE ASSET_CODE ,RM_MRM_VEHICLE_DESCRIPTION VEHICLE_DESCRIPTION, ";
                SQL = SQL + "  RM_MRD_DRIVER_CODE DRIVER_CODE ,RM_MRD_DRIVER_NAME DRIVER_NAME, ";

                SQL = SQL + "RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY SUPPLY_QTY,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY WEIGH_QTY,  RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY REJECTED_QTY  ,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY APPROVE_QTY, ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW SUPP_UNIT_PRICE_NEW,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW SUPP_AMOUNT_NEW,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRM_APPROVED_NO,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY SUPP_ENTRY    ,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO SUPP_PENO   ,   ";
                SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRD_TRANS_ENTRY TRANS_ENTRY   , ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO  TRANS_PENO  ";


                SQL = SQL + "FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + "RM_RAWMATERIAL_MASTER,";
                SQL = SQL + "RM_UOM_MASTER,";
                SQL = SQL + "RM_VENDOR_MASTER,";
                SQL = SQL + "SL_STATION_MASTER , ";
                SQL = SQL + "RM_SOURCE_MASTER, ";
                SQL = SQL + " RM_VENDOR_MASTER TRANSVENDOR ,RM_RECEIPT_APPL_MASTER ";
                SQL = SQL + "WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE    ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE    ";

                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + "  AND     RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =  TRANSVENDOR.RM_VM_VENDOR_CODE (+)";
                SQL = SQL + "  AND     RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO =  RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO  ";
                SQL = SQL + "AND RM_MRD_APPROVED = 'Y'";



                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                }

                //if (!string.IsNullOrEmpty(sSelectedTrans))
                //{
                //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                //}

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                }

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + Branch + "') ";

                }


                SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY ='Y' and RM_MRMA_APPROVED_STATUS ='Y'";

                SQL = SQL + "         and ( RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO  , RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO,RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID )  ";
                SQL = SQL + "         IN(  ";
                SQL = SQL + "select RM_MRM_RECEIPT_NO,RM_MRMA_APPROVAL_NO,AD_FIN_FINYRID_APPROVAL from RM_PE_DETAILS,RM_PE_MASTER ";
                SQL = SQL + "where  RM_PE_DETAILS.RM_MPEM_ENTRY_NO=RM_PE_MASTER.RM_MPEM_ENTRY_NO and RM_PE_DETAILS.RM_MPEM_ENTRY_TYPE='V' ";
                SQL = SQL + "and RM_MPEM_ENTRY_DATE> '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "            )  ";

                SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE,'DD-MON-YYYY')) <= '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + "  union all  SELECT RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO            RECEIPT_NO, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE          RECEIPT_DATE, ";
                SQL = SQL + "        ''             VENDOR_CODE, ";
                SQL = SQL + "     ''               VENDOR_NAME, ";
                SQL = SQL + "      ''            ORDER_NO, ";
                SQL = SQL + "      ''           PRICE_TYPE, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE       STATION_CODE, ";
                SQL = SQL + "         SL_STATION_MASTER.SALES_STN_STATION_NAME        STATION_NAME, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_RMM_RM_CODE               RM_CODE, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION     RM_DESCRIPTION, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_UM_UOM_CODE               UOM_CODE, ";
                SQL = SQL + "         RM_UOM_MASTER.RM_UM_UOM_DESC                    UOM_DESC, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE            SOURCE_CODE, ";
                SQL = SQL + "         RM_SOURCE_MASTER.RM_SM_SOURCE_DESC              SOURCE_DESC, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE      TRANSPORTER_CODE, ";
                SQL = SQL + "         TRANSVENDOR.RM_VM_VENDOR_NAME                   TRNSPORTERNAME, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO         TRANSPO_ORDER_NO, ";
                SQL = SQL + "         RM_MRM_TRNSPORTER_DOC_NO                        TRNSPORTER_DOC_NO, ";
                SQL = SQL + "         FA_FAM_ASSET_CODE                               ASSET_CODE, ";
                SQL = SQL + "         RM_MRM_VEHICLE_DESCRIPTION                      VEHICLE_DESCRIPTION, ";
                SQL = SQL + "         RM_MRD_DRIVER_CODE                              DRIVER_CODE, ";
                SQL = SQL + "         RM_MRD_DRIVER_NAME                              DRIVER_NAME, ";
                SQL = SQL + "         RM_MRD_VENDOR_DOC_NO                            VENDOR_DOC_NO, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY            SUPPLY_QTY, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY             WEIGH_QTY, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY          REJECTED_QTY, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY           APPROVE_QTY, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW        SUPP_UNIT_PRICE_NEW, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW     SUPP_AMOUNT_NEW, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO           APPROVED_NO, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY            SUPP_ENTRY, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO             SUPP_PENO, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY           TRANS_ENTRY, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO            TRANS_PENO ";
                SQL = SQL + "    FROM RM_RECEPIT_DETAILS, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "         RM_UOM_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         RM_SOURCE_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER TRANSVENDOR, ";
                SQL = SQL + "       RM_RECEIPT_APPL_MASTER ";
                SQL = SQL + "   WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = ";
                SQL = SQL + "             RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = ";
                SQL = SQL + "             RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+) ";
                SQL = SQL + "         AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE = ";
                SQL = SQL + "             RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = ";
                SQL = SQL + "             SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE = ";
                SQL = SQL + "             TRANSVENDOR.RM_VM_VENDOR_CODE ";



                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + "    AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSlectedVendor + "') ";

                }


                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                }
                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + Branch + "') ";

                }
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY = 'N' and RM_MRMA_APPROVED_STATUS ='Y' ";
                SQL = SQL + "  AND RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO =    RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO ";
                SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE,'DD-MON-YYYY')) <= '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";





                SQL = SQL + "  union all  SELECT RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO            RECEIPT_NO, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE          RECEIPT_DATE, ";
                SQL = SQL + "        ''             VENDOR_CODE, ";
                SQL = SQL + "     ''               VENDOR_NAME, ";
                SQL = SQL + "      ''            ORDER_NO, ";
                SQL = SQL + "      ''           PRICE_TYPE, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE       STATION_CODE, ";
                SQL = SQL + "         SL_STATION_MASTER.SALES_STN_STATION_NAME        STATION_NAME, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_RMM_RM_CODE               RM_CODE, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION     RM_DESCRIPTION, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_UM_UOM_CODE               UOM_CODE, ";
                SQL = SQL + "         RM_UOM_MASTER.RM_UM_UOM_DESC                    UOM_DESC, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE            SOURCE_CODE, ";
                SQL = SQL + "         RM_SOURCE_MASTER.RM_SM_SOURCE_DESC              SOURCE_DESC, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE      TRANSPORTER_CODE, ";
                SQL = SQL + "         TRANSVENDOR.RM_VM_VENDOR_NAME                   TRNSPORTERNAME, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO         TRANSPO_ORDER_NO, ";
                SQL = SQL + "         RM_MRM_TRNSPORTER_DOC_NO                        TRNSPORTER_DOC_NO, ";
                SQL = SQL + "         FA_FAM_ASSET_CODE                               ASSET_CODE, ";
                SQL = SQL + "         RM_MRM_VEHICLE_DESCRIPTION                      VEHICLE_DESCRIPTION, ";
                SQL = SQL + "         RM_MRD_DRIVER_CODE                              DRIVER_CODE, ";
                SQL = SQL + "         RM_MRD_DRIVER_NAME                              DRIVER_NAME, ";
                SQL = SQL + "         RM_MRD_VENDOR_DOC_NO                            VENDOR_DOC_NO, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY            SUPPLY_QTY, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY             WEIGH_QTY, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY          REJECTED_QTY, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY           APPROVE_QTY, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW        SUPP_UNIT_PRICE_NEW, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW     SUPP_AMOUNT_NEW, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO           APPROVED_NO, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY            SUPP_ENTRY, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO             SUPP_PENO, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY           TRANS_ENTRY, ";
                SQL = SQL + "         RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO            TRANS_PENO ";
                SQL = SQL + "    FROM RM_RECEPIT_DETAILS, ";
                SQL = SQL + "         RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + "         RM_UOM_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         RM_SOURCE_MASTER, ";
                SQL = SQL + "         RM_VENDOR_MASTER TRANSVENDOR, ";
                SQL = SQL + "       RM_RECEIPT_APPL_MASTER ";
                SQL = SQL + "   WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = ";
                SQL = SQL + "             RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = ";
                SQL = SQL + "             RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+) ";
                SQL = SQL + "         AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE = ";
                SQL = SQL + "             RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = ";
                SQL = SQL + "             SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "         AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE = ";
                SQL = SQL + "             TRANSVENDOR.RM_VM_VENDOR_CODE ";



                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + "    AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSlectedVendor + "') ";

                }


                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                }
                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + Branch + "') ";

                }
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY = 'Y' and RM_MRMA_APPROVED_STATUS ='Y' ";
                SQL = SQL + "  AND RM_RECEPIT_DETAILS.RM_MRM_APPROVED_NO =    RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO ";
                SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVALTRANS_DATE,'DD-MON-YYYY')) <= '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "         and ( RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO  , RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO,RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID )  ";
                SQL = SQL + "         IN(  ";
                SQL = SQL + "select RM_MRM_RECEIPT_NO,RM_MRMA_APPROVAL_NO,AD_FIN_FINYRID_APPROVAL from RM_PE_DETAILS,RM_PE_MASTER ";
                SQL = SQL + "where  RM_PE_DETAILS.RM_MPEM_ENTRY_NO=RM_PE_MASTER.RM_MPEM_ENTRY_NO and RM_PE_DETAILS.RM_MPEM_ENTRY_TYPE='T' ";
                SQL = SQL + "and RM_MPEM_ENTRY_DATE> '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "            )  ";
                SQL = SQL + "  ORDER BY    RECEIPT_NO ASC ";





                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }


        public DataTable FetchPendingPurchaseEntryVendorWise(string sSlectedVendor, string sSelectedRM, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();



                SQL = " SELECT  ";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO RECEIPT_NO ,";

                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE RECEIPT_DATE,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  VENDOR_CODE,";
                SQL = SQL + "  RM_VENDOR_MASTER.RM_VM_VENDOR_NAME    VENDOR_NAME,";

                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO,";

                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE PRICE_TYPE,";

                SQL = SQL + "RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + "SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_UM_UOM_CODE UOM_CODE ,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,";
                SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_DESC ,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE TRANSPORTER_CODE, ";
                SQL = SQL + "  TRANSVENDOR.RM_VM_VENDOR_NAME TRNSPORTERNAME, ";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO TRANSPO_ORDER_NO, ";

                SQL = SQL + "RM_MRM_TRNSPORTER_DOC_NO  TRNSPORTER_DOC_NO ,";


                SQL = SQL + " FA_FAM_ASSET_CODE ASSET_CODE ,RM_MRM_VEHICLE_DESCRIPTION VEHICLE_DESCRIPTION, ";
                SQL = SQL + "  RM_MRD_DRIVER_CODE DRIVER_CODE ,RM_MRD_DRIVER_NAME DRIVER_NAME, ";

                SQL = SQL + "RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY SUPPLY_QTY,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY WEIGH_QTY,  RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY REJECTED_QTY  ,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY APPROVE_QTY, ";
                //SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE SUPP_UNIT_PRICE,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT SUPP_AMOUNT,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE TRANS_RATE,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT TRANS_AMOUNT , ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT TOTAL_AMOUNT,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW SUPP_UNIT_PRICE_NEW,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW SUPP_AMOUNT_NEW,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW TRANS_RATE_NEW,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW TRANS_AMOUNT_NEW,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW TOTAL_AMOUNT_NEW,   ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_APPROVED  ,   ";
                SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRM_APPROVED_NO,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY SUPP_ENTRY    ,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO SUPP_PENO   ,   ";
                SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRD_TRANS_ENTRY TRANS_ENTRY   , ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO  TRANS_PENO  ";


                SQL = SQL + "FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + "RM_RAWMATERIAL_MASTER,";
                SQL = SQL + "RM_UOM_MASTER,";
                SQL = SQL + "RM_VENDOR_MASTER,";
                SQL = SQL + "SL_STATION_MASTER , ";
                SQL = SQL + "RM_SOURCE_MASTER, ";
                SQL = SQL + " RM_VENDOR_MASTER TRANSVENDOR ";
                SQL = SQL + "WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE    ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE    ";

                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + "  AND     RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =  TRANSVENDOR.RM_VM_VENDOR_CODE (+)";
                ///     ' SQL = SQL  +  "AND RM_MRD_APPROVED = 'N'"


                //if (!string.IsNullOrEmpty(sSlectedStation))
                //{
                //    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE IN ( '" + sSlectedStation + "') ";

                //}

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE IN ( '" + sSlectedVendor + "') ";

                }

                //if (!string.IsNullOrEmpty(sSelectedTrans))
                //{
                //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                //}

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                }

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + Branch + "') ";

                }

                //if (sApprovedStatus != "ALL")
                //{
                //    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='N'";

                //}
                SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY ='N'";

                SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + "  ORDER BY   RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ASC ";

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable FetchPendingPurchaseEntryTransporterWise(string sSlectedVendor, string sSelectedRM, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();


                //ClearScreen();



                SQL = " SELECT  ";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO RECEIPT_NO ,";

                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE RECEIPT_DATE,";
                //SQL = SQL + "RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE  VENDOR_CODE,";
                //SQL = SQL + "  RM_VENDOR_MASTER.RM_VM_VENDOR_NAME    VENDOR_NAME,";

                //SQL = SQL + "RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ORDER_NO,";

                //SQL = SQL + "RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE PRICE_TYPE,";

                SQL = SQL + "RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE STATION_CODE,";
                SQL = SQL + "SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_RMM_RM_CODE RM_CODE,";
                SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_UM_UOM_CODE UOM_CODE ,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,";
                SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_DESC ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE TRANSPORTER_CODE, ";
                SQL = SQL + "  TRANSVENDOR.RM_VM_VENDOR_NAME TRNSPORTERNAME, ";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO TRANSPO_ORDER_NO, ";

                SQL = SQL + "RM_MRM_TRNSPORTER_DOC_NO  TRNSPORTER_DOC_NO ,";


                SQL = SQL + " FA_FAM_ASSET_CODE ASSET_CODE ,RM_MRM_VEHICLE_DESCRIPTION VEHICLE_DESCRIPTION, ";
                SQL = SQL + "  RM_MRD_DRIVER_CODE DRIVER_CODE ,RM_MRD_DRIVER_NAME DRIVER_NAME, ";

                SQL = SQL + "RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY SUPPLY_QTY,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY WEIGH_QTY,  RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY REJECTED_QTY  ,";
                SQL = SQL + "RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY APPROVE_QTY, ";
                //SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE SUPP_UNIT_PRICE,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT SUPP_AMOUNT,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE TRANS_RATE,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT TRANS_AMOUNT , ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT TOTAL_AMOUNT,  ";
                //   SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW SUPP_UNIT_PRICE_NEW,  ";
                // SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW SUPP_AMOUNT_NEW,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW TRANS_RATE_NEW,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW TRANS_AMOUNT_NEW,  ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW TOTAL_AMOUNT_NEW,   ";
                //SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_APPROVED  ,   ";
                SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRM_APPROVED_NO APPROVED_NO,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_ENTRY  SUPP_ENTRY   ,  ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO   SUPP_PENO ,   ";
                SQL = SQL + "  RM_RECEPIT_DETAILS. RM_MRD_TRANS_ENTRY TRANS_ENTRY   , ";
                SQL = SQL + "  RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO  TRANS_PENO  ";


                SQL = SQL + "FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + "RM_RAWMATERIAL_MASTER,";
                SQL = SQL + "RM_UOM_MASTER,";
                SQL = SQL + "RM_VENDOR_MASTER,";
                SQL = SQL + "SL_STATION_MASTER , ";
                SQL = SQL + "RM_SOURCE_MASTER, ";
                SQL = SQL + " RM_VENDOR_MASTER TRANSVENDOR ";
                SQL = SQL + "WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE    ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE (+)    ";

                SQL = SQL + "AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                SQL = SQL + "AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + "  AND     RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =  TRANSVENDOR.RM_VM_VENDOR_CODE  ";
                ///     ' SQL = SQL  +  "AND RM_MRD_APPROVED = 'N'"


                //if (!string.IsNullOrEmpty(sSlectedStation))
                //{
                //    SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE IN ( '" + sSlectedStation + "') ";

                //}

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSlectedVendor + "') ";

                }

                //if (!string.IsNullOrEmpty(sSelectedTrans))
                //{
                //    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE IN ( '" + sSelectedTrans + "') ";

                //}

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                }
                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.AD_BR_CODE IN ( '" + Branch + "') ";

                }

                //if (sApprovedStatus != "ALL")
                //{
                //    SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='N'";

                //}
                SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MRD_TRANS_ENTRY ='N'";

                SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + "  ORDER BY   RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO ASC ";

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        #endregion

        #region "Sales Entry Report"

        public DataTable FectchSalesEntrySummary(string sSlectedVendor, string sRMCode, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");
            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT   ";
                SQL = SQL + " ENTRY_NO, ENTRY_DATE, CUSTOMER_CODE, CUSTOMER_NAME, ";
                SQL = SQL + " STATION_CODE, STATION_NAME,";
                SQL = SQL + " DEPT_CODE, DEPT_DESC, TECH_PLM_PLANT_CODE, TECH_PLM_PLANT_NAME,  ";
                SQL = SQL + " RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC, FA_FAM_ASSET_CODE, FA_FAM_ASSET_DESCRIPTION, ";
                SQL = SQL + " '' HR_EMP_EMPLOYEE_CODE, '' HR_EMP_EMPLOYEE_NAME,  ";
                SQL = SQL + " RM_MSM_BALANCE_TOTAL, RM_MSM_CREATEDBY, RM_MSM_APPROVED_BY,'' RMCODE, '' RMDESC, ";
                SQL = SQL + " '' UOM_CODE,'' UOM_DESC,MATAMOUNT,TRANSAMOUNT,TOTAL_AMOUNT, 0 AVG_COST,";
                SQL = SQL + " 0 SALE_RATE,0  QUANTITY,0 EXT_AMNT,0 TRANS_RATE,0 TRANS_AMNT, ";
                SQL = SQL + " '' INV_ACC_CODE,''  CONS_ACC_CODE, ";
                SQL = SQL + "       ad_br_code, ";
                SQL = SQL + "       MASTER_BRANCH_CODE   ";
                //SQL = SQL + "       MASTER_BRANCH_NAME , ";
                //SQL = SQL + "       MASTER_BRANCHDOC_PREFIX,  MASTER_BRANCH_POBOX,  MASTER_BRANCH_ADDRESS, ";
                //SQL = SQL + "       MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE,  MASTER_BRANCH_FAX, ";
                //SQL = SQL + "       MASTER_BRANCH_SPONSER_NAME,  MASTER_BRANCH_TRADE_LICENSE_NO,  MASTER_BRANCH_EMAIL_ID, ";
                //SQL = SQL + "       MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER ";
                SQL = SQL + " FROM rm_sales_summary_print ";
                SQL = SQL + " WHERE ";
                SQL = SQL + "   TO_DATE(TO_CHAR(ENTRY_DATE,'DD-MON-YYYY')) ";
                SQL = SQL + "   BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "   AND RM_MSM_ENTRY_TYPE='SL' ";

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND CUSTOMER_CODE IN ( '" + sSlectedVendor + "') ";
                }

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND AD_BR_CODE IN ( '" + Branch + "') ";
                }

                //if (!string.IsNullOrEmpty(sRMCode))
                //{
                //    SQL = SQL + " AND RMCODE IN ( '" + sRMCode + "') ";
                //}


                SQL = SQL + " ORDER BY ENTRY_NO";
                dtData = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                return null;
            }
            return dtData;
        }

        public DataTable FectchReturnEntrySummary(string sSlectedVendor, string sRMCode, string sFromDate, string sTodate)
        {
            DataTable dtData = new DataTable("MAIN");
            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT   ";
                SQL = SQL + " ENTRY_NO, ENTRY_DATE, CUSTOMER_CODE, CUSTOMER_NAME, ";
                SQL = SQL + " STATION_CODE, STATION_NAME,";
                SQL = SQL + " DEPT_CODE, DEPT_DESC, TECH_PLM_PLANT_CODE, TECH_PLM_PLANT_NAME,  ";
                SQL = SQL + " RM_SM_SOURCE_CODE, RM_SM_SOURCE_DESC, FA_FAM_ASSET_CODE, FA_FAM_ASSET_DESCRIPTION, ";
                SQL = SQL + " '' HR_EMP_EMPLOYEE_CODE, '' HR_EMP_EMPLOYEE_NAME,  ";
                SQL = SQL + " RM_MSM_BALANCE_TOTAL, RM_MSM_CREATEDBY, RM_MSM_APPROVED_BY,'' RMCODE, '' RMDESC, ";
                SQL = SQL + " '' UOM_CODE,'' UOM_DESC,MATAMOUNT,TRANSAMOUNT,TOTAL_AMOUNT, 0 AVG_COST,";
                SQL = SQL + " 0 SALE_RATE,0  QUANTITY,0 EXT_AMNT,0 TRANS_RATE,0 TRANS_AMNT, ";
                SQL = SQL + " '' INV_ACC_CODE,''  CONS_ACC_CODE, ";
                SQL = SQL + "       ad_br_code, ";
                SQL = SQL + "       MASTER_BRANCH_CODE   ";
                //SQL = SQL + "       MASTER_BRANCH_NAME , ";
                //SQL = SQL + "       MASTER_BRANCHDOC_PREFIX,  MASTER_BRANCH_POBOX,  MASTER_BRANCH_ADDRESS, ";
                //SQL = SQL + "       MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE,  MASTER_BRANCH_FAX, ";
                //SQL = SQL + "       MASTER_BRANCH_SPONSER_NAME,  MASTER_BRANCH_TRADE_LICENSE_NO,  MASTER_BRANCH_EMAIL_ID, ";
                //SQL = SQL + "       MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER ";
                SQL = SQL + " FROM rm_sales_summary_print ";
                SQL = SQL + " WHERE ";
                SQL = SQL + "   TO_DATE(TO_CHAR(ENTRY_DATE,'DD-MON-YYYY')) ";
                SQL = SQL + "   BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "   AND RM_MSM_ENTRY_TYPE='RT' ";

                if (!string.IsNullOrEmpty(sSlectedVendor))
                {
                    SQL = SQL + " AND CUSTOMER_CODE IN ( '" + sSlectedVendor + "') ";
                }

                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND AD_BR_CODE IN ( '" + Branch + "') ";
                }

                //if (!string.IsNullOrEmpty(sRMCode))
                //{
                //    SQL = SQL + " AND RMCODE IN ( '" + sRMCode + "') ";
                //}


                SQL = SQL + " ORDER BY ENTRY_NO";
                dtData = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                return null;
            }
            return dtData;
        }


        #endregion
        //------------------------------- END PENDING ENTRY PRUCHASE 


        //================================== STOCK REGISTER  BEGIN ============= JOMY P CHACKO 28 JAN 2013 

        #region " Stock Register"

        public double GetNextSeqNo()
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = " SELECT SEQ_WS_RPT_TMP.NEXTVAL next_seq_no   FROM DUAL";

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

        public DataSet FetchStockRegisterSummary(string sSlectedStation, string sSelectedRM, string sFromDate, string sTodate, object mngrclassobj)
        {
            DataSet dtData = new DataSet("MAIN");


            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SqlHelper clsSQLHelper = new SqlHelper();

                // USING SAME FUNCTION FOR PRINTING AS WELL AS GRID ,SO KINDLY TAKE CARE 
                SQL = " SELECT ";
                SQL = SQL + " 			A.SALES_STN_STATION_CODE STATION_CODE, A.SALES_STN_STATION_NAME STATION_NAME,";
                SQL = SQL + "  A.RM_RMM_RM_CODE RM_CODE ,  A.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION, ";

                SQL = SQL + " 			NVL(OPENING.OPQTY,0) FINYROPTQTY, NVL(OPENING.OPAMOUNT,0) FINYROPAMOUNT,";
                SQL = SQL + " 			NVL(PREVMTHBAL.PRVGRVQTY,0) PRVGRVQTY, NVL(PREVMTHBAL.PRVGRVAMOUNT,0) PRVGRVAMOUNT, ";
                SQL = SQL + " 			NVL(PREVMTHBAL.PRVISSUEQTY,0) PRVISSUEVQTY, NVL(PREVMTHBAL.PRVISSUEAMOUNT,0) PRVISSUEAMOUNT,";

                SQL = SQL + " 			(NVL(OPENING.OPQTY,0) +NVL(PREVMTHBAL.PRVGRVQTY,0))-(NVL(PREVMTHBAL.PRVISSUEQTY,0)) CURROPENQTY,";
                SQL = SQL + " 			(NVL(OPENING.OPAMOUNT,0) + NVL(PREVMTHBAL.PRVGRVAMOUNT,0)  )  - (  NVL(PREVMTHBAL.PRVISSUEAMOUNT,0) ) CURROPENAMONT, ";

                SQL = SQL + " 			NVL(GRV.GRVQTY,0) GRVQTY, NVL( GRV.GRVAMOUNT,0) GRVAMOUNT,     ";
                SQL = SQL + " 			NVL(STKIN.STKINQTY ,0) STKINQTY, NVL( STKIN.STKINAMOUNT,0) STKINAMOUNT,  ";

                SQL = SQL + " 			NVL(PHYSTKADJIN.PHYSTKADJINQTY ,0) PHYSTKADJINQTY, NVL( PHYSTKADJIN.PHYSTKADJINAMOUNT,0) PHYSTKADJINAMOUNT,  ";



                SQL = SQL + " 			NVL(ISSUE.ISSUEQTY,0) ISSUEQTY, NVL( ISSUE.ISSUEAMOUNT,0) ISSUEAMOUNT,";
                SQL = SQL + " 			NVL(STKOUT.STKOUTQTY ,0) STKOUTQTY , NVL( STKOUT.STKOUTAMOUNT ,0) STKOUTAMOUNT ,  ";
                SQL = SQL + " 			NVL(SALE.SALES_OUTQTY,0) SALEQTY ,  NVL(SALE.SALES_AMOUNT,0) SALEAMOUNT ,  ";


                SQL = SQL + " 			(NVL(OPENING.OPQTY,0) +NVL(PREVMTHBAL.PRVGRVQTY,0)+NVL(GRV.GRVQTY,0)+NVL(STKIN.STKINQTY ,0) + NVL(PHYSTKADJIN.PHYSTKADJINQTY ,0) )-(NVL(PREVMTHBAL.PRVISSUEQTY,0)+ NVL(ISSUE.ISSUEQTY,0) +NVL(STKOUT.STKOUTQTY ,0) + NVL(SALE.SALES_OUTQTY,0)) CURCLOSINGQTY, ";

                SQL = SQL + "   CASE  when  (NVL(OPENING.OPQTY,0) +NVL(PREVMTHBAL.PRVGRVQTY,0)+NVL(GRV.GRVQTY,0)+NVL(STKIN.STKINQTY ,0) + NVL(PHYSTKADJIN.PHYSTKADJINQTY ,0) )-(NVL(PREVMTHBAL.PRVISSUEQTY,0)+ NVL(ISSUE.ISSUEQTY,0) +NVL(STKOUT.STKOUTQTY ,0) + NVL(SALE.SALES_OUTQTY,0))  > 0 THEN ";

                SQL = SQL + "   TRUNC(  ( (NVL(OPENING.OPAMOUNT,0) + NVL(PREVMTHBAL.PRVGRVAMOUNT,0)+ NVL( GRV.GRVAMOUNT,0)+ NVL( STKIN.STKINAMOUNT,0) + NVL( PHYSTKADJIN.PHYSTKADJINAMOUNT,0)  )  - ( NVL(PREVMTHBAL.PRVISSUEAMOUNT,0)  +  NVL( ISSUE.ISSUEAMOUNT,0) +  NVL( STKOUT.STKOUTAMOUNT ,0)+ NVL(SALE.SALES_AMOUNT,0) ))  /  ";

                SQL = SQL + "  	((NVL(OPENING.OPQTY,0) +NVL(PREVMTHBAL.PRVGRVQTY,0)+NVL(GRV.GRVQTY,0)+NVL(STKIN.STKINQTY ,0) + NVL(PHYSTKADJIN.PHYSTKADJINQTY ,0))-(NVL(PREVMTHBAL.PRVISSUEQTY,0)+ NVL(ISSUE.ISSUEQTY,0) +NVL(STKOUT.STKOUTQTY ,0) + NVL(SALE.SALES_OUTQTY,0))),4)  ELSE 0 END AVGRATE,";

                SQL = SQL + " 			(NVL(OPENING.OPAMOUNT,0) + NVL(PREVMTHBAL.PRVGRVAMOUNT,0)+ NVL( GRV.GRVAMOUNT,0)+ NVL( STKIN.STKINAMOUNT,0)  + NVL( PHYSTKADJIN.PHYSTKADJINAMOUNT,0)  )  - ( NVL(PREVMTHBAL.PRVISSUEAMOUNT,0)  +  NVL( ISSUE.ISSUEAMOUNT,0) +  NVL( STKOUT.STKOUTAMOUNT ,0)+ NVL(SALE.SALES_AMOUNT,0) ) CURCOSINGAMUNT  ";



                // 'SQL = SQL +  "'" & msUserId +  "'";

                SQL = SQL + "   FROM ( ";
                SQL = SQL + "   SELECT RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE ,  RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + " 		RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE, SALES_STN_STATION_NAME  FROM RM_RAWMATERIAL_MASTER  , ";
                SQL = SQL + " 		RM_RAWMATERIAL_DETAILS, SL_STATION_MASTER  WHERE ";
                SQL = SQL + " 		RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE= RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE AND ";
                SQL = SQL + " 		RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE =  SL_STATION_MASTER.SALES_STN_STATION_CODE AND ";
                SQL = SQL + " 		RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE  =SL_STATION_MASTER.SALES_STN_STATION_CODE  AND ";
                SQL = SQL + "        SL_STATION_MASTER.SALES_STN_STATION_CODE  IN (  '" + sSlectedStation + "' )";



                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE IN ( '" + sSelectedRM + "') ";

                }

                SQL = SQL + " 	) A,	";
                SQL = SQL + " 	(";
                SQL = SQL + " 	SELECT SALES_STN_STATION_CODE,RM_RMM_RM_CODE,SUM(RM_OB_QTY) OPQTY, SUM( RM_OB_AMOUNT) OPAMOUNT";
                SQL = SQL + " 	 FROM RM_OPEN_BALANCES   WHERE  ad_fin_finyrid=" + mngrclass.FinYearID + "";
                SQL = SQL + " 	GROUP BY  SALES_STN_STATION_CODE,RM_RMM_RM_CODE";
                SQL = SQL + " 	) OPENING,	";

                SQL = SQL + " 	(";
                SQL = SQL + " 	select SALES_STN_STATION_CODE,  RM_IM_ITEM_CODE , ";
                SQL = SQL + " 	SUM(RM_STKL_RECD_QTY) PRVGRVQTY , SUM(RM_STKL_RECD_AMT) PRVGRVAMOUNT,";
                SQL = SQL + " 	sum(RM_STKL_ISSUE_QTY) PRVISSUEQTY ,SUM( RM_STKL_ISSUE_AMOUNT ) PRVISSUEAMOUNT ";
                SQL = SQL + " 	from RM_STOCK_LEDGER  where ";
                SQL = SQL + " 	   ad_fin_finyrid=" + mngrclass.FinYearID + "  And     ";
                SQL = SQL + " 	  RM_STKL_TRANS_DATE  <  '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";



                SQL = SQL + " 	GROUP BY  SALES_STN_STATION_CODE,RM_IM_ITEM_CODE ";
                SQL = SQL + " 	) PREVMTHBAL,	  ";

                //   '< grv means - goods receipt - STOCK TRANSFER

                SQL = SQL + " 	(";
                SQL = SQL + " 	select SALES_STN_STATION_CODE,   RM_IM_ITEM_CODE , ";
                SQL = SQL + " 	SUM(RM_STKL_RECD_QTY) GRVQTY , SUM(RM_STKL_RECD_AMT) GRVAMOUNT";
                SQL = SQL + " 	from RM_STOCK_LEDGER  where   ";
                SQL = SQL + "  RM_STKL_TRANS_TYPE = 'RECEIPT'";
                SQL = SQL + "  AND ad_fin_finyrid=" + mngrclass.FinYearID + "  And    ";
                SQL = SQL + "   RM_STKL_TRANS_DATE  between ";
                SQL = SQL + "'" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                SQL = SQL + " 	GROUP BY SALES_STN_STATION_CODE, RM_IM_ITEM_CODE ) GRV,	  ";

                SQL = SQL + " 	(";
                SQL = SQL + " 	select SALES_STN_STATION_CODE,   RM_IM_ITEM_CODE , ";
                SQL = SQL + " 	SUM(RM_STKL_RECD_QTY) STKINQTY , SUM(RM_STKL_RECD_AMT) STKINAMOUNT";
                SQL = SQL + " 	from RM_STOCK_LEDGER  where   ";
                SQL = SQL + " 	RM_STKL_TRANS_TYPE = 'STK_TRANS' AND  ad_fin_finyrid=" + mngrclass.FinYearID + "  And    ";
                SQL = SQL + " 	RM_STKL_TRANS_DATE  between ";
                SQL = SQL + "'" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + " 	GROUP BY SALES_STN_STATION_CODE, RM_IM_ITEM_CODE ) STKIN,	  ";
                // physcial stock adjustment received    type is same PHYSTK
                SQL = SQL + " 	(";
                SQL = SQL + " 	select SALES_STN_STATION_CODE,   RM_IM_ITEM_CODE , ";
                SQL = SQL + " 	SUM(RM_STKL_RECD_QTY)PHYSTKADJINQTY , SUM(RM_STKL_RECD_AMT)PHYSTKADJINAMOUNT";
                SQL = SQL + " 	from RM_STOCK_LEDGER  where   ";
                SQL = SQL + " 	RM_STKL_TRANS_TYPE = 'PHYSTK' AND  ad_fin_finyrid=" + mngrclass.FinYearID + "  And    ";
                SQL = SQL + " 	RM_STKL_TRANS_DATE  between ";
                SQL = SQL + "'" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + " 	GROUP BY SALES_STN_STATION_CODE, RM_IM_ITEM_CODE ) PHYSTKADJIN,	  ";




                SQL = SQL + " 	(select   SALES_STN_STATION_CODE,   RM_IM_ITEM_CODE, ";
                SQL = SQL + " 	sum(RM_STKL_ISSUE_QTY) ISSUEQTY, SUM( RM_STKL_ISSUE_AMOUNT ) ISSUEAMOUNT ";
                SQL = SQL + " 	from RM_STOCK_LEDGER   where     ";
                SQL = SQL + " 	RM_STKL_TRANS_TYPE = 'PHYSTK' and  ad_fin_finyrid=" + mngrclass.FinYearID + "  And  ";
                SQL = SQL + " 	RM_STKL_TRANS_DATE between ";
                SQL = SQL + "'" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                SQL = SQL + " 	GROUP BY SALES_STN_STATION_CODE, RM_IM_ITEM_CODE";
                SQL = SQL + " 	) ISSUE,";


                SQL = SQL + " 	(select   SALES_STN_STATION_CODE,   RM_IM_ITEM_CODE, ";
                SQL = SQL + " 	sum(RM_STKL_ISSUE_QTY) STKOUTQTY, SUM( RM_STKL_ISSUE_AMOUNT ) STKOUTAMOUNT ";
                SQL = SQL + " 	from RM_STOCK_LEDGER   where  ";
                SQL = SQL + "   RM_STKL_TRANS_TYPE  in( 'STK_TRANS')  ";
                SQL = SQL + "   and  ad_fin_finyrid=" + mngrclass.FinYearID + "  And  ";
                SQL = SQL + " 	RM_STKL_TRANS_DATE between ";
                SQL = SQL + "'" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + " 	GROUP BY SALES_STN_STATION_CODE, RM_IM_ITEM_CODE";
                SQL = SQL + " 	) STKOUT,";

                SQL = SQL + "   (   ";
                SQL = SQL + "  SELECT RM_IM_ITEM_CODE  RM_IM_ITEM_CODE ,SALES_STN_STATION_CODE SALES_STN_STATION_CODE ,  ";

                SQL = SQL + "   NVL(SUM(RM_STKL_ISSUE_QTY),0)  SALES_OUTQTY , ";

                SQL = SQL + "   NVL(SUM(RM_STKL_ISSUE_AMOUNT),0)SALES_AMOUNT";

                SQL = SQL + "   FROM  RM_STOCK_LEDGER  ";
                SQL = SQL + "   WHERE";
                SQL = SQL + "   RM_STKL_TRANS_TYPE  ='RM_SALE'";
                SQL = SQL + "   AND RM_STKL_TRANS_DATE between ";
                SQL = SQL + "'" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + " 	GROUP BY SALES_STN_STATION_CODE, RM_IM_ITEM_CODE   ) SALE";

                SQL = SQL + " 	WHERE	";
                SQL = SQL + " 	A.RM_RMM_RM_CODE= OPENING.RM_RMM_RM_CODE (+) AND";
                SQL = SQL + " 	A.SALES_STN_STATION_CODE= OPENING.SALES_STN_STATION_CODE (+) AND	 ";
                SQL = SQL + " 	A.RM_RMM_RM_CODE=  PREVMTHBAL.RM_IM_ITEM_CODE (+) AND 	";
                SQL = SQL + " 	A.SALES_STN_STATION_CODE= PREVMTHBAL.SALES_STN_STATION_CODE (+) AND";
                SQL = SQL + " 	A.RM_RMM_RM_CODE= GRV.RM_IM_ITEM_CODE (+) AND 	";
                SQL = SQL + " 	A.SALES_STN_STATION_CODE= GRV.SALES_STN_STATION_CODE (+) AND";



                SQL = SQL + " 	A.RM_RMM_RM_CODE= PHYSTKADJIN.RM_IM_ITEM_CODE (+) AND ";
                SQL = SQL + " 	A.SALES_STN_STATION_CODE= PHYSTKADJIN.SALES_STN_STATION_CODE (+) AND ";

                SQL = SQL + " 	A.RM_RMM_RM_CODE= ISSUE.RM_IM_ITEM_CODE (+) AND ";
                SQL = SQL + " 	A.SALES_STN_STATION_CODE= ISSUE.SALES_STN_STATION_CODE (+) AND ";

                SQL = SQL + " 	A.RM_RMM_RM_CODE= STKIN.RM_IM_ITEM_CODE (+) AND ";
                SQL = SQL + " 	A.SALES_STN_STATION_CODE= STKIN.SALES_STN_STATION_CODE (+) AND ";

                SQL = SQL + " 	A.RM_RMM_RM_CODE= STKOUT.RM_IM_ITEM_CODE (+) AND ";
                SQL = SQL + " 	A.SALES_STN_STATION_CODE= STKOUT.SALES_STN_STATION_CODE (+) AND";

                SQL = SQL + " 	A.RM_RMM_RM_CODE= SALE.RM_IM_ITEM_CODE (+) AND ";
                SQL = SQL + "  	A.SALES_STN_STATION_CODE= SALE.SALES_STN_STATION_CODE(+)   ";
                SQL = SQL + " order by A.SALES_STN_STATION_CODE ,A.RM_RMM_RM_CODE ";


                dtData = clsSQLHelper.GetDataset(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        #region "Stock report opening Closing "

        public DataSet GenerateStockOpeningClosingReport(double iSeqNo, string sReportType, DateTime dtpFrom, DateTime dtpTo, List<StationFPSEntity> objStEntity, List<BranchFPSEntity> objBranchEntity, object mngrclassobj)
        {
            DataSet dsReturn = new DataSet("STOCK_EVALUATION");
            DataSet dsData = new DataSet();

            string sRetun = string.Empty;
            try
            {

                OracleHelper oTrns = new OracleHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;



                SQL = " DELETE FROM  RM_STOCK_EVALUATION_TMP  ";
                SQL = SQL + "       WHERE  RM_USERID = '" + mngrclass.UserName + "'";
                sSQLArray.Add(SQL);


                SQL = " DELETE FROM  RM_STOCK_EVALUATION_OPT_TMP  ";
                SQL = SQL + "       WHERE  RM_USERID = '" + mngrclass.UserName + "'";
                sSQLArray.Add(SQL);

                SQL = " DELETE FROM  RM_STOCK_EVALUATION_DIV_TMP  ";
                SQL = SQL + "       WHERE  RM_USERID = '" + mngrclass.UserName + "'";
                sSQLArray.Add(SQL);

                SQL = " DELETE FROM  RM_STOCK_EVALUATION_TMP_STMT  ";
                SQL = SQL + "       WHERE  RM_USERID = '" + mngrclass.UserName + "'";
                sSQLArray.Add(SQL);




                foreach (var Data in objStEntity)
                {
                    SQL = " INSERT INTO  RM_STOCK_EVALUATION_OPT_TMP (";
                    SQL = SQL + "    SALES_STN_STATION_CODE, RM_SEQUENCE_NO, RM_USERID) ";
                    SQL = SQL + " VALUES ( '" + Data.sStationCode + "'," + iSeqNo + " , '" + mngrclass.UserName + "')";

                    sSQLArray.Add(SQL);
                }
                foreach (var Data in objBranchEntity)
                {
                    SQL = " INSERT INTO  RM_STOCK_EVALUATION_DIV_TMP (";
                    SQL = SQL + "    AD_BR_CODE, RM_SEQUENCE_NO, RM_USERID) ";
                    SQL = SQL + " VALUES ( '" + Data.sBranchCode + "'," + iSeqNo + " , '" + mngrclass.UserName + "')";

                    sSQLArray.Add(SQL);
                }

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMSTKRPT", iSeqNo.ToString(), false, Environment.MachineName, "I", sSQLArray);

                if (sRetun != "Save Successful")
                {
                    return null;
                }


                OracleCommand ocCommand = new OracleCommand("PK_RM_STOCK_REPORT.gen_stockvalue_OpenClose");
                ocCommand.Connection = ocConn;
                ocCommand.CommandType = CommandType.StoredProcedure;

                ////ocCommand.Parameters.Add("genseq", OracleType.Number).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("genseq", OracleType.Number).Value = iSeqNo;


                ////ocCommand.Parameters.Add("finyr", OracleType.Number).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("finyr", OracleType.Number).Value = mngrclass.FinYearID;

                ////ocCommand.Parameters.Add("userid", OracleType.Number).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("userid", OracleType.VarChar).Value = mngrclass.UserName;

                //ocCommand.Parameters.Add("fromdate", OracleType.DateTime).Direction = ParameterDirection.Input;
                //ocCommand.Parameters.Add("fromdate", OracleType.DateTime).Value = dtpFrom.ToString("dd-MMM-yyyy");

                //ocCommand.Parameters.Add("todatedate", OracleType.DateTime).Direction = ParameterDirection.Input;
                //ocCommand.Parameters.Add("todatedate", OracleType.DateTime).Value = dtpTo.ToString("dd-MMM-yyyy");


                //ocCommand.Parameters.Add("reporttype", OracleType.Number).Direction = ParameterDirection.Input;
                //ocCommand.Parameters.Add("reporttype", OracleType.VarChar).Value = sReportType;

                //====

                ocCommand.Parameters.Add("genseq", OracleDbType.Decimal, iSeqNo, ParameterDirection.Input);
                ocCommand.Parameters.Add("finyr", OracleDbType.Decimal, mngrclass.FinYearID, ParameterDirection.Input);
                ocCommand.Parameters.Add("userid", OracleDbType.Varchar2, mngrclass.UserName, ParameterDirection.Input);
                ocCommand.Parameters.Add("fromdate", OracleDbType.Date, dtpFrom, ParameterDirection.Input);
                ocCommand.Parameters.Add("todatedate", OracleDbType.Date, dtpTo, ParameterDirection.Input);
                ocCommand.Parameters.Add("reporttype", OracleDbType.Varchar2, sReportType, ParameterDirection.Input);
                ocCommand.Parameters.Add("cur_return", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                OracleDataAdapter odAdpt = new OracleDataAdapter(ocCommand);

                odAdpt.Fill(dsReturn);


                if (!string.IsNullOrEmpty(Filter))
                {
                    dsData = dsReturn.Clone();


                    DataTable dtData = new DataTable();
                    dtData = dsReturn.Tables[0];
                    DataRow[] drRow;
                    drRow = dtData.Select("RM_CODE IN('" + Filter + "' )");
                    foreach (DataRow copyRow in drRow)
                        dsData.Tables[0].ImportRow(copyRow);

                    return dsData;

                }
                else
                {
                    return dsReturn;
                }


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return null;

            }


        }

        #endregion

        #endregion


        #region "Stock report opening Closing "

        public DataSet GenerateStockQtyAgeingReport(double iSeqNo, string sReportType, DateTime dtpFrom, DateTime dtpTo, List<StationFPSEntity> objStEntity, List<BranchFPSEntity> objBranchEntity, object mngrclassobj)
        {
            DataSet dsReturn = new DataSet("STOCK_EVAL_REGISTER");
            DataSet dsData = new DataSet();

            string sRetun = string.Empty;
            try
            {

                OracleHelper oTrns = new OracleHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = " DELETE FROM  RM_STOCK_EVALUATION_TMP  ";
                SQL = SQL + "       WHERE  RM_USERID = '" + mngrclass.UserName + "'";
                sSQLArray.Add(SQL);


                SQL = " DELETE FROM  RM_STOCK_EVALUATION_OPT_TMP  ";
                SQL = SQL + "       WHERE  RM_USERID = '" + mngrclass.UserName + "'";
                sSQLArray.Add(SQL);

                SQL = " DELETE FROM  RM_STOCK_EVALUATION_DIV_TMP  ";
                SQL = SQL + "       WHERE  RM_USERID = '" + mngrclass.UserName + "'";
                sSQLArray.Add(SQL);

                SQL = " DELETE FROM  RM_STOCK_EVALUATION_TMP_STMT  ";
                SQL = SQL + "       WHERE  RM_USERID = '" + mngrclass.UserName + "'";
                sSQLArray.Add(SQL);

                foreach (var Data in objStEntity)
                {
                    SQL = " INSERT INTO  RM_STOCK_EVALUATION_OPT_TMP (";
                    SQL = SQL + "    SALES_STN_STATION_CODE, RM_SEQUENCE_NO, RM_USERID) ";
                    SQL = SQL + " VALUES ( '" + Data.sStationCode + "'," + iSeqNo + " , '" + mngrclass.UserName + "')";

                    sSQLArray.Add(SQL);
                }
                foreach (var Data in objBranchEntity)
                {
                    SQL = " INSERT INTO  RM_STOCK_EVALUATION_DIV_TMP (";
                    SQL = SQL + "    AD_BR_CODE, RM_SEQUENCE_NO, RM_USERID) ";
                    SQL = SQL + " VALUES ( '" + Data.sBranchCode + "'," + iSeqNo + " , '" + mngrclass.UserName + "')";

                    sSQLArray.Add(SQL);
                }

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMSTKRPT", iSeqNo.ToString(), false, Environment.MachineName, "I", sSQLArray);

                if (sRetun != "Save Successful")
                {
                    return null;
                }


                OracleCommand ocCommand = new OracleCommand("PK_RM_STOCK_REPORT.generate_stockvalueAge_ason");
                ocCommand.Connection = ocConn;
                ocCommand.CommandType = CommandType.StoredProcedure;

                ocCommand.Parameters.Add("genseq", OracleDbType.Decimal, iSeqNo, ParameterDirection.Input);
                ocCommand.Parameters.Add("finyr", OracleDbType.Decimal, mngrclass.FinYearID, ParameterDirection.Input);
                ocCommand.Parameters.Add("userid", OracleDbType.Varchar2, mngrclass.UserName, ParameterDirection.Input);
                ocCommand.Parameters.Add("fromdate", OracleDbType.Date, dtpFrom, ParameterDirection.Input);
                ocCommand.Parameters.Add("todatedate", OracleDbType.Date, dtpTo, ParameterDirection.Input);
                ocCommand.Parameters.Add("reporttype", OracleDbType.Varchar2, sReportType, ParameterDirection.Input);
                ocCommand.Parameters.Add("cur_return", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                OracleDataAdapter odAdpt = new OracleDataAdapter(ocCommand);

                odAdpt.Fill(dsReturn);
                dsReturn.Tables[0].TableName = "STOCK_EVALUATION";


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return null;

            }

            return dsReturn;
        }

        #endregion


        ///-------------- PHYSCIAL STOCK ENTRY DETEILS XL ------------------------------- //

        #region "Physcial entry details XL " 

        public DataSet PhysicalStockEntryXL(string stCode, string BranchCode, DateTime dtpFrom, DateTime dtpTo)

        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                //SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_INV_ACC_CODE INV_ACC_CODE ,";
                //SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_CONS_ACC_CODE  CONS_ACC_CODE,";

                //SQL = " select ";

                //SQL = SQL + " RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO ENTRY_NO , RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_DATE ENTRY_DATE,";
                //SQL = SQL + "   RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE STATION_CODE , SL_STATION_MASTER.SALES_STN_STATION_NAME  STATION_NAME  ,  ";

                //SQL = SQL + "  RM_PSEM_APPROVED APPROVED , RM_PSEM_APPROVEDBY APPROVEDBY , ";


                //SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE RM_Code,";
                //SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_Description,RM_RMM_RM_TYPE RM_TYPE,";

                //SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS.RM_UM_UOM_CODE_BASIC UOM_CODE_BASIC ,";
                //SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC ,";


                //SQL = SQL + " RM_PSED_STOCK_ON_HAND OPENG_STOCK_ON_HAND, RM_PSED_OPEN_RATE OPEN_RATE ,RM_PSED_OPEN_AMOUNT OPEN_AMOUNT, RM_PSED_RVD_QTY RVD_QTY , ";
                //SQL = SQL + " RM_PSED_RVD_RATE RVD_RATE , RM_PSED_RVD_AMOUNT RVD_AMOUNT , RM_PSED_STKTRN_IN_QTY STKTRN_IN_QTY , ";
                //SQL = SQL + " RM_PSED_STKTRN_IN_RATE STKTRN_IN_RATE , RM_PSED_STKTRN_IN_AMOUNT STKTRN_IN_AMOUNT , RM_PSED_STKTRN_OUT_QTY STKTRN_OUT_QTY , ";
                //SQL = SQL + " RM_PSED_STKTRN_OUT_RATE STKTRN_OUT_RATE , RM_PSED_STKTRN_OUT_AMOUNT STKTRN_OUT_AMOUNT , ";
                //SQL = SQL + " RM_PSED_SALES_QTY SALES_QTY ,RM_PSED_SALES_RATE SALES_RATE , RM_PSED_SALES_AMOUNT SALES_AMOUNT , ";
                //SQL = SQL + " RM_PSED_RECEIVED TOTAL_RECEVIED_QTY, RM_PSED_AVG_PRICE AVG_PRICE , RM_PSED_RECEIVED_AMOUNT RECEIVED_AMOUNT , RM_PSED_AVG_PRICE_NEW AVG_PRICE_NEW , ";
                //SQL = SQL + " RM_PSED_CONSUM_THEORITICAL CONSUM_THEORITICAL , RM_PSED_ACTUALQTY_FROM_PLANT ACTUALQTY_FROM_PLANT , RM_PSED_CONSUMPTION CONSUMPTION , ";
                //SQL = SQL + " RM_PSED_CONSUMPTION_AMOUNT CONSUMPTION_AMOUNT , RM_PSED_CLOSING CLOSING_QTY, RM_PSED_CLOSING_AMOUNT CLOSING_AMOUNT , ";
                //SQL = SQL + " RM_PSED_VARIANCE VARIANCE_QTY, RM_PSED_VARIANCEAMOUNT VARIANCEAMOUNT ";

                //SQL = SQL + " from ";
                //SQL = SQL + "  RM_PHYSICAL_STOCK_MASTER ,  RM_PHYSICAL_STOCK_DETAILS, RM_UOM_MASTER, RM_RAWMATERIAL_MASTER ,SL_STATION_MASTER";

                //SQL = SQL + " where ";
                //SQL = SQL + "  RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO = RM_PHYSICAL_STOCK_DETAILS.RM_PSEM_ENTRY_NO ";

                //SQL = SQL + "  AND RM_PHYSICAL_STOCK_MASTER.AD_FIN_FINYRID = RM_PHYSICAL_STOCK_DETAILS.AD_FIN_FINYRID ";

                //SQL = SQL + "  AND  RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";

                //SQL = SQL + "  AND  RM_PHYSICAL_STOCK_DETAILS.RM_UM_UOM_CODE_MEASURED = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                //SQL = SQL + "  AND  RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";

                //SQL = SQL + "  AND  RM_PSEM_ENTRY_DATE    BETWEEN   '" + System.Convert.ToDateTime(dtpFrom).ToString("dd-MMM-yyyy") + "' AND   '" + System.Convert.ToDateTime(dtpTo).ToString("dd-MMM-yyyy") + "'";

                //if( !string.IsNullOrEmpty ( stCode) )
                //{ 
                //SQL = SQL + "  AND    RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE  in(  '"+  stCode +"')"; 
                //} 

                //SQL = SQL + " ORDER BY   RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE asc  ,   ";
                //SQL = SQL + "  RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO asc  , RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE asc  ";



                SQL = " select ";

                SQL = SQL + " RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO ENTRY_NO , RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_DATE ENTRY_DATE,";
                SQL = SQL + "  RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE STATION_CODE , SL_STATION_MASTER.SALES_STN_STATION_NAME  STATION_NAME  ,  ";
                SQL = SQL + " RM_PSEM_APPROVED APPROVED , RM_PSEM_APPROVEDBY APPROVEDBY , ";
                SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE RM_Code,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_Description,RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE RM_TYPE,";
                SQL = SQL + " RM_PHYSICAL_STOCK_DETAILS.RM_UM_UOM_CODE_BASIC UOM_CODE_BASIC ,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC UOM_DESC ,";
                SQL = SQL + " RM_PSED_STOCK_ON_HAND OPENG_STOCK_ON_HAND, RM_PSED_OPEN_RATE OPEN_RATE ,RM_PSED_OPEN_AMOUNT OPEN_AMOUNT, RM_PSED_RVD_QTY RVD_QTY , ";
                SQL = SQL + " RM_PSED_RVD_RATE RVD_RATE , RM_PSED_RVD_AMOUNT RVD_AMOUNT , RM_PSED_STKTRN_IN_QTY STKTRN_IN_QTY , ";
                SQL = SQL + " RM_PSED_STKTRN_IN_RATE STKTRN_IN_RATE , RM_PSED_STKTRN_IN_AMOUNT STKTRN_IN_AMOUNT , RM_PSED_STKTRN_OUT_QTY STKTRN_OUT_QTY , ";
                SQL = SQL + " RM_PSED_STKTRN_OUT_RATE STKTRN_OUT_RATE , RM_PSED_STKTRN_OUT_AMOUNT STKTRN_OUT_AMOUNT , ";
                SQL = SQL + " RM_PSED_SALES_QTY SALES_QTY ,RM_PSED_SALES_RATE SALES_RATE , RM_PSED_SALES_AMOUNT SALES_AMOUNT , ";
                SQL = SQL + " RM_PSED_RECEIVED TOTAL_RECEVIED_QTY, RM_PSED_AVG_PRICE AVG_PRICE , RM_PSED_RECEIVED_AMOUNT RECEIVED_AMOUNT , RM_PSED_AVG_PRICE_NEW AVG_PRICE_NEW , ";
                SQL = SQL + " RM_PSED_CONSUM_THEORITICAL CONSUM_THEORITICAL , RM_PSED_ACTUALQTY_FROM_PLANT ACTUALQTY_FROM_PLANT , RM_PSED_CONSUMPTION CONSUMPTION , ";
                SQL = SQL + " RM_PSED_CONSUMPTION_AMOUNT CONSUMPTION_AMOUNT , RM_PSED_CLOSING CLOSING_QTY, RM_PSED_CLOSING_AMOUNT CLOSING_AMOUNT , ";
                SQL = SQL + " RM_PSED_VARIANCE VARIANCE_QTY, RM_PSED_VARIANCEAMOUNT VARIANCEAMOUNT ";

                SQL = SQL + " from ";
                SQL = SQL + "  RM_PHYSICAL_STOCK_MASTER ,  RM_PHYSICAL_STOCK_DETAILS, RM_UOM_MASTER, RM_RAWMATERIAL_MASTER ,SL_STATION_MASTER";

                SQL = SQL + " where ";
                SQL = SQL + "  RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO = RM_PHYSICAL_STOCK_DETAILS.RM_PSEM_ENTRY_NO ";

                SQL = SQL + "  AND RM_PHYSICAL_STOCK_MASTER.AD_FIN_FINYRID = RM_PHYSICAL_STOCK_DETAILS.AD_FIN_FINYRID ";

                SQL = SQL + "  AND  RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";

                SQL = SQL + "  AND  RM_PHYSICAL_STOCK_DETAILS.RM_UM_UOM_CODE_MEASURED = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + "  AND  RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";

                SQL = SQL + "  AND  RM_PSEM_ENTRY_DATE    BETWEEN   '" + System.Convert.ToDateTime(dtpFrom).ToString("dd-MMM-yyyy") + "' AND   '" + System.Convert.ToDateTime(dtpTo).ToString("dd-MMM-yyyy") + "'";

                if (!string.IsNullOrEmpty(BranchCode))
                {

                    SQL = SQL + " AND RM_PHYSICAL_STOCK_MASTER.AD_BR_CODE  in(  '" + BranchCode + "')";
                }

                if (!string.IsNullOrEmpty(stCode))
                {
                    SQL = SQL + "  AND    RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE  in(  '" + stCode + "')";
                }

                SQL = SQL + " ORDER BY   RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE asc  ,   ";
                SQL = SQL + "  RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO asc  , RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE asc  ";


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

        ///============================ PHYSCIAL STOCK ENTRY DETEILS XL 

        //================================= STOCK REGISTER END 



        //........................."Stock Transfer Report"................................................................




        //........................."Stock Transfer Report"................................................................


        #region "Fill Functions "

        public DataTable FillStockStation(string UserName)
        {

            DataTable dtReturn = new DataTable();

            try
            {

                String SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT   sales_stn_station_code   , sales_stn_station_name  ";
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

        public DataTable FillItemView(string Type, string fromdate, string todate, object mngrclassobj)
        {
            DataTable dtType = new DataTable();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                if (Type == "Transporter Wise Details" || Type == "Transporter Wise Summary")  //For Fuel Consumption
                {
                    SQL = " SELECT distinct RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANSPORTER_CODE CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME NAME ";
                    SQL = SQL + " FROM RM_MAT_STK_TRANSFER_DETL,RM_MAT_STK_TRANSFER_MASTER,RM_VENDOR_MASTER ";
                    SQL = SQL + " WHERE RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANSPORTER_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                    //SQL = SQL + " AND RM_VENDOR_MASTER.RM_VM_VENDOR_TYPE='Transporter'";
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO =RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO";
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.AD_FIN_FINYRID =" + mngrclass.FinYearID + "";
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_DATE  BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + " ORDER BY RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANSPORTER_CODE ASC ";
                }

                else if (Type == "Trailer Driver Wise Details" || Type == "Trailer Driver Wise Summary")  //For Issue Return
                {
                    SQL = " SELECT distinct RM_MAT_STK_TRANSFER_DETL.HR_EM_EMPLOYEE_CODE CODE, hr_employee_master.hr_emp_employee_name NAME";
                    SQL = SQL + " FROM hr_employee_master,RM_MAT_STK_TRANSFER_DETL,RM_MAT_STK_TRANSFER_MASTER";
                    SQL = SQL + " where RM_MAT_STK_TRANSFER_DETL.HR_EM_EMPLOYEE_CODE= hr_employee_master.hr_emp_employee_code ";
                    SQL = SQL + " AND HR_EMP_STATUS ='A' AND HR_DM_DESIGNATION_CODE ";
                    SQL = SQL + " IN ( SELECT RM_IP_PARAMETER_VALUE FROM RM_DEFUALTS_DRIVER_TRAILER ";
                    SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TRAILER_DESIG_CODE' ) ";
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO =RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO";
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.AD_FIN_FINYRID =" + mngrclass.FinYearID + "";
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_DATE   BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + " ORDER BY  RM_MAT_STK_TRANSFER_DETL.HR_EM_EMPLOYEE_CODE ASC";
                }

                //else if (Type == "Trailer Driver Wise Details" || Type == "Trailer Driver Wise Summary")  //For Issue Return
                //{
                //    SQL = " SELECT distinct RM_MAT_STK_TRANSFER_DETL.HR_EM_EMPLOYEE_CODE CODE, RM_MAT_STK_TRANSFER_DETL.RM_MSTD_DRIVER_DESC NAME ";
                //    SQL = SQL + " FROM hr_employee_master,RM_MAT_STK_TRANSFER_DETL,RM_MAT_STK_TRANSFER_MASTER ";
                //    SQL = SQL + " where  RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO =RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO ";
                //    SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.AD_FIN_FINYRID =" + mngrclass.FinYearID + "";
                //    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_DATE   BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(todate).ToString("dd-MMM-yyyy") + "'";
                //    SQL = SQL + " ORDER BY  RM_MAT_STK_TRANSFER_DETL.HR_EM_EMPLOYEE_CODE ASC";

                //}

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

        #region "Print Data"

        public DataSet FetchStockTransferReport(string item, string FrmStn, string ToStn, string pStatus, DateTime Fromdate, DateTime Todate)
        {
            DataSet dsReturn = new DataSet("RM_STOCKTRANSFER_REPORT");

            string sRetun = string.Empty;

            try
            {

                string sEmpCode = string.Empty;

                //SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID,";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_DATE, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM, FROM_STATION.SALES_STN_STATION_NAME  STATION_NAME_FROM ,";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_TO, TO_STATION.SALES_STN_STATION_NAME  STATION_NAME_TO ,";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_FROM_PLANT,  FROM_PLANT.TECH_PLM_PLANT_NAME  PLANT_NAME_FROM ,  ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TO_PLANT,   TO_PLANT.TECH_PLM_PLANT_NAME  PLANT_NAME_TO , ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_REMARKS,  ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_RM_AMOUNT, RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANS_AMOUNT, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_CREATEDBY,  ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_PREPARED_BY,";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_APPRV_STATUS,RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_APPRVD_BY, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_RECVD_BY,  ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_SL_NO, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE,  RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_UM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL. RM_SM_SOURCE_CODE,RM_SOURCE_MASTER.RM_SM_SOURCE_DESC , ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_IS_INTERNAL, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MTRANSPO_ORDER_NO,  RM_MAT_STK_TRANSFER_DETL.RM_MTRANSPO_FIN_FINYRID,   RM_MAT_STK_TRANSFER_DETL.RM_POD_SL_NO_TRANS, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANSPORTER_CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPNAME,  ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRNSPORTER_DOC_NO,   RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANS_CHARGES, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.FA_FAM_ASSET_CODE,   RM_MAT_STK_TRANSFER_DETL.RM_MSTD_VEHICLE_DESC,   RM_MAT_STK_TRANSFER_DETL.HR_EM_EMPLOYEE_CODE, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_DRIVER_DESC,   RM_MAT_STK_TRANSFER_DETL.RM_MSTD_ISS_DOC_NO,   RM_MAT_STK_TRANSFER_DETL.RM_MSTD_QUANTITY, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_FRMSTN_RATE,   RM_MAT_STK_TRANSFER_DETL.RM_MSTD_AMOUNT,   RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANS_AMOUNT, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_RCV_DOC_NO,   RM_MAT_STK_TRANSFER_DETL.RM_MSTD_RCV_QTY,   RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TOSTN_RATE, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRIPNO,   RM_MAT_STK_TRANSFER_DETL.RM_MSTD_REC_FLG,   RM_MAT_STK_TRANSFER_DETL.RM_MSTD_PE_DONE, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_PE_NO,   RM_MAT_STK_TRANSFER_DETL.RM_MSTD_PE_FINYR, ";
                SQL = SQL + "     RM_MAT_STK_TRANSFER_MASTER.AD_BR_CODE_FROM , ";
                SQL = SQL + "     MASTER_BRANCH.AD_BR_CODE MASTER_BRANCH_CODE ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_NAME MASTER_BRANCH_NAME, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_DOC_PREFIX MASTER_BRANCHDOC_PREFIX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_POBOX MASTER_BRANCH_POBOX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_ADDRESS MASTER_BRANCH_ADDRESS, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_CITY MASTER_BRANCH_CITY, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_PHONE MASTER_BRANCH_PHONE, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_FAX MASTER_BRANCH_FAX, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_SPONSER_NAME MASTER_BRANCH_SPONSER_NAME, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TRADE_LICENSE_NO MASTER_BRANCH_TRADE_LICENSE_NO, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_EMAIL_ID MASTER_BRANCH_EMAIL_ID, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_WEB_SITE MASTER_BRANCH_WEB_SITE, ";
                //SQL = SQL + "          MASTER_BRANCH.AD_BR_TAX_VAT_REG_NUMBER MASTER_BRANCH_VAT_REG_NUMBER ";
                SQL = SQL + " FROM                ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER,";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER ,";
                SQL = SQL + " SL_STATION_MASTER FROM_STATION,";
                SQL = SQL + " SL_STATION_MASTER TO_STATION,";
                SQL = SQL + " TECH_PLANT_MASTER  FROM_PLANT, ";
                SQL = SQL + " TECH_PLANT_MASTER  TO_PLANT, ";
                SQL = SQL + " RM_SOURCE_MASTER,";
                SQL = SQL + " RM_VENDOR_MASTER,";
                SQL = SQL + " RM_UOM_MASTER ,ad_branch_master MASTER_BRANCH ";

                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO = RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO ";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID = RM_MAT_STK_TRANSFER_DETL.AD_FIN_FINYRID ";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM = FROM_STATION.SALES_STN_STATION_CODE ";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_TO = TO_STATION.SALES_STN_STATION_CODE ";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_FROM_PLANT =FROM_PLANT.TECH_PLM_PLANT_CODE  (+) ";
                SQL = SQL + " AND   RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TO_PLANT =TO_PLANT.TECH_PLM_PLANT_CODE  (+) ";
                SQL = SQL + " AND  RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANSPORTER_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE   (+) ";
                SQL = SQL + " AND  RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                SQL = SQL + " AND  RM_MAT_STK_TRANSFER_DETL.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE  ";
                SQL = SQL + " AND  RM_MAT_STK_TRANSFER_DETL.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + "  AND RM_MAT_STK_TRANSFER_MASTER.AD_BR_CODE_FROM =  MASTER_BRANCH.AD_BR_CODE(+) ";

                SQL = SQL + " AND RM_MSTM_TRANSFER_DATE  BETWEEN '" + Fromdate.ToString("dd-MMM-yyyy") + "' AND '" + Todate.ToString("dd-MMM-yyyy") + "' ";

                if (pStatus == "Driver")
                {
                    if (!string.IsNullOrEmpty(item))
                    {

                        SQL = SQL + " AND  RM_MAT_STK_TRANSFER_DETL.HR_EM_EMPLOYEE_CODE  IN ( '" + item + "') ";
                        // SQL = SQL + " AND  RM_MAT_STK_TRANSFER_DETL.RM_MSTD_DRIVER_DESC  IN ( '" + item + "') ";
                    }
                }

                else if (pStatus == "Transporter")
                {
                    if (!string.IsNullOrEmpty(item))
                    {
                        SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANSPORTER_CODE IN ( '" + item + "') ";

                    }
                }

                if (!string.IsNullOrEmpty(FrmStn))
                {
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM IN ( '" + FrmStn + "') ";

                }

                if (!string.IsNullOrEmpty(ToStn))
                {
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_TO IN ( '" + ToStn + "') ";

                }
                if (!string.IsNullOrEmpty(Branch))
                {
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.AD_BR_CODE_FROM IN ( '" + Branch + "') ";

                }

                if (!string.IsNullOrEmpty(ToBranch))
                {
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.AD_BR_CODE_TO IN ( '" + ToBranch + "') ";

                }


                dsReturn = clsSQLHelper.GetDataset(SQL);
                dsReturn.Tables[0].TableName = "RM_STOCKTRANSFER_REPORT_PRINT";

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return null;
            }

            return dsReturn;

        }

        #endregion

        //...........................End Stock Transfer Report.................................................................

        // ---------Fill Project LookUp-------------------


        public DataTable ProjectLookUp()
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT  DISTINCT ";
                SQL = SQL + "         RM_STORE_REQUEST_MASTER.GL_COSM_ACCOUNT_CODE CODE,   ";
                SQL = SQL + "         GL_COSTING_MASTER.GL_COSM_ACCOUNT_NAME NAME  ";
                SQL = SQL + "    FROM RM_STORE_REQUEST_MASTER,   ";
                SQL = SQL + "         GL_COSTING_MASTER ";
                SQL = SQL + "   WHERE RM_STORE_REQUEST_MASTER.GL_COSM_ACCOUNT_CODE = GL_COSTING_MASTER.GL_COSM_ACCOUNT_CODE(+)  ";
                SQL = SQL + "   ORDER By RM_STORE_REQUEST_MASTER.GL_COSM_ACCOUNT_CODE ASC ";

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

                //OracleCommand ocCommand = new OracleCommand("PK_GET_GRANTED_PROJECT.Get_GrantedProjectsForRequest");
                //ocCommand.Connection = ocConn;
                //ocCommand.CommandType = CommandType.StoredProcedure;

                //ocCommand.Parameters.Add("m_user_id", OracleDbType.Varchar2, sUSERNAME, ParameterDirection.Input);


                //ocCommand.Parameters.Add("cur_Return", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                //OracleDataAdapter odAdpt = new OracleDataAdapter(ocCommand);
                //odAdpt.Fill(dtType);

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

        //---------End Fill Project LookUp--------------------



    }

    public class BranchFPSEntity
    {
        public string sBranchCode { get; set; }
    }

    public class StationFPSEntity
    {
        public string sStationCode { get; set; }
    }

}
