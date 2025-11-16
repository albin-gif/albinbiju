using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Oracle.ManagedDataAccess.Client; using Oracle.ManagedDataAccess.Types;
using System.Data;
using AccosoftUtilities;
using AccosoftLogWriter;
using AccosoftNineteenCDL;
using System.Globalization;
using System.Collections;

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class MaterialCodeChangeLogic

    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());
        String SQL = string.Empty;
        ArrayList SQLArray = new ArrayList();

        #region ""FillData"
        
        public DataTable FillView(string sFromdate, string sTodate)
        {
            DataTable dtType = new DataTable();
            try
            //-- this look i up is using various places os take care // Jomy & jins  11 / jan 23013
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                    //SQL = " SELECT DISTINCT ";
                    //SQL = SQL + " RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name ";
                    //SQL = SQL + " FROM RM_VENDOR_MASTER, RM_PO_MASTER ";
                    //SQL = SQL + " WHERE RM_PO_MASTER.RM_PO_ENTRY_TYPE = 'PURCHASE'";
                    //SQL = SQL + " AND RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_PO_MASTER.RM_VM_VENDOR_CODE";
                    //SQL = SQL + " ORDER BY RM_VM_VENDOR_NAME ASC ";

                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE Code, RM_VM_VENDOR_NAME Name  ";
                    SQL = SQL + "    FROM RM_VENDOR_MASTER, RM_RECEPIT_DETAILS  ";
                    SQL = SQL + "    WHERE   ";
                    SQL = SQL + "    RM_VENDOR_MASTER.RM_VM_VENDOR_CODE = RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE and RM_MRD_APPROVED ='N' ";
                    // SQL = SQL  + "    AND  RM_MRM_RECEIPT_DATE BETWEEN  "
                    SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                    SQL = SQL + "    ORDER BY RM_VM_VENDOR_NAME ASC  ";

                
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

        public DataTable FillPO(string VendorCode)
        {
            DataTable dtType = new DataTable();
            try
            //-- this look i up is using various places os take care // Jomy & jins  11 / jan 23013
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " select  ";
                SQL = SQL + "    RM_PO_MASTER.RM_PO_PONO Code, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME Name ,AD_FIN_FINYRID FinID ";
                SQL = SQL + "    FROM RM_PO_MASTER, RM_VENDOR_MASTER ";
                SQL = SQL + "    WHERE   ";
                SQL = SQL + "    RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "    AND RM_PO_MASTER.RM_PO_ENTRY_TYPE ='PURCHASE' ";
                SQL = SQL + "    AND RM_PO_MASTER.RM_PO_PO_STATUS ='O' ";
                SQL = SQL + "    AND RM_PO_MASTER.RM_VM_VENDOR_CODE ='" + VendorCode + "'  ";
                SQL = SQL + "    ORDER BY RM_PO_MASTER.RM_PO_PONO ASC ";
                
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

        public DataTable FillRM(string PONO,string POFinId,string stCode,string SourceCode,string RmType)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                if (RmType == "OLDRM")
                {
                SQL = " select  ";
                SQL = SQL + "    distinct RM_PO_DETAILS.RM_RMM_RM_CODE Code, RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION Name , ";
                SQL = SQL + "    RM_PO_DETAILS.RM_POD_SL_NO sl_no,RM_PO_DETAILS.RM_POD_UNIT_PRICE price ,";
                SQL = SQL + "    RM_PO_DETAILS.SALES_STN_STATION_CODE stCode,SL_STATION_MASTER.SALES_STN_STATION_NAME stname,";
                SQL = SQL + "    RM_PO_DETAILS.RM_SM_SOURCE_CODE sourceCode,RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SourceDesc ";
                SQL = SQL + "    FROM RM_PO_DETAILS, RM_RAWMATERIAL_MASTER,SL_STATION_MASTER , RM_SOURCE_MASTER ";
                SQL = SQL + "    WHERE   ";
                SQL = SQL + "    RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "    AND RM_PO_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                SQL = SQL + "    AND RM_PO_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "    AND RM_PO_DETAILS.RM_PO_PONO ='" + PONO + "'  ";
                SQL = SQL + "    AND RM_PO_DETAILS.AD_FIN_FINYRID = " + int.Parse(POFinId) + "  ";
                SQL = SQL + "    ORDER BY RM_PO_DETAILS.RM_RMM_RM_CODE ASC ";
                }

                else if (RmType == "NEWRM")
                {
                     SQL = " select  ";
                SQL = SQL + "    distinct RM_PO_DETAILS.RM_RMM_RM_CODE Code, RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION Name , ";
                SQL = SQL + "    RM_PO_DETAILS.RM_POD_SL_NO sl_no,RM_PO_DETAILS.RM_POD_UNIT_PRICE price ,";
                SQL = SQL + "    RM_PO_DETAILS.SALES_STN_STATION_CODE stCode,SL_STATION_MASTER.SALES_STN_STATION_NAME stname,";
                SQL = SQL + "    RM_PO_DETAILS.RM_SM_SOURCE_CODE sourceCode,RM_SOURCE_MASTER.RM_SM_SOURCE_DESC  SourceDesc";
                SQL = SQL + "    FROM RM_PO_DETAILS, RM_RAWMATERIAL_MASTER,SL_STATION_MASTER , RM_SOURCE_MASTER ";
                SQL = SQL + "    WHERE   ";
                SQL = SQL + "    RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + "    AND RM_PO_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                SQL = SQL + "    AND RM_PO_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "    AND RM_PO_DETAILS.RM_PO_PONO ='" + PONO + "'  ";
                SQL = SQL + "    AND RM_PO_DETAILS.AD_FIN_FINYRID = " + int.Parse(POFinId) + "  ";
                SQL = SQL + "    AND RM_PO_DETAILS.SALES_STN_STATION_CODE = '" + stCode + "'  ";
                SQL = SQL + "    AND RM_PO_DETAILS.RM_SM_SOURCE_CODE = '" + SourceCode + "'  ";
                SQL = SQL + "    ORDER BY RM_PO_DETAILS.RM_RMM_RM_CODE ASC ";
                }


                //SQL = " select  ";
                //SQL = SQL + "     distinct RM_RECEPIT_DETAILS.RM_RMM_RM_CODE Code, RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION Name , ";
                //SQL = SQL + "    RM_RECEPIT_DETAILS.RM_POD_SL_NO_TRANS sl_no,RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW price ";
                //SQL = SQL + "    FROM RM_RECEPIT_DETAILS, RM_RAWMATERIAL_MASTER ";
                //SQL = SQL + "    WHERE   ";
                //SQL = SQL + "    RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                //SQL = SQL + "    AND RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO ='" + PONO + "'  ";

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


        #region " Fetch Data"

        public DataSet FectchReceiptDetails(string SupplierCode, string PONO, string PoFinyrID, string sSelectedRM,string OldSlno, string stCode,string SourceCode, string dtpFromDate, string dtpToDate)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_FIN_FINYRID,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID, ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SL_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_UM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO, RM_MTRANSPO_FIN_FINYRID , RM_MRM_TRNSPORTER_DOC_NO ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_ORDER_QTY, RM_MRD_VENDOR_DOC_NO ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY, RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY  ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO, RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO ,RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_UNIQE_NO ";

                SQL = SQL + " FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER,";
                SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER";

                SQL = SQL + " WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE    ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_MRD_APPROVED = 'N'";
                SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))";
                SQL = SQL + " BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE      = '" + SupplierCode + "' ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO       = '" + PONO + "' ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID    = "  + int.Parse(PoFinyrID) + " ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = '" + stCode +"' ";//SINCE THE RM CODE IS LINKED WITH STATION AND SOURCE , WE WILL NOT BLE ABLE GIVE ALL STATION SLECTION 
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE      = '" + SourceCode + "' ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE         = '" + sSelectedRM + "' ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_POD_SL_NO     = " + int.Parse(OldSlno) + " ";
                
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


        #region " Insert Data "

        public string InsertSQL(string VendorCode,string RMCodeOld, int slnoOld, string RMCodeNew, int slnoNew, double RMRateNew, List<ReceiptMaterialCodeCahangeDet> EntityReceipt, object mngrclassobj)
        {
            //int fiApprNo=0;

            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQLArray.Clear();

                foreach (var Data in EntityReceipt)
                {
                    SQL = "  ";
                    SQL = "INSERT INTO  RM_RECEPIT_RM_CODE_CHANGE ( ";
                    SQL = SQL + "   RM_MRM_RECEIPT_UNIQE_NO, RM_MRM_RECEIPT_NO, AD_FIN_FINYRID,  ";
                    SQL = SQL + "   RM_VM_VENDOR_CODE, RM_MPOM_ORDER_NO, RM_MPOM_FIN_FINYRID,  ";
                    SQL = SQL + "   RM_POD_SL_NO,RM_RMM_RM_CODE_OLD,RM_RMM_RM_CODE_NEW,RM_POD_SL_NO_NEW,RM_MRD_SUPP_UNIT_PRICE_NEW,SALES_STN_STATION_CODE,RM_SM_SOURCE_CODE)  ";
                    SQL = SQL + " values (   ";
                    SQL = SQL + " '" + Data.UniqueNoReceipt + "', '" + Data.ReceiptNoReceipt + "' ,  '" + Data.RectiptFinyrIdReceipt + "',";
                    SQL = SQL + " '" + VendorCode + "' ,  '" + Data.PoOrderNo + "',";
                    SQL = SQL + " '" + Data.POFinyrId + "' ," + slnoOld + " , '" + RMCodeOld + "' , '" + RMCodeNew + "', ";
                    SQL = SQL + " " + slnoNew + " , " + RMRateNew + ",'" + Data.stCode + "','" + Data.SourceCode + "' ";
                    SQL = SQL + "   )  ";

                    SQLArray.Add(SQL);
                }

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTRMCHANAGE", "RMRM", false, Environment.MachineName, "I", SQLArray);
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
    }


    public class ReceiptMaterialCodeCahangeDet
    {
        public object UniqueNoReceipt
        {
            get;
            set;
        }

        public object ReceiptNoReceipt
        {
            get;
            set;
        }

        public object RectiptFinyrIdReceipt
        {
            get;
            set;
        }

        public object PoOrderNo
        {
            get;
            set;
        }

        public object POFinyrId
        {
            get;
            set;
        }

        public object stCode
        {
            get;
            set;
        }

        public object SourceCode
        {
            get;
            set;
        }

    }
}
