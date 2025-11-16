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

/// <summary>
/// Created By      :   Jins Mathew
/// Created On      :   09-Aug-2013
/// </summary>

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class PhysicalStockTransferEntryRMLogic
    {
        string SQL = string.Empty;

        private OracleHelper oTrns;
        private SqlHelper ClsSqlhelper;
        private OracleConnection ocConn;
        private ArrayList SQLArray;

        #region "Defatult DataSet" 
        public DataSet GetDefault(string Type, string Code)
        {
            DataSet dsReturn = new DataSet();
            ClsSqlhelper = new SqlHelper();

            try
            {
                if (Type == "STATION")
                {
                    SQL = " SELECT SALES_STN_STATION_CODE CODE, SALES_STN_STATION_NAME NAME ";
                    SQL = SQL + " FROM SL_STATION_MASTER ";
                    SQL = SQL + " WHERE SALES_STN_DELETESTATUS=0  and SALES_RAW_MATERIAL_STATION ='Y'  order by SALES_STN_STATION_NAME asc";
                }
                else if (Type == "SOURCE")
                {
                    SQL = " SELECT ";
                    SQL = SQL + "RM_SM_SOURCE_CODE CODE, RM_SM_SOURCE_DESC NAME ";
                    SQL = SQL + "FROM RM_SOURCE_MASTER  order by RM_SM_SOURCE_DESC asc ";
                }
                else if (Type == "FROMPLANT")
                {
                    SQL = " SELECT TECH_PLM_PLANT_CODE CODE, TECH_PLM_PLANT_NAME NAME ";
                    SQL = SQL + " FROM TECH_PLANT_MASTER ";
                    if (!string.IsNullOrEmpty(Code))
                    {
                        SQL = SQL + " WHERE  TECH_PLM_WASHING_PLANT ='Y' AND SALES_STN_STATION_CODE='" + Code + "' order by  TECH_PLM_PLANT_CODE asc";
                    }
                }
                else if (Type == "TOPLANT")
                {
                    SQL = " SELECT TECH_PLM_PLANT_CODE CODE, TECH_PLM_PLANT_NAME NAME ";
                    SQL = SQL + " FROM TECH_PLANT_MASTER ";
                    if (!string.IsNullOrEmpty(Code))
                    {
                        SQL = SQL + " WHERE TECH_PLM_WASHING_PLANT ='N' AND  SALES_STN_STATION_CODE='" + Code + "' order by  TECH_PLM_PLANT_CODE asc";
                    }
                }
                dsReturn = ClsSqlhelper.GetDataset(SQL);
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
            return dsReturn;
        }
        #endregion

        #region "Fill Views "
        public DataTable FillView(string Type, string Station,string BranchCode, string EntryDate, string FinId, string sSelected,string TransferDocNo,
               int OPEN_QTY = 0, string sAppStatus = "N")
        {
            DataTable dt = new DataTable();
            try
            {
                ClsSqlhelper = new SqlHelper();
                // THE DATE SHOULD BE MONTH END SINCE THE APPRAL MIGHT BE THE MONTH END // JOMY 06 OCT 2014


                if (Type == "RM") // normal case 
                {
                    EntryDate = GetLastDay(Convert.ToDateTime(EntryDate));

                    SQL = "  SELECT  ";
                    SQL = SQL + " CODE, NAME, UOMCODE, UOMDESC,RM_TYPE,INVACCCODE, ";
                    SQL = SQL + " CONSACCCODE,OPEN_QTY, RMPRICE, OPEN_VALUE ,UOMDESC ";
                    SQL = SQL + " FROM  ";
                    SQL = SQL + " (  ";
                    SQL = SQL + " SELECT MAS.RM_RMM_RM_CODE CODE, MAS.RM_RMM_RM_DESCRIPTION NAME, ";
                    SQL = SQL + " MAS.RM_UM_UOM_CODE UOMCODE,UOMMAS.RM_UM_UOM_DESC UOMDESC,MAS.RM_RMM_RM_TYPE RM_TYPE, ";
                    SQL = SQL + " RM_RMM_INV_ACC_CODE INVACCCODE, RM_RMM_CONS_ACC_CODE CONSACCCODE, ";
                    SQL = SQL + " SUM ( NVL (OPENING.OB_QTY, 0) + NVL (RECD.REC_QTY, 0) - NVL (ISSUED.ISS_QTY, 0) - NVL(ACTUAL_QTY.TRNS_QTY,0) ) OPEN_QTY, ";
                    SQL = SQL + " TRUNC (  ";
                    SQL = SQL + " CASE WHEN SUM ( NVL (OPENING.OB_QTY, 0) + NVL (RECD.REC_QTY, 0))- SUM(NVL (ISSUED.ISS_QTY, 0)) > 0 THEN  ";
                    SQL = SQL + " SUM ( NVL (OPENING.OB_VAL, 0) + NVL (RECD.REC_VALUE, 0) - NVL (ISSUED.ISS_VALUE, 0))/ SUM (NVL (OPENING.OB_QTY, 0) + NVL (RECD.REC_QTY, 0)-NVL (ISSUED.ISS_QTY, 0) ) ";
                    SQL = SQL + " WHEN SUM ( NVL (OPENING.OB_QTY, 0) + NVL (RECD.REC_QTY, 0)) > 0 THEN  ";
                    SQL = SQL + " SUM ( NVL (OPENING.OB_VAL, 0) + NVL (RECD.REC_VALUE, 0))/ SUM (NVL (OPENING.OB_QTY, 0) + NVL (RECD.REC_QTY, 0) ) ";
                    SQL = SQL + " WHEN SUM ( NVL (OPENING.OB_QTY, 0) + NVL (RECD.REC_QTY, 0))- SUM(NVL (ISSUED.ISS_QTY, 0)) = 0 THEN 0  ";
                    SQL = SQL + " ELSE 0 END ,5 ) RMPRICE , ";
                    SQL = SQL + " SUM ( ";
                    SQL = SQL + " NVL (OPENING.OB_VAL, 0) ";
                    SQL = SQL + " + NVL (RECD.REC_VALUE, 0) ";
                    SQL = SQL + " - NVL (ISSUED.ISS_VALUE, 0) ";
                    SQL = SQL + " ) OPEN_VALUE ";
                    SQL = SQL + " FROM RM_RAWMATERIAL_MASTER MAS,RM_UOM_MASTER UOMMAS, ";
                    SQL = SQL + " RM_RAWMATERIAL_DETAILS DET, ";
                    SQL = SQL + " (  ";
                    SQL = SQL + " SELECT RM_RMM_RM_CODE RM_CODE, SALES_STN_STATION_CODE STN_CODE, AD_BR_CODE,";
                    SQL = SQL + " SUM (RM_OB_QTY) ";
                    SQL = SQL + " OB_QTY, SUM ( RM_OB_AMOUNT ) OB_VAL ";
                    SQL = SQL + " FROM RM_OPEN_BALANCES ";
                    SQL = SQL + " WHERE AD_FIN_FINYRID = " + FinId + "  ";
                    SQL = SQL + " AND SALES_STN_STATION_CODE = '" + Station + "' ";
                    SQL = SQL + " AND AD_BR_CODE = '" + BranchCode + "' ";
                    SQL = SQL + " GROUP BY RM_RMM_RM_CODE, SALES_STN_STATION_CODE,AD_BR_CODE ) OPENING, ";
                    SQL = SQL + " (  ";
                    SQL = SQL + " SELECT RM_IM_ITEM_CODE RM_CODE, SALES_STN_STATION_CODE STN_CODE,AD_BR_CODE, ";
                    SQL = SQL + " SUM (NVL (RM_STKL_RECD_QTY, 0)) ";
                    SQL = SQL + " REC_QTY, ";
                    SQL = SQL + " SUM (NVL (RM_STKL_RECD_AMT, 0) ) REC_VALUE ";
                    SQL = SQL + " FROM RM_STOCK_LEDGER ";
                    SQL = SQL + " WHERE TO_DATE (TO_CHAR (RM_STKL_TRANS_DATE, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(EntryDate).ToString("dd-MMM-yyyy") + "' ";
                    SQL = SQL + " AND AD_FIN_FINYRID =" + FinId + " ";
                    SQL = SQL + " AND SALES_STN_STATION_CODE = '" + Station + "' ";
                    SQL = SQL + " AND AD_BR_CODE = '" + BranchCode + "' ";
                    SQL = SQL + " GROUP BY RM_IM_ITEM_CODE , SALES_STN_STATION_CODE ,AD_BR_CODE ";
                    SQL = SQL + " ) RECD,  ";
                    SQL = SQL + " (  ";
                    SQL = SQL + " SELECT RM_IM_ITEM_CODE RM_CODE, SALES_STN_STATION_CODE STN_CODE,AD_BR_CODE, ";
                    SQL = SQL + " SUM (NVL (RM_STKL_ISSUE_QTY, 0)) ";
                    SQL = SQL + " ISS_QTY, ";
                    SQL = SQL + " SUM ( NVL (RM_STKL_ISSUE_AMOUNT, 0) ) ISS_VALUE ";
                    SQL = SQL + " FROM RM_STOCK_LEDGER ";
                    SQL = SQL + " WHERE TO_DATE (TO_CHAR (RM_STKL_TRANS_DATE, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(EntryDate).ToString("dd-MMM-yyyy") + "' ";
                    SQL = SQL + " AND AD_FIN_FINYRID = " + FinId + " ";
                    SQL = SQL + " AND SALES_STN_STATION_CODE = '" + Station + "' ";
                    SQL = SQL + " AND AD_BR_CODE = '" + BranchCode + "' ";
                    SQL = SQL + " GROUP BY RM_IM_ITEM_CODE , SALES_STN_STATION_CODE,AD_BR_CODE  ";
                    SQL = SQL + " ) ISSUED, ";
                    SQL = SQL + " (SELECT RM_RMM_RM_CODE RM_CODE, SA_STN_STATION_CODE_FROM STN_CODE,AD_BR_CODE_FROM AD_BR_CODE, ";
                    SQL = SQL + " SUM(NVL(RM_MSTD_QUANTITY,0)) TRNS_QTY ";
                    SQL = SQL + " FROM RM_MAT_STK_TRANSFER_MASTER, RM_MAT_STK_TRANSFER_DETL ";
                    SQL = SQL + " WHERE RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO=RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO ";
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID = " + FinId + " ";
                    SQL = SQL + " AND SA_STN_STATION_CODE_FROM='" + Station + "' ";
                    SQL = SQL + " AND AD_BR_CODE_FROM='" + BranchCode + "' ";
                    SQL = SQL + " AND RM_MSTM_APPRV_STATUS='N' ";
                    if(!string.IsNullOrEmpty(TransferDocNo))
                    {
                        SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO !='" + TransferDocNo + "'";
                    }
                    SQL = SQL + " GROUP BY RM_RMM_RM_CODE , SA_STN_STATION_CODE_FROM,AD_BR_CODE_FROM ) ACTUAL_QTY ";
                    SQL = SQL + " WHERE   ";
                    SQL = SQL + " MAS.RM_RMM_RM_CODE = DET.RM_RMM_RM_CODE ";
                    SQL = SQL + " AND DET.RM_RMM_RM_CODE = OPENING.RM_CODE(+) ";
                    SQL = SQL + " AND DET.SALES_STN_STATION_CODE = OPENING.STN_CODE(+) ";
                    SQL = SQL + " AND DET.RM_RMM_RM_CODE = RECD.RM_CODE(+) ";
                    SQL = SQL + " AND DET.SALES_STN_STATION_CODE = RECD.STN_CODE(+) ";
                    SQL = SQL + " AND DET.RM_RMM_RM_CODE = ISSUED.RM_CODE(+) ";
                    SQL = SQL + " AND DET.SALES_STN_STATION_CODE = ISSUED.STN_CODE(+) ";
                    SQL = SQL + " AND DET.RM_RMM_RM_CODE = ACTUAL_QTY.RM_CODE(+) ";
                    SQL = SQL + " AND DET.SALES_STN_STATION_CODE = ACTUAL_QTY.STN_CODE(+)  ";
                    SQL = SQL + " AND DET.SALES_STN_STATION_CODE = '" + Station + "' "; 
                    SQL = SQL + " AND DET.AD_BR_CODE = OPENING.AD_BR_CODE(+) ";
                     SQL = SQL + " AND DET.AD_BR_CODE = RECD.AD_BR_CODE(+) ";
                    SQL = SQL + " AND DET.AD_BR_CODE = ISSUED.AD_BR_CODE(+) ";
                    SQL = SQL + " AND DET.AD_BR_CODE = ACTUAL_QTY.AD_BR_CODE(+)  "; 
                    SQL = SQL + " AND DET.AD_BR_CODE = '" + BranchCode + "' ";
                    SQL = SQL + " AND MAS.RM_UM_UOM_CODE = UOMMAS.RM_UM_UOM_CODE (+) ";
                    SQL = SQL + " AND MAS.RM_RMM_RM_CODE = DET.RM_RMM_RM_CODE (+) ";
                    SQL = SQL + " GROUP BY MAS.RM_RMM_RM_CODE, ";
                    SQL = SQL + " MAS.RM_UM_UOM_CODE, MAS.RM_RMM_RM_TYPE,";
                    SQL = SQL + " RM_RMM_INV_ACC_CODE, ";
                    SQL = SQL + " RM_RMM_CONS_ACC_CODE, ";
                    SQL = SQL + " RM_RMM_RM_DESCRIPTION, ";
                    SQL = SQL + " UOMMAS.RM_UM_UOM_DESC, ";
                    SQL = SQL + " DET.SALES_STN_STATION_CODE, ";
                    SQL = SQL + " DET.AD_BR_CODE ";
                   

                    SQL = SQL + " ) WHERE OPEN_QTY > " + OPEN_QTY + " ";

                    if (OPEN_QTY < 0)
                    {  // this is form the qty check do soave jomy 15 dec should be select the inv qty not excluded qty 
                        // take care 
                        SQL = SQL + "   AND  CODE  IN('" + sSelected.Replace(",", "','") + "')";
                    }
                    else
                    {
                        // this is form look up should be aty > 0 and not in itme in look up 
                        if (!string.IsNullOrEmpty(sSelected.ToString().Trim()))
                        {
                            SQL = SQL + "   AND  CODE NOT IN('" + sSelected.Replace(",", "','") + "')";
                        }
                    }


                    SQL = SQL + " ORDER BY CODE ASC ";
                }
                else if (Type == "WASHING SAND")
                {
                    //-- GRID LOOK UP  ONLY FOR WASHING SANDD , ACTION _;- THE MAINO BJECTIVE , IS TO CONVERTING ONE RAWMATERIAL TO AONTHER RAWWMATERIAL
                    //- RM - 3/ 4 TO---> RM / WAS HING SAND 3/4

                    SQL = "  SELECT  ";
                    SQL = SQL + " CODE, NAME, UOMCODE, UOMDESC,RM_TYPE,INVACCCODE, ";
                    SQL = SQL + " CONSACCCODE,0 OPEN_QTY,0 RMPRICE,0 OPEN_VALUE ,UOMDESC ";
                    SQL = SQL + " FROM  ";
                    SQL = SQL + " ( SELECT RM_RMM_RM_CODE CODE, RM_RMM_RM_DESCRIPTION NAME, ";
                    SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE UOMCODE,RM_UM_UOM_DESC UOMDESC,RM_RMM_RM_TYPE RM_TYPE, ";
                    SQL = SQL + " RM_RMM_INV_ACC_CODE INVACCCODE, RM_RMM_CONS_ACC_CODE CONSACCCODE ";
                    SQL = SQL + " FROM RM_RAWMATERIAL_MASTER ,RM_UOM_MASTER ";
                    SQL = SQL + " WHERE   ";
                    SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE (+) ";
                    SQL = SQL + " AND RM_RMM_RM_TYPE in('AGGREGATES','SAND')";
                    if (!string.IsNullOrEmpty(sSelected.ToString().Trim()))
                    {
                        SQL = SQL + "   AND  RM_RMM_RM_CODE NOT IN('" + sSelected.Replace(",", "','") + "')";
                    }
                    SQL = SQL + " )";

                }
                else if (Type == "DRIVER")
                {
                    SQL = " SELECT HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE CODE, HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_NAME NAME, ";
                    SQL = SQL + " HR_EMPLOYEE_MASTER.HR_EMP_TELNO TEL_NO, HR_EMPLOYEE_MASTER.HR_EMP_MOBILENO MOBILE_NO, ";
                    SQL = SQL + " '' OPEN_QTY, '' RMPRICE, '' UOMCODE,'' UOMDESC ";
                    SQL = SQL + " FROM HR_EMPLOYEE_MASTER  ";
                    SQL = SQL + " WHERE  HR_EMP_STATUS ='A' AND  HR_DM_DESIGNATION_CODE  ";
                    SQL = SQL + " IN ( SELECT RM_IP_PARAMETER_VALUE FROM RM_DEFUALTS_DRIVER_TRAILER ";
                    SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TRAILER_DESIG_CODE' ) order by HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE asc ";
                }
                else if (Type == "TRAILER")
                {
                    SQL = "  SELECT FA_FIXED_ASSET_MASTER.FA_FAM_REF_CODE RCODE , ";
                    SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE CODE , FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_DESCRIPTION NAME, ";
                    SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_ID ASSET_ID , FA_FIXED_ASSET_MASTER.FA_FAM_PLATENO VehicleNo,";
                    SQL = SQL + " '' OPEN_QTY, '' RMPRICE, '' UOMCODE,'' UOMDESC ";
                    SQL = SQL + " FROM FA_FIXED_ASSET_MASTER , WS_ASSET_CATEGORY_TYPE ";
                    SQL = SQL + " WHERE FA_FIXED_ASSET_MASTER.FA_FAM_VEHICLE_TYPE = WS_ASSET_CATEGORY_TYPE.SL_NO ";
                    SQL = SQL + " AND FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_STATUS ='A' ";
                    SQL = SQL + " AND FA_FIXED_ASSET_MASTER.FA_FAM_VEHICLE_TYPE ";
                    SQL = SQL + " IN( SELECT RM_IP_PARAMETER_VALUE FROM   RM_DEFUALTS_DRIVER_TRAILER ";
                    SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TRAILER_TYPE_CODE' )  order by FA_FIXED_ASSET_MASTER.FA_FAM_REF_CODE asc ";
                }

                dt = ClsSqlhelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dt;
        }

        public DataTable FillTransPOView(string stationCode, string sourcecode, string rmcode)//
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
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_NAME,";
                SQL = SQL + " RM_PO_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_NAME , ";
                SQL = SQL + " RM_PO_DETAILS.RM_POD_NEWPRICE Transprice,";
                SQL = SQL + " to_char(RM_PO_MASTER.AD_FIN_FINYRID) Finyrid, ";
                SQL = SQL + " RM_PO_PO_DATE ORDER_DATE  , RM_PO_PRICETYPE POTYPE,RM_POD_RM_VAT_TYPE,RM_POD_RM_VAT_RATE, ";
                SQL = SQL + " RM_PO_DETAILS.RM_UOM_UOM_CODE UOMCODE,";
                SQL = SQL + " RM_PO_DETAILS.RM_POD_SL_NO  Slno ";
                SQL = SQL + " FROM RM_PO_MASTER, ";
                SQL = SQL + " RM_VENDOR_MASTER,";
                SQL = SQL + " RM_PO_DETAILS,";
                SQL = SQL + " SL_STATION_MASTER,  ";//TECH_PLANT_MASTER,
                SQL = SQL + " RM_RAWMATERIAL_MASTER , RM_SOURCE_MASTER";
                SQL = SQL + " WHERE  ";
                //RM_PO_MASTER.RM_PO_PO_STATUS="O"
                SQL = SQL + "  RM_PO_DETAILS.RM_POD_STATUS = 'O' and RM_PO_APPROVED ='Y' and RM_PO_MASTER.RM_PO_ENTRY_TYPE ='STOCKTRANSFER'"; //TRANSPORTER
                                                                                                                                              // SQL = SQL + "  AND RM_PO_DETAILS.SALES_STN_STATION_CODE = TECH_PLANT_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "  AND   RM_PO_DETAILS.RM_SM_SOURCE_CODE =  RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                SQL = SQL + " AND RM_PO_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE =  SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_PO_MASTER.RM_PO_PONO = RM_PO_DETAILS.RM_PO_PONO ";
                SQL = SQL + " AND RM_PO_MASTER.AD_FIN_FINYRID = RM_PO_DETAILS.AD_FIN_FINYRID";
                SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";

                SQL = SQL + " AND RM_PO_DETAILS.SALES_STN_STATION_CODE ='" + stationCode + "'";

                SQL = SQL + " AND RM_PO_DETAILS.RM_SM_SOURCE_CODE ='" + sourcecode + "'";
                // SQL = SQL + " AND TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE = '" + plantcode + "' ";
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

        public DataTable FillBranch()
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


        #region Get Current Quantity"
        public string GetLastDay(DateTime dtLastDate)
        {
            string strCondition = string.Empty;
            DataSet dt = new DataSet();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "Select Last_Day('" + Convert.ToDateTime(dtLastDate).ToString("dd-MMM-yyyy") + "') LSTDATE From Dual";

                dt = clsSQLHelper.GetDataset(SQL);
            }

            catch (Exception ex)
            { }
            return dt.Tables[0].Rows[0]["LSTDATE"].ToString();
        }
        public DataSet GetCurrentQty(string ItemCode, string StCode,string BranchCode,string TransferDocNo, string EntryDate, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();
            // THE DATE SHOULD BE MONTH END SINCE THE APPRAL MIGHT BE THE MONTH END // JOMY 06 OCT 2014
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                // THE DATE SHOULD BE MONTH END SINCE THE APPRAL MIGHT BE THE MONTH END // JOMY 06 OCT 2014

                EntryDate = GetLastDay(Convert.ToDateTime(EntryDate));


                SQL = "  SELECT  ";
                SQL = SQL + " SUM ( NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0) - NVL (issued.iss_qty, 0) ) Current_qty, ";
                SQL = SQL + " SUM ( NVL (opening.ob_val, 0) + NVL (recd.rec_value, 0) - NVL (issued.iss_value, 0) ) Current_Price, ";
                SQL = SQL + " SUM ( NVL (OPENING.OB_QTY, 0) + NVL (RECD.REC_QTY, 0) - NVL (ISSUED.ISS_QTY, 0) - NVL(ACTUAL_QTY.TRNS_QTY,0)) ACTUAL_QTY ";
                SQL = SQL + " FROM rm_rawmaterial_master mas,RM_UOM_MASTER uommas, ";
                SQL = SQL + " rm_rawmaterial_details det, ";
                SQL = SQL + " (  ";
                SQL = SQL + "  SELECT rm_rmm_rm_code rm_code, sales_stn_station_code stn_code, AD_BR_CODE,";
                SQL = SQL + " SUM (rm_ob_qty) ";
                SQL = SQL + " ob_qty, SUM ( RM_OB_AMOUNT ) ob_val ";
                SQL = SQL + " FROM RM_OPEN_BALANCES ";
                SQL = SQL + " WHERE ad_fin_finyrid = " + mngrclass.FinYearID + " ";
                SQL = SQL + " AND sales_stn_station_code = '" + StCode + "' ";
                SQL = SQL + " AND AD_BR_CODE = '" + BranchCode + "' ";
                SQL = SQL + " GROUP BY rm_rmm_rm_code, sales_stn_station_code,AD_BR_CODE ) opening, ";
                SQL = SQL + " (  ";
                SQL = SQL + "  SELECT RM_IM_ITEM_CODE rm_code, sales_stn_station_code stn_code, AD_BR_CODE,";
                SQL = SQL + " SUM (NVL (RM_STKL_RECD_QTY, 0)) ";
                SQL = SQL + " rec_qty, ";
                SQL = SQL + " SUM (NVL (RM_STKL_RECD_AMT, 0) ) rec_value ";
                SQL = SQL + " FROM RM_STOCK_LEDGER ";
                SQL = SQL + " WHERE TO_DATE (TO_CHAR (rm_stkl_trans_date, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(EntryDate).ToString("dd-MMM-yyyy") + "' ";
                SQL = SQL + " AND ad_fin_finyrid =" + mngrclass.FinYearID + " ";
                SQL = SQL + " AND sales_stn_station_code = '" + StCode + "' ";
                SQL = SQL + " AND AD_BR_CODE = '" + BranchCode + "' ";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE , sales_stn_station_code,AD_BR_CODE  ";
                SQL = SQL + "  ) recd,  ";
                SQL = SQL + " (  ";
                SQL = SQL + "  SELECT RM_IM_ITEM_CODE rm_code, sales_stn_station_code stn_code,AD_BR_CODE, ";
                SQL = SQL + " SUM (NVL (RM_STKL_ISSUE_QTY, 0)) ";
                SQL = SQL + " iss_qty, ";
                SQL = SQL + " SUM ( NVL (RM_STKL_ISSUE_AMOUNT, 0) ) iss_value ";
                SQL = SQL + " FROM RM_STOCK_LEDGER ";
                SQL = SQL + " WHERE TO_DATE (TO_CHAR (rm_stkl_trans_date, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(EntryDate).ToString("dd-MMM-yyyy") + "' ";
                SQL = SQL + " AND ad_fin_finyrid = " + mngrclass.FinYearID + " ";
                SQL = SQL + " AND sales_stn_station_code = '" + StCode + "' ";
                 SQL = SQL + " AND AD_BR_CODE = '" + BranchCode + "' ";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE , sales_stn_station_code,AD_BR_CODE  ";
                SQL = SQL + "  ) issued, ";
                SQL = SQL + " (SELECT RM_RMM_RM_CODE RM_CODE, SA_STN_STATION_CODE_FROM STN_CODE,AD_BR_CODE_FROM AD_BR_CODE, ";
                SQL = SQL + " SUM(NVL(RM_MSTD_QUANTITY,0)) TRNS_QTY ";
                SQL = SQL + " FROM RM_MAT_STK_TRANSFER_MASTER, RM_MAT_STK_TRANSFER_DETL ";
                SQL = SQL + " WHERE RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO=RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";
                SQL = SQL + " AND SA_STN_STATION_CODE_FROM='" + StCode + "' ";
                SQL = SQL + " AND AD_BR_CODE_FROM='" + BranchCode + "' ";
                SQL = SQL + " AND RM_MSTM_APPRV_STATUS='N' ";
                //if(!string.IsNullOrEmpty(TransferDocNo))
                //{
                //    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO !='" + TransferDocNo + "' ";
                //}
                SQL = SQL + " GROUP BY RM_RMM_RM_CODE , SA_STN_STATION_CODE_FROM,AD_BR_CODE_FROM ) ACTUAL_QTY ";
                SQL = SQL + " WHERE   ";
                SQL = SQL + " mas.rm_rmm_rm_code = det.rm_rmm_rm_code ";
                SQL = SQL + " AND det.rm_rmm_rm_code = opening.rm_code(+) ";
                SQL = SQL + " AND det.sales_stn_station_code = opening.stn_code(+) ";
                SQL = SQL + " AND det.rm_rmm_rm_code = recd.rm_code(+) ";
                SQL = SQL + " AND det.sales_stn_station_code = recd.stn_code(+) ";
                SQL = SQL + " AND det.rm_rmm_rm_code = issued.rm_code(+) ";
                SQL = SQL + " AND det.sales_stn_station_code = issued.stn_code(+) ";
                SQL = SQL + " AND DET.RM_RMM_RM_CODE = ACTUAL_QTY.RM_CODE(+) ";
                SQL = SQL + " AND DET.SALES_STN_STATION_CODE = ACTUAL_QTY.STN_CODE(+)  ";
                SQL = SQL + " AND det.sales_stn_station_code = '" + StCode + "' "; 
                SQL = SQL + " AND det.AD_BR_CODE = opening.AD_BR_CODE(+) ";
                SQL = SQL + " AND det.AD_BR_CODE = recd.AD_BR_CODE(+) ";
                SQL = SQL + " AND det.AD_BR_CODE = issued.AD_BR_CODE(+) ";
                SQL = SQL + " AND DET.AD_BR_CODE = ACTUAL_QTY.AD_BR_CODE(+)  "; 
                SQL = SQL + " AND det.AD_BR_CODE = '" + BranchCode + "' ";
                SQL = SQL + " AND mas.RM_UM_UOM_CODE = uommas.RM_UM_UOM_CODE (+) ";
                SQL = SQL + " AND mas.RM_RMM_RM_CODE = det.RM_RMM_RM_CODE (+) ";
                SQL = SQL + " AND mas.rm_rmm_rm_code='" + ItemCode + "' ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        public DataSet GetCurrentQtyOnHandValidation(string ItemCode, string StCode,string BranchCode, string EntryDate, object mngrclassobj)
        {
            DataSet sReturn = new DataSet();
            // THE DATE SHOULD BE MONTH END SINCE THE APPRAL MIGHT BE THE MONTH END // JOMY 06 OCT 2014 
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                EntryDate = GetLastDay(Convert.ToDateTime(EntryDate));

                SQL = "  SELECT  ";
                SQL = SQL + " SUM ( NVL (opening.ob_qty, 0) + NVL (recd.rec_qty, 0) - NVL (issued.iss_qty, 0) ) Current_qty, ";
                SQL = SQL + " SUM ( NVL (opening.ob_val, 0) + NVL (recd.rec_value, 0) - NVL (issued.iss_value, 0) ) Current_Amt, ";
                SQL = SQL + " SUM ( NVL (OPENING.OB_QTY, 0) + NVL (RECD.REC_QTY, 0) - NVL (ISSUED.ISS_QTY, 0)  ) ACTUAL_QTY ";
                SQL = SQL + " FROM rm_rawmaterial_master mas,RM_UOM_MASTER uommas, ";
                SQL = SQL + " rm_rawmaterial_details det, ";
                SQL = SQL + " (  ";
                SQL = SQL + "  SELECT rm_rmm_rm_code rm_code, sales_stn_station_code stn_code,AD_BR_CODE, ";
                SQL = SQL + " SUM (rm_ob_qty) ";
                SQL = SQL + " ob_qty, SUM ( RM_OB_AMOUNT ) ob_val ";
                SQL = SQL + " FROM RM_OPEN_BALANCES ";
                SQL = SQL + " WHERE ad_fin_finyrid = " + mngrclass.FinYearID + " ";
                SQL = SQL + " AND sales_stn_station_code = '" + StCode + "' ";
                SQL = SQL + " AND AD_BR_CODE = '" + BranchCode + "' ";
                SQL = SQL + " GROUP BY rm_rmm_rm_code, sales_stn_station_code,AD_BR_CODE ) opening, ";
                SQL = SQL + " (  ";
                SQL = SQL + "  SELECT RM_IM_ITEM_CODE rm_code, sales_stn_station_code stn_code,AD_BR_CODE, ";
                SQL = SQL + " SUM (NVL (RM_STKL_RECD_QTY, 0)) ";
                SQL = SQL + " rec_qty, ";
                SQL = SQL + " SUM (NVL (RM_STKL_RECD_AMT, 0) ) rec_value ";
                SQL = SQL + " FROM RM_STOCK_LEDGER ";
                SQL = SQL + " WHERE TO_DATE (TO_CHAR (rm_stkl_trans_date, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(EntryDate).ToString("dd-MMM-yyyy") + "' ";
                SQL = SQL + " AND ad_fin_finyrid =" + mngrclass.FinYearID + " ";
                SQL = SQL + " AND sales_stn_station_code = '" + StCode + "' ";
                SQL = SQL + " AND AD_BR_CODE = '" + BranchCode + "' ";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE , sales_stn_station_code,AD_BR_CODE  ";
                SQL = SQL + "  ) recd,  ";
                SQL = SQL + " (  ";
                SQL = SQL + "  SELECT RM_IM_ITEM_CODE rm_code, sales_stn_station_code stn_code,AD_BR_CODE, ";
                SQL = SQL + " SUM (NVL (RM_STKL_ISSUE_QTY, 0)) ";
                SQL = SQL + " iss_qty, ";
                SQL = SQL + " SUM ( NVL (RM_STKL_ISSUE_AMOUNT, 0) ) iss_value ";
                SQL = SQL + " FROM RM_STOCK_LEDGER ";
                SQL = SQL + " WHERE TO_DATE (TO_CHAR (rm_stkl_trans_date, 'DD-MON-YYYY')) <='" + Convert.ToDateTime(EntryDate).ToString("dd-MMM-yyyy") + "' ";
                SQL = SQL + " AND ad_fin_finyrid = " + mngrclass.FinYearID + " ";
                SQL = SQL + " AND sales_stn_station_code = '" + StCode + "' ";
                SQL = SQL + " AND AD_BR_CODE = '" + BranchCode + "' ";
                SQL = SQL + " GROUP BY RM_IM_ITEM_CODE , sales_stn_station_code,AD_BR_CODE  ";
                SQL = SQL + "  ) issued  ";

                SQL = SQL + " WHERE   ";
                SQL = SQL + " mas.rm_rmm_rm_code = det.rm_rmm_rm_code ";
                SQL = SQL + " AND det.rm_rmm_rm_code = opening.rm_code(+) ";
                SQL = SQL + " AND det.sales_stn_station_code = opening.stn_code(+) ";
                SQL = SQL + " AND det.rm_rmm_rm_code = recd.rm_code(+) ";
                SQL = SQL + " AND det.sales_stn_station_code = recd.stn_code(+) ";
                SQL = SQL + " AND det.rm_rmm_rm_code = issued.rm_code(+) ";
                SQL = SQL + " AND det.sales_stn_station_code = issued.stn_code(+) ";
                SQL = SQL + " AND det.sales_stn_station_code = '" + StCode + "' ";
                SQL = SQL + " AND det.AD_BR_CODE = '" + BranchCode + "' ";
                SQL = SQL + " AND mas.RM_UM_UOM_CODE = uommas.RM_UM_UOM_CODE (+) ";
                SQL = SQL + " AND mas.RM_RMM_RM_CODE = det.RM_RMM_RM_CODE (+) ";
                SQL = SQL + " AND mas.rm_rmm_rm_code='" + ItemCode + "' ";

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

        #region "DML " 
        public string doSave(PhysicalStockTransferEntryRMEntity Entity, List<StockTransferGridEntity> objSTGridEntity, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber)
        {
            string sRetun = string.Empty;
            try
            {
                ocConn = new OracleConnection(Utilities.cnnstr);
                oTrns = new OracleHelper();
                SQLArray = new ArrayList();

                SQL = DynamicQry(Entity, objSTGridEntity);
                if (SQL != "CONTINUE")
                {
                    return SQL;
                }

                //sSQLArray.Add(SQL);
                sRetun = oTrns.SetTrans(Entity.UserName, Entity.FinYearID, Entity.Companycode, DateTime.Now, Entity.ModuleItemID, Entity.Code, Entity.DocAutogenerated, Entity.MachineName, Entity.CrudMode, SQLArray, bDocAutoGeneratedBrachWise, Entity.BranchCode, sAtuoGenBranchDocNumber);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return ex.Message;
            }
            return sRetun;
        }

        public string DynamicQry(PhysicalStockTransferEntryRMEntity Entity, List<StockTransferGridEntity> objSTGridEntity)
        {
            SQL = string.Empty;
            string sRetun = string.Empty;
            SQLArray = new ArrayList();


            try
            {
                switch (Entity.CrudMode)
                {
                    case "I":
                        SQL = " INSERT INTO RM_MAT_STK_TRANSFER_MASTER ( ";

                        SQL = SQL + "RM_MSTM_TRANSFER_NO, AD_FIN_FINYRID, RM_MSTM_TRANSFER_DATE,RM_MSTM_TRANSFER_DATE_TIME,  ";
                        SQL = SQL + "SA_STN_STATION_CODE_FROM, SA_STN_STATION_CODE_TO, RM_MSTM_FROM_PLANT,  ";
                        SQL = SQL + "RM_MSTM_TO_PLANT, RM_MSTM_DOC_STATUS, AD_CM_COMPANY_CODE,  ";

                        SQL = SQL + "RM_MSTM_REMARKS, RM_MSTM_CREATEDBY, RM_MSTM_CREATEDDATE,  ";
                        SQL = SQL + "RM_MSTM_UPDATEDBY, RM_MSTM_UPDATEDDATE, RM_MSTM_DELETESTATUS,  ";
                        SQL = SQL + "RM_MSTM_PREPARED_BY, RM_MSTM_RM_AMOUNT, RM_MSTM_TRANS_AMOUNT,  ";

                        SQL = SQL + "RM_MSTM_APPRV_STATUS, RM_MSTM_RECVD_BY, RM_MSTM_APPRVD_BY,";
                        SQL = SQL + "RM_MSTM_ENTRY_TYPE,RM_RMM_RM_CODE_WASH_SAND,RM_RMM_RM_CODE_WASH_SAND_RATE ,AD_BR_CODE_FROM, AD_BR_CODE_TO  )  ";

                        SQL = SQL + "VALUES (  ";

                        SQL = SQL + "'" + Entity.Code + "', '" + Entity.FinYearID + "', TO_DATE('" + Convert.ToDateTime(Entity.TRANSFER_DATE).ToString("dd-MMM-yyyy") + "', 'DD-MON-YYYY'), TO_DATE('" + Convert.ToDateTime(Entity.TRANSFER_DATE).ToString("dd-MMM-yyyy") + " " + Entity.EntryDateTimeText + "', 'DD-MON-YYYY HH12:MI:SS AM'), ";
                        SQL = SQL + "'" + Entity.STATION_CODE_FROM + "', '" + Entity.STATION_CODE_TO + "', '" + Entity.FROM_PLANT + "', ";
                        SQL = SQL + "'" + Entity.TO_PLANT + "', 'OPEN', '" + Entity.Companycode + "', ";

                        SQL = SQL + "'" + Entity.REMARKS + "', '" + Entity.UserName + "', '" + Convert.ToDateTime(Entity.CreatedDate).ToString("dd-MMM-yyyy") + "', ";
                        SQL = SQL + "'', '', '0', ";
                        SQL = SQL + "'" + Entity.VerifiedBy + "', '" + Convert.ToDouble(Entity.RM_AMOUNT) + "', '" + Convert.ToDouble(Entity.TRANS_AMOUNT) + "', ";

                        SQL = SQL + "'" + Entity.Approved + "', '','', '" + Entity.EntryType + "','" + Entity.WashSandCode + "','" + Entity.WashSandRate + "',";
                        SQL = SQL + "'" + Entity.FROM_BRANCH + "', '" + Entity.TO_BRANCH + "'";
                        SQL = SQL + " ) ";

                        SQLArray.Add(SQL);

                        SQL = DynamicQryDetails(Entity, objSTGridEntity);
                        if (SQL != "CONTINUE")
                        {
                            return SQL;
                        }

                        if (Entity.Approved == "Y")
                        {
                            SQL = ApprovalDetails(Entity.Code, Entity.FinYearID, Entity.EntryType);
                            if (SQL != "CONTINUE")
                            {
                                return SQL;
                            }
                        }

                        break;
                    case "U":
                        SQL = " UPDATE RM_MAT_STK_TRANSFER_MASTER ";
                        SQL = SQL + "SET ";
                        SQL = SQL + "SA_STN_STATION_CODE_FROM = '" + Entity.STATION_CODE_FROM + "', ";
                        SQL = SQL + "SA_STN_STATION_CODE_TO   = '" + Entity.STATION_CODE_TO + "', ";
                        SQL = SQL + "RM_MSTM_FROM_PLANT       = '" + Entity.FROM_PLANT + "', ";
                        SQL = SQL + "RM_MSTM_TO_PLANT         = '" + Entity.TO_PLANT + "', ";
                        SQL = SQL + "RM_MSTM_DOC_STATUS       = 'OPEN', ";
                        SQL = SQL + "RM_MSTM_REMARKS          = '" + Entity.REMARKS + "', ";
                        SQL = SQL + "RM_MSTM_UPDATEDBY        = '" + Entity.UserName + "', ";
                        SQL = SQL + "RM_MSTM_UPDATEDDATE      = '" + Convert.ToDateTime(Entity.UpdatedDate).ToString("dd-MMM-yyyy") + "', ";
                        SQL = SQL + "RM_MSTM_DELETESTATUS     = '0', ";
                        SQL = SQL + "RM_MSTM_PREPARED_BY      = '" + Entity.VerifiedBy + "', ";
                        SQL = SQL + "RM_MSTM_RM_AMOUNT        = '" + Convert.ToDouble(Entity.RM_AMOUNT) + "', ";
                        SQL = SQL + "RM_MSTM_TRANS_AMOUNT     = '" + Convert.ToDouble(Entity.TRANS_AMOUNT) + "', ";
                        SQL = SQL + "RM_MSTM_APPRV_STATUS     = '" + Entity.Approved + "', ";
                        SQL = SQL + "RM_MSTM_APPRVD_BY        = '" + Entity.ApprovedBy + "' ,";
                        SQL = SQL + "AD_BR_CODE_FROM          = '" + Entity.FROM_BRANCH + "' ,";
                        SQL = SQL + "AD_BR_CODE_TO            = '" + Entity.TO_BRANCH + "' ,";

                        SQL = SQL + "RM_MSTM_ENTRY_TYPE       = '" + Entity.EntryType + "', ";
                        SQL = SQL + "RM_RMM_RM_CODE_WASH_SAND     = '" + Entity.WashSandCode + "', ";
                        SQL = SQL + "RM_RMM_RM_CODE_WASH_SAND_RATE = " + Entity.WashSandRate + " ";


                        SQL = SQL + "WHERE ";
                        SQL = SQL + "RM_MSTM_TRANSFER_NO      = '" + Entity.Code + "' ";
                        SQL = SQL + "AND AD_FIN_FINYRID       = '" + Entity.FinYearID + "' ";



                        SQLArray.Add(SQL);

                        SQL = DynamicQryDetails(Entity, objSTGridEntity);
                        if (SQL != "CONTINUE")
                        {
                            return SQL;
                        }

                        if (Entity.Approved == "Y")
                        {
                            SQL = ApprovalDetails(Entity.Code, Entity.FinYearID, Entity.EntryType);
                            if (SQL != "CONTINUE")
                            {
                                return SQL;
                            }
                        }

                        break;
                    case "D":
                        SQL = " DELETE FROM RM_MAT_STK_TRANSFER_MASTER ";
                        SQL = SQL + " WHERE RM_MSTM_TRANSFER_NO='" + Entity.Code + "' ";
                        SQL = SQL + " AND AD_FIN_FINYRID='" + Entity.FinYearID + "' ";
                        SQL = SQL + " AND AD_CM_COMPANY_CODE='" + Entity.Companycode + "' ";

                        SQLArray.Add(SQL);

                        SQL = " DELETE FROM RM_MAT_STK_TRANSFER_DETL ";
                        SQL = SQL + " WHERE RM_MSTM_TRANSFER_NO='" + Entity.Code + "' ";
                        SQL = SQL + " AND AD_FIN_FINYRID='" + Entity.FinYearID + "' ";
                        SQL = SQL + " AND AD_CM_COMPANY_CODE='" + Entity.Companycode + "' ";

                        SQLArray.Add(SQL);

                        SQL = " DELETE FROM RM_MAT_STK_TRANSFER_TRIGGER ";
                        SQL = SQL + " WHERE RM_MSTM_TRANSFER_NO='" + Entity.Code + "' ";
                        SQL = SQL + " AND AD_FIN_FINYRID='" + Entity.FinYearID + "' ";

                        SQLArray.Add(SQL);

                        break;
                }
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return ex.Message;
            }
            return "CONTINUE";
        }

        public string DynamicQryDetails(PhysicalStockTransferEntryRMEntity Entity, List<StockTransferGridEntity> objSTGridEntity)
        {
            SQL = string.Empty;
            string sRetun = string.Empty;
            int iRow = 0;
            try
            {
                SQL = " DELETE FROM RM_MAT_STK_TRANSFER_DETL ";
                SQL = SQL + " WHERE RM_MSTM_TRANSFER_NO='" + Entity.Code + "'";
                SQL = SQL + " AND AD_FIN_FINYRID=" + Entity.FinYearID + "";

                SQLArray.Add(SQL);

                foreach (var Data in objSTGridEntity)
                {
                    if (!string.IsNullOrEmpty(Data.StRmCode))
                    {
                        ++iRow;
                        SQL = "";

                        SQL = " INSERT INTO RM_MAT_STK_TRANSFER_DETL ( ";

                        SQL = SQL + " RM_MSTM_TRANSFER_NO, AD_CM_COMPANY_CODE, AD_FIN_FINYRID,  ";
                        SQL = SQL + " RM_MSTD_DELETESTATUS, RM_MSTD_SL_NO, RM_RMM_RM_CODE,  ";
                        SQL = SQL + " RM_UM_UOM_CODE, RM_SM_SOURCE_CODE, RM_MSTD_IS_INTERNAL,  ";

                        SQL = SQL + " RM_MTRANSPO_ORDER_NO, RM_MTRANSPO_FIN_FINYRID, RM_POD_SL_NO_TRANS,  ";
                        SQL = SQL + " RM_MSTD_TRANSPORTER_CODE, RM_MSTD_TRNSPORTER_DOC_NO, RM_MSTD_TRANS_CHARGES,  ";
                        SQL = SQL + " FA_FAM_ASSET_CODE, RM_MSTD_VEHICLE_DESC, HR_EM_EMPLOYEE_CODE,  ";

                        SQL = SQL + " RM_MSTD_DRIVER_DESC, RM_MSTD_QUANTITY,  ";
                        SQL = SQL + " RM_MSTD_FRMSTN_RATE, RM_MSTD_AMOUNT, RM_MSTD_TRANS_AMOUNT,  ";
                        SQL = SQL + " RM_MSTD_RCV_DOC_NO, RM_MSTD_RCV_QTY, RM_MSTD_TOSTN_RATE,  ";

                        //SQL = SQL + " RM_MSTD_TRIPNO, RM_MSTD_REC_FLG, RM_MSTD_PE_DONE,  ";
                        //SQL = SQL + " RM_MSTD_PE_NO, RM_MSTD_PE_FINYR ";

                        SQL = SQL + " RM_MSTD_TRANS_VAT_TYPE_CODE,RM_MSTD_TRANS_VAT_PERCENTAGE)  ";
                        SQL = SQL + " VALUES (  ";

                        SQL = SQL + " '" + Entity.Code + "', '" + Entity.Companycode + "', '" + Entity.FinYearID + "','0',";

                        SQL = SQL + "  " + iRow + ", '" + Data.StRmCode + "', ";
                        SQL = SQL + " '" + Data.StUOMCode + "', '" + Entity.SOURCE_CODE + "', '" + Data.StDrivertype + "', ";

                        SQL = SQL + " '" + Data.StTransPo + "', '" + Data.StTransFinYr + "', '" + Data.StTransSlno + "', ";
                        SQL = SQL + " '" + Data.StTransCode + "', '" + Data.StTransDocNo + "', '" + Data.StTransRate + "', ";
                        SQL = SQL + " '" + Data.StAssetCode + "', '" + Data.StVehicleNo + "', '" + Data.StDriverCode + "', ";

                        SQL = SQL + " '" + Data.StDriverName + "', " + Convert.ToDouble(Data.StTransferQty) + ", ";
                        SQL = SQL + " '" + Convert.ToDouble(Data.StAvgPrice) + "', '" + Convert.ToDouble(Data.StTransferAmnt) + "', '" + Convert.ToDouble(Data.StTransPorterAmnt) + "', ";
                        SQL = SQL + " '', '" + Convert.ToDouble(Data.StTransferQty) + "', '" + Convert.ToDouble(Data.StToStnRate) + "', ";

                        //SQL = SQL + " '" + Entity + "', '" + Entity + "', '" + Entity + "', ";
                        //SQL = SQL + " '" + Entity + "', '" + Entity + "'  ";


                        SQL = SQL + " '" + Data.VatType + "','" + Data.VatRate + "' ) ";
                        SQLArray.Add(SQL);
                    }
                }


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return ex.Message;
            }
            return "CONTINUE";
        }

        public string ApprovalDetails(object Code, object FinId, string sEntryType)
        {
            SQL = string.Empty;
            string sRetun = string.Empty;

            try
            {
                SQL = "";
                SQL = SQL + " INSERT INTO RM_MAT_STK_TRANSFER_TRIGGER( ";
                SQL = SQL + " RM_MSTM_TRANSFER_NO, AD_FIN_FINYRID ,RM_MSTM_ENTRY_TYPE  ) ";
                SQL = SQL + " VALUES( ";
                SQL = SQL + " '" + Code + "', '" + FinId + "' , '" + sEntryType + "') ";

                SQLArray.Add(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return ex.Message;
            }
            return "CONTINUE";
        }

        #endregion 

        #region "Fill lookup" 
        public object FillSearchView(string type, string FromDate, string ToDate, string FilterType, object mngrclassobj)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO TRANSFERNO, RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID FINID, ";
                SQL = SQL + " TO_CHAR(RM_MSTM_TRANSFER_DATE,'DD-MON-YYYY') ENTRYDATE, SA_STN_STATION_CODE_FROM FROMSTATIONCODE,  ";
                SQL = SQL + " STFROM.SALES_STN_STATION_NAME FROMSTATION,RM_MSTM_REMARKS Remarks, ";
                SQL = SQL + " SA_STN_STATION_CODE_TO TOSTATIONCODE, STTO.SALES_STN_STATION_NAME TOSTATION,RM_MSTM_ENTRY_TYPE TYPE ,";
                SQL = SQL + " DECODE(RM_MSTM_PREPARED_BY,'','N','Y') VERIFIED, RM_MSTM_APPRV_STATUS APPROVED ";

                SQL = SQL + " FROM RM_MAT_STK_TRANSFER_MASTER, SL_STATION_MASTER STFROM, ";

                SQL = SQL + " SL_STATION_MASTER STTO ";
                SQL = SQL + " WHERE  SA_STN_STATION_CODE_FROM=STFROM.SALES_STN_STATION_CODE ";
                SQL = SQL + " AND SA_STN_STATION_CODE_TO=STTO.SALES_STN_STATION_CODE ";
                SQL = SQL + " AND RM_MSTM_TRANSFER_DATE BETWEEN '" + Convert.ToDateTime(FromDate).ToString("dd/MMM/yyyy") + "' AND '" + Convert.ToDateTime(ToDate).ToString("dd/MMM/yyyy") + "' ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";
                //if (type == "1")
                //{
                //    type = "NORMAL";
                //}
                //else
                //{
                //    type = "WASHING SAND";
                //}
                //SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_ENTRY_TYPE= '" + type + "' ";


                if (FilterType == "0")
                {
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_APPRV_STATUS='Y' ";
                }
                else if (FilterType == "1")
                {
                    SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_APPRV_STATUS='N' ";
                }

                SQL = SQL + " ORDER BY RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO DESC";

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


        public DataTable FillTrailerView()//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  SELECT   FA_FIXED_ASSET_MASTER.FA_FAM_REF_CODE REF_CODE ,  ";
                SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE code , FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_DESCRIPTION name,  ";
                SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_ID , FA_FIXED_ASSET_MASTER.FA_FAM_PLATENO  phone,RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE drcode,HR_EMP_EMPLOYEE_NAME drname  ";
                SQL = SQL + " FROM FA_FIXED_ASSET_MASTER , WS_ASSET_CATEGORY_TYPE,RM_DRIVER_MASTER_DETS,HR_EMPLOYEE_MASTER  ";
                SQL = SQL + " WHERE FA_FIXED_ASSET_MASTER.FA_FAM_VEHICLE_TYPE = WS_ASSET_CATEGORY_TYPE.SL_NO  ";
                SQL = SQL + " AND FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_STATUS ='A'  ";
                SQL = SQL + " AND  RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE = HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE(+)";
                SQL = SQL + " AND  RM_DRIVER_MASTER_DETS.FA_FAM_ASSET_CODE (+)= FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE";
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
        #endregion 

        #region "Fatch Data to For edit " 
        public DataSet GetTransferData(string TransferCode, string FinId)
        {
            DataSet dsReturn = new DataSet();
            ClsSqlhelper = new SqlHelper();

            try
            {
                SQL = " SELECT  ";

                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO TRANSID, RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID FINID, RM_MSTM_TRANSFER_DATE TRANSDATE,RM_MSTM_TRANSFER_DATE_TIME,    ";
                SQL = SQL + "  RM_MSTM_FROM_PLANT FROMPLANT,    ";
                SQL = SQL + " RM_MSTM_TO_PLANT TOPLANT, RM_MSTM_DOC_STATUS DOCSTATUS,   ";
                SQL = SQL + " RM_MSTM_REMARKS REMARKS, RM_MSTM_CREATEDBY CREATEDBY, RM_MSTM_CREATEDDATE CREATEDDATE,    ";
                SQL = SQL + " RM_MSTM_UPDATEDBY UPDATEDBY, RM_MSTM_UPDATEDDATE UPDATEDDATE,   ";
                SQL = SQL + " RM_MSTM_PREPARED_BY VERIFIEDBY, RM_MSTM_RM_AMOUNT RMAMOUNT, RM_MSTM_TRANS_AMOUNT TRANSAMT,    ";
                SQL = SQL + " RM_MSTM_APPRV_STATUS APPROVALSTATUS, RM_MSTM_APPRVD_BY APPROVEDBY,RM_MSTM_ENTRY_TYPE ,   ";
                SQL = SQL + " RM_RMM_RM_CODE_WASH_SAND,RM_RMM_RM_CODE_WASH_SAND_RATE,RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + "  AD_BR_CODE_FROM, From_ST_BR.AD_BR_NAME BRNAME_From, ";
                SQL = SQL + " AD_BR_CODE_TO ,To_ST_BR.AD_BR_NAME BRNAME_To, ";
                SQL = SQL + " SA_STN_STATION_CODE_FROM FROMSTCODE,From_ST_BR.SALES_STN_STATION_NAME STNAME_From, ";
                SQL = SQL + "  SA_STN_STATION_CODE_TO TOSTCODE,To_ST_BR.SALES_STN_STATION_NAME STNAME_To ";


                SQL = SQL + " FROM RM_MAT_STK_TRANSFER_MASTER ,RM_RAWMATERIAL_MASTER  ,";
                SQL = SQL + " SL_STATION_BRANCH_MAPP_DTLS_VW From_ST_BR, ";
                SQL = SQL + " SL_STATION_BRANCH_MAPP_DTLS_VW To_ST_BR  ";


                SQL = SQL + " where RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO='" + TransferCode + "' ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID='" + FinId + "' ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_RMM_RM_CODE_WASH_SAND = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  (+) ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM = From_ST_BR.SALES_STN_STATION_CODE  (+) ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.AD_BR_CODE_FROM = From_ST_BR.AD_BR_CODE  (+) ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_TO = To_ST_BR.SALES_STN_STATION_CODE  (+) ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.AD_BR_CODE_TO = To_ST_BR.AD_BR_CODE  (+)   ";

                dsReturn = ClsSqlhelper.GetDataset(SQL);
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
            return dsReturn;
        }

        public DataSet GetTransferWashingData(string TransferCode, string FinId)
        {
            DataSet dsReturn = new DataSet();
            ClsSqlhelper = new SqlHelper();

            try
            {
                SQL = " SELECT  ";

                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO TRANSID, RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID FINID, RM_MSTM_TRANSFER_DATE TRANSDATE,    ";
                SQL = SQL + " SA_STN_STATION_CODE_FROM FROMSTCODE, SA_STN_STATION_CODE_TO TOSTCODE, RM_MSTM_FROM_PLANT FROMPLANT,    ";
                SQL = SQL + " RM_MSTM_TO_PLANT TOPLANT, RM_MSTM_DOC_STATUS DOCSTATUS,   ";
                SQL = SQL + " RM_MSTM_REMARKS REMARKS, RM_MSTM_CREATEDBY CREATEDBY, RM_MSTM_CREATEDDATE CREATEDDATE,    ";
                SQL = SQL + " RM_MSTM_UPDATEDBY UPDATEDBY, RM_MSTM_UPDATEDDATE UPDATEDDATE,   ";
                SQL = SQL + " RM_MSTM_PREPARED_BY VERIFIEDBY, RM_MSTM_RM_AMOUNT RMAMOUNT, RM_MSTM_TRANS_AMOUNT TRANSAMT,    ";
                SQL = SQL + " RM_MSTM_APPRV_STATUS APPROVALSTATUS, RM_MSTM_APPRVD_BY APPROVEDBY  ,   ";
                SQL = SQL + "   RM_MSTM_ENTRY_TYPE, RM_RMM_RM_CODE_WASH_SAND,RM_RMM_RM_CODE_WASH_SAND_RATE,RM_RMM_RM_DESCRIPTION ";

                SQL = SQL + " FROM RM_MAT_STK_TRANSFER_MASTER,RM_RAWMATERIAL_MASTER ";
                SQL = SQL + " where RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO='" + TransferCode + "' ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID='" + FinId + "' ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_RMM_RM_CODE_WASH_SAND = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";

                dsReturn = ClsSqlhelper.GetDataset(SQL);
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
            return dsReturn;
        }

        public DataSet GetTransferDetailsData(string TransferCode, string FinId)
        {
            DataSet dsReturn = new DataSet();
            ClsSqlhelper = new SqlHelper();

            try
            {

                SQL = " SELECT  ";
                SQL = SQL + " RM_MSTM_TRANSFER_NO, AD_FIN_FINYRID,RM_MSTD_SL_NO,  ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE RMCODE, RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RMNAME,   ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_UM_UOM_CODE UOMCODE,RM_UM_UOM_DESC, RM_SM_SOURCE_CODE SOURCECODE, RM_MSTD_IS_INTERNAL INTERNALYN, ";
                SQL = SQL + " RM_MTRANSPO_ORDER_NO POORDERNO, RM_MTRANSPO_FIN_FINYRID POFINID, RM_POD_SL_NO_TRANS POSLNO,    ";
                SQL = SQL + " RM_MSTD_TRANSPORTER_CODE TRANSPOTERCODE, RM_VM_VENDOR_NAME TRANSPORTERNAME,   ";
                SQL = SQL + " RM_MSTD_TRNSPORTER_DOC_NO TRANSPORTERDOCNO, RM_MSTD_TRANS_CHARGES TRANSCHARGE,    ";
                SQL = SQL + " FA_FAM_ASSET_CODE ASSETCODE, RM_MSTD_VEHICLE_DESC VEHICLENAME, HR_EM_EMPLOYEE_CODE EMPCODE,    ";
                SQL = SQL + " RM_MSTD_DRIVER_DESC DRIVERNAME, RM_MSTD_QUANTITY QUANTITY,    ";
                SQL = SQL + " RM_MSTD_FRMSTN_RATE FRMSTNRATE, RM_MSTD_AMOUNT AMOUNT, RM_MSTD_TRANS_AMOUNT TRANSAMT,   ";
                SQL = SQL + " RM_MSTD_TOSTN_RATE TOSTNRATE,RM_MSTD_TRANS_VAT_TYPE_CODE,RM_MSTD_TRANS_VAT_PERCENTAGE   ";
                SQL = SQL + " from  ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL, RM_RAWMATERIAL_MASTER, RM_VENDOR_MASTER,RM_UOM_MASTER  ";
                SQL = SQL + " where RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANSPORTER_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+)   ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.RM_UM_UOM_CODE=RM_UOM_MASTER.RM_UM_UOM_CODE(+)   ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO='" + TransferCode + "'";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.AD_FIN_FINYRID=" + FinId + "";

                dsReturn = ClsSqlhelper.GetDataset(SQL);
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
            return dsReturn;
        }

        #endregion 

        #region "FEtch  PO Type " 
        public DataSet GetPOType(string PONO, string POFINID)
        {
            DataSet dsReturn = new DataSet();
            ClsSqlhelper = new SqlHelper();

            try
            {
                SQL = " SELECT RM_PO_PRICETYPE ";
                SQL = SQL + "FROM RM_PO_MASTER ";
                SQL = SQL + "WHERE RM_PO_PONO='" + PONO + "' ";
                SQL = SQL + "AND AD_FIN_FINYRID ='" + POFINID + "'";

                dsReturn = ClsSqlhelper.GetDataset(SQL);
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
            return dsReturn;
        }

        #endregion 

        #region "Print Data "
        public DataSet FetchVoucherPrintData(string sDocNo, object mngrclassobj)
        {
            DataSet dsReturn = new DataSet();
            ClsSqlhelper = new SqlHelper();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID,";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_DATE, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM, FROM_STATION.SALES_STN_STATION_NAME STATION_NAME_FROM ,";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_TO, TO_STATION.SALES_STN_STATION_NAME STATION_NAME_TO ,";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_FROM_PLANT,  ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TO_PLANT, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_REMARKS, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_RM_AMOUNT, RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANS_AMOUNT, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_CREATEDBY, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_PREPARED_BY,";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_APPRV_STATUS,RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_APPRVD_BY, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_RECVD_BY, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_SL_NO, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE, RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_UM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL. RM_SM_SOURCE_CODE,RM_SOURCE_MASTER.RM_SM_SOURCE_DESC , ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_IS_INTERNAL, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MTRANSPO_ORDER_NO, RM_MAT_STK_TRANSFER_DETL.RM_MTRANSPO_FIN_FINYRID, RM_MAT_STK_TRANSFER_DETL.RM_POD_SL_NO_TRANS, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANSPORTER_CODE, RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SUPPNAME, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRNSPORTER_DOC_NO, RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANS_CHARGES, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.FA_FAM_ASSET_CODE, RM_MAT_STK_TRANSFER_DETL.RM_MSTD_VEHICLE_DESC, RM_MAT_STK_TRANSFER_DETL.HR_EM_EMPLOYEE_CODE, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_DRIVER_DESC, RM_MAT_STK_TRANSFER_DETL.RM_MSTD_ISS_DOC_NO, RM_MAT_STK_TRANSFER_DETL.RM_MSTD_QUANTITY, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_FRMSTN_RATE, RM_MAT_STK_TRANSFER_DETL.RM_MSTD_AMOUNT, RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANS_AMOUNT, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_RCV_DOC_NO, RM_MAT_STK_TRANSFER_DETL.RM_MSTD_RCV_QTY, RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TOSTN_RATE, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRIPNO, RM_MAT_STK_TRANSFER_DETL.RM_MSTD_REC_FLG, RM_MAT_STK_TRANSFER_DETL.RM_MSTD_PE_DONE, ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL.RM_MSTD_PE_NO, RM_MAT_STK_TRANSFER_DETL.RM_MSTD_PE_FINYR ,";
                SQL = SQL + "     MASTER_BRANCH.AD_BR_CODE MASTER_BRANCH_CODE, ";
                SQL = SQL + "          MASTER_BRANCH.AD_BR_NAME MASTER_BRANCH_NAME, ";
                SQL = SQL + "          MASTER_BRANCH.AD_BR_DOC_PREFIX MASTER_BRANCHDOC_PREFIX, ";
                SQL = SQL + "          MASTER_BRANCH.AD_BR_POBOX MASTER_BRANCH_POBOX, ";
                SQL = SQL + "          MASTER_BRANCH.AD_BR_ADDRESS MASTER_BRANCH_ADDRESS, ";
                SQL = SQL + "          MASTER_BRANCH.AD_BR_CITY MASTER_BRANCH_CITY, ";
                SQL = SQL + "          MASTER_BRANCH.AD_BR_PHONE MASTER_BRANCH_PHONE, ";
                SQL = SQL + "          MASTER_BRANCH.AD_BR_FAX MASTER_BRANCH_FAX, ";
                SQL = SQL + "          MASTER_BRANCH.AD_BR_SPONSER_NAME MASTER_BRANCH_SPONSER_NAME, ";
                SQL = SQL + "          MASTER_BRANCH.AD_BR_TRADE_LICENSE_NO MASTER_BRANCH_TRADE_LICENSE_NO, ";
                SQL = SQL + "          MASTER_BRANCH.AD_BR_EMAIL_ID MASTER_BRANCH_EMAIL_ID, ";
                SQL = SQL + "          MASTER_BRANCH.AD_BR_WEB_SITE MASTER_BRANCH_WEB_SITE, ";
                SQL = SQL + "          MASTER_BRANCH.AD_BR_TAX_VAT_REG_NUMBER MASTER_BRANCH_VAT_REG_NUMBER ";
                SQL = SQL + " FROM ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER,";
                SQL = SQL + " RM_MAT_STK_TRANSFER_DETL,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER ,";
                SQL = SQL + " SL_STATION_MASTER FROM_STATION,";
                SQL = SQL + " SL_STATION_MASTER TO_STATION,";
                //SQL = SQL + " TECH_PLANT_MASTER FROM_PLANT, ";
                //SQL = SQL + " TECH_PLANT_MASTER TO_PLANT, ";
                SQL = SQL + " RM_SOURCE_MASTER,";
                SQL = SQL + " RM_VENDOR_MASTER,";
                SQL = SQL + " RM_UOM_MASTER ,ad_branch_master MASTER_BRANCH  ";
                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO = RM_MAT_STK_TRANSFER_DETL.RM_MSTM_TRANSFER_NO ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID = RM_MAT_STK_TRANSFER_DETL.AD_FIN_FINYRID ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_FROM = FROM_STATION.SALES_STN_STATION_CODE ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.SA_STN_STATION_CODE_TO = TO_STATION.SALES_STN_STATION_CODE ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.AD_BR_CODE_FROM =  MASTER_BRANCH.AD_BR_CODE(+) ";
                ////SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_FROM_PLANT =FROM_PLANT.TECH_PLM_PLANT_CODE (+) ";
                ////SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TO_PLANT =TO_PLANT.TECH_PLM_PLANT_CODE (+) ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.RM_MSTD_TRANSPORTER_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE (+) ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.RM_SM_SOURCE_CODE = RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + " AND RM_MAT_STK_TRANSFER_DETL.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";

                SQL = SQL + " AND RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO ='" + sDocNo + "' AND RM_MAT_STK_TRANSFER_MASTER.AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";
                SQL = SQL + " order by RM_MAT_STK_TRANSFER_MASTER.RM_MSTM_TRANSFER_NO ,RM_MAT_STK_TRANSFER_DETL.RM_MSTD_SL_NO";
                dsReturn = ClsSqlhelper.GetDataset(SQL);
                dsReturn.Tables[0].TableName = "RM_STOCKTRANSFER_REPORT_PRINT";
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
            return dsReturn;
        }

        #endregion 
    }

    #region " Entity" 
    public class StockTransferGridEntity
    {

        public string StRmCode
        {
            get;
            set;
        }

        public string StRmName
        {
            get;
            set;
        }


        public string StUOMCode
        {
            get;
            set;
        }

        public string StUOMDesc
        {
            get;
            set;
        }


        public string StDrivertype
        {
            get;
            set;
        }

        public string StTransPo
        {
            get;
            set;
        }

        public string StTransFinYr
        {
            get;
            set;
        }


        public string StTransSlno
        {
            get;
            set;
        }

        public string StTransCode
        {
            get;
            set;
        }


        public string StTransName
        {
            get;
            set;
        }

        public double StTransRate
        {
            get;
            set;
        }


        public string StTransDocNo
        {
            get;
            set;
        }

        public string StDriverCode
        {
            get;
            set;
        }

        public string StDriverName
        {
            get;
            set;
        }
        public string StAssetCode
        {
            get;
            set;
        }

        public string StVehicleNo
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


        public double StQtyOnhand
        {
            get;
            set;
        }

        public double StAvgPrice
        {
            get;
            set;
        }


        public double StTransferQty
        {
            get;
            set;
        }

        public double StTransferAmnt
        {
            get;
            set;
        }

        public double StTransPorterAmnt
        {
            get;
            set;
        }

        public double StToStnRate
        {
            get;
            set;
        }
        public double StTotalAmnt
        {
            get;
            set;
        }
    }

    public class PhysicalStockTransferEntryRMEntity : BaseEntity.BaseEntity
    {
        public object TRANSFER_DATE { get; set; }
        //  public object RMCode { get; set; }
        public object STATION_CODE_FROM { get; set; }
        public object STATION_CODE_TO { get; set; }
        public object FROM_PLANT { get; set; }
        public object TO_PLANT { get; set; }
        public object FROM_BRANCH { get; set; }
        public object TO_BRANCH { get; set; }
        public object REMARKS { get; set; }
        public object RM_AMOUNT { get; set; }
        public object TRANS_AMOUNT { get; set; }
        public object APPRV_STATUS { get; set; }
        public object UOM_CODE { get; set; }
        public object SOURCE_CODE { get; set; }
        public string EntryType
        {
            get;
            set;
        }


        public string WashSandCode
        {
            get;
            set;
        }

        public double WashSandRate
        {
            get;
            set;
        }
        public string EntryDateTimeText
        {
            get;
            set;
        }

    }
    #endregion

}
