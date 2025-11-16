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
using Oracle.ManagedDataAccess.Client; using Oracle.ManagedDataAccess.Types;
using System.Collections;
using System.Collections.Generic;
// jomoy eidited 11 mar 2016 for toll
namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class RateCardTransporterLogic
    {
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;
        public DataTable GetStation()
        {


            DataTable dtStation = new DataTable();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  SALES_STN_STATION_CODE STN_CODE,  SALES_STN_STATION_NAME STN_NAME ";
                SQL = SQL + " FROM SL_STATION_MASTER ";
                SQL = SQL + " WHERE SALES_RAW_MATERIAL_STATION = 'Y'";
                SQL = SQL + " order By  SALES_STN_STATION_CODE ";

                dtStation = clsSQLHelper.GetDataTableByCommand(SQL);


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);

            }

            return dtStation;
        }

        public DataTable GetLastSeqNo()
        {
            DataTable dtSq = new DataTable();
            SqlHelper sqhelpObj = new SqlHelper();
            try
            {
                SQL = "";
                SQL = "SELECT MAX(RM_RS_SEQ_NO) FROM RM_TRANSRATE_SHEET";
                dtSq = sqhelpObj.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
            }
            return dtSq;
        }
        public DataTable FillView(string col, string gtcode, string sUserName)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                if (col == "2")
                {
                    SQL = " SELECT RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code , RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION Name, '' Id,    RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE ,  RM_UOM_MASTER.RM_UM_UOM_DESC    ";
                    SQL = SQL + " FROM RM_RAWMATERIAL_MASTER , RM_UOM_MASTER  ";
                    SQL = SQL + "  where RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE  =  RM_UOM_MASTER.RM_UM_UOM_CODE (+)";
                    SQL = SQL + " And RM_RAWMATERIAL_MASTER.RM_RMM_RM_STATUS ='A'";

                    SQL = SQL + " ORDER BY   RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC ";
                }

                else if (col == "4")
                {
                    SQL = " SELECT ";
                    SQL = SQL + "RM_SM_SOURCE_CODE Code , RM_SM_SOURCE_DESC Name ,'' Id ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC ";
                    SQL = SQL + "FROM RM_SOURCE_MASTER ";

                    if (gtcode != "")
                    {
                        SQL = SQL + " where  RM_SM_SOURCE_CODE ='" + gtcode + "'";
                    }

                    SQL = SQL + " ORDER BY RM_SM_SOURCE_CODE  ASC ";
                }
                else if (col == "6")
                {
                    SQL = " SELECT RM_VM_VENDOR_CODE CODE, RM_VM_VENDOR_NAME NAME, '' Id ,''RM_UM_UOM_CODE ,  '' RM_UM_UOM_DESC  ";
                    SQL = SQL + " FROM RM_VENDOR_MASTER ";
                    SQL = SQL + " ORDER BY  RM_VM_VENDOR_NAME  DESC    ";
                }
                if (col == "HISTORYRM")
                {
                    SQL = " SELECT DISTINCT RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code , RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION Name, '' Id,   ";
                    SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE ,  RM_UOM_MASTER.RM_UM_UOM_DESC      ";
                    SQL = SQL + "FROM RM_RAWMATERIAL_MASTER , RM_UOM_MASTER, RM_TRANSRATE_SHEET    ";
                    SQL = SQL + "where RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE  =  RM_UOM_MASTER.RM_UM_UOM_CODE   ";
                    SQL = SQL + "And RM_RAWMATERIAL_MASTER.RM_RMM_RM_STATUS ='A'  ";
                    SQL = SQL + "AND RM_TRANSRATE_SHEET.RM_UM_UOM_CODE  =  RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                    SQL = SQL + "AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE = RM_TRANSRATE_SHEET.RM_RMM_RM_CODE ";
                    SQL = SQL + " ORDER BY  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC  ";


                }
                else if (col == "HISTORYVENDOR")
                {
                    SQL = "  SELECT DISTINCT RM_VENDOR_MASTER.RM_VM_VENDOR_CODE CODE, RM_VM_VENDOR_NAME NAME, '' Id ,''RM_UM_UOM_CODE ,   ";
                    SQL = SQL + " '' RM_UM_UOM_DESC   FROM RM_VENDOR_MASTER, RM_TRANSRATE_SHEET  ";
                    SQL = SQL + "  WHERE RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE  =  RM_VENDOR_MASTER.RM_VM_VENDOR_CODE (+)  ";
                    SQL = SQL + "  ORDER BY  RM_VM_VENDOR_NAME  DESC  ";

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


     
        public DataTable FetchData(string sType, string sCode, string sLASTDATA)
        {
            DataTable dtData = new DataTable();
            SqlHelper sqHelpObj = new SqlHelper();
            try
            {
                SQL = "";
                SQL = SQL + " SELECT DISTINCT ";
                SQL = SQL + " RM_TRANSRATE_SHEET.RM_RS_SEQ_NO, RM_TRANSRATE_SHEET.RM_RMM_RM_CODE,RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_TRANSRATE_SHEET.RM_SM_SOURCE_CODE,RM_SOURCE_MASTER.RM_SM_SOURCE_DESC, ";
                SQL = SQL + " RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE,RM_VENDOR_MASTER.RM_VM_VENDOR_NAME,";
                SQL = SQL + " RM_TRANSRATE_SHEET.RM_UM_UOM_CODE,RM_UOM_MASTER.RM_UM_UOM_DESC,";
                SQL = SQL + "   RM_TRS_EFFDATE  ";
                SQL = SQL + " FROM ";
                SQL = SQL + " RM_TRANSRATE_SHEET,RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_SOURCE_MASTER, RM_VENDOR_MASTER, RM_UOM_MASTER";
                SQL = SQL + " WHERE";
                SQL = SQL + " RM_TRANSRATE_SHEET.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " and RM_SOURCE_MASTER.RM_SM_SOURCE_CODE= RM_TRANSRATE_SHEET.RM_SM_SOURCE_CODE";
                SQL = SQL + " and RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " and RM_UOM_MASTER.RM_UM_UOM_CODE=RM_TRANSRATE_SHEET.RM_UM_UOM_CODE";

                if (sType == "RM")
                {
                    if (!string.IsNullOrEmpty(sCode))
                    {
                        SQL = SQL + " And RM_TRANSRATE_SHEET.RM_RMM_RM_CODE = '" + sCode + "'";
                    }
                }
                else
                {
                    if (!string.IsNullOrEmpty(sCode))
                    {
                        SQL = SQL + " And RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE = '" + sCode + "'";
                    }
                }

                if (sLASTDATA == "YES")
                {
                   

                    SQL = SQL + " AND RM_TRANSRATE_SHEET.RM_RMM_RM_CODE||RM_TRANSRATE_SHEET.RM_SM_SOURCE_CODE||RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE||RM_TRANSRATE_SHEET.RM_UM_UOM_CODE||RM_TRS_EFFDATE||SALES_STN_STATION_CODE IN";
                    SQL = SQL + " (SELECT ";
                    SQL = SQL + " RM_RMM_RM_CODE||RM_SM_SOURCE_CODE||RM_VM_VENDOR_CODE||RM_UM_UOM_CODE||MAX(RM_TRS_EFFDATE)||SALES_STN_STATION_CODE ";
                    SQL = SQL + " FROM ";
                    SQL = SQL + " RM_TRANSRATE_SHEET  ";

                    if (sType == "RM")
                    {
                        if (!string.IsNullOrEmpty(sCode))
                        {

                            SQL = SQL + "WHERE RM_TRANSRATE_SHEET.RM_RMM_RM_CODE = '" + sCode + "'";
                        }
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(sCode))
                        {
                            SQL = SQL + " WHERE  RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE = '" + sCode + "'";
                        }
                    }

                    SQL = SQL + " GROUP BY RM_RMM_RM_CODE, RM_SM_SOURCE_CODE, ";
                    SQL = SQL + " RM_VM_VENDOR_CODE,RM_UM_UOM_CODE,SALES_STN_STATION_CODE) ";

                }
                 

                SQL = SQL + " ORDER BY  RM_TRS_EFFDATE , RM_TRANSRATE_SHEET.RM_RMM_RM_CODE ,RM_TRANSRATE_SHEET.RM_SM_SOURCE_CODE,";
                SQL = SQL + " RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE ";
                dtData = sqHelpObj.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {

            }
            return dtData;
        }

        public DataTable FetchStationColumnsData(string sType, string sCode, string sLASTDATA)
        {
            DataTable dtData = new DataTable();
            SqlHelper sqHelpObj = new SqlHelper();
            try
            {
                SQL = "";
                SQL = SQL + " SELECT DISTINCT ";
                SQL = SQL + " RM_TRANSRATE_SHEET.RM_RS_SEQ_NO, RM_TRANSRATE_SHEET.RM_RMM_RM_CODE,RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, RM_TRANSRATE_SHEET.RM_SM_SOURCE_CODE,RM_SOURCE_MASTER.RM_SM_SOURCE_DESC, ";
                SQL = SQL + " RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE,RM_VENDOR_MASTER.RM_VM_VENDOR_NAME,RM_TRANSRATE_SHEET.RM_UM_UOM_CODE,RM_UOM_MASTER.RM_UM_UOM_DESC,";
                SQL = SQL + " RM_TRS_PRICE,   RM_TRS_QTY_TOLL_RATE, RM_TRS_TRIP_TOLL_RATE , ( RM_TRS_PRICE +RM_TRS_QTY_TOLL_RATE + RM_TRS_TRIP_TOLL_RATE ) TOTAL_RATE,";
                SQL = SQL + "   RM_TRS_EFFDATE,SALES_STN_STATION_CODE ";
                SQL = SQL + " FROM ";
                SQL = SQL + " RM_TRANSRATE_SHEET,RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_SOURCE_MASTER, RM_VENDOR_MASTER, RM_UOM_MASTER";
                SQL = SQL + " WHERE";
                SQL = SQL + " RM_TRANSRATE_SHEET.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " and RM_SOURCE_MASTER.RM_SM_SOURCE_CODE= RM_TRANSRATE_SHEET.RM_SM_SOURCE_CODE";
                SQL = SQL + " and RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE=RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                SQL = SQL + " and RM_UOM_MASTER.RM_UM_UOM_CODE=RM_TRANSRATE_SHEET.RM_UM_UOM_CODE";

                if (sType == "RM")
                {
                    if (!string.IsNullOrEmpty(sCode))
                    {
                        SQL = SQL + " And RM_TRANSRATE_SHEET.RM_RMM_RM_CODE = '" + sCode + "'";
                    }
                }
                else
                {
                    if (!string.IsNullOrEmpty(sCode))
                    {
                        SQL = SQL + " And RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE = '" + sCode + "'";
                    }
                }
                if (sLASTDATA == "YES")
                {
                    SQL = SQL + " AND RM_TRANSRATE_SHEET.RM_RMM_RM_CODE||RM_TRANSRATE_SHEET.RM_SM_SOURCE_CODE||RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE||RM_TRANSRATE_SHEET.RM_UM_UOM_CODE||RM_TRS_EFFDATE||SALES_STN_STATION_CODE IN";
                    SQL = SQL + " (SELECT ";
                    SQL = SQL + " RM_RMM_RM_CODE||RM_SM_SOURCE_CODE||RM_VM_VENDOR_CODE||RM_UM_UOM_CODE||MAX(RM_TRS_EFFDATE)||SALES_STN_STATION_CODE ";
                    SQL = SQL + " FROM ";
                    SQL = SQL + " RM_TRANSRATE_SHEET  ";

                    if (sType == "RM")
                    {
                        if (!string.IsNullOrEmpty(sCode))
                        {
                            SQL = SQL + "WHERE RM_TRANSRATE_SHEET.RM_RMM_RM_CODE = '" + sCode + "'";
                        }
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(sCode))
                        {
                            SQL = SQL + " WHERE  RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE = '" + sCode + "'";
                        }
                    }

                    SQL = SQL + " GROUP BY RM_RMM_RM_CODE, RM_SM_SOURCE_CODE, ";
                    SQL = SQL + " RM_VM_VENDOR_CODE,RM_UM_UOM_CODE,SALES_STN_STATION_CODE) ";

                }

                SQL = SQL + " ORDER BY  RM_TRS_EFFDATE DESC , RM_TRANSRATE_SHEET.RM_RMM_RM_CODE ,RM_TRANSRATE_SHEET.RM_SM_SOURCE_CODE,";
                SQL = SQL + " RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE, SALES_STN_STATION_CODE";
                dtData = sqHelpObj.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {

            }
            return dtData;
        }


        public string InsertSQL(List<RateCardTransporterDetailsEntity> objEntity, object mngrClassObj)
        {
            string sReturn = string.Empty;
            Int16 i = 0;
            SessionManager mngrclass = (SessionManager)mngrClassObj;
            OracleHelper oHelpObj = new OracleHelper();
            try
            {
                i = 0;
                sSQLArray.Clear();
                foreach (var Data in objEntity)
                {

                    // insertion section 
                    if (Data.sRMCode != null)
                    {
                        ++i;
                        SQL = "";
                        if (checkData(Data.sRMCode, Data.sSourceCode, Data.sTransporterCode, Data.sEffDate) == true)
                        {
                            SQL = " INSERT INTO RM_TRANSRATE_SHEET (";
                            SQL = SQL + "   RM_RMM_RM_CODE, RM_UM_UOM_CODE, RM_SM_SOURCE_CODE, ";
                            SQL = SQL + " RM_VM_VENDOR_CODE, RM_TRS_EFFDATE, ";
                            SQL = SQL + " RM_TRS_PRICE,   RM_TRS_QTY_TOLL_RATE, RM_TRS_TRIP_TOLL_RATE ,  ";
                              SQL = SQL + "   SALES_STN_STATION_CODE, AD_CM_COMPANY_CODE, ";
                            SQL = SQL + " RM_RS_DELETESTATUS) ";
                            SQL = SQL + " VALUES ('" + Data.sRMCode + "','" + Data.sUOMcode + "' , '" + Data.sSourceCode + "',";
                            SQL = SQL + " '" + Data.sTransporterCode + "','" + Convert.ToDateTime(Data.sEffDate).ToString("dd-MMM-yyyy") + "' ,";
                            SQL = SQL + "" + Data.sTransRate + "," + Data.dTollQtyRate + " ," + Data.dTollTripRate + ",";
                            SQL = SQL + " '" + Data.sStnCode + "' ,'" + mngrclass.CompanyCode + "' ,0 )";


                            sSQLArray.Add(SQL);
                        }
                        else
                        {
                            SQL = " UPDATE  RM_TRANSRATE_SHEET SET   RM_TRS_PRICE  = " + Data.sTransRate + " ,";
                            SQL = SQL + "    RM_TRS_QTY_TOLL_RATE = " + Data.dTollQtyRate + " , RM_TRS_TRIP_TOLL_RATE = " + Data.dTollTripRate + " ";
                            SQL = SQL + "  where   RM_RMM_RM_CODE = '" + Data.sRMCode + "' AND  RM_SM_SOURCE_CODE = '" + Data.sSourceCode + "' ";
                            SQL = SQL + " AND RM_VM_VENDOR_CODE = '" + Data.sTransporterCode + "' AND RM_TRS_EFFDATE = '" + Convert.ToDateTime(Data.sEffDate).ToString("dd-MMM-yyyy") + "' ";
                            SQL = SQL + " AND SALES_STN_STATION_CODE = '" + Data.sStnCode + "'";


                  

                            sSQLArray.Add(SQL);
                        }


                    }
                }

                sReturn = oHelpObj.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMTRNSRT", "", false, Environment.MachineName, "I", sSQLArray);

            }
            catch (Exception ex)
            {
            }
            return sReturn;
        }

        public string DeleteSQL(List<RateCardTransporterDetailsEntity> objEntity, object mngrClassObj)
        {
            string sReturn = string.Empty;
            Int16 i = 0;
            SessionManager mngrclass = (SessionManager)mngrClassObj;
            OracleHelper oHelpObj = new OracleHelper();
            try
            {
                i = 0;
                sSQLArray.Clear();
                foreach (var Data in objEntity)
                {
                    if (Data.sRMCode != null)
                    {
                        ++i;
                        SQL = "";
                        SQL = SQL + " DELETE FROM RM_TRANSRATE_SHEET ";
                        SQL = SQL + " WHERE ";
                        SQL = SQL + " RM_RMM_RM_CODE = '" + Data.sRMCode + "'";
                        SQL = SQL + " AND RM_SM_SOURCE_CODE = '" + Data.sSourceCode + "'";
                        SQL = SQL + " AND RM_VM_VENDOR_CODE = '" + Data.sTransporterCode + "'";
                        SQL = SQL + " AND RM_TRS_EFFDATE = '" + Convert.ToDateTime(Data.sEffDate).ToString("dd-MMM-yyyy") + "'";

                        sSQLArray.Add(SQL);
                    }
                }

                sReturn = oHelpObj.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMTRNSRT", "", false, Environment.MachineName, "D", sSQLArray);

            }
            catch (Exception ex)
            {
            }
            return sReturn;
        }

        public bool checkData(List<RateCardTransporterDetailsEntity> objEntity)
        {
            bool chkData = true;
            char sFirstItm = 'Y';
            DataTable dtData = new DataTable();
            SqlHelper sqHelpObj = new SqlHelper();
            string TMSQL = string.Empty;
            int i = 0;
            try
            {
                //TMSQL = "";
                //TMSQL = TMSQL + " AND RM_TRANSRATE_SHEET.RM_RS_SEQ_NO NOT IN ( ";
                //foreach (var Data in objEntity)
                //{
                //    if (sFirstItm == 'Y')
                //    {
                //        TMSQL = TMSQL + " " + Data.sSeqNo + "";
                //        sFirstItm = 'N';
                //    }
                //    else
                //    {
                //        TMSQL = TMSQL + " ," + Data.sSeqNo + "";
                //    }
                //}
                //TMSQL = TMSQL + ")";

                SQL = "";
                SQL = SQL + " SELECT DISTINCT ";
                SQL = SQL + " RM_TRANSRATE_SHEET.RM_RMM_RM_CODE, RM_TRANSRATE_SHEET.RM_SM_SOURCE_CODE, ";
                SQL = SQL + " RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE,";
                SQL = SQL + " RM_TRS_EFFDATE ";
                SQL = SQL + " FROM RM_TRANSRATE_SHEET ";
                foreach (var Data in objEntity)
                {
                    if (Data.sRMCode != null)
                    {
                        if (i == 0)
                        {
                            SQL = SQL + " WHERE RM_RMM_RM_CODE = '" + Data.sRMCode + "' AND RM_SM_SOURCE_CODE = '" + Data.sSourceCode + "'";
                            SQL = SQL + " AND RM_VM_VENDOR_CODE = '" + Data.sTransporterCode + "' AND RM_TRS_EFFDATE = '" + Convert.ToDateTime(Data.sEffDate).ToString("dd-MMM-yyyy") + "' ";
                            //SQL = SQL + TMSQL;
                        }
                        else
                        {
                            SQL = SQL + "OR( ";
                            SQL = SQL + " RM_RMM_RM_CODE = '" + Data.sRMCode + "' AND RM_SM_SOURCE_CODE = '" + Data.sSourceCode + "'";
                            SQL = SQL + " AND RM_VM_VENDOR_CODE = '" + Data.sTransporterCode + "' AND RM_TRS_EFFDATE = '" + Convert.ToDateTime(Data.sEffDate).ToString("dd-MMM-yyyy") + "' ";
                            SQL = SQL + ") ";
                            //SQL = SQL + TMSQL;
                        }
                        i++;
                    }
                }

                dtData = sqHelpObj.GetDataTableByCommand(SQL);
                if (dtData.Rows.Count <= 0)
                {
                    chkData = true;
                }
                else
                {
                    chkData = false;
                }

            }
            catch (Exception ex)
            {

            }
            return chkData;
        }

        public bool checkData(string sRMCode, string sSourceCode, string sTransporterCode, string sEffDate)
        {
            bool chkData = true;

            DataTable dtData = new DataTable();
            SqlHelper sqHelpObj = new SqlHelper();
            string TMSQL = string.Empty;
            int i = 0;
            try
            {



                SQL = SQL + " SELECT RM_TRANSRATE_SHEET.RM_RMM_RM_CODE, RM_TRANSRATE_SHEET.RM_SM_SOURCE_CODE, ";
                SQL = SQL + " RM_TRANSRATE_SHEET.RM_VM_VENDOR_CODE,";
                SQL = SQL + " RM_TRS_EFFDATE ";
                SQL = SQL + " FROM RM_TRANSRATE_SHEET ";
                SQL = SQL + " WHERE RM_RMM_RM_CODE = '" + sRMCode + "' AND RM_SM_SOURCE_CODE = '" + sSourceCode + "'";
                SQL = SQL + " AND RM_VM_VENDOR_CODE = '" + sTransporterCode + "' AND RM_TRS_EFFDATE = '" + Convert.ToDateTime(sEffDate).ToString("dd-MMM-yyyy") + "' ";



                dtData = sqHelpObj.GetDataTableByCommand(SQL);
                if (dtData.Rows.Count <= 0)
                {
                    chkData = true;
                }
                else
                {
                    chkData = false;
                }

            }
            catch (Exception ex)
            {

            }
            return chkData;
        }


    }
    public class RateCardTransporterDetailsEntity
    {

        public double sSeqNo
        {
            get;
            set;
        }
        public string sRMCode
        {
            get;
            set;
        }
        public string sSourceCode
        {
            get;
            set;
        }
        public string sTransporterCode
        {
            get;
            set;
        }
        public string sUOMcode
        {
            get;
            set;
        }
        public string sEffDate
        {
            get;
            set;
        }
        public double sTransRate
        {
            get;
            set;
        }

        public double dTollTripRate
        {
            get;
            set;
        }

        public double dTollQtyRate
        {
            get;
            set;
        }

        public string sStnCode
        {
            get;
            set;
        }
    }
}
