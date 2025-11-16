using System;
using System.Data;
using System.Collections;
//using FarPoint.Web.Spread; 
using AccosoftUtilities;
using AccosoftLogWriter;
using Oracle.ManagedDataAccess.Client;
using AccosoftNineteenCDL;

/// <summary>
/// Created By      :    Sreeraj
/// Created On      :    25-01-2013
/// Description     :    WorkshopInventory_ItemHistory
/// </summary>
/// 

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public  class ItemMovementRawmaterialLogic
    {

         static string sConString = Utilities.cnnstr;
        OracleConnection ocConn = new OracleConnection(sConString.ToString()); 
    
      //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public ItemMovementRawmaterialLogic()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        #region "FillData"

        public DataTable FillViewItem(string BrCode,string stCode,string Hidstnvalue)
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  SELECT distinct ( RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE) ITEM_CODE, RM_RMM_RM_DESCRIPTION ITEM_NAME, ";
                SQL = SQL + "        ''   PARTNO,   ";
                SQL = SQL + "        RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE INVTYPE, RM_UOM_MASTER.RM_UM_UOM_DESC UOMDESC, ";
                SQL = SQL + "        RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE UOMCODE  ";
                SQL = SQL + "     FROM  RM_RAWMATERIAL_MASTER ,  RM_UOM_MASTER,RM_RAWMATERIAL_DETAILS ";
                SQL = SQL + "     WHERE     ";
                SQL = SQL + "       RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + "        AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE = RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE ";

                 
                SQL = SQL + " AND RM_RAWMATERIAL_DETAILS.AD_BR_CODE in( '" + BrCode + "')";
                SQL = SQL + " AND RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE in( '" + stCode + "')";
                

                SQL = SQL + " ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";


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

        public DataSet FillStationCombo(object mngrclassobj)
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                if (mngrclass.UserName == "ADMIN")
                {
                    // SQL = GetStationSelectionQrs();
                    SQL = "  SELECT SALES_STN_STATION_CODE Code,SALES_STN_STATION_NAME Name  FROM  ";
                    SQL = SQL + "  SL_STATION_MASTER WHERE  SALES_STN_DELETESTATUS=0";
                    SQL = SQL + " AND  SALES_RAW_MATERIAL_STATION='Y'";

                    //if (!string.IsNullOrEmpty(strCondition))
                    //{
                    //    SQL = SQL + " And" + strCondition;
                    //}
                    SQL = SQL + "  ORDER BY SALES_STN_STATION_NAME ";

                }
                else
                {
                    SQL = "  SELECT SALES_STN_STATION_CODE Code,SALES_STN_STATION_NAME Name  FROM  ";
                    SQL = SQL + "  SL_STATION_MASTER WHERE  SALES_STN_DELETESTATUS=0";
                    SQL = SQL + "  AND  SALES_RAW_MATERIAL_STATION='Y'";
                    SQL = SQL + "  AND SALES_STN_STATION_CODE IN ( SELECT     SALES_STN_STATION_CODE FROM    WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + mngrclass.UserName + "')";


                    //if (!string.IsNullOrEmpty(strCondition))
                    //{
                    //    SQL = SQL + " And" + strCondition;
                    //}
                    SQL = SQL + "  ORDER BY SALES_STN_STATION_NAME ";
                    // SQL = GetStationSelectionQrs("  SALES_STN_STATION_CODE IN ( SELECT     SALES_STN_STATION_CODE FROM    WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + msUserId + "'");
                }





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
            return dsData;
        }

        public DataSet FillStationsForLoopingItem(object mngrclassobj,string sStationCode,string sBrCode, string sAllStation )
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                if (mngrclass.UserName == "ADMIN")
                {

                    SQL = "  SELECT SALES_STN_STATION_CODE Code,SALES_STN_STATION_NAME Name,AD_BR_CODE BR_CODE FROM  ";
                    SQL = SQL + "  SL_STATION_MASTER WHERE  SALES_STN_DELETESTATUS=0";
                    SQL = SQL + " AND  SALES_RAW_MATERIAL_STATION='Y'";

                    //if (sAllStation == "N")
                    //{
                    SQL = SQL + " AND  SALES_STN_STATION_CODE in('" + sStationCode + "')";
                    SQL = SQL + " AND  AD_BR_CODE in('" + sBrCode + "')";
                    //} 
                    SQL = SQL + "  ORDER BY AD_BR_CODE,SALES_STN_STATION_NAME ";

                }
                else
                {
                    SQL = "  SELECT SALES_STN_STATION_CODE Code,SALES_STN_STATION_NAME Name,AD_BR_CODE BR_CODE  FROM  ";
                    SQL = SQL + "  SL_STATION_MASTER WHERE  SALES_STN_DELETESTATUS=0";
                    SQL = SQL + "  AND  SALES_RAW_MATERIAL_STATION='Y'";

                    //if (sAllStation == "N")
                    //{
                    SQL = SQL + " AND  SALES_STN_STATION_CODE in('" + sStationCode + "')";
                    SQL = SQL + " AND  AD_BR_CODE in('" + sBrCode + "')";
                    //} 
                    SQL = SQL + "  AND SALES_STN_STATION_CODE IN ";
                    SQL = SQL + " ( SELECT     SALES_STN_STATION_CODE FROM    WS_GRANTED_STATION_USERS ";
                    SQL = SQL + "WHERE    AD_UM_USERID   ='" + mngrclass.UserName + "')";


                    SQL = SQL + "  ORDER BY AD_BR_CODE,SALES_STN_STATION_NAME ";

                }





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
            return dsData;
        }


        #endregion

        #region "FetchData"

        public DataSet FetchItemPriceDetailsData(string iCode,string stCode,string BrCode,string HdChkValue)
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();
                //SessionManager mngrclass = (SessionManager)mngrclassobj;
 
                             
                SQL = " SELECT  0  CURRSTOCK, ";
                SQL = SQL + "  0   RVAMOUNT,   ";
                SQL = SQL + "  RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE ,SL_STATION_MASTER.SALES_STN_STATION_NAME,";

                SQL = SQL + " 'CURRENT_STOCK' DOCTYPE ,"; 
               SQL = SQL + " 0  ISSUEQTY,0 ISSUERATE, 0 ISSUEAMOUNT , 0  SELLRATE , 0  SELLAMT , 0 TOTALQTY  , 0 TOTAMT ";
                   
                SQL = SQL + " FROM  RM_RAWMATERIAL_DETAILS,SL_STATION_MASTER ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + "    RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";

                SQL = SQL + "  AND RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE = '" + iCode + "'";

                //if (HdChkValue=="N")
                //{
                
                    SQL = SQL + "  AND RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE in( '" + stCode + "')";
                    SQL = SQL + "  AND RM_RAWMATERIAL_DETAILS.AD_BR_CODE in( '" + BrCode + "')";
                //}

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
            return dsData;
        }

        public DataSet FetchOpeningStockData(string iCode, string stCode, string BrCode,object mngrclassobj,string HdChkValue)
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "   SELECT 'OPENBAL' DOCNO, '" + mngrclass.FinYearStartDate + "' DOCDATE,   'OPEN STOCK' DOCTYPE , RM_OB_QTY  RVQTY, RM_OB_PRICE RVRATE, RM_OB_AMOUNT  RVAMOUNT, ";
                SQL = SQL + "  0 ISSUEQTY, 0 ISSUERATE, 0 ISSUEAMOUNT,  0 SELLRATE, 0  SELLAMT, 0  TOTALQTY ,0 TOTAMT  ";
                SQL = SQL + "  FROM RM_OPEN_BALANCES WHERE  RM_RMM_RM_CODE = '" + iCode + "' ";
                
                //if(HdChkValue=="N")
                //{
                SQL = SQL + "  AND  SALES_STN_STATION_CODE  in( '" + stCode + "' )";
                SQL = SQL + "  AND  AD_BR_CODE  in ('" + BrCode + "') ";
                //}

                SQL = SQL + "  AND AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";

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
            return dsData;
        }

        public DataSet FetchStockLedgerDataPreviousReceipt(string iCode, string stCode, string brCode, DateTime fromDate, object mngrclassobj, string HdChkValue)
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT  ";
                SQL = SQL + "   'PREVRV' DOCNO, '" + fromDate + "' DOCDATE,   'PREVRV' DOCTYPE , ";
                SQL = SQL + " nvl(SUM(RM_STKL_RECD_QTY) ,0) RVQTY , nvl( SUM(RM_STKL_RECD_AMT),0) RVAMOUNT   ,  ";
                SQL = SQL + "   case  when SUM(RM_STKL_RECD_QTY) > 0 then  (SUM(RM_STKL_RECD_AMT)  /SUM(RM_STKL_RECD_QTY))  else 0 end  RVRATE , ";
                SQL = SQL + "     0 ISSUEQTY, 0 ISSUERATE, 0 ISSUEAMOUNT,  0 SELLRATE, 0  SELLAMT, 0  TOTALQTY ,0 TOTAMT ";
                SQL = SQL + " FROM RM_STOCK_LEDGER  ";
                SQL = SQL + " WHERE AD_FIN_FINYRID  = " + mngrclass.FinYearID + " ";

                //if(HdChkValue=="N")
                //{
                    SQL = SQL + "  AND  SALES_STN_STATION_CODE in('" + stCode + "')";
                    SQL = SQL + "  AND  AD_BR_CODE in('" + brCode + "')";
                //}

                SQL = SQL + "   AND   RM_IM_ITEM_CODE  ='" + iCode + "'";
                SQL = SQL + " AND RM_STKL_TRANS_DATE  <  '" + fromDate.ToString("dd-MMM-yyyy") + "'";


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
            return dsData;
        }

        public DataSet FetchStockLedgerDataPreviousISSUE(string iCode, string stCode, string BrCode, DateTime fromDate, object mngrclassobj, string HdChkValue)
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT  ";
                SQL = SQL + "   'PREVISSUE' DOCNO, '" + fromDate + "' DOCDATE,   'PREVISSUE' DOCTYPE ,  ";
                SQL = SQL + "  0 RVQTY , 0 RVAMOUNT,  ";
                SQL = SQL + "  0 RVRATE , ";
                SQL = SQL + "  nvl(SUM(  RM_STKL_ISSUE_QTY ),0 )ISSUEQTY,   case when  SUM(  RM_STKL_ISSUE_QTY )  > 0 then  SUM(RM_STKL_ISSUE_AMOUNT) / SUM(  RM_STKL_ISSUE_QTY )   else 0 end   ISSUERATE, ";
                SQL = SQL + " nvl(SUM(RM_STKL_ISSUE_AMOUNT),0)  ISSUEAMOUNT,  0 SELLRATE, 0  SELLAMT, 0  TOTALQTY ,0 TOTAMT ";
                SQL = SQL + " FROM RM_STOCK_LEDGER   ";
                SQL = SQL + " WHERE AD_FIN_FINYRID  = " + mngrclass.FinYearID + " ";

                //if(HdChkValue=="N")
                //{
                    SQL = SQL + " AND  SALES_STN_STATION_CODE in('" + stCode + "')";
                    SQL = SQL + " AND  AD_BR_CODE in('" + BrCode + "')";
                //}

                SQL = SQL + " AND   RM_IM_ITEM_CODE  ='" + iCode+ "'";
                SQL = SQL + " AND RM_STKL_TRANS_DATE  <  '" + fromDate.ToString("dd-MMM-yyyy") + "'";


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
            return dsData;
        }
          
        public DataSet FetchStockLedgerDataReceiptIssueAndOtherDataSummary(string iCode, string stCode,string BrCode, DateTime fromDate, DateTime toDate, object mngrclassobj, string HdChkValue,string ExcludePhysicalstock)
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;



                SQL = " SELECT    DOCNO  ,   DOCDATE ,          ";
                SQL = SQL + "     DOCTYPE  ,    LPOORFIXEDASSET,    ";
                SQL = SQL + "     SUPPLIERNAME,     RVQTY ,    ";
                SQL = SQL + "     RVRATE ,   RVAMOUNT ,         ";
                SQL = SQL + "     ISSUEQTY ,   ISSUERATE ,    ";
                SQL = SQL + "     ISSUEAMOUNT ,  SALEQTY,   SELLRATE ,    ";
                SQL = SQL + "     SELLAMT,    TOTALQTY ,  TOTAMT  ,        ";
                SQL = SQL + "     RM_MRM_RECEIPT_NO  ";
                SQL = SQL + "  from ( ";

                SQL = SQL + "   SELECT   RM_STOCK_LEDGER.RM_STKL_DOC_NO DOCNO  , RM_STKL_TRANS_DATE DOCDATE ,          ";
                SQL = SQL + "         RM_STKL_TRANS_TYPE DOCTYPE  , RM_STKL_DOC_NO  LPOORFIXEDASSET,    ";
                SQL = SQL + "         RM_VM_VENDOR_NAME  SUPPLIERNAME,      SUM(  RM_STKL_RECD_QTY) RVQTY ,    ";
                SQL = SQL + "         0  RVRATE , SUM(  RM_STKL_RECD_AMT )  RVAMOUNT ,         ";
                SQL = SQL + "        SUM(   RM_STKL_ISSUE_QTY) ISSUEQTY , 0 ISSUERATE ,    ";
                SQL = SQL + "        SUM(   RM_STKL_ISSUE_AMOUNT) ISSUEAMOUNT ,  0 SALEQTY ,    0 SELLRATE ,    ";
                SQL = SQL + "        0 SELLAMT,   0  TOTALQTY ,0 TOTAMT  ,        ";
                SQL = SQL + "         '' RM_MRM_RECEIPT_NO   ";
                SQL = SQL + "    FROM RM_STOCK_LEDGER, RM_RECEIPT_APPL_MASTER, RM_VENDOR_MASTER   ";
                SQL = SQL + "   WHERE RM_STOCK_LEDGER.RM_STKL_DOC_NO = RM_RECEIPT_APPL_MASTER.RM_MRMA_APPROVAL_NO(+)   ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.AD_FIN_FINYRID = RM_RECEIPT_APPL_MASTER.AD_FIN_FINYRID(+)   ";
                SQL = SQL + "     AND RM_RECEIPT_APPL_MASTER.RM_MRMA_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE(+)   ";
                SQL = SQL + "     AND RM_STKL_TRANS_TYPE IN ('RECEIPT')   ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.SALES_STN_STATION_CODE in( '" + stCode + "')  ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.AD_BR_CODE in('" + BrCode + "')  ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.RM_IM_ITEM_CODE = '" + iCode + "'  ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.RM_STKL_TRANS_DATE ";
                SQL = SQL + " BETWEEN '" + fromDate.ToString("dd-MMM-yyyy") + "' AND   '" + toDate.ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + "     GROUP BY  RM_STOCK_LEDGER.RM_STKL_DOC_NO ,RM_STKL_TRANS_DATE ,RM_STKL_TRANS_TYPE ,RM_VM_VENDOR_NAME ";


                // trasfer recevpt
                
                SQL = SQL + " UNION ALL  ";

                SQL = SQL + "   SELECT   RM_STOCK_LEDGER.RM_STKL_DOC_NO DOCNO  , RM_STKL_TRANS_DATE DOCDATE ,           ";
                SQL = SQL + "         RM_STKL_TRANS_TYPE DOCTYPE  , RM_STKL_DOC_NO  LPOORFIXEDASSET,     ";
                SQL = SQL + "         'MATERIAL CONVERSION'   SUPPLIERNAME,      SUM(  RM_STKL_RECD_QTY) RVQTY ,     ";
                SQL = SQL + "         0  RVRATE , SUM(  RM_STKL_RECD_AMT )  RVAMOUNT ,          ";
                SQL = SQL + "        SUM(   RM_STKL_ISSUE_QTY) ISSUEQTY , 0 ISSUERATE ,     ";
                SQL = SQL + "        SUM(   RM_STKL_ISSUE_AMOUNT) ISSUEAMOUNT ,      0 SALEQTY ,  0 SELLRATE ,     ";
                SQL = SQL + "         0 SELLAMT,   0  TOTALQTY ,0 TOTAMT  ,         ";
                SQL = SQL + "         '' RM_MRM_RECEIPT_NO    ";
                SQL = SQL + "    FROM RM_STOCK_LEDGER        ";
                SQL = SQL + "   WHERE  ";
                SQL = SQL + "      RM_STKL_RECD_QTY >0 and RM_STKL_TRANS_TYPE IN ('STK_TRANS')  ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.SALES_STN_STATION_CODE in( '" + stCode + "')  ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.AD_BR_CODE in('" + BrCode + "')  ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.RM_IM_ITEM_CODE = '" + iCode + "'  ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.RM_STKL_TRANS_DATE ";
                SQL = SQL + "    BETWEEN '" + fromDate.ToString("dd-MMM-yyyy") + "' AND   '" + toDate.ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + "     GROUP BY  RM_STOCK_LEDGER.RM_STKL_DOC_NO ,RM_STKL_TRANS_DATE ,RM_STKL_TRANS_TYPE  ";

                // trasfer issue 
                SQL = SQL + " UNION ALL  ";

                SQL = SQL + "  SELECT   RM_STOCK_LEDGER.RM_STKL_DOC_NO DOCNO  , RM_STKL_TRANS_DATE DOCDATE ,           ";
                SQL = SQL + "         RM_STKL_TRANS_TYPE DOCTYPE  , RM_STKL_DOC_NO  LPOORFIXEDASSET,     ";
                SQL = SQL + "         'MATERIAL CONVERSION'   SUPPLIERNAME,      SUM(  RM_STKL_RECD_QTY) RVQTY ,     ";
                SQL = SQL + "         0  RVRATE , SUM(  RM_STKL_RECD_AMT )  RVAMOUNT ,          ";
                SQL = SQL + "        SUM(   RM_STKL_ISSUE_QTY) ISSUEQTY , 0 ISSUERATE ,     ";
                SQL = SQL + "        SUM(   RM_STKL_ISSUE_AMOUNT) ISSUEAMOUNT ,    0 SALEQTY ,    0 SELLRATE ,     ";
                SQL = SQL + "         0 SELLAMT,   0  TOTALQTY ,0 TOTAMT  ,         ";
                SQL = SQL + "         '' RM_MRM_RECEIPT_NO    ";
                SQL = SQL + "    FROM RM_STOCK_LEDGER        ";
                SQL = SQL + "   WHERE  ";
                SQL = SQL + "      RM_STKL_ISSUE_QTY >0 and RM_STKL_TRANS_TYPE IN ('STK_TRANS')  ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.SALES_STN_STATION_CODE in('" + stCode + "')  ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.AD_BR_CODE in('" + BrCode + "')  ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.RM_IM_ITEM_CODE = '" + iCode + "'  ";
                SQL = SQL + "     AND RM_STOCK_LEDGER.RM_STKL_TRANS_DATE ";
                SQL = SQL + "    BETWEEN '" + fromDate.ToString("dd-MMM-yyyy") + "' AND   '" + toDate.ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + "     GROUP BY  RM_STOCK_LEDGER.RM_STKL_DOC_NO ,RM_STKL_TRANS_DATE ,RM_STKL_TRANS_TYPE  ";



                SQL = SQL + " UNION ALL  ";

                //////SQL = SQL + "    SELECT  ";
                //////SQL = SQL + "         TS_DELIVERY_NOTE.PR_DLYN_VOUCHER_NO  DOCNO ,   ts_invoice_master.PROD_INVM_INVOICE_DATE,       ";
                //////SQL = SQL + "        'DN' DOCTYPE , TS_DELIVERY_NOTE.SALES_STN_STATION_CODE,            ";
                //////SQL = SQL + "         SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_NAME SUPPLIERNAME , 0 RVQTY, 0 RVRATE , 0 RVAMOUNT  ,     ";
                //////SQL = SQL + "        sum( PROD_DLYN_QTY_APP_CONVERTED)   ISSUEQTY ,  0 ISSUERATE , 0  ISSUEAMOUNT ,   ";
                //////SQL = SQL + "         0 SELLRATE ,  SUM ( PR_DLYN_UNIT_PRICE * PROD_DLYN_QTY_APP_CONVERTED )   SELLAMT,   0  TOTALQTY ,0 TOTAMT ,        ";
                //////SQL = SQL + "         TS_DELIVERY_NOTE_DETAILS.RM_RMM_RM_CODE     ";
                //////SQL = SQL + "    FROM TS_DELIVERY_NOTE,  ts_invoice_master, ";
                //////SQL = SQL + "         TS_DELIVERY_NOTE_DETAILS,   ";
                //////SQL = SQL + "         RM_RAWMATERIAL_DETAILS,   ";
                //////SQL = SQL + "         RM_UOM_CATEGORY_MASTER,   ";
                //////SQL = SQL + "         SL_CUSTOMER_MASTER   ";
                //////SQL = SQL + "   WHERE TS_DELIVERY_NOTE.PR_DLYN_DN_NO = TS_DELIVERY_NOTE_DETAILS.PR_DLYN_DN_NO   ";
                //////SQL = SQL + "     AND TS_DELIVERY_NOTE.AD_FIN_FINYRID = TS_DELIVERY_NOTE_DETAILS.AD_FIN_FINYRID   ";
                //////SQL = SQL + "     and TS_DELIVERY_NOTE.PR_DLYN_VOUCHER_NO  = ts_invoice_master.PROD_INVM_INVOICE_NO ";
                //////SQL = SQL + "     and TS_DELIVERY_NOTE.PR_DLYN_VOUCHER_FINYRID  = ts_invoice_master.AD_FIN_FINYRID ";
                //////SQL = SQL + "     AND TS_DELIVERY_NOTE_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE   ";
                //////SQL = SQL + "     AND TS_DELIVERY_NOTE.SALES_STN_STATION_CODE = RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE   ";
                //////SQL = SQL + "     AND TS_DELIVERY_NOTE_DETAILS.RM_UOM_CAT_CODE = RM_UOM_CATEGORY_MASTER.RM_UOM_CAT_CODE   ";
                //////SQL = SQL + "     AND TS_DELIVERY_NOTE.SALES_CUS_CUSTOMER_CODE = SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_CODE(+)   ";
                //////SQL = SQL + "     AND TS_DELIVERY_NOTE.PR_DLYN_EX_WORKS NOT IN ('EXTRANS')   ";

                //////SQL = SQL + "   and  ts_invoice_master.PROD_INVM_INVOICE_DATE BETWEEN '" + fromDate.ToString("dd-MMM-yyyy") + "' AND   '" + toDate.ToString("dd-MMM-yyyy") + "'";

                //////SQL = SQL + "     AND TS_DELIVERY_NOTE.SALES_STN_STATION_CODE = '" + stCode + "'";
                //////SQL = SQL + "     AND TS_DELIVERY_NOTE_DETAILS.RM_RMM_RM_CODE = '" + iCode + "'";

                //////SQL = SQL + "     GROUP BY  ";
                //////SQL = SQL + "         TS_DELIVERY_NOTE.PR_DLYN_VOUCHER_NO    ,   ts_invoice_master.PROD_INVM_INVOICE_DATE,       ";
                //////SQL = SQL + "         TS_DELIVERY_NOTE.SALES_STN_STATION_CODE,            ";
                //////SQL = SQL + "         SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_NAME ,TS_DELIVERY_NOTE_DETAILS.RM_RMM_RM_CODE   ";



                SQL = SQL + "     SELECT   ";
                SQL = SQL + "          TO_CHAR(TS_INVOICE_MASTER.PROD_INVM_INVOICE_NO)    DOCNO ,   ts_invoice_master.PROD_INVM_INVOICE_DATE,        ";
                SQL = SQL + "        'DN' DOCTYPE , TS_INVOICE_MASTER.SALES_STN_STATION_CODE,             ";
                SQL = SQL + "         SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_NAME SUPPLIERNAME , 0 RVQTY, 0 RVRATE , 0 RVAMOUNT  ,      ";
                SQL = SQL + "        0 ISSUEQTY ,  0 ISSUERATE , 0  ISSUEAMOUNT ,    ";
                SQL = SQL + "        sum( PROD_INVD_SALE_QTY_IN_DN)     SALEQTY ,  0 SELLRATE ,  SUM ( PROD_INVD_SALE_AMOUNT )   SELLAMT,  0  TOTALQTY ,0 TOTAMT ,         ";
                SQL = SQL + "          TS_INVOICE_DETAILS.RM_RMM_RM_CODE  RM_RMM_RM_CODE      ";
                SQL = SQL + "    FROM    TS_INVOICE_MASTER,  ";
                SQL = SQL + "         TS_INVOICE_DETAILS,    ";
                SQL = SQL + "         RM_RAWMATERIAL_DETAILS,    ";
                SQL = SQL + "         RM_UOM_CATEGORY_MASTER,    ";
                SQL = SQL + "         SL_CUSTOMER_MASTER    ";
                SQL = SQL + "   WHERE  TS_INVOICE_DETAILS.PROD_INVM_INVOICE_NO  = ts_invoice_master.PROD_INVM_INVOICE_NO  ";
                SQL = SQL + "     and TS_INVOICE_DETAILS.AD_FIN_FINYRID  = ts_invoice_master.AD_FIN_FINYRID  ";
                SQL = SQL + "     AND TS_INVOICE_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE      ";
                SQL = SQL + "     AND TS_INVOICE_DETAILS.RM_UOM_CAT_CODE = RM_UOM_CATEGORY_MASTER.RM_UOM_CAT_CODE    ";
                SQL = SQL + "     AND ts_invoice_master.SALES_CUS_CUSTOMER_CODE = SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_CODE(+)    ";
                SQL = SQL + "     AND TS_INVOICE_DETAILS.PROD_INVD_EX_WORKS NOT IN ('EXTRANS')    ";
                SQL = SQL + "   and  ts_invoice_master.PROD_INVM_INVOICE_DATE BETWEEN    '" + fromDate.ToString("dd-MMM-yyyy") + "' AND   '" + toDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "     AND TS_INVOICE_MASTER.SALES_STN_STATION_CODE   in( '" + stCode + "')";
                SQL = SQL + "     AND TS_INVOICE_MASTER.AD_BR_CODE in('" + BrCode + "') ";
                SQL = SQL + "     AND TS_INVOICE_DETAILS.RM_RMM_RM_CODE   = '" + iCode + "'";
                SQL = SQL + "     GROUP BY   ";
                SQL = SQL + "       TS_INVOICE_MASTER.PROD_INVM_INVOICE_NO      ,   ts_invoice_master.PROD_INVM_INVOICE_DATE,        ";
                SQL = SQL + "         TS_INVOICE_MASTER.SALES_STN_STATION_CODE,             ";
                SQL = SQL + "         SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_NAME ,TS_INVOICE_DETAILS.RM_RMM_RM_CODE    ";



                if (ExcludePhysicalstock == "NO")
                {


                    ////SQL = SQL + "UNION ALL  ";

                    ////SQL = SQL + "   SELECT  ";
                    ////SQL = SQL + "        RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO   DOCNO ,   RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_DATE,        ";
                    ////SQL = SQL + "        'PHYSTK' DOCTYPE , RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE,             ";
                    ////SQL = SQL + "        '' SUPPLIERNAME ,  ";
                    ////SQL = SQL + "     SUM( CASE WHEN RM_PSED_VARIANCE > 0  THEN RM_PSED_VARIANCE   ELSE 0 END )  RVQTY , 0 RVRATE , 0 RVAMOUNT  ,      ";
                    ////SQL = SQL + "     SUM( CASE WHEN RM_PSED_VARIANCE < 0  THEN RM_PSED_VARIANCE *-1  ELSE 0 END )    ISSUEQTY , 0 ISSUERATE , 0 ISSUEAMOUNT ,    ";
                    ////SQL = SQL + "        0 SELLRATE , 0 SELLAMT,   0  TOTALQTY ,0 TOTAMT ,         ";
                    ////SQL = SQL + "        RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE    ";
                    ////SQL = SQL + "        FROM    RM_PHYSICAL_STOCK_MASTER ";
                    ////SQL = SQL + "        INNER JOIN ";
                    ////SQL = SQL + "        RM_PHYSICAL_STOCK_DETAILS ";
                    ////SQL = SQL + "        ON (RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO =      RM_PHYSICAL_STOCK_DETAILS.RM_PSEM_ENTRY_NO)     ";
                    ////SQL = SQL + "     AND RM_PHYSICAL_STOCK_MASTER.AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
                    ////SQL = SQL + "     AND  RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE = '" + stCode + "'";
                    ////SQL = SQL + "     AND  RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE = '" + iCode + "'";
                    ////SQL = SQL + "     AND RM_PSEM_ENTRY_DATE BETWEEN '" + fromDate.ToString("dd-MMM-yyyy") + "' AND   '" + toDate.ToString("dd-MMM-yyyy") + "'  ";

                    ////SQL = SQL + "         GROUP BY  RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO     ,   RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_DATE, ";
                    ////SQL = SQL + "         RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE , ";
                    ////SQL = SQL + "          RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE";
                    ///


                    // trasfer issue 
                    SQL = SQL + " UNION ALL  ";

                    SQL = SQL + "  SELECT   RM_STOCK_LEDGER.RM_STKL_DOC_NO DOCNO  , RM_STKL_TRANS_DATE DOCDATE ,           ";
                    SQL = SQL + "         RM_STKL_TRANS_TYPE DOCTYPE  , RM_STKL_DOC_NO  LPOORFIXEDASSET,     ";
                    SQL = SQL + "         'PHYSTK'   SUPPLIERNAME,      SUM(  RM_STKL_RECD_QTY) RVQTY ,     ";
                    SQL = SQL + "         0  RVRATE , SUM(  RM_STKL_RECD_AMT )  RVAMOUNT ,          ";
                    SQL = SQL + "        SUM(   RM_STKL_ISSUE_QTY) ISSUEQTY , 0 ISSUERATE ,     ";
                    SQL = SQL + "        SUM(   RM_STKL_ISSUE_AMOUNT) ISSUEAMOUNT ,       0 SELLRATE ,     ";
                    SQL = SQL + "       0 SALEQTY ,  0 SELLAMT,   0  TOTALQTY ,0 TOTAMT  ,         ";
                    SQL = SQL + "         '' RM_MRM_RECEIPT_NO    ";
                    SQL = SQL + "    FROM RM_STOCK_LEDGER        ";
                    SQL = SQL + "   WHERE  ";
                    SQL = SQL + "      RM_STKL_ISSUE_QTY >0 and RM_STKL_TRANS_TYPE IN ('PHYSTK')  ";
                    SQL = SQL + "     AND RM_STOCK_LEDGER.AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";
                    SQL = SQL + "     AND RM_STOCK_LEDGER.SALES_STN_STATION_CODE in( '" + stCode + "')  ";
                    SQL = SQL + "     AND RM_STOCK_LEDGER.AD_BR_CODE in('" + BrCode + "')   ";
                    SQL = SQL + "     AND RM_STOCK_LEDGER.RM_IM_ITEM_CODE = '" + iCode + "'  ";
                    SQL = SQL + "     AND RM_STOCK_LEDGER.RM_STKL_TRANS_DATE ";
                    SQL = SQL + "    BETWEEN '" + fromDate.ToString("dd-MMM-yyyy") + "' AND   '" + toDate.ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + "     GROUP BY  RM_STOCK_LEDGER.RM_STKL_DOC_NO ,RM_STKL_TRANS_DATE ,RM_STKL_TRANS_TYPE  ";

                    SQL = SQL + " UNION ALL  ";

                    SQL = SQL + "   SELECT   RM_STOCK_LEDGER.RM_STKL_DOC_NO DOCNO  , RM_STKL_TRANS_DATE DOCDATE ,           ";
                    SQL = SQL + "         RM_STKL_TRANS_TYPE DOCTYPE  , RM_STKL_DOC_NO  LPOORFIXEDASSET,     ";
                    SQL = SQL + "         'PHYSTK'   SUPPLIERNAME,      SUM(  RM_STKL_RECD_QTY) RVQTY ,     ";
                    SQL = SQL + "         0  RVRATE , SUM(  RM_STKL_RECD_AMT )  RVAMOUNT ,          ";
                    SQL = SQL + "        SUM(   RM_STKL_ISSUE_QTY) ISSUEQTY , 0 ISSUERATE ,     ";
                    SQL = SQL + "        SUM(   RM_STKL_ISSUE_AMOUNT) ISSUEAMOUNT ,       0 SELLRATE ,     ";
                    SQL = SQL + "         0 SALEQTY  , 0 SELLAMT,   0  TOTALQTY ,0 TOTAMT  ,         ";
                    SQL = SQL + "         '' RM_MRM_RECEIPT_NO    ";
                    SQL = SQL + "    FROM RM_STOCK_LEDGER        ";
                    SQL = SQL + "   WHERE  ";
                    SQL = SQL + "      RM_STKL_RECD_QTY >0 and RM_STKL_TRANS_TYPE IN ('PHYSTK')  ";
                    SQL = SQL + "     AND RM_STOCK_LEDGER.AD_FIN_FINYRID = " + mngrclass.FinYearID + " ";
                    SQL = SQL + "     AND RM_STOCK_LEDGER.SALES_STN_STATION_CODE in( '" + stCode + "')  ";
                    SQL = SQL + "     AND RM_STOCK_LEDGER.AD_BR_CODE in('" + BrCode + "')  ";
                    SQL = SQL + "     AND RM_STOCK_LEDGER.RM_IM_ITEM_CODE = '" + iCode + "'  ";
                    SQL = SQL + "     AND RM_STOCK_LEDGER.RM_STKL_TRANS_DATE ";
                    SQL = SQL + "    BETWEEN '" + fromDate.ToString("dd-MMM-yyyy") + "' AND   '" + toDate.ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + "     GROUP BY  RM_STOCK_LEDGER.RM_STKL_DOC_NO ,RM_STKL_TRANS_DATE ,RM_STKL_TRANS_TYPE  ";



                }

                SQL = SQL + " ) order by DOCDATE asc ";





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
            return dsData;
        }

        #endregion
    }
}
