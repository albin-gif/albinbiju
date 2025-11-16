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
using System.Globalization;
//  jom,y  / 05 JUL 2020 

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class RawMaterialQtyCurrentStockRMX
    {
       
            static string sConString = Utilities.cnnstr;
            //   string scon = sConString;

            OracleConnection ocConn = new OracleConnection(sConString.ToString());
            String SQL = string.Empty;
            ArrayList sSQLArray = new ArrayList();

            #region "poroperty " 

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

            public string ReceiptType
            {
                get;
                set;
            }

        #endregion

        #region "Generals function " 
        public DataTable FillStation ( )
        {

            DataTable dtReturn = new DataTable();

            try
            {

                String SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                //SQL = " SELECT   sales_stn_station_code, sales_stn_station_name";
                //SQL = SQL + "    FROM SL_STATION_MASTER";
                //SQL = SQL + "    WHERE  SALES_RAW_MATERIAL_STATION  =  'Y'";
                //SQL = SQL + "    ORDER BY sales_stn_station_code ASC ";


                //SQL = "  SELECT SALES_STN_STATION_CODE  ,SALES_STN_STATION_NAME    FROM  ";
                //SQL = SQL + "  SL_STATION_MASTER WHERE   SALES_STN_STATION_STATUS ='A' AND SALES_BLOCK_STATION ='Y' ";
                //SQL = SQL + "    ORDER BY sales_stn_station_code ASC ";

                //SQL = " SELECT   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE, SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME, ";
                //SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME , ";
                //SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.GL_COSM_ACCOUNT_CODE  ";
                //SQL = SQL + " FROM ";
                //SQL = SQL + "   SL_STATION_BRANCH_MAPP_DTLS_VW WHERE   ";
                //SQL = SQL + "    SALES_STATION_BRANCH_STATUS_AI ='A' and SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_RAW_MATERIAL_STATION ='Y'    ";
                //SQL = SQL + "  and  (AD_BR_READYMIX_VISIBILE_YN = 'Y' OR AD_BR_BLOCK_VISIBILE_YN = 'Y' OR AD_BR_PRECAST_VISIBILE_YN = 'Y' ) ";


                SQL = " SELECT   DISTINCT ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME  ";
                SQL = SQL + "   FROM ";
                SQL = SQL + "   SL_STATION_BRANCH_MAPP_DTLS_VW WHERE   ";
                SQL = SQL + "    SALES_STATION_BRANCH_STATUS_AI ='A' and SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_RAW_MATERIAL_STATION ='Y'    ";
                SQL = SQL + "    and  (AD_BR_READYMIX_VISIBILE_YN = 'Y' OR AD_BR_BLOCK_VISIBILE_YN = 'Y' OR AD_BR_PRECAST_VISIBILE_YN = 'Y' ) ";
                SQL = SQL + "    ORDER BY  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE  ASC ";



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

        public double GetNextSeqNo ( )
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

        public DataSet FillCombo ( )
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            { 
                SqlHelper clsSQLHelper = new SqlHelper();

               SQL = "SELECT RM_SCM_SUBCATEGORY_CODE  CAT_CODE,  RM_SCM_SUBCATEGORY_DESC CATE_NAME  FROM RM_CATEGORY_SUB_MASTER ORDER BY RM_SCM_SUBCATEGORY_CODE ASC  ";
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

        public DataSet GetItemCode ( string subCatCode )
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {  
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT  RM_RMM_RM_CODE , RM_RMM_RM_DESCRIPTION    ";
                SQL = SQL + "    FROM RM_RAWMATERIAL_MASTER  ";
                SQL = SQL + " WHERE  RM_SCM_SUBCATEGORY_CODE = '" + subCatCode + "'  ";

                dtGrade = clsSQLHelper.GetDataTableByCommand(SQL);

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

        #region "Physcial entry details XL " 

        public DataSet RawMaterialQtyCurrentStock ( string sBranch, string stCode, string sItems, string ReportType, DateTime dtpTo, int finYrId )

        {
            DataSet sReturn = new DataSet();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT AD_BR_CODE,AD_BR_NAME,STATION_CODE,STATION_NAME, ";
                SQL = SQL + "         RM_CODE,RM_DESCRIPTION,RM_RMM_RM_TYPE,FINYROPTQTY, ";
                SQL = SQL + "         PRVGRVQTY,PRVISSUEVQTY,prevClosing, ";
                SQL = SQL + "         GRVQty,issueqty,CurrClosing, ";
                SQL = SQL + "         CATEGORYCODE,CATEGORYNAME,SUBCATEGORYCODE,SUBCATEGORYNAME ";
                SQL = SQL + " FROM ";
                SQL = SQL + "( ";
                SQL = SQL + "SELECT MainQuery.AD_BR_CODE,MainQuery.AD_BR_NAME, ";
                SQL = SQL + "       MainQuery.SALES_STN_STATION_CODE STATION_CODE,MainQuery.SALES_STN_STATION_NAME STATION_NAME, ";
                SQL = SQL + "       MainQuery.RM_RMM_RM_CODE RM_CODE,MainQuery.RM_RMM_RM_DESCRIPTION RM_DESCRIPTION,MainQuery.RM_RMM_RM_TYPE, ";
                SQL = SQL + "       NVL (OPENING.OPQTY, 0) FINYROPTQTY,NVL (PREVMTHBAL.PRVGRVQTY, 0) PRVGRVQTY,NVL(PREVMTHBAL.PRVISSUEQTY, 0) PRVISSUEVQTY, ";
                SQL = SQL + "      (NVL(OPENING.OPQTY,0)+ NVL (PREVMTHBAL.PRVGRVQTY, 0)- NVL (PREVMTHBAL.PRVISSUEQTY, 0)) prevClosing, ";
                SQL = SQL + "       NVL (GRVQry.GRVQty, 0) GRVQty,NVL (issueQry.issueqty, 0) issueqty, ";
                SQL = SQL + "      (NVL (OPENING.OPQTY, 0)+ NVL (PREVMTHBAL.PRVGRVQTY, 0)+ NVL(GRVQry.GRVQty,0))-(NVL(PREVMTHBAL.PRVISSUEQTY,0)+NVL(issueQry.issueqty,0)) CurrClosing, ";
                SQL = SQL + "      CATEGORYCODE,CATEGORYNAME,SUBCATEGORYCODE,SUBCATEGORYNAME";
                SQL = SQL + " FROM (SELECT RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE,RM_RMM_RM_DESCRIPTION,SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE,SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME, ";
                SQL = SQL + "              SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE,SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME,RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE, ";
                SQL = SQL + "              RM_RAWMATERIAL_MASTER.RM_CM_CATEGORY_CODE  CATEGORYCODE ,RM_CM_CATEGORY_DESC  CATEGORYNAME, ";
                SQL = SQL + "              RM_RAWMATERIAL_MASTER.RM_SCM_SUBCATEGORY_CODE SUBCATEGORYCODE, RM_SCM_SUBCATEGORY_DESC  SUBCATEGORYNAME ";
                SQL = SQL + "         FROM RM_RAWMATERIAL_MASTER,RM_RAWMATERIAL_DETAILS,SL_STATION_BRANCH_MAPP_DTLS_VW,RM_CATEGORY_MASTER,RM_CATEGORY_SUB_MASTER ";
                SQL = SQL + "        WHERE RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE =RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE ";
                SQL = SQL + "          AND RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE =SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE ";
                SQL = SQL + "          AND RM_RAWMATERIAL_DETAILS.AD_BR_CODE =SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE ";
                SQL = SQL + "          AND RM_RAWMATERIAL_MASTER.RM_CM_CATEGORY_CODE=RM_CATEGORY_MASTER.RM_CM_CATEGORY_CODE ";
                SQL = SQL + "          AND RM_RAWMATERIAL_MASTER.RM_SCM_SUBCATEGORY_CODE=RM_CATEGORY_SUB_MASTER.RM_SCM_SUBCATEGORY_CODE ";
                SQL = SQL + "          AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE =RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE ";
                SQL = SQL + "          AND RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE IN ('" + stCode + "') ";
                SQL = SQL + "          AND RM_RAWMATERIAL_DETAILS.AD_BR_CODE IN ('" + sBranch + "'))MainQuery, ";
                SQL = SQL + "      (SELECT AD_BR_CODE, SALES_STN_STATION_CODE, RM_RMM_RM_CODE, ";
                SQL = SQL + "         SUM (RM_OB_QTY) OPQTY,SUM (RM_OB_AMOUNT) OPAMOUNT ";
                SQL = SQL + "         FROM RM_OPEN_BALANCES ";
                SQL = SQL + "        WHERE AD_FIN_FINYRID = " + finYrId + " ";
                SQL = SQL + "          AND SALES_STN_STATION_CODE IN ('" + stCode + "')  ";
                SQL = SQL + "          AND AD_BR_CODE IN ('" + sBranch + "') ";
                SQL = SQL + "     GROUP BY AD_BR_CODE, SALES_STN_STATION_CODE, RM_RMM_RM_CODE) OPENING, ";
                SQL = SQL + "      (SELECT AD_BR_CODE,SALES_STN_STATION_CODE,RM_IM_ITEM_CODE,SUM(RM_STKL_RECD_QTY) PRVGRVQTY, ";
                SQL = SQL + "          SUM(RM_STKL_ISSUE_QTY) PRVISSUEQTY ";
                SQL = SQL + "         FROM RM_STOCK_LEDGER ";
                SQL = SQL + "        WHERE ad_fin_finyrid = " + finYrId + " ";
                SQL = SQL + "        AND TO_DATE (TO_CHAR(RM_STKL_TRANS_DATE, 'DD-MON-YYYY')) <= ";
                SQL = SQL + "      (CASE WHEN (SELECT MAX (RM_PSEM_ENTRY_DATE) FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "        WHERE RM_PSEM_APPROVED = 'Y' ";
                SQL = SQL + "          AND SALES_STN_STATION_CODE IN ('" + stCode + "')  ";
                SQL = SQL + "          AND AD_BR_CODE IN ('" + sBranch + "') ";
                SQL = SQL + "          AND AD_FIN_FINYRID = " + finYrId + ") IS NULL ";
                SQL = SQL + "         THEN (SELECT AD_FIN_FROM_DATE ";
                SQL = SQL + "         FROM AD_FINANCIAL_YEAR ";
                SQL = SQL + "        WHERE AD_FIN_FINYRID = " + finYrId + ") ";
                SQL = SQL + "         ELSE (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "         FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "        WHERE RM_PSEM_APPROVED = 'Y' ";
                SQL = SQL + "          AND SALES_STN_STATION_CODE IN  ('" + stCode + "') ";
                SQL = SQL + "          AND AD_BR_CODE IN ('" + sBranch + "') ";
                SQL = SQL + "          AND AD_FIN_FINYRID = " + finYrId + ") END) ";
                SQL = SQL + "     GROUP BY AD_BR_CODE, SALES_STN_STATION_CODE, RM_IM_ITEM_CODE) PREVMTHBAL, ";
                SQL = SQL + "      (SELECT AD_BR_CODE,RM_IM_ITEM_CODE,SALES_STN_STATION_CODE,GrvQTY ";
                SQL = SQL + "         FROM (SELECT AD_BR_CODE,RM_IM_ITEM_CODE,SALES_STN_STATION_CODE,SUM(GrvQTY) GrvQTY ";
                SQL = SQL + "         FROM (SELECT AD_BR_CODE,RM_RMM_RM_CODE RM_IM_ITEM_CODE,SALES_STN_STATION_CODE, ";
                SQL = SQL + "          SUM (RM_MRD_APPROVE_QTY) GrvQTY FROM RM_RECEPIT_DETAILS ";
                SQL = SQL + "        WHERE SALES_STN_STATION_CODE IN  ('" + stCode + "')  ";
                SQL = SQL + "          AND AD_BR_CODE IN ('" + sBranch + "') ";
                SQL = SQL + "          AND ad_fin_finyrid = " + finYrId + " ";
                SQL = SQL + "          AND RM_MRM_RECEIPT_DATE >(CASE WHEN ";
                SQL = SQL + "         (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "          FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "         WHERE RM_PSEM_APPROVED = 'Y' ";
                SQL = SQL + "           AND SALES_STN_STATION_CODE IN  ('" + stCode + "') ";
                SQL = SQL + "           AND AD_BR_CODE IN('" + sBranch + "') ";
                SQL = SQL + "           AND AD_FIN_FINYRID = " + finYrId + ") IS NULL ";
                SQL = SQL + "          THEN (SELECT AD_FIN_FROM_DATE FROM AD_FINANCIAL_YEAR WHERE AD_FIN_FINYRID = " + finYrId + ") ";
                SQL = SQL + "          ELSE (SELECT MAX (RM_PSEM_ENTRY_DATE) FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "         WHERE RM_PSEM_APPROVED ='Y' ";
                SQL = SQL + "           AND SALES_STN_STATION_CODE IN  ('" + stCode + "')  ";
                SQL = SQL + "           AND AD_BR_CODE IN ('" + sBranch + "') ";
                SQL = SQL + "           AND AD_FIN_FINYRID = " + finYrId + ")END) ";
                SQL = SQL + "           AND RM_MRM_RECEIPT_DATE <= '" + System.Convert.ToDateTime(dtpTo).ToString("dd-MMM-yyyy") + "' ";
                SQL = SQL + "      GROUP BY AD_BR_CODE,RM_RMM_RM_CODE,SALES_STN_STATION_CODE ";
                SQL = SQL + "    UNION ALL ";
                SQL = SQL + "        SELECT AD_BR_CODE,RM_IM_ITEM_CODE,SALES_STN_STATION_CODE, ";
                SQL = SQL + "           SUM(RM_STKL_RECD_QTY - RM_STKL_ISSUE_QTY) StockTransferQty ";
                SQL = SQL + "          FROM RM_STOCK_LEDGER ";
                SQL = SQL + "         WHERE RM_STKL_TRANS_TYPE = 'STK_TRANS' ";
                SQL = SQL + "           AND ad_fin_finyrid = " + finYrId + " ";
                SQL = SQL + "           AND SALES_STN_STATION_CODE IN  ('" + stCode + "')  ";
                SQL = SQL + "           AND AD_BR_CODE IN ('" + sBranch + "') ";
                SQL = SQL + "           AND ad_fin_finyrid = " + finYrId + " ";
                SQL = SQL + "           AND RM_STKL_TRANS_DATE > (CASE WHEN (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "          FROM RM_PHYSICAL_STOCK_MASTER  WHERE RM_PSEM_APPROVED = 'Y' ";
                SQL = SQL + "           AND SALES_STN_STATION_CODE IN  ('" + stCode + "') ";
                SQL = SQL + "           AND AD_BR_CODE IN ('" + sBranch + "') ";
                SQL = SQL + "           AND AD_FIN_FINYRID = " + finYrId + ")IS NULL THEN ";
                SQL = SQL + "       (SELECT AD_FIN_FROM_DATE FROM AD_FINANCIAL_YEAR WHERE AD_FIN_FINYRID = " + finYrId + ") ";
                SQL = SQL + "          ELSE (SELECT MAX (RM_PSEM_ENTRY_DATE) FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "         WHERE RM_PSEM_APPROVED =   'Y' ";
                SQL = SQL + "           AND SALES_STN_STATION_CODE IN ('" + stCode + "')  ";
                SQL = SQL + "           AND AD_BR_CODE IN ('" + sBranch + "')  ";
                SQL = SQL + "           AND AD_FIN_FINYRID = " + finYrId + ") END) ";
                SQL = SQL + "           AND RM_STKL_TRANS_DATE <= '" + System.Convert.ToDateTime(dtpTo).ToString("dd-MMM-yyyy") + "' ";
                SQL = SQL + "      GROUP BY AD_BR_CODE,RM_IM_ITEM_CODE,SALES_STN_STATION_CODE) InQry ";
                SQL = SQL + "      GROUP BY AD_BR_CODE, RM_IM_ITEM_CODE, SALES_STN_STATION_CODE))  GRVQry,  ";
                 SQL = SQL + "       (SELECT AD_BR_CODE,  SALES_STN_STATION_CODE, ";
                SQL = SQL + "                         RM_IM_ITEM_CODE,  ";
                SQL = SQL + "                         ISSUEQTY ";
                SQL = SQL + "                    FROM (SELECT PR_DELIVERY_NOTE.AD_BR_CODE, ";
                SQL = SQL + "                                   PR_DELIVERY_NOTE.SALES_STN_STATION_CODE, ";
                SQL = SQL + "                                   PR_DN_MIX_DETAILS.RM_RMM_RM_CODE ";
                SQL = SQL + "                                       RM_IM_ITEM_CODE, ";
                SQL = SQL + "                                   SUM ( ";
                SQL = SQL + "                                         (tech_pcd_weight / RM_UCM_FACTOR) ";
                SQL = SQL + "                                       * (  pr_dlyn_dn_qty ";
                SQL = SQL + "                                          - PR_DLYN_RETURN_IN_TRUCK_QTY)) ";
                SQL = SQL + "                                       ISSUEQTY ";
                SQL = SQL + "                              FROM pr_dn_mix_details, ";
                SQL = SQL + "                                   PR_DELIVERY_NOTE, ";
                SQL = SQL + "                                   RM_UOM_CONVERSION ";
                SQL = SQL + "                                 WHERE pr_dn_mix_details.prod_dlyn_dn_no =  PR_DELIVERY_NOTE.pr_dlyn_dn_no ";
                SQL = SQL + "                                   AND pr_dn_mix_details.ad_fin_finyrid =  PR_DELIVERY_NOTE.ad_fin_finyrid ";
                SQL = SQL + "                                   AND pr_dn_mix_details.rm_rmm_rm_code =  RM_UOM_CONVERSION.rm_rmm_rm_code ";
                SQL = SQL + "                                   AND pr_dn_mix_details.rm_um_uom_code =  RM_UOM_CONVERSION.RM_UM_UOM_CODE_FROM ";
                SQL = SQL + "                                   AND pr_dn_mix_details.tech_pcd_uom_code = RM_UOM_CONVERSION.RM_UM_UOM_CODE_TO ";
                SQL = SQL + "                                   AND PR_DELIVERY_NOTE.PR_CONRTN_DNCANCEL = 'N' ";
                SQL = SQL + "                                   AND pr_delivery_note.PR_DLYN_CONC_MIXED = 'Y' ";
                SQL = SQL + "                                   AND PR_DELIVERY_NOTE.AD_FIN_FINYRID =  " + finYrId + " ";
                SQL = SQL + "                                   AND PR_DELIVERY_NOTE.SALES_STN_STATION_CODE IN ('" + stCode + "') ";
                SQL = SQL + "                                   AND PR_DELIVERY_NOTE.AD_BR_CODE IN ('" + sBranch + "') ";
                SQL = SQL + "                                   AND PR_DELIVERY_NOTE.PR_DLYN_DN_DATE > ";
                SQL = SQL + "                                       (CASE ";
                SQL = SQL + "                                            WHEN (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                                    FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                                   WHERE     RM_PSEM_APPROVED = 'Y' ";
                SQL = SQL + "                                                         AND SALES_STN_STATION_CODE IN ('" + stCode + "')";
                SQL = SQL + "                                                         AND AD_BR_CODE IN ('" + sBranch + "')";
                SQL = SQL + "                                                         AND AD_FIN_FINYRID =  " + finYrId + ") ";
                SQL = SQL + "                                                     IS NULL ";
                SQL = SQL + "                                            THEN ";
                SQL = SQL + "                                                (SELECT AD_FIN_FROM_DATE ";
                SQL = SQL + "                                                   FROM AD_FINANCIAL_YEAR ";
                SQL = SQL + "                                                  WHERE AD_FIN_FINYRID =  " + finYrId + ") ";
                SQL = SQL + "                                            ELSE ";
                SQL = SQL + "                                                (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                                   FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                                  WHERE     RM_PSEM_APPROVED = 'Y' ";
                SQL = SQL + "                                                        AND SALES_STN_STATION_CODE IN  ('" + stCode + "')";
                SQL = SQL + "                                                        AND AD_BR_CODE IN ('" + sBranch + "')";
                SQL = SQL + "                                                        AND AD_FIN_FINYRID =  " + finYrId + ") ";
                SQL = SQL + "                                        END) ";
                SQL = SQL + "                          GROUP BY PR_DELIVERY_NOTE.AD_BR_CODE, ";
                SQL = SQL + "                                   pr_dn_mix_details.rm_rmm_rm_code, ";
                SQL = SQL + "                                   PR_DELIVERY_NOTE.SALES_STN_STATION_CODE ";
                SQL = SQL + "                          UNION ALL  ";
                SQL = SQL + "                          select  ";
                SQL = SQL + "                   AD_BR_CODE,  ";
                SQL = SQL + "                   SALES_STN_STATION_CODE,RM_IM_ITEM_CODE,  Sum(IssueQty) IssueQty  ";
                SQL = SQL + "                    from( SELECT AD_BR_CODE, ";
                SQL = SQL + "                                   SALES_STN_STATION_CODE, ";
                SQL = SQL + "                                   RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE ";
                SQL = SQL + "                                       RM_IM_ITEM_CODE, ";
                SQL = SQL + "                                   CASE ";
                SQL = SQL + "                                       WHEN NVL (RM_UCM_FACTOR, 0) > 0 ";
                SQL = SQL + "                                       THEN ";
                SQL = SQL + "                                           (SUM (RM_STISUD_QTY) / RM_UCM_FACTOR) ";
                SQL = SQL + "                                       ELSE ";
                SQL = SQL + "                                           SUM (RM_STISUD_QTY) ";
                SQL = SQL + "                                   END ";
                SQL = SQL + "                                       AS IssueQty ";
                SQL = SQL + "                              FROM RM_STORE_ISSUE_MASTER, ";
                SQL = SQL + "                                   RM_STORE_ISSUE_DETAILS, ";
                SQL = SQL + "                                   RM_UOM_CONVERSION ";
                SQL = SQL + "                             WHERE     RM_STORE_ISSUE_MASTER.RM_STISU_ENTRY_NO =  RM_STORE_ISSUE_DETAILS.RM_STISU_ENTRY_NO ";
                SQL = SQL + "                                   AND RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE = RM_UOM_CONVERSION.RM_RMM_RM_CODE(+) ";
                SQL = SQL + "                                   AND RM_STORE_ISSUE_DETAILS.RM_UM_UOM_CODE =  RM_UOM_CONVERSION.RM_UM_UOM_CODE_TO(+) ";
                SQL = SQL + "                                   AND RM_STORE_ISSUE_MASTER.AD_FIN_FINYRID = " + finYrId + " ";
                SQL = SQL + "                                   AND SALES_STN_STATION_CODE IN ('" + stCode + "') ";
                SQL = SQL + "                                   AND AD_BR_CODE IN ('" + sBranch + "')";
                SQL = SQL + "                                   AND RM_STISU_ENTRY_DATE > ";
                SQL = SQL + "                                       (CASE ";
                SQL = SQL + "                                            WHEN (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                                    FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                                   WHERE     RM_PSEM_APPROVED = 'Y' ";
                SQL = SQL + "                                                         AND SALES_STN_STATION_CODE IN  ('" + stCode + "') ";
                SQL = SQL + "                                                         AND AD_BR_CODE IN ('" + sBranch + "') ";
                SQL = SQL + "                                                         AND AD_FIN_FINYRID =  " + finYrId + ") ";
                SQL = SQL + "                                                     IS NULL ";
                SQL = SQL + "                                            THEN ";
                SQL = SQL + "                                                (SELECT AD_FIN_FROM_DATE ";
                SQL = SQL + "                                                   FROM AD_FINANCIAL_YEAR ";
                SQL = SQL + "                                                  WHERE AD_FIN_FINYRID =  " + finYrId + ") ";
                SQL = SQL + "                                            ELSE ";
                SQL = SQL + "                                                (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                                   FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                                  WHERE     RM_PSEM_APPROVED = 'Y' ";
                SQL = SQL + "                                                        AND SALES_STN_STATION_CODE IN  ('" + stCode + "') ";
                SQL = SQL + "                                                        AND AD_BR_CODE IN ('" + sBranch + "') ";
                SQL = SQL + "                                                        AND AD_FIN_FINYRID = " + finYrId + ") ";
                SQL = SQL + "                                        END) ";
                SQL = SQL + "                          GROUP BY RM_STORE_ISSUE_DETAILS.RM_RMM_RM_CODE, ";
                SQL = SQL + "                                   AD_BR_CODE, ";
                SQL = SQL + "                                   SALES_STN_STATION_CODE,RM_UCM_FACTOR ";
                SQL = SQL + "                                   )GROUP BY AD_BR_CODE,  ";
                SQL = SQL + "                                SALES_STN_STATION_CODE,RM_IM_ITEM_CODE ";
                SQL = SQL + "                          UNION ALL ";
                SQL = SQL + "                            SELECT AD_BR_CODE, ";
                SQL = SQL + "                                   SALES_STN_STATION_CODE, ";
                SQL = SQL + "                                   FG_FINISHED_GOODS_CONS_DETAILS.RM_RMM_RM_CODE ";
                SQL = SQL + "                                       RM_IM_ITEM_CODE, ";
                SQL = SQL + "                                   CASE ";
                SQL = SQL + "                                       WHEN NVL (FG_FNG_CONVERSION_FACT, 0) > 0 ";
                SQL = SQL + "                                       THEN ";
                SQL = SQL + "                                           (  SUM (FG_FNG_WEIGHT_ACTUAL) ";
                SQL = SQL + "                                            * FG_FNG_CONVERSION_FACT) ";
                SQL = SQL + "                                       ELSE ";
                SQL = SQL + "                                           SUM (FG_FNG_WEIGHT_ACTUAL) ";
                SQL = SQL + "                                   END ";
                SQL = SQL + "                                       AS IssueQty ";
                SQL = SQL + "                              FROM FG_FINISHED_GOODS_MASTER, ";
                SQL = SQL + "                                   FG_FINISHED_GOODS_CONS_DETAILS ";
                SQL = SQL + "                             WHERE     FG_FINISHED_GOODS_MASTER.FG_FINISHED_GOODS_DOC_NO = ";
                SQL = SQL + "                                       FG_FINISHED_GOODS_CONS_DETAILS.FG_FINISHED_GOODS_DOC_NO ";
                SQL = SQL + "                                   AND FG_FINISHED_GOODS_MASTER.AD_FIN_FINYRID = " + finYrId + "";
                SQL = SQL + "                                   AND SALES_STN_STATION_CODE IN  ('" + stCode + "')";
                SQL = SQL + "                                   AND AD_BR_CODE IN ('" + sBranch + "') ";
                SQL = SQL + "                                   AND FG_FINISHED_GOODS_MASTER.FG_PRODUCTION_DATE > ";
                SQL = SQL + "                                       (CASE ";
                SQL = SQL + "                                            WHEN (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                                    FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                                   WHERE     RM_PSEM_APPROVED =  'Y' ";
                SQL = SQL + "                                                         AND SALES_STN_STATION_CODE IN  ('" + stCode + "')";
                SQL = SQL + "                                                         AND AD_BR_CODE IN ('" + sBranch + "') ";
                SQL = SQL + "                                                         AND AD_FIN_FINYRID =  " + finYrId + ") ";
                SQL = SQL + "                                                     IS NULL ";
                SQL = SQL + "                                            THEN ";
                SQL = SQL + "                                                (SELECT AD_FIN_FROM_DATE ";
                SQL = SQL + "                                                   FROM AD_FINANCIAL_YEAR ";
                SQL = SQL + "                                                  WHERE AD_FIN_FINYRID = " + finYrId + ") ";
                SQL = SQL + "                                            ELSE ";
                SQL = SQL + "                                                (SELECT MAX (RM_PSEM_ENTRY_DATE) ";
                SQL = SQL + "                                                   FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + "                                                  WHERE     RM_PSEM_APPROVED ='Y' ";
                SQL = SQL + "                                                        AND SALES_STN_STATION_CODE IN  ('" + stCode + "')";
                SQL = SQL + "                                                        AND AD_BR_CODE IN ('" + sBranch + "')";
                SQL = SQL + "                                                        AND AD_FIN_FINYRID =  " + finYrId + ") ";
                SQL = SQL + "                                        END) ";
                SQL = SQL + "                          GROUP BY FG_FINISHED_GOODS_CONS_DETAILS.RM_RMM_RM_CODE, ";
                SQL = SQL + "                                   AD_BR_CODE, ";
                SQL = SQL + "                                   SALES_STN_STATION_CODE,FG_FNG_CONVERSION_FACT ";
                SQL = SQL + "                                   ) )issueQry ";
                SQL = SQL + "       WHERE MainQuery.RM_RMM_RM_CODE = OPENING.RM_RMM_RM_CODE(+) ";
                SQL = SQL + "         AND MainQuery.SALES_STN_STATION_CODE = OPENING.SALES_STN_STATION_CODE(+) ";
                SQL = SQL + "         AND MainQuery.AD_BR_CODE = OPENING.AD_BR_CODE(+) ";
                SQL = SQL + "         AND MainQuery.RM_RMM_RM_CODE = PREVMTHBAL.RM_IM_ITEM_CODE(+) ";
                SQL = SQL + "         AND MainQuery.SALES_STN_STATION_CODE = PREVMTHBAL.SALES_STN_STATION_CODE(+) ";
                SQL = SQL + "         AND MainQuery.AD_BR_CODE = PREVMTHBAL.AD_BR_CODE(+) ";
                SQL = SQL + "         AND MainQuery.RM_RMM_RM_CODE = GRVQry.RM_IM_ITEM_CODE(+) ";
                SQL = SQL + "         AND MainQuery.SALES_STN_STATION_CODE = GRVQry.SALES_STN_STATION_CODE(+) ";
                SQL = SQL + "         AND MainQuery.AD_BR_CODE = GRVQry.AD_BR_CODE(+) ";
                SQL = SQL + "         AND MainQuery.RM_RMM_RM_CODE = issueQry.RM_IM_ITEM_CODE(+) ";
                SQL = SQL + "         AND MainQuery.SALES_STN_STATION_CODE = issueQry.SALES_STN_STATION_CODE(+) ";
                SQL = SQL + "         AND MainQuery.AD_BR_CODE = issueQry.AD_BR_CODE(+)   ";

                SQL = SQL + ")  ";

                if (ReportType == "POSITIVE STOCK")
                {
                    SQL = SQL + "WHERE CurrClosing>=0   ";
                }
                else if (ReportType == "NEGATIVE STOCK")
                {
                    SQL = SQL + "WHERE CurrClosing <0  ";
                }
                else if (ReportType == "POSITIVE STOCK QTY GREATER THAN ZERO ")
                {
                    SQL = SQL + "WHERE CurrClosing > 0  ";
                }

                if (ReportType == "ALL" &&!string.IsNullOrEmpty(sItems))
                {
                    SQL = SQL + "WHERE";
                }
                else if(!string.IsNullOrEmpty(sItems))
                {
                    SQL = SQL + "AND";
                }
                if (!string.IsNullOrEmpty(sItems))
                {
                    SQL = SQL + " RM_CODE in ('" + sItems + "')";
                }
                SQL = SQL + "ORDER BY  AD_BR_CODE, STATION_CODE, RM_CODE ";


                sReturn = clsSQLHelper.GetDataset(SQL);
                 
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                objLogWriter.WriteSQL(SQL);
            }

            return sReturn;
        }

        #endregion

    }

    public class RawMaterialQtyCurrentStockFPSEntity
        {
            public string sStationCode { get; set; }
        }
    }
