using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AccosoftUtilities;
using Oracle.ManagedDataAccess.Client; using Oracle.ManagedDataAccess.Types;
using System.Collections;
using System.Data;
using System.Globalization;
using AccosoftNineteenCDL;

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class RawMaterailAuditLogic
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());
        String SQL = string.Empty;
        ArrayList sSQLArray = new ArrayList();

        #region " Public Variables"


        public DateTime dtpToDate { get; set; }
        public DateTime dtpFromDate
        {
            get;
            set;
        }

        public int FinYearID
        {
            get;
            set;
        }

        #endregion 

        public DataTable OpeningEntryWise_RM()
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
               // CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  ";
                SQL = SQL + " RM_OPEN_BALANCES.sALES_STN_STATION_CODE STATION_CODE, ";
                SQL = SQL + "  sl_station_master.SALES_STN_STATION_NAME station_name,  "; 
            SQL = SQL  +  " rm_rawmaterial_master.RM_RMM_RM_CODE RM_CODE  ,RM_RMM_RM_DESCRIPTION RM_DESCRIPTION ,  ";
            SQL = SQL  +  " RM_OB_QTY open_qty, RM_OB_PRICE  open_price ,  RM_OB_AMOUNT openvalues  , ";
            SQL = SQL + " RM_RMM_INV_ACC_CODE, GL_COAM_ACCOUNT_NAME  ACCOUNT_NAME   ";
            SQL = SQL  +  " FROM RM_OPEN_BALANCES ,  ";
            SQL = SQL  +  " rm_rawmaterial_master ,sl_station_master , ";
            SQL = SQL  +  " GL_COA_MASTER  ";
            SQL = SQL  +  "where   ";
            SQL = SQL  +  " rm_rawmaterial_master.RM_RMM_INV_ACC_CODE  =  GL_COA_MASTER.GL_COAM_ACCOUNT_CODE  (+)   ";
            SQL = SQL  +  " and rm_rawmaterial_master.RM_RMM_RM_CODE =   RM_OPEN_BALANCES.RM_RMM_RM_CODE  ";
            SQL = SQL  +  " and RM_OPEN_BALANCES.SALES_STN_STATION_CODE =   sl_station_master.SALES_STN_STATION_CODE  ";
            SQL = SQL  +  " and  RM_OPEN_BALANCES.ad_fin_finyrid =   " + FinYearID + "";
            SQL = SQL + "ORDER BY   RM_OPEN_BALANCES.sales_stn_station_code , rm_rawmaterial_master.RM_RMM_RM_CODE  ";
           

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }

        public DataTable OpeningEntryWise_GL() 
             
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                // CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();

            SQL = " SELECT  " ;
            SQL = SQL  + "    GL_COA_MASTER.GL_COAM_ACCOUNT_CODE  , ";
            SQL = SQL  + "    GL_COAM_ACCOUNT_NAME  ACCOUNT_NAME  ,  ";
            SQL = SQL  + "    GL_COAB_OPN_BAL openvalues  ";
            SQL = SQL  + "FROM   ";
            SQL = SQL  + "    GL_COA_BALANCES  ,   ";
            SQL = SQL  + "    GL_COA_MASTER  ";
            SQL = SQL  + "where   ";
            SQL = SQL  + "GL_COA_MASTER.GL_COAM_ACCOUNT_CODE     =  GL_COA_BALANCES.GL_COAM_ACCOUNT_CODE  ";
            SQL = SQL  + "and  GL_COA_BALANCES.ad_fin_finyrid =  " + FinYearID +"";
            SQL = SQL  + "and   GL_COA_MASTER.GL_COAM_ACCOUNT_CODE   in(  ";
            SQL = SQL  + "select  ";
            SQL = SQL + " distinct RM_RMM_INV_ACC_CODE   from rm_rawmaterial_master )       ORDER BY  GL_COA_MASTER.GL_COAM_ACCOUNT_CODE   ";



                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }
         


        public DataTable GoodsReceiptEntryWise() 
             
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                // CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();

            SQL = "  select " ;
            SQL = SQL  + "     RM_MRM_APPROVED_NO  entryno, ";
            SQL = SQL  + "     nvl(receipt_app_qty,0) receipt_app_qty,nvl(RECEIPT_AMOUNT,0) RECEIPT_AMOUNT, ";
            SQL = SQL  + "     'Stock-->' stk_ledger,nvl(stk_rcvd_qty,0) stk_rcvd_qty, nvl(Stk_rcvd_price ,0) Stk_rcvd_price , ";
            SQL = SQL  + "     nvl(RECEIPT_AMOUNT,0)   -  nvl(Stk_rcvd_price ,0) grv_stkledger_diff, ";
            SQL = SQL  + "     'GL-->' gl_ledger,  ";
            SQL = SQL  + "     NVL(gl_gl_debit,0)gl_gl_debit, NVL(gl_gl_credit,0) gl_gl_credit ,  ";
            SQL = SQL  + "     nvl(Stk_rcvd_price ,0)  - NVL (gl_gl_debit, 0) stk_gl_ledger_diff  ";
            SQL = SQL  + " from ";
            SQL = SQL  + " (   ";
            SQL = SQL  + "     select  RM_MRM_APPROVED_NO , RM_MRD_APPROVED_FINYRID afin,  ";
            SQL = SQL  + "     sum(RM_MRD_APPROVE_QTY) receipt_app_qty, sum(RM_MRD_TOTAL_AMOUNT_NEW) RECEIPT_AMOUNT ";
            SQL = SQL  + "     from RM_RECEPIT_DETAILS  ";
            SQL = SQL  + "     where  ";
            SQL = SQL  + "     RM_MRD_APPROVED_FINYRID =  " + FinYearID + "";
            SQL = SQL  + "     group by RM_MRM_APPROVED_NO ,RM_MRD_APPROVED_FINYRID ";
            SQL = SQL  + " )  A,       ";
            SQL = SQL  + " (  ";
            SQL = SQL  + "     SELECT  RM_STKL_DOC_NO,   ad_fin_finyrid bfin,  ";
            SQL = SQL  + "     sum(rm_stock_ledger.RM_STKL_RECD_QTY)  stk_rcvd_qty, ";
            SQL = SQL  + "     nvl(sum(RM_STKL_RECD_AMT ),0)  Stk_rcvd_price      ";
            SQL = SQL  + "     FROM  rm_stock_ledger  ";
            SQL = SQL  + "     WHERE    ";
            SQL = SQL  + "     rm_stock_ledger.RM_STKL_TRANS_TYPE ='RECEIPT'  ";
            SQL = SQL  + "     AND rm_stock_ledger.ad_fin_finyrid =  " + FinYearID + "";
            SQL = SQL  + "     group by     RM_STKL_DOC_NO, AD_FIN_FINYRID      ";
            SQL = SQL  + " ) b,  ";
            SQL = SQL  + " (  ";
            SQL = SQL  + "     SELECT   ";
            SQL = SQL  + "     GL_GL_DOCUMENT_NO,ad_fin_finyrid cfin,  ";
            SQL = SQL  + "     SUM( gl_general_ledger.gl_gl_debit)   GL_GL_DEBIT ,SUM(GL_GL_CREDIT) GL_GL_CREDIT ";
            SQL = SQL  + "     FROM  gl_general_ledger  ";
            SQL = SQL  + "     WHERE  ";
            SQL = SQL  + "     gl_general_ledger.ad_fin_finyrid = " + FinYearID + "";
            SQL = SQL  + "     AND   GL_GL_TRANS_TYPE   =  'RECEIPT' ";
            SQL = SQL  + "     GROUP BY GL_GL_DOCUMENT_NO,ad_fin_finyrid  ";
            SQL = SQL  + " ) c  ";
            SQL = SQL  + " where  ";
            SQL = SQL  + " a.RM_MRM_APPROVED_NO = b.RM_STKL_DOC_NO (+)  ";
            SQL = SQL  + " and   a.RM_MRM_APPROVED_NO = c.GL_GL_DOCUMENT_NO (+)  ";
            SQL = SQL  + " and   a.afin = b.bfin  (+)  ";
            SQL = SQL  + " and   a.afin  = c.cfin  (+) ";
            SQL = SQL + " order by a.RM_MRM_APPROVED_NO  asc  ";




                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }


        public DataTable PhysicalStockEntryEntryDetails() 
             
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                // CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "   ";
                SQL =  "select ";
                SQL = SQL  +  " RM_PSEM_ENTRY_NO   entryno, " ;
                SQL = SQL  +  " nvl(phyentryamount,0) phyentryamount , nvl(varianceamount,0) varianceamount , ";
                SQL = SQL  +  "  nvl( consumAndVariance,0) consumtionMinusVariance,    ";
                SQL = SQL  +  " 'Stock-->' stock_ledger , nvl(STK_issue_AMOUNT ,0) STK_issue_AMOUNT ,  nvl(RM_STKL_RECD_AMT ,0) STK_Receipt_phstk_AMOUNT ,  ";
                SQL = SQL  +  " nvl(consumAndVariance,0)    -    (nvl(STK_issue_AMOUNT ,0)- nvl(RM_STKL_RECD_AMT,0) )   Cns_MInus_Var_mns_IsuMnsRv, ";
                SQL = SQL  +  " 'GL-->' gl_ledger,  ";
                SQL = SQL  +  " NVL(gl_gl_debit,0)gl_gl_debit, NVL(gl_gl_credit,0) gl_gl_credit ,  ";
                SQL = SQL  +  "( nvl(STK_issue_AMOUNT ,0) + nvl(RM_STKL_RECD_AMT,0)) - NVL (gl_gl_debit, 0) stk_gl_ledger_diff   ";
                SQL = SQL  +  "from ";
                SQL = SQL  +  "(  ";
                SQL = SQL  +  " select  RM_PSEM_ENTRY_NO ,  AD_FIN_FINYRID      afin, ";
                SQL = SQL  +  "   sum( RM_PSED_CONSUMPTION_AMOUNT) phyentryamount  , ";
                SQL = SQL  +  "   sum(RM_PSED_VARIANCEAMOUNT) varianceamount , ";
                SQL = SQL  +  "    sum( RM_PSED_CONSUMPTION_AMOUNT - RM_PSED_VARIANCEAMOUNT) consumAndVariance  ";
                SQL = SQL  +  " from RM_PHYSICAL_STOCK_DETAILS ";
                SQL = SQL  +  " where ad_fin_finyrid =  "+ FinYearID + "";
              //  SQL = SQL  +  " --  and RM_PSEM_ENTRY_NO='14000001' and  RM_RMM_RM_CODE ='101' "
                SQL = SQL  +  " group by RM_PSEM_ENTRY_NO ,  AD_FIN_FINYRID  ";
                SQL = SQL  +  ")  A,       ";
                SQL = SQL  +  "(  ";
                SQL = SQL  +  " SELECT  RM_STKL_DOC_NO,   ad_fin_finyrid bfin,   ";
                SQL = SQL  +  "     nvl(sum(RM_STKL_ISSUE_AMOUNT ),0) STK_issue_AMOUNT , ";
                SQL = SQL  +  "        nvl(sum(RM_STKL_RECD_AMT ),0) RM_STKL_RECD_AMT  ";
                SQL = SQL  +  " FROM  rm_stock_ledger  ";
                SQL = SQL  +  " WHERE    ";
                SQL = SQL  +  " rm_stock_ledger.RM_STKL_TRANS_TYPE ='PHYSTK'  ";
                SQL = SQL  +  " AND rm_stock_ledger.ad_fin_finyrid  =  "+ FinYearID + "";
               // SQL = SQL  +  "--    and RM_STKL_DOC_NO='14000001' and  RM_IM_ITEM_CODE ='101' "
                SQL = SQL  +  " group by     RM_STKL_DOC_NO, AD_FIN_FINYRID      ";
                SQL = SQL  +  ") b,  ";
                SQL = SQL  +  "(  ";
                SQL = SQL  +  " SELECT   ";
                SQL = SQL  +  " GL_GL_DOCUMENT_NO,ad_fin_finyrid cfin,  ";
                SQL = SQL  +  " SUM( gl_general_ledger.gl_gl_debit)   GL_GL_DEBIT ,SUM(GL_GL_CREDIT) GL_GL_CREDIT ";
                SQL = SQL  +  " FROM  gl_general_ledger  ";
                SQL = SQL  +  " WHERE  ";
                SQL = SQL  +  " gl_general_ledger.ad_fin_finyrid   =   "+ FinYearID + "";
                SQL = SQL  +  " AND   GL_GL_TRANS_TYPE   =  'PHYSTK' ";
                SQL = SQL  +  " GROUP BY GL_GL_DOCUMENT_NO,ad_fin_finyrid  ";
                SQL = SQL  +  ") c  ";
                SQL = SQL  +  "where  ";
                SQL = SQL  +  "a.RM_PSEM_ENTRY_NO = b.RM_STKL_DOC_NO (+)  ";
                SQL = SQL  +  "and   a.RM_PSEM_ENTRY_NO = c.GL_GL_DOCUMENT_NO (+)  ";
                SQL = SQL  +  "and   a.afin = b.bfin  (+)  ";
                SQL = SQL  +  "and   a.afin = c.cfin  (+) ";
                SQL = SQL + "ORDER BY entryno ";



                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }


        public DataTable InventoryAccountsWiseDetails() 
             
        {
            DataTable dtData = new DataTable("MAIN");


            try
            {
                // CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();

          SQL = "  ";
SQL =   "select  ";
SQL = SQL  + " m.RM_RMM_INV_ACC_CODE ACC_CODE , GL_COAM_ACCOUNT_NAME ACCOUNT_NAME,  ";
SQL = SQL  + " m.openvalues    , "; 
SQL = SQL  + " glop.GL_OPENING_VALUES ,  ";
SQL = SQL  + " m.openvalues    -glop.GL_OPENING_VALUES  OPENING_DIFF,          ";
SQL = SQL  + " nvl(B.receipt,0) receipt_AMOUNT , ";;
SQL = SQL  + " NVL(glrv.GL_RECEIPT,0) GL_RECEIPT_Amount, ";;
SQL = SQL  + " nvl(B.receipt,0)-NVL(glrv.GL_RECEIPT,0)  RECEIPT_DIFF, "; 
SQL = SQL  + " nvl(c.stk_receipt,0) stk_receipt  , "; 
SQL = SQL  + " nvl(GL_STK_TRN_RECEIPT ,0 ) GL_STK_TRN_RECEIPT, "; 
SQL = SQL  + " nvl(c.stk_receipt,0)-nvl(GL_STK_TRN_RECEIPT ,0 )  STK_TRN_RECEIPT_DIFF, ";
SQL = SQL + "  nvl(c.stk_receipt,0)- nvl(e.stk_trn_out,0)  trans_amount,"; 
SQL = SQL  + " NVL(d.issue,0) Issue_Amount , ";
SQL = SQL  + " NVL(glphystk.GL_PHYSTK,0)  GL_PHYSTK, ";
SQL = SQL  + " NVL(d.issue,0)-     NVL(glphystk.GL_PHYSTK,0) PHY_STOCK_DIFF, ";
SQL = SQL  + " nvl(e.stk_trn_out,0) stk_trn_out,  ";
SQL = SQL  + " nvl(gltrnout.GL_stk_trn_out,0) GL_stk_trn_out, ";
SQL = SQL  + " nvl(e.stk_trn_out,0)- nvl(gltrnout.GL_stk_trn_out,0)  STK_TRN_OUT_DIFF, ";
SQL = SQL  + " nvl(f.rm_sales,0) rm_sales, ";
SQL = SQL  + " nvl( glrmsales.GL_rm_salesamount,0) GL_rm_salesamount, ";
SQL = SQL  + " nvl(f.rm_sales,0) -    nvl( glrmsales.GL_rm_salesamount,0)  RM_SALES_DIFF, ";
SQL = SQL  + " nvl(gl_other.gl_other_debit_amount,0)  gl_other_debit_amount,  ";
SQL = SQL  + " nvl(gl_other.gl_other_credit_amount,0) gl_other_credit_amount , ";
SQL = SQL  + " nvl( rmphystin.phy_stk_in_RECD_AMT,0) phy_stk_in_RECD_AMT,  ";
SQL = SQL  + " nvl(glphystk_in.GL_PHYSTK_in_amount,0)GL_PHYSTK_in_amount, ";
SQL = SQL  + " 'TOTALS-->' TOTALS,  ";
SQL = SQL  + " (m.openvalues + nvl(B.receipt,0) + nvl(c.stk_receipt,0) + nvl( rmphystin.phy_stk_in_RECD_AMT,0) )-  ";
SQL = SQL  + "  ( NVL(d.issue,0) +nvl(e.stk_trn_out,0) +  nvl(f.rm_sales,0) )  RM_CLOSING,  ";
SQL = SQL  + " ( glop.GL_OPENING_VALUES + NVL(glrv.GL_RECEIPT,0) + nvl(GL_STK_TRN_RECEIPT ,0 )  ";
SQL = SQL  + " +         nvl(gl_other.gl_other_debit_amount,0)  + nvl(glphystk_in.GL_PHYSTK_in_amount,0) ) ";
SQL = SQL  + "  - ";
SQL = SQL  + "   ( NVL(glphystk.GL_PHYSTK,0)  ";
SQL = SQL  + "   +     nvl(gltrnout.GL_stk_trn_out,0)  + nvl( glrmsales.GL_rm_salesamount,0)  + nvl(gl_other.gl_other_credit_amount,0)  ) GL_CLOSING,  ";
SQL = SQL  + " ( (m.openvalues + nvl(B.receipt,0) + nvl(c.stk_receipt,0) + nvl( rmphystin.phy_stk_in_RECD_AMT,0)  )-  ( NVL(d.issue,0) +nvl(e.stk_trn_out,0) +  nvl(f.rm_sales,0) )  ) ";
SQL = SQL  + "  -(( glop.GL_OPENING_VALUES + NVL(glrv.GL_RECEIPT,0) + nvl(GL_STK_TRN_RECEIPT ,0 ) +  nvl(glphystk_in.GL_PHYSTK_in_amount,0) +       nvl(gl_other.gl_other_debit_amount,0) ) ";
SQL = SQL  + "   - ( NVL(glphystk.GL_PHYSTK,0)  +     nvl(gltrnout.GL_stk_trn_out,0)  + nvl( glrmsales.GL_rm_salesamount,0)  + nvl(gl_other.gl_other_credit_amount,0)  ) ) FINAL_DIFF      ";
SQL = SQL  + " from ";
SQL = SQL  + " (  ";
SQL = SQL  + "         SELECT  ";
SQL = SQL  + "         RM_RMM_INV_ACC_CODE, GL_COAM_ACCOUNT_NAME , nvl( sum( RM_OB_AMOUNT),0) openvalues  ";
SQL = SQL  + "         FROM RM_OPEN_BALANCES ,  ";
SQL = SQL  + "         rm_rawmaterial_master ,  ";
SQL = SQL  + "         GL_COA_MASTER  ";
SQL = SQL  + "         where   ";
SQL = SQL  + "         rm_rawmaterial_master.RM_RMM_INV_ACC_CODE  =  GL_COA_MASTER.GL_COAM_ACCOUNT_CODE     ";
SQL = SQL  + "         and rm_rawmaterial_master.RM_RMM_RM_CODE =   RM_OPEN_BALANCES.RM_RMM_RM_CODE (+)  ";
SQL = SQL  + "         and  RM_OPEN_BALANCES.ad_fin_finyrid (+)= "+ FinYearID +"";
SQL = SQL  + "         group by RM_RMM_INV_ACC_CODE ,GL_COAM_ACCOUNT_NAME ";
SQL = SQL  + " )  M   ,         ";
SQL = SQL  + "  (  ";
SQL = SQL  + "     SELECT  GL_COAM_ACCOUNT_CODE , NVL(GL_COAB_OPN_BAL,0)  GL_OPENING_VALUES ";
SQL = SQL  + "     FROM GL_COA_BALANCES  ";
SQL = SQL  + "     WHERE  ";
SQL = SQL  + "     ad_fin_finyrid =  "+ FinYearID +"";
SQL = SQL  + "     AND gl_coam_account_code  in (  ";
SQL = SQL  + "       select  distinct RM_RMM_INV_ACC_CODE  from  rm_rawmaterial_master)  ";
SQL = SQL  + " ) glop         , ";
SQL = SQL  + " (  ";
SQL = SQL  + "     SELECT rm_.rm_rmm_inv_acc_code,  ";
SQL = SQL  + "     nvl(sum(RM_STKL_RECD_AMT),0) receipt  ";
SQL = SQL  + "     FROM rm_rawmaterial_master rm_, rm_stock_ledger  ";
SQL = SQL  + "     WHERE  rm_.rm_rmm_rm_code = rm_stock_ledger.RM_IM_ITEM_CODE  ";
SQL = SQL  + "     AND rm_stock_ledger.RM_STKL_TRANS_TYPE ='RECEIPT' ";
SQL = SQL  + "     AND rm_stock_ledger.ad_fin_finyrid =  "+ FinYearID +"";
SQL = SQL  + "     group by    RM_RMM_INV_ACC_CODE ";
SQL = SQL  + " ) b, ";
SQL = SQL  + " (  ";
SQL = SQL  + "     SELECT   ";
SQL = SQL  + "     gl_general_ledger.gl_coam_account_code  ,  ";
SQL = SQL  + "     SUM( gl_general_ledger.gl_gl_debit)   GL_RECEIPT ";
SQL = SQL  + "     FROM  gl_general_ledger  ";
SQL = SQL  + "     WHERE  ";
SQL = SQL  + "     gl_general_ledger.ad_fin_finyrid =  "+ FinYearID +"";
SQL = SQL  + "     AND gl_general_ledger.gl_gl_debit > 0  ";
SQL = SQL  + "     AND   GL_GL_TRANS_TYPE   =  'RECEIPT'    ";
SQL = SQL  + "     AND  gl_general_ledger.gl_coam_account_code  IN  ";
SQL = SQL  + "     (SELECT  DISTINCT rm_rmm_inv_acc_code FROM  ";
SQL = SQL  + "     rm_rawmaterial_master )  ";
SQL = SQL  + "     GROUP BY  ";
SQL = SQL  + "     gl_general_ledger.gl_coam_account_code  ";
SQL = SQL  + " ) glrv, ";
SQL = SQL  + "     (  ";
SQL = SQL  + "     SELECT rm_.rm_rmm_inv_acc_code,  ";
SQL = SQL  + "     nvl(sum(RM_STKL_RECD_AMT ),0) stk_receipt  ";
SQL = SQL  + "     FROM rm_rawmaterial_master rm_, rm_stock_ledger  ";
SQL = SQL  + "     WHERE  rm_.rm_rmm_rm_code = rm_stock_ledger.RM_IM_ITEM_CODE  ";
SQL = SQL  + "     AND rm_stock_ledger.RM_STKL_TRANS_TYPE ='STK_TRANS' ";
SQL = SQL  + "     AND rm_stock_ledger.ad_fin_finyrid =  "+ FinYearID +"";
SQL = SQL  + "     group by    RM_RMM_INV_ACC_CODE ";
SQL = SQL  + " ) c, ";
SQL = SQL  + "  (  ";
SQL = SQL  + "     SELECT   ";
SQL = SQL  + "     gl_general_ledger.gl_coam_account_code  ,  ";
SQL = SQL  + "     SUM( gl_general_ledger.gl_gl_debit)   GL_STK_TRN_RECEIPT ";
SQL = SQL  + "     FROM  gl_general_ledger  ";
SQL = SQL  + "     WHERE  ";
SQL = SQL  + "     gl_general_ledger.ad_fin_finyrid =  "+ FinYearID +"";
SQL = SQL  + "     AND gl_general_ledger.gl_gl_debit > 0  ";
SQL = SQL  + "     AND   GL_GL_TRANS_TYPE  ='STK_TRANS'   ";
SQL = SQL  + "     AND  gl_general_ledger.gl_coam_account_code  IN  ";
SQL = SQL  + "     (SELECT  DISTINCT rm_rmm_inv_acc_code FROM  ";
SQL = SQL  + "     rm_rawmaterial_master )  ";
SQL = SQL  + "     GROUP BY  ";
SQL = SQL  + "     gl_general_ledger.gl_coam_account_code  ";
SQL = SQL  + " ) glstkrv, ";
SQL = SQL  + " (  ";
SQL = SQL  + "     SELECT rm_.rm_rmm_inv_acc_code,   ";
SQL = SQL  + "     nvl(sum(RM_STKL_ISSUE_AMOUNT),0) issue  ";
SQL = SQL  + "     FROM rm_rawmaterial_master rm_, rm_stock_ledger  ";
SQL = SQL  + "     WHERE  rm_.rm_rmm_rm_code = rm_stock_ledger.RM_IM_ITEM_CODE  ";
SQL = SQL  + "     AND rm_stock_ledger.RM_STKL_TRANS_TYPE ='PHYSTK' ";
SQL = SQL  + "     AND rm_stock_ledger.ad_fin_finyrid =  "+ FinYearID +"";
SQL = SQL  + "     group by    RM_RMM_INV_ACC_CODE ";
SQL = SQL  + "  ) d , ";
SQL = SQL  + "  ( ";
SQL = SQL  + "     SELECt   ";
SQL = SQL  + "     gl_general_ledger.gl_coam_account_code  ,        ";
SQL = SQL  + "     SUM( gl_general_ledger.gl_gl_credit)   GL_PHYSTK ";
SQL = SQL  + "     FROM  gl_general_ledger  ";
SQL = SQL  + "     WHERE  ";
SQL = SQL  + "      gl_general_ledger.gl_gl_credit > 0  ";
SQL = SQL  + "     AND   GL_GL_TRANS_TYPE  IN( 'PHYSTK')   ";
SQL = SQL  + "     AND gl_general_ledger.ad_fin_finyrid =   "+ FinYearID +"";
SQL = SQL  + "     AND  gl_general_ledger.gl_coam_account_code  IN (  ";
SQL = SQL  + "     SELECT  DISTINCT rm_rmm_inv_acc_code FROM  ";
SQL = SQL  + "     rm_rawmaterial_master )  ";
SQL = SQL  + "     GROUP BY   ";
SQL = SQL  + "     gl_general_ledger.gl_coam_account_code  ";
SQL = SQL  + " ) glphystk , ";
SQL = SQL  + " (  ";
SQL = SQL  + "     SELECT rm_.rm_rmm_inv_acc_code,   ";
SQL = SQL  + "     nvl(sum(RM_STKL_RECD_AMT),0)  phy_stk_in_RECD_AMT ";
SQL = SQL  + "     FROM rm_rawmaterial_master rm_, rm_stock_ledger  ";
SQL = SQL  + "     WHERE  rm_.rm_rmm_rm_code = rm_stock_ledger.RM_IM_ITEM_CODE  ";
SQL = SQL  + "     AND rm_stock_ledger.RM_STKL_TRANS_TYPE ='PHYSTK' ";
SQL = SQL  + "     AND rm_stock_ledger.ad_fin_finyrid =  "+ FinYearID +"";
SQL = SQL  + "     group by    RM_RMM_INV_ACC_CODE ";
SQL = SQL  + "  ) rmphystin , ";
SQL = SQL  + "  ( ";
SQL = SQL  + "     SELECt   ";
SQL = SQL  + "     gl_general_ledger.gl_coam_account_code  ,        ";
SQL = SQL  + "     SUM( gl_general_ledger.gl_gl_debit)   GL_PHYSTK_in_amount ";
SQL = SQL  + "     FROM  gl_general_ledger  ";
SQL = SQL  + "     WHERE  ";
SQL = SQL  + "      gl_general_ledger.gl_gl_debit> 0  ";
SQL = SQL  + "     AND   GL_GL_TRANS_TYPE  IN( 'PHYSTK')   ";
SQL = SQL  + "     AND gl_general_ledger.ad_fin_finyrid =   "+ FinYearID +"";
SQL = SQL  + "     AND  gl_general_ledger.gl_coam_account_code  IN (  ";
SQL = SQL  + "     SELECT  DISTINCT rm_rmm_inv_acc_code FROM  ";
SQL = SQL  + "     rm_rawmaterial_master )  ";
SQL = SQL  + "     GROUP BY   ";
SQL = SQL  + "     gl_general_ledger.gl_coam_account_code  ";
SQL = SQL  + " ) glphystk_in ,  ";
SQL = SQL  + " (  ";
SQL = SQL  + "     SELECT rm_.rm_rmm_inv_acc_code,  ";
SQL = SQL  + "     nvl(sum(RM_STKL_ISSUE_AMOUNT ),0) stk_trn_out ";
SQL = SQL  + "     FROM rm_rawmaterial_master rm_, rm_stock_ledger  ";
SQL = SQL  + "     WHERE  rm_.rm_rmm_rm_code = rm_stock_ledger.RM_IM_ITEM_CODE  ";
SQL = SQL  + "     AND rm_stock_ledger.RM_STKL_TRANS_TYPE ='STK_TRANS' ";
SQL = SQL  + "     AND rm_stock_ledger.ad_fin_finyrid =  "+ FinYearID +"";
SQL = SQL  + "     group by    RM_RMM_INV_ACC_CODE ";
SQL = SQL  + "  ) e , ";
SQL = SQL  + "  ( ";
SQL = SQL  + "     SELECt   ";
SQL = SQL  + "     gl_general_ledger.gl_coam_account_code  ,        ";
SQL = SQL  + "     SUM( gl_general_ledger.gl_gl_credit)   GL_stk_trn_out ";
SQL = SQL  + "     FROM  gl_general_ledger  ";
SQL = SQL  + "     WHERE  ";
SQL = SQL  + "      gl_general_ledger.gl_gl_credit > 0  ";
SQL = SQL  + "     AND   GL_GL_TRANS_TYPE  IN(  'STK_TRANS' )   ";
SQL = SQL  + "     AND gl_general_ledger.ad_fin_finyrid =   "+ FinYearID +"";
SQL = SQL  + "     AND  gl_general_ledger.gl_coam_account_code  IN (  ";
SQL = SQL  + "     SELECT  DISTINCT rm_rmm_inv_acc_code FROM  ";
SQL = SQL  + "     rm_rawmaterial_master )  ";
SQL = SQL  + "     GROUP BY   ";
SQL = SQL  + "     gl_general_ledger.gl_coam_account_code  ";
SQL = SQL  + " ) gltrnout  , ";
SQL = SQL  + "  (  ";
SQL = SQL  + "     SELECT rm_.rm_rmm_inv_acc_code,  ";
SQL = SQL  + "     nvl(sum(RM_STKL_ISSUE_AMOUNT ),0) rm_sales ";
SQL = SQL  + "     FROM rm_rawmaterial_master rm_, rm_stock_ledger  ";
SQL = SQL  + "     WHERE  rm_.rm_rmm_rm_code = rm_stock_ledger.RM_IM_ITEM_CODE  ";
SQL = SQL  + "     AND rm_stock_ledger.RM_STKL_TRANS_TYPE ='RM_SALE' ";
SQL = SQL  + "     AND rm_stock_ledger.ad_fin_finyrid =   "+ FinYearID +"";
SQL = SQL  + "     group by    RM_RMM_INV_ACC_CODE ";
SQL = SQL  + "  ) f , ";
SQL = SQL  + "  ( ";
SQL = SQL  + "     SELECt   ";
SQL = SQL  + "     gl_general_ledger.gl_coam_account_code  ,        ";
SQL = SQL  + "     SUM( gl_general_ledger.gl_gl_credit)   GL_rm_salesamount ";
SQL = SQL  + "     FROM  gl_general_ledger  ";
SQL = SQL  + "     WHERE  ";
SQL = SQL  + "      gl_general_ledger.gl_gl_credit > 0  ";
SQL = SQL  + "     AND   GL_GL_TRANS_TYPE  IN(  'RM_SALE' )   ";
SQL = SQL  + "     AND gl_general_ledger.ad_fin_finyrid =  "+ FinYearID +"";
SQL = SQL  + "     AND  gl_general_ledger.gl_coam_account_code  IN (  ";
SQL = SQL  + "     SELECT  DISTINCT rm_rmm_inv_acc_code FROM  ";
SQL = SQL  + "     rm_rawmaterial_master )  ";
SQL = SQL  + "     GROUP BY   ";
SQL = SQL  + "     gl_general_ledger.gl_coam_account_code  ";
SQL = SQL  + " ) glrmsales , ";
SQL = SQL  + " (  ";
SQL = SQL  + "       select gl_coam_account_code,  ";
SQL = SQL  + "     sum(gl_gl_debit)   gl_other_debit_amount,  ";
SQL = SQL  + "     sum(gl_gl_credit) gl_other_credit_amount  ";
SQL = SQL  + "     from gl_general_ledger  ";
SQL = SQL  + "     where  ";
SQL = SQL  + "     GL_GL_TRANS_TYPE NOT IN ('RECEIPT','PHYSTK', 'STK_TRANS','RM_SALE')  ";
SQL = SQL  + "     and  ad_fin_finyrid =  "+ FinYearID +"";
SQL = SQL  + "     AND gl_coam_account_code  in (  ";
SQL = SQL  + "     SELECT  DISTINCT rm_rmm_inv_acc_code FROM  ";
SQL = SQL  + "     rm_rawmaterial_master)  ";
SQL = SQL  + "     group by gl_coam_account_code  ";
SQL = SQL  + " )      gl_other   ";
SQL = SQL  + "  where  ";
SQL = SQL  + " m.RM_RMM_INV_ACC_CODE  =glop.GL_COAM_ACCOUNT_CODE (+)  ";
SQL = SQL  + " AND m.RM_RMM_INV_ACC_CODE  = b.RM_RMM_INV_ACC_CODE  (+) ";
SQL = SQL  + " AND m.RM_RMM_INV_ACC_CODE  =glrv.GL_COAM_ACCOUNT_CODE (+)  ";
SQL = SQL  + " AND m.RM_RMM_INV_ACC_CODE  = c.RM_RMM_INV_ACC_CODE  (+) ";
SQL = SQL  + " AND m.RM_RMM_INV_ACC_CODE  =glstkrv.GL_COAM_ACCOUNT_CODE (+)  ";
SQL = SQL  + " AND m.RM_RMM_INV_ACC_CODE  =d.RM_RMM_INV_ACC_CODE  (+) ";
SQL = SQL  + " AND m.RM_RMM_INV_ACC_CODE  =glphystk.GL_COAM_ACCOUNT_CODE (+)  ";
SQL = SQL + " AND m.RM_RMM_INV_ACC_CODE  =rmphystin.RM_RMM_INV_ACC_CODE  (+) ";
SQL = SQL  + " AND m.RM_RMM_INV_ACC_CODE  =glphystk_in.GL_COAM_ACCOUNT_CODE (+)   ";
SQL = SQL  + " AND m.RM_RMM_INV_ACC_CODE  =e.RM_RMM_INV_ACC_CODE  (+) ";
SQL = SQL  + " AND m.RM_RMM_INV_ACC_CODE  =gltrnout.GL_COAM_ACCOUNT_CODE (+)  ";
SQL = SQL  + " AND m.RM_RMM_INV_ACC_CODE  =f.RM_RMM_INV_ACC_CODE  (+) ";
SQL = SQL  + " AND m.RM_RMM_INV_ACC_CODE  =glrmsales.GL_COAM_ACCOUNT_CODE (+)  ";
SQL = SQL  + " AND m.RM_RMM_INV_ACC_CODE  =gl_other.GL_COAM_ACCOUNT_CODE (+)  ";
SQL = SQL  + " order by  m.RM_RMM_INV_ACC_CODE asc ";
 

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {

                return null;
            }
            return dtData;
        }



        
    }
}
