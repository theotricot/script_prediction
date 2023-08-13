from google.cloud import bigquery
from datetime import datetime
from dateutil import relativedelta
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import pmdarima as pm
from dateutil.relativedelta import relativedelta
from datetime import datetime
from sklearn.metrics import mean_absolute_percentage_error

from sqlalchemy import create_engine, MetaData, DateTime
from sqlalchemy.orm import Session
from dotenv import load_dotenv
import os 
from sqlalchemy import Table


class Getting_data_and_predictions():
    def __init__(self):

        date_1, date_2 = datetime.now(), datetime.now() - relativedelta(months=1)
        month_1, month_2 = date_1.month, date_2.month
        year_1, year_2 = date_1.year, date_2.year
        self.date_1, self.date_2 = f"{year_1}-{month_1}-01", f"{year_2}-{month_2}-01"



    def query_product_spend(self):


        client = bigquery.Client()
        query_job = client.query(
            f"""
            SELECT period,sum(consumed_price) as price, customer_product_uuid,B.title FROM `dtp-portal-dtep.dtpbilling.billing_units` as A
            left join `dfdp-digitaldata-repositories.rapid.digital_product_v1` as B
            on A.customer_product_uuid=B.id
            where B.title in('HOMMY', 'DAMEO', 'COOKIE STORE', 'COOKIE WEB', 'STREAMSERVE', 'COLIBRI MANAGE', 'COLIBRI SALES', 'PVR', 'PICK UP PLANNING', 'OPUS', 'OPECOM V2', 'COLIBRI QBAT', 'PYXIS', 'SCR-SIRIUS CUSTOMER REPOSITORY', 'IKY  (I KNOW YOU) - CUSTOMER DATA MANAGEMENT', 'MDM-CLIENT', 'DATA QUALITY', 'QUOTATION', 'BOOSTER', 'OPTIMEEZE', 'MOVE CUSTOMER ORDER', 'CUSTOMER BOARD', 'URL SHORTENER', 'NOTIFY', 'STORE SIS', 'ORION', 'SOLUTION OFFER DESIGN', 'SOLDE AUTOMATIQUE', 'CASSIOPEE', 'BU PERFORMANCE', 'CIAM', 'CUSTOMER MOBILE APP', 'DATA COMPLIANCE', 'PADPUMP', 'OLT', 'STC', 'QB', 'HOMEVISITS', 'PRODUCT REVIEWS', 'E-REPUTATION', 'COLIBRI SCAN EVERYTHING', 'COLIBRI PASSPORT', 'COOKIE PORTAL & REPOSITORY', 'COLIBRI TASKLIST', 'PYXIS SAPHIR', 'LOCUS', 'NEXT PRODUCT TO BUY', 'WLL', 'LYS - LOYALTY SERVICES', 'FLOYD - FRONT LOYALTY DESK', 'POCKET DATA', 'LOYALTY MOBILE APP', 'OMNICARE', 'PRINTMANAGER', 'DOPANIM', 'PRODUCT FEED', 'TMS - TAG COMMANDER', 'SEARCHDEX', 'GOALKEEPER', 'SEO URL RESOLVER', 'SUIVI DE POSITION', 'PRM PLATEFORME RELATION MAGASIN', 'OREO', 'LYSA - LOYALTY SERVICE ACCOUNT', 'COMMUNITY SPACE', 'INHABITANTS JOURNEY INSIGHTER', '1 - AVAILABILITY - TRANSVERSE AUTOMATED TEST (SIT)', '2 - AVAILABILITY - ARCHITECTURE DIAGRAM', '3 - AVAILABILITY - MAINTENANCE OF TRANSVERSAL SIT/UAT ENV.', '4 - USABILITY - TRANSVERSE AUTOMATED TEST (UAT)', '5 - USABILITY - TRAINING GUIDES', '6 - FACILITATION - DELEGATION', '7 - FACILITATION - LOCAL IT PROJECTS', '8 - SATISFACTION - QUALITY', '9 - SATISFACTION - MONITORING UAT AND PROD ENV.', 'VFP - ONLINE INSTALLATION OFFERS', 'WAC-WEBACCOUNT', 'OPECOM V1', 'PAYMENT CROSS TECHNOLOGIES', 'FIDLIGHT', 'TEMPO', 'TO DO FOR SALES', '0 - STARTER PACK', 'GESTION CLIENT (WCM)', 'MOVE CUSTOMER', 'MAILBREW', 'OFFER REPOSITORY', 'BUDGET ESTIMATE', 'PROJECT BOOSTER', 'BACK OFFICE MARKETPLACE - BOMP', 'FABRIQUE À API', 'COLIBRI LAUNCHER', 'COLIBRI PRODUCT SHEET', 'COLIBRI PERMANENT INVENTORY', 'CUSTOMER FACTORY', 'COLIBRI GREENZONE PACTOOL', 'IFIXIT PARTNERSHIP', 'VISUAL RECOGNITION', 'METEOR', 'CONFIGURATORS REPOSITORY AND COMMON', 'MESSAGEO', 'DELTA', 'COOKIE PAYOUT', 'COOKIE ESCROW', 'LYS ABILITIES', 'LYS KPI', 'ULYS', 'TREND ME NOW', 'LOYALTY ACCOUNT MANAGEMENT', 'PDC', 'STORE REVIEWS', 'CUSTOMER CARE CHATBOT PLATFORM', 'AB TESTING TOOL - OPTIMIZELY', 'MEDIA PICKER', 'CCDP - COMMON INFRASTRUCTURE', 'PRODUCT MATCH AND MERGE - PMM', 'FISCAL INVENTORY', 'COMMERCIAL EDITORIAL CONTENT', 'LEGACY TRANSLATOR', 'SITEMAP', 'PAYMENT DATA REPOSITORY', 'MEDIA INTEGRATOR', 'CUSTOMER API 360', '(TOUCANTOCO) LOYALTY DASHBOARD', 'MOVE LOYALTY', 'BOOMERANG (INTERACTIVE RETURN SYSTEM)', 'DIGITAL DOCUMENT GENERATOR BRICK (DDGB)', 'RSS LABELS GENERATOR', 'MODEL SELECTOR', 'QUESTION AND ANSWERS', 'SALESFORCE SELLERS', 'SELLER MANAGEMENT', 'DOP - DIGITAL OFFER PLATFORM', 'GENESYS CLOUD - CONTACT CENTER SOLUTION', 'COUPON REPOSITORY', 'AUTOCAPAYIN', 'OPUS LEGACY', 'AUTHORIZATIONS', 'SELLER ORDER MANAGEMENT', 'SELLER PORTAL', 'CATALOG OF PRODUCT AND SERVICES', 'BRIQUE DE COTATION (BDC)', 'GEOTREND', 'CCDP - PERFORMANCE & RELIABILITY', 'OPCOM ADVISOR', 'CONTEXT REPOSITORY', 'S-MONEY', 'MIRAKL', 'MEDIA ORDER MANAGER', 'COSMIC (OPERATOR PORTAL)', 'PRODUCT PUBLICATION TRACKING', 'TRAITEMENT DES JOURNEES (COMPTA)', 'PROJECT TOOLS DIRECTORY', 'OPUS RECOMMENDATION', 'OMNISTORE', 'CYBERSECURITY CCDP', 'SELF LOYALTY', 'COLIBRI GESTION DES REFLUX', 'SELLERS REVIEWS', 'SELLER DELIVERY REPOSITORY', 'METATAG', 'WISHLISTS', 'DAM API', 'MARKETPLACE ONLINE HELP', 'THOR', 'POOLPARTY', 'CUSTOMER EXPERIENCE MEASURE', 'KOBI MODULES - CUSTOMER SPACE', 'KOBI MODULES - CUSTOMER LOGIN', 'KOBI MODULES - CUSTOMER HELP', 'KOBI MODULES - PRODUCT REVIEWS', 'KOBI MODULES - PRODUCT CATEGORIES', 'KOBI MODULES - PRODUCT DETAILS', 'KOBI MODULES - ADD TO CART', 'KOBI MODULES - ADD TO WISHLIST', 'KOBI MODULES - PRODUCT RECOMMENDATION', 'KOBI MODULES - SEARCH', 'KOBI MODULES - PROJECT TOOLS DIRECTORY', 'KOBI MODULES - CALCULATORS', 'KOBI MODULES - CRO', 'KOBI MODULES - CROSS COMPONENTS', 'KOBI MODULES - SEO PAGES GENERATOR', 'KOBI MODULES - MKP SELLERS', 'KOBI MODULES - EDITORIAL', 'KOBI MODULES - HOMEPAGE', 'KOBI MODULES - STORE', 'KOBI MODULES - CHECKOUT', 'ORDER FOLLOW UP #OFU', 'KOBI MODULES - CUSTOMER RETURN', 'WEBSITE CORE', 'KOBI MODULES - SERVICES', 'GOODWILL', 'SCORING FOR CASHING', 'OPUS CONTENT', 'OPUS CATEGORY', 'OPUS NAVIGATION', 'OPUS SEARCH', 'OPUS DIY KNOWLEDGE GRAPH', 'OPUS TAXONOMY', 'OPUS RANKING', 'ENGINEERING BOARD', 'CHECKOUT CONTROL', 'GIFTEO', 'OPUS INTERFACE', 'BOMP PRODUCTS', 'BOMP OFFERS', 'LOCAL TOOLBOX', 'OPUS CATALOGS BROADCAST', 'OPUS PRODUCT AND DECISION', 'NDB - NEW DEPOSIT BRICK', 'POSLOG ACL COFFRE', 'POSLOG', 'LEGACY MEDIA', 'ZENCARE', 'PMEDIA SERVER', 'COLIBRI COUNTING SCAN LOG', 'SELLER SERVICES MANAGEMENT', 'SELLER PRICING', 'AUDIENCE SHARING', 'PAYMENT TRANSACTION DATA FOR REFUND (PTDR)', 'LOYALTY-ORCHESTRATOR', 'POSLOG ACL TIC', 'COLOR SIMULATOR', 'MULTIPLE REVIEWS APP', 'EPRIVACY COMPLIANCE', 'BOMP POLLER OFFER PRODUCT', 'DASHBOARD DATA PRIVACY CCDP', 'BUNDLE INSTALLER', 'ENGINEERING QUALITY', 'POSLOG ACL TICCAI', 'KOBI MODULES - MKP PRODUCT CATALOG MANAGEMENT', 'KOBI MODULES - MKP OFFER CATALOG MANAGEMENT', 'KOBI MODULES - MKP INVOICING', 'KOBI MODULES - MKP ORDER MANAGEMENT', 'ELO', 'KOBI MODULES - QUESTION AND ANSWERS', 'SOFTWARE ARCHITECTURE TOOLS', 'KOBI MODULES - MKP PORTAL CROSS COMPONENTS', 'BUNDLE INSTALLER FRONT', 'MARKETING DATAFACTORY', 'PRODUCT MEDIA BOARD', 'INVENTORY AUDIT COUNTING', 'ENGINEERING TOOLS', 'KOBI MODULES - COLLABORATORS GESTURE', 'KOBI MODULES - SALES FOLLOWUP', 'SERVICE INVOICING', 'FLASH REPORT', 'PUDO CONNECTOR', 'INSTALLATION DOCUMENTS SIGNATURE MANAGEMENT', 'KOBI MODULES - SHARING', 'SERVICE MASTER DATA', 'OTR - OPERATIONAL TEAM REPOSITORY', 'ORDERSWITCHER', '3D CLOSET CONFIGURATOR', 'SERVICE PROVIDER MOBILE APP (SMA)', 'KOBI MODULES - GIFT CARDS', 'SAO - TRANSVERSAL TESTS AND RESOURCES', 'KOBI MODULES - TEMPO COLLECT', 'INTERNAL DIGITAL ANALYTICS TRACKER LEROY MERLIN', 'PROJECT EXECUTION', 'SERVICE PROVIDER MATCHING', '3D MEDIA MANAGEMENT (POC)', 'KOBI MODULES - NAVIGATION', 'KOBI MODULES - CATEGORIES ENRICHMENT', 'SERVICE OFFER BUILDER', 'AHS KPI CALCULATION', 'SERVICE SIMULATION TOOL', 'BACK FOR CARE', 'MOBILE PRODUCT SEARCH', 'COMMON ANALYTIC TOOL - PIANO ANALYTICS', 'KOBI MODULES - TMS', 'KOBI MODULES - AB TESTING', 'LYS STEWARD', 'QUALITY DASHBOARD', 'C360V - CUSTOMER 360 VIEW FOR CONTACT CENTER', 'UP20', 'CLOUD ADEO IPXE', 'LMES-OLT', 'KOBI MODULES - CUSTOMER RETURN FOLLOW UP FOR COLLABORATOR', 'SELLER FULFILMENT MANAGEMENT', 'SERVICE  OPERATOR PORTAL', 'KOBI MODULES - PRIVACY', 'ADOBE CAMPAIGN V8', 'KOBI MODULES - RUM', 'AHS - TRANSVERSE OPERATIONS', 'SERVICE PROVIDER ONBOARDING (SPO)', 'SERVICE PROVIDER MASTER DATA', 'CUSTOMER KNOWLEDGE BOARD', 'TEMPO PAYMENT EXECUTION POLICY', 'EASYPLAN', 'MDM-PLACES', 'SERVICE EXECUTION ENGINE', 'CASHING CART FRAUD', 'SHELFY', 'MYSTICA', 'KOBI MODULES - NON STANDARD PRODUCTS CONFIGURATOR', 'AI PROJECT SCORING & ACTIVATION (POC)', 'ADEO HOME SERVICES ONLINE HELP', 'MEDIA INTEGRATOR V2', 'KOBI MODULES - RETAIL MEDIA', 'LOCAL TEST', 'SXP LEGACY', 'SXP OPS & RUN', 'MOBILE APP SHARED COMPONENT AND TOOLING FOR SALES AREA', 'BOOSTER MOBILE', 'SERVICE PROVIDER PORTAL', 'AHS SALESFORCE SERVICE PROVIDERS', 'AHS COMMUNICATION CENTER', 'IMPLANT PICKING', 'RELANCE DEVIS', 'TACTIC MOBILE', 'ANALYTICS -DATA ENRICHMENT FOR SERVER SIDE', 'TEMPO ADAPTERS', 'KOBI MODULES - WISHLISTS', 'MEDIA PINNER', 'KOBI MODULES - CUSTOM AND CONTEXT', 'SALESFORCE FOR RETAIL MEDIA', 'KAMINO', 'ENGINEERING CXP TOOLS', 'OVERWATCH')
            and period < '{self.date_1}' and period >= '{self.date_2}'
            group by period, customer_product_uuid, B.title
            order by period asc"""
                )

        results = query_job.result()  # Waits for job to complete.

        query_df = query_job.to_dataframe()


        df = query_df[[ 'period', 'price', 'customer_product_uuid', 'title']]


        return df



    def dataframe_to_aiven(self, dataframe):


        load_dotenv(override=True)
        SQLALCHEMY_DATABASE_URL = os.environ.get("AIVEN")
        engine = create_engine(SQLALCHEMY_DATABASE_URL)
        meta = MetaData()

        product_spend_table = Table("Product_spend", meta, autoload_with= engine)
        date = dataframe.period.max()
        with Session(engine) as session:
            query_job=session.query(product_spend_table).filter_by(period=str(date)).all()
        
        if len(query_job)>=1:
            print("already have this data")
            pass
        else:
            coltype = {"period":DateTime}
            dataframe.to_sql("Product_spend", con=engine, if_exists="append",type=coltype)



        

    def make_predictions(self, dataframes):


        load_dotenv(override=True)
        SQLALCHEMY_DATABASE_URL = os.environ.get("AIVEN")
        engine = create_engine(SQLALCHEMY_DATABASE_URL)
        meta = MetaData()
        
        ### connect to sql table
        prediction_table = Table("prediction", meta, autoload_with= engine)

        date = datetime.now() -relativedelta(months=2)
        date = f"{date.year}-{date.month}-01"

        ## get all products from dataframe
        products = dataframes.title.unique()
        print("\n\n\n\n\n_______________1_________________\n\n\n\n")
        
        for product_name in products:
            ## loop on products to predict expenses

            ###create dataframe for selected product
            dataframe = dataframes.loc[dataframes.title == product_name]

            ### convert date string to datetime and sort by date
            dataframe["period"] = pd.to_datetime(dataframe["period"])
            dataframe = dataframe.sort_values("period")

            ### create mask to split dataframe to train and test (test for get Mean_Absolute_Percentage_Error (mape)(metric most use for timeseries))
            mask = (dataframe.period<date)
            product_title=dataframe.title.values[0]
            dataframe_test = dataframe.loc[-mask]
            dataframe_train = dataframe.loc[mask]


            if dataframe_test.shape[0]<1:
                continue
            if dataframe_train.shape[0]<3:
                continue


            try:
            ### init model, set parameters and train with training dataframe
                
                try:
                    model = pm.auto_arima(dataframe_train[["price"]], start_p=1, start_q=1,
                                        test='adf',
                                        suppress_warnings=True,
                                        stepwise=True)
                    model_name = "auto_arima"
                except:
                    model = pm.auto_arima(dataframe_train[["price"]], start_p=1, start_q=1,
                                        test='kpss',
                                        suppress_warnings=True,
                                        stepwise=True)
                    model_name = "auto_arima"

                

                ### make predictions
                prediction, confint = model.predict(start=dataframe_train.shape[0], n_periods=6, return_conf_int=True)
                
                
                ### to put existing months forecast in list to calculate mape
                y_pred = []
                pred = prediction.to_list()
                for i in range(dataframe_test.shape[0]):
                    y_pred.append(pred[i])

                ### mape
                
                mape = mean_absolute_percentage_error(y_pred, dataframe_test["price"])
                
                ## add predictions to sql table
            except Exception as e:
                print("\n\n\n\n\n__________\nfail to predict for: ",product_name,"\n__________\n\n\n\n\n\n")

                continue
            

            start = 1
            for y in prediction:
                int_min = confint[start - 1][0]
                int_max = confint[start -1][1]
                with Session(engine) as session:
                    query_job = session.query(prediction_table.c.product, prediction_table.c.date).filter_by(product=f"{product_title}", date=f"2023-{int(dataframe_train.period.max().month)+start}-01").all()
                    if len(query_job)>=1:

                        new = prediction_table.update().where(prediction_table.c.product==f"{product_title}", prediction_table.c.date==f"2023-{int(dataframe_train.period.max().month)+start}-01").values(prediction = float(y),
                                                product=str(product_title),
                                                date=f"2023-{int(dataframe_train.period.max().month)+start}-01",
                                                mape = float(mape),
                                                interval_min=int_min,
                                                interval_max=int_max,
                                                model_name=str(model_name)
                                                )
                    else:
                        
                        
                        new = prediction_table.insert().values(prediction = float(y),
                                                product=str(product_title),
                                                date=f"2023-{int(dataframe_train.period.max().month)+start}-01",
                                                mape = float(mape),
                                                interval_min=int_min,
                                                interval_max=int_max,
                                                model_name="auto_arima"
                                                )
                    session.execute(new)
                    session.commit()    
                start+=1






    def get_data_to_forecast(self):


        load_dotenv(override=True)
        SQLALCHEMY_DATABASE_URL = os.environ.get("AIVEN")
        engine = create_engine(SQLALCHEMY_DATABASE_URL)
        meta = MetaData()
        
        ### connect to sql table
        product_spend_table = Table("Product_spend", meta, autoload_with= engine)


        with Session(engine) as session:
            query_job = session.query(product_spend_table.columns.period,product_spend_table.columns.price,product_spend_table.columns.title).distinct().all()
            df = pd.DataFrame.from_records(query_job, columns=["period","price","title"])



        return df



