from flask import Blueprint
from ..getdata import Getting_data_and_predictions

forecast_blue = Blueprint("forecast", __name__,static_folder="../static", template_folder="../templates")



@forecast_blue.route("/")
def forecast():

    try:    
        dataframe_query_product_spend = Getting_data_and_predictions().query_product_spend()
    except Exception as e:
        return "error in query_product_spend:\n"+str(e)
    try:
        Getting_data_and_predictions().dataframe_to_aiven(dataframe=dataframe_query_product_spend)
    except Exception as e:
        return "error in dataframe_to_aiven:\n"+ str(e)

    try:
        dataframe_predictions=Getting_data_and_predictions().get_data_to_forecast()    
    except Exception as e:
        return "error in get_data_to_forecast:\n"+str(e)


    try:
        Getting_data_and_predictions().make_predictions(dataframes=dataframe_predictions)
    except Exception as e:
        return "error in make_predictions:\n"+str(e)



    return "end forecasting"