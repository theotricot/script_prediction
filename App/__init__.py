from flask import Flask


def create_app():
    app = Flask(__name__)

    from .routes.forecast import forecast_blue

    app.register_blueprint(forecast_blue)

    
    return app


app=create_app()