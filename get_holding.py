import os
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

def get_holdings():
    
    fyers = fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=ACCESS_TOKEN,
        log_path=""
    )

    response = fyers.holdings()

    return response


if __name__ == "__main__":
    holdings = get_holdings()
    print(holdings)