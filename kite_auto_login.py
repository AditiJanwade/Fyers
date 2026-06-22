import os
import pyotp
import requests
from kiteconnect import KiteConnect
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv, set_key

load_dotenv()
# --- Configuration ---
API_KEY = os.getenv("KITE_API_KEY")
API_SECRET = os.getenv("KITE_API_SECRET")
USER_ID = os.getenv("KITE_USER_ID")
PASSWORD = os.getenv("KITE_PASSWORD")
TOTP_SECRET = os.getenv("KITE_TOTP_SECRET")

def execute_kite_login_flow():
    """
    Implements Zerodha's official OAuth2 handshake programmatically:
    1. Post user credentials to /api/login -> yields request_id
    2. Post TOTP code to /api/twofa -> yields session context
    3. Traverses /connect/login redirect loop -> extracts request_token
    4. Computes SHA-256 checksum -> yields final daily access_token
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })
    
    # --- PHASE 1: Base Login Request ---
    print("[1/4] Submitting primary credentials...")
    login_res = session.post(
        url="https://kite.zerodha.com/api/login",
        data={"user_id": USER_ID, "password": PASSWORD}
    )
    login_data = login_res.json()
    if login_data.get("status") != "success":
        raise Exception(f"Login failed: {login_data.get('message')}")
        
    request_id = login_data["data"]["request_id"]
    
    # --- PHASE 2: 2FA Verification ---
    print("[2/4] Generating and verifying TOTP...")
    totp_token = pyotp.TOTP(TOTP_SECRET).now()
    twofa_res = session.post(
        url="https://kite.zerodha.com/api/twofa",
        data={
            "user_id": USER_ID,
            "request_id": request_id,
            "twofa_value": totp_token,
            "twofa_type": "totp"
        }
    )
    if twofa_res.json().get("status") != "success":
        raise Exception("2FA Verification rejected. Check your TOTP_SECRET key.")

    # --- PHASE 3: Tracking Redirections to capture 'request_token' ---
    print("[3/4] Traversing redirect chain for request_token...")
    initial_login_endpoint = f"https://kite.zerodha.com/connect/login?v=3&api_key={API_KEY}"
    
    current_target_url = initial_login_endpoint
    request_token = None
    
    # Manually step through the 302 jumps (Login -> Finish -> Developer App Landing Redirect)
    for jump in range(5):
        response = session.get(current_target_url, allow_redirects=False)
        
        if response.status_code in [301, 302]:
            next_destination = response.headers.get("Location")
            
            # Reconstruct relative redirect boundaries safely
            if next_destination.startswith("/"):
                next_destination = urljoin("https://kite.zerodha.com", next_destination)
                
            print(f"    -> Jump {jump+1}: Moved to {next_destination[:60]}...")
            
            # Parse parameters to scan for the token
            parsed_query = parse_qs(urlparse(next_destination).query)
            if "request_token" in parsed_query:
                request_token = parsed_query["request_token"][0]
                print("✅ Extracted request_token from landing redirect boundaries.")
                break
                
            current_target_url = next_destination
        else:
            raise Exception(f"Redirection sequence broken at status code {response.status_code}")

    if not request_token:
        raise Exception("Redirection flow completed without identifying request_token.")

    # --- PHASE 4: Fetch Access Token & Verify Session ---
    print("[4/4] Calculating SHA-256 checksum & swapping for access_token...")
    
    # The official Kite SDK library can handle the token swap and checksum internally:
    try:
        env_file = ".env"
        kite = KiteConnect(api_key=API_KEY)
        session_payload = kite.generate_session(request_token, api_secret=API_SECRET)
        set_key(env_file, "KITE_ACCESS_TOKEN", session_payload["access_token"])
        # Configure client wrapper state
        kite.set_access_token(session_payload["access_token"])
        print("🎉 Login flow successful! System access token locked.")
        return kite
        
    except Exception as error:
        print(f"❌ Failed token authentication via API endpoint: {error}")
        return None

# ==========================================
# EXECUTION ENTRYPOINT
# ==========================================
if __name__ == "__main__":
    try:
        active_kite_session = execute_kite_login_flow()
        if active_kite_session:
            # Quick status call to test your active operational window
            user_profile = active_kite_session.profile()
            print(f"\nConnected Account Profile: {user_profile.get('user_name')} ({user_profile.get('user_id')})")
    except Exception as failure:
        print(f"\nExecution Aborted: {failure}")