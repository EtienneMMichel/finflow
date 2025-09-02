import requests
import json
from .exceptions import LoginException

import requests
import json

PAYLOAD_URL = "https://forcastweb.vercel.app"


def login(user, password):
    payload = json.dumps({
        "email":user,
        "password":password
    })
    headers = {
    'Content-Type': 'application/json',
    }
    response = requests.post(f"{PAYLOAD_URL}/api/users/login",data=payload, headers=headers)
    if response.status_code != 200:
        raise LoginException(f"[{response.status_code}]Failed to login at users with user: {user}: {response.text}")
    response = json.loads(response.text)
    if response.get("message", None) != "Authentication Passed":
        raise LoginException(f"Failed to login at users with user: {user}: {response.text}")
    if response.get("user", {"role":None})["role"] != "admin":
        raise LoginException(f"Failed to login at users with user because user is not admin: {user}: {response.text}")
    if response.get("token", None) is None:
        raise LoginException(f"Failed to login at users with user because cannot find token: {user}: {response.text}")
    if response.get("exp", None) is None:
        raise LoginException(f"Failed to login at users with user because cannot find generation date: {user}: {response.text}")
    return {
        "token":response["token"],
        "exp":response["exp"],
    }
    
    
