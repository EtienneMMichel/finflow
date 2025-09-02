from ..exceptions import LoginException
import supabase
import json
import os
from dotenv import load_dotenv
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")



class SupaClient():
    async def __init__(self, supabase_url=None, supabase_key=None):
        supabase_url = SUPABASE_URL if supabase_url is None else supabase_url
        supabase_key = SUPABASE_KEY if supabase_key is None else supabase_key
        self.client = supabase.AClient(supabase_url=SUPABASE_URL, supabase_key=SUPABASE_KEY)
        self.is_logged = False
        

    async def login(self, username=None, password=None):
        response = await self.client.auth.sign_in_with_password(
            {
                "email": username,
                "password": password,
            }
        )
        if json.loads(response.model_dump_json()).get("user", {"aud":None}).get("aud", None) != "authenticated":
            raise LoginException("Failed to login via supabase")
        self.is_logged = True

    async def set_data(self, table_name, data):
        if not self.is_logged:
            await self.login()
        response = await (
            self.client.table(table_name)
            .insert(data)
            .execute()
        )
        # print(response)

    async def get_data(self, table_name):
        if not self.is_logged:
            await self.login()
        response = await (
            self.client.table("table_name")
            .select("*")
            .execute()
        )
        # print(response)