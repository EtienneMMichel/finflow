import asyncio
from finflow.utils.login import login
from finflow.utils import supa_conn

def test_login():
    pass
    # response = login(user=user, password=password)
    # print(response)

def test_login_supa_conn():
    supa_conn.login("email", "mdp")

if __name__ =="__main__":
    test_login_supa_conn()