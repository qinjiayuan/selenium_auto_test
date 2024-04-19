
#运管获取cookie
import requests


def get_code(user):
    url = 'https://uatoauth2.gf.com.cn/ws/pub/code'
    params = {"no302redirect": "1",
              "login_type": "oa",
              "client_id": "otcoms",
              "redirect_uri": "https://otcoms-test.gf.com.cn/spsrest/auth/user/login",
              "user_id": user,
              "password": "Gfte5tHw2022!"}
    response = requests.post(url=url, data=params)
    result = response.json()
    print(result)
    code = str(result["redirect"]).split("=")[1]
    print("code is %s"%code)
    getToken = response.cookies.get_dict()
    print("getToken is %s"%getToken)

    login_url = "https://otcoms-test.gf.com.cn/spsrest/auth/user/login"
    params = {"code":code}
    login_response = requests.get(url=login_url,params=params)
    getCookie = login_response.cookies.get_dict()
    print("getCookie is %s"%getCookie)

    # cookie = "LtpaToken2=%s"%getToken["LtpaToken2"] +";"+"OAUTH_TOKEN=%s"%getCookie["OAUTH_TOKEN"]+";appcmssid=%s"%getCookie["appcmssid"]
    cookie =  "OAUTH_TOKEN=%s" % getCookie[
        "OAUTH_TOKEN"] + ";appcmssid=%s" % getCookie["appcmssid"]
    return cookie
