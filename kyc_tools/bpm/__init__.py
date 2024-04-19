import requests

bpm_env = "http://bpmuatps1.gf.com.cn:8080"
#BPM查看流程
def open(docid : str , user : str):
    open_url = bpm_env + "/api/v2/document/open"
    data = {"documentId":docid,
            "user":user}
    response = requests.post(url=open_url,data=data)
    taskId = response.json()["data"]["userTask"]["id"]
    # currentOwner = response.json()["data"]["document"]["currentOwner"].split("|")[1]
    # print(f"taskId : {taskId},currentOwner : {currentOwner}")
    # return response.json()["data"]["userTask"]["id"]
    return taskId

#BPM撤销流程
def cancel_in_bpm(docId : str,taskId : str,user : str):
    cancel_url = "/api/v2/engine/cancel"
    data = {"documentId":docId,"taskId":taskId,"user":user}
    print(data)
    response = requests.post(url = bpm_env+cancel_url,data=data)
    print(response.json())
