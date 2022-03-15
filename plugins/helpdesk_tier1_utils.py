import os
import time
import requests

def paginate_all_issue(url, headers,params):
    """
    path에 해당하는 github 레포의 **모든** issue 를 json 이터레이터를 변환하여
    반환하는 함수
    """
    
    while url:
        response = requests.get(url, headers=headers, params=params)
        time.sleep(1)
        if response.status_code == 204: # 204 : 반환되는 데이터가 없음 (빈 리스트 객체임)
            return None

        issues_json = response.json()
        try :
            url = response.links.get("next").get("url")
        except AttributeError:
            url = None
            
        yield issues_json

def extract_issues(repo_owner, repo_name, token):
    """
    레포지토리의 모든 issue 를 가져온다
    - repo_owner, repo_name 에 있는 모든 issue 를 수집한다.
    - *주의 : Pull Request 도 이슈에 포함되어 있음
    """

    repo_path = f"{repo_owner}/{repo_name}"
    headers = {"Authorization": f"token {token}"}
    params = {
    "state" : "all",
    "filter" : "all"
    }
    url = f"https://api.github.com/repos/{repo_path}/issues"
    all_issues = paginate_all_issue(url=url,headers=headers,params=params)
    return all_issues

def extract_comments(comment_url, token):
    """
    comment_url 에 해당하는 request 처리를 한후에
    response 객체를 반환한다.
    """

    headers = {"Authorization": f"token {token}"}
    comments = requests.get(comment_url,headers=headers)
    time.sleep(1)
    return comments