import io
import os
import time
from tqdm import tqdm
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload, HttpError


PORT = 29999
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]


def run(file_id: str, save_dir: str, credentials_path: str):
    """
    运行下载任务，自动获取文件名，并存储到指定目录
    :param file_id: Google Drive 文件 ID
    :param save_dir: 下载目录
    :param credentials_path: 认证凭据文件路径
    :return: 下载是否成功
    """
    try:
        creds = None
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_path, SCOPES
                )
                creds = flow.run_local_server(port=PORT)
            with open("token.json", "w") as token:
                token.write(creds.to_json())

        service = build("drive", "v3", credentials=creds)

        os.makedirs(save_dir, exist_ok=True)

        file_info = get_file_info(service, file_id)
        file_name = file_info.get("name", "unknown")
        final_path = os.path.join(save_dir, file_name)

        if os.path.exists(final_path):
            print(f"\n🆗 文件已存在: {final_path}")
            return True

        temp_path = os.path.join(save_dir, f"{file_id}.part")

        success = resume_download(service, file_id, temp_path, file_info, max_retries=3)

        if success:
            os.rename(temp_path, final_path)
            print(f"\n✅ 文件下载完成: {final_path}")
            return True
        else:
            print(f"\n❌ 下载失败，临时文件已保存: {temp_path}")
            return False
    except Exception as e:
        print(f"\n❌ 任务异常: {e}")
        return False


def resume_download(service, file_id, temp_path, file_info, max_retries=3):
    """
    断点续传 Google Drive 文件（带异常处理 & 进度条）
    :param service: 已授权的 Google Drive API 客户端
    :param file_id: 需要下载的文件 ID
    :param temp_path: 临时保存路径
    :param file_info: 文件信息
    :param max_retries: 允许的最大重试次数
    :return: 下载是否成功
    """
    offset = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0

    file_name = file_info.get("name", "unknown")
    total_size = int(file_info.get("size", 0))

    request = service.files().get_media(fileId=file_id)

    # 断点续传头部
    if offset > 0:
        request.headers["Range"] = f"bytes={offset}-"

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)

    done = False
    retries = 0

    print(f"\n📥 开始下载: {file_name}, ID: {file_id}, Size: {total_size}B")

    with tqdm(
        total=total_size, unit="B", unit_scale=True, unit_divisor=1024, initial=offset
    ) as progress_bar:
        while not done and retries < max_retries:
            try:
                status, done = downloader.next_chunk()

                downloaded = fh.tell()

                with open(temp_path, "ab") as f:
                    fh.seek(0)
                    f.write(fh.read())

                progress_bar.update(downloaded)

                retries = 0  # 成功下载一部分，重置重试计数

            except (HttpError, ConnectionError, TimeoutError) as e:
                retries += 1
                retry_wait = min(2**retries, 60)  # 指数退避，最多等待 60 秒
                print(
                    f"\n[WARN] 发生错误: {e}, {retry_wait} 秒后重试 ({retries}/{max_retries})..."
                )
                time.sleep(retry_wait)
    return done


def get_file_info(service, file_id):
    """
    获取 Google Drive 文件信息
    :param service: 已授权的 Google Drive API 客户端
    :param file_id: 文件 ID
    :return: 文件信息
    """
    return service.files().get(fileId=file_id, fields="name,size").execute()


if __name__ == "__main__":
    file_id = ""
    save_dir = "/opt/download"
    credentials_path = "credential.json"

    run(file_id, save_dir, credentials_path)
