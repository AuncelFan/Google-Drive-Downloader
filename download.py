import io
import os
import time
import hashlib
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


def run(file_id: str, save_dir: str, credentials_path: str, check_sum: bool = True):
    """
    运行下载任务，自动获取文件名，并存储到指定目录
    :param file_id: Google Drive 文件 ID
    :param save_dir: 下载目录
    :param credentials_path: 认证凭据文件路径
    :param check_sum: 是否校验文件的 MD5 值
    :return: 下载是否成功
    """
    try:
        creds = init_credentials(credentials_path)

        print(f"\n 开始下载任务: {file_id}")

        service = build("drive", "v3", credentials=creds)

        os.makedirs(save_dir, exist_ok=True)

        file_info = get_file_info(service, file_id)
        file_name = file_info.get("name", "unknown")
        final_path = os.path.join(save_dir, file_name)

        if os.path.exists(final_path):
            if not check_sum:
                print(f"\n 🆗 文件已下载成功: {final_path}")
                return True
            if check_md5(final_path, file_info.get("md5Checksum", "")):
                print(f"\n 🆗 文件已下载成功: {final_path}")
                return True
            else:
                print(f"\n ⚠️ 文件已存在，但 MD5 不匹配，请手动处理: {final_path}")

        temp_path = os.path.join(save_dir, f"{file_id}.part")

        success = resume_download(service, file_id, temp_path, file_info, max_retries=3)

        if success:
            os.rename(temp_path, final_path)
            print(f"\n ✅ 文件下载完成: {final_path}")
            if not check_sum:
                return True
            if check_md5(final_path, file_info.get("md5Checksum", "")):
                print(f"\n 🆗 文件校验成功: {final_path}")
                return True
            else:
                print(f"\n ⚠️ 文件 md5 值不相符，请手动处理: {final_path}")
                return False
        else:
            print(f"\n ❌ 文件下载失败: {file_id}")
                
    except Exception as e:
        print(f"\n ❌ 任务异常: {e}")
        return False


def init_credentials(credentials_path):
    """
    初始化 Google Drive API 认证凭据
    :param credentials_path: 认证凭据文件路径
    :return: 已授权的凭据
    """
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("\n 认证凭据已过期，正在刷新...")
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"\n 刷新凭据失败: {e}, 请重新认证...")
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_path, SCOPES
                )
                creds = flow.run_local_server(port=PORT)
        else:
            print("\n 认证凭据不存在或无效，正在获取新的凭据...")
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=PORT)
        if not creds:
            raise Exception("获取凭据失败")
        with open("token.json", "w") as token:
            print("\n 正在保存认证凭据...")
            token.write(creds.to_json())
            print("\n 认证凭据保存成功!")
    return creds


def check_md5(file_path, expected_md5):
    """
    检查文件的 MD5 校验和
    :param file_path: 文件路径
    :param expected_md5: 预期的 MD5 校验和
    :return: 是否匹配
    """
    print(f"\n 📥 正在校验文件md5: {file_path}")
    md5 = hashlib.md5()
    file_size = os.path.getsize(file_path)
    chunk_size = 64 * 1024 * 1024 if file_size > 1024 * 1024 * 1024 else 4 * 1024 * 1024
    with open(file_path, "rb") as f, tqdm(
        total=file_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        dynamic_ncols=True,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
    ) as progress_bar:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            md5.update(chunk)
            progress_bar.update(len(chunk))
    progress_bar.close()
    return md5.hexdigest() == expected_md5


def resume_download(service, file_id, temp_path, file_info, max_retries=3):
    """
    断点续传 Google Drive 文件
    :param service: 已授权的 Google Drive API 客户端
    :param file_id: 文件 ID
    :param temp_path: 临时文件路径
    :param file_info: 文件信息
    :param max_retries: 最大重试次数
    :return: 是否下载成功
    """

    file_name = file_info.get("name", "unknown")
    total_size = int(file_info.get("size", 0))
    offset = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0

    request = service.files().get_media(fileId=file_id)

    print(f"\n 📥 开始下载: {file_name}, ID: {file_id}, Size: {total_size}B")

    # 创建进度条和文件对象
    with tqdm(
        total=total_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        initial=offset,
        dynamic_ncols=True,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
    ) as progress_bar, io.FileIO(temp_path, mode="ab") as f:

        downloader = MediaIoBaseDownload(f, request, chunksize=10 * 1024 * 1024)
        downloader._progress = offset
        done = False
        retries = 0

        while not done and retries < max_retries:
            try:
                status, done = downloader.next_chunk()

                downloaded = status.resumable_progress
                progress_bar.update(downloaded - offset)

                offset = downloaded

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
    return (
        service.files()
        .get(
            fileId=file_id,
            fields="name,size,md5Checksum",
            supportsAllDrives=True,
        )
        .execute()
    )


if __name__ == "__main__":

    # 示例用法
    file_id = ""
    save_dir = ""
    credentials_path = "credential.json"
    run(file_id, save_dir, credentials_path, check_sum=True)
