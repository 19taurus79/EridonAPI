import json
import os.path
from datetime import date, timedelta, datetime, timezone
from pprint import pprint

from aiogram import Bot
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from .config import TELEGRAM_BOT_TOKEN
from new_agri_bot_backend.tables import Tasks

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/tasks"]

bot = Bot(TELEGRAM_BOT_TOKEN)


async def get_all_tasks(user):
    """Shows basic usage of the Tasks API.
    Prints the title and ID of the first 10 task lists.
    """
    # creds = None
    # # The file token.json stores the user's access and refresh tokens, and is
    # # created automatically when the authorization flow completes for the first
    # # time.
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    TOKEN_ACCOUNT_FILE = os.path.join(BASE_DIR, "token.json")
    SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "credentials_task.json")
    if os.path.exists(TOKEN_ACCOUNT_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_ACCOUNT_FILE, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                SERVICE_ACCOUNT_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_ACCOUNT_FILE, "w") as token:
            token.write(creds.to_json())
    #
    try:
        service = build("tasks", "v1", credentials=creds)

        # Call the Tasks API
        # results = service.tasklists().list(maxResults=10).execute()
        results = (
            service.tasks()
            .list(
                tasklist="RnFScjhXZHRvVHhhZWN0Sg",
                showCompleted=True,
                showHidden=True,
            )
            .execute()
        )
        items = results.get("items", [])

        def date_minus_3_days():
            """Возвращает datetime-объект за 3 дня до текущего момента в UTC."""
            # Получаем текущее время в UTC и вычитаем 3 дня
            # Использование timezone.utc - лучший и более современный способ
            return datetime.now(timezone.utc) - timedelta(days=3)

        filter_date = date_minus_3_days()
        all_tasks = (
            await Tasks.select()
            .where(
                (Tasks.task_status != 2)
                | ((Tasks.created_at > filter_date) & (Tasks.task_status == 2))
            )
            .order_by(Tasks.task_status)
            .order_by(Tasks.created_at, ascending=False)
            .run()
        )
        if user.is_admin:
            return all_tasks
        else:
            user_tasks = (
                await Tasks.select().where((Tasks.task_creator == user.telegram_id))
                & (
                    (Tasks.task_status != 2)
                    | ((Tasks.created_at > filter_date) | (Tasks.task_status == 2))
                )
                .order_by(Tasks.task_status)
                .order_by(Tasks.created_at, ascending=False)
                .run()
            )
        return user_tasks
        # user_tasks_value = []
        # for task in user_tasks:
        #     user_tasks_value.append(task["task_id"])
        # filtered_tasks = [item for item in items if item["id"] in user_tasks_value]
        # if not items:
        #     print("No task lists found.")
        #     return
        #
        # print("Task lists:")
        # for item in items:
        #     print(f"{item['title']} ({item['id']})")
        #
        #     # Берём ID первого списка для примера
        #     first_tasklist_id = items[0]["id"]
        #     print(f"\nЗадачи из списка: {items[0]['title']}")
        #
        #     # Получаем задачи из первого списка
        #     tasks_result = (
        #         service.tasks()
        #         .list(
        #             tasklist="RnFScjhXZHRvVHhhZWN0Sg",
        #             showCompleted=True,
        #             showHidden=True,
        #         )
        #         .execute()
        #     )
        #     tasks = tasks_result.get("items", [])
        #
        #     if not tasks:
        #         print("Задачи не найдены.")
        #         return
        #
        #     for task in tasks:
        #         print(
        #             f"- {task['title']} (Статус: {task.get('status', 'no status')}, ID:{task['id']})"
        #         )
        # return filtered_tasks
    except HttpError as err:
        print(err)


def complete_task(task_id, user):
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    TOKEN_ACCOUNT_FILE = os.path.join(BASE_DIR, "token.json")
    SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "credentials_task.json")
    if os.path.exists(TOKEN_ACCOUNT_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_ACCOUNT_FILE, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                SERVICE_ACCOUNT_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_ACCOUNT_FILE, "w") as token:
            token.write(creds.to_json())

    service = build("tasks", "v1", credentials=creds)

    task_id = task_id
    tasklist_id = "RnFScjhXZHRvVHhhZWN0Sg"  # Например, '@default'

    # Получаем задачу
    task = service.tasks().get(tasklist=tasklist_id, task=task_id).execute()
    previous_title = task["title"]
    split_title = previous_title.split("_")
    new_title = f"{split_title[0]} виконав {user.full_name_for_orders}"
    task["title"] = new_title
    # Обновляем статус
    task["status"] = "completed"
    # Можно также установить дату выполнения (RFC3339 формат)
    import datetime

    task["completed"] = datetime.datetime.utcnow().isoformat() + "Z"

    # Отправляем обновления
    updated_task = (
        service.tasks().update(tasklist=tasklist_id, task=task_id, body=task).execute()
    )
    print(
        f"Обновленная задача: {updated_task['title']} со статусом {updated_task['status']}"
    )


def in_progress_task(task_id, user):
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    TOKEN_ACCOUNT_FILE = os.path.join(BASE_DIR, "token.json")
    SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "credentials_task.json")
    if os.path.exists(TOKEN_ACCOUNT_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_ACCOUNT_FILE, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                SERVICE_ACCOUNT_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_ACCOUNT_FILE, "w") as token:
            token.write(creds.to_json())

    service = build("tasks", "v1", credentials=creds)

    task_id = task_id
    tasklist_id = "RnFScjhXZHRvVHhhZWN0Sg"  # Например, '@default'

    # Получаем задачу
    task = service.tasks().get(tasklist=tasklist_id, task=task_id).execute()

    # Обновляем статус
    previous_title = task["title"]
    new_title = f"{previous_title}_зараз виконує {user.full_name_for_orders}"
    task["title"] = new_title
    # Можно также установить дату выполнения (RFC3339 формат)
    import datetime

    task["updated"] = datetime.datetime.utcnow().isoformat() + "Z"

    # Отправляем обновления
    updated_task = (
        service.tasks().update(tasklist=tasklist_id, task=task_id, body=task).execute()
    )
    print(
        f"Обновленная задача: {updated_task['title']} со статусом {updated_task['status']}"
    )


def get_task_by_id(task_id):
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    TOKEN_ACCOUNT_FILE = os.path.join(BASE_DIR, "token.json")
    SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "credentials_task.json")
    if os.path.exists(TOKEN_ACCOUNT_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_ACCOUNT_FILE, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                SERVICE_ACCOUNT_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_ACCOUNT_FILE, "w") as token:
            token.write(creds.to_json())

    try:
        service = build("tasks", "v1", credentials=creds)
        tasklist_id = "RnFScjhXZHRvVHhhZWN0Sg"  # Например, '@default'
        task = service.tasks().get(tasklist=tasklist_id, task=task_id).execute()
        return task
    except HttpError as err:
        print(err)

    # service = build("tasks", "v1", credentials=creds)

    # task_id = "TVZMWXNPOFAycExNbE4tag"

    # Получаем задачу


async def create_task(date, note, title, user):
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    TOKEN_ACCOUNT_FILE = os.path.join(BASE_DIR, "token.json")
    SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "credentials_task.json")
    if os.path.exists(TOKEN_ACCOUNT_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_ACCOUNT_FILE, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                SERVICE_ACCOUNT_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_ACCOUNT_FILE, "w") as token:
            token.write(creds.to_json())

    try:
        service = build("tasks", "v1", credentials=creds)
        task = {
            "due": date,
            "notes": note,
            "title": title,
        }
        results = (
            service.tasks()
            .insert(tasklist="RnFScjhXZHRvVHhhZWN0Sg", body=task)
            .execute()
        )
        await Tasks.insert(
            Tasks(
                task_id=results["id"],
                task_creator=user.telegram_id,
                task_creator_name=user.full_name_for_orders,
                task_status=0,
                task=title,
            )
        )
        admins_json = os.getenv("ADMINS", "[]")
        admins = json.loads(admins_json)
        for admin in admins:
            await bot.send_message(
                chat_id=admin, text="З'явилось нове завдання", parse_mode="HTML"
            )

        print(user)
        print(results["webViewLink"])
    except HttpError as err:
        print(err)


if __name__ == "__main__":
    # pprint(get_all_tasks())
    complete_task("b2R2X3N3VVNnVUk4RkFBVg")
