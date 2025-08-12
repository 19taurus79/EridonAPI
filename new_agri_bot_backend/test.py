import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/tasks"]


def main():
    """Shows basic usage of the Tasks API.
    Prints the title and ID of the first 10 task lists.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("tasks", "v1", credentials=creds)

        # Call the Tasks API
        results = service.tasklists().list(maxResults=10).execute()
        items = results.get("items", [])

        if not items:
            print("No task lists found.")
            return

        print("Task lists:")
        for item in items:
            print(f"{item['title']} ({item['id']})")

            # Берём ID первого списка для примера
            first_tasklist_id = items[0]["id"]
            print(f"\nЗадачи из списка: {items[0]['title']}")

            # Получаем задачи из первого списка
            tasks_result = (
                service.tasks()
                .list(tasklist=first_tasklist_id, showCompleted=True, showHidden=True)
                .execute()
            )
            tasks = tasks_result.get("items", [])

            if not tasks:
                print("Задачи не найдены.")
                return

            for task in tasks:
                print(
                    f"- {task['title']} (Статус: {task.get('status', 'no status')}, ID:{task['id']})"
                )
    except HttpError as err:
        print(err)


def complete_task():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    service = build("tasks", "v1", credentials=creds)

    task_id = "TVZMWXNPOFAycExNbE4tag"
    tasklist_id = "MDc4ODMwMjQ3NDUzMzgzMDM5MTg6MDow"  # Например, '@default'

    # Получаем задачу
    task = service.tasks().get(tasklist=tasklist_id, task=task_id).execute()

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


if __name__ == "__main__":
    main()
    # complete_task()
