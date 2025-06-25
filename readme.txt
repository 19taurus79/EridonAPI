✅ 1. Пересобрать контейнер
В папке с Dockerfile:

bash
Копіювати
Редагувати
docker build -t my-app .
my-app — имя образа, можешь заменить

. — путь к Dockerfile, текущая папка

Если хочешь пересобрать с нуля (без кэша):

bash
Копіювати
Редагувати
docker build --no-cache -t my-app .
✅ 2. Создать сеть (если ещё нет)
Например, создать bridge-сеть с именем my-network:

bash
Копіювати
Редагувати
docker network create my-network
✅ 3. Запустить контейнер и подключить к сети
bash
Копіювати
Редагувати
docker run -d --name my-container --network my-network my-app
-d — запуск в фоне

--name my-container — имя контейнера

--network my-network — подключение к сети

my-app — имя образа

🛠 Пример с портами и томами
bash
Копіювати
Редагувати
docker run -d \
  --name my-container \
  --network my-network \
  -p 8000:8000 \
  -v $(pwd)/data:/app/data \
  my-app
🔄 Если контейнер уже был
Удалить старый и запустить заново:

bash
Копіювати
Редагувати
docker rm -f my-container
docker run -d --name my-container --network my-network my-app
