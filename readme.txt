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
docker build -t eridon_api .

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

docker run -d --name eridon_api --network shared_network eridon_api

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

Пример стандартных colorId для событий (наиболее часто используемые):
colorId	Цвет фона (пример)	Описание (условное)
"1"	#a4bdfc	Светло-синий
"2"	#7ae7bf	Светло-зеленый
"3"	#dbadff	Светло-фиолетовый
"4"	#ff887c	Светло-красный
"5"	#fbd75b	Желтый
"6"	#ffb878	Оранжевый
"7"	#46d6db	Бирюзовый
"8"	#e1e1e1	Серый / нейтральный
"9"	#5484ed	Ярко-синий
"10"	#51b749	Зеленый
"11"	#dc2127	Красный
