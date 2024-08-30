# Инструкция по подключению к S3 для деплоя DAGов

Для деплоя DAGов нужно подключиться к сервису S3. Следуйте приведённым ниже шагам:

## Шаги для подключения

1. **Скачайте любой S3 клиент:**

   - Для Windows: S3 Browser
   - Для Mac: Cyberduck, Transmit
   - Либо любой другой клиент на ваш вкус

2. **Используйте следующие креды для подключения к S3:**

   - **Сервер:** `storage.yandexcloud.net`
   - **Бакет:** `airf-bds`
   - **Идентификатор ключа:** `YCAJE5tP8F8uquPgaFWIDdEZv`
   - **Секретный ключ:** `YCPzANBXNtINcYWygA1mxOaebgTBRdDQLBicDfYt`

Следуйте этим инструкциям, чтобы успешно подключиться к S3 и выполнить деплой DAGов.

3. **Скачайте DBeaver или любой аналог:**
4. **Используйте следующие креды для подключения к Postgres:**

   - **host** `rc1a-q6wzcy90onxbcwka.mdb.yandexcloud.net`
   - **port** `6432`
   - **dbname** `airf`
   - **user** `airf-user`
   - **pass** `9jLRB8Nx2flKBzO9`

5. WebUI Airflow
   - **URL** `https://c-c9qrnr8jv28kkr2v3s1b.airflow.yandexcloud.net`
   - **user** `admin`
   - **pass** `2elDpOxtqtNfcmyR!`
