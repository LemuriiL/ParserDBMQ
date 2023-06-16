import logging
from elasticsearch import Elasticsearch
from datasketch import MinHash, MinHashLSH

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs.log")
    ]
)

# Функция для генерации шинглов
def generate_shingles(text, k):
    shingles = set()
    words = text.split()
    for i in range(len(words) - k + 1):
        shingle = " ".join(words[i:i+k])
        shingles.add(shingle)
    return shingles

# Настройки подключения к Elasticsearch
es = Elasticsearch(
    [{"host": "127.0.0.1", "port": 9200, "scheme": "http"}],
    basic_auth=("elastic", "nuMZ7JRTtJpQYSORhI=n")
)

# Проверка наличия полей MinHash в индексе
index_name = "parsernews"
index_mapping = {
    "properties": {
        "MinHash": {"type": "text"},
    }
}

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, ignore=400, body={"mappings": index_mapping})
elif not es.indices.exists(index=index_name):
    es.indices.put_mapping(index=index_name, doc_type="_doc", body=index_mapping)

# Получение всех документов из индекса
query = {"query": {"match_all": {}}}
scroll = "5m"  # Время скролла
results = es.search(index=index_name, scroll=scroll, size=1000, **query)
scroll_id = results["_scroll_id"]
documents = results["hits"]["hits"]

while len(documents) > 0:
    for document in documents:
        doc_id = document["_id"]
        doc_source = document["_source"]

        # Проверка наличия поля MinHash в документе
        if "MinHash" not in doc_source:
            # Создание MinHash для текста статьи
            text = doc_source.get("Текст статьи", "")
            shingles = generate_shingles(text, k=0.5)  # Генерация шинглов (указать подходящий размер шингла)
            minhash = MinHash(num_perm=128)
            for shingle in shingles:
                minhash.update(shingle.encode("utf-8"))

            # Обновление документа в индексе с полем MinHash
            es.update(index=index_name, id=doc_id, body={"doc": {"MinHash": str(minhash)}}, refresh=True)

    # Продолжение скролла
    results = es.scroll(scroll_id=scroll_id, scroll=scroll)
    scroll_id = results["_scroll_id"]
    documents = results["hits"]["hits"]

# Создание LSH-индекса
lsh = MinHashLSH(threshold=0.5, num_perm=128)

# Поиск дублей
duplicates = {}
query = {"query": {"match_all": {}}}
results = es.search(index=index_name, scroll=scroll, size=1000, **query)

documents = results["hits"]["hits"]

for document in documents:
    doc_id = document["_id"]
    doc_source = document["_source"]
    minhash_str = doc_source.get("MinHash", "")
    if minhash_str:
        minhash = MinHash(num_perm=128)
        minhash.update(minhash_str.encode("utf-8"))
        matches = lsh.query(minhash)
        for match in matches:
            if match != doc_id:
                if match not in duplicates:
                    duplicates[match] = []
                duplicates[match].append(doc_id)
        lsh.insert(doc_id, minhash)

# Вывод результатов
for duplicate, originals in duplicates.items():
    logging.info(f"Документ {duplicate} является дубликатом документов: {', '.join(originals)}")