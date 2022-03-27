from db.database import Database
from helper.WriteAJson import writeAJson
from dataset.pessoa_dataset import dataset as pessoa_dataset
from dataset.carro_dataset import dataset as carro_dataset
from dataset.produto_dataset import dataset as produtos_dataset

produtos = Database(
    database="database",
    collection="produtos",
    dataset=produtos_dataset
)
#produtos.resetDatabase()

pessoas = Database(
    database="database",
    collection="pessoas",
    dataset=pessoa_dataset
)
#pessoas.resetDatabase()

carros = Database(
    database="database",
    collection="carros",
    dataset=carro_dataset
)
#carros.resetDatabase()

result1 = produtos.collection.aggregate([
    {"$lookup":
        {
            "from": "pessoas",  # outra colecao
            "localField": "cliente_id",  # chave estrangeira
            "foreignField": "_id",  # id da outra colecao
            "as": "cliente"  # nome da saida
        }
     },
    {"$group": {"_id": "$cliente", "total": {"$sum": "$total"}}},
    {"$sort": {"total": +1}},
    {"$unwind": '$_id'},
    {"$project": {
        "_id": 0,
        "nome": "$_id.nome",
        "total": 1,
        "desconto": {
            "$cond": {"if": {"$gte": ["$total", 10]}, "then": True, "else": False}
        }
    }}
])

writeAJson(result1, "result1")
