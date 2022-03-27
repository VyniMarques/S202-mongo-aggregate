from db.database import Database
from helper.WriteAJson import writeAJson
from dataset.produto_dataset import dataset

compras = Database(database="database", collection="produtos", dataset=dataset)
compras.resetDatabase()
