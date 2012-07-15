import sqlite3
db_file = '/home/justin/deltachi/delta-aggregate/twitter-scraper/db.sqlite'
db = sqlite3.connect(db_file)
cur = db.cursor()

with open('keywords') as f:
  for line in f.readlines():
    foo = line.split(":")
    symbol = foo[0]
    sql = "select sid from stock where symbol = '%s'" % symbol
    sid = cur.execute(sql).fetchone()[0]
    if len(foo) < 2: continue
    words = foo[1].split("”, “")
    words = [word.strip() for word in words]
    words = [word.replace("”","") for word in words]
    words = [word.replace("“","") for word in words]
    for word in words:
      print word
      sql = "insert into keyword2 (sid, word) values (\"%s\", \"%s\")" % (sid, word)
      _ = cur.execute(sql)

db.commit()
