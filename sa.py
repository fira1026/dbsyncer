# -*- coding: utf-8 -*-
import json
from collections import OrderedDict as OD

import sqlalchemy
from sqlalchemy import select, and_, or_
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import Table, MetaData, create_engine, text
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey


engine = create_engine('postgresql+psycopg2://postgres:gorillakm@localhost/postgres', strategy="threadlocal", client_encoding='utf8')
session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
conn = engine.connect()

metadata = MetaData()
metadata.reflect(engine)
Base = automap_base(metadata=metadata)
Base.prepare(engine, reflect=True)
#INSP = Inspector.from_engine(engine)



def makeExpr(key_pairs, singleEqual=False):
    keys = key_pairs.keys()
    values = key_pairs.values()
    if len(keys) != len(values):
        return -1
    expr = ""
    pairs = list(zip(keys, values))
    for key, value in pairs:
        if singleEqual:
            expr += "tObj.%s=%s," % (key, value)
        else:
            expr += "tObj.%s==%s," % (key, value)
            #expr += "%s==%s," % (key, value)
    return expr if not expr.endswith(',') else expr[:-1]


def getUpdateValue(act):
    tName = act['table']
    key_pairs = act['key_pairs']
    expr = makeExpr(key_pairs)
    local = {"record" : None, 'tObj' : None}
    content = """
tObj = Base.classes['%(tName)s']
sess=session()
record=sess.query(tObj).filter(%(expr)s).first()
sess.close() """ % { "tName" : tName, "expr": expr }
    exec(content, globals(), local)

    record = local['record']
    if record is None:
        exit(-1)

    keys = key_pairs.keys()
    columns = Base.classes[tName].__table__.columns.keys()
    update = OD()
    for column in columns:
        if column in keys:
            continue
        local = {'record' : record, 'value' : None}
        exec("value=record.%s" % column, globals(), local)
        value = local['value']
        if isinstance(value, str):
            #value = "'" + value + "'"
            value = "'%s'" % value
        update.update( OD([(column,value)]) )
    return update


def makeOutput(jd):
    i = 0
    for data in jd['data']: # a list of transactions
        j = 0
        for act in data['transaction']:
            try:
                method = act['method']
                if method == 'delete':
                    continue
                update = getUpdateValue(act)
                jd['data'][i]['transaction'][j].update(dict(update=update))
            finally:
                j += 1
        i += 1
    return jd


def readInputFile(fileName):
    with open(fileName, 'r') as fi:
        jd = fi.read()
        jd = json.JSONDecoder( object_pairs_hook=OD).decode(jd)
    return jd


def writeOutputFile(jd, fileName):
    with open(fileName, 'w+') as fo:
        fo.write(json.dumps(jd, indent=2))


def clientTask():
    jd = readInputFile(IF)
    jd = makeOutput(jd)
    print("Generated JSON: ", json.dumps(jd))
    writeOutputFile(jd, OF)



def doTran(tran):
    sess = session()
    for act in tran['transaction']:
        tName = act['table']
        tObj = Base.classes[tName]
        tb = tObj.__table__
        expr = makeExpr(act['key_pairs'])
        method = act['method']
        if method == 'update':
            update_pairs = act['update'].items()
            up_str = ','.join('%s=%s' % \
                    (key, str(value)) for key, value in update_pairs)
            up_str = "dict(%s)" % up_str
            local = { 'tb':tb, 'up_str':up_str, 'sess':sess, 'tObj':tObj }
            content = """
stm = tb.update(and_(%(expr)s)).values(eval(up_str))
sess.execute(stm) """ % { 'expr' : expr }
            exec(content, globals(), local)
            print("Doing action: ", act)
    sess.commit()
    print("Tranction commited.")
    sess.close()


def serverTask():
    jd = readInputFile(OF)
    for tran in jd['data']: # a list of transactions
        doTran(tran)


IF = "inputFile.txt"
OF = "outputFile.txt"
if __name__ == "__main__":
    #clientTask()
    serverTask()
    #test1()



def test1():
    TB = Base.classes['polls_choice']

    # method 1
    sess = session()
    rv = sess.query(TB).filter(or_(TB.id==2, TB.votes==0)).first()
    print(rv.choice_text)
    sess.close()

    # method 2
    tb = TB.__table__
    #stm = tb.select(TB.id==1)
    rv = sess.execute(tb.select(TB.id==1)).fetchone()
    print(rv.id)
    sess.close()

    act = {
             "method" : "update",
             "update" : {
                "votes": 0,
                "question_id": 2
             }
          }
    update_pairs = act['update'].items()
    print(update_pairs)
    up_str = ','.join('%s=%s' % (key, str(value)) for key, value in update_pairs)
    print(up_str)
    up="dict(question_id=2, votes=9)"
    stm = tb.update(and_(TB.id==6, TB.choice_text=='Felix')
                   ).values(eval(up))
    sess.execute(stm)
    sess.commit()
    sess.close()

def garbage():
    """
    print("tid=%(tid)s, method=%(method)s, table=%(table)s, keys=%(keys)s, values=%(values)s"  % {
                        "tid" : tid,
                        "method" : method,
                        "table" : table,
                        "keys" : ','.join(key for key in keys),
                        "values" : ','.join(str(value) for value in values)
                        })"""




