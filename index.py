#!/usr/bin/env python

import psycopg2

DBNAME = "news"

query1 = """
select title, count(*) as numOfViews from articles,log
where concat('/article/', articles.slug) = log.path
group by title order by numOfViews desc limit 3;
"""
query2 = """
select authors.name, count(*) as numOfViews
from articles, authors, log
where articles.author = authors.id
and concat('/article/', articles.slug) = log.path
group by authors.name order by numOfViews desc ;
"""
query3 = """
 select innerQuery.badDay, ROUND((100.0*innerQuery.err/innerQuery.total),3)
    as error from (select date_trunc('day', time) as badDay,
    count(*) as total,
    sum(case when status!='200 OK' then 1 else 0 end) as err
    from log
    group by badDay) as innerQuery
    where round((100.0*innerQuery.err/innerQuery.total),3) >1;
    """
result = ''


def get_data(query):
    """ fetch data from database """
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(query)
    data = c.fetchall()
    db.close()
    return data


def fileWrite(content):
    """ write result to result.txt """
    file = open('./result.txt', 'w')
    file.write(content)
    file.close()


def appendToResult(content, isError=False):
    """ formating db result to readable text """
    global result
    if(isError):
        for c in content:
            result += c[0].strftime("%Y-%m-%d") + ' - ' + str(c[1]) + '% error'
    else:
        for c in content:
            result += c[0] + ' - ' + str(c[1]) + '  views \n'
    fileWrite(result)


if __name__ == '__main__':
    result += '\n1. What are the most popular three articles of all time?\n\n'
    appendToResult(get_data(query1))
    result += ' \n2. Who are the most popular article authors of all time?\n\n'
    appendToResult(get_data(query2))
    result += '''\n3. On which days did more than
    1% of requests lead to errors?\n\n'''
    appendToResult(get_data(query3), True)
    print(result)
    fileWrite(result)
