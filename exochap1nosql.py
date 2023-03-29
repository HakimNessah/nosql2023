#!/usr/bin/env python
# coding: utf-8

# In[96]:


import pandas as pd

df = pd.read_csv("/Users/HaksNSH/Documents/GitHub/NoSQL/data/Chap1/bc.csv")
df.head(5)
print(len(df))
#print(len(df.columns))


# In[17]:


import sqlite3

conn = sqlite3.connect("/Users/HaksNSH/Desktop/untitled folder/bc2.db")
c = conn.cursor()


# In[20]:


def create_table_tomatch():
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS tomatch(id_ INT PRIMARY KEY, "rdm_float" FLOAT)""")
        #c.execute("CREATE INDEX fast_id ON id(id_)")
        conn.commit()
    except Exception as e:
        print(str(e))


# In[22]:


create_table_tomatch()


# In[27]:


import tqdm

query="""INSERT INTO tomatch(id_,rdm_float) VALUES(?,random())"""

list_of_insertion=[]
for i in tqdm.tqdm(range(50000,10000001,2)):
    values = (i,)
    list_of_insertion.append(values)
    if len(list_of_insertion) == 10000:
        c.executemany(query, list_of_insertion)
        conn.commit()
        list_of_insertion = []


# In[76]:


c.execute(""" SELECT AVG(fractal_dimension_worst), COUNT(fractal_dimension_worst), MAX(fractal_dimension_worst), MIN(fractal_dimension_worst), SUM(fractal_dimension_worst) FROM bc INNER JOIN tomatch ON tomatch.id_=bc.id """)
docs = c.fetchall()
print(docs)


# In[52]:


c.execute("""SELECT COUNT(radius_mean) FROM bc WHERE "radius_mean" > 15 """)
docs = c.fetchall()
print(docs)


# In[78]:


c.execute("""SELECT COUNT(texture_mean) FROM bc WHERE "radius_mean" > 15 and "texture_mean" > 20 """)
docs = c.fetchall()
print(docs)


# In[45]:


c.execute("""SELECT ROUND(SUM(perimeter_mean)/COUNT(perimeter_mean),2) FROM bc WHERE "diagnosis" == 'M' """)
docs = c.fetchall()
print(docs)


# In[46]:


c.execute("""SELECT ROUND(SUM(perimeter_mean)/COUNT(perimeter_mean),2) FROM bc WHERE "diagnosis" == 'B' """)
docs2 = c.fetchall()
print(docs2)


# In[50]:


print(round(docs[0][0]-docs2[0][0],2))


# In[86]:


c.execute("""SELECT diagnosis, ROUND(AVG(perimeter_mean),2) FROM bc GROUP BY diagnosis """)
docs2 = c.fetchall()
print(docs2)


# In[56]:


c.execute(""" ALTER TABLE bc RENAME COLUMN diagnosis TO label """)
conn.commit()


# In[60]:


c.execute(""" ALTER TABLE bc ADD day DATE """)
conn.commit()


# In[113]:


c.execute("""ALTER TABLE bc ADD COLUMN area_mean_int INTEGER;""")


# In[114]:


c.execute("""UPDATE bc SET area_mean_int = CAST(area_mean AS INTEGER);""")
conn.commit()


# In[116]:


c.execute("""ALTER TABLE bc
DROP COLUMN area_mean2""")
conn.commit()


# In[117]:


def create_table_tomatch2():
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS tomatch2(id_ INT PRIMARY KEY, "rdm_float" FLOAT)""")
        #c.execute("CREATE INDEX fast_id ON id(id_)")
        conn.commit()
    except Exception as e:
        print(str(e))


# In[120]:


create_table_tomatch2()


# In[121]:


import tqdm

query="""INSERT INTO tomatch2(id_,rdm_float) VALUES(?,random())"""

list_of_insertion=[]
for i in tqdm.tqdm(range(50000,10000001,10)):
    values = (i,)
    list_of_insertion.append(values)
    if len(list_of_insertion) == 10000:
        c.executemany(query, list_of_insertion)
        conn.commit()
        list_of_insertion = []


# In[124]:


c.execute(""" SELECT * FROM bc INNER JOIN tomatch ON tomatch.id_=bc.id INNER JOIN tomatch2 ON tomatch2.id_=bc.id WHERE radius_mean>15 and tomatch.rdm_float>0.5 and tomatch2.rdm_float>0.5""")
docs = c.fetchall()
print(docs)


# In[ ]:




