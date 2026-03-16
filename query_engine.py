class Table:
    def __init__(self,name,columns,rows=None):
        self.name=name; self.columns=columns; self.rows=rows or []
    def insert(self,row): self.rows.append(dict(zip(self.columns,row)))
class QueryEngine:
    def __init__(self): self.tables={}
    def create_table(self,name,columns):
        self.tables[name]=Table(name,columns); return self.tables[name]
    def select(self,table,columns=None):
        t=self.tables[table]; return Query(t,columns or t.columns)
class Query:
    def __init__(self,table,columns):
        self.table=table; self.columns=columns; self._where=None; self._order=None; self._limit=None
        self._group=None; self._having=None
    def where(self,fn): self._where=fn; return self
    def order_by(self,col,desc=False): self._order=(col,desc); return self
    def limit(self,n): self._limit=n; return self
    def group_by(self,col): self._group=col; return self
    def execute(self):
        rows=self.table.rows
        if self._where: rows=[r for r in rows if self._where(r)]
        if self._group:
            groups={}
            for r in rows: groups.setdefault(r[self._group],[]).append(r)
            rows=[{self._group:k,'count':len(v),'sum':sum(r.get('amount',0) for r in v)} for k,v in groups.items()]
        if self._order:
            col,desc=self._order; rows=sorted(rows,key=lambda r:r.get(col,0),reverse=desc)
        if self._limit: rows=rows[:self._limit]
        return [{c:r.get(c) for c in self.columns if c in r} for r in rows] if not self._group else rows
if __name__=="__main__":
    qe=QueryEngine()
    t=qe.create_table('orders',['id','customer','amount'])
    for i,(c,a) in enumerate(zip(['Alice','Bob','Alice','Charlie','Bob'],[100,200,150,300,50])):
        t.insert((i,c,a))
    r=qe.select('orders',['customer','amount']).where(lambda r:r['amount']>100).order_by('amount',desc=True).execute()
    assert r[0]['amount']==300
    assert len(r)==3
    print(f"Query results: {r}")
    print("All tests passed!")
