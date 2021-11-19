import json
import time
import psycopg2
import pymysql
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from neomodel import db
from graph.models import graphnode
from kgmodel.models import Kgmodel
import pandas as pd
import base64
import cv2

def index(request):  # 知识图谱主页面
    global context
    search = request.GET.get('search')
    resourcebeta = request.GET.get('resourcebeta')  # 自然资源要素
    businesstype = request.GET.get('businesstype')  # 业务类型
    kgs=[]#列表形式
    #img_im = cv2.imread("C:\\Users\\10724\\Pictures\\graph.png")
    #image = base64.b64encode(cv2.imencode('.png', img_im)[1]).decode()  # 解码成字符串

    li = ['uid', 'pk', 'name','orign']
    graphs = graphnode.nodes.all()
    yewu = []  # 业务类型
    zyys = []  # 所属自然资源要素

    for graph in graphs:
        # 知识图谱
        if graph.name[0]=="_": #数字情况
           graph_name=graph.name[1:]
        else:
           graph_name=graph.name
        if graph_name[-1]=='_': #图谱名和模型名相同
            graph_name=graph_name[:-1]
        graph_dic = {}#列表形式
        graph_dic.update({'id':graph.id})
        graph_dic.update({'title': graph_name})
        #graph_dic.update({'image':image})
        idi = str(graph.id)
        result = db.cypher_query(query='Match (n) where id(n)=' + idi + ' return properties(n) AS properties')
        for i in result[0][0][0]:
            if i not in li:
                graph_dic.update({i: result[0][0][0][i]})
            if i=="Businesstype":
                yewu.append(result[0][0][0][i])
            if i=='nature_belong':
                zyys.append(result[0][0][0][i])
        kgs.append(graph_dic)
    if search:
        filter_result = []
        yw=[] #业务
        zr=[] #自然
        for j in kgs:
            if search in j['title'] or search in j['kgdecribe']:
                filter_result.append(j)
        for l in filter_result:#过滤
            yw.append(l["Businesstype"])
            zr.append(l['nature_belong'])
        yewu=list(set(yw))
        zyys =list(set(zr))
        kgs=filter_result

    else:
        search = ''
    if resourcebeta:#自然资源要素
        filter_result = []
        for k in kgs:
            if resourcebeta in k["nature_belong"]:
                filter_result.append(k)
        kgs=filter_result
    else:
        resourcebeta=''

    if businesstype:#业务类型
        filter_result = []
        for k in kgs:
            if businesstype in k["Businesstype"]:
                filter_result.append(k)
        kgs=filter_result
    else:
        businesstype=''

    yewu=list(set(yewu))
    zyys=list(set(zyys))
    context = {
            'model': kgs,
            'search': search,
            "yewu": yewu,
            "zr": zyys,
            "resourcebeta":resourcebeta,
            "businesstype":businesstype
        }
    return JsonResponse(context)

def detail(request, pk):  # 图谱详情页，获取该模型的整个图谱
    dic = {}
    #获取图谱所用模型
    rel=db.cypher_query(query='MATCH (n)-[r]-(m) WHERE id(n) =' + str(pk) + ' RETURN m.name ')
    model_list=[]
    for r in rel[0][0]:
        model_list.append(r)
    result = db.cypher_query(query='MATCH (r) WHERE id(r) =' + str(pk) + ' RETURN r.name as rname,r.kgdecribe as rkgdecribe')
    kgname = result[0][0][0]
    kg_decribe=result[0][0][1]

    label_name='_'+str(pk)

    dic.update({'kg_name':kgname,'kg_decribe':kg_decribe})
    dic.update({'models':model_list})
    entity_label = []  # 获取实体标签
    rel = db.cypher_query(query='MATCH (n:' + label_name + ') RETURN count(n) as n_total')#实体数量
    dic.update({'entity_total':rel[0][0][0]})
    rel = db.cypher_query(query='MATCH (a:'+label_name+')-[r]->(b:'+label_name+') RETURN count(r) as r_total')  # 实体数量
    dic.update({'relation_total': rel[0][0][0]})

    rel = db.cypher_query(
        query='MATCH (n:' + label_name + ') RETURN n.name as n_name,id(n) as n_id,properties(n) AS properties')  # 获取节点的属性信息
    en_biaoqian = []
    for i in rel[0]:
        dico = {"id": i[1], "name": i[0],'bq':[]}
        dico.update({'property': []})
        for j in i[2]:
            if j == 'bq':
                biaoqian_li = i[2][j]
                biaoqian_list = biaoqian_li[1:][:-1].split(',')
                for bi in biaoqian_list:
                    bio = bi.replace('\'', '')
                    bio=bio.replace(' ','')
                    en_biaoqian.append(bio)
                    dico['bq'].append(bio)
            elif j!='ruku':#如果属性名为标签，获取标签对应的属性值
              dico['property'].append([j, 'string', i[2][j]])
        entity_label.append(dico)
    dic.update({'en_biaoqian': list(set(en_biaoqian))})
    re_biaoqian = []  # 关系标签
    result1 = db.cypher_query(query='MATCH (a:'+ label_name + ')-[r]->(b:'+ label_name +') RETURN r.re_type')
    for i in result1[0]:
        re_biaoqian.append(i[0])
    dic.update({'re_biaoqian': list(set(re_biaoqian))})
    dic.update({'entity_label': [], 'entity_relation': []})
    dic['entity_label'] = entity_label
    result1 = db.cypher_query(
        query="MATCH (a:" + label_name + ")-[r]->(b:" + label_name + ") RETURN id(a) as a_id,type(r),r.re_type as rtype,id(r) as r_id,id(b) as b_id")  # 获取三元组（1，包含，2）
    for i in result1[0]:
        dic['entity_relation'].append({"from": i[0], "name":i[1],"fx": "单向", "lx": i[2], "to": i[4]})
    return JsonResponse(dic)

def create_entry(request):  # 获取数据中所有的模型名，让前端进行填充
    # 获取kgmodel中所有模型名
    model_list = []  # 模型名列表
    result = db.cypher_query(query='match (n:Kgmodel) return n.name as n_name')
    for i in result[0]:
         model_list.append(i[0])
    return JsonResponse({"modelname": model_list})

@csrf_exempt
def create_info(request):
    t = json.loads(request.body.decode())
    kg_name = t['kg_name']  # 获取知识图谱名
    for i in graphnode.nodes.all():
        if i.name == kg_name:
            return JsonResponse({'msg': '图谱名称已存在，请重新输入！'})
    return JsonResponse({'msg': '保存成功！'})

@csrf_exempt
def create_base(request):  # 获取数据库的基本信息，连接数据库，获取库中所有的表名和列名:[{数据库名：[{表名：[{类名：string}，{列名：int}]},{数据库名:[{表名：[{类名：string}，{列名：int}]}]
    data_dic = {}
    t = json.loads(request.body.decode())
    db_type = t['db_type']  # 数据库类型
    db_address = t['db_address']  # 数据库地址
    db_name = t['db_name']  # 数据库名
    port = t['port']  # 端口号
    user = t['user']  # 数据库用户名
    password = t['password']  # 密码

    if db_type == 'pg':
        conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=db_address, port=port)
        cur = conn.cursor()
        cur.execute(
            "SELECT tablename FROM  pg_tables  WHERE  tablename  NOT  LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER   BY   tablename;")  # 获取所有的表名
        tablenames = cur.fetchall()

        table_list = []  # 表名
        for tablename in tablenames:
            table_list.append(tablename[0])
        data_dic = {}
        for table in table_list:
            cur.execute(
                "select column_name,data_type from information_schema.columns where table_name =" + '\'' + table + '\'' + "; ")  # 获取一个表中所有的列名
            colunmnames = cur.fetchall()
            data_dic.update({table: colunmnames})
        conn.commit()
        conn.close()

    if db_type == 'MySQL':#如果数据库类型是mysql
        db = pymysql.connect(host=db_address, port=int(port), user=user, passwd=password, db=db_name)
        cursor = db.cursor()
        cursor.execute("select table_name from information_schema.tables where table_schema="+ '\'' + db_name + '\'' + "")
        tabels = cursor.fetchall()
        table_list = []
        for table in tabels:
            table_list.append(table[0])
        for table in table_list:
            tab = []
            cursor.execute(
                "select COLUMN_NAME AS '列名', DATA_TYPE AS '数据类型' from information_schema.COLUMNS where table_name = " + '\'' + table + '\'' + " and table_schema = "+'\'' + db_name + '\''+";")
            data = cursor.fetchall()
            for da in data:
                tab.append([da[0], da[1]])
            data_dic.update({table: tab})
        db.close()
    return JsonResponse(data_dic)

@csrf_exempt
def create_generate(request):
    global csv_dic, re_name_list, re_type_list, re_name, re_type, relation, conn, arr_map, flag, db_name, db_type
    t = json.loads(request.body.decode())
    kg_name=t["kg_name"] #图谱名称
    model_name = t["model_name"]  # 模型名称
    kg_decribe = t['kg_decribe']  # 知识图谱描述
    rel = db.cypher_query(query='MATCH (n:Kgmodel{name:' + "\"" + model_name + "\"" + '}) RETURN n.Resourcemeta as n_re,n.Businesstype as n_bu')
    kg_nature_belong = rel[0][0][0]  # 图谱所属自然资源要素
    Businesstype = rel[0][0][1]  # 图谱所属业务类型
    create_time = str(time.strftime("%Y-%m-%d", time.localtime()))
    graphnode(name=kg_name).save()  # 生成一个图谱节点

    db.cypher_query(  # 设置图谱的基本信息
        query='match (n:graphnode) where n.name=' + "\"" + kg_name + "\"" + ' set n.kgdecribe=' + "\"" + kg_decribe + "\"" + ',n.nature_belong=' + "\"" + kg_nature_belong + "\"" + ',n.Businesstype=' + "\"" + Businesstype + "\"" + ',n.create_time=' + "\"" + create_time + "\"" + ',n.orign=[]')
    # 将模型和知识图谱建立连接
    kg_node = graphnode.nodes.get(name=kg_name)
    lable_name = '_' + str(kg_node.id)  # 知识图谱对应的标签名
    mod_node = Kgmodel.nodes.get(name=model_name)
    mod_lable_id='_'+str(mod_node.id)
    kg_node.kg_re_model.connect(mod_node)

    name_dic={} #{cunzuaung:村庄}
    model_name_list = []  # 该模型中的实体名
    rel = db.cypher_query(query='MATCH (n:' + mod_lable_id + ') RETURN n.名称 as n_name,n.name as nname')
    for md in rel[0]: #获取所有的实体名
        name_dic.update({md[0]:md[1]})
        model_name_list.append(md[0])

    tables=t["table"]
    entity_count=0 #实体计数
    relation_count=0 #关系计数

    dborign=[] #{数据库名1:[]，数据库名2:[]}
    lose_data=[] #缺失的属性字段[python,java,c++]
    problems=[] #存储有问题、缺失字段、无法入库的表[{'db_name':ddosi,'db_type':'pg','table':aa,'ziduan':[python,java,c++]},{'db_name':ddosi,'db_type':'pg','table':aa,'ziduan':[python,java,c++]}]

    for table in tables: #table：{"bb":[["乡镇","xinagzhen"],["行政村","dijishi"]],"basedata":{"db_type":"pg","db_name":"xingxing","db_address":"127.0.0.1","port":"5432","user":"postgres","password":"123456"}}
        flag=1 #判断该表是否有问题
        biaoming=list(table.keys())[0] #表名
        data_map_list=table[biaoming] #列表的形式[[修改后的，修改前的],[修改后的，修改前的]]
        data_map={}
        for da in data_map_list:
             data_map.update({da[0]:da[1]})

        table_name=biaoming #获取表名
        db_info=table["basedata"] #获取数据库信息
        db_name = db_info['db_name']
        db_type = db_info['db_type']
        #判断：如果该表成功入库就添加到数据库来源信息中，不能成功入库就不加
        if db_info['db_type'] == 'pg':
            conn = psycopg2.connect(dbname=db_info['db_name'], user=db_info['user'], password=db_info['password'],
                                    host=db_info['db_address'], port=db_info['port'])
        elif db_info['db_type'] == 'MySQL':
            conn=pymysql.connect(host=db_info["db_address"], port=int(db_info["port"]), user=db_info["user"], passwd=db_info["password"], db=db_info["db_name"])
        cur = conn.cursor()
        model_entity_name=[] #模型实体名和列名重叠部分
        for j in table[biaoming]: #列表
            if j[0] in model_name_list:
                model_entity_name.append(j[0])

        #如果表中字段名和模型中实体名没有重叠的部分，说明无法生成实体，提示用户修改模型中对应的实体信息
        #if len(model_entity_name)==0:

        #获取修改后的列名
        lie_fix_name=[]
        for l in data_map: #data_map的格式为修改后的、修改前的
            lie_fix_name.append(l)
        relation_entity={} #存储要进行关系映射的数据列{乡镇：[水冶镇，龙湖镇....]}
        ruku_delete='['+'\''+db_name+'\''+','+'\''+db_type+'\''+','+'\''+biaoming+'\''+']'#用于删除已经入库的信息
        num_en=0
        num_re=0
        for mod in model_entity_name:  #[xiangmu,cunzuang]
            csv_dic={mod:[]}#导入数据的csv字典
            csv_dic={'ruku':[]}#用于删除已经入库的数据信息
            cur.execute("SELECT " + data_map[mod] + " FROM " + table_name + " limit 50")
            rows = cur.fetchall()
            mod_list =[]
            for row in rows:
                mod_list.append(row[0])
                csv_dic['ruku']='['+'\''+db_name+'\''+','+'\''+db_type+'\''+','+'\''+biaoming+'\''+']'
            csv_dic[mod]=mod_list  # 获取实体
            relation_entity.update({mod:csv_dic[mod]})

            #获取数据的来源信息
            mod_len=len(csv_dic[mod])#导入实体列的长度
            #获取所有属性该模型实体的所有属性
            rel = db.cypher_query(query='MATCH (n:' + mod_lable_id + '{name:' +"\""+name_dic[mod]+"\""+ '}) RETURN properties(n) AS properties')
            csv_dic.update({'bq':[]})
            if len(rel[0]): #如果模型实体有属性
                arr = []  # 某个节点的全部属性
                sx = rel[0][0][0]
                for s in sx:
                    arr.append(s)
                remo_list = ['name', 'entity_id','名称']
                for re in remo_list:
                    arr.remove(re)
                arr_map = {}
                for ar in arr:  # 由属性名获取相应的属性值
                    rel = db.cypher_query(query='MATCH (n:' + mod_lable_id + '{name:' +"\""+name_dic[mod]+"\""+ '}) RETURN n.' + ar + '')
                    sxm = rel[0][0][0]
                    if ar=='bq':
                        sl=[]
                        bqlist=sxm.split(',')[:-1]
                        for i in range(mod_len):
                            sl.append(bqlist)  # 名称映射
                        csv_dic.update({"bq": sl})
                    else:
                        arr_map.update({sxm: ar})

                #如果属性在列表中，获取该列的数据
                for a in arr_map: #数据形式{属性值：属性名}
                    if a in lie_fix_name:#如果模型的属性值在修改后的表字段中
                        b={a:[]}
                        cur.execute("SELECT " + data_map[a] + " FROM " + table_name + " limit 50")
                        rows = cur.fetchall()
                        for row in rows:
                            b[a].append(row[0])
                        csv_dic.update(b)
                    else:  #如果模型的属性值不在修改后的表字段中，就将其添加到问题字段列表中，并且中断本次table循环
                        flag=0
                        lose_data.append(a)

            if flag==0: #如果标志为0就中断实体名循环，并且删除已经进入到图谱网络中的信息
                #基于ruku删除已经入库的该表的信息
                problems.append({'db_name': db_name, 'db_type': db_type, 'table': biaoming, 'ziduan': lose_data})
                db.cypher_query(query='MATCH (n:'+kg_name+') where n.ruku='+ruku_delete+' DETACH DELETE n')
                num_en=0
                break
            num_en+=mod_len

            data = pd.DataFrame(csv_dic)
            data.to_csv("ceshi.csv", index=False, sep=',')
            #属性路径
            arr_s='{name:line.'+mod+','
            for rou in csv_dic:
                if rou!=mod and rou!='bq' and rou!='ruku':
                    arr_s+=arr_map[rou]+':line.'+rou+','
                if rou=='bq':
                    arr_s+='bq:line.bq,'
            arr_s=arr_s[:-1]
            arr_s+='})'
            db.cypher_query(
                query='LOAD CSV WITH HEADERS FROM "file:///H:/python文件/ziran/ceshi.csv" AS line MERGE (p:' + lable_name + arr_s)
        conn.close()#关闭数据库

        relation={}
        re_name=''
        li = model_entity_name
        if flag: #如果表没问题
            if len(li)>=2:
                for i in range(len(li)):
                    naei=li[i]
                    first=name_dic[li[i]]
                    for j in range(i + 1, len(li)):
                        rel = db.cypher_query(
                            query='MATCH (n:' + mod_lable_id + '{name:' + '\'' + first + '\'' + '})-[r]->(m:' + mod_lable_id + '{name:' + '\'' + name_dic[li[j]] + '\'' + '}) RETURN type(r),properties(r) AS properties')
                        if len(rel[0]):
                            re_name = rel[0][0][0]  # 关系名
                            re_type = rel[0][0][1]['lx']  # 关系类型
                            re_len=len(relation_entity[li[j]])
                            re_name_list = []
                            for i in range(re_len):
                                num_re += 1
                                re_name_list.append(re_name)
                            relation={'from_name':relation_entity[naei],'re_name':re_name_list,'to_name':relation_entity[li[j]]}
                        rel = db.cypher_query(
                            query='MATCH (n:' + mod_lable_id + '{name:' + '\'' + first + '\'' + '})<-[r]-(m:' + mod_lable_id + '{name:' + '\'' + name_dic[li[j]] + '\'' + '}) RETURN type(r),properties(r) AS properties')
                        if len(rel[0]):
                            re_name = rel[0][0][0]  # 关系名
                            re_type = rel[0][0][1]['lx']  # 关系类型
                            re_len = len(relation_entity[li[j]])
                            re_name_list = []
                            for i in range(re_len):
                                num_re+=1
                                re_name_list.append(re_name)
                            relation = {'from_name': relation_entity[li[j]], 're_name': re_name_list,'to_name': relation_entity[naei]}

                        data = pd.DataFrame(relation)
                        data.to_csv("test1.csv", index=False, sep=',')
                        db.cypher_query(
                            query='LOAD CSV WITH HEADERS FROM "file:///H:/python文件/ziran/test1.csv" AS line match (from:' + lable_name + '{name:line.from_name}),(to:' + lable_name + '{name:line.to_name}) merge (from)-[:'+re_name+'{re_type:'+'\''+re_type+'\''+'}]->(to)')
        else:
            num_re=0
        entity_count+=num_en
        relation_count+=num_re
        if flag: #如果该表没问题
            dborign.append([db_name, db_type, biaoming])

    #设置数据来源的属性
    s = '["'+create_time+':['
    for b in dborign:
        s += b[0] + '|' + b[1] + '|' + b[2] + ','
    if s[-1]=='[':
        s+=']"]'
    else:
        s = s[:-1] + ']"]'
    db.cypher_query(query='MATCH (n:graphnode) where n.name=' +"\'"+kg_name +"\'"+' SET n.orign='+s+'')#设置图谱数据来源信息

    kg_id=kg_node.id
    if len(problems):#如果有问题，给出提示信息
        res = ''
        for i in problems:
            loda = ''
            for data in i['ziduan']:
                loda += data + ','
            loda = loda[:-1]
            res += '数据表' + i['table'] + '(' + i['db_name'] + '|' + i['db_type'] + ')缺失' + '[' + loda + ']' + '、'
        res = res[:-1] + '字段的信息造成无法入库！请在检查修改后重新入库。'
        return JsonResponse({"msg": "累计入库" + str(entity_count) + "个实体、" + str(relation_count) + "条关系。", "create": "fail",
             "id": kg_id,'warn_msg':res})
    else:
        return JsonResponse({"msg":"创建成功!累计入库"+str(entity_count)+"个实体、"+str(relation_count)+"条关系。","create":"success","id":kg_id})

def delete(request, pk):#图谱删除
    db.cypher_query(query='MATCH (r) WHERE id(r) ='+str(pk)+' DETACH DELETE r')
    label = '_' + str(pk)
    db.cypher_query(query='MATCH (r:' +label+ ') DETACH DELETE r')
    return JsonResponse({'msg':'删除成功！'})

def node_origin(request,pk):  # 数据溯源，点击某个节点，返回知识图谱创建的时间、用了数据库和数据表{"create":"2021","database":[{"db_name":'ddosi',db_type:pg,"table":[[aa,'2021'],[bb,'2021']]},{"db_name":'ddosi',db_type:pg,"table":[[aa,'2021'],[bb,'2021']]}}
    oritg = {}
    rel = db.cypher_query(query='MATCH (n) where id(n)='+str(pk)+' RETURN n.orign as norign,n.create_time as ntime')
    oritg.update({'create_time': rel[0][0][1]})
    st = rel[0][0][0]
    database = []
    for s in st:
        fen = s.split(':')
        db_name = fen[0]  # 获取数据库名
        tables = fen[1]  # 数据库中的表
        table_list = []
        tabl = tables[1:][:-1].split(',')
        for tab in tabl:
            table_list.append(tab)
        database.append({'db_name': db_name, 'table': table_list})
    oritg.update({'database': database})
    return JsonResponse(oritg)

@csrf_exempt
def relation_index_select(request):#让用户选择关系
    global index_result, index_dic
    t = json.loads(request.body.decode())
    kg_name = t['kg_name'] #获取图谱名称
    kg_node = graphnode.nodes.get(name=kg_name)
    lable_name = '_' + str(kg_node.id)

    search = t['search']
    rel = db.cypher_query(query='MATCH (n:'+lable_name+')-[r]-(m:'+lable_name+') RETURN type(r)')
    relation = []
    result=[]
    for i in rel[0]:
        relation.append(i[0])
    count=0
    for j in list(set(relation)):
        if count>5:
            break
        if search in j:
            result.append(j)
            count+=1
    return JsonResponse({'re_name':result})

@csrf_exempt
def relation_index(request):
    t = json.loads(request.body.decode())
    re_name = t['re_name'] #关系名
    kg_name=t['kg_name'] #图谱名
    kg_node = graphnode.nodes.get(name=kg_name)
    lable_name = '_' + str(kg_node.id)

    sanyuanzu = []  # 三元组
    n_id = []
    m_id = []
    rel = db.cypher_query(
        query='MATCH (n:' + lable_name + ')-[' + re_name + ']->(m:' + lable_name + ') RETURN id(n) as nid,id(m) as mid')
    for i in rel[0]:
        n_id.append(i[0])
        m_id.append(i[1])
        sanyuanzu.append({'from':i[0], 'name':re_name,'fx':'单向','to':i[1]})
    entity=list(set(n_id+m_id))

    index_result = []  # 搜索结果
    for en in entity:
        rel = db.cypher_query(
            query='MATCH (n:' + lable_name + ') where id(n)=' + str(en) + ' RETURN n.name as n_name,id(n) as nid,properties(n) AS properties')
        index_dic = {}
        for i in rel[0]:
            index_dic.update({'id': i[1], 'name': i[0],'bq':[],"property": []})
            for k in i[2]:
                if k=='bq':
                    biaoqian_li = i[2][k]
                    biaoqian_list = biaoqian_li[1:][:-1].split(',')
                    for bi in biaoqian_list:
                        bio = bi.replace('\'', '')
                        bio = bio.replace(' ', '')
                        index_dic['bq'].append(bio)
                else:
                   index_dic["property"].append([k, i[2][k]])
        index_result.append(index_dic)
    return JsonResponse({'entity': index_result, 'sanyuanzu': sanyuanzu})

def node_search(request,pk):#实体搜索
    rel = db.cypher_query(query='MATCH (n) where id(n)='+str(pk)+' RETURN n.name as n_name,id(n) as nid,properties(n) AS properties')
    index_result=[]#搜索结果
    index_dic={}
    for i in rel[0]:
            index_dic.update({'id':i[1],'name':i[0],"property":[]})
            for k in i[2]:
                index_dic["property"].append([k,i[2][k]])
            index_result.append(index_dic)
    return JsonResponse({"index_result":index_result})

@csrf_exempt
def node_index_select(request):#用户选择实体
    t = json.loads(request.body.decode())
    kg_name = t['kg_name']
    kg_node = graphnode.nodes.get(name=kg_name)
    lable_name = '_' + str(kg_node.id)
    search = t['search']
    rel = db.cypher_query(
        query='MATCH (n:' + lable_name + ') RETURN n.name as n_name,id(n) as nid,properties(n) AS properties')
    index_result = []  # 搜索结果
    count=0
    for i in rel[0]:
        if count>6:
            break
        if search in i[0]:
            index_dic = {}
            index_dic.update({'id': i[1], 'name': i[0],'bq':[],"property": []})
            for k in i[2]:
                if k=='bq':
                    biaoqian_li = i[2][k]
                    biaoqian_list = biaoqian_li[1:][:-1].split(',')
                    for bi in biaoqian_list:
                        bio = bi.replace('\'', '')
                        bio = bio.replace(' ', '')
                        index_dic['bq'].append(bio)
                else:
                    index_dic["property"].append([k, i[2][k]])
            index_result.append(index_dic)
            count += 1
    return JsonResponse({"index_result": index_result})

@csrf_exempt
def node_route(request):  # 根据两个节点的id返回这两个节点间的所有路径[]
    t = json.loads(request.body.decode())
    a_id=t['a_id'] #节点a的id
    b_id=t['b_id'] #节点b的id
    kg_name=t['kg_name'] #知识图谱的名字
    kg_node = graphnode.nodes.get(name=kg_name)
    lable_name = '_' + str(kg_node.id)

    dl = db.cypher_query(
        query='MATCH (from:'+lable_name+'),(to:'+lable_name+') where id(from)='+str(a_id)+' and id(to)='+str(b_id)+'  CALL apoc.algo.allSimplePaths(from, to, "",5) YIELD path RETURN nodes(path) as pno,relationships(path) as pth,length(path) as palen order by length(path) asc limit 5')
    num = 1
    route = []
    for d in dl[0]:
        lujing = {'lenth': d[2], 'entity': [], 'relations': []}
        # 获取实体entity=[]
        shiti = []
        for k in d[0]:
            shi = {'id': k.id, 'name': k['name'],'bq':[], 'property': []}
            rel = db.cypher_query(query='MATCH (n) where id(n)=' + str(k.id) + ' RETURN properties(n) AS properties')
            sx = rel[0][0][0]
            for m in sx:
                if m != 'name' and m != 'bq':
                    shi["property"].append([m, sx[m]])
                elif m=='bq':
                    biaoqian_li = sx[m]
                    biaoqian_list = biaoqian_li[1:][:-1].split(',')
                    for bi in biaoqian_list:
                        bio = bi.replace('\'', '')
                        bio = bio.replace(' ', '')
                        shi['bq'].append(bio)
            shiti.append(shi)
        lujing['entity'] = shiti
        guanxi = []  # 关系
        for x in d[1]:
            qishi = x.nodes[0]
            zhongdian = x.nodes[1]
            guanxi_dic = {'from_id': qishi.id, 'from_name': qishi['name'], 're_name': x.type, 'lx': x['re_type'],
                          'fx': '单向', 'to_id': zhongdian.id, 'to_name': zhongdian['name']}
            guanxi.append(guanxi_dic)
        lujing['relations'] = guanxi
        route.append(lujing)
        num += 1
    return JsonResponse({'route':route})

@csrf_exempt
def add_data(request,pk):#批量增加数据
    global relation, conn, re_name, re_type, arr_map
    t = json.loads(request.body.decode())
    #设置图谱描述
    kg_decribe=t["kg_decribe"]
    rel = db.cypher_query(query='MATCH (r) where id(r)='+str(pk)+' set r.kgdecribe='+'\''+kg_decribe+'\''+' return r.name')

    lable_name = '_' + str(pk) #标签名称

    kg_name=rel[0][0][0] #图谱名称
    model_name = t["model_name"]  # 模型名称
    mod_node = Kgmodel.nodes.get(name=model_name)
    mod_lable_id ='_'+str(mod_node.id)

    add_time=str(time.strftime("%Y-%m-%d", time.localtime()))#增加数据的时间
    name_dic = {}  # {cunzuaung:村庄}
    model_name_list = []  # 该模型中的实体名
    rel = db.cypher_query(query='MATCH (n:' + mod_lable_id + ') RETURN n.名称 as n_name,n.name as nname')
    for md in rel[0]:  # 获取所有的实体名
        name_dic.update({md[0]: md[1]})
        model_name_list.append(md[0])

    tables = t["table"]
    entity_count = 0  # 实体计数
    relation_count = 0  # 关系计数

    dborign = []  # {数据库名1:[]，数据库名2:[]}
    lose_data = []  # 缺失的属性字段[python,java,c++]
    problems = []  # 存储有问题、缺失字段、无法入库的表[{'db_name':ddosi,'db_type':'pg','table':aa,'ziduan':[python,java,c++]},{'db_name':ddosi,'db_type':'pg','table':aa,'ziduan':[python,java,c++]}]

    for table in tables:  # table：{"bb":[["乡镇","xinagzhen"],["行政村","dijishi"]],"basedata":{"db_type":"pg","db_name":"xingxing","db_address":"127.0.0.1","port":"5432","user":"postgres","password":"123456"}}
        flag = 1  # 判断该表是否有问题
        biaoming = list(table.keys())[0]  # 表名
        data_map_list = table[biaoming]  # 列表的形式[[修改后的，修改前的],[修改后的，修改前的]]
        data_map = {}
        for da in data_map_list:
            data_map.update({da[0]: da[1]})

        table_name = biaoming  # 获取表名
        db_info = table["basedata"]  # 获取数据库信息
        db_name = db_info['db_name']
        db_type = db_info['db_type']
        # 判断：如果该表成功入库就添加到数据库来源信息中，不能成功入库就不加
        if db_info['db_type'] == 'pg':
            conn = psycopg2.connect(dbname=db_info['db_name'], user=db_info['user'], password=db_info['password'],
                                    host=db_info['db_address'], port=db_info['port'])

        elif db_info['db_type'] == 'MySQL':
            conn = pymysql.connect(host=db_info["db_address"], port=int(db_info["port"]), user=db_info["user"],
                                   passwd=db_info["password"], db=db_info["db_name"])
        cur = conn.cursor()
        model_entity_name = []  # 模型实体名和列名重叠部分
        for j in table[biaoming]:  # 列表
            if j[0] in model_name_list:
                model_entity_name.append(j[0])

        # 如果表中字段名和模型中实体名没有重叠的部分，说明无法生成实体，提示用户修改模型中对应的实体信息
        # if len(model_entity_name)==0:
        #
        # 获取修改后的列名
        lie_fix_name = []
        for l in data_map:  # data_map的格式为修改后的、修改前的
            lie_fix_name.append(l)
        relation_entity = {}  # 存储要进行关系映射的数据列{乡镇：[水冶镇，龙湖镇....]}
        ruku_delete = '[' + '\'' + db_name + '\'' + ',' + '\'' + db_type + '\'' + ',' + '\'' + biaoming + '\'' + ']'  # 用于删除已经入库的信息
        num_en = 0
        num_re = 0
        for mod in model_entity_name:  # [xiangmu,cunzuang]
            csv_dic = {'ruku': []}  # 用于删除已经入库的数据信息
            cur.execute("SELECT " + data_map[mod] + " FROM " + table_name + " limit 50")
            rows = cur.fetchall()
            mod_list = []
            for row in rows:
                mod_list.append(row[0])
                csv_dic['ruku'] = '[' + '\'' + db_name + '\'' + ',' + '\'' + db_type + '\'' + ',' + '\'' + biaoming + '\'' + ']'
            csv_dic[mod] = mod_list  # 获取实体
            relation_entity.update({mod: csv_dic[mod]})

            # 获取数据的来源信息
            mod_len = len(csv_dic[mod])  # 导入实体列的长度
            # 获取所有属性该模型实体的所有属性
            rel = db.cypher_query(query='MATCH (n:' + mod_lable_id + '{name:' + "\"" + name_dic[
                mod] + "\"" + '}) RETURN properties(n) AS properties')
            csv_dic.update({'bq': []})
            if len(rel[0]):  # 如果模型实体有属性
                arr = []  # 某个节点的全部属性
                sx = rel[0][0][0]
                for s in sx:
                    arr.append(s)
                remo_list = ['name', 'entity_id', '名称']
                for re in remo_list:
                    arr.remove(re)
                arr_map = {}
                for ar in arr:  # 由属性名获取相应的属性值
                    rel = db.cypher_query(query='MATCH (n:' + mod_lable_id + '{name:' + "\"" + name_dic[
                        mod] + "\"" + '}) RETURN n.' + ar + '')
                    sxm = rel[0][0][0]
                    if ar == 'bq':
                        sl = []
                        bqlist = sxm.split(',')[:-1]
                        for i in range(mod_len):
                            sl.append(bqlist)  # 名称映射
                        csv_dic.update({"bq": sl})
                    else:
                        arr_map.update({sxm: ar})

                # 如果属性在列表中，获取该列的数据
                for a in arr_map:  # 数据形式{属性值：属性名}
                    if a in lie_fix_name:  # 如果模型的属性值在修改后的表字段中
                        b = {a: []}
                        cur.execute("SELECT " + data_map[a] + " FROM " + table_name + " limit 50")
                        rows = cur.fetchall()
                        for row in rows:
                            b[a].append(row[0])
                        csv_dic.update(b)
                    else:  # 如果模型的属性值不在修改后的表字段中，就将其添加到问题字段列表中，并且中断本次table循环
                        flag = 0
                        lose_data.append(a)

            if flag == 0:  # 如果标志为0就中断实体名循环，并且删除已经进入到图谱网络中的信息
                # 基于ruku删除已经入库的该表的信息
                problems.append({'db_name': db_name, 'db_type': db_type, 'table': biaoming, 'ziduan': lose_data})
                db.cypher_query(query='MATCH (n:' + kg_name + ') where n.ruku=' + ruku_delete + ' DETACH DELETE n')
                num_en = 0
                break
            num_en += mod_len

            data = pd.DataFrame(csv_dic)
            data.to_csv("ceshi.csv", index=False, sep=',')
            # 属性路径
            arr_s = '{name:line.' + mod + ','
            for rou in csv_dic:
                if rou != mod and rou != 'bq' and rou != 'ruku':
                    arr_s += arr_map[rou] + ':line.' + rou + ','
                if rou == 'bq':
                    arr_s += 'bq:line.bq,'
            arr_s = arr_s[:-1]
            arr_s += '})'
            db.cypher_query(
                query='LOAD CSV WITH HEADERS FROM "file:///H:/python文件/ziran/ceshi.csv" AS line MERGE (p:' + lable_name + arr_s)
        conn.close()  # 关闭数据库

        relation = {}
        re_name = ''
        li = model_entity_name
        if flag:  # 如果表没问题
            if len(li) >= 2:
                for i in range(len(li)):
                    naei = li[i]
                    first = name_dic[li[i]]
                    for j in range(i + 1, len(li)):
                        rel = db.cypher_query(
                            query='MATCH (n:' + mod_lable_id + '{name:' + '\'' + first + '\'' + '})-[r]->(m:' + mod_lable_id + '{name:' + '\'' +
                                  name_dic[li[j]] + '\'' + '}) RETURN type(r),properties(r) AS properties')
                        if len(rel[0]):
                            re_name = rel[0][0][0]  # 关系名
                            re_type = rel[0][0][1]['lx']  # 关系类型
                            re_len = len(relation_entity[li[j]])
                            re_name_list = []
                            for i in range(re_len):
                                num_re += 1
                                re_name_list.append(re_name)
                            relation = {'from_name': relation_entity[naei], 're_name': re_name_list,
                                        'to_name': relation_entity[li[j]]}
                        rel = db.cypher_query(
                            query='MATCH (n:' + mod_lable_id + '{name:' + '\'' + first + '\'' + '})<-[r]-(m:' + mod_lable_id + '{name:' + '\'' +
                                  name_dic[li[j]] + '\'' + '}) RETURN type(r),properties(r) AS properties')
                        if len(rel[0]):
                            re_name = rel[0][0][0]  # 关系名
                            re_type = rel[0][0][1]['lx']  # 关系类型
                            re_len = len(relation_entity[li[j]])
                            re_name_list = []
                            for i in range(re_len):
                                num_re += 1
                                re_name_list.append(re_name)
                            relation = {'from_name': relation_entity[li[j]], 're_name': re_name_list,
                                        'to_name': relation_entity[naei]}
                        data = pd.DataFrame(relation)
                        data.to_csv("test1.csv", index=False, sep=',')
                        db.cypher_query(
                            query='LOAD CSV WITH HEADERS FROM "file:///H:/python文件/ziran/test1.csv" AS line match (from:' + lable_name + '{name:line.from_name}),(to:' + lable_name + '{name:line.to_name}) merge (from)-[:' + re_name + '{re_type:' + '\'' + re_type + '\'' + '}]->(to)')
        else:
            num_re = 0
        entity_count += num_en
        relation_count += num_re
        if flag:  # 如果该表没问题
            dborign.append([db_name, db_type, biaoming])

    # 设置数据来源的属性
    rel=db.cypher_query(query='MATCH (r) where id(r)='+str(pk)+' return r.orign')
    query_result=rel[0][0][0]
    st = '['
    for q in query_result:
        st += '\'' + q + '\'' + ','  # 将查询结果转化为字符串的形式
    sl = '"'+add_time+':['
    for b in dborign:
        sl += b[0] + '|' + b[1] + '|' + b[2] + ','
    sl = sl[:-1] + ']"'
    st = st + sl + ']'  # 将查询结果和新加的串相连
    db.cypher_query(query='MATCH (n:graphnode) where n.name=' + "\'" + kg_name + "\'" + ' SET n.orign=' + st + '')  # 设置图谱数据来源信息
    if len(problems):#如果有问题，给出提示信息
        res = ''
        for i in problems:
            loda = ''
            for data in i['ziduan']:
                loda += data + ','
            loda = loda[:-1]
            res += '数据表' + i['table'] + '(' + i['db_name'] + '|' + i['db_type'] + ')缺失' + '[' + loda + ']' + '、'
        res = res[:-1] + '字段的信息造成无法入库！请在检查修改后重新添加。'
        return JsonResponse({"msg": "累计添加" + str(entity_count) + "个实体、" + str(relation_count) + "条关系。",'id':pk, "create": "fail",'warn_msg':res})
    else:
        return JsonResponse({"msg":"添加成功!累计入库"+str(entity_count)+"个实体、"+str(relation_count)+"条关系。","create":"success",'id':pk})

def node_click(request,pk):
    link = []
    entity = []  # 实体id
    node_id=[pk]
    rel = db.cypher_query(query='MATCH (n)<-[r]-(m) where id(n)='+str(pk)+' RETURN id(n) as nid,type(r),r.re_type,id(m) as mid')
    for node in rel[0]:
        node_id.append(node[3])
        link.append({'from':node[0], 're_name':node[1],'re_type':node[2], 'to':node[3]})
    rel = db.cypher_query(query='MATCH (n)-[r]->(m) where id(n)='+str(pk)+' RETURN id(n) as nid,type(r),r.re_type,id(m) as mid')
    for node in rel[0]:
        node_id.append(node[3])
        link.append({'from':node[3], 're_name':node[1],'re_type':node[2], 'to':node[0]})
    id_list=list(set(node_id))

    for nid in id_list:
        rel = db.cypher_query(query='MATCH (n) where id(n)=' + str(nid) + ' RETURN n.name as n_name,id(n) as nid,properties(n) AS properties')
        index_dic = {}
        for i in rel[0]:
            index_dic.update({'id': i[1], 'name': i[0], "property": []})
            for k in i[2]:
                if k!='bq':
                   index_dic["property"].append([k, i[2][k]])
            entity.append(index_dic)

    if len(link):
        return JsonResponse({'flag':'yes','link':link,'entity':entity})
    else:
        return JsonResponse({'flag':'no','link':link,'entity':entity})

def graph_route(request,pk):
    rel = db.cypher_query(query='MATCH (n) where id(n)=' + str(pk) + ' RETURN n.orign')
    ce=rel[0][0][0]
    re_len = len(ce)
    xin = []
    d_ce = {}  # 用于映射
    for c in range(re_len):
        map_list = []
        xce = ce[re_len - c - 1].split(':')
        fens = xce[1][1:][:-1].split(',')
        if len(fens[0])==0 and fens[0]=='':
            pass
        else:
            for fen in fens:  # ['ddosi|pg|gg', 'ddosi|pg|ff', 'root|mysql|ee']
                fe = fen.split('|')
                flag = 1
                for m in map_list:
                    if fe[0] == m['db_name']:  # 如果数据库名存在,中断遍历，
                        if fe[1] == m['db_type']:  # 如果数据库类型也相等
                            m['tables']=(fe[2])  # 把表添加到该类型的数据库中
                            flag = 0
                            break
                        else:  # 如果数据库类型不相同，只是数据库名字相同，就添加该项
                            map_dic = {'db_name': fe[0], 'db_type': fe[1], 'tables': fe[2]}
                            map_list.append(map_dic)
                            flag = 0
                            break
                if flag:  # 如果flag等于1，即遍历完没有数据库名字相同的，就直接添加该项
                    map_dic = {'db_name': fe[0], 'db_type': fe[1], 'tables': fe[2]}
                    map_list.append(map_dic)

        if xce[0] in d_ce.keys():
            for x in map_list:
                d_ce[xce[0]].append(x)
        else:
            d_ce[xce[0]] = map_list

    for k in d_ce:
        xin.append({'time':k,'info':d_ce[k]})
    return JsonResponse({'route_info':xin})
#标签选择
def entity_label_choose(request,pk):
    search = request.GET.get('search')
    lable_name = '_' + str(pk)
    rel = db.cypher_query(query='MATCH (n:' + lable_name + ') RETURN n.name as n_name,id(n) as n_id,n.bq as bq,properties(n) AS properties')  # 获取节点的属性信息
    entity_label=[]
    for i in rel[0]:
        if search in i[2]:
            dico = {"id": i[1], "name": i[0],'bq':[],'property':[]}
            for k in i[3]:
                if k!='bq' and k!='ruku':
                    dico['property'].append([k, 'string', i[3][k]])
                elif k=='bq':
                    biaoqian_li = i[3][k]
                    biaoqian_list = biaoqian_li[1:][:-1].split(',')
                    for bi in biaoqian_list:
                        bio = bi.replace('\'', '')
                        bio = bio.replace(' ', '')
                        dico['bq'].append(bio)
            entity_label.append(dico)
    return JsonResponse({'entity_label':entity_label})

#关系标签选择
def relation_label_choose(request,pk):
    search = request.GET.get('search') #关系标签
    lable= '_' + str(pk)
    rel = db.cypher_query(query='MATCH (n:' + lable + ')-[r]->(m:' + lable + ') where r.re_type='+'\''+search+'\''+' RETURN id(n) as nid,id(m) as mid,type(r)')
    sanyuanzu = []  # 三元组
    n_id = []
    m_id = []
    for i in rel[0]:
        n_id.append(i[0])
        m_id.append(i[1])
        sanyuanzu.append({'from':i[0], 'name':i[2],'fx':'单向','to':i[1]})
    entity = list(set(n_id+m_id))
    index_result = []  # 搜索结果
    for en in entity:
        rel = db.cypher_query(
            query='MATCH (n:' + lable + ') where id(n)=' + str(en) + ' RETURN n.name as n_name,id(n) as nid,properties(n) AS properties')
        index_dic = {}
        for i in rel[0]:
            index_dic.update({'id': i[1], 'name': i[0],'bq':[], "property": []})
            for k in i[2]:
                if k=='bq':
                    biaoqian_li = i[2][k]
                    biaoqian_list = biaoqian_li[1:][:-1].split(',')
                    for bi in biaoqian_list:
                        bio = bi.replace('\'', '')
                        bio = bio.replace(' ', '')
                        index_dic['bq'].append(bio)
                else:
                    index_dic["property"].append([k, i[2][k]])
        index_result.append(index_dic)
    return JsonResponse({'entity': index_result, 'sanyuanzu': sanyuanzu})





