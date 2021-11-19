import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from neomodel import db
from kgmodel.models import Kgmodel
import time

def index(request):  # 模型管理主页面
    global context
    search = request.GET.get('search')
    resourcebeta = request.GET.get('resourcebeta')  # 自然资源要素
    businesstype = request.GET.get('businesstype')  # 业务类型

    if search:
        if resourcebeta:
            if businesstype:
                querys = 'MATCH (n:Kgmodel) where n.name=~".*{0}.*" and n.Businesstype={1} and n.Resourcemeta={2} RETURN id(n), n.name,n.created,' \
                         'n.Businesstype,n.Resourcemeta,n.Modelversion,n.Modelstatus,n.Modeldecribe'.format(search,
                                                                                                            '"' + businesstype + '"',
                                                                                                            '"' + resourcebeta + '"')
            else:
                querys = 'MATCH (n:Kgmodel) where n.name=~".*{0}.*" and  n.Resourcemeta={1}  RETURN id(n), n.name,n.created,n.businesstype,' \
                         'n.Resourcemeta,n.Modelversion,n.Modelstatus,n.Modeldecribe'.format(
                    search, '"' + resourcebeta + '"')
        else:
            if businesstype:
                querys = 'MATCH (n:Kgmodel) where n.name=~".*{0}.*" and n.Businesstype={1}  RETURN id(n), n.name,n.created,n.Businesstype,' \
                         'n.Resourcemeta,n.Modelversion,n.Modelstatus,n.Modeldecribe'.format(search,
                                                                                             '"' + businesstype + '"')
            else:
                querys = 'MATCH (n:Kgmodel) where n.name=~".*{0}.*"   RETURN id(n),n.name,n.created,n.Businesstype,' \
                         'n.Resourcemeta,n.Modelversion,n.Modelstatus,n.Modeldecribe'.format(
                    search)

    else:
        if resourcebeta:
            if businesstype:
                querys = 'MATCH (n:Kgmodel) where  n.Businesstype={0} and n.Resourcemeta={1}  RETURN id(n),n.name,n.created,n.Businesstype,' \
                         'n.Resourcemeta,n.Modelversion,n.Modelstatus,n.Modeldecribe'.format('"' + businesstype + '"',
                                                                                             '"' + resourcebeta + '"')
            else:
                querys = 'MATCH (n:Kgmodel) where  n.Resourcemeta={0}  RETURN id(n), n.name,n.created,n.Businesstype,n.Resourcemeta,' \
                         'n.Modelversion,n.Modelstatus,n.Modeldecribe'.format(
                    '"' + resourcebeta + '"')
        else:
            if businesstype:
                querys = 'MATCH (n:Kgmodel) where  n.Businesstype={0}  RETURN id(n), n.name,n.created,n.Businesstype,' \
                         'n.Resourcemeta,n.Modelversion,n.Modelstatus,n.Modeldecribe'.format('"' + businesstype + '"')
            else:
                querys = 'MATCH (n:Kgmodel)  RETURN id(n),n.name,n.created,n.Businesstype,n.Resourcemeta,' \
                         'n.Modelversion,n.Modelstatus,n.Modeldecribe'
    data = db.cypher_query(query=querys)

    model_list = [dict(zip(["id", "modelname", "created", "Businesstype", "Resourcemeta", "Modelversion", "Modelstatus",
                            "Modeldecribe"], i)) for i in data[0]]
    yewu = []
    zyys = []
    for i in model_list:
        if i["Businesstype"] not in yewu:
            yewu.append(i["Businesstype"])
        if i["Resourcemeta"] not in zyys:
            zyys.append(i["Resourcemeta"])

    context = {
        'model': model_list,
        'search': search,
        "yewu": yewu,
        "zr": zyys,
        "resourcebeta": resourcebeta,
        "businesstype": businesstype
    }
    return JsonResponse(context)

def detail(request, pk):  # 模型详情页，获取该模型的整个图谱
    global flag, re_fx,re_lx, re_name, fana
    dic = {'entity_label': [], 'entity_relation': []}
    res = db.cypher_query(query='MATCH (r) WHERE id(r) =' + str(pk) + ' RETURN r.name as i_name,r.Businesstype as i_type,r.Modelstatus as i_status,r.Resourcemeta as i_resour,r.Modeldecribe as i_decri,r.Modelversion as i_vers')
    dic.update({'Modelname': res[0][0][0]})
    dic.update({'Modelstatus':res[0][0][2]})
    dic.update({'Businesstype':res[0][0][1]})
    dic.update({'Resourcemeta': res[0][0][3]})
    dic.update({'Modeldecribe': res[0][0][4]})
    dic.update({'Modeldecribe': res[0][0][4]})
    dic.update({'Modelversion':res[0][0][5]})

    label='_'+str(pk)#下划线加数字构成标签名
    entity_label = []#获取实体标签
    rel = db.cypher_query(query='MATCH (n:'+label+') RETURN n.name as n_name,id(n) as n_id,properties(n) AS properties')
    for i in rel[0]:
        for j in i[2]:
            if j=='名称':
                fana=i[2][j]
        dico = {"id": i[1], "name": i[0]}
        dico.update({'label': []})
        dico.update({'property': []})
        dico['property'].append(['name','string',fana])
        for j in i[2]:
            if j!='名称' and j!='name':
                if j == 'bq':
                    lable = i[2][j]
                    t = lable.split(',')
                    for k in t:
                        if k:
                            dico['label'].append(k)
                else:
                    dico['property'].append([j, 'string', i[2][j]])

        entity_label.append(dico)
    dic['entity_label']=entity_label

    result1 = db.cypher_query(
        query="MATCH (a:" + label + ")-[r]->(b:" + label + ") RETURN id(a) as a_id,type(r),properties(r) as pro,id(r) as rid,id(b) as b_id")  # 获取三元组（1，包含，2）
    for i in result1[0]:
        sx=[]
        for j in i[2]:
            if j=="fx":
                re_fx=i[2][j]
            elif j=='name':
                re_name=i[2][j]
                sx.append(['name','string',i[2][j]])
            elif j == 'lx':
                re_lx=i[2][j]
            else:
                sx.append([j,'string',i[2][j]])
        dic['entity_relation'].append({"from":i[0],"re_name":re_name,"fx":re_fx,"lx":re_lx,"sx":sx,"id":i[3],"to":i[4]})
    return JsonResponse(dic)


def entry(request):  # 进入模型创建初始化的页面
    label_name = []
    rel = db.cypher_query(query='MATCH (n:Kgmodel) RETURN id(n)')
    for i in rel[0]:
        label_name.append(i[0])
    entity = []
    relation = []
    for label in label_name:
        label_id='_'+str(label)
        rel = db.cypher_query(query='MATCH (n:'+label_id+') RETURN n.name as n_name,id(n) as n_id,n.bq as n_label LIMIT 20') #实体
        for i in rel[0]:
            label_list = []
            for j in i[2].split(','):
                if j:
                    label_list.append(j)
            entity.append({"name": i[0], 'id': i[1], 'label': label_list})
        rel = db.cypher_query(query='MATCH (n:'+label_id+')-[r]->(m:'+label_id+') RETURN type(r),r.name as r_name LIMIT 20')#关系
        for i in rel[0]:
            relation.append({"type":i[0],"direction":'单向',"name":i[1]})
    context = {'Relation': relation, 'entity': entity}
    return JsonResponse(context)

@csrf_exempt
def create(request):
    '''
    {'Modelname': '124', 'Modelversion': 'v1', 'Modelstatus': '首次创建', 'Businesstype': '用地预审', 'Resourcemeta': '矿产', 'Modeldecribe': '1243'}
    [{"id":0,"name":"实体名","bqArr":[{"bq":"业务"}],"sx":[{"bsm":"name","type":"float","text":"4"},{"bsm":"唯一标识码","type":"float","text":"5"},{"bsm":"业务数据","type":"string","text":"66"}]}]
    [{"from":"0","re_name":"同源","re_type":"时空关系","to":"1"}]
    '''
    global entity_name, label_list, entity_sx_list, entity_id, e_name
    t = json.loads(request.body.decode())
    modelshuxing = []  # 存储属性值
    modelbase = t["modelinfo"]  # 模型基本信息
    mode_name = modelbase['Modelname']

    #判断模型名是否存在
    model_list = []  # 模型名列表
    result = db.cypher_query(query='match (n:Kgmodel) return n.name as n_name')
    for i in result[0]:
        model_list.append(i[0])
    if mode_name in model_list:
        return JsonResponse({"msg":"模型已存在！请重新输入"})

    Kgmodel(name=mode_name).save() #保存模型名称
    for i in modelbase:
        if i != 'Modelname':
            modelshuxing.append(modelbase[i])
    mode_vesion = modelshuxing[0]
    mode_statue = modelshuxing[1]
    mode_type = modelshuxing[2]
    mode_Resour = modelshuxing[3]
    mode_decr = modelshuxing[4]
    mode_created=str(time.strftime("%Y-%m-%d", time.localtime()))
    db.cypher_query(
        query='MERGE (n:Kgmodel {name: ' + '\"' + mode_name + '\"' + '}) SET n.Modelversion = ' + '\"' + mode_vesion + '\"' + ',n.Modelstatus = ' + '\"' + mode_statue + '\"' + ',n.Businesstype = ' + '\"' + mode_type + '\"' + ',n.Resourcemeta = ' + '\"' + mode_Resour + '\"' + ',n.Modeldecribe = ' + '\"' + mode_decr + '\"' + ',n.created = ' + '\"' + mode_created + '\"' + ' return id(n)')

    sheng = Kgmodel.nodes.get(name=mode_name)
    lable='_'+str(sheng.id)#标签名
    # 在创建的标签下添加实体
    model_entity_base = t["model_entity"]

    for i in model_entity_base:
        entity_id = i['id']
        entity_name = i["name"]
        label_list = entity_name+','
        for lb in i["bqArr"]:
            if lb:
               label_list += lb+ ','
        entity_sx_list = []  # 实体属性列表，[[属性名1：属性值],[属性名2：属性值]]
        for att in i["sx"]:
            if att[0]!='name':
               entity_sx_list.append([att[0], att[2]])
            else:
                e_name=att[2]
        db.cypher_query(
            query='CREATE(n:' + lable + '{name:' + '\"' + entity_name + '\"' + ',entity_id:' + '\"' + str(entity_id) + '\"' + ',名称:' + '\"' + e_name + '\"' + ',bq:' + '\"' + label_list + '\"' + '})')

        for k in entity_sx_list:  # 遍历设置属性
            db.cypher_query(
                query='match(n:' + lable + ') where n.entity_id=' + '\"' + str(entity_id) + '\"' + ' set n.' + k[0] + '=' + '\"' + k[1] + '\"' + '')

    #实体关系
    entity_relation = t["entity_relation"]
    for er in entity_relation:
        re_from = er["from"]
        re_to = er["to"]
        re_name = er["re_name"]
        re_fx=er["fx"]
        re_lx=er['lx']
        re_sx=er["sx"]
        sx_string='{fx:'+'\"' + re_fx + '\"'+ ',lx:'+'\"' + re_lx + '\"'+','
        for sx in re_sx:
            sx_string+=sx[0]+':'+'\"' + sx[2] + '\"'+ ','
        sx_string=sx_string[:-1]
        sx_string+='}'
        db.cypher_query(
            query='MATCH (u:' + lable + '{entity_id:' + '\"' + str(re_from) + '\"' + '}),(y:' + lable + ' {entity_id:' + '\"' + str(re_to) + '\"' + '}) create (u)-[:'+ re_name +sx_string+']->(y)')

    context = {'msg': '创建成功！'}
    return JsonResponse(context)

#模型的删除
def delete(request, pk):
    db.cypher_query(query='MATCH (r) WHERE id(r) ='+str(pk)+' DETACH DELETE r')
    label='_'+str(pk)
    db.cypher_query(query='MATCH (r:' +label+ ') DETACH DELETE r')
    return JsonResponse({'msg':'删除成功！'})

#模型编辑
@csrf_exempt
def edit(request,pk):
    global e_name
    t = json.loads(request.body.decode())
    dl = db.cypher_query(query='MATCH (r) WHERE id(r) ='+str(pk)+' RETURN r.name')
    db.cypher_query(query='MATCH (r:' + '_'+str(pk)+ ') DETACH DELETE r')
    modelshuxing = []  # 存储属性值
    modelbase = t["modelinfo"]  # 模型基本信息
    mode_name = modelbase['Modelname']

    for i in modelbase:
        if i != 'Modelname':
            modelshuxing.append(modelbase[i])
    mode_vesion = modelshuxing[0]
    mode_statue = modelshuxing[1]
    mode_type = modelshuxing[2]
    mode_Resour = modelshuxing[3]
    mode_decr = modelshuxing[4]
    mode_created = str(time.strftime("%Y-%m-%d", time.localtime()))
    db.cypher_query(
        query='MATCH (n:Kgmodel) WHERE id(n)='+str(pk)+' SET n.name=' + '\"' + mode_name + '\"' + ',n.Modelversion = ' + '\"' + mode_vesion + '\"' + ',n.Modelstatus = ' + '\"' + mode_statue + '\"' + ',n.Businesstype = ' + '\"' + mode_type + '\"' + ',n.Resourcemeta = ' + '\"' + mode_Resour + '\"' + ',n.Modeldecribe = ' + '\"' + mode_decr + '\"' + ',n.created = ' + '\"' + mode_created + '\"' + '')

    label_name='_'+str(pk)
    model_entity_base = t["model_entity"]
    for i in model_entity_base:
        entity_id = i['id']
        e_id=i['entity_id']
        entity_name = i["name"]
        label_list = entity_name+','
        for lb in i["bqArr"]:
            if lb:
               label_list += lb + ','
        sx=''
        for att in i["sx"]:
            if att[0]!='entity_id' and att[0]!='name':
               sx+=att[0]+':'+"\'"+att[2]+"\'"+','
            elif att[0]=='name':
                e_name = att[2]
        sx=sx[:-1]
        db.cypher_query(
            query='CREATE(n:' + label_name + '{name:' + '\"' + entity_name + '\"' + ',entity_id:' + '\"' + str(e_id) + '\"' + ',名称:' + '\"' + e_name + '\"' + ',bq:' + '\"' + label_list + '\"' + '})')
        db.cypher_query(
                query='match(n:' + label_name + ') where n.entity_id=' + '\"' + str(entity_id) + '\"' + ' set n+={'+sx+'}')

    # 实体关系
    entity_relation = t["entity_relation"]
    for er in entity_relation:
        re_from = er["from"] #获取该节点的entity_id
        re_to = er["to"]
        re_name = er["re_name"]
        re_fx = er["fx"]
        re_lx = er['lx']
        re_sx = er["sx"]
        sx_string = '{fx:' + '\"' + re_fx + '\"' + ',lx:' + '\"' + re_lx + '\"' + ','
        for sx in re_sx:
            sx_string += sx[0] + ':' + '\"' + sx[2] + '\"' + ','
        sx_string = sx_string[:-1]
        sx_string += '}'
        db.cypher_query(
            query='MATCH (u:' + label_name +'),(y:' + label_name + ') WHERE u.entity_id='+'\"'+str(re_from) +'\"'+' and y.entity_id='+'\"'+str(re_to)+'\"'+' create (u)-[:' + re_name + sx_string + ']->(y)')
    return JsonResponse({'msg': '编辑成功！'})


