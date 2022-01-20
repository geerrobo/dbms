from django.shortcuts import render
import psycopg2

def index(request):
    return render(request, 'engine/index.html', {})


class Raw:
    def __init__(self, selectRaw='', fromRaw='', whereRaw='', andRaw=''):
        self.selectRaw = selectRaw
        self.fromRaw = fromRaw
        self.whereRaw = whereRaw
        self.andRaw = andRaw


def getSplit(str):
    if 'SELECT' not in str or 'FROM' not in str:
        return render(request, 'engine/index.html', {'raw': 'No input or syntax error.', 'input': str})
    splits = str.split('\n')

    hasSelect = True
    hasFrom = True
    hasWhere = False
    hasAnd = False
    previous_command = ''
    strSelect = ''
    strFrom = ''
    strWhere = ''
    strAnd = ''
    for split in splits:
        # print('SPLIT : ', split)
        if 'SELECT' in split:
            strSelect = split
            previous_command = 'SELECT'

        elif 'FROM' in split:
            strFrom = split
            previous_command = 'FROM'

        elif 'WHERE' in split:
            strWhere = split
            hasWhere = True
            previous_command = 'WHERE'

        elif 'AND' in split:
            strAnd = split
            hasAnd = True
            previous_command = 'AND'
        else:
            if previous_command is 'SELECT':
                strSelect = strSelect + split
            elif previous_command is 'FROM':
                strFrom = strFrom + split
            elif previous_command is 'WHERE':
                strWhere = strWhere + split
        # print('select EEE')
        # print(strSelect)
        # print('from EEE')
        # print(strFrom)
    # print('strSelect : ', strSelect)
    # print('strFrom : ', strFrom)
    return Raw(selectRaw=strSelect, fromRaw=strFrom, whereRaw=strWhere, andRaw=strAnd)


def getCost(exps):
    costs = 0
    for exp in exps:
        exp = ''.join(exp)
        if '..' in exp:
            index = exp.index('..')
            # print('find cost: ', index)
            c = exp[index+2:index+15]
            print(c)
            split = c.split(' row')
            costs = costs + float(split[0])
            # print(costs, ' + ', float(split[0]))
    costs = round(costs, 2)
    # print(costs)
    return costs


def rawQuery(request):
    str = request.POST['textInput']
    str = str.replace(';', '')
    costs = -1
    if 'EXPLAIN (ANALYZE ON)' in str or 'EXPLAIN (ANALYZE TRUE)' in str or 'EXPLAIN (COSTS ON)' in str or 'EXPLAIN (COSTS TRUE)' in str:
        conn = psycopg2.connect(
            host='localhost',
            database='habr',
            user='postgres',
            password='2540',
        )
        cur = conn.cursor()
        cur.execute(str)
        explain = cur.fetchall()
        costs = getCost(explain)
    else:
        conn = psycopg2.connect(
            host='localhost',
            database='habr',
            user='postgres',
            password='2540',
        )
        cur = conn.cursor()
    strRaw = str
    split = getSplit(str)
    # for feature 2 check
    cur.execute('select schemaname as schema_name, matviewname as view_name, matviewowner as owner, ispopulated as '
                'is_populated, definition from pg_matviews order by schema_name, view_name;')
    mat_lists = cur.fetchall()
    result = str
    result_bool = False
    for mat_list in mat_lists:
        split2 = getSplit(mat_list[4])
        slFrom = split.fromRaw.replace(' ', '')
        slFrom2 = split2.fromRaw.replace(' ', '')
        print('split from : ', slFrom)
        print('split2 from : ', slFrom2)
        if slFrom in slFrom2 or slFrom2 in slFrom:
            print('from in')
            print('split where : ', split.whereRaw)
            print('split2 where : ', split2.whereRaw.lstrip(' '))
            if split2.whereRaw.lstrip(' ') in split.whereRaw or split.whereRaw in split2.whereRaw:
                print('where in')
                strRaw = split.selectRaw + 'FROM ' + mat_list[1]
                result_bool = True
                break
    # for feature 2 check
    cur.execute(str)
    all = cur.fetchall()
    cur.close()
    conn.close()

    if split.selectRaw is not '' and split.fromRaw is not '' and split.whereRaw is not '' and not result_bool:
        if 'OR' in split.whereRaw:
            print('HAS OR')
            temp = split.whereRaw.split('(')
            temp1 = temp[1].split(')')
            inWheres = temp1[0].split(' OR ')
            i = 0
            raws = []
            for inWhere in inWheres:
                raws.append(split.selectRaw + '\n' + split.fromRaw +
                            '\n' + 'WHERE ' + inWhere + '\n')
                if split.andRaw is not '':
                    raws[i] = raws[i] + split.andRaw + '\n'
                i = i + 1
            strRaw = ''
            for raw in raws:
                if raw is raws[-1]:
                    strRaw = strRaw + raw
                else:
                    strRaw = strRaw + raw + 'UNION ALL\n'
            # print(strSelect)
            return render(request, 'engine/index.html', {'raw': strRaw, 'input': str, 'table': all, 'costs': costs})
        else:
            print('no or')
            return render(request, 'engine/index.html', {'raw': strRaw, 'input': str, 'table': all, 'costs': costs})
    else:
        return render(request, 'engine/index.html', {'raw': strRaw, 'input': str, 'table': all, 'costs': costs})


# def rawQuery2(request):
#     str = request.POST['textInput']
#     split = getSplit(str)
#
#     if split.selectRaw is not '' and split.fromRaw is not '' and split.whereRaw is not '':
#         conn = psycopg2.connect(
#             host='localhost',
#             database='habr',
#             user='postgres',
#             password='2540',
#         )
#         cur = conn.cursor()
#         cur.execute('select schemaname as schema_name, matviewname as view_name, matviewowner as owner, ispopulated as '
#                     'is_populated, definition from pg_matviews order by schema_name, view_name;')
#         mat_lists = cur.fetchall()
#         print(mat_lists)
#         result = str
#         for mat_list in mat_lists:
#             split2 = getSplit(mat_list[4])
#             # print('split from : ', split.fromRaw.lstrip(' '), 'aah')
#             if split2.fromRaw.lstrip(' ') in split.fromRaw:
#                 if split2.whereRaw.lstrip(' ') in split.whereRaw:
#                     result = split.selectRaw + 'FROM ' + mat_list[1]
#                     break
#         cur.execute(str)
#         all = cur.fetchall()
#         cur.close()
#         conn.close()
#         # strRaw = 'SELECT fname, dname\nFROM cpe_stu\nWHERE gpa>3.25'
#         return render(request, 'engine/index.html', {'raw': result, 'input': str, 'table': all})
#     else:
#         return render(request, 'engine/index.html', {'raw': 'Don\'t has WHERE.', 'input2': str})
