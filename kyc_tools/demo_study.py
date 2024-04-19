
#将两个列表组成字典
def dict_make():
    key_list = ['name', 'age', 'gender']
    value_list = ['Tom', 20, 'male']

    #可以将一个列表作为键，另一个列表作为值，创建字典——方法1
    dictionary = dict(zip(key_list, value_list))
    #字典推导式——方法2
    result = {key: value for key,value in zip(key_list,value_list)}
    print(f"{dictionary}")
    print(f'{result}')

#对一个字典列表进行排序
def dict_sort():
    dict_lists = [{"name":"James",
                   "age":20},
                  {"name":"May",
                   "age":14},
                  {"name":"Katy",
                   "age":23}]

    dict_demo = {"age":3,
                 "name":2,
                 "gender":1}
    #此处使用sort的时候会 遍历 每个元素，然后将其作为输入参数传递给lambda——适用于列表嵌套字典
    dict_lists.sort(key=lambda item: item.get("age"))
    print(f'{dict_lists}')

    #此处是适用对象 仅仅针对一个字典进行排序
    result = dict(sorted(dict_demo.items(),key = lambda item :item[1]))
    print(f"{result}")

#找出列表中的最大值和最小值的索引
demo_list =[4,7,2,1,3,5]
def minIndex(lis : list):
    return min(range(len(lis)) , key=lis.__getitem__)

def maxIndex(lis : list):
    return max(range(len(lis)),key = lis.__getitem__)

#同时遍历索引和值
def get_index_and_value(lis : list):
    for index,value in enumerate(lis):
        print(index,value)

#合并两个字典
def merge_dict():
    dict1  = {"age": 3,
              "name": 2,
            "gender": 1}

    dict2 = {"name":"my",
             "age1":20,}
    merge_result = {**dict1,**dict2}
    print(merge_result)


def demo_tst(get_params:str,**params1):
    return params1.get(get_params)

if __name__ == '__main__':
    dict_demo = {"age": 3,
                 "name": 2,
                 "gender": 1}
    print(dict_demo.get("id_no","12345678901234567890"))
#字典取值不要用 dict_demo['age'],请用get
    # print(dict_demo.get('age1',18)) #如果key不存在，返回默认值18
    # print(dict_demo['age1']) #程序直接崩溃

